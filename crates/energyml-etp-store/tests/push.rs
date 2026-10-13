//! Tier 5 - a store pushed into a dataspace, in islands of transactions.
//!
//! **Server to server**, which is the case the open question names: the
//! source is the bridge's fake store read through [`EnergymlStorage`], the
//! destination a second fake that takes writes and remembers them. Nothing
//! here reaches a real server: no test writes to a server that is not a fake.

mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use energyml_etp_store::push::{Options, Progress, Stop, WellKnown, WriteFormat, push};
use retp_avro::Role;
use retp_client::memory::{ManualClock, MemoryTransport};
use retp_client::{Client, Settings, writing_client};
use retp_proto::connection::{Connection, Declined, Handled, Handler, Responder};
use retp_proto::negotiate::{Offer, ProtocolOffer};
use retp_proto::wire::{Body, Delivery, EtpError};
use retp_types::datatypes::object::PutResponse;
use retp_types::datatypes::{AnyArrayItem, SupportedDataObject, Uuid};
use retp_types::dispatch::Message;
use retp_types::protocol::data_array::{
    PutDataArraysResponse, PutDataSubarraysResponse, PutUninitializedDataArraysResponse,
};
use retp_types::protocol::store::PutDataObjectsResponse;
use retp_types::protocol::transaction::{
    CommitTransactionResponse, RollbackTransactionResponse, StartTransactionResponse,
};

use common::*;

/// What the receiving store was given, in the order it was given.
#[derive(Default)]
struct Log {
    /// Each object's destination URI and its bytes.
    objects: Vec<(String, Vec<u8>)>,
    /// The `format` each object came with.
    formats: Vec<String>,
    /// Each array by `(uri, path)`: its dimensions and its doubles.
    arrays: BTreeMap<(String, String), (Vec<i64>, Vec<f64>)>,
    /// `start`, `array`, `commit`, `rollback`, as they came - `array` for a
    /// whole array or a declaration, so that *the arrays go before the
    /// commit* is an order this log can show.
    transactions: Vec<&'static str>,
    /// Refuse every commit with this reason.
    refuse_commit: Option<&'static str>,
    /// Refuse every object written, with this reason - a dataspace opened
    /// read-only, which most are.
    refuse_objects: Option<&'static str>,
    /// Announce Transaction (18) and then answer `ENOTSUPPORTED` to every
    /// `StartTransaction` - what some REST APIs do, naming the transaction
    /// `null`.
    no_transactions: bool,
    /// What this store wrote on its own, and when - listed by Discovery
    /// after a `storeLastWriteFilter`.
    changed: Vec<(String, i64)>,
    /// What it deleted, and when.
    gone: Vec<(String, i64)>,
}

/// A store that takes writes and remembers them - the destination.
struct Receiving {
    protocol: i32,
    log: Arc<Mutex<Log>>,
}

impl Handler for Receiving {
    fn offer(&self) -> ProtocolOffer {
        ProtocolOffer::v12(self.protocol, Role::Store)
    }

    fn handle(&mut self, delivery: &Delivery, responder: &mut Responder<'_>) -> Result<Handled, Declined> {
        let Body::Message(message) = &delivery.body else {
            return Err(Declined("no private protocol".into()));
        };
        let mut log = self.log.lock().unwrap();
        match message {
            Message::StartTransaction(_) => {
                if log.no_transactions {
                    let out = responder.wire().refuse(
                        delivery,
                        &EtpError::ENOTSUPPORTED.with_detail("no transactions here"),
                    );
                    responder.send(out);
                    return Ok(Handled::Answered);
                }
                log.transactions.push("start");
                let response = StartTransactionResponse {
                    transaction_uuid: Uuid([9; 16]),
                    successful: true,
                    failure_reason: String::new(),
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            Message::CommitTransaction(request) => {
                log.transactions.push("commit");
                let response = CommitTransactionResponse {
                    transaction_uuid: request.transaction_uuid,
                    successful: log.refuse_commit.is_none(),
                    failure_reason: log.refuse_commit.unwrap_or("").to_string(),
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            Message::RollbackTransaction(request) => {
                log.transactions.push("rollback");
                let response = RollbackTransactionResponse {
                    transaction_uuid: request.transaction_uuid,
                    successful: true,
                    failure_reason: String::new(),
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            Message::PutDataObjects(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_objects.keys().cloned());
                let mut success = BTreeMap::new();
                for (key, object) in &request.data_objects {
                    if let Some(why) = log.refuse_objects {
                        reply
                            .failed(
                                key.clone(),
                                &EtpError::EREQUEST_DENIED.with_detail(why.to_string()),
                            )
                            .unwrap();
                        continue;
                    }
                    log.objects
                        .push((object.resource.uri.clone(), object.data.clone()));
                    log.formats.push(object.format.clone());
                    reply.answered(key.clone()).unwrap();
                    success.insert(key.clone(), PutResponse::default());
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &PutDataObjectsResponse { success })
                    .unwrap();
                responder.send_all(out);
            }
            Message::PutDataArrays(request) => {
                log.transactions.push("array");
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_arrays.keys().cloned());
                for (key, put) in &request.data_arrays {
                    if let AnyArrayItem::ArrayOfDouble(values) = &put.array.data.item {
                        log.arrays.insert(
                            (put.uid.uri.clone(), put.uid.path_in_resource.clone()),
                            (put.array.dimensions.clone(), values.values.clone()),
                        );
                    }
                    reply.answered(key.clone()).unwrap();
                }
                let out = responder
                    .wire()
                    .last(
                        &mut reply,
                        &PutDataArraysResponse {
                            success: BTreeMap::new(),
                        },
                    )
                    .unwrap();
                responder.send_all(out);
            }
            Message::PutUninitializedDataArrays(request) => {
                log.transactions.push("array");
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_arrays.keys().cloned());
                for (key, put) in &request.data_arrays {
                    let count: i64 = put.metadata.dimensions.iter().product();
                    log.arrays.insert(
                        (put.uid.uri.clone(), put.uid.path_in_resource.clone()),
                        (
                            put.metadata.dimensions.clone(),
                            vec![f64::NAN; count.max(0) as usize],
                        ),
                    );
                    reply.answered(key.clone()).unwrap();
                }
                let out = responder
                    .wire()
                    .last(
                        &mut reply,
                        &PutUninitializedDataArraysResponse {
                            success: BTreeMap::new(),
                        },
                    )
                    .unwrap();
                responder.send_all(out);
            }
            Message::PutDataSubarrays(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_subarrays.keys().cloned());
                for (key, put) in &request.data_subarrays {
                    let Some((dimensions, into)) = log
                        .arrays
                        .get_mut(&(put.uid.uri.clone(), put.uid.path_in_resource.clone()))
                    else {
                        reply
                            .failed(
                                key.clone(),
                                &EtpError::ENOT_FOUND.with_detail("not declared".to_string()),
                            )
                            .unwrap();
                        continue;
                    };
                    let AnyArrayItem::ArrayOfDouble(from) = &put.data.item else {
                        continue;
                    };
                    let rank = dimensions.len();
                    let run = put.counts[rank - 1] as usize;
                    let rows: i64 = put.counts.iter().product::<i64>() / put.counts[rank - 1].max(1);
                    let mut index = vec![0i64; rank.saturating_sub(1)];
                    for row in 0..rows as usize {
                        let offset = (0..rank).fold(0i64, |offset, k| {
                            let at = if k + 1 == rank {
                                put.starts[k]
                            } else {
                                put.starts[k] + index[k]
                            };
                            offset * dimensions[k] + at
                        }) as usize;
                        into[offset..offset + run].copy_from_slice(&from.values[row * run..row * run + run]);
                        for k in (0..rank.saturating_sub(1)).rev() {
                            index[k] += 1;
                            if index[k] < put.counts[k] {
                                break;
                            }
                            index[k] = 0;
                        }
                    }
                    reply.answered(key.clone()).unwrap();
                }
                let out = responder
                    .wire()
                    .last(
                        &mut reply,
                        &PutDataSubarraysResponse {
                            success: BTreeMap::new(),
                        },
                    )
                    .unwrap();
                responder.send_all(out);
            }
            Message::GetResources(request) => {
                // What was written after the filter (strict, §3.12.8); every
                // change when no filter is given.
                let after = request.store_last_write_filter.unwrap_or(i64::MIN);
                let resources: Vec<retp_types::datatypes::object::Resource> = log
                    .changed
                    .iter()
                    .filter(|(_, when)| *when > after)
                    .map(|(uri, when)| retp_types::datatypes::object::Resource {
                        uri: uri.clone(),
                        store_last_write: *when,
                        ..Default::default()
                    })
                    .collect();
                let response = retp_types::protocol::discovery::GetResourcesResponse { resources };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            Message::GetDeletedResources(request) => {
                let after = request.delete_time_filter.unwrap_or(i64::MIN);
                let deleted_resources = log
                    .gone
                    .iter()
                    .filter(|(_, when)| *when > after)
                    .map(|(uri, when)| retp_types::datatypes::object::DeletedResource {
                        uri: uri.clone(),
                        deleted_time: *when,
                        ..Default::default()
                    })
                    .collect();
                let response =
                    retp_types::protocol::discovery::GetDeletedResourcesResponse { deleted_resources };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            _ => return Err(Declined("not in the receiving server".into())),
        }
        Ok(Handled::Answered)
    }
}

/// A writing session with a store that remembers what it is given.
fn receiving(refuse_commit: Option<&'static str>) -> (Client<MemoryTransport, ManualClock>, Arc<Mutex<Log>>) {
    receiving_with(Log {
        refuse_commit,
        ..Log::default()
    })
}

fn receiving_with(log: Log) -> (Client<MemoryTransport, ManualClock>, Arc<Mutex<Log>>) {
    receiving_speaking(log, &[3, 4, 9, 18])
}

/// A receiving store that speaks these protocols and no other - without 18,
/// a store that takes puts but knows no transaction.
fn receiving_speaking(
    log: Log,
    protocols: &[i32],
) -> (Client<MemoryTransport, ManualClock>, Arc<Mutex<Log>>) {
    let log = Arc::new(Mutex::new(log));
    let mut offer = Offer::new("a receiving store", "0.1", Uuid([3; 16]));
    // Both forms, so that a push may say which: the session agrees the
    // intersection, and a JSON push into a store that agreed XML alone is
    // refused before it leaves - by the client, by name.
    offer.formats = vec!["xml".to_string(), "json".to_string()];
    offer.data_objects = ["resqml20.*", "eml20.*", "eml23.*"]
        .iter()
        .map(|t| SupportedDataObject {
            qualified_type: t.to_string(),
            data_object_capabilities: BTreeMap::new(),
        })
        .collect();
    let handlers: Vec<Box<dyn Handler>> = protocols
        .iter()
        .map(|p| {
            Box::new(Receiving {
                protocol: *p,
                log: Arc::clone(&log),
            }) as Box<dyn Handler>
        })
        .collect();
    let server = Connection::server(offer, handlers, None, Uuid([0xBC; 16]), T0).unwrap();
    let clock = ManualClock::new(T0);
    let (offer, handlers) = writing_client(
        "the push's tests",
        "0.1",
        Uuid([4; 16]),
        &[
            "resqml20.*".to_string(),
            "eml20.*".to_string(),
            "eml23.*".to_string(),
        ],
    );
    let client = Client::open(
        MemoryTransport::new(server, clock.clone()),
        clock,
        offer,
        handlers,
        None,
        Settings::default(),
    )
    .unwrap();
    (client, log)
}

const THERE: &str = "eml:///dataspace('there')";

/// **Server to server, and the model arrives whole and in order.** The CRS
/// and the proxy carry no array and are referenced by the point set, so they
/// go first, in an island of their own; the point set follows with its
/// array, addressed to its proxy in the destination, the values intact.
#[test]
fn a_store_is_pushed_in_islands_targets_before_sources() {
    let source = store();
    let (mut client, log) = receiving(None);
    let mut said = Vec::new();
    let report = block(push(
        &mut client,
        &source,
        "there",
        &Options::default(),
        |event| said.push(event),
    ))
    .unwrap();

    assert!(report.stopped.is_none(), "{:?}", report.stopped);
    assert_eq!(report.left_out, Vec::<String>::new());
    assert_eq!(report.islands.len(), 2, "{:?}", report.islands);
    assert_eq!(report.sent, 2);
    assert_eq!(report.islands[0].arrays, 0);
    assert_eq!(report.islands[1].arrays, 1);
    assert_eq!(
        said[0],
        Progress::Planned {
            islands: 2,
            objects: 3
        }
    );

    let log = log.lock().unwrap();
    // **An object's arrays go with it, and before its commit.** A store
    // refuses to commit an object whose arrays it does not hold; the second
    // island writes its array between its start and its commit, never after.
    assert_eq!(log.transactions, ["start", "commit", "start", "array", "commit"]);
    assert!(report.dangling.is_empty(), "{:?}", report.dangling);
    let received: Vec<&str> = log.objects.iter().map(|(uri, _)| uri.as_str()).collect();
    assert_eq!(received.len(), 3, "{received:?}");
    let at = |uuid: &str| {
        received
            .iter()
            .position(|uri| uri.contains(uuid))
            .unwrap_or_else(|| panic!("{uuid} was not received: {received:?}"))
    };
    assert!(
        at(CRS) < at(POINTS) && at(PROXY) < at(POINTS),
        "targets before sources: {received:?}"
    );
    // Re-addressed to the destination, and still the same object.
    assert!(received.iter().all(|uri| uri.starts_with(THERE)), "{received:?}");
    let (_, points_xml) = log.objects.iter().find(|(uri, _)| uri.contains(POINTS)).unwrap();
    let xml = String::from_utf8_lossy(points_xml);
    assert!(
        xml.contains("PointSetRepresentation") && xml.contains(POINTS),
        "{}",
        &xml[..xml.len().min(400)]
    );

    let key = (
        format!("{THERE}/eml20.obj_EpcExternalPartReference({PROXY})"),
        "/points".to_string(),
    );
    let (dimensions, values) = log.arrays.get(&key).unwrap_or_else(|| {
        panic!(
            "the array was not written under its proxy: {:?}",
            log.arrays.keys()
        )
    });
    assert_eq!(dimensions, &[2, 3]);
    assert_eq!(values, &[0.0, 0.0, 0.0, 10.0, 20.0, 30.0]);
}

/// **A refused commit stops the push there and says so.** Nothing after it
/// leaves - the next island would reference what the store just refused -,
/// and the report names the island, the reason, and how many went through.
#[test]
fn a_refused_commit_stops_the_push_and_says_where() {
    let source = store();
    let (mut client, log) = receiving(Some("the dataspace is read-only today"));
    let report = block(push(&mut client, &source, "there", &Options::default(), |_| {})).unwrap();

    assert_eq!(report.sent, 0);
    let stopped = report.stopped.as_ref().expect("the push stopped");
    assert_eq!(stopped.island, 0);
    match &stopped.why {
        Stop::Refused(why) => assert!(why.contains("read-only today"), "{why}"),
        other => panic!("{other:?}"),
    }
    // One island tried, none after it: a second `start` would be the push
    // carrying on past a refusal.
    assert_eq!(log.lock().unwrap().transactions, ["start", "commit"]);
}

/// **A write the store refuses rolls the island back and stops there.** The
/// refusal is per key, with the store's own words; the transaction is rolled
/// back rather than committed around a hole; and nothing after it leaves.
#[test]
fn a_refused_write_rolls_the_island_back_and_stops() {
    let source = store();
    let (mut client, log) = receiving_with(Log {
        refuse_objects: Some("this dataspace is published"),
        ..Log::default()
    });
    let report = block(push(&mut client, &source, "there", &Options::default(), |_| {})).unwrap();

    assert_eq!(report.sent, 0);
    let stopped = report.stopped.as_ref().expect("the push stopped");
    assert_eq!(stopped.island, 0);
    match &stopped.why {
        Stop::Refused(why) => assert!(why.contains("is published"), "{why}"),
        other => panic!("{other:?}"),
    }
    let log = log.lock().unwrap();
    assert_eq!(log.transactions, ["start", "rollback"]);
    assert!(log.objects.is_empty());
}

/// **A refused commit points at what leaves the source.** The CRS is served
/// but not listed, so the point set references something the source does not
/// hold - the shape of a `PropertyKind` of the dictionary. A store that checks
/// references says « reference missing » and no more; the report says the
/// rest: which reference, and that the dictionary's kinds are the usual
/// suspect.
#[test]
fn a_refused_commit_names_what_leaves_the_source() {
    let source = store_hiding(CRS);
    let (mut client, _log) = receiving(Some("reference missing"));
    let report = block(push(&mut client, &source, "there", &Options::default(), |_| {})).unwrap();

    assert_eq!(report.dangling.len(), 1, "{:?}", report.dangling);
    assert!(report.dangling[0].1.contains(CRS), "{:?}", report.dangling);
    let stopped = report.stopped.as_ref().expect("the push stopped");
    match &stopped.why {
        Stop::Refused(why) => {
            assert!(why.contains("reference missing"), "{why}");
            assert!(why.contains("leave the source") && why.contains(CRS), "{why}");
            assert!(why.contains("PropertyKind"), "{why}");
        }
        other => panic!("{other:?}"),
    }
}

/// **The same object twice, not two objects.** Asked for JSON - what the
/// Reservoir DDMS REST API takes and the only thing it takes - every object
/// leaves as JSON Energistics off the same reflection the XML comes off, with
/// its `$type`, and says so in its `format`.
#[test]
fn a_push_can_send_json_energistics() {
    let source = store();
    let (mut client, log) = receiving(None);
    let report = block(push(
        &mut client,
        &source,
        "there",
        &Options {
            format: WriteFormat::Json,
            ..Default::default()
        },
        |_| {},
    ))
    .unwrap();
    assert!(report.stopped.is_none(), "{:?}", report.stopped);
    assert_eq!(report.left_out, Vec::<String>::new());

    let log = log.lock().unwrap();
    assert!(log.formats.iter().all(|f| f == "json"), "{:?}", log.formats);
    for (uri, bytes) in &log.objects {
        let json: serde_json::Value =
            serde_json::from_slice(bytes).unwrap_or_else(|why| panic!("{uri}: not JSON: {why}"));
        let kind = json["$type"]
            .as_str()
            .unwrap_or_else(|| panic!("{uri}: no $type in {json}"));
        assert!(uri.contains(kind), "{uri} carries {kind}");
    }
}

/// **The dictionary's kinds, pushed when asked, with their ancestry, ahead of
/// what references them.** The source holds a kind parented on the
/// dictionary's root, which it does not hold. Left to the store, the reference
/// leaves the source and is named; pushed, the root goes out as an eml 2.3
/// object the registry read, before the kind that names it, and nothing
/// leaves the source any more.
#[test]
fn the_dictionarys_kinds_are_pushed_when_asked_and_ahead_of_what_names_them() {
    let source = store_with_kind();

    // Left: the reference is named, the root is not sent.
    let (mut client, log) = receiving(None);
    let report = block(push(&mut client, &source, "there", &Options::default(), |_| {})).unwrap();
    assert!(report.stopped.is_none(), "{:?}", report.stopped);
    assert_eq!(report.dangling.len(), 1, "{:?}", report.dangling);
    assert!(
        report.dangling[0].1.contains(DICTIONARY_ROOT),
        "{:?}",
        report.dangling
    );
    assert!(
        log.lock()
            .unwrap()
            .objects
            .iter()
            .all(|(uri, _)| !uri.contains(DICTIONARY_ROOT))
    );

    // Pushed: the root arrives, first, and as the dictionary writes it.
    let (mut client, log) = receiving(None);
    let options = Options {
        well_known: WellKnown::Push(&registry()),
        ..Default::default()
    };
    let report = block(push(&mut client, &source, "there", &options, |_| {})).unwrap();
    assert!(report.stopped.is_none(), "{:?}", report.stopped);
    assert_eq!(report.left_out, Vec::<String>::new());
    assert!(
        report.dangling.is_empty(),
        "nothing leaves the source once the root is in: {:?}",
        report.dangling
    );

    let log = log.lock().unwrap();
    let received: Vec<&str> = log.objects.iter().map(|(uri, _)| uri.as_str()).collect();
    let at = |uuid: &str| {
        received
            .iter()
            .position(|uri| uri.contains(uuid))
            .unwrap_or_else(|| panic!("{uuid} not received: {received:?}"))
    };
    assert!(
        at(DICTIONARY_ROOT) < at(KIND),
        "the parent before the kind that names it: {received:?}"
    );
    let (uri, bytes) = log
        .objects
        .iter()
        .find(|(uri, _)| uri.contains(DICTIONARY_ROOT))
        .unwrap();
    assert!(uri.starts_with(THERE), "re-addressed: {uri}");
    let xml = String::from_utf8_lossy(bytes);
    assert!(
        xml.contains("<Title>property</Title>") || xml.contains(":Title>property<"),
        "the dictionary's own title: {xml}"
    );
    assert!(
        xml.contains("Energistics"),
        "the dictionary's own originator: {xml}"
    );
    assert!(xml.contains("IsAbstract"), "{xml}");
}

/// A reference that looks like a kind and is in no dictionary is **not**
/// invented: it stays a reference that leaves the source, and is named as
/// such in `left_out`.
#[test]
fn a_kind_the_dictionary_does_not_know_is_not_invented() {
    let dangling = vec![(
        "eml:///dataspace('demo')/resqml22.ContinuousProperty(p)".to_string(),
        "eml:///dataspace('demo')/eml23.PropertyKind(00000000-0000-4000-8000-00000000beef)".to_string(),
    )];
    let found = energyml_etp_store::push::well_known_kinds(&dangling, &registry());
    assert!(found.kinds.is_empty());
    assert_eq!(found.left_out.len(), 1, "{:?}", found.left_out);
    assert!(
        found.left_out[0].contains("not a kind of the official dictionary"),
        "{:?}",
        found.left_out
    );
}

/// A kind deep in the dictionary comes with **every** ancestor up to the
/// root, each once, each with the edge to its parent - so that the islands
/// order them parent first - and the root, its own parent, makes no edge.
#[test]
fn a_kind_comes_with_its_whole_ancestry_each_once() {
    use energyml_domain::property_kind::with_default;
    // Any kind at least two levels under the root.
    let (leaf, depth) = with_default(|d| {
        d.kinds()
            .iter()
            .map(|k| (k.uuid.clone(), d.ancestry(&k.uuid).len()))
            .find(|(_, depth)| *depth >= 3)
            .expect("a dictionary with three levels")
    });
    let dangling = vec![(
        "eml:///eml23.PropertyKind(source)".to_string(),
        format!("eml:///eml23.PropertyKind({leaf})"),
    )];
    let found = energyml_etp_store::push::well_known_kinds(&dangling, &registry());
    assert_eq!(found.kinds.len(), depth, "{:?}", found.left_out);
    let uuids: std::collections::BTreeSet<&str> =
        found.kinds.iter().map(|(s, _, _)| s.uuid.as_str()).collect();
    assert_eq!(uuids.len(), depth, "each once");
    // Every kind but the root has one edge, to its parent; the root has none.
    let with_edge = found.kinds.iter().filter(|(_, _, e)| e.len() == 1).count();
    assert_eq!(with_edge, depth - 1);
    assert!(
        found
            .kinds
            .iter()
            .any(|(s, _, e)| s.uuid == DICTIONARY_ROOT && e.is_empty()),
        "the root names itself, and that is no edge"
    );
}

/// **A push limited to some objects sends those and nothing else** - what a
/// front end sends after an edit. What they reference and that was not asked for
/// stays where it is, and is named as leaving the source.
#[test]
fn a_push_can_be_limited_to_some_objects() {
    let source = store();
    let listed = block(energyml_storage::storage::EnergymlStorage::resources(&source)).unwrap();
    let points = listed
        .iter()
        .find(|one| one.qualified_type.contains("PointSetRepresentation"))
        .expect("the model has a point set")
        .uri
        .clone();
    let (mut client, log) = receiving(None);
    let only = [points.clone()];
    let options = Options {
        only: Some(&only),
        ..Default::default()
    };
    let report = block(push(&mut client, &source, "there", &options, |_| {})).unwrap();
    assert!(report.stopped.is_none(), "{:?}", report.stopped);
    let sent: Vec<String> = log
        .lock()
        .unwrap()
        .objects
        .iter()
        .map(|(uri, _)| uri.clone())
        .collect();
    assert_eq!(sent.len(), 1, "one object asked for, one sent: {sent:?}");
    assert!(sent[0].contains("PointSetRepresentation"), "{sent:?}");
    // Its array went with it, addressed through a proxy that was **not**
    // sent: the listing still resolves it.
    assert_eq!(log.lock().unwrap().arrays.len(), 1, "the point set's array");
    // Its CRS and its proxy were not asked for: they leave the source, and
    // that is said.
    assert!(
        report.dangling.iter().any(|(from, _)| from == &points),
        "the references to the CRS and the proxy are named: {:?}",
        report.dangling
    );
}

/// **A store that did not agree Transaction (18) still receives the push** -
/// as plain puts, island by island, and the report says the islands went
/// without a transaction. some RDDMS servers offer no protocol 18, and a push
/// that refused it whole would leave every such store unreachable.
#[test]
fn a_store_without_transactions_is_pushed_as_plain_puts_and_the_report_says_so() {
    let source = store();
    let (mut client, log) = receiving_speaking(Log::default(), &[4, 9]);
    let report = block(push(&mut client, &source, "there", &Options::default(), |_| {})).unwrap();
    assert!(report.stopped.is_none(), "{:?}", report.stopped);
    assert!(
        report.without_transactions,
        "the report says the islands went without a transaction"
    );
    assert_eq!(report.sent, report.islands.len());
    let log = log.lock().unwrap();
    assert_eq!(log.objects.len(), 3, "every object went");
    assert_eq!(log.arrays.len(), 1, "and the array with its object");
    // The log notes the arrays too; what must be absent is any transaction.
    let transactional: Vec<&&str> = log.transactions.iter().filter(|what| **what != "array").collect();
    assert!(
        transactional.is_empty(),
        "no StartTransaction was sent to a store that does not speak it: {:?}",
        log.transactions
    );
}

/// And with 18 agreed, the report does not claim otherwise.
#[test]
fn a_store_with_transactions_is_not_said_to_be_without() {
    let source = store();
    let (mut client, _) = receiving(None);
    let report = block(push(&mut client, &source, "there", &Options::default(), |_| {})).unwrap();
    assert!(!report.without_transactions);
}

/// **A store that announces Transaction (18) and then says it has none is
/// taken at its word**: the islands go on as plain puts, and the report says
/// so. some REST APIs do exactly this - the route exists and names the
/// transaction `null`.
#[test]
fn a_store_that_says_it_has_no_transactions_is_pushed_without_them() {
    let source = store();
    let (mut client, log) = receiving_with(Log {
        no_transactions: true,
        ..Default::default()
    });
    let report = block(push(&mut client, &source, "there", &Options::default(), |_| {})).unwrap();
    assert!(report.stopped.is_none(), "{:?}", report.stopped);
    assert!(report.without_transactions);
    assert_eq!(report.sent, report.islands.len());
    let log = log.lock().unwrap();
    assert_eq!(log.objects.len(), 3, "every object went");
    assert!(
        !log.transactions.contains(&"commit"),
        "nothing was committed: {:?}",
        log.transactions
    );
}

/// **An object the store changed since it was taken is shown, not
/// overwritten**. Told what it knew of each object's `storeLastWrite`,
/// the push asks the store what it wrote since, in one listing, and stops
/// before writing anything when an object of the push is among them - or
/// was deleted there. Told to overwrite, it goes, and still names them. An
/// object the store wrote *before* it was taken is no conflict.
#[test]
fn an_object_changed_on_the_store_since_it_was_taken_is_not_overwritten() {
    use energyml_etp_store::push::Conflict;
    let source = store();
    let points_there = format!("{THERE}/resqml20.obj_PointSetRepresentation({POINTS})");
    let expected: BTreeMap<String, i64> = [(points_there.clone(), T0)].into_iter().collect();
    let checking = |overwrite: bool| Options {
        expected: Some(&expected),
        overwrite,
        ..Default::default()
    };

    // Changed there after it was taken: nothing is written.
    let (mut client, log) = receiving_with(Log {
        changed: vec![(points_there.clone(), T0 + 5)],
        ..Default::default()
    });
    let report = block(push(&mut client, &source, "there", &checking(false), |_| {})).unwrap();
    assert_eq!(
        report.conflicts,
        [Conflict {
            uri: points_there.clone(),
            taken: T0,
            store: Some(T0 + 5)
        }]
    );
    let stopped = report.stopped.as_ref().expect("stopped before writing");
    assert_eq!(stopped.island, 0);
    assert!(
        matches!(&stopped.why, Stop::Refused(why) if why.contains("changed on the store") && why.contains(&points_there)),
        "{:?}",
        stopped.why
    );
    assert_eq!(report.sent, 0);
    assert!(
        log.lock().unwrap().objects.is_empty(),
        "something was written despite the conflict: {:?}",
        log.lock().unwrap().objects
    );

    // Told to overwrite: everything goes, and the conflict is still named.
    let (mut client, log) = receiving_with(Log {
        changed: vec![(points_there.clone(), T0 + 5)],
        ..Default::default()
    });
    let report = block(push(&mut client, &source, "there", &checking(true), |_| {})).unwrap();
    assert!(report.stopped.is_none(), "{:?}", report.stopped);
    assert_eq!(report.conflicts.len(), 1);
    assert_eq!(log.lock().unwrap().objects.len(), 3);

    // Written there before it was taken, or exactly when - the write the
    // take saw: no conflict, everything goes. The second is the one the
    // listing still returns (its filter is `since - 1`), so it is the
    // comparison that decides, not the store.
    for when in [T0 - 5, T0] {
        let (mut client, _) = receiving_with(Log {
            changed: vec![(points_there.clone(), when)],
            ..Default::default()
        });
        let report = block(push(&mut client, &source, "there", &checking(false), |_| {})).unwrap();
        assert!(
            report.conflicts.is_empty(),
            "written at {when}: {:?}",
            report.conflicts
        );
        assert!(report.stopped.is_none(), "{:?}", report.stopped);
    }

    // Deleted there since it was taken: a conflict too, said as such.
    let (mut client, log) = receiving_with(Log {
        gone: vec![(points_there.clone(), T0 + 1)],
        ..Default::default()
    });
    let report = block(push(&mut client, &source, "there", &checking(false), |_| {})).unwrap();
    assert_eq!(
        report.conflicts,
        [Conflict {
            uri: points_there.clone(),
            taken: T0,
            store: None
        }]
    );
    assert!(report.stopped.is_some());
    assert!(log.lock().unwrap().objects.is_empty());
}
