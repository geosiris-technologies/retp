//! Tier 5 - a store pushed into a dataspace, in islands of transactions.
//!
//! **Server to server**, which is the case the open question names: the
//! source is the bridge's fake store read through [`EnergymlStorage`], the
//! destination a second fake that takes writes and remembers them. Nothing
//! here reaches a real server: no test writes to a server that is not a fake.

mod common;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use energyml_etp_store::push::{
    Budget, Cause, NamedReference, NoPlan, Options, Progress, SkipReason, Split, Stop, StopKind, WellKnown,
    WriteFormat, chosen, dry_run, islands, push, push_resumable, reconnecting,
};
use retp_avro::{Avro, EtpMessage, Reader, Role};
use retp_client::memory::{ManualClock, MemoryTransport};
use retp_client::native::Resuming;
use retp_client::resume::{Came, Connector, Retry, RunEvent, Setback};
use retp_client::{Client, Settings, writing_client};
use retp_proto::connection::{Connection, Declined, Handled, Handler, Responder, Resumable};
use retp_proto::negotiate::{Offer, ProtocolOffer};
use retp_proto::wire::{Body, Delivery, EtpError};
use retp_types::datatypes::MessageHeader;
use retp_types::datatypes::data_array_types::DataArrayMetadata;
use retp_types::datatypes::object::PutResponse;
use retp_types::datatypes::{AnyArrayItem, SupportedDataObject, Uuid};
use retp_types::dispatch::Message;
use retp_types::protocol::data_array::{
    GetDataArrayMetadataResponse, PutDataArraysResponse, PutDataSubarraysResponse,
    PutUninitializedDataArraysResponse,
};
use retp_types::protocol::store::{PutDataObjects, PutDataObjectsResponse};
use retp_types::protocol::transaction::{
    CommitTransaction, CommitTransactionResponse, RollbackTransactionResponse, StartTransactionResponse,
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
    /// With `refuse_commit`: refuse only this many commits, then take them.
    refuse_commit_times: Option<usize>,
    /// Behave as open-etp-server (M26) does: a transaction stays open after a
    /// refused commit until it is rolled back, and a `StartTransaction` while
    /// one is open is answered with `EINVALID_STATE`.
    sticky_transactions: bool,
    /// Whether a transaction is open, for `sticky_transactions`.
    open_transaction: bool,
    /// Answer every `StartTransaction` with an `EINVALID_STATE` exception
    /// saying this.
    refuse_start: Option<&'static str>,
    /// Refuse every object written, with this reason - a dataspace opened
    /// read-only, which most are.
    refuse_objects: Option<&'static str>,
    /// Refuse, as `EINVALID_OBJECT`, the objects whose URI holds this uuid,
    /// with this reason, and take the others.
    refuse_one: Option<(&'static str, &'static str)>,
    /// Answer a `PutDataObjects` with one `ProtocolException` for the whole
    /// request, `EREQUEST_DENIED` with this reason - no per-object map.
    refuse_request: Option<&'static str>,
    /// Refuse every array written, `EREQUEST_DENIED` with this reason.
    refuse_arrays: Option<&'static str>,
    /// Announce Transaction (18) and then answer `ENOTSUPPORTED` to every
    /// `StartTransaction` - what some REST APIs do, naming the transaction
    /// `null`.
    no_transactions: bool,
    /// What this store holds or wrote on its own, and when - listed by
    /// Discovery, after a `storeLastWriteFilter` when there is one.
    changed: Vec<(String, i64)>,
    /// What it deleted, and when.
    gone: Vec<(String, i64)>,
    /// Answer a `PutDataObjects` of more objects than this with one
    /// `ProtocolException` for the whole request, `EREQUEST_DENIED` - a store
    /// that takes small requests and refuses big ones.
    refuse_more_than: Option<usize>,
    /// End every `GetResources` with this `EREQUEST_DENIED` - a listing the
    /// store cuts short.
    refuse_listing: Option<&'static str>,
    /// Never answer a `PutDataObjects`: the client's request times out,
    /// and the session stands.
    silent_puts: bool,
    /// The `(protocol, messageType)` of every message a client put on a
    /// connection to this store, over every session, in order - the one a
    /// connection falls on included, and the session's own (`RequestSession`
    /// is `(0, 1)`, OSDU's `ResumeSession` `(2400, 1)`).
    sent: Vec<(i32, i32)>,
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
                let stuck = log.sticky_transactions && log.open_transaction;
                if stuck || log.refuse_start.is_some() {
                    let words = log
                        .refuse_start
                        .unwrap_or("a transaction is already open in this session");
                    let out = responder
                        .wire()
                        .refuse(delivery, &EtpError::EINVALID_STATE.with_detail(words));
                    responder.send(out);
                    log.transactions.push("start-refused");
                    return Ok(Handled::Answered);
                }
                log.open_transaction = true;
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
                let refused = match (log.refuse_commit, log.refuse_commit_times) {
                    (Some(why), None) => Some(why),
                    (Some(why), Some(left)) if left > 0 => {
                        log.refuse_commit_times = Some(left - 1);
                        Some(why)
                    }
                    _ => None,
                };
                // A refused commit leaves the transaction open on a sticky
                // store; any other commit ends it.
                log.open_transaction = log.sticky_transactions && refused.is_some();
                let response = CommitTransactionResponse {
                    transaction_uuid: request.transaction_uuid,
                    successful: refused.is_none(),
                    failure_reason: refused.unwrap_or("").to_string(),
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            Message::RollbackTransaction(request) => {
                log.transactions.push("rollback");
                log.open_transaction = false;
                let response = RollbackTransactionResponse {
                    transaction_uuid: request.transaction_uuid,
                    successful: true,
                    failure_reason: String::new(),
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            Message::PutDataObjects(_) if log.silent_puts => {}
            Message::PutDataObjects(request) => {
                let too_many = log
                    .refuse_more_than
                    .is_some_and(|most| request.data_objects.len() > most);
                if too_many {
                    let out = responder.wire().refuse(
                        delivery,
                        &EtpError::EREQUEST_DENIED.with_detail("too many objects at once".to_string()),
                    );
                    responder.send(out);
                    return Ok(Handled::Answered);
                }
                if let Some(why) = log.refuse_request {
                    let out = responder
                        .wire()
                        .refuse(delivery, &EtpError::EREQUEST_DENIED.with_detail(why.to_string()));
                    responder.send(out);
                    return Ok(Handled::Answered);
                }
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
                    if let Some((uuid, why)) = log.refuse_one
                        && object.resource.uri.contains(uuid)
                    {
                        reply
                            .failed(
                                key.clone(),
                                &EtpError::EINVALID_OBJECT.with_detail(why.to_string()),
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
                    if let Some(why) = log.refuse_arrays {
                        reply
                            .failed(
                                key.clone(),
                                &EtpError::EREQUEST_DENIED.with_detail(why.to_string()),
                            )
                            .unwrap();
                        continue;
                    }
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
            Message::GetDataArrayMetadata(request) => {
                // What it holds of each array, by `(uri, path)`; an array it
                // does not hold is `ENOT_FOUND`.
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_arrays.keys().cloned());
                let mut array_metadata = BTreeMap::new();
                for (key, id) in &request.data_arrays {
                    match log.arrays.get(&(id.uri.clone(), id.path_in_resource.clone())) {
                        Some((dimensions, _)) => {
                            reply.answered(key.clone()).unwrap();
                            array_metadata.insert(
                                key.clone(),
                                DataArrayMetadata {
                                    dimensions: dimensions.clone(),
                                    ..Default::default()
                                },
                            );
                        }
                        None => reply
                            .failed(
                                key.clone(),
                                &EtpError::ENOT_FOUND.with_detail("no such array".to_string()),
                            )
                            .unwrap(),
                    }
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &GetDataArrayMetadataResponse { array_metadata })
                    .unwrap();
                responder.send_all(out);
            }
            Message::GetResources(_) if log.refuse_listing.is_some() => {
                let why = log.refuse_listing.unwrap_or_default();
                let out = responder
                    .wire()
                    .refuse(delivery, &EtpError::EREQUEST_DENIED.with_detail(why.to_string()));
                responder.send(out);
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
    (session(&log, protocols, Fall::Never), log)
}

/// Where a session's connection falls.
#[derive(Debug, Clone, Copy)]
enum Fall {
    /// It does not.
    Never,
    /// On the `nth` message (counted from 1) of this protocol and type it
    /// sends, which is not delivered.
    OnThe {
        nth: usize,
        protocol: i32,
        message_type: i32,
    },
    /// On every `PutDataObjects` of more objects than this - a server that
    /// hangs up on a request too heavy for it.
    OnMoreObjectsThan(usize),
}

/// A writing session with the store whose log is `log`.
fn session(log: &Arc<Mutex<Log>>, protocols: &[i32], fall: Fall) -> Client<MemoryTransport, ManualClock> {
    connected(log, protocols, fall, None)
}

/// How the server of a new connection answers a `ResumeSession` - OSDU's
/// CoreOSDU extension, protocol 2400, in the wire format `open-etp-server`
/// publishes (`etp-1.2.avpr`).
#[derive(Debug, Clone, Copy)]
enum Answers {
    /// It gives the lost session back, negotiated from what it kept of it.
    GivesBack,
    /// It no longer has it: `ENOT_FOUND`, *Session not found or expired*, as
    /// OSDU's server answers.
    NotFound,
    /// It does not speak 2400: `EUNSUPPORTED_PROTOCOL`, as any protocol
    /// nobody announced.
    Unspoken,
}

/// A writing session with the store whose log is `log`, on a new connection;
/// with `resuming`, one that first asks back the session it names, from a
/// server that answers as said.
fn connected(
    log: &Arc<Mutex<Log>>,
    protocols: &[i32],
    fall: Fall,
    resuming: Option<(Answers, Resumable)>,
) -> Client<MemoryTransport, ManualClock> {
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
                log: Arc::clone(log),
            }) as Box<dyn Handler>
        })
        .collect();
    let server = Connection::server(offer, handlers, None, Uuid([0xBC; 16]), T0).unwrap();
    let server = match &resuming {
        Some((Answers::GivesBack | Answers::NotFound, _)) => server.accepting_resume(),
        Some((Answers::Unspoken, _)) | None => server,
    };
    let clock = ManualClock::new(T0);
    let transport = MemoryTransport::new(server, clock.clone());
    // What a real server keeps of the session it lost: the RequestSession it
    // was opened with.
    let transport = match &resuming {
        Some((Answers::GivesBack, lost)) => transport.resumes(Some(lost.request.clone())),
        _ => transport,
    };
    // Every message is written down, then the fall decides.
    let seen = AtomicUsize::new(0);
    let log_of_sent = Arc::clone(log);
    let transport = transport.falls_when(move |message| {
        let mut input = Reader::new(message);
        let header = MessageHeader::read(&mut input).unwrap();
        let kind = (header.protocol, header.message_type);
        log_of_sent.lock().unwrap().sent.push(kind);
        match fall {
            Fall::Never => false,
            Fall::OnThe {
                nth,
                protocol,
                message_type,
            } => kind == (protocol, message_type) && seen.fetch_add(1, Ordering::SeqCst) + 1 == nth,
            Fall::OnMoreObjectsThan(most) => {
                kind == (PutDataObjects::PROTOCOL, PutDataObjects::MESSAGE_TYPE)
                    && PutDataObjects::read(&mut input).unwrap().data_objects.len() > most
            }
        }
    });
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
    match resuming {
        None => Client::open(transport, clock, offer, handlers, None, Settings::default()).unwrap(),
        Some((_, lost)) => {
            Client::resume(transport, clock, offer, handlers, None, Settings::default(), lost).unwrap()
        }
    }
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
    // carrying on past a refusal. It rolls its own transaction back first.
    assert_eq!(log.lock().unwrap().transactions, ["start", "commit", "rollback"]);
}

/// **After a refused commit the push rolls its own transaction back**, because
/// open-etp-server keeps it open (its `TransProto.cpp` does not end the
/// transaction on a failed check, where ETP 1.2 18.1.1.1 step 8.b.ii does),
/// and a split's next island opens another one in the same session: that
/// `StartTransaction` is refused while the first is open. The store here
/// behaves so, and refuses the first commit only.
#[test]
fn a_refused_commit_is_rolled_back_before_the_split_opens_the_next_transaction() {
    let source = store();
    let (mut client, log) = receiving_with(Log {
        refuse_commit: Some("the dataspace is read-only today"),
        refuse_commit_times: Some(1),
        sticky_transactions: true,
        ..Log::default()
    });
    let options = Options {
        split: Split::Halve,
        ..Default::default()
    };
    let report = block(push(&mut client, &source, "there", &options, |_| {})).unwrap();
    assert!(report.stopped.is_none(), "{:?}", report.stopped);
    assert_eq!(report.splits.len(), 1, "{:?}", report.splits);
    assert_eq!(report.sent, report.islands.len());
    let log = log.lock().unwrap();
    // The first island: start, commit (refused), rollback - and only then the
    // pieces' own transactions, none refused for one still open.
    assert_eq!(
        &log.transactions[..3],
        ["start", "commit", "rollback"],
        "{:?}",
        log.transactions
    );
    assert!(
        !log.transactions.contains(&"start-refused"),
        "a start was refused for a transaction left open: {:?}",
        log.transactions
    );
}

/// **A transaction the store answers with an exception says its code and its
/// words.** open-etp-server answers `EINVALID_STATE` to a `StartTransaction`
/// while another transaction is open; the report carries that code and
/// message, not "the store answered nothing about the transaction".
#[test]
fn a_transaction_refused_with_an_exception_reaches_the_report_with_its_code() {
    let source = store();
    let (mut client, log) = receiving_with(Log {
        refuse_start: Some("another transaction is open in this session"),
        ..Log::default()
    });
    let report = block(push(&mut client, &source, "there", &Options::default(), |_| {})).unwrap();
    assert_eq!(report.sent, 0);
    let stopped = report.stopped.as_ref().expect("the push stopped");
    match &stopped.why {
        Stop::Refused(why) => {
            assert!(why.contains("the transaction would not open"), "{why}");
            assert!(
                why.contains("another transaction is open in this session"),
                "{why}"
            );
            assert!(!why.contains("answered nothing"), "{why}");
        }
        other => panic!("{other:?}"),
    }
    let said = stopped.said.as_ref().expect("the store's words");
    assert_eq!(said.code, Some(8), "EINVALID_STATE, ETP v1.2 section 24.3");
    assert!(
        said.message.contains("another transaction is open"),
        "{}",
        said.message
    );
    let diagnosis = report.diagnosis().expect("a stop");
    assert_eq!(diagnosis.kind, StopKind::Refused);
    assert_eq!(diagnosis.code, Some(8));
    assert!(diagnosis.message.contains("another transaction is open"));
    assert_eq!(diagnosis.cause, Some(Cause::Unknown));
    // Nothing was opened, so nothing to roll back.
    assert_eq!(log.lock().unwrap().transactions, ["start-refused"]);
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

// The codes below are written as ETP v1.2 section 24.3 (p.482-486) numbers
// them - EREQUEST_DENIED is 6, EINVALID_OBJECT 14 -, not read back from the
// enumeration the test server answers with: a renumbered constant would
// otherwise agree with itself.

/// **Every object the store refuses is named, with its code** - not the
/// first one quoted in a sentence. The CRS is refused, the proxy in the same
/// island is taken: the refusals name the CRS alone, with the store's code
/// and words, so that a caller can isolate it without reading text.
#[test]
fn each_refused_object_is_named_with_the_stores_code() {
    let source = store();
    let (mut client, _log) = receiving_with(Log {
        refuse_one: Some((CRS, "the CRS does not validate")),
        ..Log::default()
    });
    let report = block(push(&mut client, &source, "there", &Options::default(), |_| {})).unwrap();

    assert_eq!(report.sent, 0);
    assert_eq!(report.refusals.len(), 1, "{:?}", report.refusals);
    let refused = &report.refusals[0];
    assert!(
        refused.uri.starts_with(THERE) && refused.uri.contains(CRS),
        "addressed in the destination: {}",
        refused.uri
    );
    assert_eq!(refused.array, None);
    assert_eq!(refused.code, Some(14), "EINVALID_OBJECT, section 24.3");
    assert!(
        refused.message.contains("does not validate"),
        "{}",
        refused.message
    );
    match &report.stopped.as_ref().expect("the push stopped").why {
        Stop::Refused(why) => assert!(why.contains(CRS), "{why}"),
        other => panic!("{other:?}"),
    }
}

/// **A `ProtocolException` that answers the whole request** - one error, no
/// map - is every object's answer: each object of the island is named with
/// that one code, in the order the island sends them, and the sentence says
/// how many more there are than the one it quotes.
#[test]
fn one_exception_for_the_whole_request_refuses_each_object_of_the_island() {
    let source = store();
    let (mut client, _log) = receiving_with(Log {
        refuse_request: Some("quota exceeded"),
        ..Log::default()
    });
    let report = block(push(&mut client, &source, "there", &Options::default(), |_| {})).unwrap();

    let island = &report.islands[0];
    assert_eq!(report.refusals.len(), island.uris.len(), "{:?}", report.refusals);
    for (refused, source_uri) in report.refusals.iter().zip(&island.uris) {
        let uuid = energyml_core::identifiers::EtpUri::parse(source_uri)
            .unwrap()
            .uuid
            .unwrap();
        assert!(refused.uri.contains(&uuid), "{} for {source_uri}", refused.uri);
        assert_eq!(refused.code, Some(6), "EREQUEST_DENIED, section 24.3");
        assert!(refused.message.contains("quota exceeded"), "{}", refused.message);
    }
    match &report.stopped.as_ref().expect("the push stopped").why {
        Stop::Refused(why) => assert!(
            why.contains(&format!("and {} more refused", island.uris.len() - 1)),
            "{why}"
        ),
        other => panic!("{other:?}"),
    }
}

/// **An array the store refuses is named by its object and its path.** The
/// point set's array hangs from its RESQML 2.0.1 proxy: the refusal names
/// the proxy in the destination, the array's path, and the store's code.
#[test]
fn a_refused_array_is_named_by_its_object_and_path() {
    let source = store();
    let (mut client, _log) = receiving_with(Log {
        refuse_arrays: Some("no room for arrays"),
        ..Log::default()
    });
    let report = block(push(&mut client, &source, "there", &Options::default(), |_| {})).unwrap();

    assert_eq!(report.sent, 1, "the island without arrays went");
    assert_eq!(report.refusals.len(), 1, "{:?}", report.refusals);
    let refused = &report.refusals[0];
    assert!(
        refused.uri.starts_with(THERE) && refused.uri.contains(PROXY),
        "{}",
        refused.uri
    );
    assert!(refused.array.is_some(), "{refused:?}");
    assert_eq!(refused.code, Some(6), "EREQUEST_DENIED, section 24.3");
    assert!(refused.message.contains("no room"), "{}", refused.message);
}

/// **A push that went through refuses nothing**, and says so with an empty
/// list rather than a list nobody filled.
#[test]
fn a_push_that_went_through_names_no_refusal() {
    let source = store();
    let (mut client, _log) = receiving(None);
    let report = block(push(&mut client, &source, "there", &Options::default(), |_| {})).unwrap();
    assert!(report.stopped.is_none(), "{:?}", report.stopped);
    assert!(report.refusals.is_empty(), "{:?}", report.refusals);
}

/// **What a caller names becomes the source's own URIs**: a URI the source
/// lists, the same object's URI in another dataspace, a bare uuid in any
/// case. A name the source does not hold is refused by name.
#[test]
fn what_a_caller_names_is_matched_against_the_sources_uris() {
    let source = store();
    let listed = block(energyml_storage::storage::EnergymlStorage::resources(&source)).unwrap();
    let uri_of = |uuid: &str| {
        listed
            .iter()
            .find(|one| one.uuid.eq_ignore_ascii_case(uuid))
            .unwrap()
            .uri
            .clone()
    };
    let points = uri_of(POINTS);
    let crs = uri_of(CRS);
    let crs_kind = listed
        .iter()
        .find(|one| one.uri == crs)
        .unwrap()
        .qualified_type
        .clone();
    let names = [
        points.clone(),
        CRS.to_ascii_uppercase(),
        format!("eml:///dataspace('elsewhere')/{crs_kind}({CRS})"),
    ];
    let found = block(chosen(&source, &names)).unwrap();
    assert_eq!(found, [points, crs], "each object once, in the order named");

    let refused = block(chosen(
        &source,
        &["ee000000-0000-4000-8000-000000000009".to_string()],
    ))
    .unwrap_err();
    assert!(refused.contains("does not hold"), "{refused}");
    // The right uuid under another type is not the object.
    let wrong = format!("eml:///dataspace('elsewhere')/eml20.obj_EpcExternalPartReference({CRS})");
    assert!(block(chosen(&source, &[wrong])).is_err());
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

// ---------------------------------------------------------------------------
// Starting again: what is there, the journal, the splits, the reconnection
// ---------------------------------------------------------------------------

fn crs_there() -> String {
    format!("{THERE}/resqml20.obj_LocalDepth3dCrs({CRS})")
}

fn proxy_there() -> String {
    format!("{THERE}/eml20.obj_EpcExternalPartReference({PROXY})")
}

fn points_there() -> String {
    format!("{THERE}/resqml20.obj_PointSetRepresentation({POINTS})")
}

/// A journal file of its own for one test, removed first: written by the
/// test, read by the test, never a file from outside.
fn journal_path(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("retp-push-{name}-{}.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&path);
    path
}

fn lines_of(path: &PathBuf) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// **What the destination holds already is left out, when asked** - and
/// only what it holds whole. Listed there with its array of the same
/// dimensions, an object is skipped and named; listed there with an array of
/// other dimensions, it is sent again, since skipping it would lose it.
/// Without the option, everything goes, as before.
#[test]
fn what_the_destination_holds_already_is_left_out_when_asked() {
    let source = store();
    let held = |dimensions: Vec<i64>| Log {
        changed: vec![(crs_there(), T0), (proxy_there(), T0), (points_there(), T0)],
        arrays: [((proxy_there(), "/points".to_string()), (dimensions, vec![0.0; 6]))]
            .into_iter()
            .collect(),
        ..Log::default()
    };
    let skipping = Options {
        skip_present: true,
        ..Default::default()
    };

    // All there, the array whole: nothing is written.
    let (mut client, log) = receiving_with(held(vec![2, 3]));
    let report = block(push(&mut client, &source, "there", &skipping, |_| {})).unwrap();
    assert!(report.stopped.is_none(), "{:?}", report.stopped);
    let skipped: Vec<(&str, SkipReason)> = report
        .skipped
        .iter()
        .map(|one| (one.uri.as_str(), one.why))
        .collect();
    assert_eq!(skipped.len(), 3, "{skipped:?}");
    assert!(skipped.iter().all(|(_, why)| *why == SkipReason::Present));
    assert!(report.islands.is_empty(), "{:?}", report.islands);
    assert!(log.lock().unwrap().objects.is_empty());

    // The array there under other dimensions: the point set goes again.
    let (mut client, log) = receiving_with(held(vec![3, 2]));
    let report = block(push(&mut client, &source, "there", &skipping, |_| {})).unwrap();
    assert!(report.stopped.is_none(), "{:?}", report.stopped);
    let skipped: Vec<&str> = report.skipped.iter().map(|one| one.uri.as_str()).collect();
    assert_eq!(skipped.len(), 2, "{skipped:?}");
    assert!(!skipped.contains(&points_there().as_str()), "{skipped:?}");
    let sent: Vec<String> = log
        .lock()
        .unwrap()
        .objects
        .iter()
        .map(|(uri, _)| uri.clone())
        .collect();
    assert_eq!(sent, [points_there()]);

    // Not asked: everything goes.
    let (mut client, log) = receiving_with(held(vec![2, 3]));
    let report = block(push(&mut client, &source, "there", &Options::default(), |_| {})).unwrap();
    assert!(report.skipped.is_empty());
    assert_eq!(log.lock().unwrap().objects.len(), 3);
}

/// **A listing the store cut short cannot say what is not there**: the push
/// asked to skip what is there stops by name, before anything is written.
#[test]
fn a_listing_cut_short_stops_a_push_that_skips_what_is_there() {
    let source = store();
    let (mut client, log) = receiving_with(Log {
        refuse_listing: Some("listing denied"),
        ..Log::default()
    });
    let options = Options {
        skip_present: true,
        ..Default::default()
    };
    let report = block(push(&mut client, &source, "there", &options, |_| {})).unwrap();
    let stopped = report.stopped.as_ref().expect("stopped before writing");
    assert!(
        matches!(&stopped.why, Stop::Refused(why) if why.contains("cut short") && why.contains("listing denied")),
        "{:?}",
        stopped.why
    );
    assert_eq!(report.sent, 0);
    assert!(log.lock().unwrap().objects.is_empty());
}

/// **A journal lets a stopped push go on where it stopped.** The first push
/// commits the island without arrays and is refused the next: the journal
/// records the first, by the objects' URIs in the destination. Started again
/// with the same journal, the push leaves those out, names them, sends the
/// rest, and records it too.
#[test]
fn a_journal_lets_a_stopped_push_go_on_where_it_stopped() {
    let source = store();
    let path = journal_path("go-on");
    let options = Options {
        journal: Some(&path),
        ..Default::default()
    };

    let (mut client, _) = receiving_with(Log {
        refuse_arrays: Some("no room for arrays"),
        ..Log::default()
    });
    let report = block(push(&mut client, &source, "there", &options, |_| {})).unwrap();
    assert_eq!(report.sent, 1, "{:?}", report.stopped);
    let lines = lines_of(&path);
    assert_eq!(lines.len(), 2, "the header and one island: {lines:?}");
    assert_eq!(lines[0]["dataspace"], THERE);
    let mut committed: Vec<String> = lines[1]["committed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|uri| uri.as_str().unwrap().to_string())
        .collect();
    committed.sort();
    let mut expected = vec![crs_there(), proxy_there()];
    expected.sort();
    assert_eq!(committed, expected);

    let (mut client, log) = receiving(None);
    let report = block(push(&mut client, &source, "there", &options, |_| {})).unwrap();
    assert!(report.stopped.is_none(), "{:?}", report.stopped);
    let mut skipped: Vec<String> = report
        .skipped
        .iter()
        .inspect(|one| assert_eq!(one.why, SkipReason::Journal))
        .map(|one| one.uri.clone())
        .collect();
    skipped.sort();
    assert_eq!(skipped, expected);
    let sent: Vec<String> = log
        .lock()
        .unwrap()
        .objects
        .iter()
        .map(|(uri, _)| uri.clone())
        .collect();
    assert_eq!(sent, [points_there()], "only what the journal does not record");
    assert_eq!(lines_of(&path).len(), 3);
    let _ = std::fs::remove_file(&path);
}

/// **A journal for another dataspace, or another package, is refused by
/// name**, before anything is written: what it records would be taken for
/// what is there. Another selection of the same package is another package.
#[test]
fn a_journal_for_another_dataspace_or_package_is_refused() {
    let source = store();
    let path = journal_path("other");
    let options = Options {
        journal: Some(&path),
        ..Default::default()
    };
    let (mut client, _) = receiving(None);
    let report = block(push(&mut client, &source, "there", &options, |_| {})).unwrap();
    assert!(report.stopped.is_none(), "{:?}", report.stopped);

    let (mut client, log) = receiving(None);
    let report = block(push(&mut client, &source, "elsewhere", &options, |_| {})).unwrap();
    assert!(
        matches!(&report.stopped.as_ref().unwrap().why, Stop::Journal(why) if why.contains("written for a push into")),
        "{:?}",
        report.stopped
    );
    assert!(log.lock().unwrap().objects.is_empty());

    let listed = block(energyml_storage::storage::EnergymlStorage::resources(&source)).unwrap();
    let only = [listed[0].uri.clone()];
    let fewer = Options {
        journal: Some(&path),
        only: Some(&only),
        ..Default::default()
    };
    let (mut client, log) = receiving(None);
    let report = block(push(&mut client, &source, "there", &fewer, |_| {})).unwrap();
    assert!(
        matches!(&report.stopped.as_ref().unwrap().why, Stop::Journal(why) if why.contains("another package")),
        "{:?}",
        report.stopped
    );
    assert!(log.lock().unwrap().objects.is_empty());
    let _ = std::fs::remove_file(&path);
}

/// **A line cut while it was written is dropped**, and its island sent
/// again: the journal never says more than what is known to be there.
#[test]
fn a_journal_line_cut_while_written_is_dropped() {
    let source = store();
    let path = journal_path("cut");
    let options = Options {
        journal: Some(&path),
        ..Default::default()
    };
    let (mut client, _) = receiving(None);
    block(push(&mut client, &source, "there", &options, |_| {})).unwrap();
    // The last island's line, cut before its end of line.
    let text = std::fs::read_to_string(&path).unwrap();
    let cut = &text[..text.len() - 10];
    std::fs::write(&path, cut).unwrap();

    let (mut client, log) = receiving(None);
    let report = block(push(&mut client, &source, "there", &options, |_| {})).unwrap();
    assert!(report.stopped.is_none(), "{:?}", report.stopped);
    let sent: Vec<String> = log
        .lock()
        .unwrap()
        .objects
        .iter()
        .map(|(uri, _)| uri.clone())
        .collect();
    assert_eq!(sent, [points_there()], "the island whose line was cut");
    assert_eq!(lines_of(&path).len(), 3, "and the journal reads whole again");
    let _ = std::fs::remove_file(&path);
}

/// **A refused island is split, when asked, and its pieces go.** The store
/// refuses any request of more than one object: as before, the push stops
/// at the first island; halved, or one object at a time, it goes through,
/// the split named with what the store said. A split limit of nothing is no
/// split.
#[test]
fn a_refused_island_is_split_when_asked_and_its_pieces_go() {
    let source = store();
    let small = || Log {
        refuse_more_than: Some(1),
        ..Log::default()
    };

    let (mut client, _) = receiving_with(small());
    let report = block(push(&mut client, &source, "there", &Options::default(), |_| {})).unwrap();
    assert_eq!(report.stopped.as_ref().map(|s| s.island), Some(0));
    assert!(report.splits.is_empty());

    for how in [Split::Halve, Split::Single] {
        let (mut client, log) = receiving_with(small());
        let options = Options {
            split: how,
            ..Default::default()
        };
        let mut said = Vec::new();
        let report = block(push(&mut client, &source, "there", &options, |event| {
            said.push(event)
        }))
        .unwrap();
        assert!(report.stopped.is_none(), "{how:?}: {:?}", report.stopped);
        assert_eq!(report.splits.len(), 1, "{how:?}: {:?}", report.splits);
        let split = &report.splits[0];
        assert_eq!((split.island, split.into, split.uris.len()), (0, 2, 2));
        assert_eq!(split.refusals.len(), 2, "{:?}", split.refusals);
        assert!(
            split.refusals.iter().all(|r| r.code == Some(6)),
            "EREQUEST_DENIED, section 24.3"
        );
        assert_eq!(report.islands.len(), 3, "the two pieces in the island's place");
        assert_eq!(report.sent, 3);
        assert!(said.contains(&Progress::Split { island: 0, into: 2 }), "{said:?}");
        assert_eq!(log.lock().unwrap().objects.len(), 3);
    }

    let (mut client, _) = receiving_with(small());
    let options = Options {
        split: Split::Halve,
        split_limit: 0,
        ..Default::default()
    };
    let report = block(push(&mut client, &source, "there", &options, |_| {})).unwrap();
    assert_eq!(report.stopped.as_ref().map(|s| s.island), Some(0));
    assert!(report.splits.is_empty());
}

/// **A refusal a split cannot narrow further stops the push there**, and its
/// refusals are the last ones: the CRS is refused alone, the push stops at
/// its piece, and the split before it keeps what the whole island was told.
#[test]
fn a_refusal_split_down_to_one_object_stops_there() {
    let source = store();
    let (mut client, _) = receiving_with(Log {
        refuse_one: Some((CRS, "the CRS does not validate")),
        ..Log::default()
    });
    let options = Options {
        split: Split::Halve,
        ..Default::default()
    };
    let report = block(push(&mut client, &source, "there", &options, |_| {})).unwrap();
    assert_eq!(report.splits.len(), 1, "{:?}", report.splits);
    let stopped = report.stopped.as_ref().expect("the push stopped");
    assert_eq!(report.islands[stopped.island].uris.len(), 1);
    assert!(report.islands[stopped.island].uris[0].contains(CRS));
    assert_eq!(report.refusals.len(), 1, "{:?}", report.refusals);
    assert!(report.refusals[0].uri.contains(CRS));
    assert_eq!(report.refusals[0].code, Some(14), "EINVALID_OBJECT, section 24.3");
}

/// Sessions with one receiving store, opened one after the other by the
/// push's runner: session `n` falls as `falls[n]` says - the last entry
/// holds for every session after it - and nothing waits between two.
struct Sessions {
    log: Arc<Mutex<Log>>,
    falls: Vec<Fall>,
    opened: usize,
    pauses: Vec<Duration>,
}

impl Sessions {
    fn new(falls: Vec<Fall>) -> Self {
        Sessions {
            log: Arc::new(Mutex::new(Log::default())),
            falls,
            opened: 0,
            pauses: Vec::new(),
        }
    }
}

impl Connector for Sessions {
    type Session = Client<MemoryTransport, ManualClock>;

    fn connect(&mut self) -> Result<Self::Session, Box<dyn std::error::Error + Send + Sync>> {
        let fall = self
            .falls
            .get(self.opened)
            .or(self.falls.last())
            .copied()
            .unwrap_or(Fall::Never);
        self.opened += 1;
        Ok(session(&self.log, &[3, 4, 9, 18], fall))
    }

    fn pause(&mut self, how_long: Duration) {
        self.pauses.push(how_long);
    }
}

fn retrying(max_retries: u32) -> Retry {
    Retry {
        max_retries,
        first_pause: Duration::from_millis(10),
        longest_pause: Duration::from_millis(40),
    }
}

/// **A connection that falls is reopened, and the push goes on from the
/// first island not committed.** The first session falls on the second
/// commit, which never reaches the store; the second session sends that
/// island again, whole, and not the one committed before it.
#[test]
fn a_fallen_connection_is_reopened_and_the_push_goes_on() {
    let source = store();
    let mut sessions = Sessions::new(vec![
        Fall::OnThe {
            nth: 2,
            protocol: CommitTransaction::PROTOCOL,
            message_type: CommitTransaction::MESSAGE_TYPE,
        },
        Fall::Never,
    ]);
    let mut session = None;
    let mut events = Vec::new();
    let report = push_resumable(
        &mut sessions,
        &mut session,
        &source,
        "there",
        &Options::default(),
        &retrying(3),
        |event| events.push(event),
    )
    .unwrap();
    assert!(report.stopped.is_none(), "{:?}", report.stopped);
    assert_eq!(report.sent, 2);
    assert_eq!(report.reconnections, 1);
    // A connector that offers no resume: a new session, nothing refused,
    // and no `ResumeSession` on the wire.
    assert_eq!(report.reopened, [Came::Fresh { refused: None }]);
    assert_eq!(sent(&sessions.log, RESUME_SESSION), 0);
    assert_eq!(sessions.opened, 2);
    assert_eq!(sessions.pauses, [Duration::from_millis(10)]);
    assert!(session.is_some(), "the last session is left open");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, RunEvent::Setback(Setback::Reconnecting { .. }))),
        "{events:?}"
    );
    let log = sessions.log.lock().unwrap();
    assert_eq!(
        log.transactions,
        ["start", "commit", "start", "array", "start", "array", "commit"],
        "the island with the array twice, the first island once"
    );
    let crs_writes = log.objects.iter().filter(|(uri, _)| uri.contains(CRS)).count();
    assert_eq!(crs_writes, 1, "what committed is not sent again");
}

/// **A push gives up after so many falls in a row**, and says it did: the
/// island it stopped at is named, its outcome unknown. Told to reconnect
/// twice, it opens two new sessions after the first, and no third. An
/// island the connection falls on twice in a row is halved before the next
/// try.
#[test]
fn a_push_gives_up_after_so_many_falls_in_a_row() {
    let source = store();
    let mut sessions = Sessions::new(vec![Fall::OnThe {
        nth: 1,
        protocol: PutDataObjects::PROTOCOL,
        message_type: PutDataObjects::MESSAGE_TYPE,
    }]);
    let mut session = None;
    let report = push_resumable(
        &mut sessions,
        &mut session,
        &source,
        "there",
        &Options::default(),
        &reconnecting(2, Duration::from_millis(10)),
        |_| {},
    )
    .unwrap();
    assert_eq!(sessions.opened, 3, "the first session and two new ones");
    assert_eq!(
        sessions.pauses,
        [Duration::from_millis(10), Duration::from_millis(20)],
        "the pause doubles"
    );
    assert_eq!(report.sent, 0);
    match &report.stopped.as_ref().expect("it gave up").why {
        Stop::GaveUp { attempts, last } => {
            assert_eq!(*attempts, 3);
            assert!(last.contains("fell"), "{last}");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(report.reconnections, 2);
    assert_eq!(
        report.splits.len(),
        1,
        "halved after the second fall: {:?}",
        report.splits
    );
    assert!(
        report.splits[0].why.contains("connection"),
        "{:?}",
        report.splits[0]
    );
}

/// **A server that hangs up on heavy requests gets halves.** It falls on
/// any `PutDataObjects` of more than one object: after two falls on the
/// first island, the island is halved, and the push goes through.
#[test]
fn a_server_that_hangs_up_on_heavy_requests_gets_halves() {
    let source = store();
    let mut sessions = Sessions::new(vec![Fall::OnMoreObjectsThan(1)]);
    let mut session = None;
    let report = push_resumable(
        &mut sessions,
        &mut session,
        &source,
        "there",
        &Options::default(),
        &retrying(3),
        |_| {},
    )
    .unwrap();
    assert!(report.stopped.is_none(), "{:?}", report.stopped);
    assert_eq!(report.reconnections, 2);
    assert_eq!(report.splits.len(), 1, "{:?}", report.splits);
    assert_eq!(report.sent, report.islands.len());
    assert_eq!(report.islands.len(), 3);
}

// -- asking the server for the lost session (OSDU's ResumeSession, ADR-0003) --

/// `RequestSession` (Core, ETP v1.2 section 5.3.1) and OSDU's `ResumeSession`
/// (CoreOSDU 2400, message type 1, as `open-etp-server`'s `etp-1.2.avpr`
/// numbers it).
const REQUEST_SESSION: (i32, i32) = (0, 1);
const RESUME_SESSION: (i32, i32) = (2400, 1);

/// What the connector of a push that asks for its lost session did: the
/// sessions it opened from the start, and those it asked back.
#[derive(Default)]
struct Asked {
    opened: usize,
    resumed: usize,
}

/// A push into one receiving store over `retp_client::native::Resuming` -
/// the connector the command line and the Python bridge build, `ask` being
/// their option - whose first session falls on its second commit: a new
/// connection asks the lost session back when `ask` is on, and its server
/// answers as `answers` says.
fn pushed_after_a_fall(
    ask: bool,
    answers: Answers,
) -> (energyml_etp_store::push::Report, Arc<Mutex<Log>>, Asked) {
    let source = store();
    let log = Arc::new(Mutex::new(Log::default()));
    let asked = RefCell::new(Asked::default());
    let first_fall = Fall::OnThe {
        nth: 2,
        protocol: CommitTransaction::PROTOCOL,
        message_type: CommitTransaction::MESSAGE_TYPE,
    };
    let mut connector = Resuming {
        open: || {
            let mut asked = asked.borrow_mut();
            let fall = if asked.opened + asked.resumed == 0 {
                first_fall
            } else {
                Fall::Never
            };
            asked.opened += 1;
            Ok::<_, std::convert::Infallible>(session(&log, &[3, 4, 9, 18], fall))
        },
        resume: |lost: Resumable| {
            asked.borrow_mut().resumed += 1;
            Ok(connected(
                &log,
                &[3, 4, 9, 18],
                Fall::Never,
                Some((answers, lost)),
            ))
        },
        ask,
    };
    let mut session = None;
    let report = push_resumable(
        &mut connector,
        &mut session,
        &source,
        "there",
        &Options::default(),
        &retrying(3),
        |_| {},
    )
    .unwrap();
    assert!(report.stopped.is_none(), "{:?}", report.stopped);
    assert_eq!(report.sent, 2);
    assert_eq!(report.reconnections, 1);
    assert_eq!(
        report.reopened.len(),
        report.reconnections,
        "one word per reconnection"
    );
    {
        let log = log.lock().unwrap();
        // ADR-0003, and `Connector::resume` point 1: a session given back is
        // the envelope, not the work - the island cut goes again whole, the
        // one committed before does not, on either path.
        assert_eq!(
            log.transactions,
            ["start", "commit", "start", "array", "start", "array", "commit"],
            "the island with the array twice, the first island once"
        );
        let crs_writes = log.objects.iter().filter(|(uri, _)| uri.contains(CRS)).count();
        assert_eq!(crs_writes, 1, "what committed is not sent again");
    }
    (report, log, asked.into_inner())
}

fn sent(log: &Arc<Mutex<Log>>, kind: (i32, i32)) -> usize {
    log.lock()
        .unwrap()
        .sent
        .iter()
        .filter(|one| **one == kind)
        .count()
}

/// **Asked for, the lost session is given back, and the push goes on in
/// it.** The new connection's first word is `ResumeSession`, no
/// `RequestSession` follows it, and the report says the session came back
/// resumed.
#[test]
fn a_push_asked_to_goes_on_in_the_session_the_server_gives_back() {
    let (report, log, asked) = pushed_after_a_fall(true, Answers::GivesBack);
    assert_eq!(report.reopened, [Came::Resumed]);
    assert_eq!((asked.opened, asked.resumed), (1, 1));
    assert_eq!(sent(&log, RESUME_SESSION), 1);
    assert_eq!(
        sent(&log, REQUEST_SESSION),
        1,
        "the first session's own: the resumed one is not negotiated again"
    );
}

/// **A server that no longer has the session says so, and a new session
/// opens at once on the same connection.** OSDU's server answers
/// `ENOT_FOUND`, *Session not found or expired*: the push goes on, and the
/// report carries the server's words.
#[test]
fn a_session_the_server_no_longer_has_is_opened_anew_and_the_reason_said() {
    let (report, log, asked) = pushed_after_a_fall(true, Answers::NotFound);
    let [Came::Fresh { refused: Some(why) }] = report.reopened.as_slice() else {
        panic!("{:?}", report.reopened);
    };
    assert!(
        why.contains("ENOT_FOUND") && why.contains("Session not found or expired"),
        "{why}"
    );
    assert_eq!(
        (asked.opened, asked.resumed),
        (1, 1),
        "the new session is the resuming connection's, not another one"
    );
    assert_eq!(sent(&log, RESUME_SESSION), 1);
    assert_eq!(
        sent(&log, REQUEST_SESSION),
        2,
        "the first session, then the new one"
    );
}

/// **A server that does not speak 2400 gets a new session too**, and the
/// report says why the resume did not serve.
#[test]
fn a_server_without_resume_gets_a_new_session_and_the_reason_is_said() {
    let (report, log, _) = pushed_after_a_fall(true, Answers::Unspoken);
    let [Came::Fresh { refused: Some(why) }] = report.reopened.as_slice() else {
        panic!("{:?}", report.reopened);
    };
    assert!(
        why.contains("EUNSUPPORTED_PROTOCOL") && why.contains("2400"),
        "{why}"
    );
    assert_eq!(sent(&log, RESUME_SESSION), 1);
    assert_eq!(sent(&log, REQUEST_SESSION), 2);
}

/// **Not asked, no `ResumeSession` ever leaves**, whatever the server would
/// answer: a new session from the start, as a push always reconnected, and
/// the report says nothing was refused, because nothing was asked.
#[test]
fn a_push_not_asked_to_never_sends_resume_session() {
    for answers in [Answers::GivesBack, Answers::NotFound, Answers::Unspoken] {
        let (report, log, asked) = pushed_after_a_fall(false, answers);
        assert_eq!(report.reopened, [Came::Fresh { refused: None }], "{answers:?}");
        assert_eq!((asked.opened, asked.resumed), (2, 0), "{answers:?}");
        assert_eq!(sent(&log, RESUME_SESSION), 0, "{answers:?}");
        assert_eq!(sent(&log, REQUEST_SESSION), 2, "{answers:?}");
    }
}

// ---------------------------------------------------------------------------
// The plan without a session (`dry_run`), and why a push stopped, sorted
// ---------------------------------------------------------------------------

/// A dry run of `source` and a push of it into a receiving store, with the
/// same options: the plan must be the islands the push followed, its
/// destination URIs what the store received, in that order.
fn the_plan_is_the_push(
    source: &(dyn energyml_storage::storage::EnergymlStorage + Sync),
    options: &Options<'_>,
    what: &str,
) {
    let plan = block(dry_run(source, "there", options)).unwrap_or_else(|why| panic!("{what}: {why}"));
    let (mut client, log) = receiving(None);
    let report = block(push(&mut client, source, "there", options, |_| {})).unwrap();
    assert!(report.stopped.is_none(), "{what}: {:?}", report.stopped);
    assert!(
        !plan.islands.is_empty(),
        "{what}: a plan of nothing proves nothing"
    );
    assert_eq!(
        plan.islands, report.islands,
        "{what}: the islands, their units included"
    );
    assert_eq!(plan.dataspace, THERE, "{what}");
    assert_eq!(plan.dangling, report.dangling, "{what}");
    assert_eq!(plan.left_out, report.left_out, "{what}");
    let planned: Vec<&str> = plan
        .islands
        .iter()
        .flat_map(|island| plan.destinations_of(island))
        .collect();
    let log = log.lock().unwrap();
    let received: Vec<&str> = log.objects.iter().map(|(uri, _)| uri.as_str()).collect();
    assert_eq!(
        planned, received,
        "{what}: the destination URIs, in the order sent"
    );
}

/// **A dry run plans the islands the push follows** - the same reading, the
/// same planning, the same budget -, for each option that decides a plan:
/// none, a budget of one object, `only`, JSON, the dictionary's kinds.
#[test]
fn a_dry_run_plans_the_islands_the_push_follows() {
    let source = store();
    the_plan_is_the_push(&source, &Options::default(), "by default");

    let one_each = Options {
        budget: Budget {
            objects: 1,
            ..Budget::default()
        },
        ..Default::default()
    };
    the_plan_is_the_push(&source, &one_each, "one object per island");

    let listed = block(energyml_storage::storage::EnergymlStorage::resources(&source)).unwrap();
    let points = listed
        .iter()
        .find(|one| one.qualified_type.contains("PointSetRepresentation"))
        .unwrap()
        .uri
        .clone();
    let only = [points];
    let some = Options {
        only: Some(&only),
        ..Default::default()
    };
    the_plan_is_the_push(&source, &some, "only the point set");

    let json = Options {
        format: WriteFormat::Json,
        ..Default::default()
    };
    the_plan_is_the_push(&source, &json, "as JSON");

    let registry = registry();
    let kinds = Options {
        well_known: WellKnown::Push(&registry),
        ..Default::default()
    };
    the_plan_is_the_push(&store_with_kind(), &kinds, "with the dictionary's kinds");
}

/// **A dry run leaves out what the journal records, and writes nothing**:
/// after a push that committed its first island, the plan names the CRS and
/// the proxy as skipped by the journal and plans the point set alone; the
/// journal is the same file after it, byte for byte. A journal that is not
/// there records nothing, and is not created; one written for another
/// dataspace is refused by name.
#[test]
fn a_dry_run_leaves_out_what_the_journal_records_and_writes_nothing() {
    let source = store();
    let path = journal_path("plan");
    let options = Options {
        journal: Some(&path),
        ..Default::default()
    };
    let plan = block(dry_run(&source, "there", &options)).unwrap();
    assert!(plan.skipped.is_empty(), "{:?}", plan.skipped);
    assert_eq!(plan.islands.len(), 2, "{:?}", plan.islands);
    assert!(!path.exists(), "a dry run created the journal");

    let (mut client, _) = receiving_with(Log {
        refuse_arrays: Some("no room for arrays"),
        ..Log::default()
    });
    let report = block(push(&mut client, &source, "there", &options, |_| {})).unwrap();
    assert_eq!(report.sent, 1, "{:?}", report.stopped);
    let before = std::fs::read(&path).unwrap();

    let plan = block(dry_run(&source, "there", &options)).unwrap();
    let mut skipped: Vec<String> = plan
        .skipped
        .iter()
        .inspect(|one| assert_eq!(one.why, SkipReason::Journal))
        .map(|one| one.uri.clone())
        .collect();
    skipped.sort();
    let mut expected = vec![crs_there(), proxy_there()];
    expected.sort();
    assert_eq!(skipped, expected);
    let planned: Vec<&str> = plan
        .islands
        .iter()
        .flat_map(|island| plan.destinations_of(island))
        .collect();
    assert_eq!(planned, [points_there()], "only what the journal does not record");
    assert_eq!(
        std::fs::read(&path).unwrap(),
        before,
        "a dry run wrote to the journal"
    );

    // And the push started again follows that plan.
    let (mut client, _) = receiving(None);
    let report = block(push(&mut client, &source, "there", &options, |_| {})).unwrap();
    assert_eq!(plan.islands, report.islands);

    match block(dry_run(&source, "elsewhere", &options)) {
        Err(NoPlan::Journal(why)) => assert!(why.contains("written for a push into"), "{why}"),
        other => panic!("{other:?}"),
    }
    let _ = std::fs::remove_file(&path);
}

/// **What only the destination can answer is refused by name** - not
/// planned without: a caller who asks to skip what is there, or to check
/// what changed there, expects it applied.
#[test]
fn a_dry_run_refuses_what_needs_the_destination() {
    let source = store();
    let skipping = Options {
        skip_present: true,
        ..Default::default()
    };
    match block(dry_run(&source, "there", &skipping)) {
        Err(NoPlan::NeedsDestination(why)) => assert!(why.contains("skip_present"), "{why}"),
        other => panic!("{other:?}"),
    }
    let expected: BTreeMap<String, i64> = [(points_there(), T0)].into_iter().collect();
    let checking = Options {
        expected: Some(&expected),
        ..Default::default()
    };
    match block(dry_run(&source, "there", &checking)) {
        Err(NoPlan::NeedsDestination(why)) => assert!(why.contains("expected"), "{why}"),
        other => panic!("{other:?}"),
    }
}

/// **A cycle is flagged**: two objects that reference each other are one
/// unit of their island, named as its cycle; an object alone is a unit of
/// one, and an island without a cycle names none.
#[test]
fn a_cycle_is_flagged_in_the_plan() {
    use energyml_storage::graph::{Edge, Graph};
    use energyml_storage::resource::ObjectSummary;
    let listed: Vec<ObjectSummary> = ["a", "b", "c"]
        .iter()
        .map(|uri| ObjectSummary::new(*uri, *uri, String::new()))
        .collect();
    let graph = Graph {
        nodes: Vec::new(),
        edges: vec![
            Edge::new("a", "b", String::new()),
            Edge::new("b", "a", String::new()),
            Edge::new("c", "a", String::new()),
        ],
    };
    let planned = islands(&listed, &graph, &BTreeMap::new(), &Budget::default());
    assert_eq!(planned.len(), 1, "{planned:?}");
    assert_eq!(planned[0].units, [2, 1]);
    let cycles = planned[0].cycles();
    assert_eq!(cycles.len(), 1);
    assert_eq!(cycles[0], ["a".to_string(), "b".to_string()]);

    let none = islands(
        &listed,
        &Graph {
            nodes: Vec::new(),
            edges: vec![Edge::new("c", "a", String::new())],
        },
        &BTreeMap::new(),
        &Budget::default(),
    );
    assert!(none.iter().all(|island| island.cycles().is_empty()), "{none:?}");
    assert!(
        none.iter()
            .all(|island| island.units.iter().all(|&size| size == 1))
    );
}

/// **The words open-etp-server is documented to use are recognised, and
/// nothing else.** Each shape as its sources write it - the checks of
/// `CommitTransaction` in `src/lib/oes/eml/openkv/TransProto.cpp` (lines
/// 267-336 of `main` as read on 2026-10-15) and the check outside a
/// transaction in `StoreProto.cpp` -, then words that resemble them and are
/// not them: another case, a prefix, a suffix, a count that is no count.
#[test]
fn the_documented_refusals_are_classified_and_nothing_else() {
    // "{} Missing reference(s)", TransProto.cpp:267-290.
    assert_eq!(
        Cause::of("3 Missing reference(s)"),
        Cause::MissingReference {
            count: 3,
            space: None,
            named: Vec::new(),
            more: 0,
        }
    );
    // "{} Missing reference(s) in space {}", StoreProto.cpp, outside a transaction.
    assert_eq!(
        Cause::of("1 Missing reference(s) in space demo"),
        Cause::MissingReference {
            count: 1,
            space: Some("demo".into()),
            named: Vec::new(),
            more: 0,
        }
    );
    // "{} Orphan reference(s)", TransProto.cpp:277,292-298.
    assert_eq!(
        Cause::of("2 Orphan reference(s)"),
        Cause::OrphanReference { count: 2 }
    );
    // "{} Missing array(s)", TransProto.cpp:317-327.
    assert_eq!(Cause::of("4 Missing array(s)"), Cause::MissingArray { count: 4 });
    // "{} Orphan array(s)", TransProto.cpp:329-336.
    assert_eq!(Cause::of("5 Orphan array(s)"), Cause::OrphanArray { count: 5 });
    // "Missing array container(s): ...", TransProto.cpp:279-281,300-304.
    assert_eq!(
        Cause::of("Missing array container(s): a, b"),
        Cause::MissingArrayContainer {
            containers: "a, b".into()
        }
    );
    // Surrounding blanks are not words.
    assert_eq!(Cause::of(" 3 Missing reference(s)\n").name(), "missing_reference");

    for not_documented in [
        "",
        "reference missing",
        "3 missing reference(s)",
        "Missing reference(s)",
        "-3 Missing reference(s)",
        "three Missing reference(s)",
        "3 Missing references",
        "3 Missing reference(s) in space ",
        "the commit was refused: 3 Missing reference(s)",
        "3 Missing reference(s) - 2 reference(s) leave the source",
        "3 Orphan array(s) and more",
        "Missing array container(s):",
        "the store gave no reason",
    ] {
        assert_eq!(Cause::of(not_documented), Cause::Unknown, "{not_documented:?}");
    }
    assert_eq!(Cause::Unknown.name(), "unknown");
}

/// **A refused commit is classified from the store's words, never from the
/// hint.** Only the point set is pushed, so its references to the CRS and
/// the proxy leave the source: the sentence carries the hint, the store's
/// words are kept apart and alone, and the candidates are those two
/// references - this side's deduction, from the island that stopped.
#[test]
fn a_refused_commit_is_classified_from_the_stores_words_not_the_hint() {
    let source = store();
    let listed = block(energyml_storage::storage::EnergymlStorage::resources(&source)).unwrap();
    let points = listed
        .iter()
        .find(|one| one.qualified_type.contains("PointSetRepresentation"))
        .unwrap()
        .uri
        .clone();
    let only = [points.clone()];
    let options = Options {
        only: Some(&only),
        ..Default::default()
    };
    let (mut client, _) = receiving(Some("2 Missing reference(s)"));
    let report = block(push(&mut client, &source, "there", &options, |_| {})).unwrap();

    let stopped = report.stopped.as_ref().expect("the push stopped");
    match &stopped.why {
        Stop::Refused(why) => assert!(why.contains("leave the source"), "the hint is said: {why}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(
        stopped.said.as_ref().map(|said| said.message.as_str()),
        Some("2 Missing reference(s)"),
        "the store's words, alone"
    );
    let diagnosis = report.diagnosis().expect("a stop");
    assert_eq!(diagnosis.kind, StopKind::Refused);
    assert_eq!(diagnosis.message, "2 Missing reference(s)");
    assert_eq!(diagnosis.code, None, "a commit's answer carries no code");
    assert_eq!(
        diagnosis.cause,
        Some(Cause::MissingReference {
            count: 2,
            space: None,
            named: Vec::new(),
            more: 0,
        })
    );
    assert_eq!(report.dangling.len(), 2, "{:?}", report.dangling);
    assert_eq!(diagnosis.candidates, report.dangling);
    assert!(diagnosis.candidates.iter().all(|(from, _)| *from == points));
}

/// **A server that names the missing references is read whole, or not at
/// all.** The form is open-etp-server's from M27 (merge request !586,
/// `TransProto.cpp`, test `test_Transaction18_missing_reference_failure_reason`
/// in `TransProtoTests.cpp`): `"{N} Missing reference(s): from {type}({uuid})
/// to {type}({uuid}) using {xpath}; ..."`, 25 entries at most, then `"; ...
/// (+{k} more)"`. A list that does not parse, or whose entries and `k` do not
/// add up to `N`, is `unknown`: nothing documents it.
#[test]
fn the_missing_references_a_server_names_are_parsed_whole_or_not_at_all() {
    let a = "resqml20.obj_PointSetRepresentation";
    let from = "11111111-1111-1111-1111-111111111111";
    let to = "22222222-2222-2222-2222-222222222222";
    let entry = |n: u32| format!("from {a}({from}) to resqml20.obj_LocalDepth3dCrs({to}) using /LocalCrs{n}");

    // One entry.
    let one = format!("1 Missing reference(s): {}", entry(0));
    assert_eq!(
        Cause::of(&one),
        Cause::MissingReference {
            count: 1,
            space: None,
            named: vec![NamedReference {
                from_type: a.into(),
                from_uuid: from.into(),
                to_type: "resqml20.obj_LocalDepth3dCrs".into(),
                to_uuid: to.into(),
                xpath: "/LocalCrs0".into(),
            }],
            more: 0,
        }
    );
    // Two entries, in the server's order.
    let two = format!("2 Missing reference(s): {}; {}", entry(0), entry(1));
    match Cause::of(&two) {
        Cause::MissingReference {
            count, named, more, ..
        } => {
            assert_eq!((count, named.len(), more), (2, 2, 0));
            assert_eq!(
                (named[0].xpath.as_str(), named[1].xpath.as_str()),
                ("/LocalCrs0", "/LocalCrs1")
            );
        }
        other => panic!("{other:?}"),
    }
    // Capped: 25 named, the rest counted.
    let list: Vec<String> = (0..25).map(entry).collect();
    let capped = format!("40 Missing reference(s): {}; ... (+15 more)", list.join("; "));
    match Cause::of(&capped) {
        Cause::MissingReference {
            count, named, more, ..
        } => {
            assert_eq!((count, named.len(), more), (40, 25, 15));
        }
        other => panic!("{other:?}"),
    }
    // A placeholder the server writes for what it could not resolve (MR !586,
    // `SpaceWriter::get_references_infos`).
    let placeholder = format!(
        "1 Missing reference(s): from <obj_id 7>({from}) to <unresolved type_id 3>({to}) using <xpa_id 9>"
    );
    match Cause::of(&placeholder) {
        Cause::MissingReference { named, .. } => {
            assert_eq!(named[0].from_type, "<obj_id 7>");
            assert_eq!(named[0].to_type, "<unresolved type_id 3>");
            assert_eq!(named[0].xpath, "<xpa_id 9>");
        }
        other => panic!("{other:?}"),
    }
    // The count-only form is unchanged, and has no names.
    match Cause::of("3 Missing reference(s)") {
        Cause::MissingReference { named, more, .. } => assert_eq!((named.len(), more), (0, 0)),
        other => panic!("{other:?}"),
    }

    // Malformed named forms are `unknown`, not the count alone.
    let good = entry(0);
    for malformed in [
        "1 Missing reference(s): ".to_string(),
        "1 Missing reference(s): something".to_string(),
        "1 Missing reference(s): from a(b) to c(d)".to_string(),
        format!("1 Missing reference(s): from {a}({from}) to x({to}) using "),
        format!("1 Missing reference(s): from {a}() to x({to}) using /p"),
        format!("1 Missing reference(s): from {a} to x({to}) using /p"),
        // Entries and tail do not add up to the count.
        format!("2 Missing reference(s): {good}"),
        format!("1 Missing reference(s): {good}; ... (+3 more)"),
        // A tail that is not last, or has no number.
        format!("3 Missing reference(s): {good}; ... (+2 more); {good}"),
        format!("2 Missing reference(s): {good}; ... (+ more)"),
        format!("2 Missing reference(s): {good}; ... (+x more)"),
        // Only a tail.
        "3 Missing reference(s): ... (+3 more)".to_string(),
        // A second entry that is not one.
        format!("2 Missing reference(s): {good}; garbage"),
        // Not the documented separator.
        format!("2 Missing reference(s): {good}, {good}"),
    ] {
        assert_eq!(Cause::of(&malformed), Cause::Unknown, "{malformed:?}");
    }
}

/// **When the server names the missing references, they are its statement and
/// the deduction stays out**; the count-only form keeps the candidates.
#[test]
fn a_named_missing_reference_comes_before_the_deduction() {
    let source = store();
    let listed = block(energyml_storage::storage::EnergymlStorage::resources(&source)).unwrap();
    let points = listed
        .iter()
        .find(|one| one.qualified_type.contains("PointSetRepresentation"))
        .unwrap()
        .uri
        .clone();
    let only = [points];
    let options = Options {
        only: Some(&only),
        ..Default::default()
    };
    let named = "1 Missing reference(s): from resqml20.obj_PointSetRepresentation(11111111-1111-1111-1111-111111111111) to resqml20.obj_LocalDepth3dCrs(22222222-2222-2222-2222-222222222222) using /LocalCrs";
    let (mut client, _) = receiving(Some(named));
    let report = block(push(&mut client, &source, "there", &options, |_| {})).unwrap();
    assert_eq!(report.dangling.len(), 2, "this side's deduction exists");
    let diagnosis = report.diagnosis().expect("a stop");
    assert_eq!(diagnosis.message, named);
    match diagnosis.cause {
        Some(Cause::MissingReference {
            count, named, more, ..
        }) => {
            assert_eq!((count, named.len(), more), (1, 1, 0));
            assert_eq!(named[0].xpath, "/LocalCrs");
        }
        other => panic!("{other:?}"),
    }
    assert!(
        diagnosis.candidates.is_empty(),
        "the server named the target: {:?}",
        diagnosis.candidates
    );
}

/// **The candidates are the stopped island's alone.** With the CRS hidden,
/// the point set's reference to it leaves the source; every commit is
/// refused, so the push stops at the first island - the proxy's -, which
/// references nothing outside: no candidate, though `dangling` is not empty.
#[test]
fn the_candidates_are_those_of_the_island_that_stopped() {
    let source = store_hiding(CRS);
    let (mut client, _) = receiving(Some("1 Missing reference(s)"));
    let report = block(push(&mut client, &source, "there", &Options::default(), |_| {})).unwrap();
    assert_eq!(report.dangling.len(), 1, "{:?}", report.dangling);
    let diagnosis = report.diagnosis().expect("a stop");
    assert_eq!(diagnosis.island, 0);
    assert!(
        report.islands[0].uris.iter().all(|uri| !uri.contains(POINTS)),
        "{:?}",
        report.islands
    );
    assert_eq!(
        diagnosis.cause,
        Some(Cause::MissingReference {
            count: 1,
            space: None,
            named: Vec::new(),
            more: 0,
        })
    );
    assert!(diagnosis.candidates.is_empty(), "{:?}", diagnosis.candidates);
}

/// **A refused object carries the cause of its own words**, and so does the
/// stop, with the store's code - the first refusal, as the store wrote it.
///
/// The test server writes an error's message as `retp-proto` does, its
/// name first (`EINVALID_OBJECT: ...`): the documented words inside other
/// words are not read as them, so its refusal is `unknown`. The words alone,
/// as open-etp-server writes a write refused at once outside a transaction
/// (`EINVALID_STATE`, `StoreProto.cpp`), name the missing reference. Words
/// of no documented shape are `unknown`, and still a refusal.
#[test]
fn a_refused_object_is_classified_from_its_own_words() {
    use energyml_etp_store::push::{Island, Refusal, Report, Said, Stopped};
    let source = store();
    let (mut client, _) = receiving_with(Log {
        refuse_one: Some((CRS, "2 Missing reference(s) in space there")),
        ..Log::default()
    });
    let report = block(push(&mut client, &source, "there", &Options::default(), |_| {})).unwrap();
    assert_eq!(report.refusals.len(), 1, "{:?}", report.refusals);
    let first = &report.refusals[0];
    assert_eq!(
        first.message,
        "EINVALID_OBJECT: 2 Missing reference(s) in space there"
    );
    let diagnosis = report.diagnosis().expect("a stop");
    assert_eq!(diagnosis.code, Some(14), "EINVALID_OBJECT, ETP v1.2 section 24.3");
    assert_eq!(diagnosis.message, first.message, "the first refusal's words");
    assert_eq!(first.cause(), Cause::Unknown);
    assert_eq!(diagnosis.cause, Some(Cause::Unknown));

    // The words alone.
    let expected = Cause::MissingReference {
        count: 2,
        space: Some("there".into()),
        named: Vec::new(),
        more: 0,
    };
    let alone = Refusal {
        uri: "eml:///dataspace('there')/resqml20.obj_PointSetRepresentation(p)".into(),
        array: None,
        code: Some(15),
        message: "2 Missing reference(s) in space there".into(),
    };
    assert_eq!(alone.cause(), expected);
    let report = Report {
        islands: vec![Island {
            uris: vec!["points".into()],
            units: vec![1],
            ..Island::default()
        }],
        stopped: Some(Stopped {
            island: 0,
            why: Stop::Refused("points: 2 Missing reference(s) in space there (15)".into()),
            said: Some(Said {
                code: alone.code,
                message: alone.message.clone(),
            }),
        }),
        dangling: vec![("points".into(), "crs".into()), ("other".into(), "kind".into())],
        refusals: vec![alone],
        ..Report::default()
    };
    let diagnosis = report.diagnosis().expect("a stop");
    assert_eq!(diagnosis.code, Some(15));
    assert_eq!(diagnosis.cause, Some(expected));
    assert_eq!(diagnosis.candidates, [("points".to_string(), "crs".to_string())]);

    let (mut client, _) = receiving_with(Log {
        refuse_objects: Some("this dataspace is published"),
        ..Log::default()
    });
    let report = block(push(&mut client, &source, "there", &Options::default(), |_| {})).unwrap();
    let diagnosis = report.diagnosis().expect("a stop");
    assert_eq!(diagnosis.kind, StopKind::Refused);
    assert_eq!(diagnosis.cause, Some(Cause::Unknown));
    assert!(diagnosis.candidates.is_empty());
    assert!(report.refusals.iter().all(|one| one.cause() == Cause::Unknown));
}

/// **A request that timed out is a timeout, a connection that fell is a
/// failure** - both `Stop::Failed`, told apart: after a timeout the session
/// may stand, after a fall it does not. Neither names a cause, which is a
/// refusal's.
#[test]
fn a_timeout_is_told_from_a_lost_connection() {
    let source = store();
    let (mut client, _) = receiving_with(Log {
        silent_puts: true,
        ..Log::default()
    });
    let report = block(push(&mut client, &source, "there", &Options::default(), |_| {})).unwrap();
    let diagnosis = report.diagnosis().expect("a stop");
    assert_eq!(diagnosis.kind, StopKind::Timeout, "{:?}", report.stopped);
    assert_eq!(diagnosis.kind.name(), "timeout");
    assert_eq!(diagnosis.code, Some(26), "ETIMED_OUT, ETP v1.2 section 24.3");
    assert_eq!(diagnosis.cause, None);

    let log = Arc::new(Mutex::new(Log::default()));
    let mut client = session(
        &log,
        &[3, 4, 9, 18],
        Fall::OnThe {
            nth: 1,
            protocol: PutDataObjects::PROTOCOL,
            message_type: PutDataObjects::MESSAGE_TYPE,
        },
    );
    let report = block(push(&mut client, &source, "there", &Options::default(), |_| {})).unwrap();
    let diagnosis = report.diagnosis().expect("a stop");
    assert_eq!(diagnosis.kind, StopKind::Failed, "{:?}", report.stopped);
    assert_eq!(diagnosis.kind.name(), "failed");
    assert_eq!(diagnosis.cause, None);
}
