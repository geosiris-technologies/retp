//! The writing traits on an ETP dataspace - `EnergymlWriter` and
//! `EnergymlDataspaces` for `EtpStore` - played against a fake server that
//! takes writes and remembers what it was given.
//!
//! Nothing here reaches a real server. The server is the **second source**: each
//! test states what the trait promises (one result per item, in order; the
//! transaction is ended by the one that was opened; a read-only transaction
//! writes nothing) and reads what actually left the client in the server's log,
//! not in the store's own return value.
//!
//! # How to make each test fail
//!
//! - `an_object_is_readdressed_...`: in `writer.rs`, build the URI from
//!   `object.resource.uri` unchanged or without the dataspace (`uri_for`).
//! - `a_refused_object_...`: make `put_batch` answer `Ok` for every key.
//! - `the_same_object_twice_...`: send all objects in one round (drop
//!   `unique_rounds`): the answers collapse onto one key.
//! - `raw_objects_...`: write the bytes through `to_part`, or send `"xml"`
//!   whatever the format.
//! - `deleting_...`: answer `vec![uri]` instead of what the server says.
//! - `a_transaction_...`: skip `check_open`, or keep the transaction after a
//!   commit.
//! - `a_read_only_transaction_...`: drop `writable()` from any write.
//! - `a_listing_...`: remove `forget_listing()` from `put_batch`.
//! - `an_array_...`: skip the shape check in `put_arrays`.
//! - `dataspaces_...`: answer `Ok(())` for every dataspace.

mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use energyml_core::registry::{DynObject, ReadMode};
use energyml_etp_store::{EtpStore, read_object};
use energyml_storage::array::{ArrayData, ArrayLocation, ArrayMetadata, ArrayValues};
use energyml_storage::dataspaces::EnergymlDataspaces;
use energyml_storage::error::{Error, ExternalDataReason};
use energyml_storage::raw::{RawFormat, RawObject};
use energyml_storage::resource::Dataspace;
use energyml_storage::storage::EnergymlStorage;
use energyml_storage::write::{EnergymlWriter, Transaction, TransactionRequest};
use retp_avro::Role;
use retp_client::memory::{ManualClock, MemoryTransport};
use retp_client::notifications::uuid_text;
use retp_client::{Client, Settings, writing_client};
use retp_proto::connection::{Connection, Declined, Handled, Handler, Responder};
use retp_proto::negotiate::{Offer, ProtocolOffer};
use retp_proto::wire::{Body, Delivery, EtpError};
use retp_types::datatypes::object::{Dataspace as EtpDataspace, PutResponse, Resource};
use retp_types::datatypes::{AnyArrayItem, ArrayOfString, SupportedDataObject, Uuid};
use retp_types::dispatch::Message;
use retp_types::protocol::data_array::PutDataArraysResponse;
use retp_types::protocol::dataspace::{
    DeleteDataspacesResponse, GetDataspacesResponse, PutDataspacesResponse,
};
use retp_types::protocol::discovery::GetResourcesResponse;
use retp_types::protocol::store::{DeleteDataObjectsResponse, PutDataObjectsResponse};
use retp_types::protocol::transaction::{
    CommitTransactionResponse, RollbackTransactionResponse, StartTransactionResponse,
};

use common::*;

const CRS_TYPE: &str = "resqml20.obj_LocalDepth3dCrs";
const POINTS_TYPE: &str = "resqml20.obj_PointSetRepresentation";

/// How the server answers `StartTransaction`.
#[derive(Default, Clone, Copy)]
enum Start {
    #[default]
    Opens,
    Refuses(&'static str),
    /// `ENOTSUPPORTED`: no transactions here.
    Unsupported,
}

/// What the server was given, in the order it was given.
#[derive(Default)]
struct Log {
    /// Each object: its URI, its bytes, its `format`.
    objects: Vec<(String, Vec<u8>, String)>,
    /// Each array by `(uri, path)`: dimensions and doubles.
    arrays: BTreeMap<(String, String), (Vec<i64>, Vec<f64>)>,
    /// `start`, `commit`, `rollback`, `put`, `delete`, `array`, as they came.
    events: Vec<&'static str>,
    /// What each `StartTransaction` asked: its dataspaces, `readOnly`, `message`.
    started: Vec<(Vec<String>, bool, String)>,
    /// The dataspaces the server holds: URI and path.
    dataspaces: Vec<(String, String)>,
    /// Refuse (`EREQUEST_DENIED`) every object whose URI holds this uuid.
    refuse_uuid: Option<&'static str>,
    start: Start,
    refuse_commit: Option<&'static str>,
}

/// A store that takes writes and remembers them.
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
            Message::StartTransaction(request) => {
                log.events.push("start");
                log.started.push((
                    request.dataspace_uris.clone(),
                    request.read_only,
                    request.message.clone(),
                ));
                match log.start {
                    Start::Unsupported => {
                        let out = responder.wire().refuse(
                            delivery,
                            &EtpError::ENOTSUPPORTED.with_detail("no transactions here"),
                        );
                        responder.send(out);
                    }
                    Start::Opens | Start::Refuses(_) => {
                        let (successful, failure_reason) = match log.start {
                            Start::Refuses(why) => (false, why.to_string()),
                            _ => (true, String::new()),
                        };
                        let response = StartTransactionResponse {
                            transaction_uuid: Uuid([9; 16]),
                            successful,
                            failure_reason,
                        };
                        let out = responder.wire().respond(delivery, &response).unwrap();
                        responder.send(out);
                    }
                }
            }
            Message::CommitTransaction(request) => {
                log.events.push("commit");
                let response = CommitTransactionResponse {
                    transaction_uuid: request.transaction_uuid,
                    successful: log.refuse_commit.is_none(),
                    failure_reason: log.refuse_commit.unwrap_or("").to_string(),
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            Message::RollbackTransaction(request) => {
                log.events.push("rollback");
                let response = RollbackTransactionResponse {
                    transaction_uuid: request.transaction_uuid,
                    successful: true,
                    failure_reason: String::new(),
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            Message::PutDataObjects(request) => {
                log.events.push("put");
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_objects.keys().cloned());
                let mut success = BTreeMap::new();
                for (key, object) in &request.data_objects {
                    if log
                        .refuse_uuid
                        .is_some_and(|uuid| object.resource.uri.contains(uuid))
                    {
                        reply
                            .failed(
                                key.clone(),
                                &EtpError::EREQUEST_DENIED.with_detail("read-only on purpose"),
                            )
                            .unwrap();
                        continue;
                    }
                    log.objects.push((
                        object.resource.uri.clone(),
                        object.data.clone(),
                        object.format.clone(),
                    ));
                    reply.answered(key.clone()).unwrap();
                    success.insert(key.clone(), PutResponse::default());
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &PutDataObjectsResponse { success })
                    .unwrap();
                responder.send_all(out);
            }
            Message::DeleteDataObjects(request) => {
                log.events.push("delete");
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.uris.keys().cloned());
                let mut deleted_uris = BTreeMap::new();
                for (key, uri) in &request.uris {
                    let before = log.objects.len();
                    log.objects.retain(|(held, _, _)| held != uri);
                    if log.objects.len() == before {
                        reply
                            .failed(key.clone(), &EtpError::ENOT_FOUND.with_detail(uri.clone()))
                            .unwrap();
                        continue;
                    }
                    reply.answered(key.clone()).unwrap();
                    // The object and one it contained: more than was asked.
                    deleted_uris.insert(
                        key.clone(),
                        ArrayOfString {
                            values: vec![uri.clone(), format!("{uri}#contained")],
                        },
                    );
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &DeleteDataObjectsResponse { deleted_uris })
                    .unwrap();
                responder.send_all(out);
            }
            Message::GetResources(_) => {
                let resources = log
                    .objects
                    .iter()
                    .map(|(uri, _, _)| Resource {
                        uri: uri.clone(),
                        name: "held".into(),
                        ..Default::default()
                    })
                    .collect();
                let out = responder
                    .wire()
                    .respond(delivery, &GetResourcesResponse { resources })
                    .unwrap();
                responder.send(out);
            }
            Message::PutDataArrays(request) => {
                log.events.push("array");
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
            Message::GetDataspaces(_) => {
                let dataspaces = log
                    .dataspaces
                    .iter()
                    .map(|(uri, path)| EtpDataspace {
                        uri: uri.clone(),
                        path: path.clone(),
                        ..Default::default()
                    })
                    .collect();
                let out = responder
                    .wire()
                    .respond(delivery, &GetDataspacesResponse { dataspaces })
                    .unwrap();
                responder.send(out);
            }
            Message::PutDataspaces(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.dataspaces.keys().cloned());
                let mut success = BTreeMap::new();
                for (key, dataspace) in &request.dataspaces {
                    if dataspace.uri.contains("refused") {
                        reply
                            .failed(
                                key.clone(),
                                &EtpError::EREQUEST_DENIED.with_detail("no permission"),
                            )
                            .unwrap();
                        continue;
                    }
                    log.dataspaces
                        .push((dataspace.uri.clone(), dataspace.path.clone()));
                    reply.answered(key.clone()).unwrap();
                    success.insert(key.clone(), dataspace.uri.clone());
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &PutDataspacesResponse { success })
                    .unwrap();
                responder.send_all(out);
            }
            Message::DeleteDataspaces(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.uris.keys().cloned());
                let mut success = BTreeMap::new();
                for (key, uri) in &request.uris {
                    let before = log.dataspaces.len();
                    log.dataspaces.retain(|(held, _)| held != uri);
                    if log.dataspaces.len() == before {
                        reply
                            .failed(key.clone(), &EtpError::ENOT_FOUND.with_detail(uri.clone()))
                            .unwrap();
                        continue;
                    }
                    reply.answered(key.clone()).unwrap();
                    success.insert(key.clone(), uri.clone());
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &DeleteDataspacesResponse { success })
                    .unwrap();
                responder.send_all(out);
            }
            _ => return Err(Declined("not in the receiving server".into())),
        }
        Ok(Handled::Answered)
    }
}

type Store = EtpStore<MemoryTransport, ManualClock>;

/// A store of dataspace `demo` over a writing session with a server that
/// remembers what it is given.
fn writing_store(log: Log) -> (Store, Arc<Mutex<Log>>) {
    let log = Arc::new(Mutex::new(log));
    let mut offer = Offer::new("a receiving store", "0.1", Uuid([3; 16]));
    offer.formats = vec!["xml".to_string(), "json".to_string()];
    offer.data_objects = ["resqml20.*", "eml20.*", "eml23.*"]
        .iter()
        .map(|t| SupportedDataObject {
            qualified_type: t.to_string(),
            data_object_capabilities: BTreeMap::new(),
        })
        .collect();
    let handlers: Vec<Box<dyn Handler>> = [3, 4, 9, 18, 24]
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
        "the writer's tests",
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
    (EtpStore::new(client, "demo", registry()), log)
}

/// One of the model's objects, read by the registry: 0 the CRS, 1 the point
/// set, 2 the proxy.
fn typed(index: usize) -> Box<dyn DynObject> {
    let (qualified_type, uuid, _, xml) = model().remove(index);
    read_object(
        &registry(),
        ReadMode::Tolerant,
        &uri(qualified_type, uuid),
        xml.as_bytes(),
    )
    .unwrap()
    .0
}

fn crs_uri() -> String {
    uri(CRS_TYPE, CRS)
}

fn points() -> (ArrayLocation, ArrayData) {
    (
        ArrayLocation::etp(uri(POINTS_TYPE, POINTS), "/points"),
        ArrayData {
            metadata: ArrayMetadata::new(vec![2, 3], "double"),
            values: ArrayValues::F64(vec![0.0, 0.0, 0.0, 10.0, 20.0, 30.0]),
        },
    )
}

/// **The object leaves addressed into the dataspace, as XML, and the trait
/// says the URI it is now known under** - read from the server's log, not from
/// the store's answer.
#[test]
fn an_object_is_readdressed_into_the_dataspace_and_sent_as_xml() {
    let (mut store, log) = writing_store(Log::default());
    let results = block(store.put_objects(vec![typed(0)], false));
    assert_eq!(results.len(), 1);
    let put = results.into_iter().next().unwrap().unwrap();
    assert_eq!(put.uri, crs_uri());
    assert!(put.created_contained.is_empty() && put.deleted_contained.is_empty());

    let log = log.lock().unwrap();
    assert_eq!(log.objects.len(), 1);
    let (sent_uri, bytes, format) = &log.objects[0];
    assert_eq!(sent_uri, &put.uri);
    assert_eq!(format, "xml");
    let xml = String::from_utf8_lossy(bytes);
    assert!(xml.contains("LocalDepth3dCrs") && xml.contains(CRS), "{xml}");
}

/// **One result per object, in order, and a refusal is that object's own**: the
/// server refuses the point set and takes the two others.
#[test]
fn a_refused_object_fails_alone_and_the_others_go_through() {
    let (mut store, log) = writing_store(Log {
        refuse_uuid: Some(POINTS),
        ..Log::default()
    });
    let results = block(store.put_objects(vec![typed(0), typed(1), typed(2)], false));
    assert_eq!(results.len(), 3);
    assert!(results[0].is_ok(), "{:?}", results[0]);
    assert!(
        matches!(results[1], Err(Error::Remote { .. })),
        "{:?}",
        results[1]
    );
    assert!(results[2].is_ok(), "{:?}", results[2]);
    assert_eq!(log.lock().unwrap().objects.len(), 2);
}

/// **The same object twice is two upserts, each answered**: ETP keys the answer
/// by URI, and one request cannot carry the URI twice.
#[test]
fn the_same_object_twice_in_one_call_is_two_writes() {
    let (mut store, log) = writing_store(Log::default());
    let results = block(store.put_objects(vec![typed(0), typed(0)], false));
    assert!(results.iter().all(|r| r.is_ok()), "{results:?}");
    let log = log.lock().unwrap();
    assert_eq!(log.objects.len(), 2);
    assert_eq!(log.events, ["put", "put"]);
}

/// **Raw objects go as received**: the bytes untouched, the format as given -
/// and a URI of another dataspace, or naming no object, is refused before
/// anything leaves.
#[test]
fn raw_objects_are_sent_as_they_are_and_foreign_uris_are_refused() {
    let (mut store, log) = writing_store(Log::default());
    let not_xml = b"<anything at='all'/>".to_vec();
    let results = block(store.put_raw(
        vec![
            RawObject::new(crs_uri(), RawFormat::Xml, not_xml.clone()),
            RawObject::new(uri(POINTS_TYPE, POINTS), RawFormat::Json, b"{\"a\":1}".to_vec()),
            RawObject::new(
                format!("eml:///dataspace('other')/{CRS_TYPE}({CRS})"),
                RawFormat::Xml,
                not_xml.clone(),
            ),
            RawObject::new(DATASPACE, RawFormat::Xml, not_xml.clone()),
        ],
        false,
    ));
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].as_ref().unwrap().uri, crs_uri());
    assert!(results[1].is_ok(), "{:?}", results[1]);
    assert!(
        matches!(results[2], Err(Error::BadUri { .. })),
        "{:?}",
        results[2]
    );
    assert!(
        matches!(results[3], Err(Error::BadUri { .. })),
        "{:?}",
        results[3]
    );

    let log = log.lock().unwrap();
    assert_eq!(log.objects.len(), 2, "the two refused ones were not sent");
    assert_eq!(log.objects[0], (crs_uri(), not_xml, "xml".to_string()));
    assert_eq!(log.objects[1].2, "json");
    assert_eq!(log.objects[1].1, b"{\"a\":1}".to_vec());
}

/// **Deleting answers what the server actually deleted** (here more than was
/// asked), one result per URI; a URI of another dataspace or one that is not
/// canonical is refused without a request.
#[test]
fn deleting_gives_back_what_the_server_deleted_and_refuses_foreign_uris() {
    let (mut store, log) = writing_store(Log::default());
    assert!(block(store.put_objects(vec![typed(0)], false))[0].is_ok());
    let other = format!("eml:///dataspace('other')/{CRS_TYPE}({CRS})");
    let results = block(store.delete_objects(
        &[
            crs_uri(),
            other,
            "not a uri".to_string(),
            uri(POINTS_TYPE, POINTS),
        ],
        false,
    ));
    assert_eq!(results.len(), 4);
    assert_eq!(
        results[0].as_ref().unwrap(),
        &vec![crs_uri(), format!("{}#contained", crs_uri())]
    );
    assert!(
        matches!(results[1], Err(Error::BadUri { .. })),
        "{:?}",
        results[1]
    );
    assert!(
        matches!(results[2], Err(Error::BadUri { .. })),
        "{:?}",
        results[2]
    );
    // Held by nobody: the server's own not found.
    assert!(matches!(results[3], Err(Error::NotFound(_))), "{:?}", results[3]);
    let log = log.lock().unwrap();
    assert!(log.objects.is_empty());
    // One request for the two URIs that were asked.
    assert_eq!(log.events, ["put", "delete"]);
}

/// **A transaction is ended by the one that was opened**, once.
#[test]
fn a_transaction_is_ended_by_the_one_that_was_opened() {
    let (mut store, log) = writing_store(Log::default());
    assert!(store.open_transaction().is_none());
    let transaction = block(store.begin(TransactionRequest::new().with_message("a message"))).unwrap();
    assert_eq!(transaction.id, uuid_text(&Uuid([9; 16])));
    assert_eq!(store.open_transaction().as_ref(), Some(&transaction));
    // The writer's own dataspace was named - an empty list would mean all of them.
    assert_eq!(
        log.lock().unwrap().started,
        [(vec![DATASPACE.to_string()], false, "a message".to_string())]
    );

    // A second one is refused, by name, without a request.
    assert!(matches!(
        block(store.begin(TransactionRequest::new())),
        Err(Error::Transaction { .. })
    ));
    // Another transaction than the open one cannot end it, and nothing is sent.
    let stranger = Transaction::new("not-the-open-one", TransactionRequest::new());
    assert!(matches!(
        block(store.commit(&stranger)),
        Err(Error::Transaction { .. })
    ));
    assert!(matches!(
        block(store.rollback(&stranger)),
        Err(Error::Transaction { .. })
    ));
    assert_eq!(log.lock().unwrap().events, ["start"]);
    assert_eq!(store.open_transaction().as_ref(), Some(&transaction));

    block(store.commit(&transaction)).unwrap();
    assert!(store.open_transaction().is_none());
    // Ended: the same transaction cannot end again.
    assert!(matches!(
        block(store.commit(&transaction)),
        Err(Error::Transaction { .. })
    ));
    assert_eq!(log.lock().unwrap().events, ["start", "commit"]);
}

/// **A rollback ends the transaction too**, and the store can begin another.
#[test]
fn a_rolled_back_transaction_is_over() {
    let (mut store, log) = writing_store(Log::default());
    let transaction = block(store.begin(TransactionRequest::new())).unwrap();
    block(store.rollback(&transaction)).unwrap();
    assert!(store.open_transaction().is_none());
    let again = block(store.begin(TransactionRequest::new())).unwrap();
    block(store.commit(&again)).unwrap();
    assert_eq!(
        log.lock().unwrap().events,
        ["start", "rollback", "start", "commit"]
    );
}

/// **A transaction over another dataspace is not this store's to open**, and
/// the store's own, named, is.
#[test]
fn a_transaction_over_another_dataspace_is_refused_by_name() {
    let (mut store, log) = writing_store(Log::default());
    let other = TransactionRequest::new().over(["eml:///dataspace('other')"]);
    assert!(matches!(block(store.begin(other)), Err(Error::NotFound(_))));
    assert!(log.lock().unwrap().events.is_empty());
    assert!(store.open_transaction().is_none());

    let own = TransactionRequest::new().over([DATASPACE]);
    assert!(block(store.begin(own)).is_ok());
    assert_eq!(log.lock().unwrap().started[0].0, [DATASPACE.to_string()]);
}

/// **A server that will not open a transaction is a named refusal, one that has
/// none is another**: "not now" and "never here" are not the same word.
#[test]
fn a_transaction_the_server_will_not_open_is_not_open() {
    let (mut store, _) = writing_store(Log {
        start: Start::Refuses("busy"),
        ..Log::default()
    });
    match block(store.begin(TransactionRequest::new())) {
        Err(Error::Transaction { reason, .. }) => assert!(reason.contains("busy"), "{reason}"),
        other => panic!("expected a refused transaction, got {other:?}"),
    }
    assert!(store.open_transaction().is_none());

    let (mut store, _) = writing_store(Log {
        start: Start::Unsupported,
        ..Log::default()
    });
    assert!(matches!(
        block(store.begin(TransactionRequest::new())),
        Err(Error::Unsupported { .. })
    ));
    assert!(store.open_transaction().is_none());
}

/// **A refused commit says so and the transaction is over** (ETP 1.2 §18.1.1.1
/// step 8.b.ii: nothing of it is stored).
#[test]
fn a_refused_commit_is_an_error_and_ends_the_transaction() {
    let (mut store, log) = writing_store(Log {
        refuse_commit: Some("conflict"),
        ..Log::default()
    });
    let transaction = block(store.begin(TransactionRequest::new())).unwrap();
    match block(store.commit(&transaction)) {
        Err(Error::Transaction { reason, .. }) => assert!(reason.contains("conflict"), "{reason}"),
        other => panic!("expected a refused commit, got {other:?}"),
    }
    assert!(store.open_transaction().is_none());
    assert_eq!(log.lock().unwrap().events, ["start", "commit"]);
}

/// **A read-only transaction writes nothing**: objects, raw objects, deletions
/// and arrays are refused, and nothing leaves but the start.
#[test]
fn a_read_only_transaction_refuses_every_write() {
    let (mut store, log) = writing_store(Log::default());
    let transaction = block(store.begin(TransactionRequest::new().read_only())).unwrap();
    assert!(transaction.request.read_only);
    assert!(log.lock().unwrap().started[0].1, "readOnly was sent");

    let put = block(store.put_objects(vec![typed(0)], false));
    let raw = block(store.put_raw(
        vec![RawObject::new(crs_uri(), RawFormat::Xml, b"<x/>".to_vec())],
        false,
    ));
    let delete = block(store.delete_objects(&[crs_uri()], false));
    let arrays = block(store.put_arrays(&[points()]));
    assert!(matches!(put[0], Err(Error::Transaction { .. })), "{:?}", put[0]);
    assert!(matches!(raw[0], Err(Error::Transaction { .. })), "{:?}", raw[0]);
    assert!(
        matches!(delete[0], Err(Error::Transaction { .. })),
        "{:?}",
        delete[0]
    );
    assert!(
        matches!(arrays[0], Err(Error::Transaction { .. })),
        "{:?}",
        arrays[0]
    );
    assert_eq!(log.lock().unwrap().events, ["start"]);
}

/// **A write is not a read**: a listing taken before a write is not served
/// after it. The server lists what it holds.
#[test]
fn a_listing_taken_before_a_write_is_not_served_after_it() {
    let (mut store, _) = writing_store(Log::default());
    assert!(block(store.resources()).unwrap().is_empty());
    assert!(block(store.put_objects(vec![typed(0)], false))[0].is_ok());
    let listed = block(store.resources()).unwrap();
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(listed[0].uri, crs_uri());
}

/// **An array travels whole, addressed by the object and the path**; the shape
/// and the values must agree before anything leaves, one result per array.
#[test]
fn an_array_is_written_whole_and_a_shape_that_lies_is_refused() {
    let (mut store, log) = writing_store(Log::default());
    let (location, data) = points();
    let mut short = data.clone();
    short.values = ArrayValues::F64(vec![1.0; 5]);
    let elsewhere = ArrayLocation::etp(
        format!("eml:///dataspace('other')/{POINTS_TYPE}({POINTS})"),
        "/points",
    );

    let results = block(store.put_arrays(&[
        (location.clone(), data.clone()),
        (ArrayLocation::etp(uri(POINTS_TYPE, POINTS), "/lies"), short),
        (elsewhere, data.clone()),
    ]));
    assert_eq!(results.len(), 3);
    assert!(results[0].is_ok(), "{:?}", results[0]);
    assert!(
        matches!(
            results[1],
            Err(Error::ExternalData {
                reason: ExternalDataReason::ShapeDisagrees { promised: 6, held: 5 },
                ..
            })
        ),
        "{:?}",
        results[1]
    );
    assert!(
        matches!(results[2], Err(Error::BadUri { .. })),
        "{:?}",
        results[2]
    );

    let log = log.lock().unwrap();
    assert_eq!(log.arrays.len(), 1, "only the honest array left");
    let (dimensions, values) = &log.arrays[&(uri(POINTS_TYPE, POINTS), "/points".to_string())];
    assert_eq!(dimensions, &vec![2, 3]);
    assert_eq!(values, &vec![0.0, 0.0, 0.0, 10.0, 20.0, 30.0]);
}

/// **A block whose end only the array could say is refused by name, and
/// nothing leaves**: a `start` without a `count` means "to the end" for a read,
/// which only the array's dimensions know. Blocks and declarations themselves
/// are written through `blocks.rs` (tests/blocks.rs).
#[test]
fn a_block_without_its_count_is_refused_by_name() {
    let (mut store, log) = writing_store(Log::default());
    let (mut location, data) = points();
    location.start = vec![0, 0];
    let results = block(store.put_arrays(&[(location, data)]));
    assert!(
        matches!(
            results[0],
            Err(Error::ExternalData { .. }) | Err(Error::Unsupported { .. })
        ),
        "{:?}",
        results[0]
    );
    assert!(log.lock().unwrap().events.is_empty());
}

/// **Dataspaces: one result per item, the server's refusal is that item's**,
/// the ETP `path` comes from the URI, and a URI that is not a dataspace's is
/// refused without a request.
#[test]
fn dataspaces_are_listed_created_and_deleted_one_result_each() {
    let (mut store, log) = writing_store(Log {
        dataspaces: vec![(DATASPACE.to_string(), "demo".to_string())],
        ..Log::default()
    });
    let listed = block(store.list_dataspaces()).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].uri, DATASPACE);

    let results = block(store.put_dataspaces(vec![
        Dataspace::new("eml:///dataspace('a/b')", "a/b"),
        Dataspace::new("eml:///dataspace('refused')", "refused"),
        Dataspace::new(crs_uri(), "not a dataspace"),
        Dataspace::new("eml:///dataspace('c')", "c"),
    ]));
    assert_eq!(results.len(), 4);
    assert!(results[0].is_ok(), "{:?}", results[0]);
    assert!(
        matches!(results[1], Err(Error::Remote { .. })),
        "{:?}",
        results[1]
    );
    assert!(
        matches!(results[2], Err(Error::BadUri { .. })),
        "{:?}",
        results[2]
    );
    assert!(results[3].is_ok(), "{:?}", results[3]);
    assert!(
        log.lock()
            .unwrap()
            .dataspaces
            .contains(&("eml:///dataspace('a/b')".to_string(), "a/b".to_string())),
        "the path is the URI's"
    );

    let results = block(store.delete_dataspaces(&[
        "eml:///dataspace('a/b')".to_string(),
        "eml:///dataspace('unknown')".to_string(),
    ]));
    assert_eq!(results.len(), 2);
    assert!(results[0].is_ok(), "{:?}", results[0]);
    assert!(matches!(results[1], Err(Error::NotFound(_))), "{:?}", results[1]);
    let held: Vec<String> = log
        .lock()
        .unwrap()
        .dataspaces
        .iter()
        .map(|(u, _)| u.clone())
        .collect();
    assert_eq!(held, [DATASPACE, "eml:///dataspace('c')"]);
}

/// **A store is one dataspace**: `open_dataspace` gives itself for its own URI,
/// and not found for anything else - another dataspace, or an object.
#[test]
fn open_dataspace_is_this_store_or_not_found() {
    let (mut store, log) = writing_store(Log::default());
    assert!(matches!(
        store.open_dataspace("eml:///dataspace('other')"),
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        store.open_dataspace(&crs_uri()),
        Err(Error::NotFound(_))
    ));

    // The writer it gives writes into this dataspace.
    let writer = store.open_dataspace(DATASPACE).unwrap();
    let results = block(writer.put_objects(vec![typed(0)], false));
    assert_eq!(results[0].as_ref().unwrap().uri, crs_uri());
    assert_eq!(log.lock().unwrap().objects.len(), 1);
}
