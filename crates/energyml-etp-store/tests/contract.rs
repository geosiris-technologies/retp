//! The storage contract (`energyml_storage::contract`) played on `EtpStore`,
//! over a **scripted** in-process server.
//!
//! What this checks is the **translation** `EtpStore` makes between the storage
//! traits and the ETP messages: objects put, read back, upserted and deleted
//! under their full URI, one result per item and partial; transactions that only
//! their holder ends and whose rollback undoes; objects as bytes; the capabilities
//! the session announces against what the store then does (deletions kept,
//! several dataspaces, the dataspace door).
//!
//! What it does **not** check, on purpose:
//!
//! - **Discovery semantics** (`contract::discovery`) and **references and edges**
//!   (`contract::references`): a scripted server answers what its author scripted,
//!   so playing them here would verify the script. Their meaning over a real
//!   server is the triangulation of the private server (design note
//!   `deux-sources-un-contrat.md` section 5: the same package read as an EPC and
//!   served over ETP must answer the same `Found`).
//! - Arrays: `contract::array_refusals` is played, but the session announces
//!   `subarrays` and `uninitialized_arrays` (protocol 9 is agreed) so it asserts
//!   nothing here; see the report of this suite.
//!
//! The server holds the contract's model ([`contract::model`]) and keeps what it
//! is given: a put is an upsert (ETP 1.2 section 9.1.1), a delete answers the URI
//! deleted, and a transaction **stages** its writes until a commit (section 18.1.1.1
//! step 8.b) and drops them on a rollback (step 6.a). It keeps a tombstone for a
//! committed delete and lists them (`GetDeletedResources`, section 8.3), and
//! names each resource by the title of its object. It answers no discovery
//! beyond listing what it holds.
//!
//! The two findings this suite made at its first run are settled: an ETP store
//! passes `prune_contained` to the server, which knows containment
//! (`Capabilities::contained_objects`, CONT-1 then checks nothing), and a session
//! that states no `ChangeRetentionPeriod` keeps deletions for the default,
//! 86 400 s (ETP 1.2 section 9.2.3), so `deleted_resources` is asked (CAP-1).
//!
//! To make the other tests fail: make `EtpStore::begin` skip `check_open`'s
//! sibling checks, answer `Ok(vec![uri])` from `delete_objects` whatever the
//! server said, or keep a listing after a write (`forget_listing`). The test
//! `a_server_whose_rollback_keeps_what_was_written_is_caught` shows the suite
//! failing on a server that lies.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use energyml_core::registry::Registry;
use energyml_etp_store::EtpStore;
use energyml_storage::contract;
use energyml_storage::raw::RawFormat;
use energyml_storage::storage::EnergymlStorage;
use retp_avro::Role;
use retp_client::memory::{ManualClock, MemoryTransport};
use retp_client::{Client, Settings, writing_client};
use retp_proto::connection::{Connection, Declined, Handled, Handler, Responder};
use retp_proto::negotiate::{Offer, ProtocolOffer};
use retp_proto::wire::{Body, Delivery, EtpError};
use retp_types::datatypes::object::{
    DataObject, Dataspace as EtpDataspace, DeletedResource, PutResponse, Resource,
};
use retp_types::datatypes::{ArrayOfString, SupportedDataObject, Uuid};
use retp_types::dispatch::Message;
use retp_types::protocol::dataspace::{
    DeleteDataspacesResponse, GetDataspacesResponse, PutDataspacesResponse,
};
use retp_types::protocol::discovery::{GetDeletedResourcesResponse, GetResourcesResponse};
use retp_types::protocol::store::{
    DeleteDataObjectsResponse, GetDataObjectsResponse, PutDataObjectsResponse,
};
use retp_types::protocol::transaction::{
    CommitTransactionResponse, RollbackTransactionResponse, StartTransactionResponse,
};
use std::collections::BTreeMap;

const S: i64 = 1_000_000;
const T0: i64 = 1_760_000_000 * S;
const DATASPACE: &str = "eml:///dataspace('demo')";

fn block<F: std::future::Future>(future: F) -> F::Output {
    futures_executor::block_on(future)
}

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    energyml_eml_2_3::register(&mut registry);
    energyml_resqml_2_2::register(&mut registry);
    Arc::new(registry)
}

/// What the server holds: an object by URI, its bytes, the format it came in.
type Held = (String, Vec<u8>, String);

/// How the scripted server departs from the protocol, to prove the suite can fail.
#[derive(Default, Clone, Copy, PartialEq)]
enum Quirk {
    #[default]
    None,
    /// A rollback applies what the transaction wrote, as a commit would.
    RollbackCommits,
}

#[derive(Default)]
struct State {
    committed: Vec<Held>,
    /// The URIs deleted and committed, and when (the tombstones).
    tombstones: Vec<(String, i64)>,
    /// The URIs the open transaction deleted, kept until its commit.
    staged_tombstones: Vec<String>,
    /// The writes of the open transaction, over a copy of what is committed.
    staged: Option<Vec<Held>>,
    dataspaces: Vec<(String, String)>,
    quirk: Quirk,
}

impl State {
    /// What a request of the session sees and changes: the open transaction's
    /// copy, or the store.
    fn view(&mut self) -> &mut Vec<Held> {
        match self.staged.as_mut() {
            Some(staged) => staged,
            None => &mut self.committed,
        }
    }
}

fn initial(quirk: Quirk) -> State {
    State {
        committed: contract::model()
            .objects
            .iter()
            .map(|object| {
                (
                    format!("{DATASPACE}/{}({})", object.qualified_type, object.uuid),
                    object.xml.clone().into_bytes(),
                    "xml".to_string(),
                )
            })
            .collect(),
        staged: None,
        tombstones: Vec::new(),
        staged_tombstones: Vec::new(),
        dataspaces: vec![(DATASPACE.to_string(), "demo".to_string())],
        quirk,
    }
}

/// The title of an object's citation, from its XML: what a store names a
/// resource by (`Resource.name`).
fn title_of(data: &[u8]) -> String {
    let text = String::from_utf8_lossy(data);
    text.split_once("<eml:Title>")
        .and_then(|(_, rest)| rest.split_once("</eml:Title>"))
        .map(|(title, _)| title.to_string())
        .unwrap_or_default()
}

/// A store protocol that keeps what it is given.
struct Scripted {
    protocol: i32,
    state: Arc<Mutex<State>>,
}

impl Handler for Scripted {
    fn offer(&self) -> ProtocolOffer {
        ProtocolOffer::v12(self.protocol, Role::Store)
    }

    fn handle(&mut self, delivery: &Delivery, responder: &mut Responder<'_>) -> Result<Handled, Declined> {
        let Body::Message(message) = &delivery.body else {
            return Err(Declined("no private protocol".into()));
        };
        let mut state = self.state.lock().unwrap();
        match message {
            Message::StartTransaction(_) => {
                let copy = state.committed.clone();
                state.staged = Some(copy);
                let response = StartTransactionResponse {
                    transaction_uuid: Uuid([9; 16]),
                    successful: true,
                    failure_reason: String::new(),
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            Message::CommitTransaction(request) => {
                if let Some(staged) = state.staged.take() {
                    state.committed = staged;
                }
                let deleted = std::mem::take(&mut state.staged_tombstones);
                state.tombstones.extend(deleted.into_iter().map(|uri| (uri, T0)));
                let response = CommitTransactionResponse {
                    transaction_uuid: request.transaction_uuid,
                    successful: true,
                    failure_reason: String::new(),
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            Message::RollbackTransaction(request) => {
                state.staged_tombstones.clear();
                if let (Some(staged), Quirk::RollbackCommits) = (state.staged.take(), state.quirk) {
                    state.committed = staged;
                }
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
                    let uri = object.resource.uri.clone();
                    let held = state.view();
                    held.retain(|(held_uri, _, _)| held_uri != &uri);
                    held.push((uri, object.data.clone(), object.format.clone()));
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
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.uris.keys().cloned());
                let mut deleted_uris = BTreeMap::new();
                for (key, uri) in &request.uris {
                    let held = state.view();
                    let before = held.len();
                    held.retain(|(held_uri, _, _)| held_uri != uri);
                    if held.len() == before {
                        reply
                            .failed(key.clone(), &EtpError::ENOT_FOUND.with_detail(uri.clone()))
                            .unwrap();
                        continue;
                    }
                    reply.answered(key.clone()).unwrap();
                    if state.staged.is_some() {
                        state.staged_tombstones.push(uri.clone());
                    } else {
                        state.tombstones.push((uri.clone(), T0));
                    }
                    deleted_uris.insert(
                        key.clone(),
                        ArrayOfString {
                            values: vec![uri.clone()],
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
                let resources = state
                    .view()
                    .iter()
                    .map(|(uri, data, _)| Resource {
                        uri: uri.clone(),
                        name: title_of(data),
                        ..Default::default()
                    })
                    .collect();
                let out = responder
                    .wire()
                    .respond(delivery, &GetResourcesResponse { resources })
                    .unwrap();
                responder.send(out);
            }
            Message::GetDeletedResources(_) => {
                let deleted_resources = state
                    .tombstones
                    .iter()
                    .map(|(uri, deleted_time)| DeletedResource {
                        uri: uri.clone(),
                        deleted_time: *deleted_time,
                        ..Default::default()
                    })
                    .collect();
                let out = responder
                    .wire()
                    .respond(delivery, &GetDeletedResourcesResponse { deleted_resources })
                    .unwrap();
                responder.send(out);
            }
            Message::GetDataObjects(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.uris.keys().cloned());
                let mut found = BTreeMap::new();
                for (key, uri) in &request.uris {
                    match state.view().iter().find(|(held_uri, _, _)| held_uri == uri) {
                        Some((held_uri, data, format)) => {
                            reply.answered(key.clone()).unwrap();
                            found.insert(
                                key.clone(),
                                DataObject {
                                    resource: Resource {
                                        uri: held_uri.clone(),
                                        ..Default::default()
                                    },
                                    data: data.clone(),
                                    format: format.clone(),
                                    ..Default::default()
                                },
                            );
                        }
                        None => reply
                            .failed(key.clone(), &EtpError::ENOT_FOUND.with_detail(uri.clone()))
                            .unwrap(),
                    }
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &GetDataObjectsResponse { data_objects: found })
                    .unwrap();
                responder.send_all(out);
            }
            Message::GetDataspaces(_) => {
                let dataspaces = state
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
                    state
                        .dataspaces
                        .retain(|(held_uri, _)| held_uri != &dataspace.uri);
                    state
                        .dataspaces
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
                    let before = state.dataspaces.len();
                    state.dataspaces.retain(|(held_uri, _)| held_uri != uri);
                    if state.dataspaces.len() == before {
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
            _ => return Err(Declined("not in the scripted server".into())),
        }
        Ok(Handled::Answered)
    }
}

type Store = EtpStore<MemoryTransport, ManualClock>;

/// A fresh store of dataspace `demo` over a writing session with a server that
/// holds the contract's model.
fn store_with(quirk: Quirk) -> Store {
    let state = Arc::new(Mutex::new(initial(quirk)));
    let types = ["resqml22.*", "eml23.*"];
    let mut offer = Offer::new("a contract server", "0.1", Uuid([3; 16]));
    // XML only: JSON is TODO(verify) in the storage contract, so the session
    // agrees one format, and the other must be refused by name.
    offer.formats = vec!["xml".to_string()];
    offer.data_objects = types
        .iter()
        .map(|t| SupportedDataObject {
            qualified_type: t.to_string(),
            data_object_capabilities: BTreeMap::new(),
        })
        .collect();
    let handlers: Vec<Box<dyn Handler>> = [3, 4, 9, 18, 24]
        .iter()
        .map(|protocol| {
            Box::new(Scripted {
                protocol: *protocol,
                state: Arc::clone(&state),
            }) as Box<dyn Handler>
        })
        .collect();
    let server = Connection::server(offer, handlers, None, Uuid([0xBC; 16]), T0).unwrap();
    let clock = ManualClock::new(T0);
    let (offer, handlers) = writing_client(
        "the contract's tests",
        "0.1",
        Uuid([4; 16]),
        &types.map(|t| t.to_string()),
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
    EtpStore::new(client, "demo", registry())
}

fn make() -> Store {
    store_with(Quirk::None)
}

/// The groups assume what the session announces; if this stops being true they
/// run in another branch and prove less, so say so.
#[test]
fn the_scripted_session_announces_what_the_groups_assume() {
    let announced = make().capabilities();
    assert!(announced.writes, "{announced:?}");
    assert!(announced.transactions, "{announced:?}");
    assert!(announced.several_dataspaces, "{announced:?}");
    assert_eq!(announced.raw_formats, vec![RawFormat::Xml], "{announced:?}");
    // Unstated by the session: the default of ETP 1.2 section 9.2.3.
    assert_eq!(announced.deletions_kept_for, Some(86_400), "{announced:?}");
    assert!(announced.contained_objects, "{announced:?}");
    assert!(announced.prune_contained, "{announced:?}");
}

#[test]
fn objects() {
    block(contract::objects(&make, &registry()));
}

#[test]
fn containment_agrees_with_the_capability() {
    block(contract::containment(&make, &registry()));
}

#[test]
fn transactions() {
    block(contract::transactions(&make, &registry()));
}

#[test]
fn raw_objects() {
    block(contract::raw_objects(&make));
}

/// Expected to fail today (CAP-1): see the module doc.
#[test]
fn deletions_agree_with_the_capability() {
    block(contract::deletions(&make));
}

#[test]
fn creating_and_deleting_dataspaces_agrees_with_the_capability() {
    block(contract::dataspace_creation(&make));
}

#[test]
fn the_dataspace_door_reaches_the_stores_own_dataspace() {
    block(contract::dataspace_door(&make, &registry()));
}

#[test]
fn array_refusals_agree_with_the_capabilities() {
    block(contract::array_refusals(&make));
}

/// A server whose rollback keeps what the transaction wrote is not a server the
/// contract accepts: the suite must say which rule broke.
#[test]
fn a_server_whose_rollback_keeps_what_was_written_is_caught() {
    let lying = || store_with(Quirk::RollbackCommits);
    let registry = registry();
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        block(contract::transactions(&lying, &registry));
    }));
    let payload = outcome.expect_err("the transactions group should have broken a rule");
    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_default();
    assert!(
        message.contains("contract rule TX-3"),
        "the rollback rule should have named the departure: {message}"
    );
}
