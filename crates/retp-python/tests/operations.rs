//! The operations beyond the first scope, on a client whose other end is a
//! server connection in the same process (`retp_client::memory`): what the
//! bridge makes of each answer, with no socket and no interpreter.
//!
//! The server answers as ETP v1.2 says a store answers: a keyed request gets
//! one answer per key, positive or an error for that key (section 3.7.3); a
//! transaction is answered with `successful` and a `failureReason`, not an
//! exception (chapter 18). The values it answers with are made up here; what
//! is checked is where each lands in the bridge's result.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use energyml_etp_store::pack::Wanted;
use retp_avro::Role;
use retp_client::memory::{ManualClock, MemoryTransport};
use retp_client::{Client, Settings, Transaction, writing_client};
use retp_proto::connection::{Connection, Declined, Handled, Handler, Responder};
use retp_proto::negotiate::{Offer, ProtocolOffer};
use retp_proto::wire::{Body, Delivery, EtpError};
use retp_python::session::{
    Kind, channels, commit, create_dataspaces, dataspaces_to_put, delete_objects, open_transaction,
    pack_dataspace, ranges, ranges_from_json, remove_dataspaces, rollback, supported_types,
};
use retp_types::datatypes::channel_data::{ChannelMetadataRecord, DataItem};
use retp_types::datatypes::object::{RelationshipKind, SupportedType};
use retp_types::datatypes::{
    ArrayOfString, DataValue, DataValueItem, IndexValue, IndexValueItem, SupportedDataObject, Uuid,
};
use retp_types::dispatch::Message;
use retp_types::protocol::channel_subscribe::{GetChannelMetadataResponse, GetRangesResponse};
use retp_types::protocol::dataspace::{DeleteDataspacesResponse, PutDataspacesResponse};
use retp_types::protocol::discovery::GetResourcesResponse;
use retp_types::protocol::store::DeleteDataObjectsResponse;
use retp_types::protocol::supported_types::GetSupportedTypesResponse;
use retp_types::protocol::transaction::{
    CommitTransactionResponse, RollbackTransactionResponse, StartTransactionResponse,
};

const T0: i64 = 1_760_000_000_000_000;

/// The object URIs the server refuses to delete, the dataspaces it refuses to
/// put or delete, the channels it does not know: each contains this word.
const REFUSED: &str = "refused";
/// The uuid the server gives the transactions it opens.
const OPENED: Uuid = Uuid([9; 16]);
/// A transaction the server answers about **another** one when it is ended.
const CONFUSED: Uuid = Uuid([4; 16]);

/// What the server was sent, for the tests that check what left.
#[derive(Default)]
struct Seen {
    prune_contained: Option<bool>,
    dataspaces: Vec<(String, String, Vec<String>)>,
    transaction_message: Option<String>,
}

type Shared = Arc<Mutex<Seen>>;

fn seen(shared: &Shared) -> std::sync::MutexGuard<'_, Seen> {
    shared.lock().unwrap_or_else(|p| p.into_inner())
}

/// One protocol of the store.
struct Store {
    protocol: i32,
    seen: Shared,
}

impl Handler for Store {
    fn offer(&self) -> ProtocolOffer {
        ProtocolOffer::v12(self.protocol, Role::Store)
    }

    fn handle(&mut self, delivery: &Delivery, responder: &mut Responder<'_>) -> Result<Handled, Declined> {
        let Body::Message(message) = &delivery.body else {
            return Err(Declined("no private protocol".into()));
        };
        match message {
            Message::DeleteDataObjects(request) => {
                seen(&self.seen).prune_contained = Some(request.prune_contained_objects);
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.uris.keys().cloned());
                let mut deleted = BTreeMap::new();
                for (key, uri) in &request.uris {
                    if uri.contains(REFUSED) {
                        reply
                            .failed(key.clone(), &EtpError::ENOT_FOUND.with_detail(uri.clone()))
                            .unwrap();
                    } else {
                        reply.answered(key.clone()).unwrap();
                        deleted.insert(
                            key.clone(),
                            ArrayOfString {
                                values: vec![uri.clone(), format!("{uri}/contained")],
                            },
                        );
                    }
                }
                let out = responder
                    .wire()
                    .last(
                        &mut reply,
                        &DeleteDataObjectsResponse {
                            deleted_uris: deleted,
                        },
                    )
                    .unwrap();
                responder.send_all(out);
            }
            Message::PutDataspaces(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.dataspaces.keys().cloned());
                let mut success = BTreeMap::new();
                for (key, space) in &request.dataspaces {
                    seen(&self.seen).dataspaces.push((
                        space.uri.clone(),
                        space.path.clone(),
                        space
                            .custom_data
                            .iter()
                            .map(|(k, v)| format!("{k}={:?}", v.item))
                            .collect(),
                    ));
                    if space.uri.contains(REFUSED) {
                        reply
                            .failed(key.clone(), &EtpError::EREQUEST_DENIED.with_detail("not yours"))
                            .unwrap();
                    } else {
                        reply.answered(key.clone()).unwrap();
                        success.insert(key.clone(), String::new());
                    }
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
                    if uri.contains(REFUSED) {
                        reply
                            .failed(key.clone(), &EtpError::ENOT_FOUND.with_detail(uri.clone()))
                            .unwrap();
                    } else {
                        reply.answered(key.clone()).unwrap();
                        success.insert(key.clone(), String::new());
                    }
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &DeleteDataspacesResponse { success })
                    .unwrap();
                responder.send_all(out);
            }
            Message::StartTransaction(request) => {
                seen(&self.seen).transaction_message = Some(request.message.clone());
                let refused = request.message == REFUSED;
                let response = StartTransactionResponse {
                    transaction_uuid: OPENED,
                    successful: !refused,
                    failure_reason: if refused {
                        "the dataspace is locked".into()
                    } else {
                        String::new()
                    },
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            Message::CommitTransaction(request) => {
                let about = if request.transaction_uuid == CONFUSED {
                    Uuid([3; 16])
                } else {
                    request.transaction_uuid
                };
                let response = CommitTransactionResponse {
                    transaction_uuid: about,
                    successful: true,
                    failure_reason: String::new(),
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            Message::RollbackTransaction(request) => {
                // Refused, and without a reason: what the bridge says must
                // still name the refusal.
                let response = RollbackTransactionResponse {
                    transaction_uuid: request.transaction_uuid,
                    successful: false,
                    failure_reason: String::new(),
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            Message::GetSupportedTypes(request) => {
                if request.uri.contains(REFUSED) {
                    let out = responder
                        .wire()
                        .refuse(delivery, &EtpError::EREQUEST_DENIED.with_detail("not yours"));
                    responder.send(out);
                } else {
                    let response = GetSupportedTypesResponse {
                        supported_types: vec![
                            SupportedType {
                                data_object_type: "resqml20.obj_LocalDepth3dCrs".into(),
                                object_count: request.count_objects.then_some(2),
                                relationship_kind: RelationshipKind::Primary,
                            },
                            SupportedType {
                                data_object_type: "resqml20.obj_PointSetRepresentation".into(),
                                object_count: request.count_objects.then_some(5),
                                relationship_kind: RelationshipKind::Both,
                            },
                        ],
                    };
                    let out = responder.wire().respond(delivery, &response).unwrap();
                    responder.send(out);
                }
            }
            Message::GetChannelMetadata(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.uris.keys().cloned());
                let mut metadata = BTreeMap::new();
                for (key, uri) in &request.uris {
                    if uri.contains(REFUSED) {
                        reply
                            .failed(key.clone(), &EtpError::ENOT_FOUND.with_detail(uri.clone()))
                            .unwrap();
                    } else {
                        reply.answered(key.clone()).unwrap();
                        metadata.insert(
                            key.clone(),
                            ChannelMetadataRecord {
                                uri: uri.clone(),
                                id: 7,
                                channel_name: "GR".into(),
                                uom: "gAPI".into(),
                                ..Default::default()
                            },
                        );
                    }
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &GetChannelMetadataResponse { metadata })
                    .unwrap();
                responder.send_all(out);
            }
            Message::GetRanges(request) => {
                let data = request
                    .channel_ranges
                    .iter()
                    .flat_map(|range| range.channel_ids.iter())
                    .map(|id| DataItem {
                        channel_id: *id,
                        indexes: vec![IndexValue {
                            item: IndexValueItem::Long(100),
                        }],
                        value: DataValue {
                            item: DataValueItem::Double(42.5),
                        },
                        value_attributes: Vec::new(),
                    })
                    .collect();
                let out = responder
                    .wire()
                    .respond(delivery, &GetRangesResponse { data })
                    .unwrap();
                responder.send(out);
            }
            Message::GetResources(_) => {
                // An empty dataspace: a package of nothing is still a package.
                let out = responder
                    .wire()
                    .respond(
                        delivery,
                        &GetResourcesResponse {
                            resources: Vec::new(),
                        },
                    )
                    .unwrap();
                responder.send(out);
            }
            _ => return Err(Declined("not in this test server".into())),
        }
        Ok(Handled::Answered)
    }
}

/// A writing client, open on a store that speaks Discovery (3), Store (4),
/// Transaction (18), ChannelSubscribe (21), Dataspace (24) and SupportedTypes
/// (25).
fn session() -> (Client<MemoryTransport, ManualClock>, Shared) {
    let shared = Shared::default();
    let mut offer = Offer::new("the bridge's test store", "0.1", Uuid([2; 16]));
    offer.data_objects = ["resqml20.*", "eml20.*"]
        .iter()
        .map(|t| SupportedDataObject {
            qualified_type: t.to_string(),
            data_object_capabilities: BTreeMap::new(),
        })
        .collect();
    let handlers: Vec<Box<dyn Handler>> = [3, 4, 18, 21, 24, 25]
        .iter()
        .map(|p| {
            Box::new(Store {
                protocol: *p,
                seen: shared.clone(),
            }) as Box<dyn Handler>
        })
        .collect();
    let server = Connection::server(offer, handlers, None, Uuid([0xAB; 16]), T0).unwrap();
    let clock = ManualClock::new(T0);
    let (offer, handlers) = writing_client(
        "the bridge's tests",
        "0.1",
        Uuid([1; 16]),
        &["resqml20.*".to_string(), "eml20.*".to_string()],
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
    (client, shared)
}

fn texts(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

const KEPT: &str =
    "eml:///dataspace('demo')/resqml20.obj_LocalDepth3dCrs(aa000000-0000-4000-8000-000000000001)";
const GONE: &str =
    "eml:///dataspace('demo')/resqml20.obj_LocalDepth3dCrs(refused0-0000-4000-8000-000000000002)";

// ---- deleting objects ------------------------------------------------------

#[test]
fn deleted_objects_carry_what_the_store_deleted_and_a_refusal_carries_its_code() {
    let (mut client, shared) = session();
    let outcome = delete_objects(&mut client, &texts(&[GONE, KEPT]), true).expect("answered");
    assert_eq!(
        outcome.done,
        vec![(
            KEPT.to_string(),
            vec![KEPT.to_string(), format!("{KEPT}/contained")]
        )]
    );
    assert_eq!(outcome.failed.len(), 1, "{:?}", outcome.failed);
    let (uri, failure) = &outcome.failed[0];
    assert_eq!(uri, GONE);
    assert_eq!(failure.kind, Kind::Server);
    assert_eq!(failure.code, Some(EtpError::ENOT_FOUND.code()));
    assert_eq!(
        seen(&shared).prune_contained,
        Some(true),
        "prune_contained reaches the store"
    );
}

#[test]
fn deleting_nothing_or_the_same_uri_twice_is_refused_before_anything_leaves() {
    let (mut client, shared) = session();
    assert_eq!(
        delete_objects(&mut client, &[], false).expect_err("nothing").kind,
        Kind::BadArgument
    );
    let twice = delete_objects(&mut client, &texts(&[KEPT, KEPT]), false).expect_err("twice");
    assert_eq!(twice.kind, Kind::BadArgument);
    assert!(twice.message.contains("twice"), "{}", twice.message);
    assert_eq!(seen(&shared).prune_contained, None, "nothing was sent");
}

// ---- dataspaces ------------------------------------------------------------

#[test]
fn a_dataspace_is_put_with_its_uri_its_path_and_its_custom_data() {
    let (mut client, shared) = session();
    let custom = vec![
        ("owners".to_string(), "a@example.org,b@example.org".to_string()),
        ("legal".to_string(), "public".to_string()),
    ];
    let outcome =
        create_dataspaces(&mut client, &texts(&["carl/teapot", "refused"]), &custom).expect("answered");
    assert_eq!(
        outcome.done,
        vec![("eml:///dataspace('carl/teapot')".to_string(), String::new())]
    );
    assert_eq!(outcome.failed.len(), 1);
    assert_eq!(outcome.failed[0].0, "eml:///dataspace('refused')");
    assert_eq!(outcome.failed[0].1.code, Some(EtpError::EREQUEST_DENIED.code()));
    let sent = &seen(&shared).dataspaces;
    let (uri, path, custom_data) = &sent[0];
    assert_eq!(uri, "eml:///dataspace('carl/teapot')");
    assert_eq!(path, "carl/teapot", "the path is the one the URI names");
    // `custom_data_value`: a comma makes a list of strings, anything else one.
    assert!(
        custom_data.iter().any(|v| v.starts_with("owners=ArrayOfString")),
        "{custom_data:?}"
    );
    assert!(
        custom_data.iter().any(|v| v.starts_with("legal=String")),
        "{custom_data:?}"
    );
}

#[test]
fn an_object_uri_is_not_a_dataspace_to_put() {
    let refusal = dataspaces_to_put(&texts(&[KEPT]), &[]).expect_err("an object");
    assert_eq!(refusal.kind, Kind::BadArgument);
    assert!(refusal.message.contains("names an object"), "{}", refusal.message);
    assert_eq!(
        dataspaces_to_put(&[], &[]).expect_err("none").kind,
        Kind::BadArgument
    );
}

#[test]
fn deleted_dataspaces_answer_each_for_itself() {
    let (mut client, _) = session();
    let outcome =
        remove_dataspaces(&mut client, &texts(&["eml:///dataspace('refused')", "demo"])).expect("answered");
    assert_eq!(
        outcome.done,
        vec![("eml:///dataspace('demo')".to_string(), String::new())]
    );
    assert_eq!(outcome.failed[0].0, "eml:///dataspace('refused')");
    assert_eq!(outcome.failed[0].1.code, Some(EtpError::ENOT_FOUND.code()));
}

// ---- transactions ----------------------------------------------------------

#[test]
fn a_transaction_opens_and_commits() {
    let (mut client, shared) = session();
    let transaction = open_transaction(&mut client, &texts(&["demo"]), false, "an import").expect("opened");
    assert_eq!(transaction.uuid, OPENED);
    assert_eq!(transaction.dataspaces, texts(&["eml:///dataspace('demo')"]));
    assert_eq!(seen(&shared).transaction_message.as_deref(), Some("an import"));
    commit(&mut client, &transaction).expect("committed");
}

#[test]
fn a_store_that_will_not_open_a_transaction_says_why() {
    let (mut client, _) = session();
    let refusal = open_transaction(&mut client, &texts(&["demo"]), false, REFUSED).expect_err("refused");
    assert_eq!(refusal.kind, Kind::Server);
    assert!(refusal.message.contains("locked"), "{}", refusal.message);
}

#[test]
fn a_commit_answered_about_another_transaction_is_not_a_commit() {
    let (mut client, _) = session();
    let elsewhere = Transaction {
        uuid: CONFUSED,
        dataspaces: texts(&["eml:///dataspace('demo')"]),
        read_only: false,
    };
    let refusal = commit(&mut client, &elsewhere).expect_err("not this transaction");
    assert_eq!(refusal.kind, Kind::Server);
    assert!(
        refusal.message.contains("not in the store"),
        "a refused commit must say the writes are not there: {}",
        refusal.message
    );
}

#[test]
fn a_refused_rollback_is_said_even_without_a_reason() {
    let (mut client, _) = session();
    let transaction = open_transaction(&mut client, &[], false, "").expect("opened");
    let refusal = rollback(&mut client, &transaction).expect_err("refused");
    assert_eq!(refusal.kind, Kind::Server);
    assert!(refusal.message.contains("no reason"), "{}", refusal.message);
    assert!(
        refusal.message.contains("may still be there"),
        "{}",
        refusal.message
    );
}

// ---- supported types -------------------------------------------------------

#[test]
fn supported_types_come_with_their_counts() {
    let (mut client, _) = session();
    let rows = supported_types(&mut client, "demo", "targets", true, false).expect("listed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].data_object_type, "resqml20.obj_LocalDepth3dCrs");
    assert_eq!(rows[0].object_count, Some(2));
    assert_eq!(rows[0].relationship_kind, "Primary");
    assert_eq!(rows[1].object_count, Some(5));
}

#[test]
fn a_supported_type_list_cut_short_is_refused_and_a_scope_other_than_sources_or_targets_is_not_sent() {
    let (mut client, _) = session();
    let cut = supported_types(&mut client, "refused", "targets", false, false).expect_err("cut short");
    assert_eq!(cut.kind, Kind::Server);
    assert_eq!(cut.code, Some(EtpError::EREQUEST_DENIED.code()));
    let wrong = supported_types(&mut client, "demo", "self", false, false).expect_err("self");
    assert_eq!(wrong.kind, Kind::NotSent, "{}", wrong.message);
    let unknown = supported_types(&mut client, "demo", "everything", false, false).expect_err("no scope");
    assert_eq!(unknown.kind, Kind::BadArgument);
}

// ---- channels --------------------------------------------------------------

const LOG: &str = "eml:///dataspace('demo')/witsml20.Log(cc000000-0000-4000-8000-000000000003)";
const NO_LOG: &str = "eml:///dataspace('demo')/witsml20.Log(refused0-0000-4000-8000-000000000004)";

#[test]
fn channel_records_cross_in_the_avro_json_encoding() {
    let (mut client, _) = session();
    let records = channels(&mut client, &texts(&[NO_LOG, LOG])).expect("answered");
    assert_eq!(records.found.len(), 1, "{:?}", records.failed);
    let (uri, json) = &records.found[0];
    assert_eq!(uri, LOG);
    // The record's own encoding read back is the record: the text is the
    // library's, not a second spelling of it.
    let back = <ChannelMetadataRecord as retp_avro::Avro>::from_json_str(json).expect("its encoding");
    assert_eq!(back.id, 7);
    assert_eq!(back.channel_name, "GR");
    assert!(json.contains("\"channelName\""), "{json}");
    assert_eq!(records.failed[0].0, NO_LOG);
    assert_eq!(records.failed[0].1.code, Some(EtpError::ENOT_FOUND.code()));
}

#[test]
fn ranges_go_out_from_their_json_and_their_items_come_back_in_it() {
    let (mut client, _) = session();
    let range = r#"{"channelIds": [7, 8], "interval": {"startIndex": {"item": {"long": 0}}, "endIndex": {"item": {"long": 200}}, "uom": "m", "depthDatum": ""}, "secondaryIntervals": []}"#;
    let items = ranges(&mut client, &[range.to_string()]).expect("answered");
    assert_eq!(items.len(), 2, "one item per channel asked: {items:?}");
    let first = <DataItem as retp_avro::Avro>::from_json_str(&items[0]).expect("its encoding");
    assert_eq!(first.channel_id, 7);
    assert_eq!(first.value.item, DataValueItem::Double(42.5));
}

#[test]
fn a_range_that_is_not_a_channel_range_is_refused_and_named() {
    let refusal = ranges_from_json(&["{\"channelIds\": [1]}".to_string()]).expect_err("no interval");
    assert_eq!(refusal.kind, Kind::BadArgument);
    assert!(refusal.message.contains("range 0"), "{}", refusal.message);
    assert_eq!(ranges_from_json(&[]).expect_err("none").kind, Kind::BadArgument);
}

// ---- a dataspace as an EPC -------------------------------------------------

#[test]
fn an_empty_dataspace_packs_into_a_package_and_what_it_lacks_is_said() {
    let (mut client, _) = session();
    let registry = energyml_schemas::all();
    let wanted = Wanted {
        uris: texts(&[KEPT]),
        arrays: false,
        ..Wanted::default()
    };
    let packed = pack_dataspace(&mut client, &registry, "demo", &wanted).expect("packed");
    assert_eq!(packed.objects, 0);
    assert!(packed.epc.starts_with(b"PK"), "an EPC is a zip archive");
    assert!(
        packed
            .left_out
            .iter()
            .any(|said| said.contains(KEPT) && said.contains("does not hold")),
        "an object asked for and not there is said: {:?}",
        packed.left_out
    );
}
