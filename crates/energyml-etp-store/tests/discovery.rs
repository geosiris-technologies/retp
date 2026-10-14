//! The reading additions of the mother trait over ETP: what a context, the
//! edges, the raw bytes, the deletions and the capabilities ask of a server,
//! and how its records come back.
//!
//! Two halves.
//!
//! **Pure conversions** (`energyml_etp_store::discovery`): a fabricated ETP
//! record in, the storage record out, no session. Each assertion names a field,
//! so a conversion that drops one fails on it. Breaking them on purpose: make
//! `store_time` return `Some(timestamp)` for 0, make `edge_kind_from_etp` map
//! `Both` to `Primary`, drop `preferred_subarray_dimensions` in
//! `array_metadata_from_etp`, or send `Navigable::Both` as `Primary` in
//! `relationship_kind`.
//!
//! **Native requests**, through [`EnergymlStorage`] and a scripted server that
//! records what it was asked. The trait is the door under test: a store whose
//! `impl EnergymlStorage` does not delegate to the helpers of `discovery.rs`
//! answers by the trait's defaults, which send no context and refuse the rest,
//! and these tests fail. Breaking them on purpose: leave `resources_in` out of
//! the impl, build `includeEdges` as `false` in `native_resources_in`, or let
//! `deleted` answer `Ok(vec![])` where the server ended with an exception.
//!
//! The scripted server says what the test wants it to say; what the *real*
//! servers say is the business of the private campaign, not of this suite.

mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use energyml_etp_store::EtpStore;
use energyml_etp_store::discovery::{
    array_metadata_from_etp, context_info, dataspace_from_etp, edge_from_etp, edge_kind_from_etp,
    identifier_of, logical_symbol, relationship_kind, resources_request, scope_kind, store_time,
    summary_from_resource,
};
use energyml_storage::array::ArrayLocation;
use energyml_storage::context::{Context, DeletedFilter, Navigable, Scope};
use energyml_storage::error::Error;
use energyml_storage::graph::EdgeKind;
use energyml_storage::raw::RawFormat;
use energyml_storage::resource::ActiveStatus;
use energyml_storage::storage::EnergymlStorage;
use retp_avro::Role;
use retp_client::memory::{ManualClock, MemoryTransport};
use retp_client::{Client, Settings, reading_client, writing_client};
use retp_proto::capabilities::{Value, names};
use retp_proto::connection::{Connection, Declined, Handled, Handler, Responder};
use retp_proto::negotiate::{Offer, ProtocolOffer};
use retp_proto::wire::{Body, Delivery, EtpError};
use retp_types::datatypes::data_array_types::DataArrayMetadata;
use retp_types::datatypes::object::{
    ActiveStatusKind, ContextScopeKind, DataObject, Dataspace as EtpDataspace, DeletedResource as EtpDeleted,
    Edge as EtpEdge, RelationshipKind, Resource,
};
use retp_types::datatypes::{AnyArrayType, AnyLogicalArrayType, SupportedDataObject, Uuid};
use retp_types::dispatch::Message;
use retp_types::protocol::discovery::{
    GetDeletedResources, GetDeletedResourcesResponse, GetResources, GetResourcesEdgesResponse,
    GetResourcesResponse,
};
use retp_types::protocol::store::GetDataObjectsResponse;

use common::*;

const POINT_SET: &str = "resqml20.obj_PointSetRepresentation";
const CRS_TYPE: &str = "resqml20.obj_LocalDepth3dCrs";
const OUTSIDE: &str = "ee000000-0000-4000-8000-000000000009";
const MISSING: &str = "ff000000-0000-4000-8000-0000000000ff";

fn points_uri() -> String {
    uri(POINT_SET, POINTS)
}

fn crs_uri() -> String {
    uri(CRS_TYPE, CRS)
}

fn outside_uri() -> String {
    uri("resqml20.obj_Outside", OUTSIDE)
}

// ---------------------------------------------------------------------------
// Pure conversions
// ---------------------------------------------------------------------------

#[test]
fn a_resource_keeps_its_times_its_status_and_its_alternate_uris() {
    let resource = Resource {
        uri: points_uri(),
        name: "a point set".into(),
        alternate_uris: vec!["eml:///alternate".into()],
        store_created: T0,
        store_last_write: T0 + 5 * S,
        active_status: ActiveStatusKind::Inactive,
        ..Default::default()
    };
    let summary = summary_from_resource(
        &resource,
        "application/x-resqml+xml;version=2.0;type=obj_PointSetRepresentation",
    )
    .expect("a data object URI");
    assert_eq!(summary.uri, points_uri());
    assert_eq!(summary.uuid, POINTS);
    assert_eq!(summary.qualified_type, POINT_SET);
    assert_eq!(
        summary.content_type,
        "application/x-resqml+xml;version=2.0;type=obj_PointSetRepresentation"
    );
    assert_eq!(summary.alternate_uris, ["eml:///alternate"]);
    assert_eq!(summary.store_created, Some(T0));
    assert_eq!(summary.store_last_write, Some(T0 + 5 * S));
    assert_eq!(summary.active_status, Some(ActiveStatus::Inactive));

    let active = Resource {
        active_status: ActiveStatusKind::Active,
        ..resource
    };
    assert_eq!(
        summary_from_resource(&active, "").unwrap().active_status,
        Some(ActiveStatus::Active)
    );
}

#[test]
fn a_time_of_zero_is_not_stated_and_a_dataspace_is_not_an_object() {
    assert_eq!(store_time(0), None);
    assert_eq!(store_time(T0), Some(T0));
    let unstated = Resource {
        uri: crs_uri(),
        ..Default::default()
    };
    let summary = summary_from_resource(&unstated, "").unwrap();
    assert_eq!((summary.store_created, summary.store_last_write), (None, None));

    let dataspace = Resource {
        uri: DATASPACE.into(),
        ..Default::default()
    };
    assert!(summary_from_resource(&dataspace, "").is_none());
}

#[test]
fn an_edge_has_the_kind_the_server_stated_and_never_both() {
    let edge = |kind| EtpEdge {
        source_uri: points_uri(),
        target_uri: crs_uri(),
        relationship_kind: kind,
        ..Default::default()
    };
    let primary = edge_from_etp(&edge(RelationshipKind::Primary));
    assert_eq!(
        (primary.from.as_str(), primary.to.as_str()),
        (points_uri().as_str(), crs_uri().as_str())
    );
    assert_eq!(primary.kind, Some(EdgeKind::Primary));
    assert_eq!(
        edge_from_etp(&edge(RelationshipKind::Secondary)).kind,
        Some(EdgeKind::Secondary)
    );
    // "For edges, RelationshipKind CANNOT be Both" (ETP 1.2 23.34.13).
    assert_eq!(edge_kind_from_etp(RelationshipKind::Both), None);
    assert_eq!(edge_from_etp(&edge(RelationshipKind::Both)).kind, None);
}

#[test]
fn a_dataspace_keeps_its_store_times() {
    let dataspace = dataspace_from_etp(&EtpDataspace {
        uri: DATASPACE.into(),
        path: "demo".into(),
        store_created: T0,
        store_last_write: 0,
        ..Default::default()
    });
    assert_eq!(
        (dataspace.uri.as_str(), dataspace.name.as_str()),
        (DATASPACE, "demo")
    );
    assert_eq!(dataspace.store_created, Some(T0));
    assert_eq!(dataspace.store_last_write, None);
}

#[test]
fn array_metadata_carries_the_logical_type_the_chunking_and_the_times() {
    let meta = DataArrayMetadata {
        dimensions: vec![2, 3],
        preferred_subarray_dimensions: vec![1, 3],
        transport_array_type: AnyArrayType::ArrayOfDouble,
        logical_array_type: AnyLogicalArrayType::ArrayOfDouble64Le,
        store_created: T0,
        store_last_write: T0 + S,
        ..Default::default()
    };
    let out = array_metadata_from_etp(&meta, AnyLogicalArrayType::ArrayOfDouble64Le);
    assert_eq!(out.dimensions, [2, 3]);
    assert_eq!(out.element_type, "double");
    assert_eq!(out.logical_type.as_deref(), Some("arrayOfDouble64LE"));
    assert_eq!(out.preferred_subarray_dimensions, [1, 3]);
    assert_eq!(
        (out.store_created, out.store_last_write),
        (Some(T0), Some(T0 + S))
    );

    // A logical type corrected by the reader gives the element type, and the
    // server's own word stays visible.
    let boolean = DataArrayMetadata {
        logical_array_type: AnyLogicalArrayType::ArrayOfBoolean,
        ..meta
    };
    let corrected = array_metadata_from_etp(&boolean, AnyLogicalArrayType::ArrayOfUInt8);
    assert_eq!(corrected.element_type, "uint8");
    assert_eq!(corrected.logical_type.as_deref(), Some("arrayOfBoolean"));
}

#[test]
fn the_logical_words_are_the_ones_of_the_schema() {
    assert_eq!(
        logical_symbol(AnyLogicalArrayType::ArrayOfInt32Be),
        "arrayOfInt32BE"
    );
    assert_eq!(
        logical_symbol(AnyLogicalArrayType::ArrayOfUInt64Le),
        "arrayOfUInt64LE"
    );
    assert_eq!(
        logical_symbol(AnyLogicalArrayType::ArrayOfString),
        "arrayOfString"
    );
    assert_eq!(
        logical_symbol(AnyLogicalArrayType::ArrayOfCustom),
        "arrayOfCustom"
    );
}

#[test]
fn an_array_is_asked_for_by_its_etp_address_first_then_by_its_file() {
    let etp = ArrayLocation::etp(crs_uri(), "/points");
    let id = identifier_of(&etp).unwrap();
    assert_eq!(
        (id.uri.as_str(), id.path_in_resource.as_str()),
        (crs_uri().as_str(), "/points")
    );

    let mut both = ArrayLocation::etp(crs_uri(), "/points");
    both.file = "other.h5".into();
    assert_eq!(identifier_of(&both).unwrap().uri, crs_uri());

    let file = ArrayLocation {
        file: points_uri(),
        path_in_file: "/p".into(),
        ..Default::default()
    };
    assert_eq!(identifier_of(&file).unwrap().uri, points_uri());

    assert!(identifier_of(&ArrayLocation::default()).is_none());
}

#[test]
fn a_context_becomes_the_request_it_stands_for() {
    let mut context = Context::of(points_uri(), Scope::SourcesOrSelf)
        .with_depth(3)
        .with_types(["resqml20.*"]);
    context.navigable = Navigable::Secondary;
    context.include_secondary_targets = true;
    context.store_last_write_after = Some(T0);

    let info = context_info(&context).unwrap();
    assert_eq!(info.uri, points_uri());
    assert_eq!(info.depth, 3);
    assert_eq!(info.data_object_types, ["resqml20.*"]);
    assert_eq!(info.navigable_edges, RelationshipKind::Secondary);
    assert!(info.include_secondary_targets);
    assert!(!info.include_secondary_sources);

    let request = resources_request(&context, true).unwrap();
    assert_eq!(request.scope, ContextScopeKind::SourcesOrSelf);
    assert_eq!(request.store_last_write_filter, Some(T0));
    assert!(request.include_edges);
    assert!(!resources_request(&context, false).unwrap().include_edges);
    assert!(!request.count_objects);
    assert_eq!(request.active_status_filter, None);

    assert_eq!(scope_kind(Scope::Itself), ContextScopeKind::Self_);
    assert_eq!(scope_kind(Scope::Sources), ContextScopeKind::Sources);
    assert_eq!(scope_kind(Scope::Targets), ContextScopeKind::Targets);
    assert_eq!(scope_kind(Scope::TargetsOrSelf), ContextScopeKind::TargetsOrSelf);
    assert_eq!(relationship_kind(Navigable::Primary), RelationshipKind::Primary);
    assert_eq!(relationship_kind(Navigable::Both), RelationshipKind::Both);
}

#[test]
fn a_dataspace_is_listed_the_usual_way_and_an_impossible_depth_is_refused() {
    // Scope and depth "MUST be ignored" for a dataspace (ETP 1.2 8.2.2 row 9a);
    // the request still carries the form a listing has always been asked in.
    let context = Context::dataspace(DATASPACE).with_depth(7);
    let request = resources_request(&context, false).unwrap();
    assert_eq!(request.scope, ContextScopeKind::Targets);
    assert_eq!(request.context.depth, 1);

    let deep = Context::of(points_uri(), Scope::Targets).with_depth(u32::MAX);
    assert!(matches!(context_info(&deep), Err(Error::Unsupported { .. })));
    assert!(matches!(
        resources_request(&deep, true),
        Err(Error::Unsupported { .. })
    ));
}

// ---------------------------------------------------------------------------
// A scripted server
// ---------------------------------------------------------------------------

/// What the scripted server answers.
#[derive(Clone, Default)]
struct Script {
    resources: Vec<Resource>,
    edges: Vec<EtpEdge>,
    refuse_resources: Option<EtpError>,
    deleted: Vec<EtpDeleted>,
    refuse_deleted: Option<EtpError>,
    /// The `format` it puts on every object it serves.
    object_format: String,
}

fn script() -> Script {
    Script {
        object_format: "xml".into(),
        ..Default::default()
    }
}

/// What the scripted server was asked.
#[derive(Default)]
struct Seen {
    resources: Vec<GetResources>,
    deleted: Vec<GetDeletedResources>,
    /// Each `GetDataObjects`: its URIs and its `format`.
    objects: Vec<(Vec<String>, String)>,
}

struct Scripted {
    protocol: i32,
    script: Script,
    seen: Arc<Mutex<Seen>>,
}

impl Handler for Scripted {
    fn offer(&self) -> ProtocolOffer {
        ProtocolOffer::v12(self.protocol, Role::Store)
    }

    fn handle(&mut self, delivery: &Delivery, responder: &mut Responder<'_>) -> Result<Handled, Declined> {
        let Body::Message(message) = &delivery.body else {
            return Err(Declined("no private protocol".into()));
        };
        match message {
            Message::GetResources(request) => {
                self.seen.lock().unwrap().resources.push(request.clone());
                if let Some(refusal) = self.script.refuse_resources {
                    let out = responder
                        .wire()
                        .refuse(delivery, &refusal.with_detail("the script refuses".to_string()));
                    responder.send(out);
                    return Ok(Handled::Answered);
                }
                let mut response = responder.wire().begin_response(delivery);
                if request.include_edges {
                    let edges = GetResourcesEdgesResponse {
                        edges: self.script.edges.clone(),
                    };
                    let out = responder.wire().part(&mut response, &edges).unwrap();
                    responder.send(out);
                }
                let out = responder
                    .wire()
                    .last(
                        &mut response,
                        &GetResourcesResponse {
                            resources: self.script.resources.clone(),
                        },
                    )
                    .unwrap();
                responder.send_all(out);
            }
            Message::GetDeletedResources(request) => {
                self.seen.lock().unwrap().deleted.push(request.clone());
                if let Some(refusal) = self.script.refuse_deleted {
                    let out = responder
                        .wire()
                        .refuse(delivery, &refusal.with_detail("the script refuses".to_string()));
                    responder.send(out);
                    return Ok(Handled::Answered);
                }
                let response = GetDeletedResourcesResponse {
                    deleted_resources: self.script.deleted.clone(),
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            Message::GetDataObjects(request) => {
                self.seen
                    .lock()
                    .unwrap()
                    .objects
                    .push((request.uris.values().cloned().collect(), request.format.clone()));
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.uris.keys().cloned());
                let mut found = BTreeMap::new();
                for (key, requested) in &request.uris {
                    if requested.contains(MISSING) {
                        reply
                            .failed(key.clone(), &EtpError::ENOT_FOUND.with_detail(requested.clone()))
                            .unwrap();
                    } else {
                        reply.answered(key.clone()).unwrap();
                        found.insert(
                            key.clone(),
                            DataObject {
                                data: format!("<raw of=\"{requested}\"/>").into_bytes(),
                                format: self.script.object_format.clone(),
                                ..Default::default()
                            },
                        );
                    }
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &GetDataObjectsResponse { data_objects: found })
                    .unwrap();
                responder.send_all(out);
            }
            _ => return Err(Declined("not in the scripted server".into())),
        }
        Ok(Handled::Answered)
    }
}

/// A store over a session with a server that speaks `protocols`, agrees on
/// `formats`, states `retention` seconds of `ChangeRetentionPeriod` when it is
/// `Some`, and answers as `script` says. `writing` makes the client offer the
/// Transaction protocol too.
fn session(
    protocols: &[i32],
    formats: &[&str],
    retention: Option<i64>,
    writing: bool,
    script: Script,
) -> (EtpStore<MemoryTransport, ManualClock>, Arc<Mutex<Seen>>) {
    let seen = Arc::new(Mutex::new(Seen::default()));
    let mut offer = Offer::new("a scripted server", "0.1", Uuid([2; 16]));
    offer.data_objects = ["resqml20.*", "eml20.*", "eml23.*"]
        .iter()
        .map(|t| SupportedDataObject {
            qualified_type: t.to_string(),
            data_object_capabilities: BTreeMap::new(),
        })
        .collect();
    offer.formats = formats.iter().map(|f| f.to_string()).collect();
    if let Some(seconds) = retention {
        offer.endpoint = offer
            .endpoint
            .clone()
            .set(names::CHANGE_RETENTION_PERIOD, Value::Long(seconds))
            .unwrap();
    }
    let handlers: Vec<Box<dyn Handler>> = protocols
        .iter()
        .map(|protocol| {
            Box::new(Scripted {
                protocol: *protocol,
                script: script.clone(),
                seen: Arc::clone(&seen),
            }) as Box<dyn Handler>
        })
        .collect();
    let server = Connection::server(offer, handlers, None, Uuid([0xAB; 16]), T0).unwrap();
    let clock = ManualClock::new(T0);
    let types = [
        "resqml20.*".to_string(),
        "eml20.*".to_string(),
        "eml23.*".to_string(),
    ];
    let (offer, handlers) = if writing {
        writing_client("the discovery tests", "0.1", Uuid([1; 16]), &types)
    } else {
        reading_client("the discovery tests", "0.1", Uuid([1; 16]), &types)
    };
    let client = Client::open(
        MemoryTransport::new(server, clock.clone()),
        clock,
        offer,
        handlers,
        None,
        Settings::default(),
    )
    .unwrap();
    (EtpStore::new(client, "demo", registry()), seen)
}

fn reading(script: Script) -> (EtpStore<MemoryTransport, ManualClock>, Arc<Mutex<Seen>>) {
    session(&[3, 4, 9, 24], &["xml"], None, false, script)
}

fn edge(from: String, to: String, kind: RelationshipKind) -> EtpEdge {
    EtpEdge {
        source_uri: from,
        target_uri: to,
        relationship_kind: kind,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Native requests
// ---------------------------------------------------------------------------

/// The context goes to the server whole - uri, depth, scope, kinds, flags,
/// types, `storeLastWriteFilter`, `includeEdges` - and the records come back
/// with their times and their edge kinds, the edges leaving the answer left
/// out and said.
#[test]
fn a_context_is_answered_by_one_get_resources() {
    let mut scripted = script();
    scripted.resources = vec![
        Resource {
            uri: points_uri(),
            name: "a point set".into(),
            store_created: T0,
            store_last_write: T0 + S,
            active_status: ActiveStatusKind::Inactive,
            ..Default::default()
        },
        Resource {
            uri: crs_uri(),
            name: "a local depth CRS".into(),
            store_created: T0,
            store_last_write: T0,
            ..Default::default()
        },
    ];
    scripted.edges = vec![
        edge(points_uri(), crs_uri(), RelationshipKind::Primary),
        edge(points_uri(), outside_uri(), RelationshipKind::Secondary),
    ];
    let (store, seen) = reading(scripted);

    let mut context = Context::of(points_uri(), Scope::TargetsOrSelf)
        .with_depth(2)
        .with_types([CRS_TYPE, POINT_SET]);
    context.navigable = Navigable::Primary;
    context.include_secondary_targets = true;
    context.store_last_write_after = Some(T0 - 1);

    let found = block(store.resources_in(&context)).unwrap();

    let asked = seen.lock().unwrap().resources.clone();
    assert_eq!(asked.len(), 1, "one exchange, not a walk");
    let request = &asked[0];
    assert_eq!(request.context.uri, points_uri());
    assert_eq!(request.context.depth, 2);
    assert_eq!(request.context.data_object_types, [CRS_TYPE, POINT_SET]);
    assert_eq!(request.context.navigable_edges, RelationshipKind::Primary);
    assert!(request.context.include_secondary_targets);
    assert!(!request.context.include_secondary_sources);
    assert_eq!(request.scope, ContextScopeKind::TargetsOrSelf);
    assert_eq!(request.store_last_write_filter, Some(T0 - 1));
    assert!(request.include_edges);

    assert_eq!(found.resources.len(), 2);
    assert_eq!(found.resources[0].uri, points_uri());
    assert_eq!(found.resources[0].store_last_write, Some(T0 + S));
    assert_eq!(found.resources[0].active_status, Some(ActiveStatus::Inactive));
    assert_eq!(
        found.edges.len(),
        1,
        "the edge to an object outside the answer is left out"
    );
    assert_eq!(found.edges[0].kind, Some(EdgeKind::Primary));
    assert_eq!(found.edges[0].to, crs_uri());
    assert!(
        store.diagnostics().iter().any(|d| d.message.contains("left out")),
        "{:?}",
        store.diagnostics()
    );
}

#[test]
fn another_dataspace_is_not_found_and_the_server_is_not_asked() {
    let (store, seen) = reading(script());
    let other = Context::dataspace("eml:///dataspace('other')");
    let error = block(store.resources_in(&other)).unwrap_err();
    assert!(matches!(error, Error::NotFound(_)), "{error:?}");
    assert!(seen.lock().unwrap().resources.is_empty());

    let not_canonical = Context::of(
        "eml:///resqml20.obj_X(uuid=aa000000-0000-4000-8000-000000000001)",
        Scope::Itself,
    );
    let error = block(store.resources_in(&not_canonical)).unwrap_err();
    assert!(matches!(error, Error::BadUri { .. }), "{error:?}");
    assert!(seen.lock().unwrap().resources.is_empty());
}

#[test]
fn a_server_that_ends_with_an_exception_is_an_error_never_an_empty_listing() {
    let mut scripted = script();
    scripted.refuse_resources = Some(EtpError::ENOT_FOUND);
    let (store, _) = reading(scripted);
    let error = block(store.resources_in(&Context::of(points_uri(), Scope::Itself))).unwrap_err();
    assert!(matches!(error, Error::NotFound(_)), "{error:?}");

    let mut scripted = script();
    scripted.refuse_resources = Some(EtpError::EREQUEST_DENIED);
    let (store, _) = reading(scripted);
    let error = block(store.resources_in(&Context::dataspace(DATASPACE))).unwrap_err();
    assert!(matches!(error, Error::Remote { .. }), "{error:?}");
}

#[test]
fn the_edges_of_a_dataspace_are_one_request_with_their_kinds() {
    let mut scripted = script();
    scripted.edges = vec![
        edge(points_uri(), crs_uri(), RelationshipKind::Primary),
        edge(points_uri(), outside_uri(), RelationshipKind::Secondary),
        edge(crs_uri(), outside_uri(), RelationshipKind::Both),
    ];
    let (store, seen) = reading(scripted);

    let edges = block(store.edges(&[])).unwrap();

    let asked = seen.lock().unwrap().resources.clone();
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].context.uri, DATASPACE);
    assert!(asked[0].include_edges);
    assert_eq!(edges.len(), 3);
    assert_eq!(edges[0].kind, Some(EdgeKind::Primary));
    assert_eq!(edges[1].kind, Some(EdgeKind::Secondary));
    assert_eq!(edges[2].kind, None, "Both is no kind of an edge");
    assert!(
        store.diagnostics().iter().any(|d| d.message.contains("Both")),
        "{:?}",
        store.diagnostics()
    );
}

#[test]
fn the_edges_of_objects_are_those_leaving_them() {
    let mut scripted = script();
    scripted.edges = vec![
        edge(points_uri(), crs_uri(), RelationshipKind::Primary),
        edge(points_uri(), outside_uri(), RelationshipKind::Secondary),
        edge(crs_uri(), outside_uri(), RelationshipKind::Primary),
    ];
    let (store, seen) = reading(scripted);

    let edges = block(store.edges(&[points_uri(), crs_uri()])).unwrap();

    let pairs: Vec<(&str, &str)> = edges.iter().map(|e| (e.from.as_str(), e.to.as_str())).collect();
    assert_eq!(
        pairs,
        [
            (points_uri().as_str(), crs_uri().as_str()),
            (points_uri().as_str(), outside_uri().as_str()),
            (crs_uri().as_str(), outside_uri().as_str()),
        ]
    );
    let asked = seen.lock().unwrap().resources.clone();
    assert_eq!(asked.len(), 2, "one request per object");
    for (request, object) in asked.iter().zip([points_uri(), crs_uri()]) {
        assert_eq!(request.context.uri, object);
        assert_eq!(request.scope, ContextScopeKind::TargetsOrSelf);
        assert_eq!(request.context.depth, 1);
        assert_eq!(request.context.navigable_edges, RelationshipKind::Both);
        assert!(request.include_edges);
    }
}

#[test]
fn objects_come_as_the_bytes_the_server_sent_in_the_format_asked() {
    let (store, seen) = reading(script());
    let asked = vec![
        points_uri(),
        uri(POINT_SET, MISSING),
        "eml:///resqml20.obj_X(uuid=aa000000-0000-4000-8000-000000000001)".to_string(),
    ];

    let raw = block(store.get_raw(&asked, RawFormat::Xml));

    let object = raw[0].as_ref().unwrap();
    assert_eq!(object.uri, points_uri());
    assert_eq!(object.format, RawFormat::Xml);
    assert_eq!(
        object.bytes,
        format!("<raw of=\"{}\"/>", points_uri()).into_bytes()
    );
    assert!(
        matches!(&raw[1], Err(Error::NotFound(_))),
        "{:?}",
        raw[1].as_ref().err()
    );
    assert!(
        matches!(&raw[2], Err(Error::BadUri { .. })),
        "{:?}",
        raw[2].as_ref().err()
    );
    let sent = seen.lock().unwrap().objects.clone();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].1, "xml");
}

#[test]
fn a_format_the_session_did_not_agree_is_refused_by_name_before_the_server_is_asked() {
    let (store, seen) = reading(script());
    let raw = block(store.get_raw(&[points_uri()], RawFormat::Json));
    assert!(
        matches!(&raw[0], Err(Error::Unsupported { .. })),
        "{:?}",
        raw[0].as_ref().err()
    );
    assert!(seen.lock().unwrap().objects.is_empty());
}

#[test]
fn json_is_served_when_agreed_and_an_answer_in_another_format_is_not_relabelled() {
    let mut scripted = script();
    scripted.object_format = "json".into();
    let (store, _) = session(&[3, 4, 9, 24], &["xml", "json"], None, false, scripted);
    let raw = block(store.get_raw(&[points_uri()], RawFormat::Json));
    assert_eq!(raw[0].as_ref().unwrap().format, RawFormat::Json);

    // Asked in xml, answered in json: an error, not xml bytes that are json.
    let raw = block(store.get_raw(&[points_uri()], RawFormat::Xml));
    assert!(
        matches!(&raw[0], Err(Error::Remote { .. })),
        "{:?}",
        raw[0].as_ref().err()
    );
}

#[test]
fn deletions_are_asked_at_the_dataspace_level_with_the_filters() {
    let mut scripted = script();
    scripted.deleted = vec![
        EtpDeleted {
            uri: crs_uri(),
            deleted_time: T0 + 2 * S,
            ..Default::default()
        },
        EtpDeleted {
            uri: points_uri(),
            deleted_time: T0 + 3 * S,
            ..Default::default()
        },
    ];
    let (store, seen) = reading(scripted);
    let mut filter = DeletedFilter::of(DATASPACE);
    filter.after = Some(T0 + S);
    filter.types = vec![CRS_TYPE.into(), POINT_SET.into()];

    let deleted = block(store.deleted_resources(&filter)).unwrap();

    assert_eq!(deleted.len(), 2);
    assert_eq!(
        (deleted[0].uri.as_str(), deleted[0].deleted_time),
        (crs_uri().as_str(), T0 + 2 * S)
    );
    assert_eq!(deleted[1].deleted_time, T0 + 3 * S);
    let asked = seen.lock().unwrap().deleted.clone();
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].dataspace_uri, DATASPACE);
    assert_eq!(asked[0].delete_time_filter, Some(T0 + S));
    assert_eq!(asked[0].data_object_types, [CRS_TYPE, POINT_SET]);
}

#[test]
fn a_server_that_says_none_were_deleted_is_believed_and_one_that_refuses_is_not_mistaken_for_it() {
    let (store, _) = reading(script());
    let none = block(store.deleted_resources(&DeletedFilter::of(DATASPACE))).unwrap();
    assert!(none.is_empty());

    let mut scripted = script();
    scripted.refuse_deleted = Some(EtpError::ERETENTION_PERIOD_EXCEEDED);
    let (store, _) = reading(scripted);
    let mut filter = DeletedFilter::of(DATASPACE);
    filter.after = Some(1);
    match block(store.deleted_resources(&filter)).unwrap_err() {
        Error::Remote { reason, .. } => assert!(reason.contains("5001"), "{reason}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn deletions_are_asked_of_a_dataspace_of_this_store_and_not_of_an_object() {
    let (store, seen) = reading(script());
    let error = block(store.deleted_resources(&DeletedFilter::of(points_uri()))).unwrap_err();
    assert!(matches!(error, Error::BadUri { .. }), "{error:?}");
    let error = block(store.deleted_resources(&DeletedFilter::of("eml:///dataspace('other')"))).unwrap_err();
    assert!(matches!(error, Error::NotFound(_)), "{error:?}");
    assert!(seen.lock().unwrap().deleted.is_empty());
}

// ---------------------------------------------------------------------------
// Capabilities
// ---------------------------------------------------------------------------

#[test]
fn the_capabilities_are_what_the_session_agreed() {
    let (store, _) = reading(script());
    let capabilities = store.capabilities();
    assert!(capabilities.writes);
    assert!(!capabilities.transactions, "the client did not offer Transaction");
    assert!(capabilities.several_dataspaces);
    assert!(capabilities.subarrays);
    assert!(capabilities.uninitialized_arrays);
    assert!(capabilities.store_times);
    assert!(capabilities.edge_kinds);
    assert!(capabilities.native_context);
    assert!(!capabilities.changes);
    assert_eq!(
        capabilities.deletions_kept_for,
        Some(86_400),
        "the server stated no ChangeRetentionPeriod: its default, ETP 1.2 §9.2.3"
    );
    assert!(
        capabilities.contained_objects,
        "Store (4) agreed: containment is the server's"
    );
    assert_eq!(capabilities.raw_formats, [RawFormat::Xml]);
}

#[test]
fn a_protocol_not_agreed_is_a_capability_not_offered() {
    // Discovery alone: no Store, no DataArray, no Dataspace.
    let (store, _) = session(&[3], &["xml"], None, false, script());
    let capabilities = store.capabilities();
    assert!(!capabilities.writes);
    assert!(!capabilities.several_dataspaces);
    assert!(!capabilities.subarrays);
    assert!(!capabilities.uninitialized_arrays);
    assert!(capabilities.raw_formats.is_empty(), "no Store, no bytes to read");
    assert!(capabilities.native_context);

    // No Discovery: nothing it carries.
    let (store, _) = session(&[4, 9], &["xml"], None, false, script());
    let capabilities = store.capabilities();
    assert!(!capabilities.native_context);
    assert!(!capabilities.store_times);
    assert!(!capabilities.edge_kinds);
    assert!(capabilities.writes);
}

#[test]
fn transactions_need_both_ends_and_the_retention_is_the_one_the_server_stated() {
    // The server offers Transaction, a reading client does not: not agreed.
    let (store, _) = session(&[3, 4, 9, 18, 24], &["xml"], None, false, script());
    assert!(!store.capabilities().transactions);

    let (store, _) = session(
        &[3, 4, 9, 18, 24],
        &["xml", "json"],
        Some(7 * 86_400),
        true,
        script(),
    );
    let capabilities = store.capabilities();
    assert!(capabilities.transactions);
    assert_eq!(capabilities.deletions_kept_for, Some(7 * 86_400));
    assert_eq!(capabilities.raw_formats.len(), 2);
    assert!(capabilities.raw_formats.contains(&RawFormat::Xml));
    assert!(capabilities.raw_formats.contains(&RawFormat::Json));
}
