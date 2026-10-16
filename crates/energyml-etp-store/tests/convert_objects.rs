//! The conversions of objects, edges, dataspaces, contexts, supported types,
//! deletions and errors, in both directions (`energyml_etp_store::convert::objects`
//! and the reading functions of `energyml_etp_store::discovery`).
//!
//! **Round trips**, on records built by hand that set every field: storage to
//! ETP to storage gives back the same value, and ETP to storage to ETP the same
//! message. What a direction cannot carry is built so that it is not in the
//! record (and the documentation of the function says so): each exception is
//! commented where it is.
//!
//! **Errors**: one assertion per variant of the storage `Error`, with the code
//! ETP 1.2 24.3 (pp. 482-486, "Error codes") gives the situation.
//!
//! Breaking them on purpose: drop `alternate_uris` in `resource_to_etp`, send
//! `Primary` for `Secondary` in `edge_kind_to_etp`, forget `include_secondary_sources`
//! in `context_from_etp`, or answer `ENOT_FOUND` for `Ambiguous`.

use std::collections::BTreeMap;
use std::sync::Arc;

use energyml_core::registry::Registry;
use energyml_etp_store::convert::objects::{
    context_from_etp, context_of_request, dataspace_to_etp, deleted_resource_from_etp,
    deleted_resource_to_etp, edge_kind_to_etp, edge_to_etp, edge_to_etp_checked, error_code_of, error_to_etp,
    navigable_from_etp, resource_to_etp, resource_to_etp_named, scope_from_etp, supported_type_to_etp,
    supported_types_to_etp,
};
use energyml_etp_store::discovery::{
    context_info, dataspace_from_etp, dataspace_from_etp_said, edge_from_etp, edge_kind_from_etp,
    relationship_kind, resources_request, scope_kind, summary_from_resource,
};
use energyml_storage::context::{Context, DeletedResource, Navigable, Scope};
use energyml_storage::error::{Error, ExternalDataReason};
use energyml_storage::graph::{Edge, EdgeKind};
use energyml_storage::resource::{ActiveStatus, CustomValue, Dataspace, ObjectSummary};
use retp_proto::wire::EtpError;
use retp_types::datatypes::object::{
    ActiveStatusKind, ContextInfo, ContextScopeKind, Dataspace as EtpDataspace,
    DeletedResource as EtpDeleted, RelationshipKind, Resource,
};
use retp_types::datatypes::{ArrayOfString, DataValue, DataValueItem};

const DATASPACE: &str = "eml:///dataspace('demo')";
const POINT_SET: &str = "resqml20.obj_PointSetRepresentation";
const POINTS: &str = "bb000000-0000-4000-8000-000000000002";
const CRS: &str = "aa000000-0000-4000-8000-000000000001";
const T0: i64 = 1_700_000_000_000_000;

fn points_uri() -> String {
    format!("{DATASPACE}/{POINT_SET}({POINTS})")
}

fn crs_uri() -> String {
    format!("{DATASPACE}/resqml20.obj_LocalDepth3dCrs({CRS})")
}

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    energyml_eml_2_3::register(&mut registry);
    energyml_resqml_2_2::register(&mut registry);
    Arc::new(registry)
}

// ---------------------------------------------------------------------------
// Resources
// ---------------------------------------------------------------------------

/// A summary that sets every field the ETP `Resource` can carry, with a locator
/// equal to the URI (what `summary_from_resource` fills).
fn full_summary() -> ObjectSummary {
    let mut summary = ObjectSummary::new(points_uri(), POINTS, POINT_SET)
        .with_content_type("application/x-resqml+xml;version=2.0;type=obj_PointSetRepresentation")
        .with_locator(points_uri());
    summary.alternate_uris = vec![
        format!("eml:///{POINT_SET}({POINTS})"),
        format!("{DATASPACE}/{POINT_SET}({POINTS})?x=1"),
    ];
    summary.store_created = Some(T0);
    summary.store_last_write = Some(T0 + 5_000_000);
    summary.active_status = Some(ActiveStatus::Active);
    summary.source_count = Some(3);
    summary.target_count = Some(7);
    summary
}

#[test]
fn a_summary_survives_storage_to_etp_to_storage() {
    let summary = full_summary();
    let resource = resource_to_etp(&summary);
    assert_eq!(resource.uri, summary.uri);
    assert_eq!(resource.alternate_uris, summary.alternate_uris);
    assert_eq!(resource.store_created, T0);
    assert_eq!(resource.store_last_write, T0 + 5_000_000);
    assert_eq!(resource.active_status, ActiveStatusKind::Active);
    assert_eq!((resource.source_count, resource.target_count), (Some(3), Some(7)));
    let back = summary_from_resource(&resource, &summary.content_type).unwrap();
    assert_eq!(back, summary);

    // The other status.
    let mut inactive = full_summary();
    inactive.active_status = Some(ActiveStatus::Inactive);
    let back = summary_from_resource(&resource_to_etp(&inactive), &inactive.content_type).unwrap();
    assert_eq!(back, inactive);
}

#[test]
fn a_resource_survives_etp_to_storage_to_etp_but_for_what_storage_does_not_keep() {
    // `name` and `lastChanged` are not in a summary: the record is built with
    // the values the conversion gives them (the uuid, 0), or named by the
    // `_named` form.
    let resource = Resource {
        uri: points_uri(),
        alternate_uris: vec!["eml:///a".into(), "eml:///b".into()],
        name: POINTS.into(),
        source_count: Some(1),
        target_count: Some(2),
        last_changed: 0,
        store_last_write: T0 + 9,
        store_created: T0 + 1,
        active_status: ActiveStatusKind::Inactive,
        custom_data: BTreeMap::new(),
    };
    let summary = summary_from_resource(&resource, "").unwrap();
    assert_eq!(resource_to_etp(&summary), resource);

    let named = resource_to_etp_named(&summary, "Top Brent", 42);
    assert_eq!((named.name.as_str(), named.last_changed), ("Top Brent", 42));
    assert_eq!(named.uri, resource.uri);
    assert_eq!(named.store_created, resource.store_created);
}

#[test]
fn what_a_summary_does_not_state_is_sent_unstated_not_invented() {
    let bare = ObjectSummary::new(points_uri(), POINTS, POINT_SET);
    let resource = resource_to_etp(&bare);
    // 0 is how ETP spells "the store keeps none" (3.12.5.2: required longs).
    assert_eq!((resource.store_created, resource.store_last_write), (0, 0));
    assert_eq!((resource.source_count, resource.target_count), (None, None));
    assert!(resource.alternate_uris.is_empty());
    assert!(resource.custom_data.is_empty());
    // An unstated status is `Inactive` (documented), and reads back as stated.
    assert_eq!(resource.active_status, ActiveStatusKind::Inactive);
    let back = summary_from_resource(&resource, "").unwrap();
    assert_eq!((back.store_created, back.store_last_write), (None, None));

    // A count ETP's `int` cannot hold is unstated, never clamped.
    let mut huge = ObjectSummary::new(points_uri(), POINTS, POINT_SET);
    huge.source_count = Some(u32::MAX);
    huge.target_count = Some(i32::MAX as u32);
    let resource = resource_to_etp(&huge);
    assert_eq!(resource.source_count, None);
    assert_eq!(resource.target_count, Some(i32::MAX));
}

// ---------------------------------------------------------------------------
// Edges
// ---------------------------------------------------------------------------

#[test]
fn an_edge_survives_both_ways_for_each_kind() {
    for kind in [EdgeKind::Primary, EdgeKind::Secondary] {
        // `path` is not in ETP: the edge has none, as `edge_from_etp` reads it.
        let edge = Edge::new(points_uri(), crs_uri(), "").with_kind(kind);
        let etp = edge_to_etp(&edge);
        assert_eq!(etp.source_uri, points_uri());
        assert_eq!(etp.target_uri, crs_uri());
        assert_eq!(etp.relationship_kind, edge_kind_to_etp(kind));
        assert!(etp.custom_data.is_empty());
        assert_eq!(edge_from_etp(&etp), edge);
        assert_eq!(edge_to_etp(&edge_from_etp(&etp)), etp);
        assert_eq!(edge_to_etp_checked(&edge).unwrap(), etp);
        assert_eq!(edge_kind_from_etp(edge_kind_to_etp(kind)), Some(kind));
    }
    assert_eq!(edge_kind_to_etp(EdgeKind::Primary), RelationshipKind::Primary);
    assert_eq!(edge_kind_to_etp(EdgeKind::Secondary), RelationshipKind::Secondary);
}

#[test]
fn an_edge_of_unstated_kind_is_the_inverse_of_both_and_is_refused_when_checked() {
    let unstated = Edge::new(points_uri(), crs_uri(), "Grid2dPatch.SupportingRepresentation");
    let etp = edge_to_etp(&unstated);
    assert_eq!(etp.relationship_kind, RelationshipKind::Both);
    // ETP to storage to ETP is the same message...
    assert_eq!(edge_to_etp(&edge_from_etp(&etp)), etp);
    // ...and the path, which ETP cannot say, is gone.
    assert_eq!(edge_from_etp(&etp).path, "");
    // A server that must not send `Both` (23.34.13) refuses by name.
    assert!(matches!(
        edge_to_etp_checked(&unstated),
        Err(Error::Unsupported { .. })
    ));
}

// ---------------------------------------------------------------------------
// Dataspaces
// ---------------------------------------------------------------------------

fn full_dataspace() -> Dataspace {
    let mut dataspace = Dataspace::new(DATASPACE, "demo");
    dataspace.store_created = Some(T0);
    dataspace.store_last_write = Some(T0 + 1);
    dataspace.custom_data = BTreeMap::from([
        ("owner".to_string(), CustomValue::Text("geo".into())),
        (
            "viewers".to_string(),
            CustomValue::Texts(vec!["a@x".into(), "b@x".into()]),
        ),
        ("frozen".to_string(), CustomValue::Bool(true)),
        ("level".to_string(), CustomValue::Integer(-12)),
    ]);
    dataspace
}

#[test]
fn a_dataspace_survives_storage_to_etp_to_storage() {
    let dataspace = full_dataspace();
    let etp = dataspace_to_etp(&dataspace);
    assert_eq!((etp.uri.as_str(), etp.path.as_str()), (DATASPACE, "demo"));
    assert_eq!((etp.store_created, etp.store_last_write), (T0, T0 + 1));
    assert_eq!(etp.custom_data.len(), 4);
    let (back, left) = dataspace_from_etp_said(&etp);
    assert_eq!(back, dataspace);
    assert!(left.is_empty());
    assert_eq!(dataspace_from_etp(&etp), dataspace);
}

#[test]
fn a_dataspace_survives_etp_to_storage_to_etp() {
    let etp = EtpDataspace {
        uri: "eml:///".into(),
        path: String::new(),
        store_last_write: 0,
        store_created: T0,
        custom_data: BTreeMap::from([
            (
                "viewers".to_string(),
                DataValue {
                    item: DataValueItem::ArrayOfString(ArrayOfString {
                        values: vec!["a".into()],
                    }),
                },
            ),
            (
                "n".to_string(),
                DataValue {
                    item: DataValueItem::Long(5),
                },
            ),
            (
                "t".to_string(),
                DataValue {
                    item: DataValueItem::String("x".into()),
                },
            ),
            (
                "b".to_string(),
                DataValue {
                    item: DataValueItem::Boolean(false),
                },
            ),
        ]),
    };
    assert_eq!(dataspace_to_etp(&dataspace_from_etp(&etp)), etp);
}

#[test]
fn a_dataspace_that_states_no_time_sends_zero_and_loses_its_lexical_dates() {
    let mut dataspace = Dataspace::new(DATASPACE, "demo");
    dataspace.created = Some("2026-01-01T00:00:00Z".into());
    dataspace.last_changed = Some("2026-01-02T00:00:00Z".into());
    let etp = dataspace_to_etp(&dataspace);
    assert_eq!((etp.store_created, etp.store_last_write), (0, 0));
    let back = dataspace_from_etp(&etp);
    assert_eq!((back.store_created, back.store_last_write), (None, None));
    // ETP carries only the store times: the lexical dates are not in the record.
    assert_eq!((back.created, back.last_changed), (None, None));
}

// ---------------------------------------------------------------------------
// Contexts
// ---------------------------------------------------------------------------

const SCOPES: [(Scope, ContextScopeKind); 5] = [
    (Scope::Itself, ContextScopeKind::Self_),
    (Scope::Sources, ContextScopeKind::Sources),
    (Scope::Targets, ContextScopeKind::Targets),
    (Scope::SourcesOrSelf, ContextScopeKind::SourcesOrSelf),
    (Scope::TargetsOrSelf, ContextScopeKind::TargetsOrSelf),
];

#[test]
fn a_scope_and_a_navigation_filter_survive_both_ways() {
    for (scope, kind) in SCOPES {
        assert_eq!(scope_from_etp(kind), scope);
        assert_eq!(scope_kind(scope_from_etp(kind)), kind);
        assert_eq!(scope_from_etp(scope_kind(scope)), scope);
    }
    for navigable in [Navigable::Primary, Navigable::Secondary, Navigable::Both] {
        assert_eq!(navigable_from_etp(relationship_kind(navigable)), navigable);
    }
    for kind in [
        RelationshipKind::Primary,
        RelationshipKind::Secondary,
        RelationshipKind::Both,
    ] {
        assert_eq!(relationship_kind(navigable_from_etp(kind)), kind);
    }
}

fn full_context(scope: Scope) -> Context {
    let mut context = Context::of(points_uri(), scope)
        .with_depth(4)
        .with_types(["resqml20.*", "resqml22.TriangulatedSetRepresentation"]);
    context.navigable = Navigable::Secondary;
    context.include_secondary_targets = true;
    context.include_secondary_sources = true;
    context.store_last_write_after = Some(T0);
    context.active_status = Some(ActiveStatus::Active);
    context.count_objects = true;
    context
}

#[test]
fn a_context_survives_storage_to_etp_to_storage_for_each_scope() {
    for (scope, _) in SCOPES {
        let context = full_context(scope);
        let info = context_info(&context).unwrap();
        let back = context_from_etp(&info, scope_kind(scope)).unwrap();
        // `ContextInfo` holds the context but for the three request filters.
        let mut expected = context.clone();
        expected.store_last_write_after = None;
        expected.active_status = None;
        expected.count_objects = false;
        assert_eq!(back, expected, "{scope:?}");

        // The whole request holds them.
        let request = resources_request(&context, true).unwrap();
        let (whole, include_edges) = context_of_request(&request).unwrap();
        assert!(include_edges);
        assert_eq!(whole, context, "{scope:?}");
        assert!(
            !context_of_request(&resources_request(&context, false).unwrap())
                .unwrap()
                .1
        );
    }
}

#[test]
fn a_context_info_survives_etp_to_storage_to_etp() {
    let info = ContextInfo {
        uri: crs_uri(),
        depth: 3,
        data_object_types: vec!["eml23.*".into(), "resqml22.Activity".into()],
        navigable_edges: RelationshipKind::Primary,
        include_secondary_targets: true,
        include_secondary_sources: false,
    };
    for (_, scope) in SCOPES {
        let context = context_from_etp(&info, scope).unwrap();
        assert_eq!(context_info(&context).unwrap(), info);
        assert_eq!(scope_kind(context.scope), scope);
    }
}

#[test]
fn a_dataspace_context_comes_back_as_it_was_on_the_wire() {
    // 8.2.2 row 9a: scope and depth of a dataspace are ignored; the request
    // carries `targets` and depth 1, and that is what is read back.
    let request = resources_request(&Context::dataspace(DATASPACE).with_depth(9), true).unwrap();
    let (context, _) = context_of_request(&request).unwrap();
    assert_eq!((context.scope, context.depth), (Scope::Targets, 1));
    assert_eq!(context.uri, DATASPACE);
}

#[test]
fn a_context_with_a_bad_uri_or_a_negative_depth_is_refused_by_name() {
    let info = |uri: &str, depth: i32| ContextInfo {
        uri: uri.into(),
        depth,
        data_object_types: Vec::new(),
        navigable_edges: RelationshipKind::Both,
        include_secondary_targets: false,
        include_secondary_sources: false,
    };
    let bad_uri = context_from_etp(&info("not a uri", 1), ContextScopeKind::Targets).unwrap_err();
    assert!(matches!(bad_uri, Error::BadUri { .. }), "{bad_uri:?}");
    assert_eq!(error_code_of(&bad_uri), EtpError::EINVALID_URI);

    let negative = context_from_etp(&info(&crs_uri(), -1), ContextScopeKind::Targets).unwrap_err();
    assert!(matches!(negative, Error::OutOfBounds { .. }), "{negative:?}");
    assert_eq!(error_code_of(&negative), EtpError::EINVALID_ARGUMENT);

    // Depth 0 is a depth.
    assert_eq!(
        context_from_etp(&info(&crs_uri(), 0), ContextScopeKind::Self_)
            .unwrap()
            .depth,
        0
    );
}

// ---------------------------------------------------------------------------
// Supported types
// ---------------------------------------------------------------------------

#[test]
fn a_supported_type_names_its_qualified_type_and_its_count() {
    let registry = registry();
    let types: Vec<_> = registry.top_level_types().take(6).collect();
    assert!(!types.is_empty());
    for info in &types {
        let qualified = info.qualified_type.unwrap();
        let counted = supported_type_to_etp(info, Some(12), RelationshipKind::Both).unwrap();
        assert_eq!(counted.data_object_type, qualified);
        assert_eq!(counted.object_count, Some(12));
        assert_eq!(counted.relationship_kind, RelationshipKind::Both);
        // The way back is the registry: the name finds the same type.
        let entry = registry.by_qualified_type(&counted.data_object_type).unwrap();
        assert!(std::ptr::eq(entry.info, *info));

        let uncounted = supported_type_to_etp(info, None, RelationshipKind::Primary).unwrap();
        assert_eq!(uncounted.object_count, None);
        assert_eq!(uncounted.relationship_kind, RelationshipKind::Primary);
    }
}

#[test]
fn a_count_beyond_the_int_is_refused_and_a_nested_type_is_not_listed() {
    let registry = registry();
    let top = registry.top_level_types().next().unwrap();
    let refused =
        supported_type_to_etp(top, Some(i32::MAX as usize + 1), RelationshipKind::Both).unwrap_err();
    assert!(matches!(refused, Error::TooLarge { .. }), "{refused:?}");
    assert_eq!(
        supported_type_to_etp(top, Some(i32::MAX as usize), RelationshipKind::Both)
            .unwrap()
            .object_count,
        Some(i32::MAX)
    );

    // A type with no qualified type (a nested type, never an object of its
    // own) cannot be listed as a supported type.
    let nested = registry
        .types()
        .find(|info| info.qualified_type.is_none())
        .expect("the registry holds nested types");
    let refused = supported_type_to_etp(nested, None, RelationshipKind::Both).unwrap_err();
    assert!(matches!(refused, Error::Unsupported { .. }), "{refused:?}");
    assert!(supported_types_to_etp(&[nested], &[], false, false, RelationshipKind::Both).is_err());
}

#[test]
fn the_list_has_empty_types_only_when_asked_and_counts_only_when_asked() {
    let registry = registry();
    let types: Vec<_> = registry.top_level_types().take(3).collect();
    let first = types[0].qualified_type.unwrap().to_string();
    let second = types[1].qualified_type.unwrap().to_string();
    let counts = vec![(first.clone(), 4usize), (second.clone(), 1usize)];

    let kind = RelationshipKind::Both;
    let only_held = supported_types_to_etp(&types, &counts, true, false, kind).unwrap();
    assert_eq!(
        only_held
            .iter()
            .map(|t| (t.data_object_type.clone(), t.object_count))
            .collect::<Vec<_>>(),
        vec![(first.clone(), Some(4)), (second.clone(), Some(1))]
    );

    let with_empty = supported_types_to_etp(&types, &counts, true, true, kind).unwrap();
    assert_eq!(with_empty.len(), 3);
    assert_eq!(with_empty[2].object_count, Some(0));

    let uncounted = supported_types_to_etp(&types, &counts, false, false, kind).unwrap();
    assert_eq!(uncounted.len(), 2);
    assert!(uncounted.iter().all(|t| t.object_count.is_none()));
}

// ---------------------------------------------------------------------------
// Deletions
// ---------------------------------------------------------------------------

#[test]
fn a_deletion_survives_both_ways() {
    let deleted = DeletedResource::new(points_uri(), T0 + 77);
    let etp = deleted_resource_to_etp(&deleted);
    assert_eq!(
        (etp.uri.as_str(), etp.deleted_time),
        (points_uri().as_str(), T0 + 77)
    );
    assert!(etp.custom_data.is_empty());
    assert_eq!(deleted_resource_from_etp(&etp), deleted);

    let record = EtpDeleted {
        uri: crs_uri(),
        deleted_time: 5,
        custom_data: BTreeMap::new(),
    };
    assert_eq!(
        deleted_resource_to_etp(&deleted_resource_from_etp(&record)),
        record
    );
}

// ---------------------------------------------------------------------------
// Errors (ETP 1.2 24.3, "Error codes", pp. 482-486)
// ---------------------------------------------------------------------------

fn every_error() -> Vec<(Error, EtpError, i32)> {
    let external = |reason| Error::ExternalData {
        location: "f.h5".into(),
        reason,
    };
    vec![
        // 11 ENOT_FOUND: no such resource, data object, part or range.
        (Error::NotFound(crs_uri()), EtpError::ENOT_FOUND, 11),
        // 4004 EPLURAL_OBJECT: one URI resolved to several objects.
        (
            Error::Ambiguous {
                uri: format!("eml:///resqml20.obj_Activity({CRS})"),
                candidates: vec!["a".into(), "b".into()],
            },
            EtpError::EPLURAL_OBJECT,
            4004,
        ),
        // 9 EINVALID_URI.
        (
            Error::BadUri {
                uri: "x".into(),
                reason: "no".into(),
            },
            EtpError::EINVALID_URI,
            9,
        ),
        // 5 EINVALID_ARGUMENT: a field of the message is unusable.
        (
            Error::OutOfBounds {
                location: "a".into(),
                reason: "axis 1".into(),
            },
            EtpError::EINVALID_ARGUMENT,
            5,
        ),
        // 17 EMAXSIZE_EXCEEDED.
        (
            Error::TooLarge {
                location: "a".into(),
                reason: "9 > 8".into(),
            },
            EtpError::EMAXSIZE_EXCEEDED,
            17,
        ),
        // 16 EDATAOBJECTTYPE_NOTSUPPORTED.
        (
            Error::UnknownType("foo.Bar".into()),
            EtpError::EDATAOBJECTTYPE_NOTSUPPORTED,
            16,
        ),
        // 14 EINVALID_OBJECT: the object does not parse or validate.
        (Error::Package("broken".into()), EtpError::EINVALID_OBJECT, 14),
        (Error::Xml("broken".into()), EtpError::EINVALID_OBJECT, 14),
        (
            Error::Core(energyml_core::error::Error::Eof),
            EtpError::EINVALID_OBJECT,
            14,
        ),
        // 7 ENOTSUPPORTED: the operation is not implemented here.
        (
            Error::Unsupported {
                operation: "listing deleted objects",
                store: "epc",
            },
            EtpError::ENOTSUPPORTED,
            7,
        ),
        // 8 EINVALID_STATE: a legal message at the wrong moment.
        (
            Error::Transaction {
                store: "epc",
                reason: "one is open".into(),
            },
            EtpError::EINVALID_STATE,
            8,
        ),
        (
            external(ExternalDataReason::FeatureNotEnabled {
                feature: "hdf5",
                mime: None,
            }),
            EtpError::ENOTSUPPORTED,
            7,
        ),
        (
            external(ExternalDataReason::FileMissing),
            EtpError::ENOT_FOUND,
            11,
        ),
        (
            external(ExternalDataReason::PathMissing("/a".into())),
            EtpError::ENOT_FOUND,
            11,
        ),
        (
            external(ExternalDataReason::UuidMismatch {
                expected: "a".into(),
                found: "b".into(),
            }),
            EtpError::EINVALID_OBJECT,
            14,
        ),
        (
            external(ExternalDataReason::ShapeDisagrees { promised: 6, held: 5 }),
            EtpError::EINVALID_ARGUMENT,
            5,
        ),
        (
            external(ExternalDataReason::Backend("disk".into())),
            EtpError::EINVALID_OPERATION,
            32,
        ),
        // 32 EINVALID_OPERATION: ETP defines no code for a failure of the
        // store itself (TODO(verify) in the documentation of `error_code_of`).
        (
            Error::Io(std::io::Error::other("disk")),
            EtpError::EINVALID_OPERATION,
            32,
        ),
        (
            Error::Remote {
                store: "etp",
                reason: "closed".into(),
            },
            EtpError::EINVALID_OPERATION,
            32,
        ),
    ]
}

#[test]
fn every_storage_error_answers_the_code_the_specification_gives_it() {
    for (error, code, number) in every_error() {
        assert_eq!(error_code_of(&error), code, "{error:?}");
        let info = error_to_etp(&error);
        assert_eq!(info.code, number, "{error:?}");
        assert_eq!(code.code(), number, "{error:?}");
        // The message names the code, then says what happened (24).
        assert!(
            info.message.starts_with(&format!("{}: ", code.name())),
            "{}",
            info.message
        );
        assert!(info.message.contains(&error.to_string()), "{}", info.message);
    }
}

#[test]
fn the_table_covers_every_variant_of_the_storage_error_known_today() {
    // One entry per variant, by name: a variant added to the storage `Error`
    // must be added here (and to `error_code_of`) by whoever reviews it.
    let named: Vec<&str> = every_error()
        .iter()
        .map(|(error, _, _)| {
            let text = format!("{error:?}");
            let end = text.find(['(', ' ', '{']).unwrap_or(text.len());
            match &text[..end] {
                "NotFound" => "NotFound",
                "Ambiguous" => "Ambiguous",
                "BadUri" => "BadUri",
                "OutOfBounds" => "OutOfBounds",
                "TooLarge" => "TooLarge",
                "UnknownType" => "UnknownType",
                "Package" => "Package",
                "Unsupported" => "Unsupported",
                "Transaction" => "Transaction",
                "ExternalData" => "ExternalData",
                "Io" => "Io",
                "Remote" => "Remote",
                "Xml" => "Xml",
                "Core" => "Core",
                other => panic!("a variant the table does not know: {other}"),
            }
        })
        .collect();
    for variant in [
        "NotFound",
        "Ambiguous",
        "BadUri",
        "OutOfBounds",
        "TooLarge",
        "UnknownType",
        "Package",
        "Unsupported",
        "Transaction",
        "ExternalData",
        "Io",
        "Remote",
        "Xml",
        "Core",
    ] {
        assert!(named.contains(&variant), "no entry for {variant}");
    }
}

#[test]
fn a_not_found_and_a_bad_uri_come_back_as_the_errors_the_client_reads() {
    // `lib.rs` `refused` reads 11 as `NotFound` and 9 as `BadUri`: what a
    // server sends is what a client of this crate understands.
    assert_eq!(error_to_etp(&Error::NotFound("u".into())).code, 11);
    assert_eq!(
        error_to_etp(&Error::BadUri {
            uri: "u".into(),
            reason: "r".into()
        })
        .code,
        9
    );
}
