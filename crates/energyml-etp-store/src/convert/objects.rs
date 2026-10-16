//! Storage values into the ETP records that state them: the inverse of every
//! ETP-to-storage conversion of [`crate::discovery`], for objects, edges,
//! dataspaces, contexts, supported types, deletions and errors.
//!
//! The client of this crate turns what a server states into storage values; a
//! server built on the same storage traits needs the other direction, to
//! answer. Both directions live in one module tree so that the two cannot drift
//! apart at the first change: a field added to a record is added to its two
//! conversions in the same place, and the round trips of
//! `tests/convert_objects.rs` fail until it is.
//!
//! **What storage cannot say in ETP, and what ETP cannot say in storage, is
//! named in the documentation of each function**, never dropped without a word.
//! The rule of the crate holds here too: a conversion that cannot say a value
//! faithfully returns an error naming it, or leaves it unstated (`None`, 0 for
//! a timestamp) as the reading direction does.
//!
//! | storage | ETP | back |
//! |---|---|---|
//! | [`resource_to_etp`] | `Resource` (3.12.5.2) | `summary_from_resource` |
//! | [`edge_to_etp`] | `Edge` (23.34.13) | `edge_from_etp` |
//! | [`dataspace_to_etp`] | `Dataspace` | `dataspace_from_etp` |
//! | [`context_from_etp`] | `ContextInfo` (23.34.15) | `context_info` |
//! | [`supported_type_to_etp`] | `SupportedType` | the registry |
//! | [`deleted_resource_to_etp`] | `DeletedResource` | [`deleted_resource_from_etp`] |
//! | [`error_to_etp`] | `ErrorInfo` (24.3) | `refused` of `lib.rs` |

use energyml_core::reflect::TypeInfo;
use energyml_storage::context::{Context, DeletedResource, Navigable, Scope, is_dataspace_uri};
use energyml_storage::error::{Error, ExternalDataReason, Result};
use energyml_storage::graph::{Edge, EdgeKind};
use energyml_storage::resource::{ActiveStatus, Dataspace, ObjectSummary};
use retp_proto::wire::EtpError;
use retp_types::datatypes::ErrorInfo;
use retp_types::datatypes::object::{
    ActiveStatusKind, ContextInfo, ContextScopeKind, Dataspace as EtpDataspace,
    DeletedResource as EtpDeleted, Edge as EtpEdge, RelationshipKind, Resource, SupportedType,
};
use retp_types::protocol::discovery::GetResources;

use energyml_core::identifiers::EtpUri;
use energyml_storage::resource::CustomValue;

use crate::KIND;
use crate::discovery::store_time;
use crate::qualified_type_of;

// ---------------------------------------------------------------------------
// Resources
// ---------------------------------------------------------------------------

/// A storage summary as the ETP `Resource` that lists it.
///
/// The inverse of `discovery::summary_from_resource`:
///
/// | storage | ETP |
/// |---|---|
/// | `uri` | `uri` |
/// | `alternate_uris` | `alternateUris` |
/// | `store_created`, `store_last_write` | `storeCreated`, `storeLastWrite` (`None` is 0) |
/// | `active_status` | `activeStatus` |
/// | `source_count`, `target_count` | `sourceCount`, `targetCount` |
///
/// What storage keeps and ETP has no field for: `uuid` and `qualified_type`
/// (both are in the URI), `content_type` and `locator` (the reader derives
/// them again from the registry and the URI). What ETP requires and a summary
/// does not hold:
///
/// - `name` is the **uuid**, the name a header falls back on when a citation
///   has no title; [`resource_to_etp_named`] takes the real title;
/// - `lastChanged` is 0 (unstated), as `storeCreated` is for a store that
///   keeps none;
/// - `customData` is empty.
///
/// A `None` time is sent as 0, which `discovery::store_time` reads back as
/// unstated (ETP makes the field a required `long`, so "not kept" can only be
/// spelled 0). A `None` active status is sent as `Inactive`: an object that is
/// not known to grow is not being written. `TODO(verify)`: what a store puts
/// for an object that cannot grow (see `summary_from_resource`). A count
/// beyond `i32::MAX` cannot be said (ETP counts are `int`) and is sent
/// unstated (`None`) rather than clamped to a wrong number.
pub fn resource_to_etp(summary: &ObjectSummary) -> Resource {
    resource_to_etp_named(summary, &summary.uuid, 0)
}

/// [`resource_to_etp`] with the two fields a summary does not hold: the
/// `name` (the citation title, which a server reads from the header) and
/// `lastChanged` (the citation's last update as an ETP timestamp, 0 when
/// unstated).
pub fn resource_to_etp_named(summary: &ObjectSummary, name: &str, last_changed: i64) -> Resource {
    Resource {
        uri: summary.uri.clone(),
        alternate_uris: summary.alternate_uris.clone(),
        name: name.to_string(),
        source_count: summary.source_count.and_then(|n| i32::try_from(n).ok()),
        target_count: summary.target_count.and_then(|n| i32::try_from(n).ok()),
        last_changed,
        store_last_write: summary.store_last_write.unwrap_or(0),
        store_created: summary.store_created.unwrap_or(0),
        active_status: match summary.active_status {
            Some(ActiveStatus::Active) => ActiveStatusKind::Active,
            Some(ActiveStatus::Inactive) | None => ActiveStatusKind::Inactive,
        },
        custom_data: std::collections::BTreeMap::new(),
    }
}

// ---------------------------------------------------------------------------
// Edges
// ---------------------------------------------------------------------------

/// A storage edge kind as the ETP `RelationshipKind`: the inverse of
/// `discovery::edge_kind_from_etp`. Never `Both`: "For edges, RelationshipKind
/// CANNOT be Both" (ETP 1.2 23.34.13).
pub fn edge_kind_to_etp(kind: EdgeKind) -> RelationshipKind {
    match kind {
        EdgeKind::Primary => RelationshipKind::Primary,
        EdgeKind::Secondary => RelationshipKind::Secondary,
    }
}

/// A storage edge as an ETP `Edge`: source and target, with the kind the
/// store stated. The inverse of `discovery::edge_from_etp`.
///
/// Lost: [`Edge::path`] (where inside the source the reference sits; ETP names
/// none). Empty: `customData`.
///
/// **An edge whose kind the store does not state is sent as `Both`**, the
/// exact inverse of what `edge_from_etp` reads (`Both` has no kind), so that
/// ETP to storage to ETP gives the same record. ETP forbids `Both` on an edge
/// (23.34.13): a server answering a client should use [`edge_to_etp_checked`],
/// which refuses instead.
pub fn edge_to_etp(edge: &Edge) -> EtpEdge {
    EtpEdge {
        source_uri: edge.from.clone(),
        target_uri: edge.to.clone(),
        relationship_kind: edge.kind.map_or(RelationshipKind::Both, edge_kind_to_etp),
        custom_data: std::collections::BTreeMap::new(),
    }
}

/// [`edge_to_etp`], refusing an edge whose kind is not stated: ETP edges are
/// `Primary` or `Secondary` (23.34.13), and no kind is guessed for a store
/// that derives its edges from the objects.
pub fn edge_to_etp_checked(edge: &Edge) -> Result<EtpEdge> {
    if edge.kind.is_none() {
        return Err(Error::Unsupported {
            operation: "stating an edge whose kind the store does not know (ETP edges are Primary or Secondary, 23.34.13)",
            store: KIND,
        });
    }
    Ok(edge_to_etp(edge))
}

// ---------------------------------------------------------------------------
// Dataspaces
// ---------------------------------------------------------------------------

/// A storage dataspace as an ETP `Dataspace`: the inverse of
/// `discovery::dataspace_from_etp`.
///
/// The name is the ETP `path` (`carl/teapot`; empty for the default
/// dataspace); the store times are sent as the ETP timestamps (`None` is 0);
/// each custom value goes through [`custom_value_to_etp`]. Lost: `created` and
/// `last_changed` (the lexical dates; ETP carries only the store times).
pub fn dataspace_to_etp(dataspace: &Dataspace) -> EtpDataspace {
    EtpDataspace {
        uri: dataspace.uri.clone(),
        path: dataspace.name.clone(),
        store_last_write: dataspace.store_last_write.unwrap_or(0),
        store_created: dataspace.store_created.unwrap_or(0),
        custom_data: dataspace
            .custom_data
            .iter()
            .map(|(key, value)| (key.clone(), custom_value_to_etp(value)))
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// Contexts
// ---------------------------------------------------------------------------

/// A storage scope from the ETP `ContextScopeKind`: the inverse of
/// `discovery::scope_kind`.
pub fn scope_from_etp(kind: ContextScopeKind) -> Scope {
    match kind {
        ContextScopeKind::Self_ => Scope::Itself,
        ContextScopeKind::Sources => Scope::Sources,
        ContextScopeKind::Targets => Scope::Targets,
        ContextScopeKind::SourcesOrSelf => Scope::SourcesOrSelf,
        ContextScopeKind::TargetsOrSelf => Scope::TargetsOrSelf,
    }
}

/// A navigation filter from the ETP `RelationshipKind`: the inverse of
/// `discovery::relationship_kind`.
pub fn navigable_from_etp(kind: RelationshipKind) -> Navigable {
    match kind {
        RelationshipKind::Primary => Navigable::Primary,
        RelationshipKind::Secondary => Navigable::Secondary,
        RelationshipKind::Both => Navigable::Both,
    }
}

/// The [`Context`] a `ContextInfo` and its scope stand for: what a server
/// reads from a `GetResources` (ETP 1.2 8.3.1, 23.34.15). The inverse of
/// `discovery::context_info`.
///
/// Refused by name, never repaired: a `uri` that is not an ETP URI
/// ([`Error::BadUri`], answered `EINVALID_URI`) and a negative `depth`
/// ([`Error::OutOfBounds`], answered `EINVALID_ARGUMENT`).
///
/// What `ContextInfo` does not hold (`storeLastWriteFilter`,
/// `activeStatusFilter`, `countObjects`) is in the `GetResources` around it:
/// [`context_of_request`] reads the whole request.
///
/// For a dataspace URI the scope and the depth "MUST be ignored" (8.2.2 row
/// 9a), and `discovery::resources_request` sends `targets` and depth 1 for it:
/// the context read back carries what was on the wire, which a server ignores
/// in the same way.
pub fn context_from_etp(info: &ContextInfo, scope: ContextScopeKind) -> Result<Context> {
    is_dataspace_uri(&info.uri)?;
    let depth = u32::try_from(info.depth).map_err(|_| Error::OutOfBounds {
        location: info.uri.clone(),
        reason: format!(
            "the depth is {}; ETP 1.2 23.34.15 states no negative depth",
            info.depth
        ),
    })?;
    let mut context = Context::of(info.uri.clone(), scope_from_etp(scope));
    context.depth = depth;
    context.types = info.data_object_types.clone();
    context.navigable = navigable_from_etp(info.navigable_edges);
    context.include_secondary_targets = info.include_secondary_targets;
    context.include_secondary_sources = info.include_secondary_sources;
    Ok(context)
}

/// The [`Context`] of a whole `GetResources`, and its `includeEdges`: the
/// inverse of `discovery::resources_request`. [`context_from_etp`] for the
/// context, then the three filters of the request.
pub fn context_of_request(request: &GetResources) -> Result<(Context, bool)> {
    let mut context = context_from_etp(&request.context, request.scope)?;
    context.store_last_write_after = request.store_last_write_filter;
    context.active_status = request.active_status_filter.map(|status| match status {
        ActiveStatusKind::Active => ActiveStatus::Active,
        ActiveStatusKind::Inactive => ActiveStatus::Inactive,
    });
    context.count_objects = request.count_objects;
    Ok((context, request.include_edges))
}

// ---------------------------------------------------------------------------
// Supported types
// ---------------------------------------------------------------------------

/// One entry of a `GetSupportedTypesResponse` for a type the storage says it
/// holds (`EnergymlStorage::supported_types` gives the [`TypeInfo`]s).
///
/// - `dataObjectType` is the qualified type (`resqml20.obj_Activity`);
/// - `objectCount` is `count` (`None` when the request did not ask for counts,
///   `countObjects` false);
/// - `relationshipKind` is `kind`, the relationship through which the type is
///   reached from the context. `TODO(verify)`: how ETP 1.2 25 means this field
///   for a flat listing; `Both` is what a [`Context`] says by default.
///
/// Refused by name: a type that is not a top-level data object (no qualified
/// type, [`Error::Unsupported`]) and a count beyond `i32::MAX`
/// ([`Error::TooLarge`]); neither is clamped or left out.
pub fn supported_type_to_etp(
    info: &TypeInfo,
    count: Option<usize>,
    kind: RelationshipKind,
) -> Result<SupportedType> {
    let Some(qualified) = info.qualified_type else {
        return Err(Error::Unsupported {
            operation: "listing as a supported type a type that is not a top-level data object",
            store: KIND,
        });
    };
    let object_count = match count {
        None => None,
        Some(n) => Some(i32::try_from(n).map_err(|_| Error::TooLarge {
            location: qualified.to_string(),
            reason: format!("{n} objects; ETP objectCount is an int (at most 2147483647)"),
        })?),
    };
    Ok(SupportedType {
        data_object_type: qualified.to_string(),
        object_count,
        relationship_kind: kind,
    })
}

/// The whole answer to `GetSupportedTypes` (ETP 1.2 25): one entry per type of
/// `types`, with the counts of `counts` (what `energyml_storage::context::types_of`
/// gives: qualified type and number of objects) when `count_objects` is true.
///
/// A type with no object is listed only when `return_empty_types` is true
/// (`returnEmptyTypes`); its count is then 0. A type without a qualified type
/// is refused, as [`supported_type_to_etp`] does.
pub fn supported_types_to_etp(
    types: &[&TypeInfo],
    counts: &[(String, usize)],
    count_objects: bool,
    return_empty_types: bool,
    kind: RelationshipKind,
) -> Result<Vec<SupportedType>> {
    let mut out = Vec::with_capacity(types.len());
    for info in types {
        let held = info
            .qualified_type
            .and_then(|qualified| counts.iter().find(|(name, _)| name == qualified))
            .map_or(0, |(_, n)| *n);
        if held == 0 && !return_empty_types {
            // Still refuse a type that cannot be named, empty or not.
            supported_type_to_etp(info, None, kind)?;
            continue;
        }
        out.push(supported_type_to_etp(info, count_objects.then_some(held), kind)?);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Deletions
// ---------------------------------------------------------------------------

/// A storage deletion as an ETP `DeletedResource` (the answer to
/// `GetDeletedResources`, 8.3.4): `uri` and `deletedTime`; `customData` empty.
pub fn deleted_resource_to_etp(deleted: &DeletedResource) -> EtpDeleted {
    EtpDeleted {
        uri: deleted.uri.clone(),
        deleted_time: deleted.deleted_time,
        custom_data: std::collections::BTreeMap::new(),
    }
}

/// An ETP `DeletedResource` as a storage deletion: the inverse of
/// [`deleted_resource_to_etp`], and what `EtpStore::deleted` reads. Lost: the
/// record's `customData`, which storage has nowhere to keep.
pub fn deleted_resource_from_etp(deleted: &EtpDeleted) -> DeletedResource {
    DeletedResource::new(deleted.uri.clone(), deleted.deleted_time)
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// The ETP error code a server answers for a storage error (ETP 1.2 24.3,
/// pp. 482-486). The inverse direction is `refused` of `lib.rs`.
///
/// | storage | ETP |
/// |---|---|
/// | `NotFound` | `ENOT_FOUND` (11) |
/// | `Ambiguous` | `EPLURAL_OBJECT` (4004) |
/// | `BadUri` | `EINVALID_URI` (9) |
/// | `OutOfBounds` | `EINVALID_ARGUMENT` (5) |
/// | `TooLarge` | `EMAXSIZE_EXCEEDED` (17) |
/// | `UnknownType` | `EDATAOBJECTTYPE_NOTSUPPORTED` (16) |
/// | `Package`, `Xml`, `Core` | `EINVALID_OBJECT` (14) |
/// | `Unsupported` | `ENOTSUPPORTED` (7) |
/// | `Transaction` | `EINVALID_STATE` (8) |
/// | `ExternalData`: feature not enabled | `ENOTSUPPORTED` (7) |
/// | `ExternalData`: file or path missing | `ENOT_FOUND` (11) |
/// | `ExternalData`: uuid mismatch | `EINVALID_OBJECT` (14) |
/// | `ExternalData`: shape disagrees | `EINVALID_ARGUMENT` (5) |
/// | `ExternalData`: backend | `EINVALID_OPERATION` (32) |
/// | `Io`, `Remote` | `EINVALID_OPERATION` (32) |
///
/// `TODO(verify)`: ETP 1.2 defines no code for a failure of the store itself
/// (`Io`, `Remote`, a backend): `EINVALID_OPERATION` is the closest ("not valid
/// for that object or that state"), and the message carries the real reason.
/// The same for `OutOfBounds` (the table also has `ENOT_FOUND` for "range")
/// and `TooLarge` (`ELIMIT_EXCEEDED`).
///
/// Every variant of [`Error`] and of [`ExternalDataReason`] has an arm. Both
/// types are `#[non_exhaustive]`, so the compiler forces one more arm for a
/// variant added later; it answers `EINVALID_OPERATION` with the variant's own
/// text, and `tests/convert_objects.rs` lists every variant known today, so a
/// new one is seen at the first review of that table.
pub fn error_code_of(error: &Error) -> EtpError {
    match error {
        Error::NotFound(_) => EtpError::ENOT_FOUND,
        Error::Ambiguous { .. } => EtpError::EPLURAL_OBJECT,
        Error::BadUri { .. } => EtpError::EINVALID_URI,
        Error::OutOfBounds { .. } => EtpError::EINVALID_ARGUMENT,
        Error::TooLarge { .. } => EtpError::EMAXSIZE_EXCEEDED,
        Error::UnknownType(_) => EtpError::EDATAOBJECTTYPE_NOTSUPPORTED,
        Error::Package(_) | Error::Xml(_) | Error::Core(_) => EtpError::EINVALID_OBJECT,
        Error::Unsupported { .. } => EtpError::ENOTSUPPORTED,
        // A request by relationship kind that met an edge whose kind nobody
        // states is refused by name, like any other `Unsupported`.
        Error::UnstatedKind { .. } => EtpError::ENOTSUPPORTED,
        Error::Transaction { .. } => EtpError::EINVALID_STATE,
        Error::ExternalData { reason, .. } => match reason {
            ExternalDataReason::FeatureNotEnabled { .. } => EtpError::ENOTSUPPORTED,
            ExternalDataReason::FileMissing | ExternalDataReason::PathMissing(_) => EtpError::ENOT_FOUND,
            ExternalDataReason::UuidMismatch { .. } => EtpError::EINVALID_OBJECT,
            ExternalDataReason::ShapeDisagrees { .. } => EtpError::EINVALID_ARGUMENT,
            ExternalDataReason::Backend(_) => EtpError::EINVALID_OPERATION,
            // `ExternalDataReason` is `#[non_exhaustive]`.
            _ => EtpError::EINVALID_OPERATION,
        },
        Error::Io(_) | Error::Remote { .. } => EtpError::EINVALID_OPERATION,
        // `Error` is `#[non_exhaustive]`.
        _ => EtpError::EINVALID_OPERATION,
    }
}

/// The ETP `ErrorInfo` a server answers for a storage error: the code of
/// [`error_code_of`], and a message that names the code then gives the error's
/// own text (24, "the name, then the detail").
pub fn error_to_etp(error: &Error) -> ErrorInfo {
    error_code_of(error).with_detail(error.to_string()).error_info()
}

// -- ETP to storage (the client's side), moved here beside their inverses ---------

/// A listed ETP resource as a storage summary, or `None` when its URI does not
/// name a data object this store can address (a dataspace, a URI without a
/// uuid).
///
/// `content_type` is what the registry knows of the qualified type (empty when
/// it knows nothing); the caller looks it up, so that this function reads no
/// registry.
///
/// `activeStatus` is passed through as stated: ETP makes it required, but only
/// channels and growing objects give it a meaning (23.34.1, 23.34.11), so a
/// server's answer for any other type is whatever it fills in (the Avro zero
/// is `Active`). `TODO(verify)`: what a store puts for an object that cannot
/// grow.
pub fn summary_from_resource(resource: &Resource, content_type: &str) -> Option<ObjectSummary> {
    let parsed = EtpUri::parse(&resource.uri).ok()?;
    let qualified_type = qualified_type_of(&parsed)?;
    let mut summary = ObjectSummary::new(resource.uri.clone(), parsed.uuid?, qualified_type)
        .with_content_type(content_type)
        .with_locator(resource.uri.clone());
    summary.alternate_uris = resource.alternate_uris.clone();
    summary.store_created = store_time(resource.store_created);
    summary.store_last_write = store_time(resource.store_last_write);
    summary.active_status = Some(match resource.active_status {
        ActiveStatusKind::Active => ActiveStatus::Active,
        ActiveStatusKind::Inactive => ActiveStatus::Inactive,
    });
    // Present only when the request asked for counts; a negative count is not a
    // count.
    summary.source_count = resource.source_count.and_then(|n| u32::try_from(n).ok());
    summary.target_count = resource.target_count.and_then(|n| u32::try_from(n).ok());
    Some(summary)
}

/// The kind of an ETP edge. `Both` is a filter of a request, not the kind of
/// an edge ("For edges, RelationshipKind CANNOT be Both", ETP 1.2 23.34.13),
/// so it has none.
pub fn edge_kind_from_etp(kind: RelationshipKind) -> Option<EdgeKind> {
    match kind {
        RelationshipKind::Primary => Some(EdgeKind::Primary),
        RelationshipKind::Secondary => Some(EdgeKind::Secondary),
        RelationshipKind::Both => None,
    }
}

/// An ETP edge as a storage edge: source to target, with the kind the server
/// stated.
///
/// ETP names no path inside the source (`Grid2dPatch.SupportingRepresentation`
/// and the like): [`Edge::path`] is empty. A `Both` edge, which the
/// specification forbids, keeps its ends and has no kind.
pub fn edge_from_etp(edge: &EtpEdge) -> Edge {
    let out = Edge::new(edge.source_uri.clone(), edge.target_uri.clone(), "");
    match edge_kind_from_etp(edge.relationship_kind) {
        Some(kind) => out.with_kind(kind),
        None => out,
    }
}

/// An ETP dataspace as a storage dataspace. The name is its `path`; the
/// lexical `created` and `last_changed` stay unstated (ETP carries only the
/// store times).
pub fn dataspace_from_etp(dataspace: &EtpDataspace) -> Dataspace {
    dataspace_from_etp_said(dataspace).0
}

/// [`dataspace_from_etp`], and the keys of the custom data it could not carry
/// (a value of a form [`CustomValue`] does not hold): a caller says them rather
/// than dropping them unseen.
pub fn dataspace_from_etp_said(dataspace: &EtpDataspace) -> (Dataspace, Vec<String>) {
    let mut out = Dataspace::new(dataspace.uri.clone(), dataspace.path.clone());
    out.store_created = store_time(dataspace.store_created);
    out.store_last_write = store_time(dataspace.store_last_write);
    let mut left = Vec::new();
    for (key, value) in &dataspace.custom_data {
        match custom_value_from_etp(value) {
            Some(value) => {
                out.custom_data.insert(key.clone(), value);
            }
            None => left.push(key.clone()),
        }
    }
    (out, left)
}

/// One ETP `DataValue` of a dataspace's custom data, in a form storage holds:
/// a text, a list of texts, a boolean, an integer. `None` for any other form
/// (a float, bytes, a sparse array, null).
pub fn custom_value_from_etp(value: &retp_types::datatypes::DataValue) -> Option<CustomValue> {
    use retp_types::datatypes::DataValueItem as V;
    match &value.item {
        V::String(text) => Some(CustomValue::Text(text.clone())),
        V::ArrayOfString(texts) => Some(CustomValue::Texts(texts.values.clone())),
        V::Boolean(flag) => Some(CustomValue::Bool(*flag)),
        V::Int(n) => Some(CustomValue::Integer(i64::from(*n))),
        V::Long(n) => Some(CustomValue::Integer(*n)),
        _ => None,
    }
}

/// A storage custom value as ETP carries it.
pub fn custom_value_to_etp(value: &CustomValue) -> retp_types::datatypes::DataValue {
    use retp_types::datatypes::{ArrayOfString, DataValue, DataValueItem as V};
    let item = match value {
        CustomValue::Text(text) => V::String(text.clone()),
        CustomValue::Texts(texts) => V::ArrayOfString(ArrayOfString {
            values: texts.clone(),
        }),
        CustomValue::Bool(flag) => V::Boolean(*flag),
        CustomValue::Integer(n) => V::Long(*n),
    };
    DataValue { item }
}

/// The ETP `ContextScopeKind` of a scope.
pub fn scope_kind(scope: Scope) -> ContextScopeKind {
    match scope {
        Scope::Itself => ContextScopeKind::Self_,
        Scope::Sources => ContextScopeKind::Sources,
        Scope::Targets => ContextScopeKind::Targets,
        Scope::SourcesOrSelf => ContextScopeKind::SourcesOrSelf,
        Scope::TargetsOrSelf => ContextScopeKind::TargetsOrSelf,
    }
}

/// The ETP `RelationshipKind` of a navigation filter.
///
/// `TODO(verify)`: `Both` is allowed by 8.3.1 ("Choices are Primary, Secondary
/// or Both") and discouraged by 23.34.15 ("SHOULD NOT be set to Both"); it is
/// sent as asked, since "both" is what a [`Context`] says by default.
pub fn relationship_kind(navigable: Navigable) -> RelationshipKind {
    match navigable {
        Navigable::Primary => RelationshipKind::Primary,
        Navigable::Secondary => RelationshipKind::Secondary,
        Navigable::Both => RelationshipKind::Both,
    }
}

/// A [`Context`] as ETP's `ContextInfo`. A depth that does not fit ETP's `int`
/// is refused by name, never clamped.
pub fn context_info(context: &Context) -> Result<ContextInfo> {
    let depth = i32::try_from(context.depth).map_err(|_| Error::Unsupported {
        operation: "a context depth beyond 2147483647 (ETP depth is an int)",
        store: KIND,
    })?;
    Ok(ContextInfo {
        uri: context.uri.clone(),
        depth,
        data_object_types: context.types.clone(),
        navigable_edges: relationship_kind(context.navigable),
        include_secondary_targets: context.include_secondary_targets,
        include_secondary_sources: context.include_secondary_sources,
    })
}
