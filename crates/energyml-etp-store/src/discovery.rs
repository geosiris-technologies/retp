//! What the reading additions of `energyml-storage` 0.2.0-alpha.1 ask of an
//! ETP dataspace: a context walked natively, the edges, the objects as bytes,
//! the deletions, and what the session allows.
//!
//! Two kinds of item live here.
//!
//! **Pure conversions**, public free functions: an ETP record in, a storage
//! record out (or the other way for a [`Context`](energyml_storage::context::Context)). They read no session, so
//! each can be tested with a fabricated record (`tests/discovery.rs`).
//!
//! **Inherent helpers** of [`EtpStore`], `pub(crate)`: the exchanges. The
//! `impl EnergymlStorage for EtpStore` block of `lib.rs` delegates to them one
//! line per method (see the table).
//!
//! | the trait | ETP | helper |
//! |---|---|---|
//! | `resources_in` | Discovery (3), one `GetResources` with the context and `includeEdges` | `native_resources_in` |
//! | `edges` | Discovery (3), `GetResources` with `includeEdges` | `native_edges` |
//! | `get_raw` | Store (4), `GetDataObjects` with `format` | `raw_objects` |
//! | `deleted_resources` | Discovery (3), `GetDeletedResources` | `deleted` |
//! | `capabilities` | nothing on the wire: what the session agreed | `etp_capabilities` |
//!
//! **A refusal is said, never an empty answer.** A server that ends a request
//! with an exception gives an error (its partial answer is dropped), a format
//! the session did not agree is [`Error::Unsupported`], and what a conversion
//! cannot say it leaves unstated (`None`) rather than inventing.

use std::collections::HashSet;

use energyml_core::identifiers::EtpUri;
use energyml_storage::array::{ArrayLocation, ArrayMetadata};
use energyml_storage::capabilities::Capabilities;
use energyml_storage::context::{
    Context, DeletedFilter, DeletedResource, Found, Navigable, Scope, is_dataspace_uri,
};
use energyml_storage::error::{Diagnostic, Error, Result};
use energyml_storage::graph::{Edge, EdgeKind};
use energyml_storage::raw::{RawFormat, RawObject};
use energyml_storage::resource::{ActiveStatus, Dataspace, ObjectSummary};
use retp_client::asynchronous::get_data_objects_in_batches;
use retp_client::{Clock, Transport};
use retp_proto::capabilities::names;
use retp_proto::negotiate::Negotiated;
use retp_proto::uris::{UriUse, check_uri};
use retp_proto::wire::{Body, Delivery, Exception};
use retp_types::datatypes::data_array_types::{DataArrayIdentifier, DataArrayMetadata};
use retp_types::datatypes::object::{
    ActiveStatusKind, ContextInfo, ContextScopeKind, Dataspace as EtpDataspace, Edge as EtpEdge,
    RelationshipKind, Resource,
};
use retp_types::datatypes::{AnyLogicalArrayType, ErrorInfo};
use retp_types::dispatch::Message;
use retp_types::protocol::discovery::{GetDeletedResources, GetResources};

use crate::{EtpStore, KIND, element_type, pack, qualified_type_of, refused, remote};

/// The protocols whose agreement `capabilities_of` reads: Discovery, Store,
/// DataArray, Transaction, Dataspace.
const DISCOVERY: i32 = 3;
const STORE: i32 = 4;
const DATA_ARRAY: i32 = 9;
const TRANSACTION: i32 = 18;
const DATASPACE: i32 = 24;

// ---------------------------------------------------------------------------
// Pure conversions
// ---------------------------------------------------------------------------

/// An ETP timestamp as storage keeps it: microseconds since the Unix epoch,
/// `None` when the record carries 0.
///
/// ETP makes `storeCreated` and `storeLastWrite` required `long`s (3.12.5.2),
/// so "the store does not keep it" cannot be spelled on the wire; 0 is what a
/// server that keeps none sends (the Avro zero), and 1 January 1970 is not a
/// time a store wrote anything. `TODO(verify)`: whether a server may mean 0
/// literally; until then it is read as "not stated".
pub fn store_time(timestamp: i64) -> Option<i64> {
    (timestamp != 0).then_some(timestamp)
}

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
    let mut out = Edge::new(edge.source_uri.clone(), edge.target_uri.clone(), "");
    out.kind = edge_kind_from_etp(edge.relationship_kind);
    out
}

/// An ETP dataspace as a storage dataspace. The name is its `path`; the
/// lexical `created` and `last_changed` stay unstated (ETP carries only the
/// store times).
pub fn dataspace_from_etp(dataspace: &EtpDataspace) -> Dataspace {
    let mut out = Dataspace::new(dataspace.uri.clone(), dataspace.path.clone());
    out.store_created = store_time(dataspace.store_created);
    out.store_last_write = store_time(dataspace.store_last_write);
    out
}

/// The word ETP spells a logical array type with (`arrayOfDouble64LE`).
pub fn logical_symbol(logical: AnyLogicalArrayType) -> &'static str {
    use AnyLogicalArrayType as L;
    match logical {
        L::ArrayOfBoolean => "arrayOfBoolean",
        L::ArrayOfInt8 => "arrayOfInt8",
        L::ArrayOfUInt8 => "arrayOfUInt8",
        L::ArrayOfInt16Le => "arrayOfInt16LE",
        L::ArrayOfInt32Le => "arrayOfInt32LE",
        L::ArrayOfInt64Le => "arrayOfInt64LE",
        L::ArrayOfUInt16Le => "arrayOfUInt16LE",
        L::ArrayOfUInt32Le => "arrayOfUInt32LE",
        L::ArrayOfUInt64Le => "arrayOfUInt64LE",
        L::ArrayOfFloat32Le => "arrayOfFloat32LE",
        L::ArrayOfDouble64Le => "arrayOfDouble64LE",
        L::ArrayOfInt16Be => "arrayOfInt16BE",
        L::ArrayOfInt32Be => "arrayOfInt32BE",
        L::ArrayOfInt64Be => "arrayOfInt64BE",
        L::ArrayOfUInt16Be => "arrayOfUInt16BE",
        L::ArrayOfUInt32Be => "arrayOfUInt32BE",
        L::ArrayOfUInt64Be => "arrayOfUInt64BE",
        L::ArrayOfFloat32Be => "arrayOfFloat32BE",
        L::ArrayOfDouble64Be => "arrayOfDouble64BE",
        L::ArrayOfString => "arrayOfString",
        L::ArrayOfCustom => "arrayOfCustom",
    }
}

/// ETP array metadata as storage array metadata.
///
/// `logical` is the logical type the array is **read** as (the caller resolved
/// it with `retp_client::arrays::reading_type`, which may correct a server
/// that sends a type its transport cannot carry); it gives the element type,
/// as `lib.rs` has always done. [`ArrayMetadata::logical_type`] is the word
/// the server itself used for `logicalArrayType`, so a correction stays
/// visible. `preferredSubarrayDimensions` and the store times are carried
/// over (0 times are unstated).
pub fn array_metadata_from_etp(meta: &DataArrayMetadata, logical: AnyLogicalArrayType) -> ArrayMetadata {
    let mut out = ArrayMetadata::new(meta.dimensions.clone(), element_type(logical).to_string());
    out.logical_type = Some(logical_symbol(meta.logical_array_type).to_string());
    out.preferred_subarray_dimensions = meta.preferred_subarray_dimensions.clone();
    out.store_created = store_time(meta.store_created);
    out.store_last_write = store_time(meta.store_last_write);
    out
}

/// Where an array is asked for: the ETP address when the location has one
/// (`resource_uri`, then `path_in_file` as `pathInResource`), else its `file`
/// when that was filled with the URI of the holding object (what `arrays_of`
/// does today). `None` for a location that names nothing.
pub fn identifier_of(location: &ArrayLocation) -> Option<DataArrayIdentifier> {
    match location.etp_address() {
        Some((uri, path)) => Some(DataArrayIdentifier {
            uri: uri.to_string(),
            path_in_resource: path.to_string(),
        }),
        None => (!location.file.is_empty()).then(|| DataArrayIdentifier {
            uri: location.file.clone(),
            path_in_resource: location.path_in_file.clone(),
        }),
    }
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

/// The `GetResources` a [`Context`] stands for (ETP 1.2 8.3.1).
///
/// For a dataspace URI, scope and depth "MUST be ignored" (8.2.2 row 9a); the
/// request carries `targets` and depth 1 anyway, the form a listing has always
/// been asked in, so a server that does not ignore them still lists the
/// content. `countObjects` is never asked (storage has nowhere to put the
/// counts) and `activeStatusFilter` is not part of a [`Context`].
pub fn resources_request(context: &Context, include_edges: bool) -> Result<GetResources> {
    let mut info = context_info(context)?;
    let scope = if is_dataspace_uri(&context.uri)? {
        info.depth = 1;
        ContextScopeKind::Targets
    } else {
        scope_kind(context.scope)
    };
    Ok(GetResources {
        context: info,
        scope,
        count_objects: false,
        store_last_write_filter: context.store_last_write_after,
        active_status_filter: None,
        include_edges,
    })
}

/// What a session allows, read from what it agreed.
///
/// Nothing is asked of the server: a flag is true when the protocol that
/// carries the feature was agreed (the server announced it as a store, or the
/// session would not have it).
///
/// - `writes`: Store (4) **and** DataArray (9) agreed. `TODO(verify)`: no ETP
///   capability says a server is read-only (an ACL refuses at the first put),
///   so this says "the session can write", not "the server will accept it".
/// - `transactions`: Transaction (18) agreed - it is only when the client
///   offered it (`retp_client::writing_client`).
/// - `several_dataspaces`: Dataspace (24) agreed.
/// - `subarrays`, `uninitialized_arrays`: DataArray (9) agreed; ETP 1.2 13
///   defines `PutDataSubarrays` and `PutUninitializedDataArrays` as messages
///   of the protocol and states no optional message there.
/// - `store_times`, `edge_kinds`, `native_context`: Discovery (3) agreed - the
///   times and kinds are fields of its records and `GetResources` is the
///   native context.
/// - `deletions_kept_for`: the server's `ChangeRetentionPeriod` when it
///   **stated** one. The specification's default is 86 400 s and says stores
///   "MUST support retention of tombstones" (8.2.1), so an unstated period may
///   mean the default; `TODO(verify)`: until that is settled it is `None`
///   (unknown), and `deleted_resources` is still tried, since it is a message
///   every Discovery store answers.
/// - `raw_formats`: the formats agreed for the session, those storage knows
///   (`xml`, `json`), when Store (4) was agreed. `TODO(verify)`: which JSON
///   the server means by `json` (the RDDMS convention or another).
/// - `changes`: false. `TODO(verify)`: set by whoever implements
///   `EnergymlChanges` over `retp_client::notifications`.
// The record is `#[non_exhaustive]`: built from its default and its fields set.
#[allow(clippy::field_reassign_with_default)]
pub fn capabilities_of(negotiated: &Negotiated) -> Capabilities {
    let agreed = |protocol: i32| negotiated.protocols.contains_key(&protocol);
    let mut capabilities = Capabilities::default();
    capabilities.writes = agreed(STORE) && agreed(DATA_ARRAY);
    capabilities.transactions = agreed(TRANSACTION);
    capabilities.several_dataspaces = agreed(DATASPACE);
    capabilities.subarrays = agreed(DATA_ARRAY);
    capabilities.uninitialized_arrays = agreed(DATA_ARRAY);
    capabilities.store_times = agreed(DISCOVERY);
    capabilities.edge_kinds = agreed(DISCOVERY);
    capabilities.native_context = agreed(DISCOVERY);
    if agreed(DISCOVERY) && negotiated.theirs.sent(names::CHANGE_RETENTION_PERIOD) {
        capabilities.deletions_kept_for = negotiated
            .theirs
            .long(names::CHANGE_RETENTION_PERIOD)
            .and_then(|seconds| u64::try_from(seconds).ok());
    }
    if agreed(STORE) {
        for format in &negotiated.formats {
            if let Some(raw) = RawFormat::from_etp(format)
                && !capabilities.raw_formats.contains(&raw)
            {
                capabilities.raw_formats.push(raw);
            }
        }
    }
    capabilities
}

// ---------------------------------------------------------------------------
// The exchanges
// ---------------------------------------------------------------------------

/// What a Discovery answer held, gathered from its messages.
#[derive(Default)]
struct Gathered {
    resources: Vec<Resource>,
    edges: Vec<EtpEdge>,
    /// The exception that ended the request, if one did (one for the whole
    /// request, 3.7.3).
    ended_by: Option<ErrorInfo>,
}

fn ended_by(delivery: &Delivery) -> Option<ErrorInfo> {
    match delivery.exception()? {
        Exception::Single(error) => Some(error),
        // Discovery is not a map request: a map exception is a server fault,
        // read as its first error rather than as success.
        Exception::Map(errors) => errors.into_values().next(),
    }
}

fn gather(deliveries: &[Delivery]) -> Gathered {
    let mut out = Gathered::default();
    for delivery in deliveries {
        if let Some(error) = ended_by(delivery) {
            if out.ended_by.is_none() {
                out.ended_by = Some(error);
            }
            continue;
        }
        if let Body::Message(message) = &delivery.body {
            match message {
                Message::GetResourcesResponse(r) => out.resources.extend(r.resources.iter().cloned()),
                Message::GetResourcesEdgesResponse(r) => out.edges.extend(r.edges.iter().cloned()),
                _ => {}
            }
        }
    }
    out
}

impl<T: Transport, C: Clock> EtpStore<T, C> {
    /// A listed resource as a summary: [`summary_from_resource`] with the
    /// content type the registry gives its type. `summary()` of `lib.rs`
    /// delegates here so that `resources()`, `headers()` and a context carry
    /// the same fields.
    pub(crate) fn summary_of(&self, resource: &Resource) -> Option<ObjectSummary> {
        let parsed = EtpUri::parse(&resource.uri).ok()?;
        let qualified_type = qualified_type_of(&parsed)?;
        let content_type = self
            .registry
            .by_qualified_type(&qualified_type)
            .and_then(|entry| entry.info.content_type)
            .unwrap_or_default();
        summary_from_resource(resource, content_type)
    }

    /// A request URI this store may send: canonical (3.7.4.2), and in this
    /// store's dataspace. Another dataspace is not found here, as
    /// `energyml_storage::context::discover` answers it.
    fn check_request_uri(&self, uri: &str) -> Result<()> {
        check_uri(uri, UriUse::CanonicalOnly, false).map_err(|incident| Error::BadUri {
            uri: uri.to_string(),
            reason: incident.to_string(),
        })?;
        let parse = |value: &str| {
            EtpUri::parse(value).map_err(|why| Error::BadUri {
                uri: value.to_string(),
                reason: why.to_string(),
            })
        };
        if parse(uri)?.dataspace != parse(&self.dataspace)?.dataspace {
            return Err(Error::NotFound(uri.to_string()));
        }
        Ok(())
    }

    /// Send one Discovery request and gather its answer. An exception ends it
    /// as an error: `about` is the URI it was about (`ENOT_FOUND` is that
    /// URI's own `NotFound`, `EINVALID_URI` a `BadUri`, the rest the server's
    /// words), and a partial answer is dropped, not returned as if whole.
    fn ask(&self, request: &GetResources, about: &str) -> Result<Gathered> {
        let deliveries = self.client().call(request).map_err(remote)?;
        let gathered = gather(&deliveries);
        match &gathered.ended_by {
            Some(error) => Err(refused(about, error)),
            None => Ok(gathered),
        }
    }

    /// ETP edges as storage edges, saying once how many had a kind ETP forbids.
    fn convert_edges(&self, edges: &[EtpEdge]) -> Vec<Edge> {
        let both = edges
            .iter()
            .filter(|edge| edge.relationship_kind == RelationshipKind::Both)
            .count();
        if both > 0 {
            self.record(Diagnostic::warning(
                &self.dataspace,
                format!(
                    "{both} edge(s) came with relationshipKind Both, which an edge cannot have (ETP 1.2 23.34.13); their kind is left unstated"
                ),
            ));
        }
        edges.iter().map(edge_from_etp).collect()
    }

    /// `resources_in`: one `GetResources` with the context, its scope, the
    /// navigable edges, the secondary flags, the types, the
    /// `storeLastWriteFilter` and `includeEdges = true` (8.3.1).
    ///
    /// The edges kept are those whose two ends are among the resources, as
    /// [`Found`] says: 8.1.1.1.4 step 4 removes the edges "not connected to a
    /// node in the final set", which keeps an edge with **one** end in it, so
    /// the others are dropped here and counted in a note.
    /// `TODO(verify)`: whether step 4 means one end or both.
    ///
    /// A resource that is not a data object URI is left out with a warning, as
    /// `resources()` does.
    // `Found` is `#[non_exhaustive]`: built from its default and its fields set.
    #[allow(clippy::field_reassign_with_default)]
    pub(crate) fn native_resources_in(&self, context: &Context) -> Result<Found> {
        self.check_request_uri(&context.uri)?;
        let request = resources_request(context, true)?;
        let gathered = self.ask(&request, &context.uri)?;

        let mut resources = Vec::with_capacity(gathered.resources.len());
        for resource in &gathered.resources {
            match self.summary_of(resource) {
                Some(summary) => resources.push(summary),
                None => self.record(Diagnostic::warning(
                    &resource.uri,
                    "listed, and not a data object URI this store can address; left out",
                )),
            }
        }
        let edges = self.convert_edges(&gathered.edges);
        let held: HashSet<&str> = resources.iter().map(|summary| summary.uri.as_str()).collect();
        let (kept, dropped): (Vec<Edge>, Vec<Edge>) = edges
            .into_iter()
            .partition(|edge| held.contains(edge.from.as_str()) && held.contains(edge.to.as_str()));
        if !dropped.is_empty() {
            self.record(Diagnostic::note(
                &context.uri,
                format!(
                    "{} edge(s) with an end outside the resources found were left out",
                    dropped.len()
                ),
            ));
        }
        let mut found = Found::default();
        found.resources = resources;
        found.edges = kept;
        Ok(found)
    }

    /// `edges`: the edges the server states, from its own index and no object
    /// read.
    ///
    /// Empty `uris`: one `GetResources` on the dataspace with `includeEdges`,
    /// every edge it returns. Otherwise one request per object (`targetsOrSelf`,
    /// depth 1, both kinds), sent together, keeping the edges that **leave**
    /// that object - what deriving them from the object's references gives. An
    /// object the server does not have is that URI's `NotFound`, where the
    /// derived default would leave it out silently.
    pub(crate) fn native_edges(&self, uris: &[String]) -> Result<Vec<Edge>> {
        if uris.is_empty() {
            let context = Context::dataspace(self.dataspace.clone());
            let request = resources_request(&context, true)?;
            let gathered = self.ask(&request, &self.dataspace)?;
            return Ok(self.convert_edges(&gathered.edges));
        }
        let mut requests = Vec::with_capacity(uris.len());
        for uri in uris {
            self.check_request_uri(uri)?;
            if is_dataspace_uri(uri)? {
                return Err(Error::BadUri {
                    uri: uri.clone(),
                    reason: "a dataspace, where the edges of an object are asked".into(),
                });
            }
            requests.push(resources_request(
                &Context::of(uri.clone(), Scope::TargetsOrSelf),
                true,
            )?);
        }
        let answers = self.client().call_all(&requests);
        let mut out = Vec::new();
        for (uri, answer) in uris.iter().zip(answers) {
            let gathered = gather(&answer.map_err(remote)?);
            if let Some(error) = &gathered.ended_by {
                return Err(refused(uri, error));
            }
            let (leaving, other): (Vec<Edge>, Vec<Edge>) = self
                .convert_edges(&gathered.edges)
                .into_iter()
                .partition(|edge| &edge.from == uri);
            if !other.is_empty() {
                self.record(Diagnostic::note(
                    uri,
                    format!("{} edge(s) not leaving this object were left out", other.len()),
                ));
            }
            out.extend(leaving);
        }
        Ok(out)
    }

    /// `get_raw`: `GetDataObjects` with `format`, the bytes as the server
    /// sends them.
    ///
    /// A format the session did not agree (5.3.2) is refused by name for every
    /// URI, before the server is asked. A URI that is not canonical is that
    /// URI's `BadUri` (3.7.4); an object the server answers in another format
    /// than asked is an error, never relabelled.
    pub(crate) fn raw_objects(&self, uris: &[String], format: RawFormat) -> Vec<Result<RawObject>> {
        let word = format.as_etp();
        let agreed = self
            .client()
            .negotiated()
            .formats
            .iter()
            .find(|agreed| agreed.eq_ignore_ascii_case(word))
            .cloned();
        let Some(agreed) = agreed else {
            return uris
                .iter()
                .map(|_| {
                    Err(Error::Unsupported {
                        operation: "reading objects as bytes in a format the session did not agree",
                        store: KIND,
                    })
                })
                .collect();
        };
        let wanted: Vec<String> = uris
            .iter()
            .filter(|uri| check_uri(uri, UriUse::CanonicalOnly, false).is_ok())
            .cloned()
            .collect();
        let answers = if wanted.is_empty() {
            Ok(Default::default())
        } else {
            retp_client::blocking(get_data_objects_in_batches(
                &mut *self.client(),
                &wanted,
                &agreed,
                pack::BATCH,
            ))
        };
        let answers = match answers {
            Ok(answers) => answers,
            Err(why) => {
                let reason = why.to_string();
                return uris.iter().map(|_| Err(remote(&reason))).collect();
            }
        };
        uris.iter()
            .map(|uri| {
                if check_uri(uri, UriUse::CanonicalOnly, false).is_err() {
                    return Err(Error::BadUri {
                        uri: uri.clone(),
                        reason: "not a canonical URI (3.7.4)".into(),
                    });
                }
                match answers.get(uri) {
                    Some(Ok(object)) if object.format.eq_ignore_ascii_case(word) => {
                        Ok(RawObject::new(uri.clone(), format, object.data.clone()))
                    }
                    Some(Ok(object)) => Err(remote(format!(
                        "{uri}: asked in `{word}`, the server answered in `{}`",
                        object.format
                    ))),
                    Some(Err(error)) => Err(refused(uri, error)),
                    None => Err(Error::NotFound(uri.clone())),
                }
            })
            .collect()
    }

    /// `deleted_resources`: `GetDeletedResources` on the dataspace (8.3.4), at
    /// the dataspace level only ("the discovery MUST be done at the dataspace
    /// level").
    ///
    /// An exception is an error, `ERETENTION_PERIOD_EXCEEDED` (5001) among
    /// them: a filter older than the server keeps is never answered as "none
    /// deleted". When the server announced an `earliestRetainedChangeTime`
    /// later than the filter, the answer may miss older deletions, and a
    /// warning says so.
    pub(crate) fn deleted(&self, filter: &DeletedFilter) -> Result<Vec<DeletedResource>> {
        self.check_request_uri(&filter.dataspace)?;
        if !is_dataspace_uri(&filter.dataspace)? {
            return Err(Error::BadUri {
                uri: filter.dataspace.clone(),
                reason: "an object, where GetDeletedResources takes a dataspace (8.3.4)".into(),
            });
        }
        let earliest = self.client().negotiated().peer_earliest_retained;
        if earliest > 0 && filter.after.unwrap_or(i64::MIN) < earliest {
            self.record(Diagnostic::warning(
                &filter.dataspace,
                format!(
                    "the server retains deletions only from {earliest} (earliestRetainedChangeTime); older ones may be missing from the answer"
                ),
            ));
        }
        let request = GetDeletedResources {
            dataspace_uri: filter.dataspace.clone(),
            delete_time_filter: filter.after,
            data_object_types: filter.types.clone(),
        };
        let deliveries = self.client().call(&request).map_err(remote)?;
        let mut deleted = Vec::new();
        let mut ended = None;
        for delivery in &deliveries {
            if let Some(error) = ended_by(delivery) {
                ended.get_or_insert(error);
                continue;
            }
            if let Body::Message(Message::GetDeletedResourcesResponse(r)) = &delivery.body {
                deleted.extend(
                    r.deleted_resources
                        .iter()
                        .map(|d| DeletedResource::new(d.uri.clone(), d.deleted_time)),
                );
            }
        }
        match ended {
            Some(error) => Err(refused(&filter.dataspace, &error)),
            None => Ok(deleted),
        }
    }

    /// `capabilities`: [`capabilities_of`] the session this store holds.
    pub(crate) fn etp_capabilities(&self) -> Capabilities {
        capabilities_of(self.client().negotiated())
    }
}
