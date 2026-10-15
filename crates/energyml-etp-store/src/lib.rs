//! The bridge: an ETP v1.2 dataspace behind [`EnergymlStorage`] (tier 4).
//!
//! What reads a file reads a server, with no line of it changed: the mother
//! trait was drawn after ETP's requests, and this is the implementation it
//! was drawn for. Each method is one exchange of the client -
//!
//! | the trait | ETP |
//! |---|---|
//! | `dataspaces` | Dataspace (24), `GetDataspaces` |
//! | `resources`, `headers` | Discovery (3), `GetResources` - a header's title is the resource's name, no object is read |
//! | `get_objects` | Store (4), `GetDataObjects`, then the registry |
//! | `supported_types` | SupportedTypes (25), `GetSupportedTypes` |
//! | `arrays_of` | nothing: `energyml_storage::array::locate_with` over the object, proxies resolved from the listing |
//! | `array_metadata`, `read_array` | DataArray (9), metadata first, then whole or by sub-arrays |
//!
//! **The client is blocking and the trait is not.** A method locks the
//! session, does its exchange and gives it back; for the command line and the
//! tests that is all there is. The front end will bring a transport of its own
//! (tier 6).
//!
//! **A failure is said, never an empty answer**: a server that
//! refuses a request gives [`Error::Remote`], an object it does not have gives
//! that object's own [`Error::NotFound`], and a listing cut short is a
//! diagnostic.

#![forbid(unsafe_code)]

use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use energyml_core::identifiers::EtpUri;
use energyml_core::identity;
use energyml_core::reflect::TypeInfo;
use energyml_core::registry::{DynObject, ReadMode, Registry};
use energyml_core::value::Reflect;
use energyml_storage::array::{
    ArrayData, ArrayLocation, ArrayMetadata, ArrayValues, ProxyTarget, locate_with,
};
use energyml_storage::error::{Diagnostic, Error, ExternalDataReason, Result};
use energyml_storage::resource::{Dataspace, ObjectHeader, ObjectSummary};
use energyml_storage::storage::EnergymlStorage;
use retp_client::arrays::{
    ArrayFailure, Block, Decoded, array_metadata as etp_array_metadata, read_regions, reading_type,
};
use retp_client::asynchronous::get_data_objects_in_batches;
use retp_client::{
    Client, Clock, ResourceQuery, Transport, dataspace_uri, list_dataspaces, list_resources,
    list_supported_types,
};
use retp_proto::uris::{UriUse, check_uri};
use retp_types::datatypes::data_array_types::DataArrayIdentifier;
use retp_types::datatypes::object::{ContextScopeKind, Resource};
use retp_types::datatypes::{AnyLogicalArrayType, ErrorInfo};

/// Blocks and declarations of arrays, for the writer.
mod blocks;
/// The reading additions on a dataspace: what the server states (store times,
/// edge kinds), contexts answered natively, objects as bytes, deletions.
pub mod discovery;
pub mod pack;
pub mod push;
/// The writing traits of `energyml-storage` on a dataspace: objects, arrays,
/// transactions, and the dataspaces of the session.
mod writer;

/// What this store calls itself.
pub const KIND: &str = "etp";

/// A message size for a session where no end announced one - DataArray's
/// budget falls back on it.
pub const DEFAULT_MESSAGE: u64 = 16 * 1024 * 1024;

/// One ETP dataspace, read through the mother trait.
pub struct EtpStore<T: Transport, C: Clock> {
    session: Mutex<Client<T, C>>,
    dataspace: String,
    registry: Arc<Registry>,
    mode: ReadMode,
    listing: Mutex<Option<Vec<Resource>>>,
    diagnostics: Mutex<Vec<Diagnostic>>,
    /// The transaction `EnergymlWriter::begin` opened on the server, if any:
    /// its `transactionUuid` and what was asked.
    writer_transaction: Option<energyml_storage::Transaction>,
    /// The form objects are written in (`EnergymlWriter::put_objects`): XML for
    /// an ETP store, JSON for a Reservoir DDMS REST API, which refuses XML.
    write_format: push::WriteFormat,
}

fn remote(why: impl std::fmt::Display) -> Error {
    Error::Remote {
        store: KIND,
        reason: why.to_string(),
    }
}

/// What the server said of one URI, as the trait says it: not found is not
/// found, a URI it refuses is a bad URI, the rest is its own words.
fn refused(uri: &str, error: &ErrorInfo) -> Error {
    match error.code {
        11 => Error::NotFound(uri.to_string()),
        9 => Error::BadUri {
            uri: uri.to_string(),
            reason: error.message.clone(),
        },
        code => remote(format!("{uri}: error {code}: {}", error.message)),
    }
}

/// `resqml20.obj_PointSetRepresentation` from a canonical object URI.
pub(crate) fn qualified_type_of(uri: &EtpUri) -> Option<String> {
    Some(format!(
        "{}{}.{}",
        uri.domain.as_deref()?,
        uri.version.as_deref()?,
        uri.object_type.as_deref()?
    ))
}

/// The storage's name for an element type.
fn element_type(logical: AnyLogicalArrayType) -> &'static str {
    use AnyLogicalArrayType as L;
    match logical {
        L::ArrayOfDouble64Le | L::ArrayOfDouble64Be => "double",
        L::ArrayOfFloat32Le | L::ArrayOfFloat32Be => "float",
        L::ArrayOfInt64Le | L::ArrayOfInt64Be => "int64",
        L::ArrayOfInt32Le | L::ArrayOfInt32Be => "int32",
        L::ArrayOfInt16Le | L::ArrayOfInt16Be => "int16",
        L::ArrayOfInt8 => "int8",
        L::ArrayOfUInt64Le | L::ArrayOfUInt64Be => "uint64",
        L::ArrayOfUInt32Le | L::ArrayOfUInt32Be => "uint32",
        L::ArrayOfUInt16Le | L::ArrayOfUInt16Be => "uint16",
        L::ArrayOfUInt8 | L::ArrayOfBoolean => "uint8",
        L::ArrayOfString => "string",
        L::ArrayOfCustom => "custom",
    }
}

impl<T: Transport, C: Clock> EtpStore<T, C> {
    /// The dataspace `dataspace` - its path (`carl/teapot`) or its URI, the
    /// default one when empty - of the session `client` holds, its objects
    /// read with `registry`.
    pub fn new(client: Client<T, C>, dataspace: &str, registry: Arc<Registry>) -> Self {
        Self {
            session: Mutex::new(client),
            dataspace: dataspace_uri(dataspace),
            registry,
            mode: ReadMode::Tolerant,
            listing: Mutex::new(None),
            diagnostics: Mutex::new(Vec::new()),
            writer_transaction: None,
            write_format: push::WriteFormat::Xml,
        }
    }

    /// The form objects are written in: XML by default (an ETP store), JSON for
    /// a store that takes only the EnergyML JSON encoding (the Reservoir DDMS
    /// REST API).
    pub fn write_format(mut self, format: push::WriteFormat) -> Self {
        self.write_format = format;
        self
    }

    /// How objects are read (tolerant by default, like an EPC).
    pub fn read_mode(mut self, mode: ReadMode) -> Self {
        self.mode = mode;
        self
    }

    /// The dataspace's canonical URI.
    pub fn dataspace(&self) -> &str {
        &self.dataspace
    }

    /// The session back, to close it or to go on with it.
    pub fn into_client(self) -> Client<T, C> {
        self.session
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn client(&self) -> MutexGuard<'_, Client<T, C>> {
        self.session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn record(&self, diagnostic: Diagnostic) {
        self.diagnostics
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(diagnostic);
    }

    /// What the dataspace holds, asked once and kept: the headers and the
    /// proxies are answered from it, with no object on the wire.
    fn listing(&self) -> Result<Vec<Resource>> {
        if let Some(listed) = self.listing.lock().unwrap_or_else(|p| p.into_inner()).as_ref() {
            return Ok(listed.clone());
        }
        let listed =
            list_resources(&mut *self.client(), &ResourceQuery::new(&self.dataspace)).map_err(remote)?;
        if let Some(error) = &listed.ended_by {
            self.record(Diagnostic::warning(
                &self.dataspace,
                format!(
                    "the listing was cut short: error {}: {}",
                    error.code, error.message
                ),
            ));
        }
        *self.listing.lock().unwrap_or_else(|p| p.into_inner()) = Some(listed.resources.clone());
        Ok(listed.resources)
    }

    /// The URI of an object of this dataspace.
    fn uri_for(&self, qualified_type: &str, uuid: &str) -> String {
        format!("{}{qualified_type}({uuid})", self.object_prefix())
    }

    /// `eml:///` or `eml:///dataspace('x')/`: what goes before an object.
    fn object_prefix(&self) -> String {
        if self.dataspace.ends_with('/') {
            self.dataspace.clone()
        } else {
            format!("{}/", self.dataspace)
        }
    }

    fn summary(&self, resource: &Resource) -> Option<ObjectSummary> {
        self.summary_of(resource)
    }

    /// Read one object's bytes as the type its URI names - or, when this
    /// build does not know that spelling (an RDDMS server writes
    /// `resqml20.ObjTectonicBoundaryFeature`), as the type its root element
    /// names, said in a diagnostic.
    fn read(&self, uri: &str, bytes: &[u8]) -> Result<Box<dyn DynObject>> {
        let (object, said) = read_object(&self.registry, self.mode, uri, bytes)?;
        for what in said {
            self.record(Diagnostic::warning(uri, what));
        }
        Ok(object)
    }

    /// Where an array lives, as DataArray names it.
    ///
    /// The ETP address when the location has one (`ArrayLocation::etp`), the
    /// file field otherwise - where this store has always put the URI.
    fn identifier(location: &ArrayLocation) -> DataArrayIdentifier {
        let (uri, path) = location
            .etp_address()
            .unwrap_or((location.file.as_str(), location.path_in_file.as_str()));
        DataArrayIdentifier {
            uri: uri.to_string(),
            path_in_resource: path.to_string(),
        }
    }

    fn external(location: &ArrayLocation, reason: ExternalDataReason) -> Error {
        Error::ExternalData {
            location: format!("{} {}", location.file, location.path_in_file),
            reason,
        }
    }

    fn unresolved(location: &ArrayLocation) -> Option<Error> {
        (location.file.is_empty() && location.resource_uri.is_none()).then(|| {
            Self::external(
                location,
                ExternalDataReason::Backend(format!(
                    "its proxy `{}` is not in the dataspace",
                    location.proxy_uuid.as_deref().unwrap_or("?")
                )),
            )
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<T: Transport + Send, C: Clock + Send> EnergymlStorage for EtpStore<T, C> {
    fn store_kind(&self) -> &'static str {
        KIND
    }

    async fn dataspaces(&self) -> Result<Vec<Dataspace>> {
        let listed = list_dataspaces(&mut *self.client()).map_err(remote)?;
        if let Some(error) = &listed.ended_by {
            return Err(remote(format!(
                "the dataspaces could not all be listed: error {}: {}",
                error.code, error.message
            )));
        }
        let mut out = Vec::with_capacity(listed.items.len());
        for listed in &listed.items {
            let (dataspace, left) = discovery::dataspace_from_etp_said(listed);
            if !left.is_empty() {
                self.record(Diagnostic::warning(
                    &dataspace.uri,
                    format!(
                        "custom data of a form storage does not hold, left out: {}",
                        left.join(", ")
                    ),
                ));
            }
            out.push(dataspace);
        }
        Ok(out)
    }

    async fn resources(&self) -> Result<Vec<ObjectSummary>> {
        let listed = self.listing()?;
        let mut out = Vec::with_capacity(listed.len());
        for resource in &listed {
            match self.summary(resource) {
                Some(summary) => out.push(summary),
                None => self.record(Diagnostic::warning(
                    &resource.uri,
                    "listed, and not a data object URI this store can address; left out",
                )),
            }
        }
        Ok(out)
    }

    async fn headers(&self, uris: &[String]) -> Vec<Result<ObjectHeader>> {
        let listed = match self.listing() {
            Ok(listed) => listed,
            Err(why) => {
                let reason = why.to_string();
                return uris.iter().map(|_| Err(remote(&reason))).collect();
            }
        };
        uris.iter()
            .map(|uri| {
                let resource = listed
                    .iter()
                    .find(|r| &r.uri == uri)
                    .ok_or_else(|| Error::NotFound(uri.clone()))?;
                let summary = self.summary(resource).ok_or_else(|| Error::BadUri {
                    uri: uri.clone(),
                    reason: "not a data object URI".into(),
                })?;
                let object_version = EtpUri::parse(uri).ok().and_then(|u| u.object_version);
                Ok({
                    let mut header = ObjectHeader::new(summary);
                    header.title = Some(resource.name.clone()).filter(|t| !t.is_empty());
                    header.object_version = object_version;
                    header
                })
            })
            .collect()
    }

    async fn get_objects(&self, uris: &[String]) -> Vec<Result<Box<dyn DynObject>>> {
        // Discovery and Store take canonical URIs (§3.7.4): anything else is
        // that URI's own error, and the others are still asked.
        let (asked, refused_here): (Vec<&String>, Vec<&String>) = uris
            .iter()
            .partition(|uri| check_uri(uri, UriUse::CanonicalOnly, false).is_ok());
        let wanted: Vec<String> = asked.into_iter().cloned().collect();
        let answers = if wanted.is_empty() {
            Ok(Default::default())
        } else {
            retp_client::blocking(get_data_objects_in_batches(
                &mut *self.client(),
                &wanted,
                "xml",
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
                if refused_here.contains(&uri) {
                    return Err(Error::BadUri {
                        uri: uri.clone(),
                        reason: "not a canonical URI (§3.7.4)".into(),
                    });
                }
                match answers.get(uri) {
                    Some(Ok(object)) => self.read(uri, &object.data),
                    Some(Err(error)) => Err(refused(uri, error)),
                    None => Err(Error::NotFound(uri.clone())),
                }
            })
            .collect()
    }

    async fn supported_types(&self) -> Vec<&'static TypeInfo> {
        let answered = list_supported_types(
            &mut *self.client(),
            &self.dataspace,
            ContextScopeKind::Targets,
            false,
            true,
        );
        let named: Vec<String> = match answered {
            Ok(list) => list.items.into_iter().map(|t| t.data_object_type).collect(),
            Err(why) => {
                // No SupportedTypes: what the session agreed is what there is.
                self.record(Diagnostic::note(
                    &self.dataspace,
                    format!("the supported types are the session's, the server did not list them: {why}"),
                ));
                self.client()
                    .negotiated()
                    .data_objects
                    .iter()
                    .map(|d| d.qualified_type.clone())
                    .collect()
            }
        };
        let covered = |info: &TypeInfo| {
            let Some(qualified) = info.qualified_type else {
                return false;
            };
            named.iter().any(|n| match n.strip_suffix('*') {
                Some(family) => qualified.starts_with(family),
                None => self
                    .registry
                    .by_qualified_type(n)
                    .is_some_and(|e| std::ptr::eq(e.info, info)),
            })
        };
        self.registry
            .top_level_types()
            .filter(|info| covered(info))
            .collect()
    }

    fn arrays_of(&self, object: &dyn Reflect) -> Vec<ArrayLocation> {
        let listing = self.listing().unwrap_or_default();
        let resolver = |proxy_uuid: &str| {
            listing
                .iter()
                .find(|r| {
                    EtpUri::parse(&r.uri)
                        .ok()
                        .and_then(|u| u.uuid)
                        .is_some_and(|u| u.eq_ignore_ascii_case(proxy_uuid))
                })
                .map(|r| ProxyTarget::file(r.uri.clone()))
        };
        let mut located = locate_with(object, &resolver);
        // The eml 2.3 form names a file; over ETP an array is addressed by the
        // object that declares it.
        let own = identity::uuid_of_reflect(object)
            .zip(object.type_info().qualified_type)
            .map(|(uuid, qualified)| self.uri_for(qualified, uuid));
        for location in &mut located {
            if location.proxy_uuid.is_none()
                && let Some(own) = &own
            {
                location.file = own.clone();
            }
        }
        located
    }

    async fn array_metadata(&self, location: &ArrayLocation) -> Result<ArrayMetadata> {
        if let Some(error) = Self::unresolved(location) {
            return Err(error);
        }
        let mut client = self.client();
        let tolerance = client.tolerance();
        let answer = etp_array_metadata(&mut *client, &[Self::identifier(location)])
            .map_err(remote)?
            .pop()
            .ok_or_else(|| Self::external(location, ExternalDataReason::Backend("no answer".into())))?;
        match answer {
            Ok(meta) => {
                let logical = reading_type(
                    &location.path_in_file,
                    meta.logical_array_type,
                    meta.transport_array_type,
                    tolerance,
                )
                .map_err(|failure| {
                    Self::external(location, ExternalDataReason::Backend(failure.to_string()))
                })?;
                Ok(discovery::array_metadata_from_etp(&meta, logical))
            }
            Err(error) if error.code == 11 => Err(Self::external(
                location,
                ExternalDataReason::PathMissing(location.path_in_file.clone()),
            )),
            Err(error) => Err(Self::external(
                location,
                ExternalDataReason::Backend(format!("error {}: {}", error.code, error.message)),
            )),
        }
    }

    async fn read_array(&self, location: &ArrayLocation) -> Result<ArrayData> {
        if let Some(error) = Self::unresolved(location) {
            return Err(error);
        }
        let region = (!location.start.is_empty() || !location.count.is_empty()).then(|| Block {
            starts: location.start.iter().map(|s| (*s).max(0) as u64).collect(),
            counts: location.count.iter().map(|c| (*c).max(0) as u64).collect(),
        });
        let answer = read_regions(
            &mut *self.client(),
            &[(Self::identifier(location), region)],
            DEFAULT_MESSAGE,
            |_| {},
        )
        .map_err(remote)?
        .pop()
        .ok_or_else(|| Self::external(location, ExternalDataReason::Backend("no answer".into())))?;
        let array = match answer {
            Ok(array) => array,
            Err(ArrayFailure::Store(error)) if error.code == 11 => {
                return Err(Self::external(
                    location,
                    ExternalDataReason::PathMissing(location.path_in_file.clone()),
                ));
            }
            Err(failure) => {
                return Err(Self::external(
                    location,
                    ExternalDataReason::Backend(failure.to_string()),
                ));
            }
        };
        let values = match array.values.decode(array.logical) {
            Some(Decoded::F64(v)) => ArrayValues::F64(v),
            Some(Decoded::F32(v)) => ArrayValues::F32(v),
            Some(Decoded::I64(v)) => ArrayValues::I64(v),
            Some(Decoded::I32(v)) => ArrayValues::I32(v),
            Some(Decoded::I16(v)) => ArrayValues::I16(v),
            Some(Decoded::I8(v)) => ArrayValues::I8(v),
            Some(Decoded::U64(v)) => ArrayValues::U64(v),
            Some(Decoded::U32(v)) => ArrayValues::U32(v),
            Some(Decoded::U16(v)) => ArrayValues::U16(v),
            Some(Decoded::U8(v)) => ArrayValues::U8(v),
            // Flags as the bytes HDF5 keeps them in.
            Some(Decoded::Booleans(v)) => ArrayValues::U8(v.into_iter().map(u8::from).collect()),
            Some(Decoded::Strings(_)) | None => {
                return Err(Self::external(
                    location,
                    ExternalDataReason::Backend(format!(
                        "{:?} values are not numbers, and a storage array holds numbers",
                        array.logical
                    )),
                ));
            }
        };
        Ok(ArrayData {
            metadata: ArrayMetadata::new(
                array.dimensions.iter().map(|d| *d as i64).collect(),
                element_type(array.logical).to_string(),
            ),
            values,
        })
    }

    fn capabilities(&self) -> energyml_storage::capabilities::Capabilities {
        self.etp_capabilities()
    }

    async fn get_raw(
        &self,
        uris: &[String],
        format: energyml_storage::raw::RawFormat,
    ) -> Vec<Result<energyml_storage::raw::RawObject>> {
        self.raw_objects(uris, format)
    }

    async fn resources_in(
        &self,
        context: &energyml_storage::context::Context,
    ) -> Result<energyml_storage::context::Found> {
        self.native_resources_in(context)
    }

    /// One request instead of reading every object: what the storage trait
    /// asks an ETP client to do.
    async fn edges(&self, uris: &[String]) -> Result<Vec<energyml_storage::graph::Edge>> {
        self.native_edges(uris)
    }

    async fn deleted_resources(
        &self,
        filter: &energyml_storage::context::DeletedFilter,
    ) -> Result<Vec<energyml_storage::context::DeletedResource>> {
        self.deleted(filter)
    }

    fn diagnostics(&self) -> Vec<Diagnostic> {
        self.diagnostics.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

/// One object's bytes as the type its URI names - or, when this build does
/// not know that spelling (an RDDMS server writes `resqml20.ObjTectonicBoundaryFeature`),
/// as the type its root element names. What was said of the reading - a type
/// read under another name, a field defaulted or repaired - comes back with
/// it: a reader that keeps quiet about what it had to assume is a reader
/// nobody can check.
pub fn read_object(
    registry: &Registry,
    mode: ReadMode,
    uri: &str,
    bytes: &[u8],
) -> Result<(Box<dyn DynObject>, Vec<String>)> {
    let parsed = EtpUri::parse(uri).map_err(|why| Error::BadUri {
        uri: uri.to_string(),
        reason: why.to_string(),
    })?;
    let named = qualified_type_of(&parsed).ok_or_else(|| Error::BadUri {
        uri: uri.to_string(),
        reason: "it names no data object".into(),
    })?;
    let mut said = Vec::new();
    let entry = match registry.by_qualified_type(&named) {
        Some(entry) => entry,
        None => {
            let root = energyml_storage::epc::header::peek(bytes).root_name;
            let family = format!(
                "{}{}",
                parsed.domain.as_deref().unwrap_or(""),
                parsed.version.as_deref().unwrap_or("")
            );
            let rooted = format!("{family}.{root}");
            let entry = registry
                .by_qualified_type(&rooted)
                .ok_or_else(|| Error::UnknownType(named.clone()))?;
            said.push(format!(
                "the URI names type `{named}`, which is not an Energistics type; read as its root element says, `{rooted}`"
            ));
            entry
        }
    };
    let (object, departures) = registry.read_xml_as_named(entry.info, bytes, mode)?;
    said.extend(departures.defaulted.into_iter().map(|item| item.to_string()));
    said.extend(departures.repaired.into_iter().map(|item| item.to_string()));
    Ok((object, said))
}

/// An array as storage keeps it: numbers of the logical type the store gave.
/// Strings are not an array a package holds.
pub fn values_of(array: &retp_client::arrays::Array) -> std::result::Result<ArrayValues, String> {
    match array.values.decode(array.logical) {
        Some(Decoded::F64(v)) => Ok(ArrayValues::F64(v)),
        Some(Decoded::F32(v)) => Ok(ArrayValues::F32(v)),
        Some(Decoded::I64(v)) => Ok(ArrayValues::I64(v)),
        Some(Decoded::I32(v)) => Ok(ArrayValues::I32(v)),
        Some(Decoded::I16(v)) => Ok(ArrayValues::I16(v)),
        Some(Decoded::I8(v)) => Ok(ArrayValues::I8(v)),
        Some(Decoded::U64(v)) => Ok(ArrayValues::U64(v)),
        Some(Decoded::U32(v)) => Ok(ArrayValues::U32(v)),
        Some(Decoded::U16(v)) => Ok(ArrayValues::U16(v)),
        Some(Decoded::U8(v)) => Ok(ArrayValues::U8(v)),
        // Flags as the bytes HDF5 keeps them in.
        Some(Decoded::Booleans(v)) => Ok(ArrayValues::U8(v.into_iter().map(u8::from).collect())),
        Some(Decoded::Strings(_)) | None => Err(format!(
            "{:?} values are not numbers, and a storage array holds numbers",
            array.logical
        )),
    }
}
