//! A dataspace as an EPC, in memory: what the first button of a front end's ETP
//! bar downloads, and what a front end opens as its **working copy** - the methods of a front end's
//! session go on reading a package, whether it came from a file or from a
//! server.
//!
//! Written once, for any [`Caller`]: the blocking client of the command line
//! and the front end's asynchronous one build the same package, by either
//! transport. The arrays are **not** written here - a package's arrays live
//! in HDF5, which neither this crate nor a browser writes - they come back
//! beside the archive, for the caller to write where it can: `hdf5` natively,
//! `h5wasm` in a front end.

use std::collections::{BTreeMap, BTreeSet};

use energyml_core::identifiers::EtpUri;
use energyml_core::identity;
use energyml_core::registry::{DynObject, Registry};
use energyml_storage::array::{ArrayLocation, ArrayValues, ProxyTarget, locate_with};
use energyml_storage::epc::core_props::CoreProperties;
use energyml_storage::epc::write::{Package, write_package};
use retp_client::asynchronous::{get_data_objects_in_batches, list_resources, read_regions};
use retp_client::{Caller, ClientError, ResourceQuery, dataspace_uri};
use retp_types::datatypes::data_array_types::DataArrayIdentifier;

use crate::{DEFAULT_MESSAGE, read_object, values_of};

/// How many objects are asked for in one message. A store may cut a plural
/// answer at its `MaxResponseCount` (§3.3.2.3) - asked in handfuls, none is
/// ever cut.
pub const BATCH: usize = 64;

/// What to take of the dataspace.
#[derive(Debug, Clone, Default)]
pub struct Wanted {
    /// Only these types (`resqml20.*`, `eml23.obj_…`); every type when empty.
    pub types: Vec<String>,
    /// Only these objects, by canonical URI; the whole dataspace when empty.
    pub uris: Vec<String>,
    /// Read the arrays the objects name, too.
    pub arrays: bool,
    /// Also what the kept objects reference, and what those reference, while
    /// the dataspace holds it - a package that dangles is a package whose
    /// representation has no CRS and whose property has no kind.
    pub references: bool,
}

/// One array of the package, as HDF5 will hold it.
#[derive(Debug, Clone, PartialEq)]
pub struct PackedArray {
    /// The file the package names for it - what the caller must write.
    pub file: String,
    /// Its path in that file, as the object spells it.
    pub path: String,
    /// Its shape.
    pub dimensions: Vec<u64>,
    /// Its values, in the logical type the store gave them.
    pub values: ArrayValues,
}

/// What a read brought back, before anything is written.
///
/// The bytes **as the server sent them** beside the objects read from them:
/// a front end adds an object to a package it already has, and handing it the
/// parsed object would mean writing the XML back out - a second serializer
/// on a path that has one.
#[derive(Default)]
pub struct Gathered {
    /// Each object's URI, in the order they were taken.
    pub uris: Vec<String>,
    /// Each object, read.
    pub objects: Vec<Box<dyn DynObject>>,
    /// Each object's bytes, as they came.
    pub raw: Vec<Vec<u8>>,
    /// The arrays those objects name, when they were asked for.
    pub arrays: Vec<PackedArray>,
    /// What could not be taken, and why.
    pub left_out: Vec<String>,
    /// Each listed object's `storeLastWrite`, by its URI in the dataspace -
    /// what the caller knew of it when it took it, and what a push compares
    /// the store against before overwriting (`push::Options::expected`).
    /// A store that says no time for an object leaves it out.
    pub taken: BTreeMap<String, i64>,
}

/// A dataspace, packed.
#[derive(Debug, Default)]
pub struct Packed {
    /// The archive itself.
    pub epc: Vec<u8>,
    /// Its arrays, to be written into the files the objects name.
    pub arrays: Vec<PackedArray>,
    /// How many objects the package holds.
    pub objects: usize,
    /// What could not be taken, and why - an object that does not read, an
    /// array the store refused. Never silence: a thinner package than the
    /// dataspace is a fact the caller is told.
    pub left_out: Vec<String>,
    /// [`Gathered::taken`]: each listed object's `storeLastWrite`.
    pub taken: BTreeMap<String, i64>,
}

/// Everything of `dataspace` that `wanted` asks for, as an EPC.
pub async fn pack<K: Caller>(
    client: &mut K,
    dataspace: &str,
    registry: &Registry,
    wanted: &Wanted,
) -> Result<Packed, ClientError> {
    let gathered = gather(client, dataspace, registry, wanted).await?;
    let mut packed = Packed {
        objects: gathered.objects.len(),
        arrays: gathered.arrays,
        left_out: gathered.left_out,
        taken: gathered.taken,
        ..Packed::default()
    };
    let external_files = files_of(&gathered.objects);
    let core = CoreProperties::new(
        "energyml-rust, from an ETP dataspace",
        retp_client::rest::iso(client_now()),
    );
    let package = Package {
        external_files,
        ..Package::new(gathered.objects.iter().map(AsRef::as_ref).collect(), core)
    };
    let mut diagnostics = Vec::new();
    let mut epc = std::io::Cursor::new(Vec::new());
    write_package(&package, &mut epc, &mut diagnostics).map_err(|why| {
        ClientError::Invalid(
            retp_proto::wire::EtpError::EINVALID_OBJECT.with_detail(format!("writing the package: {why}")),
        )
    })?;
    packed
        .left_out
        .extend(diagnostics.into_iter().map(|d| d.to_string()));
    packed.epc = epc.into_inner();
    Ok(packed)
}

/// The file each `EpcExternalPartReference` among these objects stands for.
fn files_of(objects: &[Box<dyn DynObject>]) -> BTreeMap<String, String> {
    let mut files = BTreeMap::new();
    for object in objects {
        if object
            .type_info()
            .qualified_type
            .is_some_and(|kind| kind.ends_with("EpcExternalPartReference"))
            && let Some(uuid) = identity::uuid_of_reflect(object.as_reflect())
        {
            files.insert(uuid.to_string(), format!("{uuid}.h5"));
        }
    }
    files
}

/// The same reading, stopping before the package is written - what a front end
/// uses to add a few objects to the package it already has (**listing is
/// not reading**, and an object arrives when somebody opens it).
pub async fn gather<K: Caller>(
    client: &mut K,
    dataspace: &str,
    registry: &Registry,
    wanted: &Wanted,
) -> Result<Gathered, ClientError> {
    // The whole dataspace, whatever is wanted of it: an array hangs from an
    // `EpcExternalPartReference` that no filter would name, and a package
    // whose proxy is missing is a package whose arrays have no file.
    let uri = dataspace_uri(dataspace);
    let listing = list_resources(client, &ResourceQuery::new(&uri)).await?;
    let all: Vec<String> = listing
        .resources
        .iter()
        .map(|resource| resource.uri.clone())
        .collect();
    let taken: BTreeMap<String, i64> = listing
        .resources
        .iter()
        .filter(|resource| resource.store_last_write > 0)
        .map(|resource| (resource.uri.clone(), resource.store_last_write))
        .collect();
    let asked: BTreeSet<&str> = wanted.uris.iter().map(String::as_str).collect();
    let held: Vec<String> = listing
        .resources
        .iter()
        .filter(|resource| asked.is_empty() || asked.contains(resource.uri.as_str()))
        .filter(|resource| wanted.types.is_empty() || of_a_wanted_type(&resource.uri, &wanted.types))
        .map(|resource| resource.uri.clone())
        .collect();
    let mut packed = Packed::default();
    for uri in &wanted.uris {
        if !held.contains(uri) {
            packed
                .left_out
                .push(format!("{uri}: the dataspace does not hold it"));
        }
    }

    let mut objects: Vec<Box<dyn DynObject>> = Vec::new();
    let mut uris: Vec<String> = Vec::new();
    let mut raw: Vec<Vec<u8>> = Vec::new();
    take(
        client,
        &held,
        registry,
        &mut objects,
        &mut uris,
        &mut raw,
        &mut packed.left_out,
    )
    .await?;

    if wanted.references {
        // What they name, and what that names, while the dataspace has it.
        // It ends: the dataspace is finite, and nothing is taken twice.
        loop {
            let more = referenced(&objects, &uris, &all);
            if more.is_empty() {
                break;
            }
            take(
                client,
                &more,
                registry,
                &mut objects,
                &mut uris,
                &mut raw,
                &mut packed.left_out,
            )
            .await?;
        }
    }

    // The file each `EpcExternalPartReference` stands for. Its own uuid names
    // it: a dataspace says nothing of file names, and two proxies must not
    // land in one file.
    let mut external_files: BTreeMap<String, String> = BTreeMap::new();
    for (uri, object) in uris.iter().zip(&objects) {
        if object
            .type_info()
            .qualified_type
            .is_some_and(|kind| kind.ends_with("EpcExternalPartReference"))
            && let Some(uuid) = identity::uuid_of_reflect(object.as_reflect())
        {
            external_files.insert(uuid.to_string(), format!("{uuid}.h5"));
            let _ = uri;
        }
    }

    if wanted.arrays {
        let located = locations(&uris, &objects, &all);
        // The proxies those arrays hang from belong in the package, whatever
        // was asked for: without its `EpcExternalPartReference` part, an
        // object naming a file names nothing.
        let wanted_proxies: BTreeSet<String> = located
            .iter()
            .filter_map(|(_, location)| location.proxy_uuid.clone())
            .filter(|uuid| !external_files.contains_key(uuid))
            .collect();
        let missing: Vec<String> = wanted_proxies
            .iter()
            .filter_map(|uuid| {
                all.iter().find(|uri| {
                    EtpUri::parse(uri)
                        .ok()
                        .and_then(|u| u.uuid)
                        .is_some_and(|u| u.eq_ignore_ascii_case(uuid))
                })
            })
            .cloned()
            .collect();
        let taken = objects.len();
        take(
            client,
            &missing,
            registry,
            &mut objects,
            &mut uris,
            &mut raw,
            &mut packed.left_out,
        )
        .await?;
        for object in &objects[taken..] {
            if let Some(uuid) = identity::uuid_of_reflect(object.as_reflect()) {
                external_files.insert(uuid.to_string(), format!("{uuid}.h5"));
            }
        }
        packed.arrays = read(client, &located, &external_files, &mut packed.left_out).await?;
    }

    Ok(Gathered {
        uris,
        objects,
        raw,
        arrays: packed.arrays,
        left_out: packed.left_out,
        taken,
    })
}

/// Read these objects into the package, in handfuls, and say which could not
/// be taken rather than leaving a package quietly thinner than the dataspace.
async fn take<K: Caller>(
    client: &mut K,
    wanted: &[String],
    registry: &Registry,
    objects: &mut Vec<Box<dyn DynObject>>,
    uris: &mut Vec<String>,
    raw: &mut Vec<Vec<u8>>,
    left_out: &mut Vec<String>,
) -> Result<(), ClientError> {
    // The handfuls of `BATCH` go out together, `at_once` of them in flight
    // (the parallel batches); a handful that fails loses
    // only its own objects, each said by name.
    let answers = get_data_objects_in_batches(client, wanted, "xml", BATCH).await?;
    for uri in wanted {
        match answers.get(uri) {
            Some(Ok(object)) => match read_object(registry, Default::default(), uri, &object.data) {
                Ok((read, said)) => {
                    left_out.extend(said);
                    objects.push(read);
                    // The bytes as the server sent them: a front end adds an
                    // object to a package it already has, and writing the
                    // parsed object back out would be a second serializer
                    // on a path that has one.
                    raw.push(object.data.clone());
                    uris.push(uri.clone());
                }
                Err(why) => left_out.push(format!("{uri}: {why}")),
            },
            Some(Err(error)) => left_out.push(format!("{uri}: {}", error.message)),
            None => left_out.push(format!("{uri}: the server answered nothing for it")),
        }
    }
    Ok(())
}

/// What these objects reference and the package does not hold yet, as the
/// dataspace addresses it. A reference the dataspace does not hold is left
/// alone: it is the checker's to report, not this one's to invent.
fn referenced(objects: &[Box<dyn DynObject>], held: &[String], all: &[String]) -> Vec<String> {
    let uuid_of = |uri: &String| {
        EtpUri::parse(uri)
            .ok()
            .and_then(|parsed| parsed.uuid)
            .map(|uuid| uuid.to_ascii_lowercase())
    };
    let known: BTreeSet<String> = held.iter().filter_map(uuid_of).collect();
    let mut wanted: BTreeSet<String> = BTreeSet::new();
    for object in objects {
        for edge in identity::collect_refs(object.as_reflect()) {
            let uuid = edge.target.uuid.to_ascii_lowercase();
            if !uuid.is_empty() && !known.contains(&uuid) {
                wanted.insert(uuid);
            }
        }
    }
    all.iter()
        .filter(|uri| uuid_of(uri).is_some_and(|uuid| wanted.contains(&uuid)))
        .cloned()
        .collect()
}

/// Whether the object a URI names is of one of these types. A store filters
/// on its own; the filter is applied here too, the listing this reads being
/// the whole dataspace.
fn of_a_wanted_type(uri: &str, types: &[String]) -> bool {
    let Some(kind) = EtpUri::parse(uri)
        .ok()
        .as_ref()
        .and_then(crate::qualified_type_of)
    else {
        return false;
    };
    types.iter().any(|pattern| covers(pattern, &kind))
}

/// Whether `pattern` - a qualified type, or a family with a `*` - names
/// `kind`.
fn covers(pattern: &str, kind: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(family) => kind.starts_with(family),
        None => kind.eq_ignore_ascii_case(pattern),
    }
}

/// Where the arrays of these objects are, as the dataspace addresses them:
/// through an `EpcExternalPartReference` before eml 2.3, through the object
/// itself from 2.3 on.
fn locations(
    uris: &[String],
    objects: &[Box<dyn DynObject>],
    listed: &[String],
) -> Vec<(String, ArrayLocation)> {
    let resolver = |proxy_uuid: &str| {
        listed
            .iter()
            .find(|uri| {
                EtpUri::parse(uri)
                    .ok()
                    .and_then(|u| u.uuid)
                    .is_some_and(|u| u.eq_ignore_ascii_case(proxy_uuid))
            })
            .map(|uri| ProxyTarget::file(uri.clone()))
    };
    let mut out = Vec::new();
    for (uri, object) in uris.iter().zip(objects) {
        for mut location in locate_with(object.as_reflect(), &resolver) {
            if location.proxy_uuid.is_none() {
                location.file = uri.clone();
            }
            out.push((uri.clone(), location));
        }
    }
    out
}

/// Read them all, and say which could not be read rather than writing a
/// package whose arrays are quietly missing.
async fn read<K: Caller>(
    client: &mut K,
    located: &[(String, ArrayLocation)],
    external_files: &BTreeMap<String, String>,
    left_out: &mut Vec<String>,
) -> Result<Vec<PackedArray>, ClientError> {
    let asked: Vec<(DataArrayIdentifier, Option<retp_client::arrays::Block>)> = located
        .iter()
        .map(|(_, location)| {
            (
                DataArrayIdentifier {
                    uri: location.file.clone(),
                    path_in_resource: location.path_in_file.clone(),
                },
                None,
            )
        })
        .collect();
    if asked.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for (answer, (owner, location)) in read_regions(client, &asked, DEFAULT_MESSAGE, |_| {})
        .await?
        .into_iter()
        .zip(located)
    {
        let array = match answer {
            Ok(array) => array,
            Err(why) => {
                left_out.push(format!("{owner} {}: {why}", location.path_in_file));
                continue;
            }
        };
        let file = match file_of(location, external_files) {
            Some(file) => file,
            None => {
                left_out.push(format!(
                    "{owner} {}: no file is named for it",
                    location.path_in_file
                ));
                continue;
            }
        };
        match values_of(&array) {
            Ok(values) => out.push(PackedArray {
                file,
                path: location.path_in_file.clone(),
                dimensions: array.dimensions.clone(),
                values,
            }),
            Err(why) => left_out.push(format!("{owner} {}: {why}", location.path_in_file)),
        }
    }
    Ok(out)
}

/// The file an array goes into: its proxy's, or one named after the object
/// that declares it (eml 2.3 and later, where there is no proxy).
fn file_of(location: &ArrayLocation, external_files: &BTreeMap<String, String>) -> Option<String> {
    if let Some(proxy) = &location.proxy_uuid {
        return external_files.get(proxy).cloned();
    }
    let uuid = EtpUri::parse(&location.file).ok()?.uuid?;
    Some(format!("{uuid}.h5"))
}

/// The moment the package says it was made. Reading a clock is the caller's
/// in this workspace; a package needs a date, and this is the one place a
/// date is written without one being given.
fn client_now() -> i64 {
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| i64::try_from(since.as_micros()).unwrap_or(0))
    }
    #[cfg(target_arch = "wasm32")]
    {
        0
    }
}
