//! A store sent into a dataspace, in **islands of transactions**.
//!
//! The parade against a Reservoir DDMS that hangs up
//! without a word during a heavy request: **over-cut**. A model with big
//! arrays and many objects leaves in *many* transactions rather than one, so
//! that a connection lost halfway costs one island and not the model
//! (a heavy push is cut into islands of transactions). The source is anything behind [`EnergymlStorage`] - an
//! EPC, another dataspace, another server -, which is what makes this the
//! inverse of [`crate::pack`].
//!
//! # What an island is, and in which order they go
//!
//! Two rules, and the first wins over the second:
//!
//! 1. **targets before sources.** An object goes after everything it
//!    references - its CRS, its interpretation, its proxy - so that no island
//!    ever asks a store to hold a reference to something it has not seen. The
//!    open question was whether a store refuses such a reference, accepts it
//!    dangling, or checks it; the answer here is not to find out. A cycle -
//!    two objects naming each other, which RESQML allows - stays in **one**
//!    island, since no order can split it;
//! 2. **what carries no array goes first, and in islands of its own**, as far
//!    as the first rule allows. Proposed: the XML-only objects that
//!    reference nobody, or only other XML-only objects - features,
//!    interpretations, CRS - in one transaction or a few, then the
//!    representations and properties, which reference those and carry the
//!    arrays, in smaller ones. So among everything whose targets are already
//!    out, what carries no array goes before what does; and an island is
//!    **cut** when the kind changes, never reordered against the first rule.
//!
//! An object and its arrays are **never apart**: the arrays hang off the
//! object, they are written in its island, after its XML and before the
//! commit - a store refuses to commit an object whose arrays it does not
//! hold, and that refusal is the one this order exists to avoid.
//!
//! Then a budget - so many objects, so many bytes - cuts the rest. Its
//! defaults are a starting point to **measure against**, not a finding.
//!
//! # What leaves the source
//!
//! A reference to an object the source does not hold - a `PropertyKind` of
//! the official dictionary, most often - is not the push's to invent. But a
//! store that checks references at commit may refuse for it, and *« reference
//! missing »* is all it will say. So the report **names** every reference
//! that leaves the source, and a refused commit points at them.
//!
//! And when a store does want them - a commit error such as 'reference
//! missing' may come from exactly this - [`WellKnown::Push`] sends the
//! dictionary's kinds the source references and does not hold, **with their
//! ancestry**: a kind names its parent, which names its own, up to the root
//! that names itself. They are written as the dictionary writes them, from the
//! five fields the build compacted it to ([`energyml_domain::property_kind`])
//! and the dictionary's own citation, and they go out ahead of what references
//! them like any other target.
//!
//! # What stops a push
//!
//! An island that the store refuses - a transaction it will not open, a write
//! it will not take, a commit it will not do - **stops the push there**, and
//! the report says what was sent, what failed and why, and what was left. Not
//! because a model half sent is acceptable, but because carrying on would
//! send islands whose references point at the one that failed.
//!
//! [`Report::diagnosis`] sorts the stop for a caller that acts on it: its
//! kind (refused, failed, timed out, gave up, journal), the store's own words
//! and code apart from the hint this side adds, what those words name when a
//! known server is documented to use them ([`Cause`]), and, for a missing
//! reference, the references that may be missing ([`Diagnosis::candidates`]).
//!
//! # The plan, without a session
//!
//! [`dry_run`] is the same push up to the point where it would ask the
//! destination anything: the islands it would send, with their cycles, their
//! objects' URIs in the destination, what is left out and what leaves the
//! source - one code path, so that the plan is the one the push follows.
//!
//! # Starting again
//!
//! Four things, each the caller's choice and each off by default, make a
//! push that stopped one that can go on:
//!
//! - **what is already there is left out** ([`Options::skip_present`]):
//!   before the plan, the destination dataspace is listed, and an object it
//!   holds under the same URI - with each of its arrays answering metadata
//!   of the same dimensions - is not sent again. The content is **not**
//!   compared: a store rewrites what it is given (its own citation fields,
//!   its own XML), so two writings of one object do not compare, and an
//!   object changed in the package since it was sent goes unseen. That is
//!   the choice, named; a listing the store cut short is refused by name;
//! - **a journal** ([`Options::journal`], [`journal`]): each island is
//!   recorded in a file as it commits, and a push started again with the
//!   same journal leaves out what it records. A journal written for another
//!   package, or another dataspace, is refused by name;
//! - **a refused island is split** ([`Options::split`]): halved, or cut into
//!   its objects one by one, and the pieces sent in its place, so that what
//!   the store would take goes and the refusal is narrowed to what it will
//!   not; the last refusal still lands in [`Report::refusals`];
//! - **a fallen connection is reopened** ([`push_resumable`]): the runner of
//!   long operations (`retp_client::resume`, ADR-0003) opens a new session
//!   and the push goes on from the first island not committed, which is
//!   redone **whole** - a session that died with its transaction open did
//!   not commit it (TODO(verify): the ETP v1.2 section that says a store
//!   rolls back a transaction its session left open), and writing an object
//!   again replaces it (`PutDataObjects` is an upsert, ETP v1.2 section
//!   9.1.1, p.178). An array written again over one the store holds - a
//!   store without transactions keeps what an interrupted island wrote - is
//!   taken to replace it too (TODO(verify): the DataArray section, protocol
//!   9, that says what `PutDataArrays` does with an array already there).

pub mod journal;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;

use energyml_core::identifiers::EtpUri;
use energyml_core::registry::{DynObject, to_part};
use energyml_core::xml::XmlWriteOptions;
use energyml_storage::array::{ArrayData, ArrayLocation, ArrayValues};
use energyml_storage::graph::Graph;
use energyml_storage::resource::ObjectSummary;
use energyml_storage::storage::EnergymlStorage;
use retp_client::arrays::{Array, ArrayFailure, ArrayValues as Wire, Grouping};
use retp_client::asynchronous::{
    array_metadata, commit_transaction, deleted_since, list_resources, put_data_objects,
    rollback_transaction, start_transaction, write_arrays_with, written_since,
};
use retp_client::resume::{
    self, Came, Connector, Operation, Retry, RunError, RunEvent, StepFailure, Stepped,
};
use retp_client::{Blocking, Caller, ClientError, Ended, Opened, ResourceQuery, dataspace_uri};
use retp_proto::wire::EtpError;
use retp_types::datatypes::data_array_types::DataArrayIdentifier;
use retp_types::datatypes::object::{DataObject, Resource};
use retp_types::datatypes::{AnyArrayType, AnyLogicalArrayType};

use crate::{DEFAULT_MESSAGE, qualified_type_of};

/// What a push is told: how much per island, in which form, and whether
/// the dictionary's kinds go with it.
#[derive(Clone)]
pub struct Options<'a> {
    /// How much one island may hold.
    pub budget: Budget,
    /// XML for an ETP store, JSON for the Reservoir DDMS REST API.
    pub format: WriteFormat,
    /// Whether the official dictionary's `PropertyKind`s the source
    /// references without holding are pushed too.
    pub well_known: WellKnown<'a>,
    /// Only these objects of the source, by URI - what a front end sends after an
    /// edit: the three objects that changed, not the two thousand its working
    /// copy holds. `None` is everything.
    ///
    /// What they reference and that is not among them is **left where it
    /// is**, and named as leaving the source: on a store the source came
    /// from, it is already there.
    pub only: Option<&'a [String]>,
    /// How an island's arrays travel: together under the message budget
    /// and in flight (the default, as automatic as the chunking of a big
    /// object), or one per message and one after the other - the bypass.
    pub grouping: Grouping,
    /// What the caller knew of each object's `storeLastWrite` when it took
    /// it, by its URI **in the destination dataspace** (`pack::Packed::taken`).
    /// Before anything is written, the store is asked what it wrote since
    /// the earliest of them: an object it wrote after the caller took it -
    /// or deleted since - is a [`Conflict`], and the push stops there with
    /// nothing written, unless [`Options::overwrite`]. `None` checks nothing:
    /// a push from a file knows no time.
    pub expected: Option<&'a BTreeMap<String, i64>>,
    /// Write over what changed on the store anyway; the conflicts are still
    /// named in the report.
    pub overwrite: bool,
    /// Leave out what the destination dataspace already holds: an object
    /// listed there under its URI (Discovery, `GetResources`), each of its
    /// arrays answering `GetDataArrayMetadata` with the dimensions the
    /// source gives it. The content is not compared (see the module's
    /// documentation). Each object left out is in [`Report::skipped`].
    pub skip_present: bool,
    /// A file where each island is recorded as it commits; started again
    /// with the same file, the push leaves out what it records
    /// ([`journal`]). `None` keeps no record.
    pub journal: Option<&'a Path>,
    /// What becomes of an island the store refuses as a whole.
    pub split: Split,
    /// How many times the pieces of one island of the plan may be split
    /// again - a bound on how far a refusal is chased.
    pub split_limit: u32,
}

/// What becomes of an island the store refuses - a write it will not take, a
/// commit it will not do.
///
/// A cycle of objects that reference each other is never split, whatever
/// is asked: no order could send it in two.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Split {
    /// The push stops there: the island is named, its refusals listed.
    #[default]
    Off,
    /// The island is cut in two halves, sent in its place, first half first;
    /// a half refused is halved again, down to one object.
    Halve,
    /// The island is cut into its objects, each in a transaction of its own
    /// - an object with its arrays alone.
    Single,
}

/// An object the store changed since the caller took it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conflict {
    /// The object, in the destination dataspace.
    pub uri: String,
    /// Its `storeLastWrite` as the caller knew it.
    pub taken: i64,
    /// Its `storeLastWrite` on the store now - `None` when the store deleted
    /// it since.
    pub store: Option<i64>,
}

impl Default for Options<'_> {
    fn default() -> Self {
        Options {
            budget: Budget::default(),
            format: WriteFormat::Xml,
            well_known: WellKnown::Leave,
            only: None,
            grouping: Grouping::Together,
            expected: None,
            overwrite: false,
            skip_present: false,
            journal: None,
            split: Split::Off,
            split_limit: 8,
        }
    }
}

/// The official dictionary's `PropertyKind`s a source references and does
/// not hold: left to the store, or pushed with everything else.
///
/// A store should not want them - the dictionary is the vocabulary every
/// store is supposed to know - and some do: the sign is a commit refused for
/// a missing reference, which the report's hint names. `Push` needs the
/// registry that reads eml 2.3, which is how a kind becomes an object.
#[derive(Clone, Copy)]
pub enum WellKnown<'a> {
    /// Not the push's to send: the reference stays as it is, and is named.
    Leave,
    /// Sent, with their ancestry, read by this registry.
    Push(&'a energyml_core::registry::Registry),
}

/// The form the objects leave in.
///
/// An ETP store takes XML - the form an EPC holds, `to_part`'s. The Reservoir
/// DDMS REST API takes **JSON Energistics only** (`PUT …/resources`, with
/// `$type`), and refuses XML by name; the caller opened the door, so the
/// caller says. Both come off the same reflection, which is what makes them
/// the same object twice rather than two objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteFormat {
    /// `format: "xml"`, the EPC part.
    Xml,
    /// `format: "json"`, the EnergyML JSON encoding.
    Json,
}

impl WriteFormat {
    /// The word ETP uses for it (`DataObject.format`).
    pub(crate) fn name(self) -> &'static str {
        match self {
            WriteFormat::Xml => "xml",
            WriteFormat::Json => "json",
        }
    }

    /// The object written in this form - the one place an object becomes the
    /// bytes it leaves in, for `push` and for the writer (`writer.rs`).
    pub(crate) fn write(self, object: &dyn DynObject) -> std::result::Result<String, String> {
        match self {
            WriteFormat::Xml => to_part(object, XmlWriteOptions::default()).map_err(|why| why.to_string()),
            WriteFormat::Json => {
                energyml_core::json::write_string(object.as_reflect(), false).map_err(|why| why.to_string())
            }
        }
    }
}

/// How much one island may hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Budget {
    /// Objects, at most - a cycle larger than this still goes whole.
    pub objects: usize,
    /// Bytes of XML and arrays together, at most - same exception.
    pub bytes: u64,
}

impl Default for Budget {
    /// A hundred objects or thirty-two mebibytes: numbers to measure against
    /// on a server that hangs up, not a finding.
    fn default() -> Self {
        Budget {
            objects: 100,
            bytes: 32 * 1024 * 1024,
        }
    }
}

/// What one object weighs in the plan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Weight {
    /// Its XML, plus the arrays it names.
    pub bytes: u64,
    /// How many arrays it names.
    pub arrays: usize,
}

/// One transaction's worth of objects.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Island {
    /// In the order they are sent - targets before sources -, each by its
    /// URI in the **source**.
    pub uris: Vec<String>,
    /// What they weigh together.
    pub bytes: u64,
    /// How many arrays go with them.
    pub arrays: usize,
    /// The sizes of the units the island is made of, in the order of
    /// `uris`: a unit is one object, or one **cycle** - objects that
    /// reference each other, which no order can send apart and no
    /// [`Split`] cuts. A unit of more than one object is a cycle; they add
    /// up to `uris.len()`. Empty for an island built by hand, which then
    /// says nothing of its cycles.
    pub units: Vec<usize>,
}

impl Island {
    /// The URIs of each unit, in order (see [`Island::units`]).
    pub fn unit_uris(&self) -> Vec<&[String]> {
        let mut at = 0;
        let mut out = Vec::with_capacity(self.units.len());
        for &size in &self.units {
            let Some(unit) = self.uris.get(at..at + size) else {
                break;
            };
            out.push(unit);
            at += size;
        }
        out
    }

    /// The cycles of the island - its units of more than one object -, each
    /// by its URIs in the source, in order.
    pub fn cycles(&self) -> Vec<&[String]> {
        self.unit_uris()
            .into_iter()
            .filter(|unit| unit.len() > 1)
            .collect()
    }
}

/// Cut a store into islands. Pure: the graph and the weights in, the plan
/// out, nothing sent.
///
/// `resources` gives the order ties are broken in - the source's own -, and
/// every object of `weights` that the resources do not list is left out.
/// Each island says which of its objects form a cycle ([`Island::units`]).
pub fn islands(
    resources: &[ObjectSummary],
    graph: &Graph,
    weights: &BTreeMap<String, Weight>,
    budget: &Budget,
) -> Vec<Island> {
    plan(resources, graph, weights, budget)
}

/// [`islands`]: the one function both [`push`] and [`dry_run`] plan with.
fn plan(
    resources: &[ObjectSummary],
    graph: &Graph,
    weights: &BTreeMap<String, Weight>,
    budget: &Budget,
) -> Vec<Island> {
    let uris: Vec<&str> = resources.iter().map(|one| one.uri.as_str()).collect();
    let index: BTreeMap<&str, usize> = uris.iter().enumerate().map(|(i, uri)| (*uri, i)).collect();
    // `from` references `to`: an edge, in the direction a reference points.
    let mut targets: Vec<Vec<usize>> = vec![Vec::new(); uris.len()];
    for edge in &graph.edges {
        if let (Some(&from), Some(&to)) = (index.get(edge.from.as_str()), index.get(edge.to.as_str()))
            && from != to
            && !targets[from].contains(&to)
        {
            targets[from].push(to);
        }
    }

    // Strongly connected components: a cycle comes out as one, and it is
    // never split. Tarjan's own emission order already puts targets first;
    // it is not kept as the order, because it does not say *which* of two
    // unrelated components goes first, and the second rule does.
    let components = tarjan(&targets);
    let weigh = |component: &[usize]| {
        component.iter().fold(Weight::default(), |acc, &i| {
            let w = weights.get(uris[i]).copied().unwrap_or_default();
            Weight {
                bytes: acc.bytes + w.bytes,
                arrays: acc.arrays + w.arrays,
            }
        })
    };

    // The condensation: what each component references, outside itself.
    let mut of = vec![0usize; uris.len()];
    for (c, members) in components.iter().enumerate() {
        for &i in members {
            of[i] = c;
        }
    }
    let mut refs: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); components.len()];
    for (i, ts) in targets.iter().enumerate() {
        for &t in ts {
            if of[i] != of[t] {
                refs[of[i]].insert(of[t]);
            }
        }
    }
    let mut dependants: Vec<Vec<usize>> = vec![Vec::new(); components.len()];
    for (c, referenced) in refs.iter().enumerate() {
        for &t in referenced {
            dependants[t].push(c);
        }
    }

    // Kahn's order with a preference: a component is *ready* once every
    // component it references is out, and among the ready ones, **what
    // carries no array goes first**, then the source's own order. `false`
    // sorts before `true`, which is the whole trick.
    let key = |c: usize| (weigh(&components[c]).arrays > 0, components[c][0], c);
    let mut waiting: Vec<usize> = refs.iter().map(BTreeSet::len).collect();
    let mut ready: BTreeSet<(bool, usize, usize)> = (0..components.len())
        .filter(|&c| waiting[c] == 0)
        .map(key)
        .collect();
    let mut order = Vec::with_capacity(components.len());
    while let Some(&first) = ready.iter().next() {
        ready.remove(&first);
        let c = first.2;
        order.push(c);
        for &d in &dependants[c] {
            waiting[d] -= 1;
            if waiting[d] == 0 {
                ready.insert(key(d));
            }
        }
    }

    let mut out: Vec<Island> = Vec::new();
    for c in order {
        let component = &components[c];
        let weight = weigh(component);
        let carries = weight.arrays > 0;
        let fits = |island: &Island| {
            island.uris.len() + component.len() <= budget.objects
                && island.bytes + weight.bytes <= budget.bytes
        };
        let open = match out.last() {
            // The kind changing cuts an island; the order is never touched.
            Some(last) if (last.arrays > 0) == carries && fits(last) => true,
            _ => false,
        };
        if !open {
            out.push(Island::default());
        }
        let island = out.last_mut().expect("just pushed");
        island.uris.extend(component.iter().map(|&i| uris[i].to_string()));
        island.bytes += weight.bytes;
        island.arrays += weight.arrays;
        island.units.push(component.len());
    }
    out
}

/// An island cut into pieces, its units never apart: `None` when it is one
/// unit - one object, or one cycle -, which nothing can cut. The pieces keep
/// the island's order, so targets still go before sources.
fn cut(island: &Island, weights: &BTreeMap<String, Weight>, how: Split) -> Option<Vec<Island>> {
    let units = &island.units;
    if units.len() < 2 {
        return None;
    }
    let groups: Vec<Vec<usize>> = match how {
        Split::Off => return None,
        Split::Halve => {
            let half = units.len().div_ceil(2);
            vec![units[..half].to_vec(), units[half..].to_vec()]
        }
        Split::Single => units.iter().map(|&unit| vec![unit]).collect(),
    };
    let mut at = 0;
    let pieces = groups
        .into_iter()
        .map(|group| {
            let count: usize = group.iter().sum();
            let uris = island.uris[at..at + count].to_vec();
            at += count;
            let (bytes, arrays) = uris.iter().fold((0, 0), |(bytes, arrays), uri| {
                let w = weights.get(uri).copied().unwrap_or_default();
                (bytes + w.bytes, arrays + w.arrays)
            });
            Island {
                uris,
                bytes,
                arrays,
                units: group,
            }
        })
        .collect();
    Some(pieces)
}

/// Every reference that leaves the source: `(from, to)` where the source
/// does not hold `to`. Not the push's to repair - most are the official
/// dictionary's `PropertyKind`s -, but a store that checks references at
/// commit refuses for them, and the refusal has to point somewhere.
pub fn dangling(resources: &[ObjectSummary], graph: &Graph) -> Vec<(String, String)> {
    let held: BTreeSet<&str> = resources.iter().map(|one| one.uri.as_str()).collect();
    let mut out = Vec::new();
    for edge in &graph.edges {
        if held.contains(edge.from.as_str()) && !held.contains(edge.to.as_str()) {
            let pair = (edge.from.clone(), edge.to.clone());
            if !out.contains(&pair) {
                out.push(pair);
            }
        }
    }
    out
}

/// What [`well_known_kinds`] found.
pub struct FoundKinds {
    /// Each kind as an object of the source's shape: its summary, the object
    /// read by the registry, and the edges its `Parent` makes.
    pub kinds: Vec<(
        ObjectSummary,
        Box<dyn DynObject>,
        Vec<energyml_storage::graph::Edge>,
    )>,
    /// A reference that looked like a kind and could not become one - not in
    /// the dictionary, or a registry that cannot read eml 2.3. Named.
    pub left_out: Vec<String>,
}

/// The dictionary's `PropertyKind`s among the references that leave the
/// source, **with their ancestry**, as objects.
///
/// Only `eml23.PropertyKind` references are looked up: the dictionary's kinds
/// are eml 2.3 objects addressed by uuid, where RESQML 2.0.1 names a kind by
/// its word and holds no such object. Each kind is written **as the
/// dictionary writes it** - the same element, the same fields - from the five
/// fields the build compacted the dictionary to and the dictionary's own
/// citation; then read by the registry, so that what leaves is an object the
/// reader accepted and not a string this module composed. A kind's uri keeps
/// the shape of the reference that named it - with the source's dataspace,
/// or without - so that the edge from the referencing object resolves.
pub fn well_known_kinds(
    dangling: &[(String, String)],
    registry: &energyml_core::registry::Registry,
) -> FoundKinds {
    use energyml_domain::property_kind::with_default;
    let mut found = FoundKinds {
        kinds: Vec::new(),
        left_out: Vec::new(),
    };
    let mut done: BTreeSet<String> = BTreeSet::new();
    for (_, to) in dangling {
        let Ok(parsed) = EtpUri::parse(to) else { continue };
        if qualified_type_of(&parsed).as_deref() != Some("eml23.PropertyKind") {
            continue;
        }
        let Some(uuid) = parsed.uuid.clone() else { continue };
        let chain: Vec<energyml_domain::property_kind::DictionaryKind> =
            with_default(|dictionary| dictionary.ancestry(&uuid).into_iter().cloned().collect());
        if chain.is_empty() {
            found.left_out.push(format!(
                "{to}: not a kind of the official dictionary, and the source does not hold it"
            ));
            continue;
        }
        for kind in chain {
            let kind_uuid = kind.uuid.to_ascii_lowercase();
            if !done.insert(kind_uuid.clone()) {
                continue;
            }
            // The same shape as the reference that named it.
            let uri = to.replace(&uuid, &kind_uuid);
            let xml = kind_xml(&kind);
            let object = match registry.read_xml(
                "application/x-eml+xml;version=2.3;type=PropertyKind",
                xml.as_bytes(),
            ) {
                Ok(object) => object,
                Err(why) => {
                    found.left_out.push(format!(
                        "{uri}: the dictionary's kind does not read with this registry: {why}"
                    ));
                    continue;
                }
            };
            let edges = kind
                .parent
                .as_deref()
                .filter(|parent| !parent.eq_ignore_ascii_case(&kind_uuid))
                .map(|parent| {
                    energyml_storage::graph::Edge::new(
                        uri.clone(),
                        to.replace(&uuid, &parent.to_ascii_lowercase()),
                        "Parent",
                    )
                })
                .into_iter()
                .collect();
            let summary = ObjectSummary::new(uri, kind_uuid, "eml23.PropertyKind")
                .with_content_type("application/x-eml+xml;version=2.3;type=PropertyKind")
                .with_locator("PropertyKindDictionary_v2.3");
            found.kinds.push((summary, object, edges));
        }
    }
    found
}

/// A kind as the dictionary's document writes it - the element, the
/// citation the dictionary carries on every kind, the fields the compaction
/// kept. What the compaction dropped (the description) is not made up.
fn kind_xml(kind: &energyml_domain::property_kind::DictionaryKind) -> String {
    // The one door for XML text written by hand (core's `xml::escape_attr`).
    let escape = energyml_core::xml::escape_attr;
    let mut out = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<eml:PropertyKind xmlns:eml=\"http://www.energistics.org/energyml/data/commonv2\" schemaVersion=\"2.3\" uuid=\"{}\">\
<eml:Citation><eml:Title>{}</eml:Title><eml:Originator>Energistics</eml:Originator>\
<eml:Creation>2016-12-09T17:30:47Z</eml:Creation>\
<eml:Format>Energistics:PWLS Generator.ps1:2021.11.17</eml:Format>\
<eml:LastUpdate>2021-11-17T23:35:26Z</eml:LastUpdate></eml:Citation>\
<eml:IsAbstract>{}</eml:IsAbstract>",
        escape(&kind.uuid),
        escape(&kind.title),
        kind.is_abstract
    );
    if let Some(deprecated) = &kind.deprecated {
        out.push_str(&format!(
            "<eml:DeprecationDate>{}</eml:DeprecationDate>",
            escape(deprecated)
        ));
    }
    if let Some(class) = &kind.quantity_class {
        out.push_str(&format!(
            "<eml:QuantityClass>{}</eml:QuantityClass>",
            escape(class)
        ));
    }
    if let Some(parent) = &kind.parent {
        out.push_str(&format!(
            "<eml:Parent><eml:Uuid>{}</eml:Uuid><eml:QualifiedType>eml23.PropertyKind</eml:QualifiedType></eml:Parent>",
            escape(parent)
        ));
    }
    out.push_str("</eml:PropertyKind>");
    out
}

/// A refusal, with what leaves the source beside it when anything does.
///
/// A store that checks references says *« reference missing »* and no more;
/// this is the half it does not say. The dictionary is named because it is
/// the usual suspect: a store should not want its kinds pushed, and some do.
fn with_hint(why: String, dangling: &[(String, String)]) -> String {
    if dangling.is_empty() {
        return why;
    }
    let shown: Vec<String> = dangling.iter().take(3).map(|(_, to)| to.clone()).collect();
    let more = if dangling.len() > 3 {
        format!(", and {} more", dangling.len() - 3)
    } else {
        String::new()
    };
    format!(
        "{why} - {} reference(s) leave the source ({}{more}); a store that checks references at commit may want them pushed too, the official dictionary's PropertyKinds included",
        dangling.len(),
        shown.join(", ")
    )
}

/// Tarjan's strongly connected components, in the order the algorithm emits
/// them - a component after every component it reaches. Roots are visited in
/// index order, so the result is the same for the same input.
fn tarjan(targets: &[Vec<usize>]) -> Vec<Vec<usize>> {
    struct State<'a> {
        targets: &'a [Vec<usize>],
        index: Vec<Option<usize>>,
        low: Vec<usize>,
        on_stack: Vec<bool>,
        stack: Vec<usize>,
        next: usize,
        out: Vec<Vec<usize>>,
    }
    fn visit(state: &mut State<'_>, v: usize) {
        state.index[v] = Some(state.next);
        state.low[v] = state.next;
        state.next += 1;
        state.stack.push(v);
        state.on_stack[v] = true;
        for &w in state.targets[v].iter() {
            match state.index[w] {
                None => {
                    visit(state, w);
                    state.low[v] = state.low[v].min(state.low[w]);
                }
                Some(index) if state.on_stack[w] => state.low[v] = state.low[v].min(index),
                Some(_) => {}
            }
        }
        if state.low[v] == state.index[v].expect("set above") {
            let mut component = Vec::new();
            loop {
                let w = state.stack.pop().expect("v is on the stack");
                state.on_stack[w] = false;
                component.push(w);
                if w == v {
                    break;
                }
            }
            // Members in index order - the source's - so that two objects of
            // one cycle keep the order the source listed them in.
            component.sort_unstable();
            state.out.push(component);
        }
    }
    let n = targets.len();
    let mut state = State {
        targets,
        index: vec![None; n],
        low: vec![0; n],
        on_stack: vec![false; n],
        stack: Vec::new(),
        next: 0,
        out: Vec::new(),
    };
    for v in 0..n {
        if state.index[v].is_none() {
            visit(&mut state, v);
        }
    }
    state.out
}

/// Why a push stopped.
#[derive(Debug)]
pub enum Stop {
    /// The store said no, and this is what it said.
    Refused(String),
    /// The client could not go on - the connection fell, the session closed.
    /// An `Interrupted` here means the island's outcome is **unknown**.
    Failed(ClientError),
    /// [`push_resumable`] reopened the session as many times in a row as it
    /// was allowed to, and it fell each time - or would not open. The
    /// island's outcome is **unknown**: the connection may have fallen after
    /// its commit; a push started again with the journal sends it again,
    /// which replaces what is there.
    GaveUp {
        /// Failures in a row.
        attempts: u32,
        /// The last one, in words.
        last: String,
    },
    /// The journal ([`Options::journal`]) would not serve: written for
    /// another package or dataspace, unreadable, or failing to record an
    /// island that committed - in which case the push stops after it, so
    /// that the journal never says less than what is known to be there.
    Journal(String),
}

/// Where a push stopped, when it did.
#[derive(Debug)]
pub struct Stopped {
    /// The island that did not go through, by rank in [`Report::islands`].
    pub island: usize,
    /// Why.
    pub why: Stop,
    /// What the store said when it refused, **in its own words and alone** -
    /// [`Stop::Refused`] may add this side's hint to them ([`Report::dangling`]),
    /// this never does. `None` when the stop is not the store's refusal, or
    /// when the refusal is this side's (a conflict, a listing cut short).
    pub said: Option<Said>,
}

/// What a store said when it refused: its words, and its code when it gave one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Said {
    /// The ETP error code (`ErrorInfo.code`, ETP v1.2 section 24.1) - `None`
    /// for a refused commit or transaction, whose answer carries a reason
    /// and no code (`CommitTransactionResponse.failureReason`), or for a
    /// refusal of an array this side could not read.
    pub code: Option<i32>,
    /// The words, as they came (`retp_client` replaces an empty reason by
    /// a sentence of its own saying so).
    pub message: String,
}

/// The kind of a stop, for a caller that acts on it rather than reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopKind {
    /// The store refused ([`Stop::Refused`]).
    Refused,
    /// The client could not go on ([`Stop::Failed`]), for another reason than
    /// a request that timed out: the connection fell, the session closed.
    Failed,
    /// A request had no answer in time (`ClientError::TimedOut`): the session
    /// may still stand, and the island's outcome is unknown.
    Timeout,
    /// [`push_resumable`] reopened the session as many times as it was
    /// allowed to ([`Stop::GaveUp`]).
    GaveUp,
    /// The journal would not serve ([`Stop::Journal`]).
    Journal,
}

impl StopKind {
    /// Its name in a report: `refused`, `failed`, `timeout`, `gave_up`,
    /// `journal`.
    pub fn name(self) -> &'static str {
        match self {
            StopKind::Refused => "refused",
            StopKind::Failed => "failed",
            StopKind::Timeout => "timeout",
            StopKind::GaveUp => "gave_up",
            StopKind::Journal => "journal",
        }
    }
}

/// What a store's refusal says is wrong, **when the words are ones a known
/// server is documented to use** - and [`Cause::Unknown`] for everything
/// else, never a guess.
///
/// The words recognised are open-etp-server's (the OSDU Reservoir DDMS
/// server), read in its sources: the checks of its `CommitTransaction`
/// (`src/lib/oes/eml/openkv/TransProto.cpp`, lines 267-336 of the `main`
/// branch read on 2026-10-15) and the check a write outside a transaction
/// makes at once (`StoreProto.cpp`, the `local_tx.isLocal()` block after
/// line 1799). The line numbers move with the server.
///
/// TODO(verify): the same words on the wire from a live open-etp-server - the
/// shapes are read from its sources, not captured from a session; in
/// particular that a per-object refusal's `ErrorInfo.message` carries the
/// words alone, with no prefix (this library's own server side writes the
/// error's name first, which this match does not read as the words).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cause {
    /// `"{N} Missing reference(s)"` at commit (`TransProto.cpp:267-290`): `N`
    /// objects written reference something the dataspace does not hold. Or,
    /// outside a transaction, `EINVALID_STATE` `"{N} Missing reference(s) in
    /// space {space}"` at once (`StoreProto.cpp`). The server names no
    /// target: see [`Diagnosis::candidates`].
    MissingReference {
        /// How many, as the server counts them.
        count: u64,
        /// The space it names, outside a transaction.
        space: Option<String>,
    },
    /// `"{N} Orphan reference(s)"` at commit (`TransProto.cpp:277,292-298`).
    OrphanReference {
        /// How many.
        count: u64,
    },
    /// `"{N} Missing array(s)"` at commit (`TransProto.cpp:317-327`): arrays
    /// an object written names, that were never written.
    MissingArray {
        /// How many.
        count: u64,
    },
    /// `"{N} Orphan array(s)"` at commit (`TransProto.cpp:329-336`): arrays
    /// written that no object written claims.
    OrphanArray {
        /// How many.
        count: u64,
    },
    /// `EINVALID_STATE` `"Missing array container(s): ..."` at commit
    /// (`TransProto.cpp:279-281,300-304`): arrays whose object the dataspace
    /// does not hold.
    MissingArrayContainer {
        /// What follows the colon, as the server wrote it.
        containers: String,
    },
    /// Words this library knows no server to use.
    Unknown,
}

impl Cause {
    /// The cause a store's words name: the **whole** message, trimmed, must
    /// have one of the documented shapes - a message that merely contains one
    /// is not read as it, since nothing documents such a message.
    pub fn of(message: &str) -> Cause {
        let text = message.trim();
        if let Some(containers) = text.strip_prefix("Missing array container(s): ") {
            return Cause::MissingArrayContainer {
                containers: containers.to_string(),
            };
        }
        let Some((digits, rest)) = text.split_once(' ') else {
            return Cause::Unknown;
        };
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Cause::Unknown;
        }
        let Ok(count) = digits.parse::<u64>() else {
            return Cause::Unknown;
        };
        match rest {
            "Missing reference(s)" => Cause::MissingReference { count, space: None },
            "Orphan reference(s)" => Cause::OrphanReference { count },
            "Missing array(s)" => Cause::MissingArray { count },
            "Orphan array(s)" => Cause::OrphanArray { count },
            _ => match rest.strip_prefix("Missing reference(s) in space ") {
                Some(space) if !space.is_empty() => Cause::MissingReference {
                    count,
                    space: Some(space.to_string()),
                },
                _ => Cause::Unknown,
            },
        }
    }

    /// Its name in a report: `missing_reference`, `orphan_reference`,
    /// `missing_array`, `orphan_array`, `missing_array_container`, `unknown`.
    pub fn name(&self) -> &'static str {
        match self {
            Cause::MissingReference { .. } => "missing_reference",
            Cause::OrphanReference { .. } => "orphan_reference",
            Cause::MissingArray { .. } => "missing_array",
            Cause::OrphanArray { .. } => "orphan_array",
            Cause::MissingArrayContainer { .. } => "missing_array_container",
            Cause::Unknown => "unknown",
        }
    }
}

/// Why a push stopped, sorted - [`Report::diagnosis`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnosis {
    /// The island that did not go through, by rank in [`Report::islands`].
    pub island: usize,
    /// What kind of stop.
    pub kind: StopKind,
    /// For a refusal, the store's own words ([`Stopped::said`]) when it gave
    /// any, this side's sentence otherwise; for any other stop, what stopped
    /// it, in words.
    pub message: String,
    /// The ETP error code, when there is one: the store's for a refusal,
    /// the client error's for a failure or a timeout.
    pub code: Option<i32>,
    /// For a refusal, what the store's words name ([`Cause::of`] on
    /// [`Stopped::said`], never on the hint this side adds); `None` for a
    /// stop that is not a refusal.
    pub cause: Option<Cause>,
    /// For [`Cause::MissingReference`], the references that may be missing:
    /// the [`Report::dangling`] pairs whose `from` is in the stopped island.
    ///
    /// **This side's deduction, not the server's statement** - the server
    /// counts the missing references and names none. It holds because the
    /// plan sends every target the source holds *before* what references it
    /// (targets go in earlier islands, or earlier in the same one, a cycle
    /// whole), so a reference from the stopped island can only miss when its
    /// target leaves the source - and every such reference is in `dangling`,
    /// including those `only` cut, since `only` removes its objects from the
    /// source before `dangling` is computed. A target already in the
    /// dataspace is not missing, so the candidates may be more than the
    /// server's count. They may also be fewer: a target the source holds and
    /// that could not be sent ([`Report::left_out`]) is not in `dangling`, and
    /// the server may count a reference its own reading found and this
    /// library's did not. Empty for any other cause.
    pub candidates: Vec<(String, String)>,
}

impl Stopped {
    /// Its kind: a [`Stop::Failed`] on a request that timed out is a
    /// [`StopKind::Timeout`], apart from a lost connection.
    pub fn kind(&self) -> StopKind {
        match &self.why {
            Stop::Refused(_) => StopKind::Refused,
            Stop::Failed(ClientError::TimedOut { .. }) => StopKind::Timeout,
            Stop::Failed(_) => StopKind::Failed,
            Stop::GaveUp { .. } => StopKind::GaveUp,
            Stop::Journal(_) => StopKind::Journal,
        }
    }
}

/// The ETP error code a client error carries, when it carries one.
fn code_of(error: &ClientError) -> Option<i32> {
    match error {
        ClientError::Refused(incident)
        | ClientError::Invalid(incident)
        | ClientError::TimedOut { incident, .. }
        | ClientError::AnswerRefused { incident, .. } => Some(incident.code()),
        _ => None,
    }
}

impl Refusal {
    /// What its message names ([`Cause::of`]): the store's own words, so
    /// the same shapes as a refused commit's - a write outside a transaction
    /// refused at once for a missing reference, most of all.
    pub fn cause(&self) -> Cause {
        Cause::of(&self.message)
    }
}

/// What a push came to.
#[derive(Debug, Default)]
pub struct Report {
    /// The plan, in the order it was followed.
    pub islands: Vec<Island>,
    /// How many islands were committed - the first `sent` of `islands`.
    pub sent: usize,
    /// Where it stopped, or `None` when every island went through.
    pub stopped: Option<Stopped>,
    /// Objects of the source that could not be sent at all, with why - one
    /// that does not read, one whose URI names no dataspace object. Named
    /// rather than dropped: a push that quietly sends fewer objects looks
    /// exactly like a smaller model.
    pub left_out: Vec<String>,
    /// References that leave the source, `(from, to)` - see [`dangling`].
    pub dangling: Vec<(String, String)>,
    /// The session did not agree Transaction (18), so each island went as
    /// plain puts: an island refused half-way may be **partly there**, and
    /// nothing could roll it back. Said, because a push without it looks
    /// exactly like a push with it.
    pub without_transactions: bool,
    /// The objects the store changed since the caller took them
    /// (`Options::expected`). With `overwrite`, they went anyway.
    pub conflicts: Vec<Conflict>,
    /// **Every** object - or array - of the island that stopped that the
    /// store would not take, each with the store's own code and words, in
    /// the order the island sends them. [`Stop::Refused`] quotes the first
    /// only; a caller that isolates the culprit, or looks for the reference
    /// a store says is missing, reads this list, not that text.
    ///
    /// Empty when the island stopped for another reason: a transaction that
    /// would not open, a refused commit (its reason names no object), a lost
    /// connection.
    pub refusals: Vec<Refusal>,
    /// The objects not sent because the destination holds them already
    /// ([`Options::skip_present`]) or the journal records them
    /// ([`Options::journal`]), by their URI in the destination. They are in
    /// no island.
    pub skipped: Vec<Skipped>,
    /// Each island that was split and sent as pieces in its place
    /// ([`Options::split`], or a connection that kept falling on it), in the
    /// order it happened. `islands` holds the pieces.
    pub splits: Vec<SplitIsland>,
    /// How many new sessions [`push_resumable`] opened after the connection
    /// fell.
    pub reconnections: usize,
    /// How each of those sessions came back, in order - one per
    /// reconnection: [`Came::Resumed`] when the server gave the lost session
    /// back (OSDU's `ResumeSession`, asked only when the connector offers it,
    /// `retp_client::native::Resuming`), or [`Came::Fresh`] - with why the
    /// resume did not serve, when one was asked: the server's refusal
    /// (`ENOT_FOUND`, *Session not found or expired*; `EUNSUPPORTED_PROTOCOL`
    /// from a server that does not speak 2400), or the attempt's failure.
    /// Either way the island that was cut was sent again whole: a resumed
    /// session knows nothing of what was in flight.
    pub reopened: Vec<Came>,
}

impl Report {
    /// Why the push stopped, sorted ([`Diagnosis`]); `None` when every
    /// island went through.
    pub fn diagnosis(&self) -> Option<Diagnosis> {
        let stopped = self.stopped.as_ref()?;
        let kind = stopped.kind();
        let (message, code) = match (&stopped.why, &stopped.said) {
            (Stop::Refused(_), Some(said)) => (said.message.clone(), said.code),
            (Stop::Refused(why), None) => (why.clone(), None),
            (Stop::Failed(error), _) => (error.to_string(), code_of(error)),
            (Stop::GaveUp { last, .. }, _) => (last.clone(), None),
            (Stop::Journal(why), _) => (why.clone(), None),
        };
        let cause = match kind {
            // The store's words alone: never the sentence the hint was added to.
            StopKind::Refused => Some(
                stopped
                    .said
                    .as_ref()
                    .map_or(Cause::Unknown, |said| Cause::of(&said.message)),
            ),
            _ => None,
        };
        let candidates = match &cause {
            Some(Cause::MissingReference { .. }) => {
                let island: BTreeSet<&str> = self
                    .islands
                    .get(stopped.island)
                    .map(|one| one.uris.iter().map(String::as_str).collect())
                    .unwrap_or_default();
                self.dangling
                    .iter()
                    .filter(|(from, _)| island.contains(from.as_str()))
                    .cloned()
                    .collect()
            }
            _ => Vec::new(),
        };
        Some(Diagnosis {
            island: stopped.island,
            kind,
            message,
            code,
            cause,
            candidates,
        })
    }
}

/// An object left out because it is there already.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skipped {
    /// The object, in the destination dataspace.
    pub uri: String,
    /// How that is known.
    pub why: SkipReason,
}

/// How an object is known to be there already.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// The destination lists it, and holds each of its arrays with the same
    /// dimensions.
    Present,
    /// The journal records it in an island that committed.
    Journal,
}

/// An island split, and sent as pieces in its place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitIsland {
    /// Its rank in [`Report::islands`]: its pieces start there.
    pub island: usize,
    /// Its objects, as the plan names them.
    pub uris: Vec<String>,
    /// Into how many pieces.
    pub into: usize,
    /// Why it was split: the store's refusal, or the connection falling.
    pub why: String,
    /// What the store refused of it, when it did.
    pub refusals: Vec<Refusal>,
}

/// One object, or one of its arrays, that a store would not take.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    /// The object, as addressed in the destination dataspace - for an array,
    /// the object it hangs from (a RESQML 2.0.1 proxy for a 2.0.1 array).
    pub uri: String,
    /// The array's path in its object, when the refusal is an array's.
    pub array: Option<String>,
    /// The ETP error code the store answered with (`ErrorInfo.code`, ETP
    /// v1.2 section 24.1) - a per-object answer of a `ProtocolException`, or
    /// the one error of a `ProtocolException` that answered the whole
    /// request. `None` when the refusal is this side's: an array the source
    /// could not read, or that the wire cannot carry.
    pub code: Option<i32>,
    /// What was said.
    pub message: String,
}

/// The source's own URIs for what a caller names - for [`Options::only`].
///
/// Each name is a URI the source lists, the same object's URI in another
/// dataspace (matched by its type and uuid), or a bare uuid in any case. A
/// name the source does not hold is **refused** by name, as is a name that
/// matches nothing at all: a push that quietly sends less looks exactly like
/// a smaller model.
pub async fn chosen(source: &(dyn EnergymlStorage + Sync), names: &[String]) -> Result<Vec<String>, String> {
    let listed = source
        .resources()
        .await
        .map_err(|why| format!("the source could not be listed: {why}"))?;
    let mut out: Vec<String> = Vec::with_capacity(names.len());
    for name in names {
        let parsed = EtpUri::parse(name).ok();
        let (kind, uuid) = match &parsed {
            Some(uri) => (qualified_type_of(uri), uri.uuid.clone()),
            None => (None, Some(name.trim().to_string())),
        };
        let found = listed
            .iter()
            .find(|one| one.uri == *name)
            .or_else(|| {
                let uuid = uuid.as_deref()?;
                listed.iter().find(|one| {
                    one.uuid.eq_ignore_ascii_case(uuid)
                        && kind.as_deref().is_none_or(|kind| one.qualified_type == kind)
                })
            })
            .ok_or_else(|| format!("{name}: the source does not hold it"))?;
        if !out.contains(&found.uri) {
            out.push(found.uri.clone());
        }
    }
    Ok(out)
}

/// What a push says as it goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Progress {
    /// The plan is made.
    Planned {
        /// Islands to send.
        islands: usize,
        /// Objects in them.
        objects: usize,
    },
    /// One island committed.
    Sent {
        /// Its rank.
        island: usize,
        /// Of how many.
        of: usize,
    },
    /// An island was split, and its pieces take its place ([`SplitIsland`]).
    Split {
        /// Its rank: the first piece's.
        island: usize,
        /// Into how many pieces.
        into: usize,
    },
}

/// One object ready to leave: its destination URI, its XML, its arrays.
struct Ready {
    uri: String,
    name: String,
    xml: Vec<u8>,
    arrays: Vec<ReadyArray>,
}

/// One array of an object ready to leave: where the destination will hold
/// it, where the source holds it, and its dimensions as the source gives
/// them.
struct ReadyArray {
    id: DataArrayIdentifier,
    location: ArrayLocation,
    dimensions: Vec<i64>,
}

/// Everything read from the source, before the destination is asked
/// anything.
struct Prepared {
    space: String,
    /// By the object's URI in the source.
    ready: BTreeMap<String, Ready>,
    weights: BTreeMap<String, Weight>,
    /// What goes, in the source's order.
    listed: Vec<ObjectSummary>,
    graph: Graph,
    /// What the reading found to say: `left_out`, `dangling` - and, once
    /// the journal is read, `skipped`.
    report: Report,
}

impl Prepared {
    /// What the journal is keyed by ([`journal::fingerprint`]): what leaves,
    /// as it leaves.
    fn fingerprint(&self) -> String {
        journal::fingerprint(self.ready.values().map(|one| {
            (
                one.uri.as_str(),
                one.xml.as_slice(),
                one.arrays
                    .iter()
                    .map(|array| {
                        (
                            array.id.uri.as_str(),
                            array.id.path_in_resource.as_str(),
                            array.dimensions.as_slice(),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
        }))
    }

    /// Leave out what the journal records as committed - by URI in the
    /// destination -, each object named in `report.skipped`. What [`push`]
    /// does with a journal before it plans, and [`dry_run`] too.
    fn leave_out(&mut self, committed: &BTreeSet<String>) {
        let listed = std::mem::take(&mut self.listed);
        for one in listed {
            let there = self
                .ready
                .get(&one.uri)
                .is_some_and(|ready| committed.contains(&ready.uri));
            if there && let Some(gone) = self.ready.remove(&one.uri) {
                self.report.skipped.push(Skipped {
                    uri: gone.uri,
                    why: SkipReason::Journal,
                });
            } else {
                self.listed.push(one);
            }
        }
    }
}

/// How many arrays one `GetDataArrayMetadata` asks about when
/// [`Options::skip_present`] checks what is there.
const METADATA_BATCH: usize = 100;

/// What a push would send, worked out without a session ([`dry_run`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    /// The destination, as a dataspace URI.
    pub dataspace: String,
    /// The islands, in the order a push sends them, each object by its URI
    /// in the **source**, each with its units ([`Island::units`], so its
    /// cycles).
    pub islands: Vec<Island>,
    /// Each object of `islands`, by its URI in the source: its URI in the
    /// destination dataspace, as the push addresses it.
    pub destinations: BTreeMap<String, String>,
    /// Objects of the source that could not be sent at all, with why
    /// ([`Report::left_out`]).
    pub left_out: Vec<String>,
    /// References that leave the source, `(from, to)` ([`dangling`]).
    pub dangling: Vec<(String, String)>,
    /// The objects the journal records as committed ([`Options::journal`]),
    /// left out of the plan, by their URI in the destination - always
    /// [`SkipReason::Journal`].
    pub skipped: Vec<Skipped>,
}

impl Plan {
    /// The destination URIs of an island's objects, in its order.
    pub fn destinations_of(&self, island: &Island) -> Vec<&str> {
        island
            .uris
            .iter()
            .filter_map(|uri| self.destinations.get(uri).map(String::as_str))
            .collect()
    }
}

/// Why [`dry_run`] made no plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoPlan {
    /// An option only the destination can honour - [`Options::skip_present`],
    /// [`Options::expected`] -: a plan made without it would not be the one
    /// the push follows, and a caller who asked for it expects it applied.
    NeedsDestination(String),
    /// The source could not be read.
    Source(String),
    /// The journal would not serve ([`Stop::Journal`] in a push).
    Journal(String),
}

impl std::fmt::Display for NoPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NoPlan::NeedsDestination(why) | NoPlan::Source(why) => f.write_str(why),
            NoPlan::Journal(why) => write!(f, "the journal: {why}"),
        }
    }
}

impl std::error::Error for NoPlan {}

/// What [`push`] would send into `dataspace`, **without a session**: the
/// source read and written as the push writes it, the arrays located and
/// weighed, what cannot be sent named (`left_out`), the references that
/// leave the source (`dangling`), what the journal records left out - and
/// the islands of the same planning, under the same budget. Nothing is
/// asked of any server, and nothing is written: the journal is read, never
/// created nor repaired.
///
/// The same code as the push, step by step, so that the plan cannot drift
/// from what it sends: the push then follows these islands, unless the store
/// refuses one and [`Options::split`] cuts it, or the connection keeps
/// falling on one ([`push_resumable`] halves it). [`Options::grouping`],
/// `split`, `split_limit` and `overwrite` change how islands travel, not
/// which, and are not read.
///
/// The dataspace is **required**: the destination URIs are in it, and a
/// journal is written for one dataspace and refused for another.
///
/// What needs the destination is refused by name ([`NoPlan::NeedsDestination`]):
/// [`Options::skip_present`] (what the dataspace holds) and
/// [`Options::expected`] (what it changed since).
pub async fn dry_run(
    source: &(dyn EnergymlStorage + Sync),
    dataspace: &str,
    options: &Options<'_>,
) -> Result<Plan, NoPlan> {
    if options.skip_present {
        return Err(NoPlan::NeedsDestination(
            "skip_present asks the destination what it holds already, and a plan asks it nothing: plan without it, or push with it".to_string(),
        ));
    }
    if options.expected.is_some() {
        return Err(NoPlan::NeedsDestination(
            "expected asks the destination what it changed since, and a plan asks it nothing: plan without it, or push with it".to_string(),
        ));
    }
    let mut prepared = prepare(source, dataspace, options)
        .await
        .map_err(NoPlan::Source)?;
    if let Some(path) = options.journal {
        let committed =
            journal::recorded(path, &prepared.space, &prepared.fingerprint()).map_err(NoPlan::Journal)?;
        prepared.leave_out(&committed);
    }
    let islands = plan(
        &prepared.listed,
        &prepared.graph,
        &prepared.weights,
        &options.budget,
    );
    let destinations = islands
        .iter()
        .flat_map(|island| island.uris.iter())
        .filter_map(|uri| prepared.ready.get(uri).map(|one| (uri.clone(), one.uri.clone())))
        .collect();
    Ok(Plan {
        dataspace: prepared.space,
        islands,
        destinations,
        left_out: prepared.report.left_out,
        dangling: prepared.report.dangling,
        skipped: prepared.report.skipped,
    })
}

/// A source that could not be read, as [`push`] has always said it: not
/// sent.
fn not_sent(why: String) -> ClientError {
    ClientError::Invalid(EtpError::EINVALID_ARGUMENT.with_detail(why))
}

/// Send everything `source` holds into `dataspace`, island by island, over
/// one session: a connection that falls stops the push there
/// ([`Stop::Failed`]). [`push_resumable`] opens a new session instead and
/// goes on. [`dry_run`] says which islands, with no session.
pub async fn push<K: Caller>(
    client: &mut K,
    source: &(dyn EnergymlStorage + Sync),
    dataspace: &str,
    options: &Options<'_>,
    mut progress: impl FnMut(Progress),
) -> Result<Report, ClientError> {
    let prepared = prepare(source, dataspace, options).await.map_err(not_sent)?;
    let mut pushing = Pushing::new(source, options, prepared);
    while !pushing.done() {
        let advanced = pushing.advance(client, false).await;
        for said in pushing.progress.drain(..) {
            progress(said);
        }
        advanced?;
    }
    Ok(pushing.report)
}

/// [`push`], opening a new session when the connection falls, driven by the
/// runner of long operations (`retp_client::resume::run`, ADR-0003).
///
/// `session` is the session to start with, when one is open, and the one
/// left open at the end. A step that fails for a reason another try may cure
/// (`retp_client::resume::worth_retrying`: the connection fell, a request
/// timed out, an answer could not be taken) is tried again after `retry`'s
/// pause, which doubles - in a **new session** when the connection is gone,
/// opened the whole way by `connector`, whose credentials are its own; or,
/// when the connector offers it (`retp_client::native::Resuming`, off unless
/// asked), in the lost session given back by the server - OSDU's
/// `ResumeSession`, tried first, a new session at once when refused
/// ([`Report::reopened`] says which). The
/// island it was sending is sent again **whole**, from its first object, and
/// the islands committed before are not. An island the connection falls on
/// twice in a row is halved before the next try: a server that hangs up on a
/// request too heavy for it hangs up on it again. After `retry.max_retries`
/// failures in a row, the push stops ([`Stop::GaveUp`]).
///
/// The source is read once, before any session is asked anything. The
/// errors returned are [`push`]'s: a source that cannot be read.
pub fn push_resumable<C: Connector>(
    connector: &mut C,
    session: &mut Option<C::Session>,
    source: &(dyn EnergymlStorage + Sync),
    dataspace: &str,
    options: &Options<'_>,
    retry: &Retry,
    on_event: impl FnMut(RunEvent<Progress>),
) -> Result<Report, ClientError> {
    let prepared = retp_client::blocking(prepare(source, dataspace, options)).map_err(not_sent)?;
    let mut pushing = Pushing::new(source, options, prepared);
    if pushing.done() {
        return Ok(pushing.report);
    }
    match resume::run(connector, session, &mut pushing, retry, on_event) {
        Ok(()) => {}
        Err(RunError::GaveUp { attempts, last, .. }) => {
            let at = pushing.report.sent;
            pushing.stop_at(at, Stop::GaveUp { attempts, last });
        }
        Err(RunError::Refused { why, .. }) => {
            let at = pushing.report.sent;
            pushing.stop_at(at, Stop::Failed(why));
        }
        Err(RunError::Final(never)) => match never {},
    }
    Ok(pushing.report)
}

/// The [`Retry`] of [`push_resumable`] for *a new session at most `times`
/// times in a row, the first after `first_pause`*: the pause doubles after
/// each fall in a row, up to thirty seconds - or `first_pause`, when that is
/// longer. What the command line's `--reconnect` and `--reconnect-pause`
/// mean, and the Python bridge's `reconnect` and `reconnect_pause`.
pub fn reconnecting(times: u32, first_pause: Duration) -> Retry {
    Retry {
        // The runner counts failures, the first one included; `times` counts
        // the sessions opened after one.
        max_retries: times.saturating_add(1),
        first_pause,
        longest_pause: first_pause.max(Retry::default().longest_pause),
    }
}

/// Read the source: each object written as it will leave, its arrays
/// located and weighed, what cannot be sent named. Nothing is asked of the
/// destination. The first step of [`push`] and of [`dry_run`] alike.
async fn prepare(
    source: &(dyn EnergymlStorage + Sync),
    dataspace: &str,
    options: &Options<'_>,
) -> Result<Prepared, String> {
    let format = options.format;
    let space = dataspace_uri(dataspace);
    let mut resources = source
        .resources()
        .await
        .map_err(|why| format!("the source could not be listed: {why}"))?;
    // The whole listing stays for what an array needs to be addressed - a
    // RESQML 2.0.1 proxy that is not among the chosen objects still names
    // where their numbers hang - and only the chosen ones are read and sent.
    let all = resources.clone();
    if let Some(only) = options.only {
        resources.retain(|one| only.iter().any(|uri| uri == &one.uri));
    }
    // The references as the objects write them, not the edges the source
    // states: an ETP server's edges join objects it holds, so a reference that
    // leaves the source - what `dangling` reports - is in the objects only.
    let mut graph = source
        .derived_graph(&[])
        .await
        .map_err(|why| format!("the source's graph could not be read: {why}"))?;
    let uris: Vec<String> = resources.iter().map(|one| one.uri.clone()).collect();
    let mut objects = source.get_objects(&uris).await;

    // The dictionary's kinds, when asked for: one more object each, listed
    // and read like the source's own, with the edges their parents make.
    let mut report = Report::default();
    if let WellKnown::Push(registry) = options.well_known {
        let found = well_known_kinds(&dangling(&resources, &graph), registry);
        report.left_out.extend(found.left_out);
        for (summary, object, edges) in found.kinds {
            resources.push(summary);
            objects.push(Ok(object));
            graph.edges.extend(edges);
        }
    }

    // Everything an island will need, gathered once: the XML as this library
    // writes it - the round-trip campaign on real files
    // is what says that is the object -, and where each array
    // is, with its size from the metadata alone. The values are read when the
    // island leaves, not before: a model's arrays are the model.
    let mut ready: BTreeMap<String, Ready> = BTreeMap::new();
    let mut weights: BTreeMap<String, Weight> = BTreeMap::new();
    let mut listed: Vec<ObjectSummary> = Vec::new();
    for (summary, object) in resources.iter().zip(objects) {
        let object: Box<dyn DynObject> = match object {
            Ok(object) => object,
            Err(why) => {
                report
                    .left_out
                    .push(format!("{}: it does not read: {why}", summary.uri));
                continue;
            }
        };
        let uri = match in_dataspace(&summary.uri, &space) {
            Ok(uri) => uri,
            Err(why) => {
                report.left_out.push(format!("{}: {why}", summary.uri));
                continue;
            }
        };
        let written = format.write(object.as_ref());
        let xml = match written {
            Ok(text) => text.into_bytes(),
            Err(why) => {
                report.left_out.push(format!(
                    "{}: it does not write as {}: {why}",
                    summary.uri,
                    format.name()
                ));
                continue;
            }
        };
        let mut arrays = Vec::new();
        let mut bytes = xml.len() as u64;
        let mut refused = None;
        for location in source.arrays_of(object.as_reflect()) {
            let owner = match owner_of(&location, &uri, &all, &space) {
                Ok(owner) => owner,
                Err(why) => {
                    refused = Some(why);
                    break;
                }
            };
            let dimensions = match source.array_metadata(&location).await {
                Ok(meta) => {
                    let count: u64 = meta.dimensions.iter().map(|d| (*d).max(0) as u64).product();
                    bytes += count * element_bytes(&meta.element_type);
                    meta.dimensions
                }
                Err(why) => {
                    refused = Some(format!(
                        "{}: its array {} has no metadata: {why}",
                        summary.uri, location.path_in_file
                    ));
                    break;
                }
            };
            arrays.push(ReadyArray {
                id: DataArrayIdentifier {
                    uri: owner,
                    path_in_resource: location.path_in_file.clone(),
                },
                location,
                dimensions,
            });
        }
        if let Some(why) = refused {
            report.left_out.push(why);
            continue;
        }
        weights.insert(
            summary.uri.clone(),
            Weight {
                bytes,
                arrays: arrays.len(),
            },
        );
        let name = source
            .header(&summary.uri)
            .await
            .ok()
            .and_then(|h| h.title)
            .unwrap_or_default();
        ready.insert(
            summary.uri.clone(),
            Ready {
                uri,
                name,
                xml,
                arrays,
            },
        );
        listed.push(summary.clone());
    }
    report.dangling = dangling(&resources, &graph);
    Ok(Prepared {
        space,
        ready,
        weights,
        listed,
        graph,
        report,
    })
}

/// A push under way: the plan, where it stands, and what it came to so far.
/// One step at a time, so that the runner of long operations can drive it
/// ([`push_resumable`]) as well as a plain loop ([`push`]).
struct Pushing<'a> {
    source: &'a (dyn EnergymlStorage + Sync),
    options: Options<'a>,
    space: String,
    /// By the object's URI in the source; what was skipped is no longer in it.
    ready: BTreeMap<String, Ready>,
    weights: BTreeMap<String, Weight>,
    listed: Vec<ObjectSummary>,
    graph: Graph,
    journal: Option<journal::Journal>,
    report: Report,
    /// For each island of `report.islands`, how many more times it may be
    /// split for a refusal.
    splits_left: Vec<u32>,
    /// Whether the destination was asked what it must be (conflicts, what it
    /// holds), and the plan made.
    checked: bool,
    /// Whether the push stopped before its end.
    stopped: bool,
    /// A store that agreed Transaction (18) and then said it has none.
    said_no_transactions: bool,
    /// Said since it was last read.
    progress: Vec<Progress>,
}

impl<'a> Pushing<'a> {
    /// The push, the journal opened when there is one, and what it records
    /// left out. A journal that does not match stops the push before
    /// anything is asked of the destination.
    fn new(source: &'a (dyn EnergymlStorage + Sync), options: &Options<'a>, mut prepared: Prepared) -> Self {
        let mut stopped = false;
        let journal = match options.journal {
            None => None,
            Some(path) => {
                let print = prepared.fingerprint();
                match journal::Journal::open(path, &prepared.space, &print, prepared.ready.len()) {
                    Ok((journal, committed)) => {
                        prepared.leave_out(&committed);
                        Some(journal)
                    }
                    Err(why) => {
                        prepared.report.stopped = Some(Stopped {
                            island: 0,
                            why: Stop::Journal(why),
                            said: None,
                        });
                        stopped = true;
                        None
                    }
                }
            }
        };
        let Prepared {
            space,
            ready,
            weights,
            listed,
            graph,
            report,
        } = prepared;
        Pushing {
            source,
            options: options.clone(),
            space,
            ready,
            weights,
            listed,
            graph,
            journal,
            report,
            splits_left: Vec::new(),
            checked: false,
            stopped,
            said_no_transactions: false,
            progress: Vec::new(),
        }
    }

    /// Whether there is nothing left to do: stopped, or every island sent.
    fn done(&self) -> bool {
        self.stopped || (self.checked && self.report.sent >= self.report.islands.len())
    }

    /// The next step: what the destination must be asked first, then one
    /// island. `resuming` returns a failure another session may cure rather
    /// than stopping on it.
    async fn advance<K: Caller>(&mut self, client: &mut K, resuming: bool) -> Result<(), ClientError> {
        if self.checked {
            self.send_next(client, resuming).await
        } else {
            self.check(client).await
        }
    }

    /// Whether this session sends islands in transactions.
    fn transactional<K: Caller>(&self, client: &K) -> bool {
        !self.said_no_transactions && client.negotiated().protocols.contains_key(&18)
    }

    /// Before anything is written: the conflicts, what the destination
    /// holds, the plan.
    async fn check<K: Caller>(&mut self, client: &mut K) -> Result<(), ClientError> {
        // The conflict seen **before** writing: what the store wrote since
        // the caller took its objects, in one listing. An object changed there
        // is shown, not overwritten - unless the caller said so.
        let mut refusal = None;
        if let Some(expected) = self.options.expected {
            self.report.conflicts = conflicts_of(client, &self.space, &self.ready, expected).await?;
            if !self.report.conflicts.is_empty() && !self.options.overwrite {
                let named: Vec<String> = self
                    .report
                    .conflicts
                    .iter()
                    .map(|c| match c.store {
                        Some(_) => c.uri.clone(),
                        None => format!("{} (deleted there)", c.uri),
                    })
                    .collect();
                refusal = Some(format!(
                    "{} object(s) changed on the store since they were taken, nothing was written: {} - push again with overwrite to replace them",
                    named.len(),
                    named.join(", ")
                ));
            }
        }
        if self.options.skip_present && refusal.is_none() {
            match present(client, &self.space, &self.ready).await? {
                Ok(there) => {
                    let listed = std::mem::take(&mut self.listed);
                    for one in listed {
                        if there.contains(&one.uri)
                            && let Some(gone) = self.ready.remove(&one.uri)
                        {
                            self.report.skipped.push(Skipped {
                                uri: gone.uri,
                                why: SkipReason::Present,
                            });
                        } else {
                            self.listed.push(one);
                        }
                    }
                }
                Err(why) => refusal = Some(why),
            }
        }
        // The plan [`dry_run`] makes too, from what is left.
        self.report.islands = plan(&self.listed, &self.graph, &self.weights, &self.options.budget);
        self.splits_left = vec![self.options.split_limit; self.report.islands.len()];
        self.progress.push(Progress::Planned {
            islands: self.report.islands.len(),
            objects: self.listed.len(),
        });
        self.checked = true;
        match refusal {
            // This side's refusal: the store said nothing.
            Some(why) => self.stop_at(0, Stop::Refused(why)),
            None => self.report.without_transactions = !self.transactional(client),
        }
        Ok(())
    }

    /// The next island: its transaction when the store speaks them, its
    /// objects, then their arrays, then the commit - and on the first thing
    /// the store will not take, a rollback, and a split or a stop. A
    /// rollback that fails changes nothing about what is reported: the
    /// transaction is not committed either way.
    async fn send_next<K: Caller>(&mut self, client: &mut K, resuming: bool) -> Result<(), ClientError> {
        let rank = self.report.sent;
        let island = self.report.islands[rank].clone();
        // A transaction when the store speaks them - and a store that agreed
        // 18 and then says it has none (some REST APIs name the transaction
        // `null`) is taken at its word from here on. A store that did not
        // agree Transaction (18) - some RDDMS servers do not offer it - takes
        // each island as plain puts: the islands still order what goes, and
        // still bound what a cut costs; what they lose is the atomicity, and
        // the report says so.
        let transactional = self.transactional(client);
        if !transactional {
            self.report.without_transactions = true;
        }
        let transaction = if transactional {
            match start_transaction(
                client,
                std::slice::from_ref(&self.space),
                false,
                "energyml-rust push",
            )
            .await?
            {
                Opened::Open(transaction) => Some(transaction),
                Opened::Refused(why) => {
                    self.refused_at(
                        rank,
                        format!("the transaction would not open: {why}"),
                        Some(Said {
                            code: None,
                            message: why,
                        }),
                    );
                    return Ok(());
                }
                Opened::Unsupported(_) => {
                    self.said_no_transactions = true;
                    self.report.without_transactions = true;
                    None
                }
            }
        } else {
            None
        };
        let sent = send_island(
            client,
            self.source,
            &island,
            &self.ready,
            self.options.format,
            self.options.grouping,
        )
        .await;
        match sent {
            Ok(()) => match &transaction {
                None => self.committed(rank, &island),
                Some(transaction) => match commit_transaction(client, transaction).await? {
                    Ended::Done => self.committed(rank, &island),
                    Ended::Refused(said) => {
                        // The store's words kept apart from the hint, so that
                        // what classifies them never reads ours.
                        let why = with_hint(format!("the commit was refused: {said}"), &self.report.dangling);
                        let said = Said {
                            code: None,
                            message: said,
                        };
                        self.refused(rank, why, Some(said), Vec::new());
                    }
                },
            },
            Err(Halt {
                stop: Stop::Refused(why),
                refusals,
            }) => {
                // The first refusal, as the store said it: the sentence quotes
                // it, and the hint may follow.
                let said = refusals.first().map(|first| Said {
                    code: first.code,
                    message: first.message.clone(),
                });
                let why = match &transaction {
                    Some(transaction) => {
                        let _ = rollback_transaction(client, transaction).await;
                        why
                    }
                    None => with_hint(
                        format!(
                            "{why} - no transaction to roll back: what this island wrote before the refusal stays"
                        ),
                        &self.report.dangling,
                    ),
                };
                self.refused(rank, why, said, refusals);
            }
            Err(Halt {
                stop: Stop::Failed(why),
                refusals,
            }) => {
                if resuming && resume::worth_retrying(&why) {
                    // The same session is used again when it still stands (an
                    // answer that could not be taken): what this island opened
                    // in it is closed first. A session that fell took its
                    // transaction with it.
                    let stands = !why.is_connection_lost() && !matches!(why, ClientError::TimedOut { .. });
                    if stands && let Some(transaction) = &transaction {
                        let _ = rollback_transaction(client, transaction).await;
                    }
                    return Err(why);
                }
                self.report.refusals = refusals;
                self.stop_at(rank, Stop::Failed(why));
            }
            Err(Halt { stop, refusals }) => {
                self.report.refusals = refusals;
                self.stop_at(rank, stop);
            }
        }
        Ok(())
    }

    /// The island at `rank` committed: counted, said, and recorded in the
    /// journal - a record that fails stops the push after it.
    fn committed(&mut self, rank: usize, island: &Island) {
        self.report.sent += 1;
        self.progress.push(Progress::Sent {
            island: rank,
            of: self.report.islands.len(),
        });
        let uris: Vec<String> = island
            .uris
            .iter()
            .filter_map(|uri| self.ready.get(uri).map(|one| one.uri.clone()))
            .collect();
        if let Some(journal) = &mut self.journal
            && let Err(why) = journal.record(rank, &uris)
        {
            self.stop_at(
                rank + 1,
                Stop::Journal(format!(
                    "island {} committed, and the journal could not record it: {why}",
                    rank + 1
                )),
            );
        }
    }

    /// The store refused the island at `rank` as a whole: split, when asked
    /// and still allowed, or the push stops there.
    /// `said` is the store's own words, apart from the sentence `why`.
    fn refused(&mut self, rank: usize, why: String, said: Option<Said>, refusals: Vec<Refusal>) {
        let how = self.options.split;
        if how != Split::Off && self.splits_left[rank] > 0 {
            match self.split_at(rank, how, why, refusals, true) {
                Ok(()) => return,
                Err((why, refusals)) => {
                    self.report.refusals = refusals;
                    self.refused_at(rank, why, said);
                    return;
                }
            }
        }
        self.report.refusals = refusals;
        self.refused_at(rank, why, said);
    }

    /// Put the pieces of the island at `rank` in its place - or give `why`
    /// and `refusals` back when it is one unit, which nothing cuts. `spend`
    /// counts the split against [`Options::split_limit`].
    fn split_at(
        &mut self,
        rank: usize,
        how: Split,
        why: String,
        refusals: Vec<Refusal>,
        spend: bool,
    ) -> Result<(), (String, Vec<Refusal>)> {
        let island = &self.report.islands[rank];
        let Some(pieces) = cut(island, &self.weights, how) else {
            return Err((why, refusals));
        };
        let left = if spend {
            self.splits_left[rank].saturating_sub(1)
        } else {
            self.splits_left[rank]
        };
        let into = pieces.len();
        self.report.splits.push(SplitIsland {
            island: rank,
            uris: island.uris.clone(),
            into,
            why,
            refusals,
        });
        self.report.islands.splice(rank..=rank, pieces);
        self.splits_left
            .splice(rank..=rank, std::iter::repeat_n(left, into));
        self.progress.push(Progress::Split { island: rank, into });
        Ok(())
    }

    /// The push stops at the island of rank `island`, for `why`.
    fn stop_at(&mut self, island: usize, why: Stop) {
        self.report.stopped = Some(Stopped {
            island,
            why,
            said: None,
        });
        self.stopped = true;
    }

    /// The store refused the island at `island`: the sentence, and the
    /// store's own words apart ([`Stopped::said`]).
    fn refused_at(&mut self, island: usize, why: String, said: Option<Said>) {
        self.report.stopped = Some(Stopped {
            island,
            why: Stop::Refused(why),
            said,
        });
        self.stopped = true;
    }
}

/// The push as an operation of the runner (`retp_client::resume::run`): one
/// step is the check, then one island each.
impl<K: Blocking> Operation<K> for Pushing<'_> {
    type Progress = Progress;
    type Error = std::convert::Infallible;

    fn step(&mut self, client: &mut K) -> Result<Stepped, StepFailure<Self::Error>> {
        retp_client::blocking(self.advance(client, true))?;
        Ok(if self.done() {
            Stepped::Finished
        } else {
            Stepped::Progressed
        })
    }

    fn current_step(&self) -> String {
        if !self.checked {
            return format!("planning the push into {}", self.space);
        }
        let rank = self.report.sent;
        match self.report.islands.get(rank) {
            Some(island) => format!(
                "island {} of {} ({} object(s))",
                rank + 1,
                self.report.islands.len(),
                island.uris.len()
            ),
            None => format!("the end of the push into {}", self.space),
        }
    }

    /// The island the connection keeps falling on, in two halves.
    fn halve(&mut self) -> bool {
        if !self.checked || self.stopped || self.report.sent >= self.report.islands.len() {
            return false;
        }
        let rank = self.report.sent;
        self.split_at(
            rank,
            Split::Halve,
            "the connection kept falling on it".to_string(),
            Vec::new(),
            false,
        )
        .is_ok()
    }

    fn take_progress(&mut self) -> Vec<Progress> {
        std::mem::take(&mut self.progress)
    }

    fn resumed(&mut self, came: &Came, since: Option<i64>) {
        // Nothing to catch up on: the island is sent again whole, and what
        // committed before stays committed - whether the server gave the
        // session back or not, since it gives back the envelope, not the
        // work (`Connector::resume`, point 1).
        let _ = since;
        self.report.reconnections += 1;
        self.report.reopened.push(came.clone());
    }
}

/// The objects of `ready`, by their URI in the source, that the destination
/// holds already: listed under their URI in the dataspace (Discovery,
/// `GetResources`, ETP v1.2 section 8.3.1), each of their arrays answering
/// `GetDataArrayMetadata` (DataArray, protocol 9) with the dimensions the
/// source gives it.
///
/// An array the store does not answer for, or answers with other
/// dimensions, makes its object one to send: sending it again replaces what
/// is there, skipping it would lose it. A listing the store cut short cannot
/// say what is missing from it, and is refused by name (the inner `Err`).
async fn present<K: Caller>(
    client: &mut K,
    space: &str,
    ready: &BTreeMap<String, Ready>,
) -> Result<Result<BTreeSet<String>, String>, ClientError> {
    let listing = list_resources(client, &ResourceQuery::new(space)).await?;
    if let Some(error) = listing.ended_by {
        return Ok(Err(format!(
            "the destination's listing was cut short ({}: {}), so what it holds already cannot be told; nothing was written",
            error.code, error.message
        )));
    }
    let held: BTreeSet<String> = listing
        .resources
        .iter()
        .map(|resource| resource.uri.to_ascii_lowercase())
        .collect();
    let listed_there: Vec<&String> = ready
        .iter()
        .filter(|(_, one)| held.contains(&one.uri.to_ascii_lowercase()))
        .map(|(source_uri, _)| source_uri)
        .collect();
    let arrays: Vec<(&String, &ReadyArray)> = listed_there
        .iter()
        .flat_map(|source_uri| {
            ready[*source_uri]
                .arrays
                .iter()
                .map(move |array| (*source_uri, array))
        })
        .collect();
    let mut short: BTreeSet<&String> = BTreeSet::new();
    for handful in arrays.chunks(METADATA_BATCH) {
        let ids: Vec<DataArrayIdentifier> = handful.iter().map(|(_, array)| array.id.clone()).collect();
        let answers = array_metadata(client, &ids).await?;
        for ((source_uri, array), answer) in handful.iter().zip(answers) {
            let same = answer.is_ok_and(|meta| meta.dimensions == array.dimensions);
            if !same {
                short.insert(source_uri);
            }
        }
    }
    Ok(Ok(listed_there
        .into_iter()
        .filter(|source_uri| !short.contains(source_uri))
        .cloned()
        .collect()))
}

/// The objects among `ready` the store wrote - or deleted - after the
/// caller took them: one Discovery listing from the earliest time known,
/// and the deleted ones when the transport has a route for them (the REST
/// API has none, and refuses by name: then a deletion cannot be seen).
async fn conflicts_of<K: Caller>(
    client: &mut K,
    space: &str,
    ready: &BTreeMap<String, Ready>,
    expected: &BTreeMap<String, i64>,
) -> Result<Vec<Conflict>, ClientError> {
    let known: BTreeMap<String, (String, i64)> = ready
        .values()
        .filter_map(|one| {
            expected
                .get(&one.uri)
                .map(|taken| (one.uri.to_ascii_lowercase(), (one.uri.clone(), *taken)))
        })
        .collect();
    let Some(since) = known.values().map(|(_, taken)| *taken).min() else {
        return Ok(Vec::new());
    };
    let mut conflicts = Vec::new();
    // The filter is strict (§3.12.8): `since - 1` so that an object written
    // exactly at the earliest time known is listed, and compared.
    for resource in written_since(client, space, since - 1, &[]).await? {
        if let Some((uri, taken)) = known.get(&resource.uri.to_ascii_lowercase())
            && resource.store_last_write > *taken
        {
            conflicts.push(Conflict {
                uri: uri.clone(),
                taken: *taken,
                store: Some(resource.store_last_write),
            });
        }
    }
    match deleted_since(client, space, since - 1, &[]).await {
        Ok(deleted) => {
            for gone in deleted {
                if let Some((uri, taken)) = known.get(&gone.uri.to_ascii_lowercase())
                    && gone.deleted_time > *taken
                {
                    conflicts.push(Conflict {
                        uri: uri.clone(),
                        taken: *taken,
                        store: None,
                    });
                }
            }
        }
        // No route for it on this transport: a deletion goes unseen, which
        // the REST client says by name - not a reason to stop the push.
        Err(ClientError::Invalid(_)) => {}
        Err(other) => return Err(other),
    }
    conflicts.sort_by(|a, b| a.uri.cmp(&b.uri));
    Ok(conflicts)
}

/// Why an island did not go through, and every object or array in it the
/// store refused ([`Report::refusals`]).
struct Halt {
    stop: Stop,
    refusals: Vec<Refusal>,
}

impl Halt {
    fn failed(why: ClientError) -> Self {
        Halt {
            stop: Stop::Failed(why),
            refusals: Vec::new(),
        }
    }

    /// The refusals, and a [`Stop::Refused`] that quotes the first and
    /// counts the others.
    fn refused(refusals: Vec<Refusal>) -> Self {
        let first = &refusals[0];
        let what = match &first.array {
            Some(path) => format!("{} {path}", first.uri),
            None => first.uri.clone(),
        };
        let mut why = match first.code {
            Some(code) => format!("{what}: {} ({code})", first.message),
            None => format!("{what}: {}", first.message),
        };
        if refusals.len() > 1 {
            why.push_str(&format!(", and {} more refused", refusals.len() - 1));
        }
        Halt {
            stop: Stop::Refused(why),
            refusals,
        }
    }
}

/// One island's objects and arrays, into the open transaction.
async fn send_island<K: Caller>(
    client: &mut K,
    source: &(dyn EnergymlStorage + Sync),
    island: &Island,
    ready: &BTreeMap<String, Ready>,
    format: WriteFormat,
    grouping: Grouping,
) -> Result<(), Halt> {
    let mut objects = Vec::with_capacity(island.uris.len());
    for uri in &island.uris {
        let one = ready.get(uri).expect("an island holds only what was readied");
        objects.push(DataObject {
            resource: Resource {
                uri: one.uri.clone(),
                name: one.name.clone(),
                ..Default::default()
            },
            format: format.name().into(),
            blob_id: None,
            data: one.xml.clone(),
        });
    }
    let answers = put_data_objects(client, &objects, false)
        .await
        .map_err(Halt::failed)?;
    // Every refusal, in the order the island sends them - not the first
    // only, and not in the order of the answer's map.
    let refusals: Vec<Refusal> = objects
        .iter()
        .filter_map(|object| {
            let uri = &object.resource.uri;
            match answers.get(uri) {
                Some(Err(error)) => Some(Refusal {
                    uri: uri.clone(),
                    array: None,
                    code: Some(error.code),
                    message: error.message.clone(),
                }),
                _ => None,
            }
        })
        .collect();
    if !refusals.is_empty() {
        return Err(Halt::refused(refusals));
    }

    let mut writes = Vec::new();
    for uri in &island.uris {
        let one = &ready[uri];
        for ReadyArray { id, location, .. } in &one.arrays {
            let mine = |message: String| {
                Halt::refused(vec![Refusal {
                    uri: id.uri.clone(),
                    array: Some(location.path_in_file.clone()),
                    code: None,
                    message,
                }])
            };
            let data = source
                .read_array(location)
                .await
                .map_err(|why| mine(format!("the array could not be read from the source: {why}")))?;
            writes.push((id.clone(), to_wire(&data).map_err(mine)?));
        }
    }
    if writes.is_empty() {
        return Ok(());
    }
    let written = write_arrays_with(client, &writes, DEFAULT_MESSAGE, grouping, |_| {})
        .await
        .map_err(Halt::failed)?;
    let refusals: Vec<Refusal> = writes
        .iter()
        .zip(written)
        .filter_map(|((id, _), outcome)| {
            let failure = outcome.err()?;
            let (code, message) = match failure {
                ArrayFailure::Store(error) => (Some(error.code), error.message),
                other => (None, other.to_string()),
            };
            Some(Refusal {
                uri: id.uri.clone(),
                array: Some(id.path_in_resource.clone()),
                code,
                message,
            })
        })
        .collect();
    if !refusals.is_empty() {
        return Err(Halt::refused(refusals));
    }
    Ok(())
}

/// The same object, addressed in the destination dataspace.
fn in_dataspace(uri: &str, space: &str) -> Result<String, String> {
    let parsed = EtpUri::parse(uri).map_err(|why| format!("its URI does not parse: {why}"))?;
    let kind = qualified_type_of(&parsed).ok_or_else(|| "its URI names no data object".to_string())?;
    let uuid = parsed.uuid.ok_or_else(|| "its URI carries no uuid".to_string())?;
    Ok(format!("{space}/{kind}({uuid})"))
}

/// The object a data array hangs from, in the destination.
///
/// RESQML 2.0.1 hangs its arrays from an `EpcExternalPartReference` - the
/// proxy -, eml 2.3 from the object itself; the location says which. A proxy
/// the source does not hold is refused by name: the array would be addressed
/// to an object that will never exist.
fn owner_of(
    location: &ArrayLocation,
    object: &str,
    resources: &[ObjectSummary],
    space: &str,
) -> Result<String, String> {
    let Some(proxy) = &location.proxy_uuid else {
        return Ok(object.to_string());
    };
    let found = resources
        .iter()
        .find(|one| one.uuid.eq_ignore_ascii_case(proxy))
        .ok_or_else(|| {
            format!(
                "{object}: its array {} goes through a proxy the source does not hold ({proxy})",
                location.path_in_file
            )
        })?;
    Ok(format!(
        "{space}/{}({})",
        found.qualified_type,
        found.uuid.to_ascii_lowercase()
    ))
}

/// An array as storage holds it, as the wire carries it: the logical type
/// says what the numbers are, the transport is one §13.2.2.1 allows for it,
/// and the values are widened into it where the wire has no narrower box.
/// The one conversion: `push` and the writer (`writer.rs`) both call it.
pub(crate) fn to_wire(data: &ArrayData) -> Result<Array, String> {
    use AnyArrayType as T;
    use AnyLogicalArrayType as L;
    let dimensions: Vec<u64> = data
        .metadata
        .dimensions
        .iter()
        .map(|d| (*d).max(0) as u64)
        .collect();
    let (values, logical, transport) = match &data.values {
        ArrayValues::F64(v) => (Wire::Doubles(v.clone()), L::ArrayOfDouble64Le, T::ArrayOfDouble),
        ArrayValues::F32(v) => (Wire::Floats(v.clone()), L::ArrayOfFloat32Le, T::ArrayOfFloat),
        ArrayValues::I64(v) => (Wire::Longs(v.clone()), L::ArrayOfInt64Le, T::ArrayOfLong),
        ArrayValues::I32(v) => (Wire::Ints(v.clone()), L::ArrayOfInt32Le, T::ArrayOfInt),
        ArrayValues::I16(v) => (
            Wire::Ints(v.iter().map(|x| i32::from(*x)).collect()),
            L::ArrayOfInt16Le,
            T::ArrayOfInt,
        ),
        ArrayValues::I8(v) => (
            Wire::Ints(v.iter().map(|x| i32::from(*x)).collect()),
            L::ArrayOfInt8,
            T::ArrayOfInt,
        ),
        ArrayValues::U8(v) => (
            Wire::Ints(v.iter().map(|x| i32::from(*x)).collect()),
            L::ArrayOfUInt8,
            T::ArrayOfInt,
        ),
        ArrayValues::U16(v) => (
            Wire::Ints(v.iter().map(|x| i32::from(*x)).collect()),
            L::ArrayOfUInt16Le,
            T::ArrayOfInt,
        ),
        ArrayValues::U32(v) => (
            Wire::Longs(v.iter().map(|x| i64::from(*x)).collect()),
            L::ArrayOfUInt32Le,
            T::ArrayOfLong,
        ),
        // Bit for bit: the reader undoes it with `as u64`.
        ArrayValues::U64(v) => (
            Wire::Longs(v.iter().map(|x| *x as i64).collect()),
            L::ArrayOfUInt64Le,
            T::ArrayOfLong,
        ),
        other => {
            return Err(format!(
                "this build does not send {} arrays",
                other.element_type()
            ));
        }
    };
    Ok(Array {
        dimensions,
        logical,
        transport,
        values,
        blocks: 0,
    })
}

/// Bytes per element, from the name storage gives a type. An unknown name
/// counts as eight - a planning weight errs on the heavy side, and the value
/// itself is refused by name later if it cannot be sent.
fn element_bytes(element_type: &str) -> u64 {
    match element_type {
        "int8" | "uint8" => 1,
        "int16" | "uint16" => 2,
        "float" | "int32" | "uint32" => 4,
        _ => 8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use energyml_storage::graph::Edge;

    fn summary(uri: &str) -> ObjectSummary {
        ObjectSummary::new(uri, uri, String::new())
    }

    fn edge(from: &str, to: &str) -> Edge {
        Edge::new(from, to, String::new())
    }

    fn graph(edges: Vec<Edge>) -> Graph {
        Graph {
            nodes: Vec::new(),
            edges,
        }
    }

    fn weighing(pairs: &[(&str, u64, usize)]) -> BTreeMap<String, Weight> {
        pairs
            .iter()
            .map(|(uri, bytes, arrays)| {
                (
                    uri.to_string(),
                    Weight {
                        bytes: *bytes,
                        arrays: *arrays,
                    },
                )
            })
            .collect()
    }

    fn flat(islands: &[Island]) -> Vec<&str> {
        islands
            .iter()
            .flat_map(|one| one.uris.iter().map(String::as_str))
            .collect()
    }

    /// The first rule: an object goes after everything it references, however
    /// the source listed them.
    #[test]
    fn targets_go_before_sources_whatever_the_source_order() {
        // Listed sources first, on purpose: property → representation → CRS.
        let listed = [summary("prop"), summary("rep"), summary("crs")];
        let g = graph(vec![edge("prop", "rep"), edge("rep", "crs")]);
        let out = islands(&listed, &g, &weighing(&[]), &Budget::default());
        assert_eq!(flat(&out), ["crs", "rep", "prop"]);
    }

    /// The second rule where the first says nothing: an XML-only object that
    /// nothing orders against a representation goes **before** it, so that the
    /// XML-only islands are all at the front and not
    /// scattered by the source's listing order.
    #[test]
    fn xml_only_goes_first_when_nothing_orders_it() {
        // The representation and its CRS are listed first, an isolated feature
        // last: Tarjan alone would emit the feature after the representation.
        let listed = [summary("rep"), summary("crs"), summary("feature")];
        let g = graph(vec![edge("rep", "crs")]);
        let w = weighing(&[("rep", 1000, 1), ("crs", 10, 0), ("feature", 10, 0)]);
        let out = islands(&listed, &g, &w, &Budget::default());
        assert_eq!(out.len(), 2, "{out:?}");
        assert_eq!(out[0].uris, ["crs", "feature"]);
        assert_eq!(out[1].uris, ["rep"]);
    }

    /// And never against the first rule: an XML-only object that references a
    /// representation - an `Activity` - goes after it, however light it is.
    #[test]
    fn the_first_rule_still_wins_over_the_second() {
        let listed = [summary("activity"), summary("rep"), summary("crs")];
        let g = graph(vec![edge("activity", "rep"), edge("rep", "crs")]);
        let w = weighing(&[("activity", 10, 0), ("rep", 1000, 1), ("crs", 10, 0)]);
        let out = islands(&listed, &g, &w, &Budget::default());
        assert_eq!(flat(&out), ["crs", "rep", "activity"]);
    }

    /// What leaves the source is named, once per pair, and only from what the
    /// source holds - an edge between two absent objects is not the source's.
    #[test]
    fn references_leaving_the_source_are_named() {
        let listed = [summary("prop"), summary("rep")];
        let kind = "eml:///eml23.PropertyKind(dictionary)";
        let g = graph(vec![
            edge("prop", kind),
            edge("prop", kind),
            edge("prop", "rep"),
            edge("elsewhere", "also-elsewhere"),
        ]);
        assert_eq!(
            dangling(&listed, &g),
            vec![("prop".to_string(), kind.to_string())]
        );
    }

    /// A refused commit points at what leaves the source - the half of
    /// « reference missing » the store does not say - and says nothing extra
    /// when nothing leaves.
    #[test]
    fn a_refused_commit_points_at_what_leaves_the_source() {
        let bare = with_hint("the commit was refused: no".into(), &[]);
        assert_eq!(bare, "the commit was refused: no");
        let leaving: Vec<(String, String)> =
            (0..5).map(|i| ("prop".to_string(), format!("kind{i}"))).collect();
        let hinted = with_hint("the commit was refused: no".into(), &leaving);
        assert!(hinted.contains("5 reference(s) leave the source"), "{hinted}");
        assert!(hinted.contains("kind0, kind1, kind2, and 2 more"), "{hinted}");
        assert!(hinted.contains("PropertyKind"), "{hinted}");
    }

    /// A cycle cannot be ordered, so it is never cut - even past the budget.
    #[test]
    fn a_cycle_stays_in_one_island_past_the_budget() {
        let listed = [summary("a"), summary("b"), summary("c")];
        let g = graph(vec![edge("a", "b"), edge("b", "a"), edge("c", "a")]);
        let tight = Budget {
            objects: 1,
            bytes: u64::MAX,
        };
        let out = islands(&listed, &g, &weighing(&[]), &tight);
        assert_eq!(out.len(), 2, "{out:?}");
        assert_eq!(out[0].uris, ["a", "b"]);
        assert_eq!(out[1].uris, ["c"]);
    }

    /// The second rule: what carries no array is cut off from what does, but
    /// nothing is reordered for it.
    #[test]
    fn the_kind_changing_cuts_an_island_and_reorders_nothing() {
        let listed = [
            summary("crs"),
            summary("feature"),
            summary("rep"),
            summary("prop"),
        ];
        let g = graph(vec![
            edge("rep", "crs"),
            edge("rep", "feature"),
            edge("prop", "rep"),
        ]);
        let w = weighing(&[
            ("crs", 10, 0),
            ("feature", 10, 0),
            ("rep", 1000, 1),
            ("prop", 1000, 1),
        ]);
        let out = islands(&listed, &g, &w, &Budget::default());
        assert_eq!(out.len(), 2, "{out:?}");
        assert_eq!(out[0].uris, ["crs", "feature"]);
        assert_eq!(out[0].arrays, 0);
        assert_eq!(out[1].uris, ["rep", "prop"]);
        assert_eq!(out[1].arrays, 2);
        assert_eq!(out[1].bytes, 2000);
    }

    /// The budget cuts, in both units, and a component that would overflow
    /// starts the next island rather than being split.
    #[test]
    fn the_budget_cuts_by_objects_and_by_bytes() {
        let listed: Vec<ObjectSummary> = (0..5).map(|i| summary(&format!("o{i}"))).collect();
        let g = graph(Vec::new());
        let by_objects = islands(
            &listed,
            &g,
            &weighing(&[]),
            &Budget {
                objects: 2,
                bytes: u64::MAX,
            },
        );
        assert_eq!(
            by_objects.iter().map(|one| one.uris.len()).collect::<Vec<_>>(),
            [2, 2, 1]
        );

        let w = weighing(&[
            ("o0", 60, 0),
            ("o1", 60, 0),
            ("o2", 60, 0),
            ("o3", 60, 0),
            ("o4", 60, 0),
        ]);
        let by_bytes = islands(
            &listed,
            &g,
            &w,
            &Budget {
                objects: 100,
                bytes: 125,
            },
        );
        assert_eq!(
            by_bytes.iter().map(|one| one.uris.len()).collect::<Vec<_>>(),
            [2, 2, 1]
        );
    }

    /// A split keeps the island's order and never cuts a cycle: halved, a
    /// cycle of two and two lone objects make two pieces with the cycle whole
    /// in the first; one by one, three pieces; and a cycle alone, or one
    /// object, cannot be cut at all.
    #[test]
    fn a_split_keeps_the_order_and_never_cuts_a_cycle() {
        let listed = [summary("a"), summary("b"), summary("c"), summary("d")];
        let g = graph(vec![edge("a", "b"), edge("b", "a"), edge("c", "a")]);
        let w = weighing(&[("a", 10, 0), ("b", 20, 0), ("c", 30, 0), ("d", 40, 0)]);
        let planned = plan(&listed, &g, &w, &Budget::default());
        assert_eq!(planned.len(), 1, "{planned:?}");
        let island = &planned[0];
        assert_eq!(island.units, [2, 1, 1], "the cycle is one unit: {island:?}");
        let cycles: Vec<&[String]> = island.cycles();
        assert_eq!(
            cycles,
            [&["a".to_string(), "b".to_string()][..]],
            "and it is named"
        );

        let halves = cut(island, &w, Split::Halve).expect("three units cut");
        let uris: Vec<Vec<&str>> = halves
            .iter()
            .map(|piece| piece.uris.iter().map(String::as_str).collect())
            .collect();
        assert_eq!(
            uris.concat(),
            flat(std::slice::from_ref(island)),
            "the order is kept"
        );
        assert_eq!(halves.len(), 2);
        assert_eq!(halves[0].units, [2, 1], "the cycle whole, in the first half");
        assert_eq!(halves[0].bytes, 10 + 20 + 30);
        assert_eq!(halves[1].uris, ["d"]);

        let single = cut(island, &w, Split::Single).expect("three units cut");
        assert_eq!(single.len(), 3);
        assert_eq!(single[0].uris.len(), 2, "the cycle is never cut");

        let one_unit = Island {
            units: vec![3],
            ..halves[0].clone()
        };
        assert!(cut(&single[0], &w, Split::Halve).is_none());
        assert!(cut(&one_unit, &w, Split::Single).is_none());
        assert!(cut(island, &w, Split::Off).is_none());
    }

    /// An edge to something the source does not list is not a reason to lose
    /// the object: the reference dangles in the source already.
    #[test]
    fn an_edge_out_of_the_source_is_ignored() {
        let listed = [summary("a")];
        let g = graph(vec![edge("a", "elsewhere")]);
        let out = islands(&listed, &g, &weighing(&[]), &Budget::default());
        assert_eq!(flat(&out), ["a"]);
    }

    /// The destination URI keeps the type and the uuid and takes the
    /// dataspace - and a URI that names no object is refused, never sent
    /// under a made-up one.
    #[test]
    fn an_object_is_readdressed_to_the_dataspace() {
        let space = "eml:///dataspace('there')";
        assert_eq!(
            in_dataspace(
                "eml:///resqml20.obj_HorizonInterpretation(ABCDEF00-0000-4000-8000-000000000001)",
                space
            )
            .unwrap(),
            "eml:///dataspace('there')/resqml20.obj_HorizonInterpretation(abcdef00-0000-4000-8000-000000000001)"
        );
        assert!(in_dataspace("eml:///dataspace('here')", space).is_err());
    }

    /// A 2.0.1 array is addressed to its proxy, an eml 2.3 one to its object.
    #[test]
    fn an_array_hangs_from_its_proxy_or_from_its_object() {
        let space = "eml:///dataspace('there')";
        let object = "eml:///dataspace('there')/resqml20.obj_PointSetRepresentation(p)";
        let proxy = ObjectSummary::new(
            "eml:///eml20.obj_EpcExternalPartReference(X1)",
            "X1",
            "eml20.obj_EpcExternalPartReference",
        );
        let through = ArrayLocation {
            proxy_uuid: Some("x1".into()),
            path_in_file: "/points".into(),
            ..Default::default()
        };
        assert_eq!(
            owner_of(&through, object, std::slice::from_ref(&proxy), space).unwrap(),
            "eml:///dataspace('there')/eml20.obj_EpcExternalPartReference(x1)"
        );
        let direct = ArrayLocation {
            proxy_uuid: None,
            ..through.clone()
        };
        assert_eq!(owner_of(&direct, object, &[], space).unwrap(), object);
        assert!(
            owner_of(&through, object, &[], space)
                .unwrap_err()
                .contains("does not hold")
        );
    }

    /// Every integer width the storage holds goes out under a transport
    /// §13.2.2.1 allows for its logical type, and decodes back to itself.
    #[test]
    fn every_element_type_travels_as_its_logical_type_allows() {
        use retp_client::arrays::allowed_transports;
        let cases: Vec<ArrayValues> = vec![
            ArrayValues::F64(vec![1.5]),
            ArrayValues::F32(vec![1.5]),
            ArrayValues::I64(vec![-1]),
            ArrayValues::I32(vec![-1]),
            ArrayValues::I16(vec![-1]),
            ArrayValues::I8(vec![-1]),
            ArrayValues::U8(vec![200]),
            ArrayValues::U16(vec![60000]),
            ArrayValues::U32(vec![4_000_000_000]),
            ArrayValues::U64(vec![u64::MAX]),
        ];
        for values in cases {
            let element_type = values.element_type();
            let data = ArrayData {
                metadata: energyml_storage::array::ArrayMetadata::new(vec![1], element_type.to_string()),
                values: values.clone(),
            };
            let wire = to_wire(&data).unwrap();
            assert!(
                allowed_transports(wire.logical).contains(&wire.transport),
                "{element_type}: {:?} as {:?}",
                wire.logical,
                wire.transport
            );
            // Back through the reader's own decoding, the storage value is
            // what it was.
            assert_eq!(crate::values_of(&wire).unwrap(), values, "{element_type}");
        }
    }
}
