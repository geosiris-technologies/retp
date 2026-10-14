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
//! send islands whose references point at the one that failed. The retry is
//! the runner's business (ADR-0002): an island is redone **whole**, since the
//! session that died rolled it back.

use std::collections::{BTreeMap, BTreeSet};

use energyml_core::identifiers::EtpUri;
use energyml_core::registry::{DynObject, to_part};
use energyml_core::xml::XmlWriteOptions;
use energyml_storage::array::{ArrayData, ArrayLocation, ArrayValues};
use energyml_storage::graph::Graph;
use energyml_storage::resource::ObjectSummary;
use energyml_storage::storage::EnergymlStorage;
use retp_client::arrays::{Array, ArrayValues as Wire, Grouping};
use retp_client::asynchronous::{
    commit_transaction, deleted_since, put_data_objects, rollback_transaction, start_transaction,
    write_arrays_with, written_since,
};
use retp_client::{Caller, ClientError, Ended, Opened, dataspace_uri};
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
    fn name(self) -> &'static str {
        match self {
            WriteFormat::Xml => "xml",
            WriteFormat::Json => "json",
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
    /// In the order they are sent - targets before sources.
    pub uris: Vec<String>,
    /// What they weigh together.
    pub bytes: u64,
    /// How many arrays go with them.
    pub arrays: usize,
}

/// Cut a store into islands. Pure: the graph and the weights in, the plan
/// out, nothing sent.
///
/// `resources` gives the order ties are broken in - the source's own -, and
/// every object of `weights` that the resources do not list is left out.
pub fn islands(
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
    }
    out
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
            let key = kind.uuid.to_ascii_lowercase();
            if !done.insert(key.clone()) {
                continue;
            }
            // The same shape as the reference that named it.
            let uri = to.replace(&uuid, &key);
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
                .filter(|parent| !parent.eq_ignore_ascii_case(&key))
                .map(|parent| {
                    energyml_storage::graph::Edge::new(
                        uri.clone(),
                        to.replace(&uuid, &parent.to_ascii_lowercase()),
                        "Parent",
                    )
                })
                .into_iter()
                .collect();
            let summary = ObjectSummary::new(uri, key, "eml23.PropertyKind")
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
    let escape = |s: &str| {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
    };
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
}

/// Where a push stopped, when it did.
#[derive(Debug)]
pub struct Stopped {
    /// The island that did not go through, by rank in [`Report::islands`].
    pub island: usize,
    /// Why.
    pub why: Stop,
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
}

impl Stopped {
    fn refused(island: usize, why: impl Into<String>) -> Self {
        Stopped {
            island,
            why: Stop::Refused(why.into()),
        }
    }
}

/// One object ready to leave: its destination URI, its XML, its arrays.
struct Ready {
    uri: String,
    name: String,
    xml: Vec<u8>,
    arrays: Vec<(DataArrayIdentifier, ArrayLocation)>,
}

/// Send everything `source` holds into `dataspace`, island by island.
pub async fn push<K: Caller>(
    client: &mut K,
    source: &(dyn EnergymlStorage + Sync),
    dataspace: &str,
    options: &Options<'_>,
    mut progress: impl FnMut(Progress),
) -> Result<Report, ClientError> {
    let Options {
        budget,
        format,
        well_known,
        only,
        grouping,
        expected,
        overwrite,
    } = options;
    let format = *format;
    let grouping = *grouping;
    let overwrite = *overwrite;
    let well_known = *well_known;
    let space = dataspace_uri(dataspace);
    let not_sent = |why: String| ClientError::Invalid(EtpError::EINVALID_ARGUMENT.with_detail(why));
    let mut resources = source
        .resources()
        .await
        .map_err(|why| not_sent(format!("the source could not be listed: {why}")))?;
    // The whole listing stays for what an array needs to be addressed - a
    // RESQML 2.0.1 proxy that is not among the chosen objects still names
    // where their numbers hang - and only the chosen ones are read and sent.
    let all = resources.clone();
    if let Some(only) = only {
        resources.retain(|one| only.iter().any(|uri| uri == &one.uri));
    }
    // The references as the objects write them, not the edges the source
    // states: an ETP server's edges join objects it holds, so a reference that
    // leaves the source - what `dangling` reports - is in the objects only.
    let mut graph = source
        .derived_graph(&[])
        .await
        .map_err(|why| not_sent(format!("the source's graph could not be read: {why}")))?;
    let uris: Vec<String> = resources.iter().map(|one| one.uri.clone()).collect();
    let mut objects = source.get_objects(&uris).await;

    // The dictionary's kinds, when asked for: one more object each, listed
    // and read like the source's own, with the edges their parents make.
    let mut report = Report::default();
    if let WellKnown::Push(registry) = well_known {
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
        let written = match format {
            WriteFormat::Xml => {
                to_part(object.as_ref(), XmlWriteOptions::default()).map_err(|why| why.to_string())
            }
            WriteFormat::Json => {
                energyml_core::json::write_string(object.as_reflect(), false).map_err(|why| why.to_string())
            }
        };
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
            match source.array_metadata(&location).await {
                Ok(meta) => {
                    let count: u64 = meta.dimensions.iter().map(|d| (*d).max(0) as u64).product();
                    bytes += count * element_bytes(&meta.element_type);
                }
                Err(why) => {
                    refused = Some(format!(
                        "{}: its array {} has no metadata: {why}",
                        summary.uri, location.path_in_file
                    ));
                    break;
                }
            }
            arrays.push((
                DataArrayIdentifier {
                    uri: owner,
                    path_in_resource: location.path_in_file.clone(),
                },
                location,
            ));
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

    report.islands = islands(&listed, &graph, &weights, budget);
    report.dangling = dangling(&resources, &graph);
    progress(Progress::Planned {
        islands: report.islands.len(),
        objects: listed.len(),
    });

    // The conflict seen **before** writing: what the store wrote since
    // the caller took its objects, in one listing. An object changed there
    // is shown, not overwritten - unless the caller said so.
    if let Some(expected) = expected {
        report.conflicts = conflicts_of(client, &space, &ready, expected).await?;
        if !report.conflicts.is_empty() && !overwrite {
            let named: Vec<String> = report
                .conflicts
                .iter()
                .map(|c| match c.store {
                    Some(_) => c.uri.clone(),
                    None => format!("{} (deleted there)", c.uri),
                })
                .collect();
            report.stopped = Some(Stopped::refused(
                0,
                format!(
                    "{} object(s) changed on the store since they were taken, nothing was written: {} - push again with overwrite to replace them",
                    named.len(),
                    named.join(", ")
                ),
            ));
            return Ok(report);
        }
    }

    // A store that did not agree Transaction (18) - some RDDMS servers do
    // not offer it - takes each island as plain puts. The islands still
    // order what goes, and still bound what a cut costs; what they lose is
    // the atomicity, and the report says so.
    let mut transactional = client.negotiated().protocols.contains_key(&18);
    report.without_transactions = !transactional;
    for rank in 0..report.islands.len() {
        let island = report.islands[rank].clone();
        // A transaction when the store speaks them - and a store that agreed
        // 18 and then says it has none (some REST APIs name the transaction
        // `null`) is taken at its word from here on.
        let transaction = if transactional {
            match start_transaction(client, std::slice::from_ref(&space), false, "energyml-rust push").await?
            {
                Opened::Open(transaction) => Some(transaction),
                Opened::Refused(why) => {
                    report.stopped = Some(Stopped::refused(
                        rank,
                        format!("the transaction would not open: {why}"),
                    ));
                    return Ok(report);
                }
                Opened::Unsupported(_) => {
                    transactional = false;
                    report.without_transactions = true;
                    None
                }
            }
        } else {
            None
        };
        let Some(transaction) = transaction else {
            match send_island(client, source, &island, &ready, format, grouping).await {
                Ok(()) => {
                    report.sent += 1;
                    progress(Progress::Sent {
                        island: rank,
                        of: report.islands.len(),
                    });
                }
                Err(Stop::Refused(why)) => {
                    let why = with_hint(
                        format!(
                            "{why} - no transaction to roll back: what this island wrote before the refusal stays"
                        ),
                        &report.dangling,
                    );
                    report.stopped = Some(Stopped::refused(rank, why));
                    return Ok(report);
                }
                Err(failed) => {
                    report.stopped = Some(Stopped {
                        island: rank,
                        why: failed,
                    });
                    return Ok(report);
                }
            }
            continue;
        };
        // The objects, then their arrays, then the commit - and on the first
        // thing the store will not take, a rollback and the report. A
        // rollback that fails changes nothing about what is reported: the
        // transaction is not committed either way.
        let stop = send_island(client, source, &island, &ready, format, grouping).await;
        match stop {
            Ok(()) => match commit_transaction(client, &transaction).await? {
                Ended::Done => {
                    report.sent += 1;
                    progress(Progress::Sent {
                        island: rank,
                        of: report.islands.len(),
                    });
                }
                Ended::Refused(why) => {
                    let why = with_hint(format!("the commit was refused: {why}"), &report.dangling);
                    report.stopped = Some(Stopped::refused(rank, why));
                    return Ok(report);
                }
            },
            Err(Stop::Refused(why)) => {
                let _ = rollback_transaction(client, &transaction).await;
                report.stopped = Some(Stopped::refused(rank, why));
                return Ok(report);
            }
            Err(failed) => {
                report.stopped = Some(Stopped {
                    island: rank,
                    why: failed,
                });
                return Ok(report);
            }
        }
    }
    Ok(report)
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

/// One island's objects and arrays, into the open transaction.
async fn send_island<K: Caller>(
    client: &mut K,
    source: &(dyn EnergymlStorage + Sync),
    island: &Island,
    ready: &BTreeMap<String, Ready>,
    format: WriteFormat,
    grouping: Grouping,
) -> Result<(), Stop> {
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
        .map_err(Stop::Failed)?;
    for (uri, answer) in &answers {
        if let Err(error) = answer {
            return Err(Stop::Refused(format!(
                "{uri}: {} ({})",
                error.message, error.code
            )));
        }
    }

    let mut writes = Vec::new();
    for uri in &island.uris {
        for (id, location) in &ready[uri].arrays {
            let data = source.read_array(location).await.map_err(|why| {
                Stop::Refused(format!(
                    "{uri}: its array {} could not be read: {why}",
                    location.path_in_file
                ))
            })?;
            writes.push((
                id.clone(),
                to_wire(&data).map_err(|why| Stop::Refused(format!("{uri}: {why}")))?,
            ));
        }
    }
    if writes.is_empty() {
        return Ok(());
    }
    let written = write_arrays_with(client, &writes, DEFAULT_MESSAGE, grouping, |_| {})
        .await
        .map_err(Stop::Failed)?;
    for ((id, _), outcome) in writes.iter().zip(written) {
        if let Err(failure) = outcome {
            return Err(Stop::Refused(format!(
                "{} {}: {failure}",
                id.uri, id.path_in_resource
            )));
        }
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
