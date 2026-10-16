//! Objects held by value, over ETP: what the wire does not say, and how this
//! store finds it out without guessing.
//!
//! # What ETP says
//!
//! A contained data object is "a standalone data object in its own right"
//! (ETP 1.2 section 9.1.3.1, p. 179), so a server lists it in discovery like
//! any other, and states its by-value relationship as an `Edge` whose source is
//! the contained object and whose target is the container (section 8.1.1.1.1,
//! p. 156). Two things follow from the text:
//!
//! - a by-value relationship is **always Primary** (section 8.1.1.1.2, p. 157,
//!   and the `RelationshipKind` table of section 23.34.13, p. 473: "a ByValue
//!   relationship is ALWAYS organizational"; a Secondary one "is always
//!   specified using the DOR construct (never the ByValue construct)"). A
//!   Secondary edge is therefore a reference, with certainty;
//! - nothing else tells a by-value edge from a Primary reference: `Resource`
//!   has no "contained in" field, `Edge` has no "by value" field, and
//!   `customData` is vendor-defined, so a standard client cannot read a marker
//!   from it. Which types contain which is left to each ML's implementation
//!   guide (section 9.1.3, p. 178), which is not published for ETP 1.2.
//!
//! # What this store does
//!
//! It proves containment from the data, never from a pattern of names. An edge
//! `source -> target` is a by-value containment when all of the following hold:
//!
//! 1. it is Primary (above);
//! 2. both ends are listed, and the type of the source can be nested in another
//!    object at all (`TypeInfo::is_nestable`, generated from the schemas);
//! 3. the target, read (or the part that holds it), holds an object with the
//!    source's uuid inline, directly: no other nested object lies between
//!    ([`energyml_core::identity::nested_objects`]).
//!
//! The third is the proof: the container's own text says it holds the object
//! (section 9.2.2 row 15, p. 188: a store returns the contained objects in the
//! container, "as defined by the container data object schema"). The second
//! only bounds the cost: a dataspace with no nestable type (any RESQML 2.0.1
//! one) sends no request more and reads no object more. The reads of the third
//! are one `GetDataObjects` for the distinct targets, once per listing.
//!
//! What cannot be proven stays what the server said: an edge to a container the
//! server will not give, or one that does not hold the source inline, is a
//! plain edge, and a server that states no edges gives no containment (a note
//! says so). Where a Primary reference and a by-value containment would join
//! the same two objects, the wire shows one edge and this store reports it as
//! the containment: the container does hold the object.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use energyml_core::identifiers::EtpUri;
use energyml_core::identity::nested_objects;
use energyml_core::value::Reflect;
use energyml_storage::error::{Diagnostic, Result};
use energyml_storage::graph::{self, Edge, EdgeKind};
use energyml_storage::resource::ObjectSummary;
use retp_client::{Clock, Transport};

use crate::{EtpStore, qualified_type_of};

/// What a dataspace holds by value, as far as it can be proven.
#[derive(Debug, Default)]
pub(crate) struct Containment {
    /// For each contained object, its direct containers, sorted.
    containers: BTreeMap<String, Vec<String>>,
}

impl Containment {
    fn is_empty(&self) -> bool {
        self.containers.is_empty()
    }
}

/// A listed object, as the derivation needs it.
struct Listed {
    uuid: String,
    nestable: bool,
}

impl<T: Transport, C: Clock> EtpStore<T, C> {
    /// The containment of the dataspace, derived once per listing.
    pub(crate) fn containment(&self) -> Arc<Containment> {
        if let Some(known) = self
            .containment
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
        {
            return known.clone();
        }
        let derived = Arc::new(self.derive_containment());
        *self.containment.lock().unwrap_or_else(|p| p.into_inner()) = Some(derived.clone());
        derived
    }

    fn derive_containment(&self) -> Containment {
        // Containment is a fact of Discovery and Store together
        // (`Capabilities::contained_objects`).
        if !self.etp_capabilities().contained_objects {
            return Containment::default();
        }
        let Ok(listed) = self.listing() else {
            return Containment::default();
        };
        let mut known: HashMap<String, Listed> = HashMap::new();
        for resource in &listed {
            let Ok(parsed) = EtpUri::parse(&resource.uri) else {
                continue;
            };
            let (Some(qualified), Some(uuid)) = (qualified_type_of(&parsed), parsed.uuid.clone()) else {
                continue;
            };
            let nestable = self
                .registry
                .by_qualified_type(&qualified)
                .is_some_and(|entry| entry.info.is_nestable);
            known.insert(
                resource.uri.clone(),
                Listed {
                    uuid: uuid.to_ascii_lowercase(),
                    nestable,
                },
            );
        }
        // No type that can be nested: no request, no read.
        if !known.values().any(|listed| listed.nestable) {
            return Containment::default();
        }
        let stated = match self.stated_edges(&[]) {
            Ok(stated) => stated,
            Err(why) => {
                self.record(Diagnostic::note(
                    &self.dataspace,
                    format!("containment not derived, the edges could not be listed: {why}"),
                ));
                return Containment::default();
            }
        };
        let candidates: Vec<&Edge> = stated
            .iter()
            .filter(|edge| edge.kind == Some(EdgeKind::Primary) && edge.from != edge.to)
            .filter(|edge| known.get(&edge.from).is_some_and(|listed| listed.nestable))
            .filter(|edge| known.contains_key(&edge.to))
            .collect();
        if candidates.is_empty() {
            return Containment::default();
        }
        // The proof: who holds whom inline, in the text of each candidate
        // container. A read object gives its whole tree, so a container that is
        // itself contained (a `ChannelSet`) is proven by the part that holds it,
        // and the containers that are the source of a candidate edge themselves
        // are read only if something is still unproven after the others.
        let sources: HashSet<&str> = candidates.iter().map(|edge| edge.from.as_str()).collect();
        let targets: BTreeSet<&str> = candidates.iter().map(|edge| edge.to.as_str()).collect();
        let mut to_read: Vec<String> = targets
            .iter()
            .filter(|uri| !sources.contains(**uri))
            .map(|uri| uri.to_string())
            .collect();
        if to_read.is_empty() {
            to_read = targets.iter().map(|uri| uri.to_string()).collect();
        }
        let mut read_already: HashSet<String> = HashSet::new();
        let mut held_by: HashSet<(String, String)> = HashSet::new();
        let pair_of = |edge: &Edge| {
            known
                .get(&edge.from)
                .zip(known.get(&edge.to))
                .map(|(source, target)| (source.uuid.clone(), target.uuid.clone()))
        };
        while !to_read.is_empty() {
            for (uri, read) in to_read.iter().zip(self.read_objects(&to_read)) {
                match (read, known.get(uri)) {
                    (Ok(object), Some(root)) => {
                        held_by.extend(direct_holdings(&root.uuid, object.as_reflect()));
                    }
                    (Err(why), _) => self.record(Diagnostic::note(
                        uri,
                        format!("read to find what it holds by value, and not read: {why}"),
                    )),
                    (Ok(_), None) => {}
                }
            }
            read_already.extend(to_read);
            to_read = candidates
                .iter()
                .filter(|edge| pair_of(edge).is_some_and(|pair| !held_by.contains(&pair)))
                .map(|edge| edge.to.clone())
                .filter(|uri| !read_already.contains(uri))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
        }
        let mut containers: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for edge in candidates {
            let proved = known
                .get(&edge.from)
                .zip(known.get(&edge.to))
                .is_some_and(|(source, target)| {
                    held_by.contains(&(source.uuid.clone(), target.uuid.clone()))
                });
            if proved {
                containers
                    .entry(edge.from.clone())
                    .or_default()
                    .insert(edge.to.clone());
            }
        }
        Containment {
            containers: containers
                .into_iter()
                .map(|(contained, held_by)| (contained, held_by.into_iter().collect()))
                .collect(),
        }
    }

    /// Whether an object of this qualified type can be nested in another
    /// object at all (`TypeInfo::is_nestable`): the necessary condition for it
    /// to be held by value, known from the type alone.
    fn nestable_type(&self, qualified_type: &str) -> bool {
        self.registry
            .by_qualified_type(qualified_type)
            .is_some_and(|entry| entry.info.is_nestable)
    }

    fn nestable_uri(&self, uri: &str) -> bool {
        EtpUri::parse(uri)
            .ok()
            .as_ref()
            .and_then(qualified_type_of)
            .is_some_and(|qualified| self.nestable_type(&qualified))
    }

    /// The summary of an object with the URIs of its direct containers
    /// (`ObjectSummary::contained_in`), which `Resource` has no field for.
    pub(crate) fn with_containers(&self, summary: ObjectSummary) -> ObjectSummary {
        // What cannot be nested is not held by value: nothing to derive.
        if !self.nestable_type(&summary.qualified_type) {
            return summary;
        }
        match self.containment().containers.get(&summary.uri) {
            Some(held_by) => summary.with_contained_in(held_by.clone()),
            None => summary,
        }
    }

    /// The edges with the by-value containments marked
    /// ([`Edge::by_value`]): those the derivation proved, and only those.
    pub(crate) fn mark_containment(&self, edges: Vec<Edge>) -> Vec<Edge> {
        // Nothing to mark unless a Primary edge starts from a type that can be
        // nested: no listing, no request, no read is asked for otherwise.
        let candidate = |edge: &Edge| edge.kind == Some(EdgeKind::Primary) && self.nestable_uri(&edge.from);
        if !edges.iter().any(candidate) {
            return edges;
        }
        let containment = self.containment();
        if containment.is_empty() {
            return edges;
        }
        edges
            .into_iter()
            .map(|edge| {
                let proved = edge.kind == Some(EdgeKind::Primary)
                    && containment
                        .containers
                        .get(&edge.from)
                        .is_some_and(|held_by| held_by.contains(&edge.to));
                if proved { edge.contained() } else { edge }
            })
            .collect()
    }

    /// `references`: what the objects **write**, each reference attributed to
    /// the object that writes it (ETP 1.2 section 8.1.1.1.1, p. 156).
    ///
    /// A server lists the contained objects as data objects (section 9.1.3.1),
    /// so the trait's default would read a `Log` and every `Channel` it holds,
    /// and state the references of each `Channel` twice: once from the `Log`
    /// that holds it inline, once from the `Channel` read on its own. This
    /// reads the objects that hold nothing's containment (the parts) and
    /// attributes the references written inside a contained object to it
    /// ([`graph::attribute_to_writers`]), as a store that lists its parts only
    /// does.
    pub(crate) fn native_references(&self, uris: &[String]) -> Result<Vec<Edge>> {
        let listed = self.listing()?;
        let summaries: Vec<ObjectSummary> = listed
            .iter()
            .filter_map(|resource| self.summary(resource))
            .map(|summary| self.with_containers(summary))
            .collect();
        let contained: Vec<&ObjectSummary> = summaries
            .iter()
            .filter(|summary| !summary.contained_in.is_empty())
            .collect();
        let by_uri: HashMap<&str, &ObjectSummary> = contained
            .iter()
            .map(|entry| (entry.uri.as_str(), *entry))
            .collect();
        let parts_of_asked: Vec<String> = if uris.is_empty() {
            summaries
                .iter()
                .filter(|summary| summary.contained_in.is_empty())
                .map(|summary| summary.uri.clone())
                .collect()
        } else {
            let mut parts = BTreeSet::new();
            for uri in uris {
                match by_uri.get(uri.as_str()) {
                    Some(entry) => parts.extend(holding_parts(&by_uri, entry)),
                    None => {
                        parts.insert(uri.clone());
                    }
                }
            }
            parts.into_iter().collect()
        };
        let readable: Vec<&ObjectSummary> = parts_of_asked
            .iter()
            .filter_map(|uri| summaries.iter().find(|summary| &summary.uri == uri))
            .collect();
        if !uris.is_empty() && readable.is_empty() {
            return Ok(Vec::new());
        }
        let wanted: Vec<String> = readable.iter().map(|summary| summary.uri.clone()).collect();
        let mut held = Vec::with_capacity(readable.len());
        for (summary, read) in readable.into_iter().zip(self.read_objects(&wanted)) {
            match read {
                Ok(object) => held.push((summary, object)),
                Err(why) => self.record(Diagnostic::warning(
                    &summary.uri,
                    format!("not read, its references are not listed: {why}"),
                )),
            }
        }
        let graph = graph::build(
            held.iter()
                .map(|(summary, object)| (*summary, object.as_reflect())),
        );
        let contained_owned: Vec<ObjectSummary> = contained.iter().map(|entry| (*entry).clone()).collect();
        let written = graph::attribute_to_writers(
            graph.edges,
            held.iter()
                .map(|(summary, object)| (*summary, object.as_reflect())),
            &contained_owned,
        );
        if uris.is_empty() {
            return Ok(written);
        }
        // What was asked for, and what the asked objects hold: a part read
        // only for a contained object it holds says nothing of its own.
        Ok(written
            .into_iter()
            .filter(|edge| {
                uris.contains(&edge.from)
                    || by_uri
                        .get(edge.from.as_str())
                        .is_some_and(|entry| uris.iter().any(|uri| holds(&by_uri, entry, uri)))
            })
            .collect())
    }
}

/// The parts that hold a contained object, at any depth: the containers that
/// are not themselves contained.
fn holding_parts(by_uri: &HashMap<&str, &ObjectSummary>, entry: &ObjectSummary) -> BTreeSet<String> {
    let mut parts = BTreeSet::new();
    let mut seen = HashSet::new();
    let mut up: Vec<&str> = entry.contained_in.iter().map(String::as_str).collect();
    while let Some(uri) = up.pop() {
        if !seen.insert(uri) {
            continue;
        }
        match by_uri.get(uri) {
            Some(container) => up.extend(container.contained_in.iter().map(String::as_str)),
            None => {
                parts.insert(uri.to_string());
            }
        }
    }
    parts
}

/// Whether the contained object `entry` sits, at any depth, inside `container`.
fn holds(by_uri: &HashMap<&str, &ObjectSummary>, entry: &ObjectSummary, container: &str) -> bool {
    let mut seen = HashSet::new();
    let mut up: Vec<&str> = entry.contained_in.iter().map(String::as_str).collect();
    while let Some(uri) = up.pop() {
        if uri == container {
            return true;
        }
        if !seen.insert(uri) {
            continue;
        }
        if let Some(above) = by_uri.get(uri) {
            up.extend(above.contained_in.iter().map(String::as_str));
        }
    }
    false
}

/// The pairs (contained uuid, direct container uuid), lowercase, that the text
/// of `root` states: each object nested in it, with the innermost nested object
/// whose path encloses its own, or `root` itself when there is none.
fn direct_holdings(root_uuid: &str, root: &dyn Reflect) -> Vec<(String, String)> {
    let nested = nested_objects(root);
    nested
        .iter()
        .map(|inner| {
            let parent = nested
                .iter()
                .filter(|outer| {
                    inner.path.len() > outer.path.len()
                        && inner.path.starts_with(outer.path.as_str())
                        && inner.path.as_bytes()[outer.path.len()] == b'.'
                })
                .max_by_key(|outer| outer.path.len())
                .map_or_else(
                    || root_uuid.to_string(),
                    |outer| outer.key.uuid.to_ascii_lowercase(),
                );
            (inner.key.uuid.to_ascii_lowercase(), parent)
        })
        .collect()
}
