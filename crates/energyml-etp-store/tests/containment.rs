//! Objects held by value (a WITSML `Log` holding `ChannelSet`s holding
//! `Channel`s), read through `EtpStore` over a **scripted** server that does
//! what ETP 1.2 says a server does and nothing more.
//!
//! The server lists every object it holds, contained ones included (section
//! 9.1.3.1, p. 179: contained objects are standalone data objects in their own
//! right), and states one `Edge` per relationship with the contained object as
//! source and its container as target (section 8.1.1.1.1, p. 156). It states
//! the by-value ones as Primary (section 8.1.1.1.2, p. 157). Nothing on the
//! wire says "by value": `Resource` has no field for it, `Edge` has none, and
//! `customData` is empty. What the client must still know, it proves from the
//! container's own text (`src/containment.rs`).
//!
//! The model is the one of the storage contract (`contract_witsml`), written
//! from the XSD, not from the code judged here. Three rules are asserted, each
//! with the section that states it:
//!
//! 1. `ObjectSummary::contained_in` names the direct containers (section 9.1.3,
//!    p. 178).
//! 2. A by-value edge is told from a reference (section 8.1.1.1.2, p. 157: a
//!    by-value relationship is Primary; a Secondary one is "always specified
//!    using the DOR construct"), and a Primary reference is not taken for one.
//! 3. A reference is listed once per place it is written, by the object that
//!    writes it (section 8.1.1.1.1, p. 156), whether or not the server lists
//!    the contained objects.
//!
//! To make these tests fail: leave `with_containers` out of `resources()`
//! (rule 1); make `derive_containment` accept every candidate without the
//! `held.contains` proof, or drop `mark_containment` from `native_edges`
//! (rule 2); delete the `references` override of the `EnergymlStorage` impl, so
//! the trait's default reads every listed object (rule 3).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use energyml_etp_store::EtpStore;
use energyml_storage::contract_witsml::{self as witsml, WitsmlModel};
use energyml_storage::graph::Edge;
use energyml_storage::storage::EnergymlStorage;
use retp_avro::Role;
use retp_client::memory::{ManualClock, MemoryTransport};
use retp_client::{Client, Settings, reading_client};
use retp_proto::connection::{Connection, Declined, Handled, Handler, Responder};
use retp_proto::negotiate::{Offer, ProtocolOffer};
use retp_proto::wire::{Body, Delivery, EtpError};
use retp_types::datatypes::object::{
    ActiveStatusKind, DataObject, Edge as EtpEdge, RelationshipKind, Resource,
};
use retp_types::datatypes::{SupportedDataObject, Uuid};
use retp_types::dispatch::Message;
use retp_types::protocol::discovery::{GetResourcesEdgesResponse, GetResourcesResponse};
use retp_types::protocol::store::GetDataObjectsResponse;

const S: i64 = 1_000_000;
const T0: i64 = 1_760_000_000 * S;
const DATASPACE: &str = "eml:///dataspace('demo')";

fn block<F: std::future::Future>(future: F) -> F::Output {
    futures_executor::block_on(future)
}

fn uri_of(qualified_type: &str, uuid: &str) -> String {
    format!("{DATASPACE}/{qualified_type}({uuid})")
}

fn uuid_of(uri: &str) -> String {
    let (_, tail) = uri.rsplit_once('(').expect("an object URI");
    tail.trim_end_matches(')').to_string()
}

/// What the scripted server holds and what it was asked.
#[derive(Default)]
struct Server {
    resources: Vec<Resource>,
    edges: Vec<EtpEdge>,
    /// The objects it serves, by URI. Others are `ENOT_FOUND`.
    objects: BTreeMap<String, String>,
    /// How many `GetResources` it was sent.
    listings: usize,
    /// The URIs of every `GetDataObjects`.
    asked: Vec<String>,
}

struct Scripted {
    protocol: i32,
    server: Arc<Mutex<Server>>,
}

impl Handler for Scripted {
    fn offer(&self) -> ProtocolOffer {
        ProtocolOffer::v12(self.protocol, Role::Store)
    }

    fn handle(&mut self, delivery: &Delivery, responder: &mut Responder<'_>) -> Result<Handled, Declined> {
        let Body::Message(message) = &delivery.body else {
            return Err(Declined("no private protocol".into()));
        };
        let mut server = self.server.lock().unwrap();
        match message {
            Message::GetResources(request) => {
                server.listings += 1;
                let mut response = responder.wire().begin_response(delivery);
                if request.include_edges {
                    let edges = GetResourcesEdgesResponse {
                        edges: server.edges.clone(),
                    };
                    let out = responder.wire().part(&mut response, &edges).unwrap();
                    responder.send(out);
                }
                let out = responder
                    .wire()
                    .last(
                        &mut response,
                        &GetResourcesResponse {
                            resources: server.resources.clone(),
                        },
                    )
                    .unwrap();
                responder.send_all(out);
            }
            Message::GetDataObjects(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.uris.keys().cloned());
                let mut found = BTreeMap::new();
                for (key, requested) in &request.uris {
                    server.asked.push(requested.clone());
                    match server.objects.get(requested) {
                        Some(xml) => {
                            reply.answered(key.clone()).unwrap();
                            found.insert(
                                key.clone(),
                                DataObject {
                                    data: xml.clone().into_bytes(),
                                    format: "xml".into(),
                                    ..Default::default()
                                },
                            );
                        }
                        None => reply
                            .failed(key.clone(), &EtpError::ENOT_FOUND.with_detail(requested.clone()))
                            .unwrap(),
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

fn resource(uri: String, name: &str) -> Resource {
    Resource {
        uri,
        name: name.to_string(),
        store_created: T0,
        store_last_write: T0,
        active_status: ActiveStatusKind::Active,
        ..Default::default()
    }
}

fn etp_edge(from: &str, to: &str, kind: RelationshipKind) -> EtpEdge {
    EtpEdge {
        source_uri: from.to_string(),
        target_uri: to.to_string(),
        relationship_kind: kind,
        ..Default::default()
    }
}

fn store_over(server: Arc<Mutex<Server>>) -> EtpStore<MemoryTransport, ManualClock> {
    let mut offer = Offer::new("a scripted server", "0.1", Uuid([2; 16]));
    offer.data_objects = ["witsml21.*", "eml23.*"]
        .iter()
        .map(|t| SupportedDataObject {
            qualified_type: t.to_string(),
            data_object_capabilities: BTreeMap::new(),
        })
        .collect();
    offer.formats = vec!["xml".to_string()];
    let handlers: Vec<Box<dyn Handler>> = [3, 4]
        .iter()
        .map(|protocol| {
            Box::new(Scripted {
                protocol: *protocol,
                server: Arc::clone(&server),
            }) as Box<dyn Handler>
        })
        .collect();
    let connection = Connection::server(offer, handlers, None, Uuid([0xAB; 16]), T0).unwrap();
    let clock = ManualClock::new(T0);
    let (offer, handlers) = reading_client(
        "the containment tests",
        "0.1",
        Uuid([1; 16]),
        &["witsml21.*".to_string(), "eml23.*".to_string()],
    );
    let client = Client::open(
        MemoryTransport::new(connection, clock.clone()),
        clock,
        offer,
        handlers,
        None,
        Settings::default(),
    )
    .unwrap();
    EtpStore::new(client, "demo", Arc::new(energyml_schemas::all()))
}

/// The server of the WITSML model: it lists the parts and the contained
/// objects, serves the parts and the `Channel`s (each as a standalone data
/// object), and states the relationships the way section 8.1.1.1.2 says the
/// WITSML rules do: the by-value ones Primary, the reference of a `Channel` to
/// a `Wellbore` Secondary.
///
/// `reference_kind` is what it states for the DORs: Secondary, or Primary for a
/// server whose rules make them so (in RESQML all relationships are primary,
/// section 8.2.2, p. 167).
fn witsml_server(model: &WitsmlModel, reference_kind: RelationshipKind) -> Arc<Mutex<Server>> {
    let mut server = Server::default();
    let uri_in_model = |uuid: &str| uri_of(model.type_of(uuid), uuid);
    for part in &model.parts {
        let uri = uri_of(part.qualified_type, &part.uuid);
        server.resources.push(resource(uri.clone(), &part.title));
        server.objects.insert(uri, part.xml.clone());
    }
    for content in &model.contents {
        let uri = uri_of(content.qualified_type, content.uuid);
        server.resources.push(resource(uri.clone(), content.title));
        if content.qualified_type == "witsml21.Channel" {
            let channel = witsml::standalone_channel(content.uuid, content.title);
            server.objects.insert(uri, channel.xml);
        }
    }
    for (contained, container) in model.containment() {
        server.edges.push(etp_edge(
            &uri_in_model(&contained),
            &uri_in_model(&container),
            RelationshipKind::Primary,
        ));
    }
    for (holder, target) in model.stated().difference(&model.containment()) {
        server.edges.push(etp_edge(
            &uri_in_model(holder),
            &uri_in_model(target),
            reference_kind,
        ));
    }
    Arc::new(Mutex::new(server))
}

fn uuid_pairs<'a>(edges: impl IntoIterator<Item = &'a Edge>) -> BTreeSet<(String, String)> {
    edges
        .into_iter()
        .map(|e| (uuid_of(&e.from), uuid_of(&e.to)))
        .collect()
}

/// Rule 1. ETP 1.2 section 9.1.3 (p. 178): an object "MAY be included in one or
/// more container data objects"; section 8.1.1.1.1 (p. 156) makes the contained
/// object the source of the relationship and its container the target. A
/// `Channel` is contained by its `ChannelSet`s (not by the `Log`, the container
/// of its container), a `Log` by none.
#[test]
fn the_direct_containers_of_an_object_are_derived_from_the_container_text() {
    let model = witsml::model_witsml();
    let store = store_over(witsml_server(&model, RelationshipKind::Secondary));
    let listed = block(store.resources()).expect("listed");
    assert_eq!(
        listed.len(),
        model.held_uuids().len(),
        "every object the server lists"
    );
    for summary in &listed {
        let uuid = uuid_of(&summary.uri);
        let said: BTreeSet<String> = summary.contained_in.iter().map(|u| uuid_of(u)).collect();
        let wanted: BTreeSet<String> = model.containers_of(&uuid).iter().map(|c| c.to_string()).collect();
        assert_eq!(said, wanted, "the direct containers of {uuid}");
    }
    // The header of a contained object says the same.
    let channel = uri_of("witsml21.Channel", witsml::CHANNEL_SHARED);
    let header = block(store.header(&channel)).expect("header");
    assert_eq!(
        header.summary.contained_in.len(),
        2,
        "the shared Channel is in two sets"
    );
}

/// Rule 2. Containment edges are marked `by_value` and the references are not.
/// ETP 1.2 section 8.1.1.1.2 (p. 157) and 23.34.13 (p. 473): a by-value
/// relationship is Primary; a Secondary one is a DOR. `Channel -> Wellbore` is
/// the specification's own example of a secondary reference.
#[test]
fn a_containment_edge_is_told_from_a_reference() {
    let model = witsml::model_witsml();
    let store = store_over(witsml_server(&model, RelationshipKind::Secondary));
    let edges = block(store.edges(&[])).expect("edges");
    let by_value = uuid_pairs(edges.iter().filter(|e| e.by_value));
    assert_eq!(by_value, model.containment());
    // Every edge of the model is there, once, and none is both.
    assert_eq!(uuid_pairs(&edges), model.stated());
    let reference = edges
        .iter()
        .find(|e| uuid_of(&e.from) == witsml::CHANNEL_SHARED && uuid_of(&e.to) == witsml::WELLBORE)
        .expect("the reference of the Channel to the Wellbore");
    assert!(!reference.by_value);
    // The same for the edges leaving one object.
    let shared = uri_of("witsml21.Channel", witsml::CHANNEL_SHARED);
    let leaving = block(store.edges(std::slice::from_ref(&shared))).expect("edges of one object");
    assert_eq!(
        uuid_pairs(leaving.iter().filter(|e| e.by_value)),
        BTreeSet::from([
            (witsml::CHANNEL_SHARED.to_string(), witsml::SET_A.to_string()),
            (witsml::CHANNEL_SHARED.to_string(), witsml::SET_PPFG.to_string()),
        ])
    );
}

/// Rule 2, the other way: a server whose rules make every relationship Primary
/// (RESQML, ETP 1.2 section 8.2.2, p. 167) states `Channel -> Wellbore` as a
/// Primary edge, and the wire cannot tell it from a containment. It is not
/// taken for one, because the `Wellbore` does not hold the `Channel`.
#[test]
fn a_primary_reference_is_not_taken_for_a_containment() {
    let model = witsml::model_witsml();
    let store = store_over(witsml_server(&model, RelationshipKind::Primary));
    let edges = block(store.edges(&[])).expect("edges");
    assert_eq!(
        uuid_pairs(edges.iter().filter(|e| e.by_value)),
        model.containment()
    );
    let listed = block(store.resources()).expect("listed");
    let wellbore = listed
        .iter()
        .find(|s| uuid_of(&s.uri) == witsml::WELLBORE)
        .expect("the Wellbore");
    assert!(wellbore.contained_in.is_empty());
}

/// Rule 3. ETP 1.2 section 8.1.1.1.1 (p. 156): the data object that contains a
/// DOR is its source. Each DOR the parts write is listed once per place it is
/// written, by the object that writes it: the `Channel`, not the `Log` that
/// holds it inline, and not once more because the server also lists and serves
/// that `Channel` as a data object (section 9.1.3.1). The expected count is
/// read off the text of the parts, one `eml:Uuid` per DOR.
#[test]
fn a_reference_is_listed_once_per_place_it_is_written() {
    let model = witsml::model_witsml();
    let server = witsml_server(&model, RelationshipKind::Secondary);
    let store = store_over(server.clone());
    let written: usize = model
        .parts
        .iter()
        .map(|p| p.xml.matches("<eml:Uuid>").count())
        .sum();
    assert_eq!(written, 11, "the DORs the model writes, counted in its text");

    let references = block(store.references(&[])).expect("references");
    assert_eq!(references.len(), written);
    assert_eq!(
        uuid_pairs(&references),
        model.written().into_iter().collect::<BTreeSet<_>>()
    );
    for container in [witsml::LOG, witsml::SET_A, witsml::SET_PPFG] {
        assert!(
            references.iter().all(|e| uuid_of(&e.from) != container),
            "the container {container} writes no reference of its own"
        );
    }
    // The contained objects were not read: their references come with the
    // part that holds them.
    let contained: BTreeSet<String> = model.content_uuids();
    let asked = server.lock().unwrap().asked.clone();
    assert!(
        asked.iter().all(|uri| !contained.contains(&uuid_of(uri))),
        "a contained object was read for its references: {asked:?}"
    );
}

/// The references of one contained object are its own, read from the part that
/// holds it (the trait's rule for an asked object), and the part is read once.
#[test]
fn the_references_of_a_contained_object_are_read_from_its_part() {
    let model = witsml::model_witsml();
    let server = witsml_server(&model, RelationshipKind::Secondary);
    let store = store_over(server.clone());
    let shared = uri_of("witsml21.Channel", witsml::CHANNEL_SHARED);
    let references = block(store.references(std::slice::from_ref(&shared))).expect("references");
    assert!(!references.is_empty());
    assert!(references.iter().all(|e| e.from == shared));
    // One reference to the Wellbore, one to the kind, one to the company, in
    // each of the two sets that hold the Channel.
    assert_eq!(references.len(), 6);
    let asked = server.lock().unwrap().asked.clone();
    let log = uri_of("witsml21.Log", witsml::LOG);
    assert!(
        asked.iter().all(|uri| *uri == log),
        "only the part is read: {asked:?}"
    );
}

/// The cost is bounded: a dataspace that holds no type which can be nested
/// asks nothing more than before (one listing, no object).
#[test]
fn a_dataspace_without_nestable_types_pays_nothing() {
    let mut server = Server::default();
    let well = uri_of("witsml21.Well", witsml::WELL);
    let wellbore = uri_of("witsml21.Wellbore", witsml::WELLBORE);
    server.resources.push(resource(well.clone(), "a well"));
    server.resources.push(resource(wellbore.clone(), "a wellbore"));
    server
        .edges
        .push(etp_edge(&wellbore, &well, RelationshipKind::Primary));
    let server = Arc::new(Mutex::new(server));
    let store = store_over(server.clone());
    let listed = block(store.resources()).expect("listed");
    assert!(listed.iter().all(|s| s.contained_in.is_empty()));
    let edges = block(store.edges(&[])).expect("edges");
    assert_eq!(edges.len(), 1);
    assert!(!edges[0].by_value);
    let server = server.lock().unwrap();
    // One listing for `resources`, one for `edges`; nothing read.
    assert_eq!(server.listings, 2);
    assert!(server.asked.is_empty());
}
