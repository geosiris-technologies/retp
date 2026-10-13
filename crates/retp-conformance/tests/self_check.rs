//! The suite, against a store of this crate's own - and against the same
//! store told to misbehave.
//!
//! **A check that cannot fail proves nothing.** So the fake has knobs, one
//! per rule a store could break without the session refusing it, and each
//! test turns one on and reads the finding that must go red. A knob that
//! turns nothing red is a check that is not looking.

use std::collections::BTreeMap;

use retp_avro::Role;
use retp_client::memory::{ManualClock, MemoryTransport};
use retp_client::{Client, Settings, writing_client};
use retp_conformance::checks::run;
use retp_conformance::{Options, Report, Verdict};
use retp_proto::connection::{Connection, Declined, Handled, Handler, Responder};
use retp_proto::negotiate::{Offer, ProtocolOffer};
use retp_proto::wire::{Body, Delivery, EtpError};
use retp_types::datatypes::ArrayOfString;
use retp_types::datatypes::data_array_types::{DataArray, DataArrayMetadata};
use retp_types::datatypes::object::PutResponse;
use retp_types::datatypes::object::{DataObject, Dataspace, Resource, SupportedType};
use retp_types::datatypes::{
    AnyArray, AnyArrayItem, AnyArrayType, AnyLogicalArrayType, ArrayOfDouble, ArrayOfFloat,
};
use retp_types::datatypes::{SupportedDataObject, Uuid};
use retp_types::dispatch::Message;
use retp_types::protocol::data_array::{
    GetDataArrayMetadataResponse, GetDataArraysResponse, GetDataSubarraysResponse, PutDataArraysResponse,
    PutDataSubarraysResponse, PutUninitializedDataArraysResponse,
};
use retp_types::protocol::dataspace::{
    DeleteDataspacesResponse, GetDataspacesResponse, PutDataspacesResponse,
};
use retp_types::protocol::discovery::{GetDeletedResourcesResponse, GetResourcesResponse};
use retp_types::protocol::store::{
    DeleteDataObjectsResponse, GetDataObjectsResponse, PutDataObjectsResponse,
};
use retp_types::protocol::supported_types::GetSupportedTypesResponse;
use retp_types::protocol::transaction::{
    CommitTransactionResponse, RollbackTransactionResponse, StartTransactionResponse,
};

const S: i64 = 1_000_000;
const T0: i64 = 1_760_000_000 * S;
const SPACE: &str = "eml:///dataspace('demo')";
const FEATURE: &str =
    "eml:///dataspace('demo')/resqml20.obj_GeneticBoundaryFeature(aa000000-0000-4000-8000-000000000001)";
const POINTS: &str = "bb000000-0000-4000-8000-000000000002";
const PROXY: &str = "cc000000-0000-4000-8000-000000000003";
const POINTS_URI: &str =
    "eml:///dataspace('demo')/resqml20.obj_PointSetRepresentation(bb000000-0000-4000-8000-000000000002)";
const PROXY_URI: &str =
    "eml:///dataspace('demo')/eml20.obj_EpcExternalPartReference(cc000000-0000-4000-8000-000000000003)";

fn citation(title: &str) -> String {
    format!(
        "<eml:Citation><eml:Title>{title}</eml:Title><eml:Originator>test</eml:Originator>\
<eml:Creation>2026-08-02T00:00:00Z</eml:Creation><eml:Format>test</eml:Format></eml:Citation>"
    )
}

/// A point set whose points hang from a RESQML 2.0.1 proxy: the array the
/// DataArray checks have to find, through the listing.
fn points_xml() -> String {
    format!(
        "<resqml:obj_PointSetRepresentation xmlns:resqml=\"http://www.energistics.org/energyml/data/resqmlv2\" \
xmlns:eml=\"http://www.energistics.org/energyml/data/commonv2\" uuid=\"{POINTS}\" schemaVersion=\"2.0\">{}\
<resqml:NodePatch><resqml:PatchIndex>0</resqml:PatchIndex><resqml:Count>4</resqml:Count><resqml:Geometry>\
<resqml:Points xsi:type=\"resqml:Point3dHdf5Array\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\
<resqml:Coordinates><eml:PathInHdfFile>/points</eml:PathInHdfFile>\
<eml:HdfProxy><eml:ContentType>application/x-eml+xml;version=2.0;type=obj_EpcExternalPartReference</eml:ContentType>\
<eml:Title>proxy</eml:Title><eml:UUID>{PROXY}</eml:UUID></eml:HdfProxy></resqml:Coordinates>\
</resqml:Points></resqml:Geometry></resqml:NodePatch></resqml:obj_PointSetRepresentation>",
        citation("a point set")
    )
}

fn proxy_xml() -> String {
    format!(
        "<eml:EpcExternalPartReference xmlns:eml=\"http://www.energistics.org/energyml/data/commonv2\" \
uuid=\"{PROXY}\" schemaVersion=\"2.0\">{}<eml:MimeType>application/x-hdf5</eml:MimeType></eml:EpcExternalPartReference>",
        citation("the HDF5 proxy")
    )
}

/// How the fake store may misbehave.
#[derive(Debug, Clone, Copy, Default)]
struct Knobs {
    /// Resources come with a URI that is not canonical.
    misspelt_uris: bool,
    /// A malformed URI is answered with resources instead of EINVALID_URI.
    lenient_uris: bool,
    /// An absent object is silently left out of the map instead of ENOT_FOUND.
    forgets_absent: bool,
    /// storeLastWrite comes before storeCreated.
    clock_backwards: bool,
    /// A deleted dataspace stays listed.
    keeps_deleted: bool,
    /// The array comes back with fewer values than its metadata declares.
    short_array: bool,
    /// A second transaction opens while one is open.
    unlimited_transactions: bool,
    /// A rollback applies what was sent, as a commit would.
    ignores_rollback: bool,
    /// Another client writes into the suite's dataspace as soon as it is
    /// created: an object the suite must never delete, nor its dataspace.
    foreign_in_new_space: bool,
    /// The arrays of `demo` cannot be served; `other` holds a copy that can.
    broken_demo_arrays: bool,
    /// Creating a dataspace is refused, as to a user who is not an admin.
    refuses_dataspaces: bool,
    /// The store records an activity of its own for every object written
    /// into a dataspace the suite made, as some stores do.
    records_activities: bool,
    /// GetDataSubarrays ignores `starts`: every sub-array is read from the
    /// first index.
    shifted_subarrays: bool,
    /// PutDataSubarrays is answered without the key of the block, and the
    /// block is not written - what some servers do with an object they drop.
    forgets_block_keys: bool,
    /// GetDataArrayMetadata gives the dimensions of an array the suite
    /// wrote the other way round.
    transposed_metadata: bool,
    /// The doubles written are kept as 32-bit floats, and served so - what
    /// some servers do.
    stores_float32: bool,
    /// No dataspace is listed: everything is in the default one, `eml:///` -
    /// what some WITSML stores do.
    no_dataspaces: bool,
    /// GetDeletedResources is refused.
    refuses_deleted: bool,
    /// A GetDataObjects of several URIs leaves the last one out, unsaid.
    drops_one_of_several: bool,
    /// GetResources from an object is refused, as if the object were not there.
    refuses_object_graph: bool,
    /// GetSupportedTypes is never answered: the request times out.
    silent_supported_types: bool,
    /// GetResources with activeStatusFilter null cannot be read, as
    /// some servers cannot: it answers when the filter is set.
    cannot_read_null_filter: bool,
    /// GetResources with no filter is refused for its size
    /// (ERESPONSECOUNT_EXCEEDED, a 10 000 limit), and answered filtered.
    refuses_for_size: bool,
}

/// The demo point set's four nodes: rows a sub-array can tell apart.
const DEMO_VALUES: [f64; 12] = [
    0.0, 0.0, 0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0,
];

/// A second dataspace, holding a copy of `demo`'s point set.
const OTHER: &str = "eml:///dataspace('other')";
/// What another client puts in the suite's dataspace (`foreign_in_new_space`).
const FOREIGN: &str = "resqml20.obj_GeneticBoundaryFeature(dd000000-0000-4000-8000-000000000004)";

/// `demo`'s URIs, moved to `other`.
fn in_other(uri: &str) -> String {
    uri.replace(SPACE, OTHER)
}

/// The dataspace an object URI is in.
fn space_of(uri: &str) -> String {
    match uri.find("')") {
        Some(at) if uri.starts_with("eml:///dataspace(") => uri[..at + 2].to_string(),
        _ => DEFAULT.to_string(),
    }
}

/// The default dataspace, which every store has (§21.2.2 row 1).
const DEFAULT: &str = "eml:///";

/// What a transaction holds until it ends.
#[derive(Default)]
struct Open {
    uuid: Option<Uuid>,
    staged: Vec<(String, DataObject)>,
}

/// A store: one dataspace, one feature, and whatever the suite creates.
struct Store {
    protocol: i32,
    knobs: Knobs,
    /// The dataspaces the suite created (and their objects).
    made: std::sync::Arc<std::sync::Mutex<BTreeMap<String, BTreeMap<String, DataObject>>>>,
    /// The transaction open, if one is.
    open: std::sync::Arc<std::sync::Mutex<Open>>,
    /// The arrays written, by (uri, path): dimensions and values.
    arrays: Written,
}

/// The arrays a store was given, by (uri, path): dimensions and values.
type Written = std::sync::Arc<std::sync::Mutex<BTreeMap<(String, String), (Vec<i64>, Vec<f64>)>>>;

/// Put an object where it belongs, or say the dataspace is not there.
fn apply(made: &mut BTreeMap<String, BTreeMap<String, DataObject>>, uri: &str, object: &DataObject) -> bool {
    let space = space_of(uri);
    match made.get_mut(&space) {
        Some(held) => {
            held.insert(uri.to_string(), object.clone());
            true
        }
        None => false,
    }
}

impl Store {
    fn stamps(&self) -> (i64, i64) {
        if self.knobs.clock_backwards {
            (T0, T0 - S)
        } else {
            (T0, T0 + S)
        }
    }

    /// The values as the store keeps them.
    fn kept(&self, values: Vec<f64>) -> Vec<f64> {
        if self.knobs.stores_float32 {
            values.into_iter().map(|v| f64::from(v as f32)).collect()
        } else {
            values
        }
    }
}

/// Values as they travel back: doubles, or 32-bit floats.
fn served(values: Vec<f64>, float32: bool) -> AnyArray {
    AnyArray {
        item: if float32 {
            AnyArrayItem::ArrayOfFloat(ArrayOfFloat {
                values: values.into_iter().map(|v| v as f32).collect(),
            })
        } else {
            AnyArrayItem::ArrayOfDouble(ArrayOfDouble { values })
        },
    }
}

impl Handler for Store {
    fn offer(&self) -> ProtocolOffer {
        ProtocolOffer::v12(self.protocol, Role::Store)
    }

    fn handle(&mut self, delivery: &Delivery, responder: &mut Responder<'_>) -> Result<Handled, Declined> {
        let Body::Message(message) = &delivery.body else {
            return Err(Declined("no private protocol".into()));
        };
        let (created, written) = self.stamps();
        let mut made = self.made.lock().unwrap();
        match message {
            Message::GetDataspaces(_) if self.knobs.no_dataspaces => {
                let out = responder
                    .wire()
                    .respond(
                        delivery,
                        &GetDataspacesResponse {
                            dataspaces: Vec::new(),
                        },
                    )
                    .unwrap();
                responder.send(out);
            }
            Message::GetDataspaces(_) => {
                let mut dataspaces = vec![Dataspace {
                    uri: SPACE.into(),
                    path: "demo".into(),
                    store_created: created,
                    store_last_write: written,
                    ..Default::default()
                }];
                dataspaces.push(Dataspace {
                    uri: OTHER.into(),
                    path: "other".into(),
                    store_created: created,
                    store_last_write: written,
                    ..Default::default()
                });
                for uri in made.keys().filter(|uri| uri.as_str() != DEFAULT) {
                    dataspaces.push(Dataspace {
                        uri: uri.clone(),
                        path: uri
                            .trim_start_matches("eml:///dataspace('")
                            .trim_end_matches("')")
                            .to_string(),
                        store_created: created,
                        store_last_write: written,
                        ..Default::default()
                    });
                }
                let out = responder
                    .wire()
                    .respond(delivery, &GetDataspacesResponse { dataspaces })
                    .unwrap();
                responder.send(out);
            }
            Message::GetResources(request)
                if self.knobs.refuses_for_size
                    && request.active_status_filter.is_none()
                    && request.context.uri.ends_with("('demo')") =>
            {
                let out = responder.wire().refuse(
                    delivery,
                    &EtpError::ERESPONSECOUNT_EXCEEDED
                        .with_detail("GetResources response exceeded MaxResponseCount 10000"),
                );
                responder.send(out);
            }
            Message::GetResources(request)
                if self.knobs.cannot_read_null_filter && request.active_status_filter.is_none() =>
            {
                let out = responder.wire().refuse(delivery, &EtpError::EINVALID_MESSAGE.with_detail("Error while trying deserialize protocol 3 messageType : 1 messageName : GetResources"));
                responder.send(out);
            }
            Message::GetResources(request) => {
                let uri = &request.context.uri;
                if !uri.starts_with("eml:///") && !self.knobs.lenient_uris {
                    let out = responder
                        .wire()
                        .refuse(delivery, &EtpError::EINVALID_URI.with_detail(uri.clone()));
                    responder.send(out);
                    return Ok(Handled::Answered);
                }
                // An object's URI: the store knows it, and its graph is empty.
                let object = uri
                    .rsplit('/')
                    .next()
                    .is_some_and(|last| last.contains('(') && !last.starts_with("dataspace("));
                if object {
                    let out = if self.knobs.refuses_object_graph {
                        responder
                            .wire()
                            .refuse(delivery, &EtpError::ENOT_FOUND.with_detail(uri.clone()))
                    } else {
                        responder
                            .wire()
                            .respond(
                                delivery,
                                &GetResourcesResponse {
                                    resources: Vec::new(),
                                },
                            )
                            .unwrap()
                    };
                    responder.send(out);
                    return Ok(Handled::Answered);
                }
                let known = uri == SPACE
                    || uri == OTHER
                    || uri == DEFAULT
                    || made.contains_key(uri)
                    || !uri.starts_with("eml:///");
                if !known {
                    let out = responder
                        .wire()
                        .refuse(delivery, &EtpError::ENOT_FOUND.with_detail(uri.clone()));
                    responder.send(out);
                    return Ok(Handled::Answered);
                }
                let mut resources = Vec::new();
                let demo = vec![
                    (FEATURE.to_string(), "a feature".to_string()),
                    (POINTS_URI.to_string(), "a point set".to_string()),
                    (PROXY_URI.to_string(), "the HDF5 proxy".to_string()),
                ];
                let objects: Vec<(String, String)> = if uri == DEFAULT {
                    let mut objects = if self.knobs.no_dataspaces {
                        demo
                    } else {
                        Vec::new()
                    };
                    objects.extend(
                        made.get(DEFAULT)
                            .into_iter()
                            .flat_map(|held| held.keys().map(|k| (k.clone(), "made".to_string()))),
                    );
                    objects
                } else if let Some(held) = made.get(uri) {
                    held.keys().map(|k| (k.clone(), "made".to_string())).collect()
                } else if uri == OTHER {
                    demo.into_iter().map(|(u, n)| (in_other(&u), n)).collect()
                } else {
                    demo
                };
                for (object, name) in objects {
                    let spelt = if self.knobs.misspelt_uris {
                        object.replace("eml:///", "etp://")
                    } else {
                        object
                    };
                    resources.push(Resource {
                        uri: spelt,
                        name,
                        store_created: created,
                        store_last_write: written,
                        ..Default::default()
                    });
                }
                let out = responder
                    .wire()
                    .respond(delivery, &GetResourcesResponse { resources })
                    .unwrap();
                responder.send(out);
            }
            Message::GetDataObjects(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.uris.keys().cloned());
                let mut found = BTreeMap::new();
                for (key, uri) in &request.uris {
                    let served = |xml: String| DataObject {
                        data: xml.into_bytes(),
                        format: "xml".into(),
                        ..Default::default()
                    };
                    let held = if uri == FEATURE {
                        Some(served("<resqml:obj_GeneticBoundaryFeature uuid=\"aa000000-0000-4000-8000-000000000001\"/>".into()))
                    } else if uri == POINTS_URI {
                        Some(served(points_xml()))
                    } else if uri == PROXY_URI {
                        Some(served(proxy_xml()))
                    } else if *uri == in_other(POINTS_URI) {
                        Some(served(points_xml()))
                    } else if *uri == in_other(PROXY_URI) {
                        Some(served(proxy_xml()))
                    } else {
                        made.values().find_map(|held| held.get(uri).cloned())
                    };
                    let last = request.uris.len() > 1 && request.uris.keys().next_back() == Some(key);
                    match held {
                        Some(_) if self.knobs.drops_one_of_several && last => {
                            reply.answered(key.clone()).unwrap()
                        }
                        Some(object) => {
                            reply.answered(key.clone()).unwrap();
                            found.insert(key.clone(), object);
                        }
                        None if self.knobs.forgets_absent => {
                            reply.answered(key.clone()).unwrap();
                        }
                        None => reply
                            .failed(key.clone(), &EtpError::ENOT_FOUND.with_detail(uri.clone()))
                            .unwrap(),
                    }
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &GetDataObjectsResponse { data_objects: found })
                    .unwrap();
                responder.send_all(out);
            }
            Message::PutDataObjects(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_objects.keys().cloned());
                let mut success = BTreeMap::new();
                let mut open = self.open.lock().unwrap();
                for (key, object) in &request.data_objects {
                    let uri = object.resource.uri.clone();
                    // Inside a transaction the put waits for the commit.
                    let taken = if open.uuid.is_some() {
                        open.staged.push((uri.clone(), object.clone()));
                        true
                    } else {
                        apply(&mut made, &uri, object)
                    };
                    if taken && self.knobs.records_activities {
                        let space = space_of(&uri);
                        if space != SPACE {
                            let n = made.get(&space).map(|held| held.len()).unwrap_or(0);
                            let activity =
                                format!("{space}/resqml20.obj_Activity(ee000000-0000-4000-8000-{n:012x})");
                            if let Some(held) = made.get_mut(&space) {
                                held.insert(
                                    activity.clone(),
                                    DataObject {
                                        resource: Resource {
                                            uri: activity,
                                            ..Default::default()
                                        },
                                        format: "xml".into(),
                                        ..Default::default()
                                    },
                                );
                            }
                        }
                    }
                    if taken {
                        reply.answered(key.clone()).unwrap();
                        success.insert(key.clone(), PutResponse::default());
                    } else {
                        reply
                            .failed(
                                key.clone(),
                                &EtpError::ENOT_FOUND.with_detail(format!("{uri}: no such dataspace")),
                            )
                            .unwrap();
                    }
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &PutDataObjectsResponse { success })
                    .unwrap();
                responder.send_all(out);
            }
            Message::DeleteDataObjects(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.uris.keys().cloned());
                let mut deleted_uris = BTreeMap::new();
                for (key, uri) in &request.uris {
                    let mut gone = false;
                    for held in made.values_mut() {
                        gone |= held.remove(uri).is_some();
                    }
                    if gone {
                        reply.answered(key.clone()).unwrap();
                        deleted_uris.insert(
                            key.clone(),
                            ArrayOfString {
                                values: vec![uri.clone()],
                            },
                        );
                    } else {
                        reply
                            .failed(key.clone(), &EtpError::ENOT_FOUND.with_detail(uri.clone()))
                            .unwrap();
                    }
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &DeleteDataObjectsResponse { deleted_uris })
                    .unwrap();
                responder.send_all(out);
            }
            Message::PutDataspaces(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.dataspaces.keys().cloned());
                let mut success = BTreeMap::new();
                for (key, space) in &request.dataspaces {
                    if self.knobs.refuses_dataspaces {
                        reply
                            .failed(
                                key.clone(),
                                &EtpError::EREQUEST_DENIED.with_detail("must be an admin"),
                            )
                            .unwrap();
                        continue;
                    }
                    let held = made.entry(space.uri.clone()).or_default();
                    if self.knobs.foreign_in_new_space {
                        let uri = format!("{}/{FOREIGN}", space.uri);
                        held.insert(
                            uri.clone(),
                            DataObject {
                                resource: Resource {
                                    uri,
                                    ..Default::default()
                                },
                                format: "xml".into(),
                                ..Default::default()
                            },
                        );
                    }
                    reply.answered(key.clone()).unwrap();
                    success.insert(key.clone(), space.uri.clone());
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &PutDataspacesResponse { success })
                    .unwrap();
                responder.send_all(out);
            }
            Message::DeleteDataspaces(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.uris.keys().cloned());
                let mut success = BTreeMap::new();
                for (key, uri) in &request.uris {
                    let held = if self.knobs.keeps_deleted {
                        made.contains_key(uri)
                    } else {
                        made.remove(uri).is_some()
                    };
                    if held {
                        reply.answered(key.clone()).unwrap();
                        success.insert(key.clone(), uri.clone());
                    } else {
                        reply
                            .failed(key.clone(), &EtpError::ENOT_FOUND.with_detail(uri.clone()))
                            .unwrap();
                    }
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &DeleteDataspacesResponse { success })
                    .unwrap();
                responder.send_all(out);
            }
            Message::GetDataArrayMetadata(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_arrays.keys().cloned());
                let mut found = BTreeMap::new();
                let written = self.arrays.lock().unwrap();
                for (key, id) in &request.data_arrays {
                    let demo = id.uri == PROXY_URI && id.path_in_resource == "/points";
                    let other = id.uri == in_other(PROXY_URI) && id.path_in_resource == "/points";
                    let dimensions = if (demo && !self.knobs.broken_demo_arrays) || other {
                        Some(vec![4, 3])
                    } else {
                        written
                            .get(&(id.uri.clone(), id.path_in_resource.clone()))
                            .map(|(d, _)| {
                                if self.knobs.transposed_metadata {
                                    d.iter().rev().copied().collect()
                                } else {
                                    d.clone()
                                }
                            })
                    };
                    if demo && self.knobs.broken_demo_arrays {
                        reply
                            .failed(
                                key.clone(),
                                &EtpError::EREQUEST_DENIED.with_detail("Internal server error"),
                            )
                            .unwrap();
                    } else if let Some(dimensions) = dimensions {
                        reply.answered(key.clone()).unwrap();
                        let float32 = self.knobs.stores_float32 && !demo && !other;
                        found.insert(
                            key.clone(),
                            DataArrayMetadata {
                                dimensions,
                                transport_array_type: if float32 {
                                    AnyArrayType::ArrayOfFloat
                                } else {
                                    AnyArrayType::ArrayOfDouble
                                },
                                logical_array_type: if float32 {
                                    AnyLogicalArrayType::ArrayOfFloat32Le
                                } else {
                                    AnyLogicalArrayType::ArrayOfDouble64Le
                                },
                                ..Default::default()
                            },
                        );
                    } else {
                        reply
                            .failed(
                                key.clone(),
                                &EtpError::ENOT_FOUND
                                    .with_detail(format!("{} {}", id.uri, id.path_in_resource)),
                            )
                            .unwrap();
                    }
                }
                let out = responder
                    .wire()
                    .last(
                        &mut reply,
                        &GetDataArrayMetadataResponse {
                            array_metadata: found,
                        },
                    )
                    .unwrap();
                responder.send_all(out);
            }
            Message::GetDataArrays(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_arrays.keys().cloned());
                let mut found = BTreeMap::new();
                let written = self.arrays.lock().unwrap();
                for (key, id) in &request.data_arrays {
                    let demo = id.uri == PROXY_URI
                        && id.path_in_resource == "/points"
                        && !self.knobs.broken_demo_arrays;
                    let other = id.uri == in_other(PROXY_URI) && id.path_in_resource == "/points";
                    let held = if demo || other {
                        let values = if self.knobs.short_array {
                            vec![0.0, 0.0, 0.0]
                        } else {
                            DEMO_VALUES.to_vec()
                        };
                        Some((vec![4, 3], values))
                    } else {
                        written
                            .get(&(id.uri.clone(), id.path_in_resource.clone()))
                            .cloned()
                    };
                    let float32 = self.knobs.stores_float32 && !demo && !other;
                    if let Some((dimensions, values)) = held {
                        reply.answered(key.clone()).unwrap();
                        found.insert(
                            key.clone(),
                            DataArray {
                                dimensions,
                                data: served(values, float32),
                            },
                        );
                    } else {
                        reply
                            .failed(
                                key.clone(),
                                &EtpError::ENOT_FOUND.with_detail(id.path_in_resource.clone()),
                            )
                            .unwrap();
                    }
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &GetDataArraysResponse { data_arrays: found })
                    .unwrap();
                responder.send_all(out);
            }
            Message::PutDataArrays(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_arrays.keys().cloned());
                let mut success = BTreeMap::new();
                let mut written = self.arrays.lock().unwrap();
                for (key, put) in &request.data_arrays {
                    let values = match &put.array.data.item {
                        AnyArrayItem::ArrayOfDouble(ArrayOfDouble { values }) => values.clone(),
                        _ => Vec::new(),
                    };
                    written.insert(
                        (put.uid.uri.clone(), put.uid.path_in_resource.clone()),
                        (put.array.dimensions.clone(), self.kept(values)),
                    );
                    reply.answered(key.clone()).unwrap();
                    success.insert(key.clone(), String::new());
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &PutDataArraysResponse { success })
                    .unwrap();
                responder.send_all(out);
            }
            Message::PutUninitializedDataArrays(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_arrays.keys().cloned());
                let mut success = BTreeMap::new();
                let mut written = self.arrays.lock().unwrap();
                for (key, declared) in &request.data_arrays {
                    let dimensions = declared.metadata.dimensions.clone();
                    let count: i64 = dimensions.iter().product();
                    written.insert(
                        (declared.uid.uri.clone(), declared.uid.path_in_resource.clone()),
                        (dimensions, vec![0.0; count.max(0) as usize]),
                    );
                    reply.answered(key.clone()).unwrap();
                    success.insert(key.clone(), String::new());
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &PutUninitializedDataArraysResponse { success })
                    .unwrap();
                responder.send_all(out);
            }
            Message::PutDataSubarrays(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_subarrays.keys().cloned());
                let mut success = BTreeMap::new();
                let mut written = self.arrays.lock().unwrap();
                for (key, block) in &request.data_subarrays {
                    let place = (block.uid.uri.clone(), block.uid.path_in_resource.clone());
                    let values = match &block.data.item {
                        AnyArrayItem::ArrayOfDouble(ArrayOfDouble { values }) => values.clone(),
                        _ => Vec::new(),
                    };
                    match written.get_mut(&place) {
                        Some(_) if self.knobs.forgets_block_keys => reply.answered(key.clone()).unwrap(),
                        // Whole rows, as the suite writes them: the block starts at its first row.
                        Some((dimensions, held)) => {
                            let inner: i64 = dimensions[1..].iter().product();
                            let at = (block.starts[0] * inner) as usize;
                            held[at..at + values.len()].copy_from_slice(&self.kept(values));
                            reply.answered(key.clone()).unwrap();
                            success.insert(key.clone(), String::new());
                        }
                        None => reply
                            .failed(key.clone(), &EtpError::ENOT_FOUND.with_detail("not declared"))
                            .unwrap(),
                    }
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &PutDataSubarraysResponse { success })
                    .unwrap();
                responder.send_all(out);
            }
            Message::GetDataSubarrays(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_subarrays.keys().cloned());
                let mut found = BTreeMap::new();
                let written = self.arrays.lock().unwrap();
                for (key, asked) in &request.data_subarrays {
                    let id = &asked.uid;
                    let demo = (id.uri == PROXY_URI && !self.knobs.broken_demo_arrays)
                        || id.uri == in_other(PROXY_URI);
                    let held = if demo && id.path_in_resource == "/points" {
                        Some((vec![4, 3], DEMO_VALUES.to_vec()))
                    } else {
                        written
                            .get(&(id.uri.clone(), id.path_in_resource.clone()))
                            .cloned()
                    };
                    match held {
                        // Whole rows: the first dimension cut, the others whole.
                        Some((dimensions, values)) => {
                            let inner: i64 = dimensions[1..].iter().product();
                            let first = if self.knobs.shifted_subarrays {
                                0
                            } else {
                                asked.starts[0]
                            };
                            let from = (first * inner) as usize;
                            let to = from + (asked.counts[0] * inner) as usize;
                            reply.answered(key.clone()).unwrap();
                            let float32 = self.knobs.stores_float32 && !demo;
                            found.insert(
                                key.clone(),
                                DataArray {
                                    dimensions: asked.counts.clone(),
                                    data: served(values[from..to].to_vec(), float32),
                                },
                            );
                        }
                        None => reply
                            .failed(
                                key.clone(),
                                &EtpError::ENOT_FOUND.with_detail(id.path_in_resource.clone()),
                            )
                            .unwrap(),
                    }
                }
                let out = responder
                    .wire()
                    .last(
                        &mut reply,
                        &GetDataSubarraysResponse {
                            data_subarrays: found,
                        },
                    )
                    .unwrap();
                responder.send_all(out);
            }
            Message::GetDeletedResources(_) => {
                let out = if self.knobs.refuses_deleted {
                    responder
                        .wire()
                        .refuse(delivery, &EtpError::ENOTSUPPORTED.with_detail("no tombstones"))
                } else {
                    responder
                        .wire()
                        .respond(
                            delivery,
                            &GetDeletedResourcesResponse {
                                deleted_resources: Vec::new(),
                            },
                        )
                        .unwrap()
                };
                responder.send(out);
            }
            Message::GetSupportedTypes(_) if self.knobs.silent_supported_types => {}
            Message::GetSupportedTypes(_) => {
                let response = GetSupportedTypesResponse {
                    supported_types: vec![SupportedType {
                        data_object_type: "resqml20.obj_GeneticBoundaryFeature".into(),
                        object_count: Some(1),
                        ..Default::default()
                    }],
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            Message::StartTransaction(_) => {
                let mut open = self.open.lock().unwrap();
                if open.uuid.is_some() && !self.knobs.unlimited_transactions {
                    let out = responder.wire().refuse(
                        delivery,
                        &EtpError::EMAX_TRANSACTIONS_EXCEEDED.with_detail("one transaction at a time"),
                    );
                    responder.send(out);
                    return Ok(Handled::Answered);
                }
                let uuid = Uuid([open.staged.len() as u8 + 7; 16]);
                open.uuid = Some(uuid);
                let response = StartTransactionResponse {
                    transaction_uuid: uuid,
                    successful: true,
                    failure_reason: String::new(),
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            Message::CommitTransaction(request) => {
                let mut open = self.open.lock().unwrap();
                for (uri, object) in open.staged.drain(..) {
                    apply(&mut made, &uri, &object);
                }
                open.uuid = None;
                let response = CommitTransactionResponse {
                    transaction_uuid: request.transaction_uuid,
                    successful: true,
                    failure_reason: String::new(),
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            Message::RollbackTransaction(request) => {
                let mut open = self.open.lock().unwrap();
                for (uri, object) in open.staged.drain(..) {
                    if self.knobs.ignores_rollback {
                        apply(&mut made, &uri, &object);
                    }
                }
                open.uuid = None;
                let response = RollbackTransactionResponse {
                    transaction_uuid: request.transaction_uuid,
                    successful: true,
                    failure_reason: String::new(),
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            _ => return Err(Declined("not served".into())),
        }
        Ok(Handled::Answered)
    }
}

type Made = std::sync::Arc<std::sync::Mutex<BTreeMap<String, BTreeMap<String, DataObject>>>>;

fn client(knobs: Knobs) -> Client<MemoryTransport, ManualClock> {
    client_and_store(knobs).0
}

fn client_and_store(knobs: Knobs) -> (Client<MemoryTransport, ManualClock>, Made) {
    let mut offer = Offer::new("a fake store", "0.1", Uuid([2; 16]));
    offer.data_objects = ["resqml20.*", "eml20.*"]
        .iter()
        .map(|t| SupportedDataObject {
            qualified_type: t.to_string(),
            data_object_capabilities: BTreeMap::new(),
        })
        .collect();
    let made: Made = std::sync::Arc::new(std::sync::Mutex::new(
        [(DEFAULT.to_string(), BTreeMap::new())].into_iter().collect(),
    ));
    let open = std::sync::Arc::new(std::sync::Mutex::new(Open::default()));
    let arrays = std::sync::Arc::new(std::sync::Mutex::new(BTreeMap::new()));
    let handlers: Vec<Box<dyn Handler>> = [3, 4, 9, 18, 24, 25]
        .iter()
        .map(|p| {
            Box::new(Store {
                protocol: *p,
                knobs,
                made: std::sync::Arc::clone(&made),
                open: std::sync::Arc::clone(&open),
                arrays: std::sync::Arc::clone(&arrays),
            }) as Box<dyn Handler>
        })
        .collect();
    let server = Connection::server(offer, handlers, None, Uuid([0xAB; 16]), T0).unwrap();
    let clock = ManualClock::new(T0);
    let (offer, handlers) = writing_client(
        "the suite's tests",
        "0.1",
        Uuid([1; 16]),
        &["resqml20.*".to_string(), "eml20.*".to_string()],
    );
    (
        Client::open(
            MemoryTransport::new(server, clock.clone()),
            clock,
            offer,
            handlers,
            None,
            Settings::default(),
        )
        .unwrap(),
        made,
    )
}

fn report(knobs: Knobs, write: bool) -> Report {
    let mut client = client(knobs);
    let options = Options {
        write,
        ..Default::default()
    };
    let report = run(&mut client, &options, "memory://fake");
    client.close("done").unwrap();
    report
}

fn verdict(report: &Report, id: &str) -> Verdict {
    report
        .findings
        .iter()
        .find(|one| one.id == id)
        .unwrap_or_else(|| panic!("no finding {id}: {report}"))
        .verdict
}

/// A store that follows the rules gets no failure - and every check that
/// could observe something did, which is the half a green report owes.
#[test]
fn a_conformant_store_passes_everything_it_announces() {
    let report = report(Knobs::default(), true);
    assert!(!report.has_failures(), "{report}");
    for id in [
        "core.open-session.session-id",
        "core.ping-pong",
        "core.acknowledge",
        "dataspace.list",
        "dataspace.timestamps",
        "discovery.dataspace",
        "discovery.invalid-uri",
        "discovery.not-found",
        "discovery.timestamps",
        "store.get",
        "store.get.not-found",
        "supported-types.list",
        "data-array.metadata",
        "data-array.not-found",
        "data-array.read",
        "data-array.subarray",
        "discovery.deleted",
        "discovery.neighbours",
        "discovery.null-filter",
        "store.get.several",
        "transaction.start-rollback",
        "transaction.second-refused",
        "transaction.rollback-leaves-nothing",
        "transaction.commit-keeps",
        "wire.response-time",
        "write.dataspace.put",
        "write.object.put",
        "write.object.read-back",
        "write.object.timestamps",
        "write.object.delete",
        "write.object.gone",
        "write.dataspace.delete",
        "write.dataspace.gone",
        "wire.message-ids",
        "wire.correlation",
        "wire.fin",
    ] {
        assert_eq!(verdict(&report, id), Verdict::Pass, "{id}: {report}");
    }
}

#[test]
fn a_second_transaction_opened_is_a_failure() {
    let report = report(
        Knobs {
            unlimited_transactions: true,
            ..Default::default()
        },
        false,
    );
    assert_eq!(
        verdict(&report, "transaction.second-refused"),
        Verdict::Fail,
        "{report}"
    );
}

#[test]
fn a_rollback_that_keeps_what_was_put_is_a_failure() {
    let report = report(
        Knobs {
            ignores_rollback: true,
            ..Default::default()
        },
        true,
    );
    assert_eq!(
        verdict(&report, "transaction.rollback-leaves-nothing"),
        Verdict::Fail,
        "{report}"
    );
    // The commit path is untouched by that knob.
    assert_eq!(
        verdict(&report, "transaction.commit-keeps"),
        Verdict::Pass,
        "{report}"
    );
}

#[test]
fn an_array_shorter_than_its_metadata_is_a_failure() {
    let report = report(
        Knobs {
            short_array: true,
            ..Default::default()
        },
        false,
    );
    assert_eq!(verdict(&report, "data-array.read"), Verdict::Fail, "{report}");
}

/// Without `--write`, nothing is written and the report says so.
#[test]
fn without_write_the_writing_checks_are_not_checked_and_nothing_is_written() {
    let report = report(Knobs::default(), false);
    assert!(!report.has_failures(), "{report}");
    assert_eq!(verdict(&report, "write.*"), Verdict::NotChecked);
    assert!(report.findings.iter().all(|one| one.id != "write.dataspace.put"));
}

#[test]
fn a_resource_with_a_uri_that_is_not_canonical_is_a_failure() {
    let report = report(
        Knobs {
            misspelt_uris: true,
            ..Default::default()
        },
        false,
    );
    assert_eq!(verdict(&report, "discovery.dataspace"), Verdict::Fail, "{report}");
}

#[test]
fn a_malformed_uri_answered_with_resources_is_a_failure() {
    let report = report(
        Knobs {
            lenient_uris: true,
            ..Default::default()
        },
        false,
    );
    assert_eq!(
        verdict(&report, "discovery.invalid-uri"),
        Verdict::Fail,
        "{report}"
    );
}

#[test]
fn an_absent_object_left_out_of_the_map_is_a_failure() {
    let report = report(
        Knobs {
            forgets_absent: true,
            ..Default::default()
        },
        false,
    );
    assert_eq!(verdict(&report, "store.get.not-found"), Verdict::Fail, "{report}");
}

#[test]
fn a_store_whose_clock_runs_backwards_is_a_failure() {
    let report = report(
        Knobs {
            clock_backwards: true,
            ..Default::default()
        },
        true,
    );
    assert_eq!(
        verdict(&report, "dataspace.timestamps"),
        Verdict::Fail,
        "{report}"
    );
    assert_eq!(
        verdict(&report, "discovery.timestamps"),
        Verdict::Fail,
        "{report}"
    );
    assert_eq!(
        verdict(&report, "write.object.timestamps"),
        Verdict::Fail,
        "{report}"
    );
}

#[test]
fn a_deleted_dataspace_still_listed_is_a_failure() {
    let report = report(
        Knobs {
            keeps_deleted: true,
            ..Default::default()
        },
        true,
    );
    assert_eq!(
        verdict(&report, "write.dataspace.gone"),
        Verdict::Fail,
        "{report}"
    );
}

/// The faults a client never sends, against this crate's own server side -
/// `retp-proto`'s, which the wire's tests already hold to these rules; what
/// is checked here is that the suite sends them and reads the answers.
#[test]
fn the_faults_draw_their_exceptions() {
    let mut offer = Offer::new("a fake store", "0.1", Uuid([2; 16]));
    offer.data_objects = vec![SupportedDataObject {
        qualified_type: "resqml20.*".into(),
        data_object_capabilities: BTreeMap::new(),
    }];
    let made = std::sync::Arc::new(std::sync::Mutex::new(BTreeMap::new()));
    let open = std::sync::Arc::new(std::sync::Mutex::new(Open::default()));
    let handlers: Vec<Box<dyn Handler>> = [3, 24]
        .iter()
        .map(|p| {
            Box::new(Store {
                protocol: *p,
                knobs: Knobs::default(),
                made: std::sync::Arc::clone(&made),
                open: std::sync::Arc::clone(&open),
                arrays: std::sync::Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            }) as Box<dyn Handler>
        })
        .collect();
    let server = Connection::server(offer, handlers, None, Uuid([0xAB; 16]), T0).unwrap();
    let clock = ManualClock::new(T0);
    let mut ours = Offer::new("the suite's tests", "0.1", Uuid([1; 16]));
    ours.data_objects = vec![SupportedDataObject {
        qualified_type: "resqml20.*".into(),
        data_object_capabilities: BTreeMap::new(),
    }];
    let findings =
        retp_conformance::raw::checks(MemoryTransport::new(server, clock.clone()), clock, ours, None);
    let report = Report {
        server: "fake".into(),
        url: "memory://fake".into(),
        protocols: vec![],
        findings,
        ..Default::default()
    };
    for id in [
        "raw.unsupported-protocol",
        "raw.unsupported-protocol.store",
        "raw.unsupported-protocol.data-array",
        "raw.unsupported-protocol.transaction",
        "raw.unsupported-protocol.supported-types",
        "raw.unsupported-protocol.put-dataspaces",
        "raw.unsupported-protocol.put-data-objects",
        "raw.invalid-message-type",
        "raw.invalid-message",
    ] {
        assert_eq!(verdict(&report, id), Verdict::Pass, "{id}: {report}");
    }
}

/// The report can be rendered either way from a real run.
#[test]
fn the_report_renders() {
    let report = report(Knobs::default(), false);
    let text = report.markdown();
    assert!(
        text.contains("# ETP v1.2 conformance - a fake store 0.1"),
        "{text}"
    );
    assert!(text.contains("`wire.message-ids`"));
    assert_eq!(report.json()["server"], "a fake store 0.1");
    // Who wrote it, and that a failure may be the tool's: at the head of both.
    let head = &text[..text.find("Checked:").unwrap()];
    for said in [
        "checks an ETP v1.2 server",
        "may come from this tool's own implementation",
    ] {
        assert!(head.contains(said), "{said}: {head}");
        assert!(report.json()["about"].as_str().unwrap().contains(said), "{said}");
    }
}

/// The samples the suite writes are documents this library reads strictly:
/// a store that refuses one refuses a schema-valid object, which is then the
/// finding - never a defect of the suite's own XML.
#[test]
fn the_samples_are_valid_documents() {
    let registry = energyml_schemas::all();
    let samples = retp_conformance::samples::samples("eml:///dataspace('example/test')", 0x1234);
    assert!(samples.len() >= 10, "{}", samples.len());
    for sample in &samples {
        energyml_etp_store::read_object(
            &registry,
            energyml_core::registry::ReadMode::Strict,
            &sample.uri,
            &sample.object.data,
        )
        .unwrap_or_else(|why| panic!("{}: {why:?}", sample.qualified_type));
        let uuid = energyml_core::identifiers::EtpUri::parse(&sample.uri)
            .unwrap()
            .uuid
            .unwrap();
        assert!(uuid.starts_with(retp_conformance::samples::PREFIX), "{uuid}");
    }
    // Two of them carry an array, of both releases' kinds, each on the
    // object that references it - never on a proxy (an `ExternalPartReference`
    // is not supposed to carry arrays).
    let arrays: Vec<_> = samples
        .iter()
        .filter_map(|s| s.array.as_ref().map(|(id, _)| (s, id)))
        .collect();
    assert_eq!(arrays.len(), 2);
    for (sample, id) in &arrays {
        assert_eq!(id.uri, sample.uri, "{}", sample.qualified_type);
    }
    // One goes whole, the other in blocks.
    assert_eq!(samples.iter().filter(|s| s.in_blocks).count(), 1);
    assert!(samples.iter().filter(|s| s.in_blocks).all(|s| s.array.is_some()));
    // And PRODML is among the families sent.
    assert!(samples.iter().any(|s| s.qualified_type.starts_with("prodml23.")));
}

/// A store that follows the rules on writes: the samples go in, their arrays
/// read back equal, they are listed, deleted, and the dataspace goes.
#[test]
fn the_samples_go_in_and_come_out() {
    let report = report(Knobs::default(), true);
    for id in [
        "write.samples.announced",
        "write.samples.listed",
        "write.samples.delete",
        "write.array.put",
        "write.array.read-back",
        "write.array.blocks",
        "write.array.metadata",
        "write.array.subarray",
        "write.dataspace.delete",
    ] {
        assert_eq!(verdict(&report, id), Verdict::Pass, "{id}: {report}");
    }
    // This store takes what it did not announce: said, not failed.
    assert_eq!(
        verdict(&report, "write.samples.unannounced"),
        Verdict::Tolerated,
        "{report}"
    );
    assert!(
        report
            .uploads
            .iter()
            .all(|one| one.put == "accepted" && one.deleted == "deleted"),
        "{:?}",
        report.uploads
    );
    let text = report.markdown();
    assert!(text.contains("## Objects the suite wrote"), "{text}");
    assert!(text.contains("| Write arrays | **yes** |"), "{text}");
}

/// **A dataspace that exists is never written into**: the name given is
/// listed, and nothing is put, nothing deleted.
#[test]
fn a_dataspace_that_exists_is_never_written_into() {
    let (mut client, made) = client_and_store(Knobs::default());
    let options = Options {
        write: true,
        write_dataspace: Some("demo".into()),
        ..Default::default()
    };
    let report = run(&mut client, &options, "memory://fake");
    client.close("done").unwrap();
    assert_eq!(
        verdict(&report, "write.dataspace.put"),
        Verdict::NotChecked,
        "{report}"
    );
    assert!(
        report
            .requests
            .iter()
            .all(|one| !one.request.starts_with("Put") && !one.request.starts_with("Delete")),
        "{:?}",
        report.requests
    );
    // Nothing anywhere: no dataspace made, nothing in the default one.
    assert!(
        made.lock()
            .unwrap()
            .iter()
            .all(|(space, held)| space == DEFAULT && held.is_empty())
    );
    assert!(
        report.notes.iter().any(|note| note.contains("already exists")),
        "{:?}",
        report.notes
    );
}

/// **An object the suite did not write is never deleted**, and neither is
/// the dataspace holding it - even the suite's own dataspace.
#[test]
fn an_object_the_suite_did_not_write_is_never_deleted() {
    let (mut client, made) = client_and_store(Knobs {
        foreign_in_new_space: true,
        ..Default::default()
    });
    let options = Options {
        write: true,
        write_dataspace: Some("example/test".into()),
        ..Default::default()
    };
    let report = run(&mut client, &options, "memory://fake");
    client.close("done").unwrap();
    let held = made.lock().unwrap();
    let space = "eml:///dataspace('example/test')";
    let objects = held
        .get(space)
        .unwrap_or_else(|| panic!("the dataspace was deleted: {report}"));
    assert!(
        objects.contains_key(&format!("{space}/{FOREIGN}")),
        "the foreign object was deleted: {report}"
    );
    // Nothing but the suite's own is gone: what is left is the foreign
    // object, and what the suite kept because it appeared beside it.
    assert!(
        objects
            .keys()
            .all(|uri| uri.ends_with(FOREIGN) || uri.contains("0f0e0d0c-0b0a-4908-8706")),
        "{:?}",
        objects.keys().collect::<Vec<_>>()
    );
    assert_eq!(
        verdict(&report, "write.dataspace.delete"),
        Verdict::NotChecked,
        "{report}"
    );
    // It appeared while the suite wrote: the first object is kept, and said so.
    assert_eq!(
        verdict(&report, "write.object.delete"),
        Verdict::NotChecked,
        "{report}"
    );
}

/// An array that cannot be served in the first dataspace is looked for in
/// the next one, and the report says where it was read.
#[test]
fn an_array_is_looked_for_in_the_next_dataspace() {
    let report = report(
        Knobs {
            broken_demo_arrays: true,
            ..Default::default()
        },
        false,
    );
    assert_eq!(verdict(&report, "data-array.read"), Verdict::Pass, "{report}");
    let tried = report
        .findings
        .iter()
        .find(|one| one.id == "data-array.dataspaces")
        .unwrap();
    assert_eq!(tried.verdict, Verdict::Pass, "{report}");
    assert!(
        tried.said.contains("demo") && tried.said.contains("other"),
        "{}",
        tried.said
    );
}

/// The head of the report: what is supported, and what each request cost.
#[test]
fn the_report_sums_up_support_and_response_times() {
    let report = report(Knobs::default(), false);
    let text = report.markdown();
    assert!(text.contains("## What this server supports"), "{text}");
    assert!(text.contains("| List dataspaces | **yes** |"), "{text}");
    assert!(text.contains("| Create a dataspace | **not tested** |"), "{text}");
    assert!(text.contains("## Requests sent and response times"), "{text}");
    let get = report
        .requests
        .iter()
        .find(|one| one.request == "GetDataObjects")
        .expect("GetDataObjects was sent");
    assert!(
        get.count >= 2 && get.answered + get.refused + get.failed == get.count,
        "{get:?}"
    );
    assert!(
        report.json()["requests"]
            .as_array()
            .is_some_and(|all| !all.is_empty())
    );
}

/// **A dataspace the suite may not create**: with a fallback named, the
/// samples go into that existing dataspace, only the suite's own objects are
/// deleted there - another client's object stays - and the dataspace is
/// never deleted.
#[test]
fn a_fallback_dataspace_keeps_everything_but_the_suites_own_objects() {
    let (mut client, made) = client_and_store(Knobs {
        refuses_dataspaces: true,
        ..Default::default()
    });
    let theirs = format!("{SPACE}/{FOREIGN}");
    made.lock().unwrap().entry(SPACE.to_string()).or_default().insert(
        theirs.clone(),
        DataObject {
            resource: Resource {
                uri: theirs.clone(),
                ..Default::default()
            },
            format: "xml".into(),
            ..Default::default()
        },
    );
    let options = Options {
        write: true,
        write_fallback: Some("demo".into()),
        ..Default::default()
    };
    let report = run(&mut client, &options, "memory://fake");
    client.close("done").unwrap();
    assert_eq!(verdict(&report, "write.dataspace.put"), Verdict::Fail, "{report}");
    assert_eq!(
        verdict(&report, "write.samples.delete"),
        Verdict::Pass,
        "{report}"
    );
    assert!(
        report.uploads.iter().all(|one| one.uri.starts_with(SPACE)),
        "{:?}",
        report.uploads
    );
    let held = made.lock().unwrap();
    let left: Vec<&String> = held
        .get(SPACE)
        .map(|objects| objects.keys().collect())
        .unwrap_or_default();
    assert_eq!(left, vec![&theirs], "only the other client's object stays");
    assert!(
        report
            .requests
            .iter()
            .all(|one| one.request != "DeleteDataspaces"),
        "{:?}",
        report.requests
    );
    assert!(
        report
            .notes
            .iter()
            .any(|note| note.contains("--write-fallback-dataspace")),
        "{:?}",
        report.notes
    );
}

/// **A store that adds objects of its own while the suite writes** (some stores'
/// activities): nothing is deleted - not the suite's objects, which they
/// reference, nor the dataspace - and the report says why.
#[test]
fn objects_the_store_made_keep_everything_and_the_report_says_why() {
    let (mut client, made) = client_and_store(Knobs {
        records_activities: true,
        ..Default::default()
    });
    let options = Options {
        write: true,
        write_dataspace: Some("example/test".into()),
        ..Default::default()
    };
    let report = run(&mut client, &options, "memory://fake");
    client.close("done").unwrap();
    assert!(
        report
            .requests
            .iter()
            .all(|one| one.request != "DeleteDataObjects" && one.request != "DeleteDataspaces"),
        "{:?}",
        report.requests
    );
    assert_eq!(
        verdict(&report, "write.samples.delete"),
        Verdict::NotChecked,
        "{report}"
    );
    assert_eq!(
        verdict(&report, "write.object.delete"),
        Verdict::NotChecked,
        "{report}"
    );
    let why = report
        .findings
        .iter()
        .find(|one| one.id == "write.samples.delete")
        .unwrap();
    assert!(
        why.said.contains("resqml20.obj_Activity") && why.said.contains("nothing was deleted"),
        "{}",
        why.said
    );
    assert!(
        report
            .uploads
            .iter()
            .filter(|one| one.put == "accepted")
            .all(|one| one.deleted.starts_with("kept:")),
        "{:?}",
        report.uploads
    );
    assert!(
        report.notes.iter().any(|note| note.contains("store added")),
        "{:?}",
        report.notes
    );
    // Everything is still there.
    let held = made.lock().unwrap();
    let space = held
        .get("eml:///dataspace('example/test')")
        .expect("the dataspace stays");
    assert!(space.keys().any(|uri| uri.contains("obj_Activity")));
    assert!(
        space
            .keys()
            .filter(|uri| uri.contains("0f0e0d0c-0b0a-4908-8706"))
            .count()
            >= 10,
        "{:?}",
        space.keys().collect::<Vec<_>>()
    );
}

/// A store that reads every sub-array from the first index answers the
/// right count of the wrong values: both sub-array checks say so - on the
/// array it held, against the whole of it, and on the one the suite wrote.
#[test]
fn a_subarray_read_from_the_wrong_place_is_a_failure() {
    let report = report(
        Knobs {
            shifted_subarrays: true,
            ..Default::default()
        },
        true,
    );
    assert_eq!(verdict(&report, "data-array.subarray"), Verdict::Fail, "{report}");
    assert_eq!(
        verdict(&report, "write.array.subarray"),
        Verdict::Fail,
        "{report}"
    );
    // The whole reads are not touched.
    assert_eq!(verdict(&report, "data-array.read"), Verdict::Pass, "{report}");
    assert_eq!(
        verdict(&report, "write.array.metadata"),
        Verdict::Pass,
        "{report}"
    );
}

/// A block answered without its key, and not written: the array declared
/// and filled block by block is a failure, and the whole one is not.
#[test]
fn a_block_answered_without_its_key_is_a_failure() {
    let report = report(
        Knobs {
            forgets_block_keys: true,
            ..Default::default()
        },
        true,
    );
    assert_eq!(verdict(&report, "write.array.blocks"), Verdict::Fail, "{report}");
    let blocks = report
        .findings
        .iter()
        .find(|one| one.id == "write.array.blocks")
        .unwrap();
    assert!(
        blocks.said.contains("neither accepted nor refused"),
        "{}",
        blocks.said
    );
    assert_eq!(verdict(&report, "write.array.put"), Verdict::Pass, "{report}");
}

/// Metadata that does not state the dimensions an array was written with.
#[test]
fn metadata_other_than_what_was_written_is_a_failure() {
    let report = report(
        Knobs {
            transposed_metadata: true,
            ..Default::default()
        },
        true,
    );
    assert_eq!(
        verdict(&report, "write.array.metadata"),
        Verdict::Fail,
        "{report}"
    );
}

/// Doubles kept as 32-bit floats, as some servers keep them: the round trip
/// fails and names the cause; the blocks and the sub-array, which land and
/// come back where they were sent, are not failed a second time for it.
#[test]
fn doubles_kept_as_floats_fail_the_round_trip_and_say_why() {
    let report = report(
        Knobs {
            stores_float32: true,
            ..Default::default()
        },
        true,
    );
    assert_eq!(
        verdict(&report, "write.array.read-back"),
        Verdict::Fail,
        "{report}"
    );
    let put = report
        .findings
        .iter()
        .find(|one| one.id == "write.array.put")
        .unwrap();
    assert!(put.said.contains("rounded to 32-bit floats"), "{}", put.said);
    for id in [
        "write.array.blocks",
        "write.array.subarray",
        "write.array.metadata",
    ] {
        assert_eq!(verdict(&report, id), Verdict::Pass, "{id}: {report}");
    }
    let metadata = report
        .findings
        .iter()
        .find(|one| one.id == "write.array.metadata")
        .unwrap();
    assert!(
        metadata.said.contains("written as ArrayOfDouble64Le"),
        "{}",
        metadata.said
    );
}

/// A store that lists no dataspace keeps its objects in the default one,
/// which every store MUST support (§21.2.2 row 1, §8.2.2): the suite reads
/// there instead of stopping at « no dataspace to look in ».
#[test]
fn a_store_without_dataspaces_is_read_in_the_default_one() {
    let report = report(
        Knobs {
            no_dataspaces: true,
            ..Default::default()
        },
        false,
    );
    for id in ["discovery.dataspace", "store.get", "data-array.read"] {
        assert_eq!(verdict(&report, id), Verdict::Pass, "{id}: {report}");
    }
    let discovery = report
        .findings
        .iter()
        .find(|one| one.id == "discovery.dataspace")
        .unwrap();
    assert!(discovery.said.starts_with("eml:///:"), "{}", discovery.said);
}

/// A dataspace the suite cannot create: its samples go into the first
/// listed dataspace whose name says "test", and only they are deleted - the
/// dataspace stays.
#[test]
fn without_a_dataspace_of_its_own_the_suite_writes_in_a_test_one() {
    let (mut client, made) = client_and_store(Knobs {
        refuses_dataspaces: true,
        ..Default::default()
    });
    let test = "eml:///dataspace('team/tests')";
    made.lock().unwrap().insert(test.to_string(), BTreeMap::new());
    let options = Options {
        write: true,
        ..Default::default()
    };
    let report = run(&mut client, &options, "memory://fake");
    client.close("done").unwrap();
    assert!(
        !report.uploads.is_empty() && report.uploads.iter().all(|one| one.uri.starts_with(test)),
        "{:?}",
        report.uploads
    );
    assert_eq!(
        verdict(&report, "write.samples.delete"),
        Verdict::Pass,
        "{report}"
    );
    let held = made.lock().unwrap();
    assert!(
        held.get(test).is_some_and(|objects| objects.is_empty()),
        "the dataspace stays, emptied of the suite's objects"
    );
    assert!(
        report
            .requests
            .iter()
            .all(|one| one.request != "DeleteDataspaces"),
        "{:?}",
        report.requests
    );
}

/// And with no dataspace saying "test", the default one, `eml:///`: the
/// samples go there, without a dataspace segment, and only they are deleted.
#[test]
fn without_a_test_dataspace_the_suite_writes_in_the_default_one() {
    let (mut client, made) = client_and_store(Knobs {
        refuses_dataspaces: true,
        ..Default::default()
    });
    let theirs =
        "eml:///resqml20.obj_GeneticBoundaryFeature(dd000000-0000-4000-8000-000000000009)".to_string();
    made.lock().unwrap().get_mut(DEFAULT).unwrap().insert(
        theirs.clone(),
        DataObject {
            resource: Resource {
                uri: theirs.clone(),
                ..Default::default()
            },
            format: "xml".into(),
            ..Default::default()
        },
    );
    let options = Options {
        write: true,
        ..Default::default()
    };
    let report = run(&mut client, &options, "memory://fake");
    client.close("done").unwrap();
    assert!(
        !report.uploads.is_empty()
            && report.uploads.iter().all(|one| one.uri.starts_with("eml:///")
                && !one.uri.starts_with("eml:///dataspace(")
                && !one.uri.starts_with("eml:////")),
        "{:?}",
        report.uploads
    );
    assert_eq!(
        verdict(&report, "write.samples.delete"),
        Verdict::Pass,
        "{report}"
    );
    let held = made.lock().unwrap();
    let left: Vec<&String> = held
        .get(DEFAULT)
        .map(|objects| objects.keys().collect())
        .unwrap_or_default();
    assert_eq!(left, vec![&theirs], "only the other client's object stays");
    assert!(
        report
            .requests
            .iter()
            .all(|one| one.request != "DeleteDataspaces"),
        "{:?}",
        report.requests
    );
}

/// The reads on what the store holds go red one by one: the tombstones
/// refused, a key of several left out, an object's graph refused.
#[test]
fn the_reads_on_what_the_store_holds_say_each_refusal() {
    for (knobs, id) in [
        (
            Knobs {
                refuses_deleted: true,
                ..Default::default()
            },
            "discovery.deleted",
        ),
        (
            Knobs {
                drops_one_of_several: true,
                ..Default::default()
            },
            "store.get.several",
        ),
        (
            Knobs {
                refuses_object_graph: true,
                ..Default::default()
            },
            "discovery.neighbours",
        ),
    ] {
        let report = report(knobs, false);
        assert_eq!(verdict(&report, id), Verdict::Fail, "{id}: {report}");
    }
}

/// **A request that times out opens the session again** for the checks
/// that follow (when a command times out, cut and open
/// the connection again for the following tests): the check that sent it
/// fails, the report says the session was opened again, and the next
/// checks run on the new one.
#[test]
fn a_request_that_times_out_opens_the_session_again() {
    let knobs = Knobs {
        silent_supported_types: true,
        ..Default::default()
    };
    let mut session = retp_conformance::renewing::Renewing::new(
        client(knobs),
        retp_client::native::Reopen(|| Ok::<_, retp_client::ClientError>(client(knobs))),
    );
    let report = run(&mut session, &Options::default(), "memory://fake");
    session.into_inner().close("done").unwrap();
    assert_eq!(
        verdict(&report, "supported-types.list"),
        Verdict::Fail,
        "{report}"
    );
    assert!(
        report.notes.iter().any(|note| note.contains("opened again")),
        "{:?}",
        report.notes
    );
    // The checks after it, on the new session, still observe.
    assert_eq!(
        verdict(&report, "transaction.start-rollback"),
        Verdict::Pass,
        "{report}"
    );
    // And every exchange is in the appendix, readable: the URI a request
    // carried, the answer's header.
    let text = report.markdown();
    assert!(text.contains("## Requests sent and messages received"), "{text}");
    assert!(
        report
            .exchanges
            .iter()
            .any(|one| one.request.ends_with("GetResources")
                && one.sent.contains("eml:///dataspace('demo')")
                && one.sent.contains("\"context\"")),
        "{:?}",
        report.exchanges.first()
    );
    assert!(
        report
            .exchanges
            .iter()
            .any(|one| one.received.iter().any(|r| r.contains("correlationId")))
    );
    assert!(
        report
            .exchanges
            .iter()
            .any(|one| one.outcome.starts_with("not answered")),
        "the timed-out request is there too"
    );
    // A new session starts its message ids over: not a fault of the store.
    assert_eq!(verdict(&report, "wire.message-ids"), Verdict::Pass, "{report}");
}

/// **A store that cannot read activeStatusFilter null** - which the schema
/// allows - is asked again with the filter set: the report says so, and the
/// checks go on with the listing that came back.
#[test]
fn a_store_that_cannot_read_a_null_filter_is_said_and_read_all_the_same() {
    let report = report(
        Knobs {
            cannot_read_null_filter: true,
            ..Default::default()
        },
        false,
    );
    assert_eq!(
        verdict(&report, "discovery.null-filter"),
        Verdict::Fail,
        "{report}"
    );
    let said = &report
        .findings
        .iter()
        .find(|one| one.id == "discovery.null-filter")
        .unwrap()
        .said;
    assert!(said.contains("answered with Active"), "{said}");
    for id in ["discovery.dataspace", "store.get"] {
        assert_eq!(verdict(&report, id), Verdict::Pass, "{id}: {report}");
    }
}

/// The appendix shows a long array as its first values and its count, and a
/// long string cut; the rest as it is.
#[test]
fn the_appendix_shortens_long_arrays_and_strings() {
    use retp_conformance::exchanges::shortened;
    let long: Vec<serde_json::Value> = (0..40).map(|n| serde_json::json!(n)).collect();
    let short = shortened(serde_json::json!({ "values": long, "uri": "eml:///x", "dimensions": [16, 3] }));
    let values = short["values"].as_array().unwrap();
    assert_eq!(values.len(), 9);
    assert_eq!(values[8], "… (40 values)");
    assert_eq!(short["uri"], "eml:///x");
    assert_eq!(short["dimensions"], serde_json::json!([16, 3]));
    let text = "x".repeat(700);
    assert!(
        shortened(serde_json::json!(text))
            .as_str()
            .unwrap()
            .ends_with("… (700 characters)")
    );
}

/// A listing refused **for its size** is not a store that cannot read the
/// null: `discovery.null-filter` is not failed for it, and the filtered
/// listing is still read.
#[test]
fn a_listing_refused_for_its_size_is_not_a_null_problem() {
    let report = report(
        Knobs {
            refuses_for_size: true,
            ..Default::default()
        },
        false,
    );
    assert_eq!(
        verdict(&report, "discovery.null-filter"),
        Verdict::NotChecked,
        "{report}"
    );
    assert_eq!(verdict(&report, "store.get"), Verdict::Pass, "{report}");
}
