//! The bridge's fake server and the store over it - shared by the tests
//! that read from it (`etp_bridge.rs`) and the one that pushes what it holds
//! somewhere else (`push.rs`).
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;

use energyml_core::registry::Registry;
use energyml_etp_store::EtpStore;
use retp_avro::Role;
use retp_client::memory::{ManualClock, MemoryTransport};
use retp_client::{Client, Settings, reading_client};
use retp_proto::connection::{Connection, Declined, Handled, Handler, Responder};
use retp_proto::negotiate::{Offer, ProtocolOffer};
use retp_proto::wire::{Body, Delivery, EtpError};
use retp_types::datatypes::data_array_types::{DataArray, DataArrayMetadata};
use retp_types::datatypes::object::{DataObject, Dataspace as EtpDataspace, Resource};
use retp_types::datatypes::{
    AnyArray, AnyArrayItem, AnyArrayType, AnyLogicalArrayType, ArrayOfDouble, SupportedDataObject, Uuid,
};
use retp_types::dispatch::Message;
use retp_types::protocol::data_array::{GetDataArrayMetadataResponse, GetDataArraysResponse};
use retp_types::protocol::dataspace::GetDataspacesResponse;
use retp_types::protocol::discovery::GetResourcesResponse;
use retp_types::protocol::store::GetDataObjectsResponse;

pub const S: i64 = 1_000_000;
pub const T0: i64 = 1_760_000_000 * S;

pub const CRS: &str = "aa000000-0000-4000-8000-000000000001";
pub const POINTS: &str = "bb000000-0000-4000-8000-000000000002";
pub const PROXY: &str = "cc000000-0000-4000-8000-000000000003";
pub const DATASPACE: &str = "eml:///dataspace('demo')";

pub fn citation(title: &str) -> String {
    format!(
        "<eml:Citation><eml:Title>{title}</eml:Title><eml:Originator>test</eml:Originator>\
<eml:Creation>2026-08-02T00:00:00Z</eml:Creation><eml:Format>test</eml:Format></eml:Citation>"
    )
}

/// The model the server holds: (qualified type, uuid, title, XML).
pub fn model() -> Vec<(&'static str, &'static str, &'static str, String)> {
    let crs = format!(
        "<resqml:obj_LocalDepth3dCrs xmlns:resqml=\"http://www.energistics.org/energyml/data/resqmlv2\" \
xmlns:eml=\"http://www.energistics.org/energyml/data/commonv2\" uuid=\"{CRS}\" schemaVersion=\"2.0\">{}\
<resqml:XOffset>1000</resqml:XOffset><resqml:YOffset>2000</resqml:YOffset><resqml:ZOffset>0</resqml:ZOffset>\
<resqml:ArealRotation uom=\"rad\">0</resqml:ArealRotation>\
<resqml:ProjectedAxisOrder>easting northing</resqml:ProjectedAxisOrder>\
<resqml:ProjectedUom>m</resqml:ProjectedUom><resqml:VerticalUom>m</resqml:VerticalUom>\
<resqml:ZIncreasingDownward>true</resqml:ZIncreasingDownward>\
<resqml:VerticalCrs xsi:type=\"eml:VerticalUnknownCrs\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\
<eml:Unknown>none</eml:Unknown></resqml:VerticalCrs>\
<resqml:ProjectedCrs xsi:type=\"eml:ProjectedCrsEpsgCode\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\
<eml:EpsgCode>32631</eml:EpsgCode></resqml:ProjectedCrs></resqml:obj_LocalDepth3dCrs>",
        citation("a local depth CRS")
    );
    let points = format!(
        "<resqml:obj_PointSetRepresentation xmlns:resqml=\"http://www.energistics.org/energyml/data/resqmlv2\" \
xmlns:eml=\"http://www.energistics.org/energyml/data/commonv2\" uuid=\"{POINTS}\" schemaVersion=\"2.0\">{}\
<resqml:NodePatch><resqml:PatchIndex>0</resqml:PatchIndex><resqml:Count>2</resqml:Count><resqml:Geometry>\
<resqml:LocalCrs><eml:ContentType>application/x-resqml+xml;version=2.0;type=obj_LocalDepth3dCrs</eml:ContentType>\
<eml:Title>a local depth CRS</eml:Title><eml:UUID>{CRS}</eml:UUID></resqml:LocalCrs>\
<resqml:Points xsi:type=\"resqml:Point3dHdf5Array\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\
<resqml:Coordinates><eml:PathInHdfFile>/points</eml:PathInHdfFile>\
<eml:HdfProxy><eml:ContentType>application/x-eml+xml;version=2.0;type=obj_EpcExternalPartReference</eml:ContentType>\
<eml:Title>proxy</eml:Title><eml:UUID>{PROXY}</eml:UUID></eml:HdfProxy></resqml:Coordinates>\
</resqml:Points></resqml:Geometry></resqml:NodePatch></resqml:obj_PointSetRepresentation>",
        citation("a point set")
    );
    let proxy = format!(
        "<eml:EpcExternalPartReference xmlns:eml=\"http://www.energistics.org/energyml/data/commonv2\" \
uuid=\"{PROXY}\" schemaVersion=\"2.0\">{}<eml:MimeType>application/x-hdf5</eml:MimeType></eml:EpcExternalPartReference>",
        citation("the HDF5 proxy")
    );
    vec![
        ("resqml20.obj_LocalDepth3dCrs", CRS, "a local depth CRS", crs),
        (
            "resqml20.obj_PointSetRepresentation",
            POINTS,
            "a point set",
            points,
        ),
        (
            "eml20.obj_EpcExternalPartReference",
            PROXY,
            "the HDF5 proxy",
            proxy,
        ),
    ]
}

/// How the server spells the types in its URIs.
#[derive(Clone, Copy, PartialEq)]
pub enum Spelling {
    Energistics,
    /// As some RDDMS servers do: the class name, `resqml20.ObjLocalDepth3dCrs`.
    ClassName,
}

pub fn object_uri(qualified_type: &str, uuid: &str, spelling: Spelling) -> String {
    let spelled = match spelling {
        Spelling::Energistics => qualified_type.to_string(),
        Spelling::ClassName => qualified_type.replace(".obj_", ".Obj"),
    };
    format!("{DATASPACE}/{spelled}({uuid})")
}

/// A server handler serving [`model`], one protocol per instance.
pub struct Served {
    pub protocol: i32,
    pub spelling: Spelling,
    /// A uuid left out of the listing while its object is still served: what
    /// makes a reference **leave the source** - the case a push must name.
    pub hide: Option<&'static str>,
    /// Also serve [`KIND`], an eml 2.3 `PropertyKind` of the source whose
    /// parent is the dictionary's root - a reference leaving the source
    /// **towards the official dictionary**, the case a push may resolve.
    pub with_kind: bool,
}

/// The uuid of a `PropertyKind` the source holds.
pub const KIND: &str = "dd000000-0000-4000-8000-000000000004";
/// The dictionary's root kind, *property* - every kind's ancestor.
pub const DICTIONARY_ROOT: &str = "a48c9c25-1e3a-43c8-be6a-044224cc69cb";

/// The source's own kind: parented on the dictionary's root, which the source
/// does not hold.
pub fn kind_xml() -> String {
    format!(
        "<eml:PropertyKind xmlns:eml=\"http://www.energistics.org/energyml/data/commonv2\" schemaVersion=\"2.3\" uuid=\"{KIND}\">\
<eml:Citation><eml:Title>a kind of ours</eml:Title><eml:Originator>test</eml:Originator>\
<eml:Creation>2026-08-02T00:00:00Z</eml:Creation><eml:Format>test</eml:Format></eml:Citation>\
<eml:IsAbstract>false</eml:IsAbstract><eml:QuantityClass>unitless</eml:QuantityClass>\
<eml:Parent><eml:Uuid>{DICTIONARY_ROOT}</eml:Uuid><eml:QualifiedType>eml23.PropertyKind</eml:QualifiedType><eml:Title>property</eml:Title></eml:Parent>\
</eml:PropertyKind>"
    )
}

impl Handler for Served {
    fn offer(&self) -> ProtocolOffer {
        ProtocolOffer::v12(self.protocol, Role::Store)
    }

    fn handle(&mut self, delivery: &Delivery, responder: &mut Responder<'_>) -> Result<Handled, Declined> {
        let Body::Message(message) = &delivery.body else {
            return Err(Declined("no private protocol".into()));
        };
        let mut model = model();
        if self.with_kind {
            model.push(("eml23.PropertyKind", KIND, "a kind of ours", kind_xml()));
        }
        match message {
            Message::GetDataspaces(_) => {
                let response = GetDataspacesResponse {
                    dataspaces: vec![EtpDataspace {
                        uri: DATASPACE.into(),
                        path: "demo".into(),
                        ..Default::default()
                    }],
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            Message::GetResources(_) => {
                let resources = model
                    .iter()
                    .filter(|(_, uuid, _, _)| Some(*uuid) != self.hide)
                    .map(|(ty, uuid, title, _)| Resource {
                        uri: object_uri(ty, uuid, self.spelling),
                        name: (*title).into(),
                        ..Default::default()
                    })
                    .collect();
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
                    match model.iter().find(|(_, uuid, _, _)| uri.contains(uuid)) {
                        Some((_, _, _, xml)) => {
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
            // The one array: through the proxy's URI, as RESQML 2.0.1 asks -
            // any other URI is not found, which is what checks the bridge
            // resolved the proxy.
            Message::GetDataArrayMetadata(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_arrays.keys().cloned());
                let mut found = BTreeMap::new();
                for (key, id) in &request.data_arrays {
                    if id.uri.contains(PROXY) && id.path_in_resource == "/points" {
                        reply.answered(key.clone()).unwrap();
                        found.insert(
                            key.clone(),
                            DataArrayMetadata {
                                dimensions: vec![2, 3],
                                transport_array_type: AnyArrayType::ArrayOfDouble,
                                logical_array_type: AnyLogicalArrayType::ArrayOfDouble64Le,
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
                for (key, id) in &request.data_arrays {
                    if id.uri.contains(PROXY) && id.path_in_resource == "/points" {
                        reply.answered(key.clone()).unwrap();
                        found.insert(
                            key.clone(),
                            DataArray {
                                dimensions: vec![2, 3],
                                data: AnyArray {
                                    item: AnyArrayItem::ArrayOfDouble(ArrayOfDouble {
                                        values: vec![0.0, 0.0, 0.0, 10.0, 20.0, 30.0],
                                    }),
                                },
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
            _ => return Err(Declined("not in this test server".into())),
        }
        Ok(Handled::Answered)
    }
}

pub fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    energyml_eml_2_0::register(&mut registry);
    energyml_resqml_2_0_1::register(&mut registry);
    energyml_eml_2_3::register(&mut registry);
    Arc::new(registry)
}

/// A store over a session with a server that speaks `protocols`.
pub fn store_with(protocols: &[i32], spelling: Spelling) -> EtpStore<MemoryTransport, ManualClock> {
    store_with_hiding(protocols, spelling, None)
}

/// The same, with one object served but **not listed**: the point set still
/// references it, so the reference leaves the source.
pub fn store_hiding(hide: &'static str) -> EtpStore<MemoryTransport, ManualClock> {
    store_with_hiding(&[3, 4, 9, 24], Spelling::Energistics, Some(hide))
}

pub fn store_with_hiding(
    protocols: &[i32],
    spelling: Spelling,
    hide: Option<&'static str>,
) -> EtpStore<MemoryTransport, ManualClock> {
    store_with_all(protocols, spelling, hide, false)
}

/// The same, holding [`KIND`] as well.
pub fn store_with_kind() -> EtpStore<MemoryTransport, ManualClock> {
    store_with_all(&[3, 4, 9, 24], Spelling::Energistics, None, true)
}

pub fn store_with_all(
    protocols: &[i32],
    spelling: Spelling,
    hide: Option<&'static str>,
    with_kind: bool,
) -> EtpStore<MemoryTransport, ManualClock> {
    let mut offer = Offer::new("a test server", "0.1", Uuid([2; 16]));
    offer.data_objects = ["resqml20.*", "eml20.*", "eml23.*"]
        .iter()
        .map(|t| SupportedDataObject {
            qualified_type: t.to_string(),
            data_object_capabilities: BTreeMap::new(),
        })
        .collect();
    let handlers: Vec<Box<dyn Handler>> = protocols
        .iter()
        .map(|p| {
            Box::new(Served {
                protocol: *p,
                spelling,
                hide,
                with_kind,
            }) as Box<dyn Handler>
        })
        .collect();
    let server = Connection::server(offer, handlers, None, Uuid([0xAB; 16]), T0).unwrap();
    let clock = ManualClock::new(T0);
    let (offer, handlers) = reading_client(
        "the bridge's tests",
        "0.1",
        Uuid([1; 16]),
        &[
            "resqml20.*".to_string(),
            "eml20.*".to_string(),
            "eml23.*".to_string(),
        ],
    );
    let client = Client::open(
        MemoryTransport::new(server, clock.clone()),
        clock,
        offer,
        handlers,
        None,
        Settings::default(),
    )
    .unwrap();
    EtpStore::new(client, "demo", registry())
}

pub fn store() -> EtpStore<MemoryTransport, ManualClock> {
    store_with(&[3, 4, 9, 24], Spelling::Energistics)
}

pub fn block<F: std::future::Future>(future: F) -> F::Output {
    futures_executor::block_on(future)
}

pub fn uri(qualified_type: &str, uuid: &str) -> String {
    object_uri(qualified_type, uuid, Spelling::Energistics)
}
