//! Tier 3 - the client over a transport, without a socket.
//!
//! The transport is `retp_client::memory::MemoryTransport`, whose
//! other end is a server [`Connection`] in memory, and [`ManualClock`] a clock
//! it moves forward by exactly the time it was asked to wait. So a five-minute
//! timeout takes no time, and the client under test is the one the command
//! line uses - only the transport differs.

use std::collections::BTreeMap;
use std::time::Duration;

use retp_avro::Role;
use retp_avro::{Avro, Reader};
use retp_client::arrays::{
    Array, ArrayFailure, ArrayProgress, ArrayValues, Block, Grouping, Plan as ArrayPlan, allowed_transports,
    array_metadata, logical_element_size, plan, read_arrays, read_regions, reading_type,
    transport_element_bound, write_arrays, write_arrays_with,
};
use retp_client::download::{Download, Progress, download};
use retp_client::download::{DownloadReport, download_resumable};
use retp_client::memory::{ManualClock, MemoryTransport};
use retp_client::rest::{BlockingHttp, Http, HttpRequest, HttpResponse, Method, RestClient};
use retp_client::resume::{Connector, Retry, RunError, RunEvent, Setback};
use retp_client::{
    AsyncClient, Client, ClientError, Clock, Ended, Opened, ResourceQuery, ServerProfile, Settings, Sink,
    TransportError, asynchronous, commit_transaction, dataspace_uri, delete_data_objects, delete_dataspaces,
    described_client, get_data_objects, list_dataspaces, list_resources, list_supported_types,
    object_file_name, plain_text_warning, put_data_objects, put_dataspaces, reading_client,
    rollback_transaction, start_transaction, writing_client,
};
use retp_proto::capabilities::{Capabilities, Scope, Value as CapabilityValue};
use retp_proto::connection::Authorization;
use retp_proto::connection::{Connection, Declined, Handled, Handler, Responder};
use retp_proto::credentials::Credentials;
use retp_proto::negotiate::{Offer, ProtocolOffer, Tolerance};
use retp_proto::probe::{self, HandshakeSettings, Profile};
use retp_proto::wire::chunk_objects;
use retp_proto::wire::{Body, Delivery, EtpError};
use retp_types::datatypes::MessageHeader;
use retp_types::datatypes::data_array_types::{DataArray, DataArrayIdentifier, DataArrayMetadata};
use retp_types::datatypes::object::ContextScopeKind;
use retp_types::datatypes::object::{DataObject, Dataspace, PutResponse, Resource};
use retp_types::datatypes::object::{Edge, SupportedType};
use retp_types::datatypes::{
    AnyArray, AnyArrayItem, AnyArrayType, AnyLogicalArrayType, ArrayOfDouble, ArrayOfInt, ErrorInfo,
};
use retp_types::datatypes::{ArrayOfString, DataValue, DataValueItem, SupportedDataObject, Uuid};
use retp_types::dispatch::Message;
use retp_types::protocol::data_array::{
    GetDataArrayMetadataResponse, GetDataArraysResponse, GetDataSubarraysResponse, PutDataArraysResponse,
    PutDataSubarraysResponse, PutUninitializedDataArraysResponse,
};
use retp_types::protocol::dataspace::{DeleteDataspacesResponse, PutDataspacesResponse};
use retp_types::protocol::discovery::{GetResourcesEdgesResponse, GetResourcesResponse};
use retp_types::protocol::store::{
    Chunk, DeleteDataObjectsResponse, GetDataObjects, GetDataObjectsResponse, PutDataObjects,
    PutDataObjectsResponse,
};
use retp_types::protocol::supported_types::GetSupportedTypesResponse;
use retp_types::protocol::transaction::{
    CommitTransactionResponse, RollbackTransactionResponse, StartTransactionResponse,
};

const S: i64 = 1_000_000;
const T0: i64 = 1_760_000_000 * S;
/// The one object the fake store holds, and one it does not.
const HELD: &str = "eml:///resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)";
const LOST: &str = "eml:///resqml20.obj_HorizonInterpretation(00000000-0000-0000-0000-000000000000)";

/// The in-memory transport of `retp_client::memory`, with the two
/// ways these tests make a connection fall: after so many messages, or on a
/// `GetDataObjects` asking for more objects than a server can take.
type Loopback = MemoryTransport;

fn loopback(
    server: Connection,
    clock: &ManualClock,
    falls_after: Option<usize>,
    falls_on_more_than: Option<usize>,
) -> Loopback {
    let mut transport = MemoryTransport::new(server, clock.clone());
    if let Some(n) = falls_after {
        transport = transport.falls_after(n);
    }
    if let Some(most) = falls_on_more_than {
        transport = transport.falls_when(move |message| {
            let mut input = Reader::new(message);
            let header = MessageHeader::read(&mut input).unwrap();
            (header.protocol, header.message_type) == (4, 1)
                && GetDataObjects::read(&mut input).unwrap().uris.len() > most
        });
    }
    transport
}

/// What the test store lists - one object it holds, one it lost -, by its
/// ETP handler and by the fake REST API alike.
/// The one object the test store holds - with its uuid, as every EnergyML
/// object has one, and as the REST API's plural answers are keyed by it.
const OBJECT: &[u8] = b"<obj uuid=\"421a7a05-033a-450d-bcef-051352023578\"/>";

const RESOURCES: [(&str, &str); 2] = [
    (
        "resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)",
        "Top Brent",
    ),
    (
        "resqml20.obj_LocalDepth3dCrs(00000000-0000-0000-0000-000000000000)",
        "a lost CRS",
    ),
];

/// A store that knows two objects and one resource, and answers slowly when
/// told to.
/// An array the fake store was given: its dimensions and its values.
type WrittenArray = (Vec<i64>, Vec<f64>);

struct Store {
    protocol: i32,
    silent: bool,
    /// Answers with a `blobId` beside the data, as some RDDMS servers do.
    blob_beside_data: bool,
    /// Keeps its first `GetDataObjects` and answers it after the next one -
    /// a store answering in another order than it was asked, which ETP
    /// allows: its own message ids still grow.
    defer_first_get: bool,
    deferred: Option<Delivery>,
    /// What has been written into it, by URI - so that *upsert* can be seen
    /// rather than assumed: writing twice must leave one.
    held: std::cell::RefCell<BTreeMap<String, Vec<u8>>>,
    /// The arrays written into it, by path - so that what a write left can be
    /// **read back** rather than assumed. Doubles only: enough to tell a
    /// correct block walk from a wrong one, which is what is being checked.
    arrays: std::cell::RefCell<BTreeMap<String, WrittenArray>>,
    /// Refuses every write with `EREQUEST_DENIED` and this message - a store
    /// opened read-only, which is most of them.
    read_only: Option<&'static str>,
    /// Answers a commit with `successful: false` and this reason.
    commit_fails: Option<&'static str>,
    /// Answers about **another** transaction than the one asked about - a
    /// store confusing two, which nothing in the protocol makes impossible.
    commit_of_another: bool,
}

impl Store {
    fn new(protocol: i32, silent: bool, blob_beside_data: bool) -> Self {
        Store {
            protocol,
            silent,
            blob_beside_data,
            defer_first_get: false,
            deferred: None,
            held: std::cell::RefCell::new(BTreeMap::new()),
            arrays: std::cell::RefCell::new(BTreeMap::new()),
            read_only: None,
            commit_fails: None,
            commit_of_another: false,
        }
    }

    /// Each URI of `request` found when it holds `421a7a05`, `ENOT_FOUND`
    /// otherwise.
    fn answer_objects(&self, delivery: &Delivery, request: &GetDataObjects, responder: &mut Responder<'_>) {
        let mut reply = responder
            .wire()
            .begin_response(delivery)
            .for_keys(request.uris.keys().cloned());
        let mut found = BTreeMap::new();
        for (key, uri) in &request.uris {
            // What was written into it comes first - the bytes as they
            // arrived, reassembled from their chunks by the wire before this
            // handler ever saw them.
            if let Some(held) = self.held.borrow().get(uri) {
                reply.answered(key.clone()).unwrap();
                found.insert(
                    key.clone(),
                    DataObject {
                        data: held.clone(),
                        ..Default::default()
                    },
                );
                continue;
            }
            if uri.contains("421a7a05") {
                reply.answered(key.clone()).unwrap();
                let blob_id = self.blob_beside_data.then_some(Uuid([7; 16]));
                found.insert(
                    key.clone(),
                    DataObject {
                        data: OBJECT.to_vec(),
                        blob_id,
                        ..Default::default()
                    },
                );
            } else {
                reply
                    .failed(key.clone(), &EtpError::ENOT_FOUND.with_detail(uri.clone()))
                    .unwrap();
            }
        }
        // A store sends a big object as the wire allows it (§3.7.3.2): the
        // carrier with the data emptied and a BLOB id, then the chunks, the
        // last with the FIN. Exactly what this client does on the way out,
        // and what its receive side reassembles on the way in.
        let mut response = GetDataObjectsResponse { data_objects: found };
        let mut next = 0u8;
        let chunks: Vec<Chunk> = chunk_objects(response.data_objects.values_mut(), 3000, || {
            next += 1;
            Uuid([next; 16])
        });
        if chunks.is_empty() {
            let out = responder.wire().last(&mut reply, &response).unwrap();
            responder.send_all(out);
            return;
        }
        let mut out = vec![responder.wire().part(&mut reply, &response).unwrap()];
        let (last, rest) = chunks.split_last().unwrap();
        for chunk in rest {
            out.push(responder.wire().part(&mut reply, chunk).unwrap());
        }
        out.extend(responder.wire().last(&mut reply, last).unwrap());
        responder.send_all(out);
    }
}

impl Handler for Store {
    fn offer(&self) -> ProtocolOffer {
        // ChannelStreaming pairs a producer with a consumer, not a store
        // with a customer (§6.1).
        let role = if self.protocol == 1 {
            Role::Producer
        } else {
            Role::Store
        };
        let mut offer = ProtocolOffer::v12(self.protocol, role);
        if self.protocol == 9 {
            offer.capabilities = Capabilities::empty(Scope::Protocol)
                .set(
                    "MaxDataArraySize",
                    CapabilityValue::Long(STORE_ARRAY_LIMIT as i64),
                )
                .unwrap();
        }
        offer
    }

    fn handle(&mut self, delivery: &Delivery, responder: &mut Responder<'_>) -> Result<Handled, Declined> {
        if self.silent {
            return Ok(Handled::Answered);
        }
        let Body::Message(message) = &delivery.body else {
            return Err(Declined("no private protocol here".into()));
        };
        // StoreNotification (5): a subscription is confirmed, and one change
        // follows on its own - what a client reads as a stream.
        if let Message::SubscribeNotifications(request) = message {
            let response = retp_types::protocol::store_notification::SubscribeNotificationsResponse {
                success: request
                    .request
                    .keys()
                    .map(|k| (k.clone(), "subscribed".to_string()))
                    .collect(),
            };
            let out = responder.wire().respond(delivery, &response).unwrap();
            responder.send(out);
            let changed = retp_types::protocol::store_notification::ObjectChanged {
                change: retp_types::datatypes::object::ObjectChange {
                    change_kind: retp_types::datatypes::object::ObjectChangeKind::Update,
                    change_time: T0,
                    data_object: DataObject {
                        resource: Resource {
                            uri: HELD.into(),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                },
                request_uuid: request
                    .request
                    .values()
                    .next()
                    .map(|s| s.request_uuid)
                    .unwrap_or_default(),
            };
            let out = responder.wire().notify(&changed).unwrap();
            responder.send(out);
            return Ok(Handled::Answered);
        }
        if let Message::UnsubscribeNotifications(request) = message {
            let ended = retp_types::protocol::store_notification::SubscriptionEnded {
                reason: "unsubscribed".into(),
                request_uuid: request.request_uuid,
            };
            let out = responder.wire().respond(delivery, &ended).unwrap();
            responder.send(out);
            return Ok(Handled::Answered);
        }
        // ChannelSubscribe (21): one channel, id 7, described, subscribed -
        // and one data item sent on its own after the subscription -,
        // ranges answered, subscriptions stopped by name.
        if let Message::GetChannelMetadata(request) = message {
            let response = retp_types::protocol::channel_subscribe::GetChannelMetadataResponse {
                metadata: request
                    .uris
                    .iter()
                    .map(|(key, uri)| (key.clone(), channel_record(7, uri)))
                    .collect(),
            };
            let out = responder.wire().respond(delivery, &response).unwrap();
            responder.send(out);
            return Ok(Handled::Answered);
        }
        if let Message::SubscribeChannels(request) = message {
            let response = retp_types::protocol::channel_subscribe::SubscribeChannelsResponse {
                success: request
                    .channels
                    .keys()
                    .map(|k| (k.clone(), "subscribed".to_string()))
                    .collect(),
            };
            let out = responder.wire().respond(delivery, &response).unwrap();
            responder.send(out);
            let data = retp_types::protocol::channel_subscribe::ChannelData {
                data: vec![data_item(7, 100, 1.5)],
            };
            let out = responder.wire().notify(&data).unwrap();
            responder.send(out);
            return Ok(Handled::Answered);
        }
        if let Message::UnsubscribeChannels(request) = message {
            let stopped = retp_types::protocol::channel_subscribe::SubscriptionsStopped {
                reason: "unsubscribed".into(),
                channel_ids: request.channel_ids.clone(),
            };
            let out = responder.wire().respond(delivery, &stopped).unwrap();
            responder.send(out);
            return Ok(Handled::Answered);
        }
        if let Message::GetRanges(_) = message {
            let response = retp_types::protocol::channel_subscribe::GetRangesResponse {
                data: vec![data_item(7, 100, 1.5), data_item(7, 101, 2.5)],
            };
            let out = responder.wire().respond(delivery, &response).unwrap();
            responder.send(out);
            return Ok(Handled::Answered);
        }
        // ChannelStreaming (1): told to start, the producer describes its
        // channel and streams one item; told to stop, it says nothing.
        if let Message::StartStreaming(_) = message {
            let metadata = retp_types::protocol::channel_streaming::ChannelMetadata {
                channels: vec![channel_record(
                    9,
                    "eml:///witsml20.Channel(00000000-0000-0000-0000-000000000009)",
                )],
            };
            let out = responder.wire().notify(&metadata).unwrap();
            responder.send(out);
            let data = retp_types::protocol::channel_streaming::ChannelData {
                data: vec![data_item(9, 5, 42.0)],
            };
            let out = responder.wire().notify(&data).unwrap();
            responder.send(out);
            return Ok(Handled::Answered);
        }
        if let Message::StopStreaming(_) = message {
            return Ok(Handled::Answered);
        }
        match message {
            Message::GetDeletedResources(request) => {
                // One object deleted at T0: listed when asked from before,
                // not after.
                let since = request.delete_time_filter.unwrap_or(i64::MIN);
                let deleted = if since <= T0 {
                    vec![retp_types::datatypes::object::DeletedResource {
                        uri: format!("eml:///{}", RESOURCES[1].0),
                        deleted_time: T0,
                        ..Default::default()
                    }]
                } else {
                    Vec::new()
                };
                let response = retp_types::protocol::discovery::GetDeletedResourcesResponse {
                    deleted_resources: deleted,
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
                Ok(Handled::Answered)
            }
            Message::GetResources(request) => {
                // Two resources - one the store holds, one it lost -, filtered
                // by type, counted, and with an edge, when asked. With
                // `storeLastWriteFilter`, only the held one, written at T0.
                let since = request.store_last_write_filter;
                let counts = request.count_objects.then_some(3);
                let resources: Vec<Resource> = RESOURCES
                    .into_iter()
                    .filter(|(uri, _)| {
                        let types = &request.context.data_object_types;
                        types.is_empty() || types.iter().any(|t| uri.starts_with(t.trim_end_matches('*')))
                    })
                    .filter(|(uri, _)| since.is_none_or(|since| since <= T0 && uri.contains("421a7a05")))
                    .map(|(uri, name)| Resource {
                        uri: format!("eml:///{uri}"),
                        name: name.into(),
                        source_count: counts,
                        target_count: counts,
                        store_last_write: T0,
                        ..Default::default()
                    })
                    .collect();
                let mut response = responder.wire().begin_response(delivery);
                if request.include_edges {
                    let edges = GetResourcesEdgesResponse {
                        edges: vec![Edge {
                            source_uri: "eml:///a".into(),
                            target_uri: "eml:///b".into(),
                            ..Default::default()
                        }],
                    };
                    let out = responder.wire().part(&mut response, &edges).unwrap();
                    responder.send(out);
                }
                let out = responder
                    .wire()
                    .last(&mut response, &GetResourcesResponse { resources })
                    .unwrap();
                responder.send_all(out);
                Ok(Handled::Answered)
            }
            Message::GetSupportedTypes(request) => {
                let response = GetSupportedTypesResponse {
                    supported_types: vec![SupportedType {
                        data_object_type: "resqml20.obj_HorizonInterpretation".into(),
                        object_count: request.count_objects.then_some(1),
                        ..Default::default()
                    }],
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
                Ok(Handled::Answered)
            }
            Message::GetDataObjects(request) => {
                if self.defer_first_get {
                    self.defer_first_get = false;
                    self.deferred = Some(delivery.clone());
                    return Ok(Handled::Answered);
                }
                self.answer_objects(delivery, request, responder);
                if let Some(earlier) = self.deferred.take()
                    && let Body::Message(Message::GetDataObjects(asked)) = &earlier.body
                {
                    self.answer_objects(&earlier, asked, responder);
                }
                Ok(Handled::Answered)
            }
            Message::PutDataObjects(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_objects.keys().cloned());
                let mut success = BTreeMap::new();
                // A request whose action never closed - its last part without
                // the FIN - is one a store times out rather than answers
                // (§3.5.4.2); this one refuses it at once, by name, so that a
                // client that forgets the FIN hears of it.
                if !delivery.last {
                    for key in request.data_objects.keys() {
                        reply
                            .failed(
                                key.clone(),
                                &EtpError::EINVALID_STATE.with_detail(
                                    "the request never closed: its last part carries no FIN".to_string(),
                                ),
                            )
                            .unwrap();
                    }
                    let out = responder
                        .wire()
                        .last(&mut reply, &PutDataObjectsResponse { success })
                        .unwrap();
                    responder.send_all(out);
                    return Ok(Handled::Answered);
                }
                for (key, object) in &request.data_objects {
                    // A store that simply does not mention a key: the one case
                    // where nothing at all is known about a write.
                    if object.resource.uri.contains("ignored") {
                        reply.answered(key.clone()).unwrap();
                        continue;
                    }
                    if let Some(why) = self.read_only {
                        // §3.1.1.2 §4: the store *should* say why, and what it
                        // says is the only thing the person can act on.
                        reply
                            .failed(
                                key.clone(),
                                &EtpError::EREQUEST_DENIED.with_detail(why.to_string()),
                            )
                            .unwrap();
                        continue;
                    }
                    // Upsert (§9.1.1): the second write of a URI replaces the
                    // first, it does not fail and it does not add a second.
                    self.held
                        .borrow_mut()
                        .insert(object.resource.uri.clone(), object.data.clone());
                    reply.answered(key.clone()).unwrap();
                    success.insert(key.clone(), PutResponse::default());
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &PutDataObjectsResponse { success })
                    .unwrap();
                responder.send_all(out);
                Ok(Handled::Answered)
            }
            Message::DeleteDataObjects(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.uris.keys().cloned());
                let mut deleted_uris = BTreeMap::new();
                for (key, uri) in &request.uris {
                    // A container refuses unless the cascade was asked for
                    // (§24.3): a business refusal, with something to do about
                    // it, not a breakdown.
                    if uri.contains("container") && !request.prune_contained_objects {
                        // Each its own reason: a store refusing two things for
                        // two reasons is the case that tells a per-key
                        // exception from a single one.
                        let why = if uri.contains("second") {
                            "it is published"
                        } else {
                            "it holds two objects"
                        };
                        let refusal = EtpError::ENOCASCADE_DELETE.with_detail(format!("{uri}: {why}"));
                        reply.failed(key.clone(), &refusal).unwrap();
                        continue;
                    }
                    self.held.borrow_mut().remove(uri);
                    reply.answered(key.clone()).unwrap();
                    let also = if uri.contains("container") {
                        vec![format!("{uri}/inside")]
                    } else {
                        Vec::new()
                    };
                    let mut values = vec![uri.clone()];
                    values.extend(also);
                    deleted_uris.insert(key.clone(), ArrayOfString { values });
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &DeleteDataObjectsResponse { deleted_uris })
                    .unwrap();
                responder.send_all(out);
                Ok(Handled::Answered)
            }
            Message::PutDataspaces(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.dataspaces.keys().cloned());
                let mut success = BTreeMap::new();
                for (key, space) in &request.dataspaces {
                    // What an OSDU store requires and this one imitates: no
                    // owners, no dataspace - and it says which key is missing.
                    if !space.custom_data.contains_key("owners") {
                        let refusal =
                            EtpError::EINVALID_ARGUMENT.with_detail("owners is required".to_string());
                        reply.failed(key.clone(), &refusal).unwrap();
                        continue;
                    }
                    reply.answered(key.clone()).unwrap();
                    success.insert(key.clone(), space.uri.clone());
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &PutDataspacesResponse { success })
                    .unwrap();
                responder.send_all(out);
                Ok(Handled::Answered)
            }
            Message::DeleteDataspaces(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.uris.keys().cloned());
                let mut success = BTreeMap::new();
                for (key, uri) in &request.uris {
                    // A store holds `demo` and nothing else: anything else is
                    // refused by name, and the one it holds goes.
                    if uri != "eml:///dataspace('demo')" {
                        let refusal = EtpError::ENOT_FOUND.with_detail(format!("{uri}: no such dataspace"));
                        reply.failed(key.clone(), &refusal).unwrap();
                        continue;
                    }
                    reply.answered(key.clone()).unwrap();
                    success.insert(key.clone(), uri.clone());
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &DeleteDataspacesResponse { success })
                    .unwrap();
                responder.send_all(out);
                Ok(Handled::Answered)
            }
            Message::StartTransaction(request) => {
                // A store says no with `successful: false` and a reason, not
                // with an exception - which is why a client reading only the
                // uuid would carry on writing into nothing.
                let response = if request.dataspace_uris.iter().any(|u| u.contains("locked")) {
                    StartTransactionResponse {
                        transaction_uuid: Uuid([0; 16]),
                        successful: false,
                        failure_reason: "another transaction holds this dataspace".into(),
                    }
                } else {
                    StartTransactionResponse {
                        transaction_uuid: Uuid([9; 16]),
                        successful: true,
                        failure_reason: String::new(),
                    }
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
                Ok(Handled::Answered)
            }
            Message::CommitTransaction(request) => {
                let response = CommitTransactionResponse {
                    transaction_uuid: if self.commit_of_another {
                        Uuid([1; 16])
                    } else {
                        request.transaction_uuid
                    },
                    successful: self.commit_fails.is_none(),
                    // A store that refuses without a word: the empty reason is
                    // itself a case, and it must not reach a screen as a
                    // refusal with nothing wrong.
                    failure_reason: self.commit_fails.unwrap_or("").to_string(),
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
                Ok(Handled::Answered)
            }
            Message::RollbackTransaction(request) => {
                let response = RollbackTransactionResponse {
                    transaction_uuid: request.transaction_uuid,
                    successful: true,
                    failure_reason: String::new(),
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
                Ok(Handled::Answered)
            }
            Message::PutDataArrays(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_arrays.keys().cloned());
                // A store that answers **something else** than the response
                // that confirms the write. Not a fancy case: one RDDMS server alone
                // deviates from the spec ten times, and a client that read any
                // answer as a confirmation would report a write that never
                // happened.
                if request
                    .data_arrays
                    .values()
                    .any(|put| put.uid.path_in_resource.contains("mute"))
                {
                    let mut reply = responder
                        .wire()
                        .begin_response(delivery)
                        .for_keys(request.data_arrays.keys().cloned());
                    for key in request.data_arrays.keys() {
                        reply.answered(key.clone()).unwrap();
                    }
                    let out = responder
                        .wire()
                        .last(
                            &mut reply,
                            &PutDataSubarraysResponse {
                                success: BTreeMap::new(),
                            },
                        )
                        .unwrap();
                    responder.send_all(out);
                    return Ok(Handled::Answered);
                }
                for (key, put) in &request.data_arrays {
                    // One key refused by name, the others taken: what §3.7.3
                    // allows a map response, and what a client must sort out.
                    if put.uid.path_in_resource.contains("refused") {
                        reply
                            .failed(
                                key.clone(),
                                &EtpError::EREQUEST_DENIED.with_detail("not this one"),
                            )
                            .unwrap();
                        continue;
                    }
                    if let AnyArrayItem::ArrayOfDouble(values) = &put.array.data.item {
                        self.arrays.borrow_mut().insert(
                            put.uid.path_in_resource.clone(),
                            (put.array.dimensions.clone(), values.values.clone()),
                        );
                    }
                    reply.answered(key.clone()).unwrap();
                }
                let out = responder
                    .wire()
                    .last(
                        &mut reply,
                        &PutDataArraysResponse {
                            success: BTreeMap::new(),
                        },
                    )
                    .unwrap();
                responder.send_all(out);
                Ok(Handled::Answered)
            }
            Message::PutUninitializedDataArrays(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_arrays.keys().cloned());
                for (key, put) in &request.data_arrays {
                    // Declared: the store knows the shape, and an array of it
                    // waits for the subarrays. `NaN` so that a cell nobody
                    // wrote cannot pass for one that was written as zero.
                    let count: i64 = put.metadata.dimensions.iter().product();
                    let empty = vec![f64::NAN; count.max(0) as usize];
                    self.arrays.borrow_mut().insert(
                        put.uid.path_in_resource.clone(),
                        (put.metadata.dimensions.clone(), empty),
                    );
                    reply.answered(key.clone()).unwrap();
                }
                let out = responder
                    .wire()
                    .last(
                        &mut reply,
                        &PutUninitializedDataArraysResponse {
                            success: BTreeMap::new(),
                        },
                    )
                    .unwrap();
                responder.send_all(out);
                Ok(Handled::Answered)
            }
            Message::PutDataSubarrays(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_subarrays.keys().cloned());
                for (key, put) in &request.data_subarrays {
                    let mut held = self.arrays.borrow_mut();
                    // A subarray of an array nobody declared has nowhere to
                    // go: saying so is what makes the order testable.
                    let Some((dimensions, into)) = held.get_mut(&put.uid.path_in_resource) else {
                        let refusal = EtpError::ENOT_FOUND
                            .with_detail(format!("{} was not declared", put.uid.path_in_resource));
                        reply.failed(key.clone(), &refusal).unwrap();
                        continue;
                    };
                    let AnyArrayItem::ArrayOfDouble(from) = &put.data.item else {
                        continue;
                    };
                    // Row by row - walked here on purpose rather than by the
                    // client's own helper: a block placed by two readings of
                    // the same arithmetic is what this checks.
                    let rank = dimensions.len();
                    let run = put.counts[rank - 1] as usize;
                    let rows: i64 = put.counts.iter().product::<i64>() / put.counts[rank - 1].max(1);
                    let mut index = vec![0i64; rank.saturating_sub(1)];
                    for row in 0..rows as usize {
                        let offset = (0..rank).fold(0i64, |offset, k| {
                            let at = if k + 1 == rank {
                                put.starts[k]
                            } else {
                                put.starts[k] + index[k]
                            };
                            offset * dimensions[k] + at
                        }) as usize;
                        into[offset..offset + run].copy_from_slice(&from.values[row * run..row * run + run]);
                        for k in (0..rank.saturating_sub(1)).rev() {
                            index[k] += 1;
                            if index[k] < put.counts[k] {
                                break;
                            }
                            index[k] = 0;
                        }
                    }
                    reply.answered(key.clone()).unwrap();
                }
                let out = responder
                    .wire()
                    .last(
                        &mut reply,
                        &PutDataSubarraysResponse {
                            success: BTreeMap::new(),
                        },
                    )
                    .unwrap();
                responder.send_all(out);
                Ok(Handled::Answered)
            }
            Message::GetDataArrayMetadata(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_arrays.keys().cloned());
                let mut found = BTreeMap::new();
                for (key, id) in &request.data_arrays {
                    // What was written into it comes first: a store that could
                    // not describe what it has just been given would make a
                    // round trip untestable.
                    if let Some((dimensions, _)) = self.arrays.borrow().get(&id.path_in_resource) {
                        reply.answered(key.clone()).unwrap();
                        found.insert(
                            key.clone(),
                            DataArrayMetadata {
                                dimensions: dimensions.clone(),
                                transport_array_type: AnyArrayType::ArrayOfDouble,
                                logical_array_type: AnyLogicalArrayType::ArrayOfDouble64Le,
                                ..Default::default()
                            },
                        );
                        continue;
                    }
                    match stored_array(&id.path_in_resource) {
                        Some(array) => {
                            reply.answered(key.clone()).unwrap();
                            found.insert(key.clone(), array.metadata());
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
                        &GetDataArrayMetadataResponse {
                            array_metadata: found,
                        },
                    )
                    .unwrap();
                responder.send_all(out);
                Ok(Handled::Answered)
            }
            Message::GetDataArrays(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_arrays.keys().cloned());
                let mut found = BTreeMap::new();
                for (key, id) in &request.data_arrays {
                    if let Some((dimensions, values)) = self.arrays.borrow().get(&id.path_in_resource) {
                        reply.answered(key.clone()).unwrap();
                        found.insert(
                            key.clone(),
                            DataArray {
                                dimensions: dimensions.clone(),
                                data: AnyArray {
                                    item: AnyArrayItem::ArrayOfDouble(ArrayOfDouble {
                                        values: values.clone(),
                                    }),
                                },
                            },
                        );
                        continue;
                    }
                    let array = stored_array(&id.path_in_resource).unwrap();
                    let counts = array.dimensions.clone();
                    match array.block(&vec![0; counts.len()], &counts) {
                        Ok(data) => {
                            reply.answered(key.clone()).unwrap();
                            found.insert(key.clone(), data);
                        }
                        Err(error) => reply.failed(key.clone(), &error).unwrap(),
                    }
                }
                let out = responder
                    .wire()
                    .last(&mut reply, &GetDataArraysResponse { data_arrays: found })
                    .unwrap();
                responder.send_all(out);
                Ok(Handled::Answered)
            }
            Message::GetDataSubarrays(request) => {
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_subarrays.keys().cloned());
                let mut found = BTreeMap::new();
                for (key, asked) in &request.data_subarrays {
                    // What was written into it comes first, carved the same
                    // way the client carved it to send - the two walks of the
                    // arithmetic that a round trip is there to confront.
                    if let Some((dimensions, values)) = self.arrays.borrow().get(&asked.uid.path_in_resource)
                    {
                        let rank = dimensions.len();
                        let run = asked.counts[rank - 1] as usize;
                        let rows: i64 = asked.counts.iter().product::<i64>() / asked.counts[rank - 1].max(1);
                        let mut out = Vec::with_capacity(rows as usize * run);
                        let mut index = vec![0i64; rank.saturating_sub(1)];
                        for _ in 0..rows {
                            let offset = (0..rank).fold(0i64, |offset, k| {
                                let at = if k + 1 == rank {
                                    asked.starts[k]
                                } else {
                                    asked.starts[k] + index[k]
                                };
                                offset * dimensions[k] + at
                            }) as usize;
                            out.extend_from_slice(&values[offset..offset + run]);
                            for k in (0..rank.saturating_sub(1)).rev() {
                                index[k] += 1;
                                if index[k] < asked.counts[k] {
                                    break;
                                }
                                index[k] = 0;
                            }
                        }
                        reply.answered(key.clone()).unwrap();
                        found.insert(
                            key.clone(),
                            DataArray {
                                dimensions: asked.counts.clone(),
                                data: AnyArray {
                                    item: AnyArrayItem::ArrayOfDouble(ArrayOfDouble { values: out }),
                                },
                            },
                        );
                        continue;
                    }
                    let array = stored_array(&asked.uid.path_in_resource).unwrap();
                    let starts: Vec<u64> = asked.starts.iter().map(|v| *v as u64).collect();
                    let counts: Vec<u64> = asked.counts.iter().map(|v| *v as u64).collect();
                    match array.block(&starts, &counts) {
                        Ok(data) => {
                            reply.answered(key.clone()).unwrap();
                            found.insert(key.clone(), data);
                        }
                        Err(error) => reply.failed(key.clone(), &error).unwrap(),
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
                Ok(Handled::Answered)
            }
            _ => Err(Declined("not in this test store".into())),
        }
    }
}

fn server(silent: bool, now: i64) -> Connection {
    server_with(silent, false, now)
}

fn server_with(silent: bool, blob_beside_data: bool, now: i64) -> Connection {
    server_full(silent, blob_beside_data, None, now)
}

fn server_full(
    silent: bool,
    blob_beside_data: bool,
    authorizer: Option<Box<dyn retp_proto::connection::Authorizer>>,
    now: i64,
) -> Connection {
    server_deferring(silent, blob_beside_data, authorizer, false, None, now)
}

/// [`server_full`], whose `GetDataObjects` answers come in another order than
/// they were asked when `defer` is set.
fn server_deferring(
    silent: bool,
    blob_beside_data: bool,
    authorizer: Option<Box<dyn retp_proto::connection::Authorizer>>,
    defer: bool,
    carrying: Option<i64>,
    now: i64,
) -> Connection {
    let mut offer = Offer::new("a test store", "0.1", Uuid([2; 16]));
    offer.data_objects = vec![SupportedDataObject {
        qualified_type: "resqml20.*".into(),
        data_object_capabilities: BTreeMap::new(),
    }];
    if let Some(payload) = carrying {
        offer.endpoint = offer
            .endpoint
            .set(
                retp_proto::capabilities::names::MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE,
                CapabilityValue::Long(payload),
            )
            .unwrap();
    }
    Connection::server(
        offer,
        vec![
            Box::new(Store::new(3, silent, blob_beside_data)),
            Box::new(Store {
                defer_first_get: defer,
                ..Store::new(4, silent, blob_beside_data)
            }),
            Box::new(Store::new(24, silent, blob_beside_data)),
            Box::new(Store::new(25, silent, blob_beside_data)),
            Box::new(Store::new(9, silent, blob_beside_data)),
            Box::new(Store::new(5, silent, blob_beside_data)),
            Box::new(Store::new(21, silent, blob_beside_data)),
            Box::new(Store::new(1, silent, blob_beside_data)),
        ],
        authorizer,
        Uuid([0xAB; 16]),
        now,
    )
    .unwrap()
}

fn open(silent: bool, falls_after: Option<usize>) -> Client<Loopback, ManualClock> {
    open_carrying(silent, falls_after, None)
}

/// The same session, the store saying how much one message may carry.
fn open_carrying(
    silent: bool,
    falls_after: Option<usize>,
    carrying: Option<i64>,
) -> Client<Loopback, ManualClock> {
    let clock = ManualClock::new(T0);
    let transport = loopback(
        server_deferring(silent, false, None, false, carrying, T0),
        &clock,
        falls_after,
        None,
    );
    let (offer, handlers) = reading_client("test client", "0.1", Uuid([1; 16]), &["resqml20.*".to_string()]);
    let handlers: Vec<Box<dyn Handler>> = handlers
        .into_iter()
        .filter(|h| h.offer().protocol != 24)
        .collect();
    Client::open(transport, clock, offer, handlers, None, Settings::default()).unwrap()
}

/// An answer the client refuses ends the call **at once**, with the reason -
/// not at the timeout, five minutes later, as the first `get` on one RDDMS did. The same answer is taken by a tolerant client.
#[test]
fn a_refused_answer_ends_the_call_at_once() {
    for tolerance in [Tolerance::Tolerant, Tolerance::Strict] {
        let clock = ManualClock::new(T0);
        let transport = loopback(server_with(false, true, T0), &clock, None, None);
        let (mut offer, handlers) =
            reading_client("test client", "0.1", Uuid([1; 16]), &["resqml20.*".to_string()]);
        offer.tolerance = tolerance;
        let handlers: Vec<Box<dyn Handler>> = handlers
            .into_iter()
            .filter(|h| h.offer().protocol != 24)
            .collect();
        let mut client = Client::open(
            transport,
            clock.clone(),
            offer,
            handlers,
            None,
            Settings::default(),
        )
        .unwrap();
        let uri =
            "eml:///resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)".to_string();
        let answer = get_data_objects(&mut client, std::slice::from_ref(&uri), "xml");
        match tolerance {
            Tolerance::Tolerant => assert_eq!(answer.unwrap()[&uri].as_ref().unwrap().data, OBJECT),
            Tolerance::Strict => match answer {
                Err(ClientError::AnswerRefused { incident, .. }) => {
                    assert_eq!(incident.code(), EtpError::EINVALID_ARGUMENT.code());
                }
                other => panic!("{other:?}"),
            },
        }
        assert!(
            clock.now() - T0 < 5 * S,
            "answered in {} s",
            (clock.now() - T0) / S
        );
    }
}

/// A Discovery query carries what it was given: the type filter, the
/// counts, the edges - and the edges come back apart from the resources.
#[test]
fn a_resource_query_carries_its_filter_counts_and_edges() {
    let mut client = open(false, None);
    let mut query = ResourceQuery::new("eml:///");
    query.data_object_types = vec!["resqml20.obj_HorizonInterpretation".into()];
    query.count_objects = true;
    query.include_edges = true;
    let listed = list_resources(&mut client, &query).unwrap();
    assert_eq!(listed.resources.len(), 1, "the type filter reached the store");
    assert_eq!(
        (listed.resources[0].source_count, listed.resources[0].target_count),
        (Some(3), Some(3))
    );
    assert_eq!(listed.edges.len(), 1);
    assert_eq!(listed.edges[0].target_uri, "eml:///b");
    let plain = list_resources(&mut client, &ResourceQuery::new("eml:///")).unwrap();
    assert!(plain.edges.is_empty() && plain.resources.iter().all(|r| r.source_count.is_none()));
}

/// `GetSupportedTypes` goes to its store - and a scope §22.3.1 does not
/// allow, or a URI that is not canonical, is refused before it leaves.
#[test]
fn supported_types_are_asked_as_the_spec_says() {
    let mut client = open(false, None);
    let counted =
        list_supported_types(&mut client, "eml:///", ContextScopeKind::Targets, true, false).unwrap();
    assert_eq!(counted.items.len(), 1);
    assert_eq!(counted.items[0].object_count, Some(1));
    let uncounted =
        list_supported_types(&mut client, "eml:///", ContextScopeKind::Sources, false, false).unwrap();
    assert_eq!(uncounted.items[0].object_count, None);
    for (uri, scope) in [
        ("eml:///", ContextScopeKind::Self_),
        ("eml:///", ContextScopeKind::TargetsOrSelf),
        ("demo", ContextScopeKind::Targets),
    ] {
        let refused = list_supported_types(&mut client, uri, scope, false, false);
        assert!(
            matches!(refused, Err(ClientError::Invalid(_))),
            "{uri} {scope:?}: {refused:?}"
        );
    }
}

/// A format the session did not agree is refused before the server is
/// asked: the test store agrees to `xml` only, of the `xml` and `json` this
/// client offers.
#[test]
fn a_format_not_agreed_is_refused_before_asking() {
    let mut client = open(false, None);
    assert_eq!(client.negotiated().formats, ["xml"]);
    let uri = "eml:///resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)".to_string();
    match get_data_objects(&mut client, std::slice::from_ref(&uri), "json") {
        Err(ClientError::Invalid(incident)) => {
            assert!(incident.message().contains("`json`"), "{}", incident.message())
        }
        other => panic!("{other:?}"),
    }
}

/// What the session agreed to is not a filter: this one agreed to
/// `resqml20.*`, and an `eml20` object the store serves is read all the
/// same - an RDDMS server agrees to `resqml20` and serves every type it holds.
#[test]
fn an_object_of_a_type_not_agreed_is_read() {
    let mut client = open(false, None);
    let agreed: Vec<&str> = client
        .negotiated()
        .data_objects
        .iter()
        .map(|d| d.qualified_type.as_str())
        .collect();
    assert_eq!(agreed, ["resqml20.*"]);
    let uri = "eml:///eml20.obj_EpcExternalPartReference(421a7a05-033a-450d-bcef-051352023578)".to_string();
    let answers = get_data_objects(&mut client, std::slice::from_ref(&uri), "xml").unwrap();
    assert_eq!(answers[&uri].as_ref().map(|o| o.data.as_slice()), Ok(OBJECT));
}

/// A file per object is named by its type and UUID - the version too when
/// the URI carries one - and a URI naming no object gives no name.
#[test]
fn objects_are_filed_by_type_and_uuid() {
    let uuid = "421a7a05-033a-450d-bcef-051352023578";
    assert_eq!(
        object_file_name(
            &format!("eml:///dataspace('demo')/resqml20.obj_HorizonInterpretation({uuid})"),
            "xml"
        )
        .as_deref(),
        Some(format!("resqml20.obj_HorizonInterpretation_{uuid}.xml").as_str())
    );
    assert_eq!(
        object_file_name(
            &format!("eml:///eml23.Activity(uuid={uuid},version='2.1')"),
            "json"
        )
        .as_deref(),
        Some(format!("eml23.Activity_{uuid}_2.1.json").as_str())
    );
    assert_eq!(object_file_name("eml:///dataspace('demo')", "xml"), None);
    assert_eq!(dataspace_uri("carl/teapot"), "eml:///dataspace('carl/teapot')");
    assert_eq!(dataspace_uri(""), "eml:///");
    assert_eq!(dataspace_uri("eml:///dataspace('x')"), "eml:///dataspace('x')");
}

/// A download writes a file per object the store answers, names the one it
/// lost, and - run again - takes the files already there as done: they are
/// its journal. `overwrite` writes them again.
#[test]
fn a_download_goes_on_from_the_files_already_there() {
    let directory = std::env::temp_dir().join(format!("retp-download-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    let mut client = open(false, None);
    let mut job = Download::new("eml:///", &directory);
    job.batch = 1;
    let mut batches = 0;
    let first = download(&mut client, &job, |p| {
        batches += matches!(p, Progress::Batch { .. }) as usize
    })
    .unwrap();
    assert_eq!(
        (
            first.listed,
            first.written,
            first.already_there,
            first.failed.len()
        ),
        (2, 1, 0, 1)
    );
    assert_eq!(batches, 2, "one batch per object at a batch of 1");
    assert!(!first.complete(), "a lost object makes the download partial");
    let file = directory.join("resqml20.obj_HorizonInterpretation_421a7a05-033a-450d-bcef-051352023578.xml");
    assert_eq!(std::fs::read(&file).unwrap(), OBJECT);
    assert!(
        std::fs::read_dir(&directory).unwrap().all(|e| !e
            .unwrap()
            .path()
            .to_string_lossy()
            .ends_with(".part"))
    );

    let again = download(&mut client, &job, |_| {}).unwrap();
    assert_eq!(
        (again.written, again.already_there, again.failed.len()),
        (0, 1, 1),
        "only the lost one is asked again"
    );

    job.overwrite = true;
    let rewritten = download(&mut client, &job, |_| {}).unwrap();
    assert_eq!((rewritten.written, rewritten.already_there), (1, 0));
    std::fs::remove_dir_all(&directory).unwrap();
}

/// A client described in JSON speaks the protocols it lists, and asks for
/// the types it gives.
#[test]
fn a_described_client_speaks_what_it_lists() {
    let description = retp_proto::describe::OfferDescription::parse(
        r#"{ "types": ["resqml20.obj_HorizonInterpretation"], "protocols": [3] }"#,
    )
    .unwrap();
    let (offer, handlers) = described_client("test client", "0.1", Uuid([1; 16]), &description).unwrap();
    assert_eq!(handlers.len(), 1);
    let clock = ManualClock::new(T0);
    let transport = loopback(server(false, T0), &clock, None, None);
    let client = Client::open(transport, clock, offer, handlers, None, Settings::default()).unwrap();
    assert_eq!(
        client.negotiated().protocols.keys().copied().collect::<Vec<_>>(),
        [3]
    );
    let types: Vec<&str> = client
        .negotiated()
        .data_objects
        .iter()
        .map(|d| d.qualified_type.as_str())
        .collect();
    assert_eq!(types, ["resqml20.obj_HorizonInterpretation"]);
}

// ---------------------------------------------------------------------------
// Long operations (ADR-0003)
// ---------------------------------------------------------------------------

/// How one connection of a [`Connections`] behaves.
#[derive(Clone, Copy, Default)]
struct Plan {
    falls_after: Option<usize>,
    falls_on_more_than: Option<usize>,
}

/// A connector whose connections follow their plans, one after the other -
/// the last plan for every connection past the list - and whose pauses move
/// the test clock.
struct Connections {
    clock: ManualClock,
    plans: Vec<Plan>,
    opened: usize,
    pauses: Vec<Duration>,
    /// `Authorize` calls the servers saw, when they ask for one.
    authorizations: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
    /// What this connector's `resume` answers: `None` - the default and the
    /// shipped behaviour - it offers none; `Some(Ok(()))` the server takes
    /// the session back; `Some(Err(why))` it refuses.
    resume: Option<Result<(), String>>,
    /// How many times the resume was tried.
    resumes: usize,
}

impl Connections {
    fn new(plans: &[Plan]) -> Self {
        Self {
            clock: ManualClock::new(T0),
            plans: plans.to_vec(),
            opened: 0,
            pauses: Vec::new(),
            authorizations: None,
            resume: None,
            resumes: 0,
        }
    }
}

impl Connector for Connections {
    type Session = Client<Loopback, ManualClock>;

    fn connect(&mut self) -> Result<Client<Loopback, ManualClock>, Box<dyn std::error::Error + Send + Sync>> {
        let plan = self
            .plans
            .get(self.opened)
            .or(self.plans.last())
            .copied()
            .unwrap_or_default();
        self.opened += 1;
        let now = self.clock.now();
        let authorizer: Option<Box<dyn retp_proto::connection::Authorizer>> =
            self.authorizations.clone().map(|seen| {
                Box::new(move |authorization: &str, _now: i64| {
                    seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if authorization == "Bearer good" {
                        Authorization::Granted { expires_at: None }
                    } else {
                        Authorization::Challenge(vec!["Bearer".into()])
                    }
                }) as Box<dyn retp_proto::connection::Authorizer>
            });
        let credentials = self
            .authorizations
            .as_ref()
            .map(|_| Credentials::bearer("good").unwrap());
        let transport = loopback(
            server_full(false, false, authorizer, now),
            &self.clock,
            plan.falls_after,
            plan.falls_on_more_than,
        );
        let (offer, handlers) =
            reading_client("test client", "0.1", Uuid([1; 16]), &["resqml20.*".to_string()]);
        Ok(Client::open(
            transport,
            self.clock.clone(),
            offer,
            handlers,
            credentials,
            Settings::default(),
        )?)
    }

    fn resume(
        &mut self,
    ) -> Option<Result<Client<Loopback, ManualClock>, Box<dyn std::error::Error + Send + Sync>>> {
        let answer = self.resume.clone()?;
        self.resumes += 1;
        Some(match answer {
            Ok(()) => self.connect(),
            Err(why) => Err(why.into()),
        })
    }

    fn pause(&mut self, how_long: Duration) {
        self.pauses.push(how_long);
        self.clock.advance(how_long);
    }
}

fn scratch(name: &str) -> std::path::PathBuf {
    let directory = std::env::temp_dir().join(format!("retp-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    directory
}

/// Run a download of the test store's two objects, a batch at a time.
fn resumable(
    connections: &mut Connections,
    directory: &std::path::Path,
    batch: usize,
    retry: Retry,
) -> (
    Result<DownloadReport, Box<retp_client::download::Stopped>>,
    Vec<Setback>,
) {
    let mut job = Download::new("eml:///", directory);
    job.batch = batch;
    let mut setbacks = Vec::new();
    let mut session = None;
    let result = download_resumable(connections, &mut session, &job, &retry, |event| {
        if let RunEvent::Setback(setback) = event {
            setbacks.push(setback);
        }
    });
    (result, setbacks)
}

/// A download cut by a lost connection goes on in a new session, from the
/// batch that was cut - and ends as a download that was never cut.
#[test]
fn a_dropped_download_resumes_where_it_stopped() {
    let calm = scratch("calm");
    let (whole, _) = resumable(
        &mut Connections::new(&[Plan::default()]),
        &calm,
        1,
        Retry::default(),
    );
    let whole = whole.unwrap();

    // The first session falls once its first GetDataObjects is out:
    // RequestSession, GetResources, GetDataObjects - the third message.
    let cut = scratch("cut");
    let mut connections = Connections::new(&[
        Plan {
            falls_after: Some(3),
            ..Default::default()
        },
        Plan::default(),
    ]);
    let (resumed, setbacks) = resumable(&mut connections, &cut, 1, Retry::default());
    let resumed = resumed.unwrap();
    assert_eq!(connections.opened, 2);
    assert_eq!(resumed, whole, "the same report as a download never cut");
    assert_eq!((resumed.written, resumed.failed.len()), (1, 1));

    // And the reconnection was said, with its step, count, pause and cause.
    let [
        Setback::Reconnecting {
            step,
            attempt,
            max,
            pause,
            why,
        },
        Setback::Reopened {
            resumed,
            refused,
            since,
            too_late,
        },
    ] = setbacks.as_slice()
    else {
        panic!("{setbacks:#?}");
    };
    // No connector here offers the server's own resume, so the way back is
    // the whole one, and nothing was refused because nothing was tried.
    assert!(!resumed && refused.is_none() && !too_late, "{setbacks:#?}");
    // And it says from when changes can be asked: the watermark of the
    // session that fell, less the propagation delay.
    assert!(since.is_some(), "{setbacks:#?}");
    assert_eq!(
        (step.as_str(), *attempt, *max, *pause),
        ("objects 1-1 of 2", 1, 3, Duration::from_secs(1))
    );
    assert!(why.contains("fell"), "{why}");
    let _ = std::fs::remove_dir_all(&calm);
    let _ = std::fs::remove_dir_all(&cut);
}

/// A new session is a new session: the server asks for authorization again,
/// and gets it - no shortcut (#232).
#[test]
fn a_reconnection_authorizes_again() {
    let directory = scratch("authorized");
    // RequestSession, Authorize, RequestSession, GetResources, GetDataObjects.
    let mut connections = Connections::new(&[
        Plan {
            falls_after: Some(5),
            ..Default::default()
        },
        Plan::default(),
    ]);
    let seen = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    connections.authorizations = Some(seen.clone());
    let (result, _) = resumable(&mut connections, &directory, 1, Retry::default());
    assert_eq!(result.unwrap().written, 1);
    assert_eq!(
        (
            connections.opened,
            seen.load(std::sync::atomic::Ordering::Relaxed)
        ),
        (2, 2),
        "one authorization per session"
    );
    let _ = std::fs::remove_dir_all(&directory);
}

/// The same step failing `max_retries` times in a row stops the run, by the
/// step's name - after pauses that doubled.
#[test]
fn the_same_step_failing_max_retries_times_stops_by_name() {
    let directory = scratch("gave-up");
    let mut connections = Connections::new(&[Plan {
        falls_on_more_than: Some(0),
        ..Default::default()
    }]);
    let (result, _) = resumable(&mut connections, &directory, 1, Retry::default());
    let stopped = result.unwrap_err();
    let RunError::GaveUp { step, attempts, last } = &stopped.why else {
        panic!("{stopped}");
    };
    assert_eq!((step.as_str(), *attempts), ("objects 1-1 of 2", 3));
    assert!(last.contains("fell"), "{last}");
    assert_eq!(connections.opened, 3);
    assert_eq!(
        connections.pauses,
        [Duration::from_secs(1), Duration::from_secs(2)]
    );
    assert!(stopped.to_string().contains("objects 1-1 of 2"), "{stopped}");
    let _ = std::fs::remove_dir_all(&directory);
}

/// Failures at different steps do not add up: a step that succeeds resets
/// the count. Two falls, each after progress, with a limit of two.
#[test]
fn progress_resets_the_retry_count() {
    let directory = scratch("reset");
    // Session 1: RequestSession, GetResources, GetDataObjects #1 - falls.
    // Session 2: RequestSession, GetDataObjects #1 answered, #2 - falls.
    // Session 3: RequestSession, GetDataObjects #2 answered.
    let plans = [
        Plan {
            falls_after: Some(3),
            ..Default::default()
        },
        Plan {
            falls_after: Some(3),
            ..Default::default()
        },
        Plan::default(),
    ];
    let mut connections = Connections::new(&plans);
    let retry = Retry {
        max_retries: 2,
        ..Retry::default()
    };
    let (result, _) = resumable(&mut connections, &directory, 1, retry);
    assert_eq!(result.unwrap().written, 1);
    assert_eq!(connections.opened, 3);
    let _ = std::fs::remove_dir_all(&directory);
}

/// An object the store does not have is reported - and not retried: no
/// second session for a refusal that would come back the same. And a request
/// the session cannot make stops the run at once.
#[test]
fn a_refusal_is_reported_not_retried() {
    let directory = scratch("refused");
    let mut connections = Connections::new(&[Plan::default()]);
    let (result, setbacks) = resumable(&mut connections, &directory, 2, Retry::default());
    let report = result.unwrap();
    assert_eq!(report.failed.len(), 1);
    assert_eq!(report.failed[0].1.code, EtpError::ENOT_FOUND.code());
    assert!(setbacks.is_empty() && connections.opened == 1, "{setbacks:#?}");

    let mut job = Download::new("eml:///", &directory);
    job.format = "json".into();
    let mut session = None;
    let mut connections = Connections::new(&[Plan::default()]);
    let stopped =
        download_resumable(&mut connections, &mut session, &job, &Retry::default(), |_| {}).unwrap_err();
    assert!(
        matches!(
            stopped.why,
            RunError::Refused {
                why: ClientError::Invalid(_),
                ..
            }
        ),
        "{stopped}"
    );
    assert_eq!(connections.opened, 1);
    let _ = std::fs::remove_dir_all(&directory);
}

/// A server that falls on a message too big for it falls there every time:
/// the step is halved before the count runs out, and the download ends.
#[test]
fn a_failing_step_is_halved_before_giving_up() {
    let directory = scratch("halved");
    let mut connections = Connections::new(&[Plan {
        falls_on_more_than: Some(1),
        ..Default::default()
    }]);
    let (result, setbacks) = resumable(&mut connections, &directory, 2, Retry::default());
    assert_eq!(result.unwrap().written, 1);
    assert!(
        setbacks
            .iter()
            .any(|s| matches!(s, Setback::Halved { step } if step == "objects 1-1 of 2")),
        "{setbacks:#?}"
    );
    assert_eq!(connections.opened, 3);
    let _ = std::fs::remove_dir_all(&directory);
}

// ---------------------------------------------------------------------------
// Data arrays (protocol 9)
// ---------------------------------------------------------------------------

/// The test store's `MaxDataArraySize`: the smallest the specification
/// allows (§13.2.3).
const STORE_ARRAY_LIMIT: u64 = 100_000;

/// An array the test store holds: its values are their own flat index, so a
/// value out of place is seen.
struct StoredArray {
    dimensions: Vec<u64>,
    logical: AnyLogicalArrayType,
    transport: AnyArrayType,
    preferred: Vec<u64>,
    /// Answers every block with its counts reversed.
    lies: bool,
    /// Answers every block one value short of its counts.
    short: bool,
}

fn stored_array(path: &str) -> Option<StoredArray> {
    let array = |dimensions: &[u64], logical, transport| StoredArray {
        dimensions: dimensions.to_vec(),
        logical,
        transport,
        preferred: Vec::new(),
        lies: false,
        short: false,
    };
    use AnyArrayType as T;
    use AnyLogicalArrayType as L;
    Some(match path {
        "/small" => array(&[3, 4], L::ArrayOfDouble64Le, T::ArrayOfDouble),
        "/large" => array(&[300, 100], L::ArrayOfDouble64Le, T::ArrayOfDouble),
        "/ints" => array(&[50_000], L::ArrayOfInt32Le, T::ArrayOfInt),
        "/bytes" => array(&[60_000], L::ArrayOfInt16Le, T::Bytes),
        "/chunked" => StoredArray {
            preferred: vec![10, 100],
            ..array(&[300, 100], L::ArrayOfDouble64Le, T::ArrayOfDouble)
        },
        "/short" => StoredArray {
            short: true,
            ..array(&[3, 4], L::ArrayOfDouble64Le, T::ArrayOfDouble)
        },
        // One vendor's RDDMS: every array said to be booleans.
        "/booleans" => array(&[3, 4], L::ArrayOfBoolean, T::ArrayOfDouble),
        "/liar" => StoredArray {
            lies: true,
            ..array(&[300, 100], L::ArrayOfDouble64Le, T::ArrayOfDouble)
        },
        _ => return None,
    })
}

/// The flat, row-major indices of a block - worked out element by element,
/// not the way the client copies rows.
fn block_indices(dimensions: &[u64], starts: &[u64], counts: &[u64]) -> Vec<u64> {
    let total: u64 = counts.iter().product();
    (0..total)
        .map(|linear| {
            let mut rest = linear;
            let mut index = vec![0; counts.len()];
            for k in (0..counts.len()).rev() {
                index[k] = rest % counts[k];
                rest /= counts[k];
            }
            (0..dimensions.len()).fold(0, |offset, k| offset * dimensions[k] + starts[k] + index[k])
        })
        .collect()
}

impl StoredArray {
    fn metadata(&self) -> DataArrayMetadata {
        DataArrayMetadata {
            dimensions: self.dimensions.iter().map(|d| *d as i64).collect(),
            preferred_subarray_dimensions: self.preferred.iter().map(|d| *d as i64).collect(),
            transport_array_type: self.transport,
            logical_array_type: self.logical,
            ..Default::default()
        }
    }

    /// A block, or `ELIMIT_EXCEEDED` when it is over the store's limit -
    /// what §13.2.2 row 5 has a store answer.
    fn block(&self, starts: &[u64], counts: &[u64]) -> Result<DataArray, retp_proto::wire::Incident> {
        let element = logical_element_size(self.logical).unwrap();
        let bytes = counts.iter().product::<u64>() * element;
        if bytes > STORE_ARRAY_LIMIT {
            return Err(EtpError::ELIMIT_EXCEEDED.with_detail(format!("{bytes} bytes asked at once")));
        }
        let mut indices = block_indices(&self.dimensions, starts, counts);
        if self.short {
            indices.pop();
        }
        let item = match self.transport {
            AnyArrayType::ArrayOfDouble => AnyArrayItem::ArrayOfDouble(ArrayOfDouble {
                values: indices.iter().map(|i| *i as f64).collect(),
            }),
            AnyArrayType::ArrayOfInt => AnyArrayItem::ArrayOfInt(ArrayOfInt {
                values: indices.iter().map(|i| *i as i32).collect(),
            }),
            AnyArrayType::Bytes => {
                AnyArrayItem::Bytes(indices.iter().flat_map(|i| (*i as i16).to_le_bytes()).collect())
            }
            other => panic!("the test store holds no {other:?}"),
        };
        let mut dimensions: Vec<i64> = counts.iter().map(|c| *c as i64).collect();
        if self.lies {
            dimensions.reverse();
        }
        Ok(DataArray {
            dimensions,
            data: AnyArray { item },
        })
    }
}

fn id(path: &str) -> DataArrayIdentifier {
    DataArrayIdentifier {
        uri: "eml:///eml20.obj_EpcExternalPartReference(421a7a05-033a-450d-bcef-051352023578)".into(),
        path_in_resource: path.into(),
    }
}

/// Sizes as §13.2.2.1 gives them: `bytes` carries the logical size (rule 2);
/// `arrayOfInt` and `arrayOfLong` are Avro varints, 5 and 10 bytes at worst
/// (rule 3, and Avro 1.11's zig-zag encoding); a string has no size before it
/// is read.
#[test]
fn an_element_costs_what_its_type_says() {
    use AnyArrayType as T;
    use AnyLogicalArrayType as L;
    assert_eq!(logical_element_size(L::ArrayOfInt32Le), Some(4));
    assert_eq!(logical_element_size(L::ArrayOfDouble64Be), Some(8));
    assert_eq!(logical_element_size(L::ArrayOfString), None);
    assert_eq!(transport_element_bound(T::Bytes, L::ArrayOfInt16Le), Some(2));
    assert_eq!(transport_element_bound(T::ArrayOfInt, L::ArrayOfInt32Le), Some(5));
    assert_eq!(
        transport_element_bound(T::ArrayOfLong, L::ArrayOfInt64Le),
        Some(10)
    );
    assert_eq!(transport_element_bound(T::ArrayOfString, L::ArrayOfString), None);
}

/// Small enough, it is read whole; too big, it is cut along its outer
/// dimension; one row too big, the next dimension is cut too; the store's
/// own chunking is followed when it fits; an element bigger than a message
/// has no plan.
#[test]
fn an_array_is_read_whole_or_by_blocks_that_fit() {
    assert_eq!(plan(&[10, 10], 8, 1000, &[]), Ok(ArrayPlan::Whole));

    let ArrayPlan::Blocks(rows) = plan(&[100, 10], 8, 1000, &[]).unwrap() else {
        panic!()
    };
    assert_eq!(rows.len(), 9, "12 rows of 80 bytes a block");
    assert_eq!(
        rows[0],
        Block {
            starts: vec![0, 0],
            counts: vec![12, 10]
        }
    );
    assert_eq!(
        rows[8],
        Block {
            starts: vec![96, 0],
            counts: vec![4, 10]
        }
    );

    let ArrayPlan::Blocks(pieces) = plan(&[2, 1000], 8, 1000, &[]).unwrap() else {
        panic!()
    };
    assert_eq!(pieces.len(), 16, "a row of 8000 bytes is cut in 8");
    assert_eq!(
        pieces[1],
        Block {
            starts: vec![0, 125],
            counts: vec![1, 125]
        }
    );

    let ArrayPlan::Blocks(chunked) = plan(&[100, 10], 8, 1000, &[10, 10]).unwrap() else {
        panic!()
    };
    assert!(
        chunked.len() == 10 && chunked.iter().all(|b| b.counts == [10, 10]),
        "{chunked:?}"
    );
    let ArrayPlan::Blocks(ignored) = plan(&[100, 10], 8, 1000, &[50, 10]).unwrap() else {
        panic!()
    };
    assert_eq!(
        ignored.len(),
        9,
        "a chunking bigger than a message is not followed"
    );

    assert!(
        plan(&[10], 2000, 1000, &[])
            .unwrap_err()
            .0
            .contains("one element")
    );
}

/// Whatever the shape and the budget, the blocks cover every element once,
/// and none is over the budget - counted element by element, not trusted
/// from the plan.
#[test]
fn blocks_cover_the_array_exactly_once() {
    let shapes: Vec<Vec<u64>> = vec![
        vec![1],
        vec![7],
        vec![3, 5],
        vec![5, 3],
        vec![2, 3, 4],
        vec![4, 1, 6],
        vec![1, 7, 1],
    ];
    for dimensions in &shapes {
        for element in [1u64, 2, 8] {
            for budget in [8u64, 9, 13, 24, 40, 1000] {
                let Ok(planned) = plan(dimensions, element, budget, &[]) else {
                    assert!(element > budget);
                    continue;
                };
                let blocks = match planned {
                    ArrayPlan::Whole => vec![Block {
                        starts: vec![0; dimensions.len()],
                        counts: dimensions.clone(),
                    }],
                    ArrayPlan::Blocks(blocks) => blocks,
                };
                let total: u64 = dimensions.iter().product();
                let mut seen = vec![0u32; total as usize];
                for block in &blocks {
                    assert!(
                        block.len() * element <= budget,
                        "{dimensions:?} {budget}: {block:?}"
                    );
                    for index in block_indices(dimensions, &block.starts, &block.counts) {
                        seen[index as usize] += 1;
                    }
                }
                assert!(
                    seen.iter().all(|n| *n == 1),
                    "{dimensions:?} element {element} budget {budget}: {seen:?}"
                );
            }
        }
    }
}

/// The metadata first, then each array as it fits: whole, or by sub-arrays
/// the store accepts - its `MaxDataArraySize` is the smallest allowed, and it
/// answers `ELIMIT_EXCEEDED` beyond. Every value lands where it belongs, in
/// doubles, in Avro ints, in bytes; a missing array and a store that sends
/// the wrong block are each said, the others read.
#[test]
fn arrays_are_read_by_metadata_then_by_what_fits() {
    let mut client = open(false, None);
    let paths = [
        "/small",
        "/large",
        "/ints",
        "/bytes",
        "/chunked",
        "/missing",
        "/liar",
        "/booleans",
        "/short",
    ];
    let ids: Vec<DataArrayIdentifier> = paths.iter().map(|p| id(p)).collect();
    let mut planned = BTreeMap::new();
    let arrays = read_arrays(&mut client, &ids, 16 * 1024 * 1024, |event| {
        if let ArrayProgress::Planned { path, blocks, .. } = event {
            planned.insert(path, blocks);
        }
    })
    .unwrap();

    let doubles = |n: usize| ArrayValues::Doubles((0..n).map(|i| i as f64).collect());
    let small = arrays[0].as_ref().unwrap();
    assert_eq!((small.dimensions.clone(), small.blocks), (vec![3, 4], 1));
    assert_eq!(small.values, doubles(12));

    let large = arrays[1].as_ref().unwrap();
    assert_eq!(large.values, doubles(30_000), "the blocks are put back in place");
    assert_eq!(large.blocks, 3, "240 000 bytes in blocks of at most 100 000");

    let ints = arrays[2].as_ref().unwrap();
    assert_eq!(ints.values, ArrayValues::Ints((0..50_000).collect()));
    assert_eq!(
        ints.blocks, 3,
        "arrayOfInt is budgeted at 5 bytes a value: 20 000 a block"
    );

    let bytes = arrays[3].as_ref().unwrap();
    let expected: Vec<u8> = (0..60_000i32).flat_map(|i| (i as i16).to_le_bytes()).collect();
    assert_eq!(bytes.values, ArrayValues::Bytes(expected));
    assert_eq!(bytes.blocks, 2);

    let chunked = arrays[4].as_ref().unwrap();
    assert_eq!(
        (chunked.blocks, &chunked.values),
        (30, &doubles(30_000)),
        "the store's chunks of 10 rows"
    );

    assert!(
        matches!(&arrays[5], Err(ArrayFailure::Store(ErrorInfo { code, .. })) if *code == EtpError::ENOT_FOUND.code())
    );
    assert!(
        matches!(&arrays[6], Err(ArrayFailure::Inconsistent(why)) if why.contains("asked")),
        "{:?}",
        arrays[6]
    );
    assert_eq!(planned.get("/large"), Some(&3));

    let booleans = arrays[7].as_ref().unwrap();
    assert_eq!(
        (booleans.logical, &booleans.values),
        (AnyLogicalArrayType::ArrayOfDouble64Le, &doubles(12)),
        "read as its transport says"
    );
    assert!(
        matches!(&arrays[8], Err(ArrayFailure::Inconsistent(why)) if why.contains("11 came")),
        "{:?}",
        arrays[8]
    );
}

/// A region - eml 2.3's `StartIndex` and `Count` - is read by sub-arrays
/// and comes back with the region's shape, every value its own: whole when it
/// fits a message, by blocks when it does not. A region outside the array is
/// refused, not clipped.
#[test]
fn a_region_is_read_as_its_start_and_count_say() {
    let mut client = open(false, None);
    let regions = [
        Block {
            starts: vec![100, 10],
            counts: vec![150, 20],
        },
        Block {
            starts: vec![0, 0],
            counts: vec![300, 50],
        },
        Block {
            starts: vec![290, 0],
            counts: vec![20, 100],
        },
    ];
    let asked: Vec<_> = regions.iter().map(|r| (id("/large"), Some(r.clone()))).collect();
    let read = read_regions(&mut client, &asked, 16 * 1024 * 1024, |_| {}).unwrap();
    for (region, answer) in regions.iter().zip(&read).take(2) {
        let array = answer.as_ref().unwrap();
        assert_eq!(array.dimensions, region.counts);
        let expected: Vec<f64> = block_indices(&[300, 100], &region.starts, &region.counts)
            .into_iter()
            .map(|i| i as f64)
            .collect();
        assert_eq!(array.values, ArrayValues::Doubles(expected), "{region:?}");
    }
    assert_eq!(read[0].as_ref().unwrap().blocks, 1, "24 000 bytes: one sub-array");
    assert_eq!(read[1].as_ref().unwrap().blocks, 2, "120 000 bytes: two");
    assert!(
        matches!(&read[2], Err(ArrayFailure::Plan(why)) if why.contains("not inside")),
        "{:?}",
        read[2]
    );
}

/// Blocks are grouped into requests only while they fit one message: the
/// `MaxDataArraySize` limits **one array** (§13.2.3), not a message: the
/// three blocks `/large` is cut into ride together when a message can carry
/// them, and go one by one when it cannot - the same blocks either way, one
/// round trip or three.
#[test]
fn blocks_are_grouped_only_while_they_fit_a_message() {
    let requests = |carrying: Option<i64>| {
        let mut client = open_carrying(false, None, carrying);
        let (mut requests, mut blocks) = (0, 0);
        let read = read_arrays(
            &mut client,
            &[id("/large")],
            16 * 1024 * 1024,
            |event| match event {
                ArrayProgress::Read { total, .. } => requests = total,
                ArrayProgress::Planned { blocks: n, .. } => blocks = n,
                ArrayProgress::Written { .. } => unreachable!("this reads"),
            },
        )
        .unwrap();
        assert!(read[0].is_ok(), "{read:?}");
        (blocks, requests)
    };
    assert_eq!(
        requests(None),
        (3, 1),
        "three blocks of the store's limit, one message"
    );
    assert_eq!(
        requests(Some(150_000)),
        (3, 3),
        "the same three, a message holding but one"
    );
}

/// Bytes are numbers once their logical type is known - its size, its sign,
/// its byte order -, worked out here by hand.
#[test]
fn bytes_are_read_as_their_logical_type_says() {
    use AnyLogicalArrayType as L;
    let bytes = ArrayValues::Bytes(vec![0x01, 0x00, 0xFF, 0xFF]);
    assert_eq!(bytes.to_f64s(L::ArrayOfInt16Le), Some(vec![1.0, -1.0]));
    assert_eq!(bytes.to_f64s(L::ArrayOfUInt16Le), Some(vec![1.0, 65535.0]));
    assert_eq!(bytes.to_f64s(L::ArrayOfInt16Be), Some(vec![256.0, -1.0]));
    assert_eq!(bytes.to_f64s(L::ArrayOfInt32Le), Some(vec![-65535.0]));
    let one = ArrayValues::Bytes(vec![0x00, 0x00, 0x80, 0x3F]);
    assert_eq!(one.to_f64s(L::ArrayOfFloat32Le), Some(vec![1.0]));
    assert_eq!(
        ArrayValues::Bytes(vec![0x3F, 0x80, 0x00, 0x00]).to_f64s(L::ArrayOfFloat32Be),
        Some(vec![1.0])
    );
    assert_eq!(
        ArrayValues::Strings(vec!["a".into()]).to_f64s(L::ArrayOfString),
        None
    );
}

/// The logical and transport types go together as §13.2.2.1's table says -
/// sampled row by row. A pair the table forbids is read as the transport
/// means when tolerant, and refused when strict or when the transport is
/// `bytes`, which means nothing by itself.
#[test]
fn a_type_its_transport_cannot_carry_is_read_as_the_transport_says() {
    use AnyArrayType as T;
    use AnyLogicalArrayType as L;
    assert_eq!(
        allowed_transports(L::ArrayOfInt32Le),
        [T::Bytes, T::ArrayOfInt, T::ArrayOfLong]
    );
    assert_eq!(allowed_transports(L::ArrayOfInt64Le), [T::Bytes, T::ArrayOfLong]);
    assert_eq!(allowed_transports(L::ArrayOfDouble64Le), [T::ArrayOfDouble]);
    assert_eq!(allowed_transports(L::ArrayOfFloat32Be), [T::Bytes]);
    assert_eq!(allowed_transports(L::ArrayOfBoolean), [T::ArrayOfBoolean]);
    assert!(allowed_transports(L::ArrayOfCustom).is_empty());

    assert_eq!(
        reading_type("/a", L::ArrayOfInt32Le, T::ArrayOfLong, Tolerance::Strict),
        Ok(L::ArrayOfInt32Le)
    );
    assert_eq!(
        reading_type("/a", L::ArrayOfBoolean, T::ArrayOfDouble, Tolerance::Tolerant),
        Ok(L::ArrayOfDouble64Le)
    );
    assert!(matches!(
        reading_type("/a", L::ArrayOfBoolean, T::ArrayOfDouble, Tolerance::Strict),
        Err(ArrayFailure::Inconsistent(_))
    ));
    assert!(
        matches!(reading_type("/a", L::ArrayOfDouble64Le, T::Bytes, Tolerance::Tolerant), Err(ArrayFailure::Inconsistent(why)) if why.contains("13.2.2.1"))
    );
}

/// Asked for, an `Acknowledge` comes back for each request, named, before
/// the answer - and it answers nothing: the request still gets its answer.
/// Not asked for, none comes.
#[test]
fn acknowledgements_are_asked_and_heard() {
    let mut client = open(false, None);
    client.ask_acknowledge(true);
    let listed = list_resources(&mut client, &ResourceQuery::new("eml:///")).unwrap();
    assert_eq!(listed.resources.len(), 2, "the answer still came");
    let heard = client.acknowledgements();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert_eq!(heard[0].name, "Discovery.GetResources");

    client.ask_acknowledge(false);
    list_resources(&mut client, &ResourceQuery::new("eml:///")).unwrap();
    assert!(client.acknowledgements().is_empty());
}

/// Idle, a client listens: the keepalive `Ping` goes out when it is due and
/// its `Pong` is taken in - the watermark moves to the server's clock - and
/// the session is still there for the next request.
#[test]
fn an_idle_client_keeps_its_session() {
    let mut client = open(false, None);
    let opened = client.watermark();
    client.idle(Duration::from_secs(40)).unwrap();
    client.idle(Duration::from_secs(1)).unwrap();
    assert_eq!(
        client.watermark(),
        Some(T0 + 40 * S),
        "a Pong came back: was {opened:?}"
    );
    assert!(client.unsolicited().is_empty());
    assert_eq!(
        list_resources(&mut client, &ResourceQuery::new("eml:///"))
            .unwrap()
            .resources
            .len(),
        2
    );
}

#[test]
fn transport_is_a_trait_and_the_tests_use_a_fake() {
    let mut client = open(false, None);
    assert_eq!(client.negotiated().session_id, Uuid([0xAB; 16]));
    let listed = list_resources(&mut client, &ResourceQuery::new("eml:///")).unwrap();
    assert_eq!(listed.resources.len(), 2);
    assert_eq!(listed.resources[0].name, "Top Brent");
    assert_eq!(listed.ended_by, None);
    client.close("done").unwrap();
}

#[test]
fn objects_come_back_each_with_its_own_answer() {
    let mut client = open(false, None);
    let uris = vec![
        "eml:///resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)".to_string(),
        "eml:///resqml20.obj_HorizonInterpretation(00000000-0000-0000-0000-000000000000)".to_string(),
    ];
    let answers = get_data_objects(&mut client, &uris, "xml").unwrap();
    assert_eq!(answers[&uris[0]].as_ref().unwrap().data, OBJECT);
    let missing = answers[&uris[1]].as_ref().unwrap_err();
    assert_eq!(
        missing.code,
        EtpError::ENOT_FOUND.code(),
        "one error for the one missing, not for the batch"
    );
}

#[test]
fn a_non_canonical_uri_is_refused_before_the_server_is_asked() {
    let mut client = open(false, None);
    let refused = list_resources(
        &mut client,
        &ResourceQuery::new("eml:///resqml20.obj_X(uuid=421a7a05-033a-450d-bcef-051352023578)"),
    );
    assert!(
        matches!(refused, Err(ClientError::Invalid(incident)) if incident.code() == EtpError::EINVALID_URI.code())
    );
}

#[test]
fn response_timeout_is_a_named_refusal() {
    let mut client = open(true, None);
    let refused = list_resources(&mut client, &ResourceQuery::new("eml:///")).unwrap_err();
    // The session outlives one request that timed out: the shell keeps it.
    assert!(!refused.is_connection_lost());
    let ClientError::TimedOut { incident, .. } = refused else {
        panic!("{refused:?}");
    };
    assert_eq!(incident.code(), EtpError::ETIMED_OUT.code());
    assert!(incident.message().contains("300 s"), "{}", incident.message());
}

#[test]
fn interrupted_write_is_neither_success_nor_failure() {
    // The RequestSession is the first message out, the PutDataObjects the
    // second - and the connection falls right after it.
    let mut client = open(true, Some(2));
    let written = client.call(&PutDataObjects::default()).unwrap_err();
    assert!(
        matches!(
            written,
            ClientError::Interrupted {
                sent: true,
                confirmed: false,
                ..
            }
        ),
        "an interrupted write is neither Ok nor a plain failure: {written:?}"
    );
    // And the connection is gone: the shell opens a new session.
    assert!(written.is_connection_lost());
}

#[test]
fn server_profile_changes_no_protocol_code() {
    let settings = HandshakeSettings::default();
    let base = probe::handshake("wss://rddms.example/api/etp", Profile::Native, &settings).unwrap();
    let standard = ServerProfile::Standard.adapt(base.clone());
    let rddms = ServerProfile::Rddms {
        data_partition: "opendes".into(),
    }
    .adapt(base.clone());
    assert_eq!(standard, base, "the standard profile adds nothing");
    let added: Vec<_> = rddms
        .headers
        .iter()
        .filter(|h| !base.headers.contains(h))
        .collect();
    assert_eq!(added, [&("data-partition-id".to_string(), "opendes".to_string())]);
    assert_eq!(
        (rddms.url.as_str(), &rddms.subprotocols),
        (base.url.as_str(), &base.subprotocols)
    );

    // A browser sets no header: the partition goes in the query string, as
    // ETP's own attributes do (§4.3.2), and no header appears.
    let browser = probe::handshake("wss://rddms.example/api/etp", Profile::Browser, &settings).unwrap();
    let rddms = ServerProfile::Rddms {
        data_partition: "open des".into(),
    }
    .adapt(browser.clone());
    assert!(rddms.headers.is_empty(), "{:?}", rddms.headers);
    assert_eq!(rddms.url, format!("{}&data-partition-id=open%20des", browser.url));
    // And nothing past the handshake takes a profile: `Client::open` has no
    // parameter for one - which is the property, held by the type.
}

#[test]
fn plain_ws_to_a_remote_host_is_said() {
    for local in [
        "ws://localhost:9002",
        "ws://127.0.0.1/etp",
        "ws://[::1]:9002/x",
        "wss://anything.example",
    ] {
        assert_eq!(plain_text_warning(local), None, "{local}");
    }
    for remote in [
        "ws://etp.example.org/etp",
        "ws://10.0.0.4:9002",
        "ws://user@host.example:80",
    ] {
        let warning = plain_text_warning(remote).unwrap_or_else(|| panic!("{remote} passed without a word"));
        assert!(warning.contains("wss://"), "{warning}");
    }
}

// ---------------------------------------------------------------------------
// The client for whoever cannot block
// ---------------------------------------------------------------------------

/// What the front end's socket would be: the messages the client sent, kept for
/// the test to carry.
#[derive(Clone, Default)]
struct Outbox(
    std::rc::Rc<std::cell::RefCell<Vec<Vec<u8>>>>,
    std::rc::Rc<std::cell::Cell<bool>>,
);

impl Outbox {
    fn take(&self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.0.borrow_mut())
    }
}

impl Sink for Outbox {
    fn send(&mut self, message: &[u8]) -> Result<(), TransportError> {
        if self.1.get() {
            return Err(TransportError::new("the socket refuses to send"));
        }
        self.0.borrow_mut().push(message.to_vec());
        Ok(())
    }
    fn close(&mut self) {}
}

/// The test harness: a store in memory, and a client fed by hand.
struct Harness {
    client: AsyncClient<ManualClock>,
    outbox: Outbox,
    server: Connection,
    clock: ManualClock,
}

impl Harness {
    fn new(
        authorizer: Option<Box<dyn retp_proto::connection::Authorizer>>,
        credentials: Option<Credentials>,
    ) -> Self {
        Self::with(authorizer, credentials, false)
    }

    fn with(
        authorizer: Option<Box<dyn retp_proto::connection::Authorizer>>,
        credentials: Option<Credentials>,
        defer: bool,
    ) -> Self {
        let clock = ManualClock::new(T0);
        let outbox = Outbox::default();
        let (offer, handlers) =
            reading_client("test harness", "0.1", Uuid([1; 16]), &["resqml20.*".to_string()]);
        let handlers: Vec<Box<dyn Handler>> = handlers
            .into_iter()
            .filter(|h| h.offer().protocol != 24)
            .collect();
        let client = AsyncClient::start(
            Box::new(outbox.clone()),
            clock.clone(),
            offer,
            handlers,
            credentials,
        )
        .unwrap();
        Harness {
            client,
            outbox,
            server: server_deferring(false, false, authorizer, defer, None, T0),
            clock,
        }
    }

    /// Carry what the client sent to the store, and the store's answers
    /// back, in the order of the wire - a peer's message ids only grow
    /// (§3.5.4.1 §5.b). How many messages crossed.
    fn carry(&mut self) -> usize {
        let sent = self.outbox.take();
        let mut replies = Vec::new();
        for message in &sent {
            replies.extend(
                self.server
                    .receive(message, self.clock.now())
                    .send
                    .into_iter()
                    .map(|o| o.bytes),
            );
        }
        for reply in &replies {
            self.client.received(reply);
        }
        sent.len() + replies.len()
    }

    /// Poll `future` to its end, carrying messages whenever it waits. A
    /// future that waits with nothing left to carry would wait for ever: that
    /// is a failure, said at once.
    fn drive<F: std::future::Future>(&mut self, future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        loop {
            if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                return output;
            }
            assert!(self.carry() > 0, "the future waits, and nothing is left to carry");
        }
    }

    fn opened(mut self) -> Self {
        let client = self.client.clone();
        self.drive(client.opened()).unwrap();
        self
    }
}

/// Counts its wakes.
struct Wakes(std::sync::atomic::AtomicUsize);

impl std::task::Wake for Wakes {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

fn poll_once<F: std::future::Future>(future: F) -> Option<F::Output> {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    {
        std::task::Poll::Ready(output) => Some(output),
        std::task::Poll::Pending => None,
    }
}

/// Written once, the operations answer the same on either client: a
/// listing, objects by URI, and an array read by blocks (`/large` does not
/// fit one message) - asked of the blocking client, and of the one a front end
/// feeds by hand.
#[test]
fn the_asynchronous_client_reads_as_the_blocking_one() {
    let mut blocking = open(false, None);
    let mut harness = Harness::new(None, None).opened();
    let mut client = harness.client.clone();

    let query = ResourceQuery::new("eml:///");
    let listed = harness
        .drive(asynchronous::list_resources(&mut client, &query))
        .unwrap();
    assert_eq!(listed, list_resources(&mut blocking, &query).unwrap());
    assert_eq!(listed.resources.len(), 2, "not two empty answers agreeing");

    let uris = vec![
        "eml:///resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)".to_string(),
        "eml:///resqml20.obj_LocalDepth3dCrs(00000000-0000-0000-0000-000000000000)".to_string(),
    ];
    let got = harness
        .drive(asynchronous::get_data_objects(&mut client, &uris, "xml"))
        .unwrap();
    assert_eq!(got, get_data_objects(&mut blocking, &uris, "xml").unwrap());

    let mut blocks = 0;
    let read = harness
        .drive(asynchronous::read_arrays(
            &mut client,
            &[id("/large")],
            16 * 1024 * 1024,
            |event| {
                if let ArrayProgress::Planned { blocks: n, .. } = event {
                    blocks = n;
                }
            },
        ))
        .unwrap();
    assert!(
        blocks > 1,
        "read by blocks, as the blocking one reads it: {blocks}"
    );
    assert_eq!(
        read,
        read_arrays(&mut blocking, &[id("/large")], 16 * 1024 * 1024, |_| {}).unwrap()
    );
}

/// Two requests out at once, and the store answers the second first - ETP
/// allows it, its message ids still growing: each is answered with its own.
/// The answers are gathered by request, never by who came first.
#[test]
fn requests_in_flight_are_answered_each_to_its_own() {
    let mut harness = Harness::with(None, None, true).opened();
    let (mut a, mut b) = (harness.client.clone(), harness.client.clone());
    let held = "eml:///resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)".to_string();
    let lost = "eml:///resqml20.obj_LocalDepth3dCrs(00000000-0000-0000-0000-000000000000)".to_string();
    let mut first = std::pin::pin!(asynchronous::get_data_objects(
        &mut a,
        std::slice::from_ref(&held),
        "xml"
    ));
    let mut second = std::pin::pin!(asynchronous::get_data_objects(
        &mut b,
        std::slice::from_ref(&lost),
        "xml"
    ));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(second.as_mut().poll(&mut cx).is_pending());
    assert_eq!(
        harness.outbox.0.borrow().len(),
        2,
        "both requests are out before any answer"
    );

    harness.carry();
    let std::task::Poll::Ready(second) = second.as_mut().poll(&mut cx) else {
        panic!("the second answer came")
    };
    let std::task::Poll::Ready(first) = first.as_mut().poll(&mut cx) else {
        panic!("the first answer came")
    };
    assert_eq!(
        first.unwrap()[&held].as_ref().map(|o| o.data.clone()),
        Ok(OBJECT.to_vec())
    );
    assert_eq!(
        second.unwrap()[&lost].as_ref().map_err(|e| e.code),
        Err(11),
        "ENOT_FOUND, its own"
    );
}

/// A request waits, and nothing but a message or the time wakes it; its
/// answer ends it.
#[test]
fn a_request_is_woken_by_its_answer() {
    let mut harness = Harness::new(None, None).opened();
    let wakes = std::sync::Arc::new(Wakes(std::sync::atomic::AtomicUsize::new(0)));
    let waker = std::task::Waker::from(wakes.clone());
    let mut cx = std::task::Context::from_waker(&waker);
    let mut client = harness.client.clone();
    let query = ResourceQuery::new("eml:///");
    let mut listing = std::pin::pin!(asynchronous::list_resources(&mut client, &query));

    assert!(listing.as_mut().poll(&mut cx).is_pending());
    assert_eq!(wakes.0.load(std::sync::atomic::Ordering::Relaxed), 0);
    harness.client.tick();
    assert_eq!(
        wakes.0.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "the time passing wakes the waiting, who look"
    );
    assert!(
        listing.as_mut().poll(&mut cx).is_pending(),
        "and wait again: the answer is not there"
    );
    harness.carry();
    assert!(
        wakes.0.load(std::sync::atomic::Ordering::Relaxed) >= 2,
        "the answer woke it"
    );
    assert!(matches!(
        listing.as_mut().poll(&mut cx),
        std::task::Poll::Ready(Ok(_))
    ));
}

/// The socket falls with two requests out: both end `Interrupted`, sent,
/// their outcome unknown (#201) - and the next one is refused at once, the
/// connection being gone.
#[test]
fn a_lost_socket_ends_every_request_in_flight() {
    let harness = Harness::new(None, None).opened();
    let (mut a, mut b) = (harness.client.clone(), harness.client.clone());
    let query = ResourceQuery::new("eml:///");
    let mut first = std::pin::pin!(asynchronous::list_resources(&mut a, &query));
    let mut second = std::pin::pin!(asynchronous::list_resources(&mut b, &query));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(first.as_mut().poll(&mut cx).is_pending() && second.as_mut().poll(&mut cx).is_pending());

    harness.client.lost(TransportError::new("the socket closed"));
    for future in [first.as_mut(), second.as_mut()] {
        match future.poll(&mut cx) {
            std::task::Poll::Ready(Err(ClientError::Interrupted { sent, confirmed, .. })) => {
                assert!(sent && !confirmed)
            }
            other => panic!("{other:?}"),
        }
    }
    let mut c = harness.client.clone();
    let refused = poll_once(asynchronous::list_resources(&mut c, &query));
    assert!(
        matches!(&refused, Some(Err(e)) if e.is_connection_lost()),
        "{refused:?}"
    );
}

/// A request the socket would not send ends `Interrupted`, **not sent** - so
/// that a write in that state could be sent again without being sent twice
/// (#201) - and the connection is gone.
#[test]
fn a_request_the_socket_refused_is_known_unsent() {
    let harness = Harness::new(None, None).opened();
    harness.outbox.1.set(true);
    let mut client = harness.client.clone();
    match poll_once(asynchronous::list_resources(
        &mut client,
        &ResourceQuery::new("eml:///"),
    )) {
        Some(Err(ClientError::Interrupted { sent, confirmed, .. })) => assert!(!sent && !confirmed),
        other => panic!("{other:?}"),
    }
}

/// A front end cannot set a header, so it authorizes in the session (§4.1.4.1):
/// `RequestSession`, refused, `Authorize`, `RequestSession` again - driven by
/// hand, as the blocking client drives it.
#[test]
fn a_page_authorizes_inside_the_session() {
    let seen = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = seen.clone();
    let authorizer = Box::new(move |authorization: &str, _now: i64| {
        counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if authorization == "Bearer good" {
            Authorization::Granted { expires_at: None }
        } else {
            Authorization::Challenge(vec!["Bearer".into()])
        }
    }) as Box<dyn retp_proto::connection::Authorizer>;
    let mut harness = Harness::new(Some(authorizer), Some(Credentials::bearer("good").unwrap())).opened();
    assert_eq!(
        seen.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "one Authorize"
    );
    let mut client = harness.client.clone();
    let listed = harness.drive(asynchronous::list_resources(
        &mut client,
        &ResourceQuery::new("eml:///"),
    ));
    assert_eq!(listed.unwrap().resources.len(), 2);
}

// ---------------------------------------------------------------------------
// The REST transport
// ---------------------------------------------------------------------------

const REST_BASE: &str = "https://rddms.example/api/reservoir-ddms/v2";
const DEMO: &str = "eml:///dataspace('demo')";

/// A URI of the test store, in the dataspace `demo` - the REST API names
/// every dataspace.
fn in_demo(object: &str) -> String {
    format!("{DEMO}/{object}")
}

fn demo_id(path: &str) -> DataArrayIdentifier {
    DataArrayIdentifier {
        uri: in_demo("eml20.obj_EpcExternalPartReference(421a7a05-033a-450d-bcef-051352023578)"),
        path_in_resource: path.into(),
    }
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            out.push(u8::from_str_radix(&text[i + 1..i + 3], 16).unwrap());
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

/// An Avro enumeration's ordinal, as the REST API gives it.
fn ordinal<E: Avro>(value: &E) -> i64 {
    let mut out = retp_avro::Writer::new();
    value.write(&mut out);
    i64::from(Reader::new(&out.finish()).int().unwrap())
}

/// The Reservoir DDMS REST API over **the test store's own data** - the same
/// resources, the same object, the same arrays its ETP handler serves -, so
/// that one operation can be asked by both transports and compared. Answers
/// as the API's description (`rc/rddms-rest/`) says, `<DataObjects>` wrapper
/// included; records each URL asked.
#[derive(Clone, Default)]
struct FakeRest {
    asked: std::rc::Rc<std::cell::RefCell<Vec<String>>>,
    /// Every answer has this status.
    status: Option<u16>,
    /// Replayed as is for an array path: (metadata, values) as one OSDU service answered.
    recorded: Option<(&'static str, &'static str, &'static str)>,
    /// More objects held, each by its uuid and whether the API can say it in
    /// JSON: one that cannot empties every JSON answer that asks it - or,
    /// told `referencedContent=false`, fails it with a 500 -, as one OSDU service's does.
    extra: Vec<(&'static str, bool)>,
    /// Instead, leave out of a JSON answer only the object it cannot say -
    /// what another server might do.
    omits: bool,
    /// A server with **no plural read**: one RDDMS REST API carries the
    /// Reservoir DDMS shapes under `/etp` and not that endpoint, so the whole of it answers 404 and the objects have to be
    /// asked one at a time.
    no_plural: bool,
    /// Every handful of requests the client handed over at once, and how
    /// many of them it allowed to travel at the same time.
    batches: std::rc::Rc<std::cell::RefCell<Vec<(usize, usize)>>>,
    /// Every write, as `(method, path with its query, body)` - what the
    /// tests of the writes read back.
    written: std::rc::Rc<std::cell::RefCell<Vec<(String, String, serde_json::Value)>>>,
    /// Answer every `PUT` with `200 false` - the definition's way of refusing
    /// a write without a status.
    says_false: bool,
    /// `POST …/transactions` answers `null`: the route exists and opens
    /// nothing, as some do.
    null_transaction: bool,
}

/// An extra object's XML, as the API returns it: nested, with a comment and
/// a quoted `/>` for the scanner that cuts `<DataObjects>` to step over.
fn extra_xml(uuid: &str) -> String {
    format!(
        "<resqml20:Grid2dRepresentation xmlns:resqml20=\"http://www.energistics.org/energyml/data/resqmlv2\" uuid=\"{uuid}\" note=\"1 /> 0\">\
         <!-- </resqml20:Grid2dRepresentation> --><eml20:Citation xmlns:eml20=\"http://www.energistics.org/energyml/data/commonv2\">\
         <eml20:Title>{uuid}</eml20:Title></eml20:Citation></resqml20:Grid2dRepresentation>"
    )
}

fn json_response(status: u16, json: serde_json::Value) -> HttpResponse {
    HttpResponse {
        status,
        challenge: (status == 401).then(|| "Bearer".to_string()),
        body: serde_json::to_vec(&json).unwrap(),
    }
}

impl Http for FakeRest {
    fn send(
        &mut self,
        request: HttpRequest,
    ) -> impl std::future::Future<Output = Result<HttpResponse, TransportError>> {
        std::future::ready(Ok(self.answer(request)))
    }

    /// This fake runs nothing at the same time; it only notes what it was
    /// given at once, and what it was allowed to do with it.
    fn send_all(
        &mut self,
        requests: Vec<HttpRequest>,
        at_once: usize,
    ) -> impl std::future::Future<Output = Vec<Result<HttpResponse, TransportError>>> {
        self.batches.borrow_mut().push((requests.len(), at_once));
        std::future::ready(
            requests
                .into_iter()
                .map(|request| Ok(self.answer(request)))
                .collect(),
        )
    }
}

impl BlockingHttp for FakeRest {}

impl FakeRest {
    fn answer(&self, request: HttpRequest) -> HttpResponse {
        use serde_json::json;
        self.asked.borrow_mut().push(request.url.clone());
        if let Some(status) = self.status {
            return json_response(status, json!({"statusCode": status, "message": "as told"}));
        }
        let rest = request.url.strip_prefix(REST_BASE).expect("a URL of the API");
        let (path, query) = rest.split_once('?').unwrap_or((rest, ""));
        let params: Vec<(String, String)> = query
            .split('&')
            .filter(|p| !p.is_empty())
            .map(|p| {
                let (k, v) = p.split_once('=').unwrap_or((p, ""));
                (percent_decode(k), percent_decode(v))
            })
            .collect();
        let param = |name: &str| params.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone());
        let many = |name: &str| -> Vec<u64> {
            params
                .iter()
                .filter(|(k, _)| k == name)
                .map(|(_, v)| v.parse().unwrap())
                .collect()
        };
        let segments: Vec<String> = path.split('/').skip(1).map(percent_decode).collect();
        let segments: Vec<&str> = segments.iter().map(String::as_str).collect();
        let resource = |object: &str, name: &str| {
            json!({
                "uri": in_demo(object), "alternateUris": [], "name": name,
                "lastChanged": "2025-07-31T15:15:36.000Z", "storeLastWrite": "2025-07-31T15:15:38.151Z",
                "storeCreated": "2025-07-31T15:15:38.151Z", "activeStatus": "Active", "customData": {}
            })
        };
        let record = |what: &str| {
            let body = request
                .body
                .as_deref()
                .map(|b| serde_json::from_slice(b).unwrap_or(json!(null)))
                .unwrap_or(json!(null));
            self.written
                .borrow_mut()
                .push((what.to_string(), rest.to_string(), body));
        };
        match segments.as_slice() {
            ["dataspaces"] if request.method == Method::Post => {
                record("POST");
                json_response(200, json!({"success": {}}))
            }
            ["dataspaces", "demo", "resources"] if request.method == Method::Put => {
                record("PUT");
                json_response(200, json!(!self.says_false))
            }
            ["dataspaces", "demo", "resources", "arrays"] if request.method == Method::Put => {
                record("PUT");
                json_response(200, json!(!self.says_false))
            }
            ["dataspaces", "demo", "resources", _, _] if request.method == Method::Delete => {
                record("DELETE");
                json_response(200, json!({}))
            }
            ["dataspaces", "demo"] if request.method == Method::Delete => {
                record("DELETE");
                json_response(200, json!({}))
            }
            ["dataspaces", "demo", "transactions"] if request.method == Method::Post => {
                record("POST");
                if self.null_transaction {
                    return json_response(200, json!(null));
                }
                json_response(200, json!("7b1f2a9c-3d4e-4f50-8a6b-1c2d3e4f5a6b"))
            }
            ["dataspaces", "demo", "transactions", _] if request.method == Method::Put => {
                record("PUT");
                json_response(200, json!(""))
            }
            ["dataspaces", "demo", "transactions", _] if request.method == Method::Delete => {
                record("DELETE");
                json_response(200, json!(""))
            }
            ["dataspaces"] => json_response(200, json!([{
                "uri": DEMO, "path": "demo", "storeLastWrite": "2025-07-31T15:15:38.151Z",
                "storeCreated": "2025-07-31T15:15:38.151Z", "customData": {}
            }])),
            ["dataspaces", "demo", kind @ ("resources" | "graph"), "all"] => {
                let skip: usize = param("$skip").map_or(0, |s| s.parse().unwrap());
                let top: usize = param("$top").map_or(1000, |s| s.parse().unwrap());
                let page: Vec<serde_json::Value> =
                    RESOURCES.iter().skip(skip).take(top).map(|(object, name)| resource(object, name)).collect();
                if *kind == "graph" {
                    let links = if skip == 0 {
                        json!([{"source": in_demo(RESOURCES[0].0), "target": in_demo(RESOURCES[1].0), "path": "rsq:LocalCrs"}])
                    } else {
                        json!([])
                    };
                    json_response(200, json!({"resources": page, "links": links}))
                } else {
                    json_response(200, serde_json::Value::Array(page))
                }
            }
            ["dataspaces", "demo", "resources"] => json_response(200, json!([{"name": "resqml20.obj_HorizonInterpretation", "count": 1}])),
            ["dataspaces", "demo", "resources", _, uuid, "arrays", array, "metadata"] => {
                if let Some((recorded, metadata, _)) = self.recorded
                    && *array == recorded
                {
                    return HttpResponse { status: 200, challenge: None, body: metadata.as_bytes().to_vec() };
                }
                let Some(stored) = stored_array(array) else {
                    return json_response(404, json!({"statusCode": 404, "message": "no such array"}));
                };
                let meta = stored.metadata();
                json_response(200, json!({
                    "uid": {"uri": in_demo(&format!("eml20.obj_EpcExternalPartReference({uuid})")), "pathInResource": array},
                    "dimensions": meta.dimensions, "preferredSubarrayDimensions": meta.preferred_subarray_dimensions,
                    "logicalArrayType": ordinal(&meta.logical_array_type), "transportArrayType": ordinal(&meta.transport_array_type),
                    "storeLastWrite": "1970-01-01T00:00:00.000Z", "storeCreated": "1970-01-01T00:00:00.000Z", "customData": {}
                }))
            }
            ["dataspaces", "demo", "resources", _, _, "arrays", array] => {
                if let Some((recorded, _, values)) = self.recorded
                    && *array == recorded
                {
                    return HttpResponse { status: 200, challenge: None, body: values.as_bytes().to_vec() };
                }
                let stored = stored_array(array).expect("an array of the store");
                let (starts, counts) = (many("starts"), many("counts"));
                let (starts, counts) = if starts.is_empty() {
                    (vec![0; stored.dimensions.len()], stored.dimensions.clone())
                } else {
                    (starts, counts)
                };
                let block = match stored.block(&starts, &counts) {
                    Ok(block) => block,
                    Err(incident) => return json_response(400, json!({"statusCode": 400, "message": incident.message()})),
                };
                let base64 = param("format").as_deref() == Some("base64");
                let data = match (&block.data.item, base64) {
                    (AnyArrayItem::ArrayOfDouble(a), true) => json!(b64(a.values.iter().flat_map(|v| v.to_le_bytes()))),
                    (AnyArrayItem::ArrayOfInt(a), true) => json!(b64(a.values.iter().flat_map(|v| v.to_le_bytes()))),
                    (AnyArrayItem::Bytes(raw), true) => json!(b64(raw.iter().copied())),
                    (AnyArrayItem::ArrayOfDouble(a), false) => json!(a.values),
                    (AnyArrayItem::ArrayOfInt(a), false) => json!(a.values),
                    (other, _) => panic!("not in this fake: {other:?}"),
                };
                json_response(200, json!({"uid": {}, "data": {"data": data, "dimensions": block.dimensions}}))
            }
            // One object, as a server without the plural read serves it -
            // wrapped in the same `<DataObjects>` its plural answers use,
            // which is what one serializer on the server side gives. A server
            // that sends it bare is read just as well: the scanner takes the
            // document itself when there is no wrapper.
            ["dataspaces", "demo", "resources", _, uuid] if uuid.starts_with("421a7a05") => HttpResponse {
                status: 200,
                challenge: None,
                body: match param("$format").as_deref() {
                    Some("json") => br#"{"$type":"resqml20.obj_HorizonInterpretation","Uuid":"421a7a05-033a-450d-bcef-051352023578"}"#.to_vec(),
                    _ => format!(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><DataObjects>{}</DataObjects>",
                        std::str::from_utf8(OBJECT).unwrap()
                    )
                    .into_bytes(),
                },
            },
            ["dataspaces", "multi-resources", "get-content"] if self.no_plural => {
                json_response(404, json!({"detail": "Not Found"}))
            }
            ["dataspaces", "multi-resources", "get-content"] => {
                let asked: serde_json::Value = serde_json::from_slice(request.body.as_deref().unwrap()).unwrap();
                let uris: Vec<&str> = asked["uris"].as_array().unwrap().iter().filter_map(|u| u.as_str()).collect();
                let held: Vec<&str> = uris.iter().copied().filter(|u| u.contains("421a7a05")).collect();
                let extra: Vec<(&str, bool)> =
                    self.extra.iter().copied().filter(|(uuid, _)| uris.iter().any(|u| u.contains(uuid))).collect();
                if param("$format").as_deref() == Some("json") {
                    let referenced = param("referencedContent").as_deref() != Some("false");
                    if extra.iter().any(|(_, json)| !json) && !self.omits {
                        return match referenced {
                            true => HttpResponse { status: 201, challenge: None, body: b"[]".to_vec() },
                            false => json_response(500, json!({"description": "Empty object"})),
                        };
                    }
                    if !referenced && held.is_empty() && extra.is_empty() {
                        return json_response(404, json!({"description": "Object not Found"}));
                    }
                    // What the API sends: each reference with the object it
                    // names inside (`_data`) unless told not to, each array
                    // node with its metadata when told to.
                    let arrays = param("arrayMetadata").as_deref() == Some("true");
                    let mut feature = json!({"$type": "eml20.DataObjectReference", "UUID": "7d67a5b2-89a2-4c84-b5bc-f25b8e0a075b"});
                    if referenced {
                        feature["_data"] = json!({"$type": "resqml20.obj_GeneticBoundaryFeature", "SchemaVersion": "2.0",
                                                  "Uuid": "7d67a5b2-89a2-4c84-b5bc-f25b8e0a075b",
                                                  "Citation": {"$type": "eml20.Citation", "Title": "Brent"}});
                    }
                    let mut values = json!({"$type": "eml20.Hdf5Dataset", "PathInHdfFile": "/large"});
                    if arrays {
                        values["_data"] = json!({"LogicalArrayType": ordinal(&AnyLogicalArrayType::ArrayOfDouble64Le),
                                                 "TransportArrayType": ordinal(&AnyArrayType::ArrayOfDouble),
                                                 "Dimensions": [300, 100], "PreferredSubarrayDimensions": [], "CustomData": {},
                                                 "StoreCreated": "1970-01-01T00:00:00.000Z", "StoreLastWrite": "1970-01-01T00:00:00.000Z",
                                                 "Uid": {"Uri": demo_id("/large").uri, "PathInResource": "/large"}});
                    }
                    let objects: Vec<serde_json::Value> = held
                        .iter()
                        .map(|_| json!({
                            "$type": "resqml20.obj_HorizonInterpretation", "SchemaVersion": "2.0",
                            "Uuid": "421a7a05-033a-450d-bcef-051352023578",
                            "Citation": {"$type": "eml20.Citation", "Title": "Top Brent"},
                            "InterpretedFeature": feature, "Values": values
                        }))
                        .chain(
                            extra
                                .iter()
                                .filter(|(_, json)| *json)
                                .map(|(uuid, _)| json!({"$type": "resqml20.obj_Grid2dRepresentation", "Uuid": uuid})),
                        )
                        .collect();
                    return HttpResponse { status: 201, challenge: None, body: serde_json::to_vec(&objects).unwrap() };
                }
                let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><DataObjects>");
                for _ in held {
                    xml.push_str(std::str::from_utf8(OBJECT).unwrap());
                }
                for (uuid, _) in extra {
                    xml.push_str(&extra_xml(uuid));
                }
                xml.push_str("</DataObjects>");
                HttpResponse { status: 201, challenge: None, body: xml.into_bytes() }
            }
            _ => json_response(404, json!({"statusCode": 404, "message": format!("Cannot GET {path}")})),
        }
    }
}

fn b64(bytes: impl Iterator<Item = u8>) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes.collect::<Vec<u8>>())
}

fn rest(fake: FakeRest) -> RestClient<FakeRest> {
    RestClient::new(
        fake,
        REST_BASE,
        Some(Credentials::bearer("good").unwrap()),
        Some("opendes".into()),
    )
}

/// Without the dataspace segment - so that the two transports' URIs compare.
fn undemo(uri: &str) -> String {
    uri.replace("dataspace('demo')/", "")
}

/// One operation, both transports, the same store: the listing - followed
/// page by page over REST -, objects by URI - the REST's `<DataObjects>`
/// wrapper taken off -, a lost one refused `ENOT_FOUND` by both, and arrays
/// of doubles and of ints read by blocks under the same limit. The same answers, or one transport
/// is wrong.
#[test]
fn the_rest_client_reads_as_the_websocket_one() {
    let mut etp = open(false, None);
    let fake = FakeRest::default();
    let mut rest = rest(fake.clone())
        .with_page(1)
        .with_array_limit(STORE_ARRAY_LIMIT as i64);

    let by_etp = list_resources(&mut etp, &ResourceQuery::new("eml:///")).unwrap();
    let by_rest = list_resources(&mut rest, &ResourceQuery::new(DEMO)).unwrap();
    let names = |l: &retp_client::ResourceList| -> Vec<(String, String)> {
        l.resources
            .iter()
            .map(|r| (undemo(&r.uri), r.name.clone()))
            .collect()
    };
    assert_eq!(names(&by_rest), names(&by_etp));
    assert_eq!(by_rest.resources.len(), 2, "not two empty listings agreeing");
    assert!(
        fake.asked.borrow().iter().any(|u| u.contains("$skip=1")),
        "the second page was asked: {:?}",
        fake.asked.borrow()
    );

    let held = "resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)";
    let lost = "resqml20.obj_LocalDepth3dCrs(00000000-0000-0000-0000-000000000000)";
    let etp_objects = get_data_objects(
        &mut etp,
        &[format!("eml:///{held}"), format!("eml:///{lost}")],
        "xml",
    )
    .unwrap();
    let rest_objects = get_data_objects(&mut rest, &[in_demo(held), in_demo(lost)], "xml").unwrap();
    let data = |m: &BTreeMap<String, Result<DataObject, ErrorInfo>>, uri: &str| {
        m[uri].as_ref().map(|o| o.data.clone()).map_err(|e| e.code)
    };
    assert_eq!(
        data(&rest_objects, &in_demo(held)),
        data(&etp_objects, &format!("eml:///{held}"))
    );
    assert_eq!(
        data(&rest_objects, &in_demo(held)),
        Ok(OBJECT.to_vec()),
        "the wrapper is off"
    );
    assert_eq!(
        fake.asked
            .borrow()
            .iter()
            .filter(|u| u.contains("multi-resources/get-content"))
            .count(),
        1,
        "both objects in one request"
    );
    assert_eq!(data(&rest_objects, &in_demo(lost)), Err(11));
    assert_eq!(data(&etp_objects, &format!("eml:///{lost}")), Err(11));

    // Doubles and ints: the two kinds of numbers base64 carries.
    let by_etp = read_arrays(&mut etp, &[id("/large"), id("/ints")], 16 * 1024 * 1024, |_| {}).unwrap();
    let mut blocks = 0;
    let by_rest = read_arrays(
        &mut rest,
        &[demo_id("/large"), demo_id("/ints")],
        16 * 1024 * 1024,
        |event| {
            if let ArrayProgress::Planned { blocks: n, .. } = event {
                blocks = blocks.max(n);
            }
        },
    )
    .unwrap();
    assert!(blocks > 1, "read by blocks under the limit: {blocks}");
    assert!(by_rest.iter().all(Result::is_ok), "{by_rest:?}");
    assert_eq!(by_rest, by_etp);
}

/// An OSDU service answer, replayed as it came: its metadata and a 2 × 3 block
/// of points in base64. The values are those the ETP client read from the
/// same server - two transports, one fact.
#[test]
fn an_osdu_service_answer_reads_as_its_etp_values() {
    const METADATA: &str = r#"{"uid":{"uri":"eml:///dataspace('demo')/eml20.obj_EpcExternalPartReference(421a7a05-033a-450d-bcef-051352023578)","pathInResource":"/points"},"dimensions":[2,3],"preferredSubarrayDimensions":[],"logicalArrayType":0,"transportArrayType":4,"storeLastWrite":"1970-01-01T00:00:00.000Z","storeCreated":"1970-01-01T00:00:00.000Z","customData":{}}"#;
    const VALUES: &str = r#"{"uid":{},"data":{"data":"AAAAAOqqGkEAAACgi61YQQAAAEAUuIfAAAAAAByrGkEAAACgi61YQQAAAGAVuofA","dimensions":[2,3]}}"#;
    let fake = FakeRest {
        recorded: Some(("/points", METADATA, VALUES)),
        ..Default::default()
    };
    let mut rest = rest(fake);
    let read = read_arrays(&mut rest, &[demo_id("/points")], 16 * 1024 * 1024, |_| {}).unwrap();
    let array = read[0].as_ref().unwrap();
    assert_eq!(array.dimensions, [2, 3]);
    assert_eq!(
        array.values.to_f64s(array.logical).unwrap(),
        [
            436922.5,
            6469166.5,
            -759.0098876953125,
            436935.0,
            6469166.5,
            -759.2604370117188
        ]
    );
    // And the dates the API writes, against Python's `datetime`.
    assert_eq!(
        retp_client::rest::micros_of("2025-07-31T15:15:38.151Z"),
        Some(1_753_974_938_151_000)
    );
    assert_eq!(
        retp_client::rest::iso(1_753_974_938_151_000),
        "2025-07-31T15:15:38.151Z"
    );
}

/// **Another RDDMS's answer to the same question, which is not the OSDU service's.**
///
/// Reported on every array of a 3D load: *« unreadable metadata »*.
/// That server's `…/arrays/{path}/metadata` answers its own `ArrayMetadata` - the
/// enumerations as **symbols**, and **no `transportArrayType` at all** - where
/// the OSDU service answers the whole of ETP's `DataArrayMetadata` with ordinals. Both are
/// honest JSON bindings of the same fact; reading only one of them made every
/// array of that server unreadable.
///
/// The bytes and the expected values are the OSDU service's, from the test above: two
/// servers, two shapes, one fact - the triangulation this repository asks for.
#[test]
fn a_metadata_without_a_transport_type_reads_by_its_logical_type() {
    const METADATA: &str = r#"{"uid":{"uri":"eml:///dataspace('demo')/eml20.obj_EpcExternalPartReference(421a7a05-033a-450d-bcef-051352023578)","pathInResource":"/points"},"dimensions":[2,3],"preferredSubarrayDimensions":[],"logicalArrayType":"arrayOfDouble64LE","storeLastWrite":0,"storeCreated":0,"customData":{}}"#;
    const VALUES: &str = r#"{"uid":{},"data":{"data":"AAAAAOqqGkEAAACgi61YQQAAAEAUuIfAAAAAAByrGkEAAACgi61YQQAAAGAVuofA","dimensions":[2,3]}}"#;
    let fake = FakeRest {
        recorded: Some(("/points", METADATA, VALUES)),
        ..Default::default()
    };
    let mut rest = rest(fake);
    let read = read_arrays(&mut rest, &[demo_id("/points")], 16 * 1024 * 1024, |_| {}).unwrap();
    let array = read[0].as_ref().unwrap();
    assert_eq!(array.dimensions, [2, 3]);
    assert_eq!(
        array.values.to_f64s(array.logical).unwrap(),
        [
            436922.5,
            6469166.5,
            -759.0098876953125,
            436935.0,
            6469166.5,
            -759.2604370117188
        ]
    );
}

/// The one logical type a missing transport does **not** settle on its own:
/// §13.2.2.1 lets a little-endian integer travel as `bytes`, `arrayOfInt` or
/// `arrayOfLong`. The values say which - a list is not base64 - and that is a
/// reading of the data, not a guess about it.
#[test]
fn a_list_of_numbers_says_it_did_not_travel_as_bytes() {
    const METADATA: &str = r#"{"uid":{"pathInResource":"/triangles"},"dimensions":[2,3],"logicalArrayType":"arrayOfInt32LE","storeLastWrite":0,"storeCreated":0,"customData":{}}"#;
    const VALUES: &str = r#"{"uid":{},"data":{"data":[0,1,2,2,1,3],"dimensions":[2,3]}}"#;
    let fake = FakeRest {
        recorded: Some(("/triangles", METADATA, VALUES)),
        ..Default::default()
    };
    let mut rest = rest(fake);
    let read = read_arrays(&mut rest, &[demo_id("/triangles")], 16 * 1024 * 1024, |_| {}).unwrap();
    let array = read[0].as_ref().unwrap();
    assert_eq!(
        array.values.to_f64s(array.logical).unwrap(),
        [0.0, 1.0, 2.0, 2.0, 1.0, 3.0]
    );
}

/// And a store that contradicts itself is **refused by name**. A big-endian
/// integer may only travel as `bytes` (§13.2.2.1), so a JSON list of numbers
/// under one is a store saying two things: reading it as little-endian would
/// be a package full of plausible wrong values, which is the one outcome
/// worse than a refusal.
#[test]
fn a_shape_the_logical_type_forbids_is_refused() {
    const METADATA: &str = r#"{"uid":{"pathInResource":"/counts"},"dimensions":[2],"logicalArrayType":"arrayOfInt16BE","storeLastWrite":0,"storeCreated":0,"customData":{}}"#;
    const VALUES: &str = r#"{"uid":{},"data":{"data":[1,2],"dimensions":[2]}}"#;
    let fake = FakeRest {
        recorded: Some(("/counts", METADATA, VALUES)),
        ..Default::default()
    };
    let mut rest = rest(fake);
    let read = read_arrays(&mut rest, &[demo_id("/counts")], 16 * 1024 * 1024, |_| {}).unwrap();
    let why = format!("{:?}", read[0].as_ref().unwrap_err());
    assert!(why.contains("Bytes"), "{why}");
    assert!(why.contains("JSON"), "{why}");
}

/// A logical type this build does not know is refused by name too - and the
/// message says what was unreadable, not merely that something was.
#[test]
fn a_logical_type_that_is_not_one_is_named() {
    const METADATA: &str = r#"{"uid":{"pathInResource":"/points"},"dimensions":[2],"logicalArrayType":"arrayOfQuaternion","storeLastWrite":0,"storeCreated":0,"customData":{}}"#;
    const VALUES: &str = r#"{"uid":{},"data":{"data":[1,2],"dimensions":[2]}}"#;
    let fake = FakeRest {
        recorded: Some(("/points", METADATA, VALUES)),
        ..Default::default()
    };
    let mut rest = rest(fake);
    let read = read_arrays(&mut rest, &[demo_id("/points")], 16 * 1024 * 1024, |_| {}).unwrap();
    let why = format!("{:?}", read[0].as_ref().unwrap_err());
    assert!(why.contains("logical array type"), "{why}");
    assert!(why.contains("/points"), "{why}");
}

/// The graph comes with each edge's path in its source object - the REST's
/// own - and a kind the REST does not give, said as Primary.
#[test]
fn the_rest_graph_gives_each_edge_its_path() {
    let mut rest = rest(FakeRest::default());
    let query = ResourceQuery {
        include_edges: true,
        ..ResourceQuery::new(DEMO)
    };
    let listed = list_resources(&mut rest, &query).unwrap();
    assert_eq!(listed.resources.len(), 2);
    assert_eq!(listed.edges.len(), 1, "{:?}", listed.edges);
    let edge = &listed.edges[0];
    assert_eq!(
        (undemo(&edge.source_uri), undemo(&edge.target_uri)),
        (
            format!("eml:///{}", RESOURCES[0].0),
            format!("eml:///{}", RESOURCES[1].0)
        )
    );
    assert_eq!(
        edge.custom_data["path"].item,
        retp_types::datatypes::DataValueItem::String("rsq:LocalCrs".into())
    );
}

/// What the REST API has no counterpart for is refused by name, before any
/// request: an object's own resource, the default dataspace, the types a
/// store holds none of, a message it does not carry.
#[test]
fn the_rest_client_refuses_by_name_what_rest_does_not_do() {
    let fake = FakeRest::default();
    let mut rest = rest(fake.clone());
    let refused = |r: Result<_, ClientError>, words: &str| match r {
        Err(ClientError::Invalid(incident)) => assert!(
            incident.message().contains(words),
            "{words}: {}",
            incident.message()
        ),
        other => panic!("{words}: {other:?}"),
    };
    let self_scope = ResourceQuery {
        scope: ContextScopeKind::Self_,
        ..ResourceQuery::new(in_demo(
            "resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)",
        ))
    };
    refused(
        list_resources(&mut rest, &self_scope).map(|_| ()),
        "never the object itself",
    );
    refused(
        list_resources(&mut rest, &ResourceQuery::new("eml:///")).map(|_| ()),
        "default",
    );
    refused(
        list_supported_types(&mut rest, DEMO, ContextScopeKind::Targets, false, true).map(|_| ()),
        "holds none of",
    );
    // `PutDataObjects` had no counterpart at first; the deleted-resources
    // listing still has none.
    let deleted = retp_types::protocol::discovery::GetDeletedResources::default();
    refused(
        poll_once(retp_client::Caller::call(&mut rest, &deleted))
            .expect("ready")
            .map(|_| ()),
        "no counterpart",
    );
    assert!(
        fake.asked.borrow().is_empty(),
        "nothing was asked of the server: {:?}",
        fake.asked.borrow()
    );
}

/// A status is the ETP answer a server would have given: credentials refused,
/// `AuthorizationNeeded` with the challenge; the server unreachable or
/// failing, a lost connection - which a resumed operation retries.
#[test]
fn rest_statuses_become_what_etp_would_say() {
    let mut refused = rest(FakeRest {
        status: Some(401),
        ..Default::default()
    });
    match list_dataspaces(&mut refused) {
        Err(ClientError::AuthorizationNeeded { challenges }) => assert_eq!(challenges, ["Bearer"]),
        other => panic!("{other:?}"),
    }
    let mut failing = rest(FakeRest {
        status: Some(503),
        ..Default::default()
    });
    let error = list_dataspaces(&mut failing).unwrap_err();
    assert!(error.is_connection_lost(), "{error:?}");
    let listed = list_dataspaces(&mut rest(FakeRest::default())).unwrap();
    assert_eq!(
        (listed.items[0].path.as_str(), listed.items[0].store_last_write),
        ("demo", 1_753_974_938_151_000)
    );
}

/// JSON over REST is Energistics' JSON: the objects the API embeds in each
/// reference, and the metadata in each array node, are taken out.
#[test]
fn json_over_rest_is_energistics_json() {
    let mut rest = rest(FakeRest::default());
    let held = in_demo("resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)");
    let got = get_data_objects(&mut rest, std::slice::from_ref(&held), "json").unwrap();
    let data = got[&held].as_ref().unwrap().data.clone();
    let json: serde_json::Value = serde_json::from_slice(&data).unwrap();
    assert_eq!(json["Uuid"], "421a7a05-033a-450d-bcef-051352023578");
    assert!(!String::from_utf8(data).unwrap().contains("_data"), "{json}");
    assert_eq!(
        json["InterpretedFeature"]["UUID"], "7d67a5b2-89a2-4c84-b5bc-f25b8e0a075b",
        "the reference stays"
    );
}

/// A prefetch brings, in one request, the object, what it references -
/// addressed in its own dataspace (P11), as Energistics JSON - and its
/// arrays' metadata, which the reads that follow do not ask again.
#[test]
fn a_prefetch_brings_the_object_its_references_and_its_arrays() {
    let fake = FakeRest::default();
    let mut rest = rest(fake.clone());
    let held = in_demo("resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)");
    let lost = in_demo("resqml20.obj_LocalDepth3dCrs(00000000-0000-0000-0000-000000000000)");
    let prefetch = poll_once(rest.prefetch(&[held.clone(), lost.clone()], true))
        .expect("ready")
        .unwrap();

    let feature = in_demo("resqml20.obj_GeneticBoundaryFeature(7d67a5b2-89a2-4c84-b5bc-f25b8e0a075b)");
    let mut expected = vec![held.clone(), feature];
    expected.sort();
    assert_eq!(prefetch.objects.keys().cloned().collect::<Vec<_>>(), expected);
    assert!(
        prefetch
            .objects
            .values()
            .all(|o| !String::from_utf8_lossy(o).contains("_data"))
    );
    assert_eq!(prefetch.missing, [lost]);
    assert_eq!(prefetch.arrays.len(), 1);
    assert_eq!(prefetch.arrays[0].0, demo_id("/large"));
    assert_eq!(prefetch.arrays[0].1.dimensions, [300, 100]);
    assert_eq!(
        fake.asked.borrow().len(),
        2,
        "one request, and one in XML to tell the lost one from one without JSON"
    );

    // The array's metadata is not asked again.
    let read = read_arrays(
        &mut rest.with_array_limit(STORE_ARRAY_LIMIT as i64),
        &[demo_id("/large")],
        16 * 1024 * 1024,
        |_| {},
    )
    .unwrap();
    assert!(read[0].is_ok());
    assert!(
        !fake.asked.borrow().iter().any(|u| u.ends_with("/metadata")),
        "{:?}",
        fake.asked.borrow()
    );
}

/// In JSON the API omits an object it does not hold, but answers `[]` to the
/// whole request as soon as one it holds cannot be said in JSON (one OSDU
/// service). The client asks XML which it holds, then halves the batch: every
/// object the server can give comes, the one without JSON is refused
/// `ENOSUPPORTEDFORMATS` - and comes as its XML in a prefetch -, the lost one
/// `ENOT_FOUND`; in fewer requests than one per object.
#[test]
fn an_object_without_json_does_not_hide_the_others() {
    const GOOD: [&str; 6] = [
        "0a000000-0000-0000-0000-000000000001",
        "0a000000-0000-0000-0000-000000000002",
        "0a000000-0000-0000-0000-000000000003",
        "0a000000-0000-0000-0000-000000000004",
        "0a000000-0000-0000-0000-000000000005",
        "0a000000-0000-0000-0000-000000000006",
    ];
    const BAD: &str = "0b000000-0000-0000-0000-000000000000";
    let fake = FakeRest {
        extra: GOOD
            .iter()
            .map(|uuid| (*uuid, true))
            .chain([(BAD, false)])
            .collect(),
        ..Default::default()
    };
    let mut client = rest(fake.clone());
    let grid = |uuid: &str| in_demo(&format!("resqml20.obj_Grid2dRepresentation({uuid})"));
    let held = in_demo("resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)");
    let lost = in_demo("resqml20.obj_LocalDepth3dCrs(00000000-0000-0000-0000-000000000000)");
    let mut uris: Vec<String> = GOOD.iter().map(|uuid| grid(uuid)).collect();
    uris.insert(3, grid(BAD));
    uris.extend([held.clone(), lost.clone()]);

    let got = get_data_objects(&mut client, &uris, "json").unwrap();
    let code = |uri: &str| got[uri].as_ref().map(|_| ()).map_err(|e| e.code);
    for uuid in GOOD {
        assert_eq!(code(&grid(uuid)), Ok(()), "{uuid}");
    }
    assert_eq!(code(&held), Ok(()));
    assert_eq!(code(&grid(BAD)), Err(21));
    assert!(got[&grid(BAD)].as_ref().unwrap_err().message.contains("no JSON"));
    assert_eq!(code(&lost), Err(11));
    // In JSON: the batch; then its halves - [g1 g2 g3 BAD] refused,
    // [g4 g5 g6 held] answered -; [g1 g2] answered, so [g3 BAD] is refused
    // without asking; [g3] answered, so BAD alone is, without asking. Five,
    // where one by one would be nine.
    let json_asks = fake
        .asked
        .borrow()
        .iter()
        .filter(|u| u.contains("$format=json"))
        .count();
    assert_eq!(json_asks, 5, "{:?}", fake.asked.borrow());

    let prefetch = poll_once(client.prefetch(&uris, false)).expect("ready").unwrap();
    assert_eq!(
        prefetch.without_json.keys().cloned().collect::<Vec<_>>(),
        [grid(BAD)]
    );
    assert_eq!(
        prefetch.without_json[&grid(BAD)],
        extra_xml(BAD).into_bytes(),
        "the element whole"
    );
    assert_eq!(prefetch.missing, std::slice::from_ref(&lost));
    assert!(GOOD.iter().all(|uuid| prefetch.objects.contains_key(&grid(uuid))));
    assert!(prefetch.objects.contains_key(&held));

    // A server that leaves out only what it cannot say: the answer is not
    // `[]`, and the one it left out and holds is said without JSON at once.
    let fake = FakeRest { omits: true, ..fake };
    fake.asked.borrow_mut().clear();
    let got = get_data_objects(&mut rest(fake.clone()), &uris, "json").unwrap();
    assert_eq!(got[&grid(BAD)].as_ref().map(|_| ()).map_err(|e| e.code), Err(21));
    assert_eq!(got[&lost].as_ref().map(|_| ()).map_err(|e| e.code), Err(11));
    assert_eq!(
        fake.asked.borrow().len(),
        2,
        "one in JSON, one in XML: {:?}",
        fake.asked.borrow()
    );
}

/// A uuid names one object in one dataspace only: the same object copied in
/// two dataspaces is two objects, and each is read - one request per
/// dataspace, the API's answers being keyed by uuid.
#[test]
fn a_uuid_in_two_dataspaces_is_two_objects() {
    let fake = FakeRest::default();
    let mut client = rest(fake.clone());
    let object = "resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)";
    let uris = [in_demo(object), format!("eml:///dataspace('copy')/{object}")];
    let got = get_data_objects(&mut client, &uris, "xml").unwrap();
    for uri in &uris {
        assert_eq!(
            got[uri].as_ref().map(|o| o.data.clone()).map_err(|e| e.code),
            Ok(OBJECT.to_vec()),
            "{uri}"
        );
    }
    assert_eq!(fake.asked.borrow().len(), 2);
}

/// A round trip to an OSDU server costs more than the answer it brings
/// (one OSDU service: ~0.6 s a request), so the requests of one ETP message go to
/// the transport **together**, with how many of them it may run at once: the
/// metadata of both arrays in one handful, then every block of both in
/// another - `MaxDataArraySize` cuts an array, it does not cut a handful.
#[test]
fn the_requests_of_one_message_go_to_the_transport_together() {
    let fake = FakeRest::default();
    let mut client = rest(fake.clone())
        .with_array_limit(STORE_ARRAY_LIMIT as i64)
        .with_at_once(3);
    let read = read_arrays(
        &mut client,
        &[demo_id("/large"), demo_id("/ints")],
        16 * 1024 * 1024,
        |_| {},
    )
    .unwrap();
    assert!(read.iter().all(Result::is_ok), "{read:?}");

    let batches = fake.batches.borrow().clone();
    assert!(
        batches.iter().all(|(_, allowed)| *allowed == 3),
        "how many may travel at once: {batches:?}"
    );
    let handfuls: Vec<usize> = batches.into_iter().map(|(requests, _)| requests).collect();
    assert_eq!(
        handfuls[0], 2,
        "the metadata of both arrays at once: {handfuls:?}"
    );
    assert!(handfuls[1] > 2, "then their blocks, together: {handfuls:?}");
    assert_eq!(
        handfuls.len(),
        2,
        "two round trips, not one a block: {handfuls:?}"
    );
}

/// Ten arrays in one message, and each metadata comes back for **its own**
/// array. A map is keyed by strings (§3.7.3), where `"10"` sorts before
/// `"2"`: reading the answers in key order crossed them, and it only showed
/// past nine - a dataspace of one OSDU service was the first to have ten.
#[test]
fn the_metadata_of_ten_arrays_come_back_each_to_its_own() {
    let mut client = open(false, None);
    let paths = [
        "/small", "/large", "/ints", "/small", "/large", "/ints", "/small", "/large", "/ints", "/small",
        "/large",
    ];
    let ids: Vec<DataArrayIdentifier> = paths.iter().map(|path| id(path)).collect();
    let metadata = array_metadata(&mut client, &ids).unwrap();
    let shape = |path: &str| match path {
        "/small" => vec![3, 4],
        "/large" => vec![300, 100],
        _ => vec![50_000],
    };
    let got: Vec<Vec<i64>> = metadata
        .iter()
        .map(|meta| meta.as_ref().unwrap().dimensions.clone())
        .collect();
    assert_eq!(got, paths.iter().map(|path| shape(path)).collect::<Vec<_>>());
}

/// Not every Reservoir DDMS REST API reads several objects in one request:
/// another's, under `/etp`, has no `multi-resources/get-content` at all. The
/// client finds that out once - a 404 on the endpoint, not on the objects -
/// and asks one at a time from then on, which is slower and right.
#[test]
fn a_server_without_a_plural_read_is_asked_one_object_at_a_time() {
    let fake = FakeRest {
        no_plural: true,
        ..Default::default()
    };
    let mut client = rest(fake.clone());
    let held = in_demo("resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)");
    let lost = in_demo("resqml20.obj_LocalDepth3dCrs(00000000-0000-0000-0000-000000000000)");
    let got = get_data_objects(&mut client, &[held.clone(), lost.clone()], "xml").unwrap();
    assert_eq!(
        got[&held].as_ref().map(|o| o.data.clone()).map_err(|e| e.code),
        Ok(OBJECT.to_vec())
    );
    assert_eq!(
        got[&lost].as_ref().map(|_| ()).map_err(|e| e.code),
        Err(11),
        "and a lost one is still lost"
    );

    // The plural endpoint was tried once; after that, one request per object.
    let asked = fake.asked.borrow().clone();
    assert_eq!(asked.iter().filter(|u| u.contains("multi-resources")).count(), 1);
    fake.asked.borrow_mut().clear();
    let again = get_data_objects(&mut client, std::slice::from_ref(&held), "xml").unwrap();
    assert!(again[&held].is_ok());
    assert_eq!(
        fake.asked
            .borrow()
            .iter()
            .filter(|u| u.contains("multi-resources"))
            .count(),
        0,
        "asked once, remembered: {:?}",
        fake.asked.borrow()
    );
}

// --- Tier 5: writing ------------------------------------------------------
//
// **Tried against a fake store, and never against anyone's server.** The rule:
// never modify a server that is not one's own - so third-party servers stay
// read-only, even for an attempt undone afterwards.

/// A session that keeps the Dataspace protocol, which writing one needs.
///
/// `carrying` is what the store **states** one message may hold: stated, the
/// client refuses too big an object before sending it; left silent, nothing
/// here is guarded and the transport or the store answers.
fn open_writing(read_only: Option<&'static str>) -> Client<Loopback, ManualClock> {
    open_writing_carrying(read_only, None)
}

fn open_writing_carrying(
    read_only: Option<&'static str>,
    carrying: Option<i64>,
) -> Client<Loopback, ManualClock> {
    transacting(read_only, carrying, None, false)
}

/// The same, with a store that refuses a commit - or answers about another
/// transaction than the one asked about.
fn transacting(
    read_only: Option<&'static str>,
    carrying: Option<i64>,
    commit_fails: Option<&'static str>,
    commit_of_another: bool,
) -> Client<Loopback, ManualClock> {
    transacting_capped(read_only, carrying, commit_fails, commit_of_another, None)
}

/// The same, with a store that states how big an object it holds.
fn transacting_capped(
    read_only: Option<&'static str>,
    carrying: Option<i64>,
    commit_fails: Option<&'static str>,
    commit_of_another: bool,
    object_cap: Option<i64>,
) -> Client<Loopback, ManualClock> {
    transacting_counted(
        read_only,
        carrying,
        commit_fails,
        commit_of_another,
        object_cap,
        None,
    )
}

/// A writing session whose transport counts the messages of one kind -
/// `(protocol, message type)` - the client sends: how many `PutDataArrays`
/// three arrays cost is the fact grouping is about.
fn counting_writes(
    carrying: Option<i64>,
    kind: (i32, i32),
) -> (
    Client<Loopback, ManualClock>,
    std::sync::Arc<std::sync::atomic::AtomicUsize>,
) {
    let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let client = transacting_counted(None, carrying, None, false, None, Some((kind, count.clone())));
    (client, count)
}

fn transacting_counted(
    read_only: Option<&'static str>,
    carrying: Option<i64>,
    commit_fails: Option<&'static str>,
    commit_of_another: bool,
    object_cap: Option<i64>,
    counting: Option<((i32, i32), std::sync::Arc<std::sync::atomic::AtomicUsize>)>,
) -> Client<Loopback, ManualClock> {
    let clock = ManualClock::new(T0);
    let mut offer = Offer::new("a test store", "0.1", Uuid([2; 16]));
    if let Some(cap) = object_cap {
        offer.endpoint = offer
            .endpoint
            .set(
                retp_proto::capabilities::names::MAX_DATA_OBJECT_SIZE,
                CapabilityValue::Long(cap),
            )
            .unwrap();
    }
    offer.data_objects = vec![SupportedDataObject {
        qualified_type: "resqml20.*".into(),
        data_object_capabilities: BTreeMap::new(),
    }];
    if let Some(payload) = carrying {
        offer.endpoint = offer
            .endpoint
            .set(
                retp_proto::capabilities::names::MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE,
                CapabilityValue::Long(payload),
            )
            .unwrap();
    }
    let store = move |protocol: i32| -> Box<dyn Handler> {
        Box::new(Store {
            read_only,
            commit_fails,
            commit_of_another,
            ..Store::new(protocol, false, false)
        })
    };
    let server = Connection::server(
        offer,
        vec![store(3), store(4), store(18), store(24), store(25), store(9)],
        None,
        Uuid([0xAB; 16]),
        T0,
    )
    .unwrap();
    let mut transport = loopback(server, &clock, None, None);
    if let Some((kind, count)) = counting {
        transport = transport.falls_when(move |message| {
            let mut input = Reader::new(message);
            let header = MessageHeader::read(&mut input).unwrap();
            if (header.protocol, header.message_type) == kind {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            false
        });
    }
    let (offer, handlers) = writing_client("test client", "0.1", Uuid([1; 16]), &["resqml20.*".to_string()]);
    Client::open(transport, clock, offer, handlers, None, Settings::default()).unwrap()
}

fn an_object(uri: &str, bytes: &[u8]) -> DataObject {
    DataObject {
        resource: Resource {
            uri: uri.to_string(),
            ..Default::default()
        },
        format: "xml".into(),
        blob_id: None,
        data: bytes.to_vec(),
    }
}

const WRITTEN: &str =
    "eml:///dataspace('demo')/resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)";

/// **`PutDataObjects` is an upsert** (§9.1.1, p.178), and the difference from
/// an insert is the whole point: writing an object that is already there
/// replaces it. A client that read first to decide would double every round
/// trip to enforce a rule the store does not have - and would still race.
#[test]
fn put_is_upsert_not_insert() {
    let mut client = open_writing(None);
    let first = put_data_objects(&mut client, &[an_object(WRITTEN, b"<v1/>")], false).unwrap();
    assert!(first[WRITTEN].is_ok(), "{first:?}");

    let again = put_data_objects(&mut client, &[an_object(WRITTEN, b"<v2/>")], false).unwrap();
    assert!(
        again.get(WRITTEN).is_some_and(Result::is_ok),
        "a second write is not a failure: {again:?}"
    );
    // And it left **one**: read back, the object carries the second bytes,
    // so the second write replaced the first rather than sitting beside it.
}

/// A refusal to delete a container is a **business** answer with something to
/// do about it - ask again with the cascade - not a breakdown, and it arrives
/// per key rather than ending the request (§24.3, p.485).
#[test]
fn delete_of_a_container_is_refused_by_name() {
    let mut client = open_writing(None);
    let container = "eml:///dataspace('demo')/resqml20.obj_EpcExternalPartReference(container)".to_string();
    let second_container =
        "eml:///dataspace('demo')/resqml20.obj_EpcExternalPartReference(second-container)".to_string();
    let plain = WRITTEN.to_string();
    let asked = [container.clone(), second_container.clone(), plain.clone()];
    let answers = delete_data_objects(&mut client, &asked, false).unwrap();

    let refused = answers[&container].as_ref().unwrap_err();
    assert_eq!(refused.code, 4003, "{refused:?}");
    assert!(refused.message.contains("holds two objects"), "{refused:?}");
    // **Each key keeps its own reason.** A per-key exception read as a single
    // one would give both containers the same message, and the person would
    // go looking for contained objects on the one that is simply published.
    let second = answers[&second_container].as_ref().unwrap_err();
    assert!(second.message.contains("it is published"), "{second:?}");
    // And the one beside them went through: a refusal on one key does not
    // carry off the others.
    assert_eq!(answers[&plain].as_ref().unwrap(), &vec![plain.clone()]);

    // Asked again with the cascade, the container goes - and the store says
    // what went with it, which a bare success would have hidden.
    let cascaded = delete_data_objects(&mut client, std::slice::from_ref(&container), true).unwrap();
    let deleted = cascaded[&container].as_ref().unwrap();
    assert!(deleted.contains(&format!("{container}/inside")), "{deleted:?}");
}

/// A store that will not be written to says so, and **what it says is the
/// only thing the person can act on** - the specification recommends sending
/// it for exactly that reason (§3.1.1.2 §4, p.33). Replacing it with a message
/// of ours would lose the only useful half.
#[test]
fn read_only_server_refusal_is_reported() {
    let mut client = open_writing(Some("this dataspace is published and cannot be written"));
    let answers = put_data_objects(&mut client, &[an_object(WRITTEN, b"<v1/>")], false).unwrap();
    let refused = answers[WRITTEN].as_ref().unwrap_err();
    assert_eq!(refused.code, 6, "{refused:?}");
    assert!(
        refused.message.contains("published and cannot be written"),
        "{refused:?}"
    );
}

/// An object above the store's `MaxDataObjectSize` is refused **before
/// anything leaves**, naming both numbers: the store would not hold it
/// however it were sent, and chunking it would only make the refusal arrive
/// later and from further away.
#[test]
fn an_object_over_the_stores_cap_is_refused_before_it_is_sent() {
    let mut client = transacting_capped(None, None, None, false, Some(100_000));
    let big = an_object(WRITTEN, &vec![b'x'; 200_000]);
    match put_data_objects(&mut client, &[big], false) {
        Err(ClientError::Invalid(error)) => {
            assert!(error.message().contains("MaxDataObjectSize"), "{error:?}");
            assert!(
                error.message().contains("200000") && error.message().contains("100000"),
                "{error:?}"
            );
        }
        other => panic!("an object the store cannot hold went out: {other:?}"),
    }
}

/// **An object bigger than one message goes out in `Chunk`s** (§3.7.3.2),
/// and arrives whole. The session's messages are 4 KiB, the object 40 KB;
/// the store's handler sees the object reassembled - the wire does that
/// before the handler - and gives it back byte for byte. Two readings of
/// one fact: the bytes that left, the bytes that were held.
#[test]
fn blob_too_big_is_chunked() {
    let mut client = open_writing_carrying(None, Some(4096));
    let bytes: Vec<u8> = (0..40_000u32).map(|n| (n % 251) as u8).collect();
    // Two BLOBs in one message, each with its own id (§3.7.3.2 §1): with one
    // id for both, the store's reassembly would run the two together.
    let other_uri =
        "eml:///dataspace('demo')/resqml20.obj_HorizonInterpretation(5b6c7d8e-1111-4222-8333-944455566677)";
    let other: Vec<u8> = bytes.iter().rev().copied().collect();
    let written = put_data_objects(
        &mut client,
        &[an_object(WRITTEN, &bytes), an_object(other_uri, &other)],
        false,
    )
    .unwrap();
    assert!(
        written[WRITTEN].is_ok() && written[other_uri].is_ok(),
        "{written:?}"
    );

    let back = get_data_objects(&mut client, &[WRITTEN.to_string(), other_uri.to_string()], "xml").unwrap();
    let held = back[WRITTEN].as_ref().unwrap();
    assert_eq!(held.data.len(), bytes.len());
    assert_eq!(held.data, bytes, "reassembled as it left");
    assert_eq!(
        back[other_uri].as_ref().unwrap().data,
        other,
        "the second BLOB is its own"
    );
}

/// A small object is **not** chunked: sent inline, no BLOB, one message. The
/// same round trip, to say that the chunking is a consequence of size and
/// not the new way everything goes.
#[test]
fn a_small_object_goes_inline() {
    let mut client = open_writing_carrying(None, Some(4096));
    let bytes = b"<small/>".to_vec();
    let written = put_data_objects(&mut client, &[an_object(WRITTEN, &bytes)], false).unwrap();
    assert!(written[WRITTEN].is_ok(), "{written:?}");
    let back = get_data_objects(&mut client, &[WRITTEN.to_string()], "xml").unwrap();
    assert_eq!(back[WRITTEN].as_ref().unwrap().data, bytes);
}

/// Writing a dataspace carries what an OSDU store requires in `customData`, and **nothing is invented**: a key the caller
/// left out is left out, and the store names what it wants.
#[test]
fn a_dataspace_is_written_with_what_the_store_requires() {
    let mut client = open_writing(None);
    let bare = Dataspace {
        uri: "eml:///dataspace('neuf')".into(),
        path: "neuf".into(),
        ..Default::default()
    };
    let answers = put_dataspaces(&mut client, std::slice::from_ref(&bare)).unwrap();
    let refused = answers["eml:///dataspace('neuf')"].as_ref().unwrap_err();
    assert!(refused.message.contains("owners is required"), "{refused:?}");

    let with_owners = Dataspace {
        custom_data: [(
            "owners".to_string(),
            DataValue {
                item: DataValueItem::ArrayOfString(ArrayOfString {
                    values: vec!["data.default.owners@x.y".into()],
                }),
            },
        )]
        .into_iter()
        .collect(),
        ..bare
    };
    let answers = put_dataspaces(&mut client, &[with_owners]).unwrap();
    assert!(answers["eml:///dataspace('neuf')"].is_ok(), "{answers:?}");
}

/// Deleting dataspaces is answered **per dataspace**: the one the store
/// holds goes, the one it does not is refused by name - and the refusal does
/// not take the other down with it.
#[test]
fn dataspaces_are_deleted_each_for_itself() {
    let mut client = open_writing(None);
    let asked = [
        "eml:///dataspace('demo')".to_string(),
        "eml:///dataspace('absent')".to_string(),
    ];
    let answers = delete_dataspaces(&mut client, &asked).unwrap();
    assert_eq!(
        answers["eml:///dataspace('demo')"].as_ref().unwrap(),
        "eml:///dataspace('demo')"
    );
    let refused = answers["eml:///dataspace('absent')"].as_ref().unwrap_err();
    assert!(refused.message.contains("no such dataspace"), "{refused:?}");
}

/// A format the session did not agree is refused here rather than at the
/// store: the session is what knows, and a refusal that names the agreed list
/// is one somebody can act on.
#[test]
fn a_format_the_session_did_not_agree_is_refused_before_sending() {
    let mut client = open_writing(None);
    let mut object = an_object(WRITTEN, b"{}");
    object.format = "json".into();
    match put_data_objects(&mut client, &[object], false) {
        Err(ClientError::Invalid(error)) => assert!(error.message().contains("json"), "{error:?}"),
        other => panic!("a format nobody agreed went out: {other:?}"),
    }
}

/// **A write whose outcome nobody stated is not a write that worked.** A store
/// that answers about two of three objects leaves the third *unknown*, and the
/// map says so rather than leaving the key out - a caller that iterates the
/// answers would otherwise see two successes and no sign of the third.
#[test]
fn a_key_the_store_never_mentioned_is_said_rather_than_left_out() {
    let mut client = open_writing(None);
    let ignored = "eml:///dataspace('demo')/resqml20.obj_HorizonInterpretation(ignored)";
    let answers = put_data_objects(
        &mut client,
        &[an_object(WRITTEN, b"<v1/>"), an_object(ignored, b"<v2/>")],
        false,
    )
    .unwrap();

    assert_eq!(answers.len(), 2, "every URI asked about comes back: {answers:?}");
    assert!(answers[WRITTEN].is_ok(), "{answers:?}");
    let unknown = answers[ignored].as_ref().unwrap_err();
    assert!(unknown.message.contains("answered nothing"), "{unknown:?}");
}

/// **Written then read back, and the numbers are the same.** Two paths over
/// one fact - the client cuts an array into blocks to send it, the store puts
/// them back, and the client reassembles what it reads - so a block placed at
/// the wrong offset by either side shows up here as a value out of place.
///
/// The array is deliberately bigger than one message: written whole it would
/// exercise nothing, and the block walk is the part that can be wrong.
#[test]
fn an_array_written_in_blocks_reads_back_unchanged() {
    // A message small enough that 600 doubles cannot ride in one.
    let mut client = open_writing_carrying(None, Some(4096));
    let dimensions = vec![40u64, 5, 3];
    let count: u64 = dimensions.iter().product();
    // Every value distinct, so that a run copied to the wrong row is visible.
    let values: Vec<f64> = (0..count).map(|n| n as f64 * 1.5).collect();
    let array = Array {
        dimensions: dimensions.clone(),
        logical: AnyLogicalArrayType::ArrayOfDouble64Le,
        transport: AnyArrayType::ArrayOfDouble,
        values: ArrayValues::Doubles(values.clone()),
        blocks: 0,
    };
    let id = demo_id("/written");

    let mut blocks = 0;
    let written = write_arrays(&mut client, &[(id.clone(), array)], 4096, |event| {
        if let ArrayProgress::Planned { blocks: n, .. } = event {
            blocks = n;
        }
    })
    .unwrap();
    assert!(written[0].is_ok(), "{written:?}");
    assert!(
        blocks > 1,
        "the point is the block walk; it went whole in {blocks}"
    );

    let read = read_arrays(&mut client, std::slice::from_ref(&id), 4096, |_| {}).unwrap();
    let back = read[0].as_ref().unwrap();
    assert_eq!(back.dimensions, dimensions);
    assert_eq!(back.values.to_f64s(back.logical).unwrap(), values);
}

/// An array that fits goes in **one** `PutDataArrays`, and is never declared
/// first: `PutUninitializedDataArrays` exists to give subarrays somewhere to
/// land, and sending it for an array that needs none is a round trip for
/// nothing.
#[test]
fn an_array_that_fits_is_written_whole() {
    let mut client = open_writing(None);
    let array = Array {
        dimensions: vec![2, 3],
        logical: AnyLogicalArrayType::ArrayOfDouble64Le,
        transport: AnyArrayType::ArrayOfDouble,
        values: ArrayValues::Doubles(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]),
        blocks: 0,
    };
    let id = demo_id("/small");
    let mut blocks = 0;
    let written = write_arrays(&mut client, &[(id.clone(), array)], 16 * 1024 * 1024, |event| {
        if let ArrayProgress::Planned { blocks: n, .. } = event {
            blocks = n;
        }
    })
    .unwrap();
    assert!(written[0].is_ok(), "{written:?}");
    assert_eq!(blocks, 1);

    let read = read_arrays(&mut client, std::slice::from_ref(&id), 16 * 1024 * 1024, |_| {}).unwrap();
    assert_eq!(
        read[0]
            .as_ref()
            .unwrap()
            .values
            .to_f64s(AnyLogicalArrayType::ArrayOfDouble64Le)
            .unwrap(),
        vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    );
}

/// Values that do not fill the dimensions are refused **before anything
/// leaves**, naming both counts. Sent, they would write a shape the store
/// believes and nothing could later show to be wrong.
#[test]
fn values_that_do_not_fill_the_dimensions_are_refused() {
    let mut client = open_writing(None);
    let array = Array {
        dimensions: vec![2, 3],
        logical: AnyLogicalArrayType::ArrayOfDouble64Le,
        transport: AnyArrayType::ArrayOfDouble,
        values: ArrayValues::Doubles(vec![1.0, 2.0, 3.0]),
        blocks: 0,
    };
    let written = write_arrays(
        &mut client,
        &[(demo_id("/short"), array)],
        16 * 1024 * 1024,
        |_| {},
    )
    .unwrap();
    match written[0].as_ref().unwrap_err() {
        ArrayFailure::Inconsistent(why) => {
            assert!(why.contains('6') && why.contains('3'), "{why}");
        }
        other => panic!("an array of the wrong length went out: {other:?}"),
    }
}

/// One array's failure is **its own**: the others of the same call go through.
/// An array half written is a failure that must be named, never one that
/// carries off its neighbours.
#[test]
fn an_array_that_fails_does_not_carry_off_the_others() {
    let mut client = open_writing(None);
    let good = Array {
        dimensions: vec![2],
        logical: AnyLogicalArrayType::ArrayOfDouble64Le,
        transport: AnyArrayType::ArrayOfDouble,
        values: ArrayValues::Doubles(vec![1.0, 2.0]),
        blocks: 0,
    };
    let bad = Array {
        values: ArrayValues::Doubles(vec![1.0]),
        ..good.clone()
    };
    let written = write_arrays(
        &mut client,
        &[(demo_id("/bad"), bad), (demo_id("/good"), good)],
        16 * 1024 * 1024,
        |_| {},
    )
    .unwrap();
    assert!(written[0].is_err(), "{written:?}");
    assert!(written[1].is_ok(), "{written:?}");
}

/// **An answer that is not the confirmation is not a confirmation.** A store
/// that replies with something else - and one RDDMS server alone deviates from the spec
/// ten times - leaves the write neither confirmed nor explained, and that is
/// what is reported: taking it for success would announce a write that never
/// happened.
#[test]
fn an_answer_that_is_not_the_confirmation_is_not_taken_for_one() {
    let mut client = open_writing(None);
    let array = Array {
        dimensions: vec![2],
        logical: AnyLogicalArrayType::ArrayOfDouble64Le,
        transport: AnyArrayType::ArrayOfDouble,
        values: ArrayValues::Doubles(vec![1.0, 2.0]),
        blocks: 0,
    };
    let written = write_arrays(
        &mut client,
        &[(demo_id("/mute"), array)],
        16 * 1024 * 1024,
        |_| {},
    )
    .unwrap();
    match written[0].as_ref().unwrap_err() {
        ArrayFailure::Store(error) => assert!(error.message.contains("answered nothing"), "{error:?}"),
        other => panic!("{other:?}"),
    }
}

const SPACE: &str = "eml:///dataspace('demo')";

/// A transaction opens, takes the writes, and commits - the shape the front end's
/// push needs, end to end.
#[test]
fn a_transaction_opens_takes_writes_and_commits() {
    let mut client = open_writing(None);
    let Opened::Open(transaction) =
        start_transaction(&mut client, &[SPACE.to_string()], false, "un essai").unwrap()
    else {
        panic!("the store refused to open one");
    };
    assert!(!transaction.read_only);
    assert_eq!(transaction.dataspaces, vec![SPACE.to_string()]);

    let written = put_data_objects(&mut client, &[an_object(WRITTEN, b"<v1/>")], false).unwrap();
    assert!(written[WRITTEN].is_ok(), "{written:?}");

    assert_eq!(
        commit_transaction(&mut client, &transaction).unwrap(),
        Ended::Done
    );
}

/// **A store says no with `successful: false`, not with an exception.** A
/// client that read only the uuid would carry on writing into a transaction
/// that was never opened, and commit something that does not exist.
#[test]
fn a_transaction_the_store_would_not_open_is_not_one() {
    let mut client = open_writing(None);
    let locked = "eml:///dataspace('locked')".to_string();
    match start_transaction(&mut client, &[locked], false, "").unwrap() {
        Opened::Refused(why) => assert!(why.contains("another transaction"), "{why}"),
        Opened::Open(_) => panic!("a refusal was read as an open transaction"),
        Opened::Unsupported(why) => panic!("a refusal was read as no transactions at all: {why}"),
    }
}

/// **A refused commit is the expensive one**: the writes are not in the store.
/// It is returned, never swallowed - and a store that refuses without a word
/// says so, rather than reaching a screen as a refusal with nothing wrong.
#[test]
fn a_refused_commit_is_said_and_so_is_a_reason_nobody_gave() {
    let mut client = transacting(None, None, Some("a constraint was violated"), false);
    let Opened::Open(transaction) = start_transaction(&mut client, &[SPACE.to_string()], false, "").unwrap()
    else {
        panic!("the store refused to open one");
    };
    match commit_transaction(&mut client, &transaction).unwrap() {
        Ended::Refused(why) => assert!(why.contains("constraint was violated"), "{why}"),
        Ended::Done => panic!("a refused commit was read as done"),
    }

    let mut client = transacting(None, None, Some(""), false);
    let Opened::Open(transaction) = start_transaction(&mut client, &[SPACE.to_string()], false, "").unwrap()
    else {
        panic!("the store refused to open one");
    };
    match commit_transaction(&mut client, &transaction).unwrap() {
        Ended::Refused(why) => assert!(why.contains("no reason"), "{why}"),
        Ended::Done => panic!("a refused commit was read as done"),
    }
}

/// A store answering about **another** transaction is answering about another
/// transaction. Nothing in the protocol makes it impossible, and believing it
/// would report a commit that happened somewhere else.
#[test]
fn a_commit_answered_about_another_transaction_is_not_ours() {
    let mut client = transacting(None, None, None, true);
    let Opened::Open(transaction) = start_transaction(&mut client, &[SPACE.to_string()], false, "").unwrap()
    else {
        panic!("the store refused to open one");
    };
    match commit_transaction(&mut client, &transaction).unwrap() {
        Ended::Refused(why) => assert!(why.contains("another transaction"), "{why}"),
        Ended::Done => panic!("a commit of someone else's transaction was read as ours"),
    }
}

/// Rolling back is the other way out, and it says the same kind of thing.
#[test]
fn a_transaction_rolls_back() {
    let mut client = open_writing(None);
    let Opened::Open(transaction) = start_transaction(&mut client, &[SPACE.to_string()], false, "").unwrap()
    else {
        panic!("the store refused to open one");
    };
    assert_eq!(
        rollback_transaction(&mut client, &transaction).unwrap(),
        Ended::Done
    );
}

/// A dataspace URI that is not canonical is refused **before anything leaves**
/// (§3.7.4.2): the session is untouched, where a store would have opened
/// something first.
#[test]
fn a_transaction_over_a_uri_that_is_not_canonical_is_refused_before_sending() {
    let mut client = open_writing(None);
    let refused = start_transaction(&mut client, &["not a uri".to_string()], false, "").unwrap_err();
    assert!(
        matches!(&refused, ClientError::Invalid(incident) if incident.code() == EtpError::EINVALID_URI.code()),
        "{refused:?}"
    );
}

// --- Tier 5 over REST -----------------------------------------------------

/// The same four writes as over the WebSocket, put where the Reservoir DDMS
/// REST API puts them: objects as JSON Energistics on
/// `PUT …/resources`, arrays whole on `PUT …/resources/arrays` with their
/// container named, a deletion on its own path, a dataspace on
/// `POST /dataspaces` with what OSDU wants in `CustomData`.
#[test]
fn rest_writes_go_where_the_api_puts_them() {
    let fake = FakeRest::default();
    let written = std::rc::Rc::clone(&fake.written);
    let mut rest = rest(fake);

    let object = DataObject {
        resource: Resource {
            uri: in_demo("resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)"),
            ..Default::default()
        },
        format: "json".into(),
        blob_id: None,
        data:
            br#"{"$type":"resqml20.obj_HorizonInterpretation","Uuid":"421a7a05-033a-450d-bcef-051352023578"}"#
                .to_vec(),
    };
    let put = put_data_objects(&mut rest, std::slice::from_ref(&object), false).unwrap();
    assert!(put[&object.resource.uri].is_ok(), "{put:?}");

    let array = Array {
        dimensions: vec![2, 3],
        logical: AnyLogicalArrayType::ArrayOfDouble64Le,
        transport: AnyArrayType::ArrayOfDouble,
        values: ArrayValues::Doubles(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]),
        blocks: 0,
    };
    let wrote = write_arrays(
        &mut rest,
        &[(demo_id("/points"), array)],
        16 * 1024 * 1024,
        |_| {},
    )
    .unwrap();
    assert!(wrote[0].is_ok(), "{wrote:?}");

    let gone = delete_data_objects(&mut rest, std::slice::from_ref(&object.resource.uri), false).unwrap();
    assert_eq!(
        gone[&object.resource.uri].as_ref().unwrap(),
        &vec![object.resource.uri.clone()]
    );

    let space = Dataspace {
        uri: "eml:///dataspace('demo')".into(),
        path: "demo".into(),
        custom_data: [(
            "owners".to_string(),
            DataValue {
                item: DataValueItem::ArrayOfString(ArrayOfString {
                    values: vec!["data.default.owners@x.y".into()],
                }),
            },
        )]
        .into_iter()
        .collect(),
        ..Default::default()
    };
    let made = put_dataspaces(&mut rest, std::slice::from_ref(&space)).unwrap();
    assert!(made["eml:///dataspace('demo')"].is_ok(), "{made:?}");

    let dropped = delete_dataspaces(&mut rest, &["eml:///dataspace('demo')".to_string()]).unwrap();
    assert!(dropped["eml:///dataspace('demo')"].is_ok(), "{dropped:?}");

    let written = written.borrow();
    let paths: Vec<(&str, &str)> = written
        .iter()
        .map(|(m, p, _)| (m.as_str(), p.split('?').next().unwrap()))
        .collect();
    assert_eq!(
        paths,
        [
            ("PUT", "/dataspaces/demo/resources"),
            ("PUT", "/dataspaces/demo/resources/arrays"),
            (
                "DELETE",
                "/dataspaces/demo/resources/resqml20.obj_HorizonInterpretation/421a7a05-033a-450d-bcef-051352023578"
            ),
            ("POST", "/dataspaces"),
            ("DELETE", "/dataspaces/demo"),
        ]
    );
    // The object body is the JSON Energistics itself, as a list.
    assert_eq!(written[0].2[0]["$type"], "resqml20.obj_HorizonInterpretation");
    // The array names its container, and its values are numbers.
    let put_array = &written[1].2[0];
    assert_eq!(put_array["ContainerType"], "eml20.obj_EpcExternalPartReference");
    assert_eq!(put_array["ContainerUuid"], "421a7a05-033a-450d-bcef-051352023578");
    assert_eq!(put_array["PathInResource"], "/points");
    assert_eq!(put_array["Dimensions"], serde_json::json!([2, 3]));
    assert_eq!(
        put_array["Data"],
        serde_json::json!([1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
    );
    assert_eq!(put_array["ArrayType"], "arrayOfDouble");
    // The dataspace carries what OSDU wants, spelled as the API spells it.
    assert_eq!(written[3].2[0]["DataspaceId"], "demo");
    assert_eq!(
        written[3].2[0]["CustomData"]["owners"][0],
        "data.default.owners@x.y"
    );
}

/// An object handed over as XML is refused **by key and by name**: the API
/// takes JSON Energistics only, and this client holds no registry to
/// convert. Sent anyway, the API would refuse it with less to go on.
#[test]
fn an_xml_object_is_refused_by_the_rest_client_by_name() {
    let mut rest = rest(FakeRest::default());
    let object = DataObject {
        resource: Resource {
            uri: in_demo("resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)"),
            ..Default::default()
        },
        format: "xml".into(),
        blob_id: None,
        data: b"<obj/>".to_vec(),
    };
    let put = put_data_objects(&mut rest, std::slice::from_ref(&object), false).unwrap();
    let refused = put[&object.resource.uri].as_ref().unwrap_err();
    assert_eq!(refused.code, EtpError::ENOSUPPORTEDFORMATS.code(), "{refused:?}");
    assert!(refused.message.contains("JSON Energistics"), "{refused:?}");
}

/// A REST transaction is **one** dataspace and rides on every request after:
/// opened by `POST …/transactions`, its id goes on the next write as
/// `transactionId`, and `PUT …/transactions/{id}` commits it. Two dataspaces
/// are refused before anything leaves - that is the one thing ETP's message
/// can ask for and this API cannot do.
/// A server whose transaction route answers `null` has no transactions:
/// said as `Opened::Unsupported`, which a push takes as *go on without*,
/// and not as a refusal, which would stop it at its first island.
#[test]
fn a_null_rest_transaction_is_no_transaction_at_all() {
    let mut rest = rest(FakeRest {
        null_transaction: true,
        ..Default::default()
    });
    match start_transaction(&mut rest, &[DEMO.to_string()], false, "").unwrap() {
        Opened::Unsupported(why) => assert!(why.contains("null"), "{why}"),
        other => panic!("a null transaction was read as {other:?}"),
    }
}

#[test]
fn a_rest_transaction_is_one_dataspace_and_rides_on_every_request() {
    let fake = FakeRest::default();
    let written = std::rc::Rc::clone(&fake.written);
    let asked = std::rc::Rc::clone(&fake.asked);
    let mut rest = rest(fake);

    let two = [DEMO.to_string(), "eml:///dataspace('other')".to_string()];
    let refused = start_transaction(&mut rest, &two, false, "").unwrap_err();
    assert!(
        matches!(&refused, ClientError::Invalid(incident) if incident.message().contains("exactly one dataspace")),
        "{refused:?}"
    );

    let Opened::Open(transaction) =
        start_transaction(&mut rest, &[DEMO.to_string()], false, "un essai").unwrap()
    else {
        panic!("the API refused to open one");
    };
    assert_eq!(
        transaction.uuid,
        Uuid([
            0x7b, 0x1f, 0x2a, 0x9c, 0x3d, 0x4e, 0x4f, 0x50, 0x8a, 0x6b, 0x1c, 0x2d, 0x3e, 0x4f, 0x5a, 0x6b
        ])
    );

    let object = DataObject {
        resource: Resource {
            uri: in_demo("resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)"),
            ..Default::default()
        },
        format: "json".into(),
        blob_id: None,
        data: br#"{"$type":"resqml20.obj_HorizonInterpretation"}"#.to_vec(),
    };
    put_data_objects(&mut rest, std::slice::from_ref(&object), false).unwrap();
    assert_eq!(commit_transaction(&mut rest, &transaction).unwrap(), Ended::Done);

    let written = written.borrow();
    assert_eq!(written[0].0, "POST");
    assert_eq!(written[0].1, "/dataspaces/demo/transactions");
    // The write inside the transaction carries its id.
    assert!(
        written[1]
            .1
            .contains("transactionId=7b1f2a9c-3d4e-4f50-8a6b-1c2d3e4f5a6b"),
        "{}",
        written[1].1
    );
    assert_eq!(written[2].0, "PUT");
    assert!(
        written[2]
            .1
            .starts_with("/dataspaces/demo/transactions/7b1f2a9c-3d4e-4f50-8a6b-1c2d3e4f5a6b"),
        "{}",
        written[2].1
    );

    // Committed, the id rides no more.
    drop(written);
    put_data_objects(&mut rest, std::slice::from_ref(&object), false).unwrap();
    let last = asked.borrow().last().cloned().unwrap_or_default();
    assert!(!last.contains("transactionId"), "{last}");
}

/// Asked to end another transaction than the one it has open, the client says
/// so rather than ending the one it has.
#[test]
fn a_rest_commit_of_another_transaction_is_refused() {
    let mut rest = rest(FakeRest::default());
    let Opened::Open(_) = start_transaction(&mut rest, &[DEMO.to_string()], false, "").unwrap() else {
        panic!("the API refused to open one");
    };
    let other = retp_client::Transaction {
        uuid: Uuid([1; 16]),
        dataspaces: vec![DEMO.to_string()],
        read_only: false,
    };
    match commit_transaction(&mut rest, &other).unwrap() {
        Ended::Refused(why) => assert!(why.contains("is not"), "{why}"),
        Ended::Done => panic!("someone else's transaction was ended"),
    }
}

/// An array the message budget cuts goes over the **one** route the
/// definition has: a declaration first - shape and type, no `Data` -, then
/// each block with its `Starts` and `Counts` and the whole array's
/// dimensions, which the API wants on every item (OSDU Reservoir DMS 1.2).
#[test]
fn a_rest_array_too_big_for_a_message_is_declared_then_filled_by_starts_and_counts() {
    let fake = FakeRest::default();
    let written = std::rc::Rc::clone(&fake.written);
    let mut rest = rest(fake);
    // Bigger than the specification's default payload, which is what bounds
    // a session that states nothing.
    let count = 2_200_000;
    let array = Array {
        dimensions: vec![count as u64],
        logical: AnyLogicalArrayType::ArrayOfDouble64Le,
        transport: AnyArrayType::ArrayOfDouble,
        values: ArrayValues::Doubles((0..count).map(|i| i as f64).collect()),
        blocks: 0,
    };
    let wrote = write_arrays(&mut rest, &[(demo_id("/big"), array)], 16 * 1024 * 1024, |_| {}).unwrap();
    assert!(wrote[0].is_ok(), "{wrote:?}");

    let written = written.borrow();
    assert!(
        written.len() >= 3,
        "a declaration and at least two blocks: {}",
        written.len()
    );
    assert!(
        written
            .iter()
            .all(|(m, p, _)| m == "PUT" && p.starts_with("/dataspaces/demo/resources/arrays")),
        "{:?}",
        written
            .iter()
            .map(|(m, p, _)| (m.as_str(), p.as_str()))
            .collect::<Vec<_>>()
    );
    let declared = &written[0].2[0];
    assert!(
        declared.get("Data").is_none(),
        "the declaration carries no values: {declared}"
    );
    assert_eq!(declared["Dimensions"], serde_json::json!([count]));
    assert_eq!(declared["ArrayType"], "arrayOfDouble");
    let first = &written[1].2[0];
    assert_eq!(first["Starts"], serde_json::json!([0]));
    assert_eq!(
        first["Dimensions"],
        serde_json::json!([count]),
        "every block says the whole array's size"
    );
    let carried: usize = written[1..]
        .iter()
        .map(|(_, _, body)| body[0]["Counts"][0].as_u64().unwrap() as usize)
        .sum();
    assert_eq!(carried, count, "the blocks cover the array exactly once");
    // And the values of the first block are the first values.
    assert_eq!(first["Data"][1], 1.0);
}

/// A subarray of an array this client has never seen is refused by key and by
/// name: the API wants the whole array's dimensions, and making them up would
/// write into a shape nobody stated.
#[test]
fn a_rest_subarray_of_an_unknown_array_is_refused_by_name() {
    let mut rest = rest(FakeRest::default());
    let request = retp_types::protocol::data_array::PutDataSubarrays {
        data_subarrays: [(
            "1".to_string(),
            retp_types::datatypes::data_array_types::PutDataSubarraysType {
                uid: demo_id("/never-declared"),
                data: AnyArray {
                    item: AnyArrayItem::ArrayOfDouble(ArrayOfDouble { values: vec![1.0] }),
                },
                starts: vec![0],
                counts: vec![1],
            },
        )]
        .into_iter()
        .collect(),
    };
    let deliveries = poll_once(retp_client::Caller::call(&mut rest, &request))
        .expect("ready")
        .unwrap();
    let refused = deliveries.iter().find_map(|d| match &d.body {
        Body::Message(Message::ProtocolException(e)) => Some(e.clone()),
        _ => None,
    });
    let text = format!("{refused:?}");
    assert!(text.contains("declare the array first"), "{text}");
}

/// Bytes travel as the base64 string the definition allows in `Data`.
#[test]
fn rest_bytes_travel_as_base64() {
    let fake = FakeRest::default();
    let written = std::rc::Rc::clone(&fake.written);
    let mut rest = rest(fake);
    let array = Array {
        dimensions: vec![2],
        logical: AnyLogicalArrayType::ArrayOfInt16Be,
        transport: AnyArrayType::Bytes,
        values: ArrayValues::Bytes(vec![0x00, 0x01, 0x00, 0x02]),
        blocks: 0,
    };
    let wrote = write_arrays(&mut rest, &[(demo_id("/be"), array)], 16 * 1024 * 1024, |_| {}).unwrap();
    assert!(wrote[0].is_ok(), "{wrote:?}");
    let written = written.borrow();
    assert_eq!(written[0].2[0]["Data"], "AAEAAg==");
    assert_eq!(written[0].2[0]["ArrayType"], "bytes");
}

/// The definition answers a `PUT` with a boolean: `200 false` is a refusal,
/// and a client reading the status alone would report a write that did not
/// happen.
#[test]
fn a_rest_put_answered_false_is_a_refusal() {
    let mut rest = rest(FakeRest {
        says_false: true,
        ..Default::default()
    });
    let object = DataObject {
        resource: Resource {
            uri: in_demo("resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)"),
            ..Default::default()
        },
        format: "json".into(),
        blob_id: None,
        data: br#"{"$type":"resqml20.obj_HorizonInterpretation"}"#.to_vec(),
    };
    let put = put_data_objects(&mut rest, std::slice::from_ref(&object), false).unwrap();
    let refused = put[&object.resource.uri].as_ref().unwrap_err();
    assert!(refused.message.contains("answered false"), "{refused:?}");
}

/// **Several requests in flight, on one thread.** The store keeps its first
/// `GetDataObjects` until the second arrives (`defer_first_get`): a client
/// that sent them one after the other would wait for ever on the first.
/// `call_all` sends the window before waiting, and each answer goes to its
/// own request - in the order asked, whatever the order answered.
#[test]
fn several_requests_go_out_before_the_first_is_answered() {
    let clock = ManualClock::new(T0);
    let transport = loopback(
        server_deferring(false, false, None, true, None, T0),
        &clock,
        None,
        None,
    );
    let (offer, handlers) = reading_client("test client", "0.1", Uuid([1; 16]), &["resqml20.*".to_string()]);
    let handlers: Vec<Box<dyn Handler>> = handlers
        .into_iter()
        .filter(|h| h.offer().protocol != 24)
        .collect();
    let mut client = Client::open(transport, clock, offer, handlers, None, Settings::default()).unwrap();
    let asked = |uri: &str| GetDataObjects {
        uris: [("1".to_string(), uri.to_string())].into_iter().collect(),
        format: "xml".into(),
    };
    let answers = client.call_all(&[asked(HELD), asked(LOST)]);
    // (HELD is the object the fake holds; LOST any other.)
    assert_eq!(answers.len(), 2);
    let first = answers[0].as_ref().expect("the deferred one is answered too");
    assert!(first.iter().any(|d| matches!(&d.body, Body::Message(Message::GetDataObjectsResponse(r)) if r.data_objects.contains_key("1"))), "the first request got its object");
    let second = answers[1].as_ref().expect("the second is answered");
    assert!(
        second.iter().any(|d| d.exception().is_some()),
        "the second asked for the lost object and got its refusal"
    );
    // Nothing of it went astray.
    assert!(client.unsolicited().is_empty());
}

/// The same window, as futures: the front end's client keeps `at_once` requests
/// out and awaits them together.
#[test]
fn the_async_client_keeps_a_window_in_flight() {
    let mut harness = Harness::with(None, None, true).opened();
    let client = harness.client.clone();
    let asked = |uri: &str| GetDataObjects {
        uris: [("1".to_string(), uri.to_string())].into_iter().collect(),
        format: "xml".into(),
    };
    let requests = [asked(HELD), asked(LOST)];
    let answers = harness.drive(client.call_all(&requests, 4));
    assert_eq!(answers.len(), 2);
    let first = answers[0].as_ref().expect("the deferred one is answered too");
    assert!(first.iter().any(|d| matches!(&d.body, Body::Message(Message::GetDataObjectsResponse(r)) if r.data_objects.contains_key("1"))), "the first request got its object");
    let second = answers[1].as_ref().expect("the second is answered");
    assert!(
        second.iter().any(|d| d.exception().is_some()),
        "the second asked for the lost object and got its refusal"
    );
}

/// **A stream is read apart.** Subscribed to StoreNotification (5), what the
/// store sends on its own for it comes through `events`, and not through
/// `unsolicited` - where a front end would have to sort it out of everything
/// else. Unsubscribing hands back what was kept, so nothing is lost.
#[test]
fn a_subscribed_protocol_is_read_apart_from_the_unsolicited() {
    let clock = ManualClock::new(T0);
    let transport = loopback(
        server_deferring(false, false, None, false, None, T0),
        &clock,
        None,
        None,
    );
    let (offer, mut handlers) =
        reading_client("test client", "0.1", Uuid([1; 16]), &["resqml20.*".to_string()]);
    handlers.retain(|h| h.offer().protocol != 24);
    let mut client = Client::open(transport, clock, offer, handlers, None, Settings::default()).unwrap();
    assert!(
        client.negotiated().protocols.contains_key(&5),
        "the store agreed StoreNotification"
    );

    client.subscribe(5);
    let request = retp_types::protocol::store_notification::SubscribeNotifications {
        request: [(
            "1".to_string(),
            retp_types::datatypes::object::SubscriptionInfo {
                context: retp_types::datatypes::object::ContextInfo {
                    uri: "eml:///".into(),
                    depth: 1,
                    ..Default::default()
                },
                scope: ContextScopeKind::TargetsOrSelf,
                request_uuid: Uuid([5; 16]),
                include_object_data: false,
                format: "xml".into(),
            },
        )]
        .into_iter()
        .collect(),
    };
    let confirmed = client.call(&request).unwrap();
    assert!(
        confirmed
            .iter()
            .any(|d| matches!(&d.body, Body::Message(Message::SubscribeNotificationsResponse(_))))
    );

    let events = client.events(5, std::time::Duration::from_millis(10)).unwrap();
    assert_eq!(
        events.len(),
        1,
        "the change the store sent on its own: {events:?}"
    );
    assert!(
        matches!(&events[0], retp_proto::connection::Event::Delivered(d) if matches!(&d.body, Body::Message(Message::ObjectChanged(_))))
    );
    assert!(
        client.unsolicited().is_empty(),
        "nothing of the stream went with the unsolicited"
    );
    // Taken once: asked again, nothing.
    assert!(
        client
            .events(5, std::time::Duration::from_millis(10))
            .unwrap()
            .is_empty()
    );

    // What is kept and not taken is handed back on unsubscribing. The
    // change follows the confirmation on the wire: a moment for it to land.
    client.call(&request).unwrap();
    client.idle(std::time::Duration::from_millis(10)).unwrap();
    client.unsubscribe(5);
    let back = client.unsolicited();
    assert_eq!(back.len(), 1, "{back:?}");
}

/// Three small doubles arrays, `n` values each.
fn small_arrays(n: usize, paths: &[&str]) -> Vec<(DataArrayIdentifier, Array)> {
    paths
        .iter()
        .map(|path| {
            (
                demo_id(path),
                Array {
                    dimensions: vec![n as u64],
                    logical: AnyLogicalArrayType::ArrayOfDouble64Le,
                    transport: AnyArrayType::ArrayOfDouble,
                    values: ArrayValues::Doubles((0..n).map(|i| i as f64).collect()),
                    blocks: 0,
                },
            )
        })
        .collect()
}

/// **Several arrays travel in one message, and each answers for itself.**
/// Three arrays that fit together cost one `PutDataArrays`, not three (a
/// round trip costs more than what it carries: 300 of them made a push to
/// one RDDMS take eight minutes). The store refuses the first by its key:
/// the second and the third are written, the first is named, nothing else
/// is carried off - first and not middle, so that an answer given to the
/// wrong key cannot pass by symmetry. And the bypass does what it says:
/// one message each.
#[test]
fn whole_arrays_go_together_and_each_answers_by_its_key() {
    const PUT_DATA_ARRAYS: (i32, i32) = (9, 4);
    let (mut client, puts) = counting_writes(None, PUT_DATA_ARRAYS);
    let writes = small_arrays(4, &["/refused", "/b", "/c"]);
    let written = write_arrays(&mut client, &writes, 16 * 1024 * 1024, |_| {}).unwrap();
    assert_eq!(
        puts.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "three arrays that fit went in three messages"
    );
    assert!(written[1].is_ok() && written[2].is_ok(), "{written:?}");
    match &written[0] {
        Err(ArrayFailure::Store(error)) => assert!(error.message.contains("not this one"), "{error:?}"),
        other => panic!("the refused key was read as {other:?}"),
    }
    let back = read_arrays(
        &mut client,
        &[demo_id("/b"), demo_id("/c")],
        16 * 1024 * 1024,
        |_| {},
    )
    .unwrap();
    assert!(back.iter().all(Result::is_ok), "{back:?}");

    let (mut client, puts) = counting_writes(None, PUT_DATA_ARRAYS);
    let written =
        write_arrays_with(&mut client, &writes, 16 * 1024 * 1024, Grouping::OneByOne, |_| {}).unwrap();
    assert_eq!(
        puts.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "one by one is one message each"
    );
    assert!(
        written[0].is_err() && written[1].is_ok() && written[2].is_ok(),
        "{written:?}"
    );
}

/// **The budget cuts the groups, not the arrays.** With a message of 4096
/// bytes - 2816 for the values once the header's share is kept -, three
/// arrays of 640 bytes ride together, their identifiers counted; two of
/// 1600 cannot, and go in two messages - each still whole, since each fits
/// alone.
#[test]
fn a_group_is_cut_where_the_message_budget_says() {
    const PUT_DATA_ARRAYS: (i32, i32) = (9, 4);
    let (mut client, puts) = counting_writes(Some(4096), PUT_DATA_ARRAYS);
    let written = write_arrays(&mut client, &small_arrays(80, &["/p", "/q", "/r"]), 4096, |_| {}).unwrap();
    assert!(written.iter().all(Result::is_ok), "{written:?}");
    assert_eq!(
        puts.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "1920 bytes of three arrays fit one message of 4096"
    );

    let (mut client, puts) = counting_writes(Some(4096), PUT_DATA_ARRAYS);
    let mut planned = Vec::new();
    let written = write_arrays(&mut client, &small_arrays(200, &["/s", "/t"]), 4096, |event| {
        if let ArrayProgress::Planned { blocks, .. } = event {
            planned.push(blocks);
        }
    })
    .unwrap();
    assert!(written.iter().all(Result::is_ok), "{written:?}");
    assert_eq!(planned, [1, 1], "each fits alone, so each is whole");
    assert_eq!(
        puts.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "3200 bytes of two arrays do not fit one message of 4096"
    );
}

/// **Blocked arrays are declared together and filled in flight, and come
/// back intact.** Two arrays too big for the message: one
/// `PutUninitializedDataArrays` for both, then their blocks - and what is
/// read back is what was written, value for value, which is what a block
/// sent to the wrong array or the wrong offset would break.
#[test]
fn blocked_arrays_are_declared_together_and_filled_in_flight() {
    const DECLARE: (i32, i32) = (9, 9);
    let (mut client, declarations) = counting_writes(Some(4096), DECLARE);
    let writes = small_arrays(600, &["/u", "/v"]);
    let mut blocks = BTreeMap::new();
    let written = write_arrays(&mut client, &writes, 4096, |event| {
        if let ArrayProgress::Written { path, done, of } = event {
            blocks.insert(path, (done, of));
        }
    })
    .unwrap();
    assert!(written.iter().all(Result::is_ok), "{written:?}");
    assert_eq!(
        declarations.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "two arrays, one declaration"
    );
    assert!(
        blocks.values().all(|(done, of)| done == of && *of > 1),
        "{blocks:?}"
    );
    let back = read_arrays(&mut client, &[demo_id("/u"), demo_id("/v")], 4096, |_| {}).unwrap();
    for (one, (_, array)) in back.iter().zip(&writes) {
        let one = one.as_ref().unwrap();
        assert_eq!(
            one.values.to_f64s(one.logical).unwrap(),
            array.values.to_f64s(array.logical).unwrap()
        );
    }
}

/// A channel as the fake store describes one.
fn channel_record(id: i64, uri: &str) -> retp_types::datatypes::channel_data::ChannelMetadataRecord {
    retp_types::datatypes::channel_data::ChannelMetadataRecord {
        uri: uri.to_string(),
        id,
        channel_name: "GR".into(),
        uom: "gAPI".into(),
        ..Default::default()
    }
}

/// One value of a channel at one index.
fn data_item(channel: i64, index: i64, value: f64) -> retp_types::datatypes::channel_data::DataItem {
    use retp_types::datatypes::{DataValue, DataValueItem, IndexValue, IndexValueItem};
    retp_types::datatypes::channel_data::DataItem {
        channel_id: channel,
        indexes: vec![IndexValue {
            item: IndexValueItem::Long(index),
        }],
        value: DataValue {
            item: DataValueItem::Double(value),
        },
        value_attributes: Vec::new(),
    }
}

/// **A watch is confirmed by its key, and what the store says on its own
/// about it comes typed, apart from everything else.** Then it is ended,
/// and the store says so. What the store sent for the watch never lands
/// with the unsolicited - a front end would have had to sort it out of the
/// faults and timeouts there.
#[test]
fn a_watch_is_confirmed_and_its_changes_read_as_notifications() {
    use retp_client::notifications::{
        Notification, Watch, notifications, subscribe_notifications, unsubscribe_notifications,
    };
    let mut client = open(false, None);
    let watch = Watch::of(DEMO, Uuid([5; 16]));
    let confirmed = subscribe_notifications(&mut client, &[watch]).unwrap();
    assert_eq!(confirmed, [Ok(())]);

    client.idle(std::time::Duration::from_millis(10)).unwrap();
    let heard = notifications(&mut client).unwrap();
    assert_eq!(heard.len(), 1, "{heard:?}");
    match &heard[0] {
        Notification::Changed { change, subscription } => {
            assert_eq!(*subscription, Uuid([5; 16]));
            assert_eq!(change.data_object.resource.uri, HELD);
        }
        other => panic!("the change came as {other:?}"),
    }
    assert!(
        client.unsolicited().is_empty(),
        "the watch's stream went with the unsolicited"
    );

    let ended = unsubscribe_notifications(&mut client, Uuid([5; 16])).unwrap();
    assert_eq!(ended, Ok("unsubscribed".to_string()));
}

/// **The catch-up from a watermark**: what was written and what was
/// deleted since a time, by the store's clock - asked from before T0, one
/// of each; from after, nothing. What a client back from a cut asks
/// instead of everything.
#[test]
fn changes_since_a_watermark_list_what_was_written_and_deleted() {
    use retp_client::notifications::changes_since;
    let mut client = open(false, None);
    let before = changes_since(&mut client, DEMO, T0 - 1, &[]).unwrap();
    assert_eq!(before.written.len(), 1, "{:?}", before.written);
    assert!(before.written[0].uri.contains("421a7a05"));
    assert_eq!(before.deleted.len(), 1, "{:?}", before.deleted);
    assert!(before.deleted[0].uri.contains("00000000-0000"));
    let after = changes_since(&mut client, DEMO, T0 + 1, &[]).unwrap();
    assert!(after.written.is_empty() && after.deleted.is_empty(), "{after:?}");
}

/// **Channels are described, subscribed, read apart, ranged and
/// unsubscribed** (21). The data the store sends on its own after the
/// subscription comes typed through the channel stream, not with the
/// unsolicited.
#[test]
fn channels_are_described_subscribed_and_their_data_read_apart() {
    use retp_client::channels::{
        CHANNEL_SUBSCRIBE, ChannelEvent, channel_events, channel_metadata, get_ranges, subscribe_channels,
        unsubscribe_channels,
    };
    use retp_types::datatypes::channel_data::{ChannelRangeInfo, ChannelSubscribeInfo};
    let mut client = open(false, None);
    let uri = "eml:///witsml20.Channel(00000000-0000-0000-0000-000000000007)".to_string();
    let described = channel_metadata(&mut client, std::slice::from_ref(&uri)).unwrap();
    let record = described[0].as_ref().expect("described");
    assert_eq!(
        (record.id, record.channel_name.as_str(), record.uri.as_str()),
        (7, "GR", uri.as_str())
    );

    let subscribed = subscribe_channels(
        &mut client,
        &[ChannelSubscribeInfo {
            channel_id: 7,
            data_changes: true,
            ..Default::default()
        }],
    )
    .unwrap();
    assert_eq!(subscribed, [Ok(())]);
    client.idle(std::time::Duration::from_millis(10)).unwrap();
    let events = channel_events(&mut client, CHANNEL_SUBSCRIBE).unwrap();
    match events.as_slice() {
        [ChannelEvent::Data(items)] => {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].channel_id, 7);
        }
        other => panic!("the data came as {other:?}"),
    }
    assert!(
        client.unsolicited().is_empty(),
        "the channel's stream went with the unsolicited"
    );

    let ranged = get_ranges(
        &mut client,
        Uuid([21; 16]),
        &[ChannelRangeInfo {
            channel_ids: vec![7],
            ..Default::default()
        }],
    )
    .unwrap();
    assert_eq!(ranged.map(|items| items.len()), Ok(2));

    let stopped = unsubscribe_channels(&mut client, &[7]).unwrap();
    assert_eq!(stopped, [Ok(())]);
}

/// **A stream is started with a message that expects no answer** (1):
/// `StartStreaming` goes out, nothing waits for it, and what the producer
/// then sends on its own - its channels, their values - is read from the
/// stream. And a caller without a wire refuses to say it, by name.
#[test]
fn a_stream_is_started_by_a_message_that_expects_no_answer() {
    use retp_client::channels::{
        CHANNEL_STREAMING, ChannelEvent, channel_events, start_streaming, stop_streaming,
    };
    let mut client = open(false, None);
    assert!(
        client.negotiated().protocols.contains_key(&1),
        "the producer agreed ChannelStreaming"
    );
    start_streaming(&mut client).unwrap();
    client.idle(std::time::Duration::from_millis(10)).unwrap();
    let events = channel_events(&mut client, CHANNEL_STREAMING).unwrap();
    match events.as_slice() {
        [ChannelEvent::Metadata(channels), ChannelEvent::Data(items)] => {
            assert_eq!(channels[0].id, 9);
            assert_eq!(items[0].channel_id, 9);
        }
        other => panic!("the stream came as {other:?}"),
    }
    stop_streaming(&mut client).unwrap();
    assert!(client.unsolicited().is_empty());

    let mut rest = rest(FakeRest::default());
    match retp_client::channels::start_streaming(&mut rest) {
        Err(ClientError::Invalid(why)) => assert!(why.to_string().contains("no wire"), "{why}"),
        other => panic!("a caller without a wire said it anyway: {other:?}"),
    }
}

/// **A stream ends when the connection does.** Subscribed to a protocol,
/// whoever reads its events after the transport fell finds the end there -
/// not with the unsolicited, where a front end's stream reader would never look,
/// and not nowhere, which is a reader waiting for ever on a socket that is
/// gone.
#[test]
fn a_lost_connection_ends_the_streams() {
    // Falls on the second message sent: the handshake goes, the ping does not.
    let mut client = open(false, Some(2));
    client.subscribe(5);
    assert!(client.ping().is_err(), "the transport was to fall on the ping");
    let events = client
        .events(5, std::time::Duration::from_millis(10))
        .unwrap_or_default();
    assert!(
        matches!(events.as_slice(), [retp_proto::connection::Event::Closed { reason }] if reason.contains("lost")),
        "the stream did not hear the end: {events:?}"
    );
}

/// **The server's own resume is tried first, and a refusal is said, not
/// swallowed.** ADR-0003 fixes the order: OSDU's resume when the connector
/// offers one, a whole new session otherwise or when it was refused. The
/// refusal reaches whoever is watching, because a client that silently
/// reconnects the long way when the short one was expected is a client
/// nobody can debug.
#[test]
fn the_server_is_asked_to_take_the_session_back_before_a_new_one_is_opened() {
    // The server takes it back: no fresh session is opened for the retry.
    let directory = scratch("resumed");
    let mut connections = Connections {
        resume: Some(Ok(())),
        ..Connections::new(&[
            Plan {
                falls_after: Some(3),
                ..Default::default()
            },
            Plan::default(),
        ])
    };
    let (result, setbacks) = resumable(&mut connections, &directory, 1, Retry::default());
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(connections.resumes, 1, "the resume was not tried first");
    let reopened: Vec<&Setback> = setbacks
        .iter()
        .filter(|s| matches!(s, Setback::Reopened { .. }))
        .collect();
    assert!(
        matches!(
            reopened.as_slice(),
            [Setback::Reopened {
                resumed: true,
                refused: None,
                ..
            }]
        ),
        "{setbacks:#?}"
    );

    // It refuses: the way back is the whole one, and the reason is said.
    let directory = scratch("resume-refused");
    let mut connections = Connections {
        resume: Some(Err("Session not found or expired".into())),
        ..Connections::new(&[
            Plan {
                falls_after: Some(3),
                ..Default::default()
            },
            Plan::default(),
        ])
    };
    let (result, setbacks) = resumable(&mut connections, &directory, 1, Retry::default());
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(connections.resumes, 1);
    let said = setbacks.iter().find_map(|s| match s {
        Setback::Reopened {
            resumed: false,
            refused: Some(why),
            ..
        } => Some(why.clone()),
        _ => None,
    });
    assert_eq!(
        said.as_deref(),
        Some("Session not found or expired"),
        "{setbacks:#?}"
    );
}

/// **A `null` in a JSON array is a no-value, and this client writes them
/// itself.** JSON has neither `NaN` nor `Infinity` (RFC 8259), so the only
/// valid way to write *no value* - which is what a RESQML continuous
/// property holds for every inactive cell - is `null`. Refusing it lost the
/// **whole array**, which is the worst shape of failure: a property that
/// reads as unreadable rather than as one cell without a value.
#[test]
fn a_null_in_a_json_array_is_a_no_value_not_a_refusal() {
    const METADATA: &str = r#"{"uid":{"uri":"eml:///dataspace('demo')/eml20.obj_EpcExternalPartReference(421a7a05-033a-450d-bcef-051352023578)","pathInResource":"/points"},"dimensions":[4],"preferredSubarrayDimensions":[],"logicalArrayType":0,"transportArrayType":4,"storeLastWrite":"1970-01-01T00:00:00.000Z","storeCreated":"1970-01-01T00:00:00.000Z","customData":{}}"#;
    const VALUES: &str = r#"{"uid":{},"data":{"data":[1.5,null,3.5,null],"dimensions":[4]}}"#;
    let mut rest = rest(FakeRest {
        recorded: Some(("/points", METADATA, VALUES)),
        ..Default::default()
    });
    let read = read_arrays(&mut rest, &[demo_id("/points")], 16 * 1024 * 1024, |_| {}).unwrap();
    let array = read[0].as_ref().expect("the array is readable");
    let values = array.values.to_f64s(array.logical).unwrap();
    assert_eq!(values.len(), 4);
    assert_eq!((values[0], values[2]), (1.5, 3.5));
    assert!(values[1].is_nan() && values[3].is_nan(), "{values:?}");
}
