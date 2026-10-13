//! The second transport: the REST API OSDU's Reservoir DDMS serves beside its
//! ETP WebSocket.
//!
//! A [`RestClient`] **is a [`Caller`]**: it takes the ETP request an operation
//! sends - `GetResources`, `GetDataObjects`, `GetDataSubarrays`… -, makes the
//! REST calls it takes, and gives back the ETP answer a server would have
//! sent. Everything above - the operations, the array planner, the resumed
//! download, the storage bridge, the shell - runs on it unchanged.
//!
//! It does no input or output itself: it builds [`HttpRequest`]s and reads
//! [`HttpResponse`]s, and an [`Http`] sends them - `ureq` natively, `fetch` in
//! a front end. What REST has no counterpart for is refused by name, never
//! answered empty (§6 of the design).

use std::collections::BTreeMap;
use std::future::Future;

use base64::Engine as _;
use energyml_core::identifiers::EtpUri;
use retp_avro::{Avro, EtpMessage, Reader, Role, Writer};
use retp_proto::capabilities::{Capabilities, Scope};
use retp_proto::credentials::Credentials;
use retp_proto::negotiate::{Negotiated, NegotiatedProtocol, v12};
use retp_proto::wire::{Body, Delivery, EtpError, Flags, Place, Tolerance};
use retp_types::datatypes::data_array_types::{DataArray, DataArrayIdentifier, DataArrayMetadata};
use retp_types::datatypes::object::PutResponse;
use retp_types::datatypes::object::{
    ActiveStatusKind, ContextScopeKind, DataObject, Dataspace, Edge, RelationshipKind, Resource,
    SupportedType,
};
use retp_types::datatypes::{
    AnyArray, AnyArrayItem, AnyArrayType, AnyLogicalArrayType, ArrayOfBoolean, ArrayOfDouble, ArrayOfFloat,
    ArrayOfInt, ArrayOfLong, ArrayOfString, DataValue, DataValueItem, ErrorInfo, MessageHeader, Uuid,
};
use retp_types::dispatch::{self, Message};
use retp_types::protocol::core::ProtocolException;
use retp_types::protocol::data_array::{
    GetDataArrayMetadataResponse, GetDataArraysResponse, GetDataSubarraysResponse,
};
use retp_types::protocol::data_array::{
    PutDataArraysResponse, PutDataSubarraysResponse, PutUninitializedDataArraysResponse,
};
use retp_types::protocol::dataspace::{
    DeleteDataspacesResponse, GetDataspacesResponse, PutDataspacesResponse,
};
use retp_types::protocol::discovery::{GetResourcesEdgesResponse, GetResourcesResponse};
use retp_types::protocol::store::GetDataObjectsResponse;
use retp_types::protocol::store::{DeleteDataObjectsResponse, PutDataObjectsResponse};
use retp_types::protocol::supported_types::GetSupportedTypesResponse;
use retp_types::protocol::transaction::{
    CommitTransactionResponse, RollbackTransactionResponse, StartTransactionResponse,
};
use serde_json::Value as Json;

use crate::caller::Caller;
use crate::{ClientError, TransportError};

/// An HTTP method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    /// `GET`.
    Get,
    /// `POST`.
    Post,
    /// `PUT`.
    Put,
    /// `DELETE`.
    Delete,
}

/// A request to send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    /// Its method.
    pub method: Method,
    /// The whole URL, query string included.
    pub url: String,
    /// Its headers - credentials among them: never print a request whole.
    pub headers: Vec<(String, String)>,
    /// Its body, if any.
    pub body: Option<Vec<u8>>,
}

/// What came back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    /// The status code.
    pub status: u16,
    /// `WWW-Authenticate`, when the server sent one.
    pub challenge: Option<String>,
    /// The body.
    pub body: Vec<u8>,
}

/// What sends a request: `ureq` natively, `fetch` in a front end.
pub trait Http {
    /// Send `request`; a failure to reach the server is a [`TransportError`],
    /// an answer - whatever its status - is not.
    fn send(&mut self, request: HttpRequest) -> impl Future<Output = Result<HttpResponse, TransportError>>;

    /// Send them all, at most `at_once` of them in flight, and answer **in
    /// the order asked**. The client hands over every request of one ETP
    /// message at once, because a round trip to an OSDU server costs far more
    /// than the answer itself (measured on one OSDU service: ~0.6 s a request,
    /// whatever it carries). This default sends them one after the other; a
    /// transport that can do better says so - `ureq` by threads, `fetch` by
    /// letting the front end run them.
    fn send_all(
        &mut self,
        requests: Vec<HttpRequest>,
        at_once: usize,
    ) -> impl Future<Output = Vec<Result<HttpResponse, TransportError>>> {
        async move {
            let _ = at_once;
            let mut answers = Vec::with_capacity(requests.len());
            for request in requests {
                answers.push(self.send(request).await);
            }
            answers
        }
    }
}

/// An [`Http`] whose requests are over when they return - `ureq`: a REST
/// client over it is [`Blocking`](crate::Blocking), and the functions at the
/// root of the crate run on it.
pub trait BlockingHttp: Http {}

impl<H: BlockingHttp> crate::caller::Blocking for RestClient<H> {}

/// How many resources one page of a listing asks for (`$top`).
pub const PAGE: usize = 1000;

/// How many requests of one ETP message go to the server at once, unless
/// told otherwise - polite, and already most of what parallelism gives.
pub const AT_ONCE: usize = 4;

/// An ETP client over the Reservoir DDMS REST API.
pub struct RestClient<H: Http> {
    http: H,
    /// `https://…/api/reservoir-ddms/v2`, without its last slash.
    base: String,
    credentials: Option<Credentials>,
    partition: Option<String>,
    page: usize,
    at_once: usize,
    /// Whether this server reads several objects in one request.
    ///
    /// **Not every Reservoir DDMS REST API has it.** OSDU's does
    /// (`POST /dataspaces/multi-resources/get-content`); another RDDMS's, which
    /// carries the same shapes under `/etp`, does not - its description lists
    /// 34 paths and that is not one of them. Unknown until a
    /// plural read is tried: `None` means *not asked yet*, and a 404 settles
    /// it for the life of the client rather than once per read.
    plural: Option<bool>,
    next_id: i64,
    negotiated: Negotiated,
    tolerance: Tolerance,
    /// Each array's metadata, as last read, with whether the store named a
    /// transport type for it - see [`Carriage`].
    metadata: BTreeMap<(String, String), Carriage>,
    said_no_kind: bool,
    /// The transaction this client has open, if one - see [`RestTransaction`].
    transaction: Option<RestTransaction>,
}

/// A REST transaction: **one dataspace**, an id the API chose, and the uuid
/// ETP carries it as.
///
/// ETP's `StartTransaction` spans a list of dataspaces and the session *is*
/// the transaction; the Reservoir DDMS opens one **per dataspace**
/// (`POST /dataspaces/{id}/transactions`) and wants its id on every request
/// after - `transactionId` - until `PUT` commits it or `DELETE` rolls it
/// back. So the client keeps the id and puts it on every
/// call, which is what was asked for: caching the transaction id.
#[derive(Debug, Clone)]
struct RestTransaction {
    dataspace: String,
    id: String,
    uuid: Uuid,
}

impl<H: Http> RestClient<H> {
    /// A client of the API at `base` (`https://…/api/reservoir-ddms/v2`),
    /// sending `credentials` and the OSDU `partition` with every request.
    pub fn new(http: H, base: &str, credentials: Option<Credentials>, partition: Option<String>) -> Self {
        let base = base.trim_end_matches('/').to_string();
        Self {
            http,
            negotiated: described(&base),
            base,
            credentials,
            partition,
            page: PAGE,
            at_once: AT_ONCE,
            plural: None,
            next_id: 1,
            tolerance: Tolerance::Tolerant,
            metadata: BTreeMap::new(),
            said_no_kind: false,
            transaction: None,
        }
    }

    /// Ask for `resources` resources per page of a listing - a test asks for
    /// few, to see the pages followed.
    pub fn with_page(mut self, resources: usize) -> Self {
        self.page = resources.max(1);
        self
    }

    /// How many requests of one ETP message the transport may run at once;
    /// [`AT_ONCE`] by default, `1` to send them one after the other.
    pub fn with_at_once(mut self, requests: usize) -> Self {
        self.at_once = requests.max(1);
        self
    }

    /// The most an array's read may return at once, in bytes - the REST API
    /// does not say its own, where ETP announces `MaxDataArraySize`. The
    /// array planner cuts larger arrays into blocks under it; without it, the
    /// planner's default budget (16 MiB) holds.
    pub fn with_array_limit(mut self, bytes: i64) -> Self {
        if let Some(protocol) = self.negotiated.protocols.get_mut(&9)
            && let Ok(caps) = Capabilities::empty(Scope::Protocol).set(
                retp_proto::capabilities::names::MAX_DATA_ARRAY_SIZE,
                retp_proto::capabilities::Value::Long(bytes),
            )
        {
            protocol.theirs = caps;
        }
        self
    }

    /// The API's base URL.
    pub fn base(&self) -> &str {
        &self.base
    }

    /// What the server says of its build (`GET /health/info`): its version,
    /// and that it answers - the REST counterpart of a `Ping`, without the
    /// server's clock a `Pong` carries.
    /// Whether the server is there and answering, and what it calls itself.
    ///
    /// `/health/info` when it has one; **`GET /dataspaces` when it has not**,
    /// which is not a lesser check - it is the first thing a front end asks
    /// anyway. Another RDDMS REST API carries the Reservoir DDMS shapes without
    /// that endpoint, and taking its absence for "this is not a
    /// REST API" refused a server that works.
    pub async fn alive(&mut self) -> Result<String, ClientError> {
        match self.health().await {
            Ok(said) => Ok(said),
            Err(ClientError::Transport(_)) => {
                // It answers `/dataspaces` or it does not answer at all: the
                // error of that request is the one worth showing, not the
                // absence of a health endpoint.
                self.dataspaces(0).await?;
                Ok("a Reservoir DDMS REST API without /health/info".to_string())
            }
            Err(other) => Err(other),
        }
    }

    pub async fn health(&mut self) -> Result<String, ClientError> {
        let json = match self.get("/health/info".to_string()).await? {
            Ok(json) => json,
            // Said as what it means for whoever typed the URL, not as the
            // code the server sent. A 404 here is the ordinary mistake - the
            // path of an ETP WebSocket given to the REST client - and the
            // answer is in the message rather than in a document nobody has
            // open (a user reported that `https://…/etp` answered
            // `ENOT_FOUND: ENOT_FOUND: {"detail":"Not Found"}`, which says
            // the code twice and what to do never).
            Err(error) if error.code == EtpError::ENOT_FOUND.code() => {
                return Err(ClientError::Transport(TransportError::new(format!(
                    "{}/health/info answers `not found`: this is not the root of a Reservoir DDMS REST API. OSDU serves it at `https://<host>/api/reservoir-ddms/v2`; an ETP server is reached at `wss://<host>/<path>` instead, which is the same server by its other door.",
                    self.base
                ))));
            }
            Err(error) => {
                return Err(ClientError::Transport(TransportError::new(format!(
                    "{}/health/info: {}",
                    self.base, error.message
                ))));
            }
        };
        Ok(format!(
            "{} {} ({})",
            text(&json, "artifactId"),
            text(&json, "version"),
            text(&json, "buildTime")
        ))
    }

    async fn answer(&mut self, message: Message) -> Result<Vec<Delivery>, ClientError> {
        let request = self.next_id;
        self.next_id += 2;
        match message {
            Message::GetDataspaces(_) => self.dataspaces(request).await,
            Message::GetResources(r) => self.resources(request, r).await,
            Message::GetSupportedTypes(r) => self.supported_types(request, r).await,
            Message::GetDataObjects(r) => self.data_objects(request, r).await,
            Message::GetDataArrayMetadata(r) => self.array_metadata(request, r.data_arrays).await,
            Message::GetDataArrays(r) => {
                let asked = r.data_arrays.into_iter().map(|(k, id)| (k, (id, None))).collect();
                self.arrays(request, asked, false).await
            }
            Message::GetDataSubarrays(r) => {
                let asked = r
                    .data_subarrays
                    .into_iter()
                    .map(|(k, s)| (k, (s.uid, Some((s.starts, s.counts)))))
                    .collect();
                self.arrays(request, asked, true).await
            }
            Message::PutDataObjects(r) => self.put_objects(request, r).await,
            Message::DeleteDataObjects(r) => self.delete_objects(request, r).await,
            Message::PutDataspaces(r) => self.put_dataspaces(request, r).await,
            Message::DeleteDataspaces(r) => self.delete_dataspaces(request, r).await,
            Message::PutDataArrays(r) => self.put_arrays(request, r).await,
            Message::PutUninitializedDataArrays(r) => self.put_uninitialized(request, r).await,
            Message::PutDataSubarrays(r) => self.put_subarrays(request, r).await,
            Message::StartTransaction(r) => self.start_transaction(request, r).await,
            Message::CommitTransaction(r) => self.end_transaction(request, r.transaction_uuid, true).await,
            Message::RollbackTransaction(r) => self.end_transaction(request, r.transaction_uuid, false).await,
            other => {
                let (protocol, message_type) = other.header();
                Err(unsupported(format!(
                    "message {protocol}/{message_type} has no counterpart in the Reservoir DDMS REST API - use the ETP WebSocket"
                )))
            }
        }
    }

    // -- the translations -------------------------------------------------

    async fn dataspaces(&mut self, request: i64) -> Result<Vec<Delivery>, ClientError> {
        let answer = self.get("/dataspaces".to_string()).await?;
        let body = match answer {
            Ok(Json::Array(items)) => Message::GetDataspacesResponse(GetDataspacesResponse {
                dataspaces: items.iter().map(dataspace).collect(),
            }),
            Ok(other) => return Err(unreadable("/dataspaces", &other)),
            Err(error) => single(error),
        };
        Ok(vec![self.delivery(request, body, true)])
    }

    async fn resources(
        &mut self,
        request: i64,
        asked: retp_types::protocol::discovery::GetResources,
    ) -> Result<Vec<Delivery>, ClientError> {
        let context = asked.context;
        let place = match Target::of(&context.uri)? {
            Target::Dataspace(id) => format!("/dataspaces/{}/{{}}/all", encode(&id)),
            Target::Object {
                dataspace,
                qualified_type,
                uuid,
            } => {
                let direction = match asked.scope {
                    ContextScopeKind::Targets => "targets",
                    ContextScopeKind::Sources => "sources",
                    other => {
                        return Err(unsupported(format!(
                            "scope {other:?} from an object: the REST API lists an object's targets or its sources, never the object itself"
                        )));
                    }
                };
                format!(
                    "/dataspaces/{}/{{}}/{qualified_type}/{uuid}/{direction}",
                    encode(&dataspace)
                )
            }
        };
        let kind = if asked.include_edges { "graph" } else { "resources" };
        let path = place.replacen("{}", kind, 1);
        // A wildcard type is filtered here, the API taking exact types.
        let (exact, families): (Vec<&String>, Vec<&String>) =
            context.data_object_types.iter().partition(|t| !t.ends_with(".*"));
        let mut query: Vec<(String, String)> = Vec::new();
        if families.is_empty() && !exact.is_empty() {
            query.push(("dataObjectTypes".into(), context.data_object_types.join(",")));
        }
        if asked.count_objects {
            query.push(("countObjects".into(), "true".into()));
        }
        if context.depth > 1 && matches!(Target::of(&context.uri)?, Target::Object { .. }) {
            query.push(("depth".into(), context.depth.to_string()));
        }
        if let Some(after) = asked.store_last_write_filter {
            query.push(("storeLastWriteFilter".into(), iso(after)));
        }

        let mut resources = Vec::new();
        let mut edges = Vec::new();
        let mut skip = 0;
        loop {
            let mut page = query.clone();
            page.push(("$skip".into(), skip.to_string()));
            page.push(("$top".into(), self.page.to_string()));
            let answer = self.get(format!("{path}?{}", query_string(&page))).await?;
            let json = match answer {
                Ok(json) => json,
                Err(error) => return Ok(vec![self.delivery(request, single(error), true)]),
            };
            let (items, links) = match &json {
                Json::Array(items) => (items.clone(), Vec::new()),
                Json::Object(o) => (
                    o.get("resources")
                        .and_then(Json::as_array)
                        .cloned()
                        .unwrap_or_default(),
                    o.get("links")
                        .and_then(Json::as_array)
                        .cloned()
                        .unwrap_or_default(),
                ),
                other => return Err(unreadable(&path, other)),
            };
            let count = items.len();
            resources.extend(items.iter().map(resource));
            edges.extend(links.iter().map(edge));
            if count < self.page {
                break;
            }
            skip += count;
        }
        if !families.is_empty() {
            let wanted = &context.data_object_types;
            resources.retain(|r| wanted.iter().any(|w| covers(w, &type_of(&r.uri))));
        }
        if !edges.is_empty() && !self.said_no_kind {
            self.said_no_kind = true;
            log::warn!(
                target: "retp_proto",
                "{}: the REST graph does not say whether a relationship is primary or secondary; each edge is given as Primary, its reference path in customData",
                self.base
            );
        }
        let mut out = vec![self.delivery(
            request,
            Message::GetResourcesResponse(GetResourcesResponse { resources }),
            edges.is_empty(),
        )];
        if !edges.is_empty() {
            out.push(self.delivery(
                request,
                Message::GetResourcesEdgesResponse(GetResourcesEdgesResponse { edges }),
                true,
            ));
        }
        Ok(out)
    }

    async fn supported_types(
        &mut self,
        request: i64,
        asked: retp_types::protocol::supported_types::GetSupportedTypes,
    ) -> Result<Vec<Delivery>, ClientError> {
        let Target::Dataspace(id) = Target::of(&asked.uri)? else {
            return Err(unsupported(
                "the types found from an object: the REST API only counts a dataspace's".into(),
            ));
        };
        if asked.return_empty_types {
            return Err(unsupported(
                "the types a store supports and holds none of: the REST API lists only the types a dataspace holds".into(),
            ));
        }
        let answer = self.get(format!("/dataspaces/{}/resources", encode(&id))).await?;
        let body = match answer {
            Ok(Json::Array(items)) => Message::GetSupportedTypesResponse(GetSupportedTypesResponse {
                supported_types: items
                    .iter()
                    .map(|t| SupportedType {
                        data_object_type: text(t, "name"),
                        object_count: asked
                            .count_objects
                            .then(|| t.get("count").and_then(Json::as_i64).unwrap_or(0) as i32),
                        relationship_kind: RelationshipKind::Primary,
                    })
                    .collect(),
            }),
            Ok(other) => return Err(unreadable("types", &other)),
            Err(error) => single(error),
        };
        Ok(vec![self.delivery(request, body, true)])
    }

    /// `GetDataObjects`, in **one** request per dataspace: `POST
    /// /dataspaces/multi-resources/get-content`. The answers come back keyed
    /// by uuid - the API does not say which request each answers, and a uuid
    /// names one object in one dataspace only -; an object it does not return
    /// is one it does not hold: the API omits it. In JSON, what it holds and
    /// cannot say in JSON is told apart ([`RestClient::json_objects`]).
    async fn data_objects(
        &mut self,
        request: i64,
        asked: retp_types::protocol::store::GetDataObjects,
    ) -> Result<Vec<Delivery>, ClientError> {
        let format = asked.format.clone();
        let mut errors = BTreeMap::new();
        let mut groups: BTreeMap<String, Vec<(String, Asked)>> = BTreeMap::new();
        for (key, uri) in asked.uris {
            match Target::of(&uri) {
                Ok(Target::Object { dataspace, uuid, .. }) => {
                    let uuid = uuid.to_ascii_lowercase();
                    groups.entry(dataspace).or_default().push((key, (uri, uuid)));
                }
                _ => {
                    errors.insert(
                        key,
                        EtpError::EINVALID_URI
                            .with_detail(format!("{uri} names no object"))
                            .error_info(),
                    );
                }
            }
        }
        let mut found = BTreeMap::new();
        for wanted in groups.into_values() {
            let objects: Vec<Asked> = wanted.iter().map(|(_, asked)| asked.clone()).collect();
            // A server without the plural endpoint is served one object per
            // request. Slower - a round trip each - and said nowhere else:
            // what matters is that the objects come back.
            if self.plural == Some(false) {
                let answered = self.one_by_one(&objects, &format).await?;
                take(answered, wanted, &format, &mut found, &mut errors);
                continue;
            }
            let answered = if format == "json" {
                // The API's default, spelled: with `referencedContent=false`
                // it answers an object without JSON by a 500 (see
                // `json_objects`). What it embeds is taken out after.
                self.json_objects(&objects, "referencedContent=true")
                    .await?
                    .map(|read| {
                        let found = read.found.into_iter().map(|(uuid, mut json)| {
                            strip_embedded(&mut json);
                            (uuid, serde_json::to_vec(&json).expect("JSON"))
                        });
                        (found.collect(), read.no_json)
                    })
            } else {
                let query = format!("$format={format}");
                self.get_content(&objects, &query)
                    .await?
                    .map(|body| (xml_contents(&body), BTreeMap::new()))
            };
            let (contents, no_json) = match answered {
                Ok(answered) => answered,
                // The endpoint itself is absent - not the objects. Remembered,
                // and the same read is made again one object at a time.
                Err(error) if error.code == EtpError::ENOT_FOUND.code() && self.plural.is_none() => {
                    self.plural = Some(false);
                    let answered = self.one_by_one(&objects, &format).await?;
                    take(answered, wanted, &format, &mut found, &mut errors);
                    continue;
                }
                Err(error) => {
                    for (key, _) in wanted {
                        errors.insert(key, error.clone());
                    }
                    continue;
                }
            };
            if self.plural.is_none() {
                self.plural = Some(true);
            }
            take((contents, no_json), wanted, &format, &mut found, &mut errors);
        }
        Ok(self.map_answer(
            request,
            Message::GetDataObjectsResponse(GetDataObjectsResponse { data_objects: found }),
            errors,
        ))
    }

    /// Objects, **what they reference, and their arrays' metadata**, in one
    /// request per dataspace - what a front end asks before drawing an object
    /// (a front end's first read). The API embeds each
    /// referenced object in its reference (`_data`) and each array's metadata
    /// in its node; they are taken out, the objects given back as Energistics
    /// JSON and addressed in the dataspace of the object that references them
    /// (P11), the arrays' metadata kept for the reads that follow. No ETP
    /// message carries this: it is the REST API's own - and JSON only, the
    /// API embedding nothing in XML.
    pub async fn prefetch(&mut self, uris: &[String], arrays: bool) -> Result<Prefetch, ClientError> {
        let mut groups: BTreeMap<String, Vec<Asked>> = BTreeMap::new();
        for uri in uris {
            match Target::of(uri)? {
                Target::Object { dataspace, uuid, .. } => {
                    groups
                        .entry(dataspace)
                        .or_default()
                        .push((uri.clone(), uuid.to_ascii_lowercase()));
                }
                Target::Dataspace(_) => {
                    return Err(ClientError::Invalid(
                        EtpError::EINVALID_URI.with_detail(format!("{uri} names no object")),
                    ));
                }
            }
        }
        let mut prefetch = Prefetch::default();
        let query = format!("referencedContent=true&arrayMetadata={arrays}");
        for (dataspace, asked) in groups {
            let mut read = match self.json_objects(&asked, &query).await? {
                Ok(read) => read,
                Err(error) => {
                    return Err(ClientError::Refused(
                        EtpError::from_code(error.code)
                            .unwrap_or(EtpError::EINVALID_ARGUMENT)
                            .with_detail(error.message),
                    ));
                }
            };
            for (uri, uuid) in asked {
                if let Some(mut object) = read.found.remove(&uuid) {
                    unroll(&mut object, &dataspace, &mut prefetch);
                    prefetch
                        .objects
                        .insert(uri, serde_json::to_vec(&object).expect("JSON"));
                } else if let Some(xml) = read.no_json.remove(&uuid) {
                    prefetch.without_json.insert(uri, xml);
                } else {
                    prefetch.missing.push(uri);
                }
            }
        }
        for (id, meta) in &prefetch.arrays {
            // Embedded in the object's own JSON, where both types are stated:
            // `embedded_array` refuses the node otherwise.
            let carriage = Carriage {
                meta: meta.clone(),
                transport_stated: true,
            };
            self.metadata
                .insert((id.uri.clone(), id.path_in_resource.clone()), carriage);
        }
        Ok(prefetch)
    }

    /// Each object by itself: `GET /dataspaces/{id}/resources/{type}/{guid}`,
    /// for a server that has no plural read. One request per object, sent
    /// together ([`Http::send_all`]) so that they at least travel at once.
    async fn one_by_one(
        &mut self,
        asked: &[Asked],
        format: &str,
    ) -> Result<(BTreeMap<String, Vec<u8>>, BTreeMap<String, Vec<u8>>), ClientError> {
        let mut paths = Vec::with_capacity(asked.len());
        for (uri, _) in asked {
            let Ok(Target::Object {
                dataspace,
                qualified_type,
                uuid,
            }) = Target::of(uri)
            else {
                paths.push(String::new());
                continue;
            };
            paths.push(format!(
                "/dataspaces/{}/resources/{qualified_type}/{uuid}?$format={format}&referencedContent=false",
                encode(&dataspace)
            ));
        }
        let calls = paths
            .iter()
            .filter(|path| !path.is_empty())
            .map(|path| (Method::Get, path.clone(), None))
            .collect();
        let responses = self.send_all(calls).await?;
        let mut found = BTreeMap::new();
        let mut answers = responses.into_iter();
        for ((_, uuid), path) in asked.iter().zip(&paths) {
            if path.is_empty() {
                continue;
            }
            let Some(response) = answers.next() else { break };
            if status(&response).is_err() {
                continue;
            }
            // One object, alone: its own bytes, whatever wrapper it came in.
            let data = match format {
                "json" => strip_one(&response.body),
                _ => xml_contents(&response.body)
                    .into_values()
                    .next()
                    .unwrap_or(response.body),
            };
            found.insert(uuid.clone(), data);
        }
        Ok((found, BTreeMap::new()))
    }

    /// One `POST /dataspaces/multi-resources/get-content?{query}`, its body.
    async fn get_content(
        &mut self,
        asked: &[Asked],
        query: &str,
    ) -> Result<Result<Vec<u8>, ErrorInfo>, ClientError> {
        let uris: Vec<&str> = asked.iter().map(|(uri, _)| uri.as_str()).collect();
        let body = serde_json::to_vec(&serde_json::json!({ "uris": uris })).expect("a list of strings");
        let path = format!("/dataspaces/multi-resources/get-content?{query}");
        let response = self.send(Method::Post, path, Some(body)).await?;
        Ok(status(&response).map(|()| response.body))
    }

    /// One plural read in JSON: each object returned, by its lowercase uuid.
    async fn json_content(
        &mut self,
        asked: &[Asked],
        query: &str,
    ) -> Result<Result<BTreeMap<String, Json>, ErrorInfo>, ClientError> {
        let body = match self.get_content(asked, &format!("$format=json&{query}")).await? {
            Ok(body) => body,
            Err(error) => return Ok(Err(error)),
        };
        let json: Json = serde_json::from_slice(&body).map_err(|why| ClientError::AnswerRefused {
            request: 0,
            incident: EtpError::EINVALID_MESSAGE
                .with_detail(format!("multi-resources/get-content: not JSON ({why})")),
        })?;
        let Json::Array(items) = json else {
            return Err(unreadable("multi-resources/get-content", &json));
        };
        Ok(Ok(items
            .into_iter()
            .filter_map(|item| Some((item.get("Uuid")?.as_str()?.to_ascii_lowercase(), item)))
            .collect()))
    }

    /// Objects in JSON - **every one the server can give**, which the API
    /// does not do by itself: it omits an object it does not hold, but
    /// answers `[]` to the whole request as soon as one it holds cannot be
    /// said in JSON (one OSDU service: each object whose root element is spelled
    /// `obj_…`, which RESQML 2.0.1 names without the prefix, and each
    /// resqml20 Activity). ETP asks the opposite: « The store MUST return all
    /// data objects that it can » (§9.2.2 row 2.b). What the JSON answer
    /// lacks is asked again in XML, which tells an object not held from one
    /// held without JSON; the held ones are halved until each half answers or
    /// is one object without JSON - a few requests per such object, not one
    /// per object asked. With `referencedContent=true` only: told `false`,
    /// the API answers the same request by a `500 Empty object`, and a lost
    /// object asked alone by a 404.
    async fn json_objects(
        &mut self,
        asked: &[Asked],
        query: &str,
    ) -> Result<Result<JsonRead, ErrorInfo>, ClientError> {
        let mut read = JsonRead {
            found: match self.json_content(asked, query).await? {
                Ok(found) => found,
                Err(error) => return Ok(Err(error)),
            },
            ..Default::default()
        };
        let lacking: Vec<Asked> = asked
            .iter()
            .filter(|(_, uuid)| !read.found.contains_key(uuid))
            .cloned()
            .collect();
        if lacking.is_empty() {
            return Ok(Ok(read));
        }
        let mut xml = match self.get_content(&lacking, "$format=xml").await? {
            Ok(body) => xml_contents(&body),
            Err(error) => return Ok(Err(error)),
        };
        let held: Vec<Asked> = lacking
            .into_iter()
            .filter(|(_, uuid)| xml.contains_key(uuid))
            .collect();
        if !read.found.is_empty() {
            // An answer that is not `[]` refused nothing: what it left out,
            // and the XML holds, has no JSON.
            absorb(&held, BTreeMap::new(), &mut read, &mut xml);
            return Ok(Ok(read));
        }
        // Sets of held objects, each with whether its JSON answer is known to
        // be `[]` - that is, to hold one object at least without JSON.
        let mut sets = vec![(held, true)];
        while let Some((set, refused)) = sets.pop() {
            if set.is_empty() {
                continue;
            }
            if !refused {
                let got = match self.json_content(&set, query).await? {
                    Ok(got) => got,
                    Err(error) => return Ok(Err(error)),
                };
                if !got.is_empty() {
                    absorb(&set, got, &mut read, &mut xml);
                    continue;
                }
            }
            if set.len() == 1 {
                absorb(&set, BTreeMap::new(), &mut read, &mut xml);
                continue;
            }
            let (first, second) = set.split_at(set.len() / 2);
            let got = match self.json_content(first, query).await? {
                Ok(got) => got,
                Err(error) => return Ok(Err(error)),
            };
            if got.is_empty() {
                sets.push((first.to_vec(), true));
                sets.push((second.to_vec(), false));
            } else {
                // The first half refused nothing: the second holds the culprit.
                absorb(first, got, &mut read, &mut xml);
                sets.push((second.to_vec(), true));
            }
        }
        Ok(Ok(read))
    }

    async fn array_metadata(
        &mut self,
        request: i64,
        asked: BTreeMap<String, DataArrayIdentifier>,
    ) -> Result<Vec<Delivery>, ClientError> {
        let mut found = BTreeMap::new();
        let mut errors = BTreeMap::new();
        let keys: Vec<String> = asked.keys().cloned().collect();
        let ids: Vec<DataArrayIdentifier> = asked.into_values().collect();
        for (key, meta) in keys.into_iter().zip(self.metadata_of_all(&ids).await?) {
            match meta {
                Ok(carriage) => {
                    found.insert(key, carriage.meta);
                }
                Err(error) => {
                    errors.insert(key, error);
                }
            }
        }
        Ok(self.map_answer(
            request,
            Message::GetDataArrayMetadataResponse(GetDataArrayMetadataResponse {
                array_metadata: found,
            }),
            errors,
        ))
    }

    #[allow(clippy::type_complexity)]
    async fn arrays(
        &mut self,
        request: i64,
        asked: BTreeMap<String, (DataArrayIdentifier, Option<(Vec<i64>, Vec<i64>)>)>,
        sub: bool,
    ) -> Result<Vec<Delivery>, ClientError> {
        let mut found = BTreeMap::new();
        let mut errors = BTreeMap::new();
        let wanted: Vec<(String, DataArrayIdentifier, Option<(Vec<i64>, Vec<i64>)>)> = asked
            .into_iter()
            .map(|(key, (id, region))| (key, id, region))
            .collect();
        let ids: Vec<DataArrayIdentifier> = wanted.iter().map(|(_, id, _)| id.clone()).collect();
        let metas = self.metadata_of_all(&ids).await?;
        // The blocks of every array of the message, then one round of
        // requests for them all: a round trip costs more than an answer.
        let mut reading: Vec<(String, Option<AnyArrayType>, AnyLogicalArrayType, String)> = Vec::new();
        let mut paths = Vec::new();
        for ((key, id, region), meta) in wanted.into_iter().zip(metas) {
            let carriage = match meta {
                Ok(carriage) => carriage,
                Err(error) => {
                    errors.insert(key, error);
                    continue;
                }
            };
            let meta = &carriage.meta;
            let path = match array_path(&id) {
                Ok(path) => path,
                Err(error) => {
                    errors.insert(key, error);
                    continue;
                }
            };
            let binary = !matches!(
                meta.transport_array_type,
                AnyArrayType::ArrayOfBoolean | AnyArrayType::ArrayOfString
            );
            let mut query: Vec<(String, String)> = Vec::new();
            if let Some((starts, counts)) = &region {
                query.extend(starts.iter().map(|s| ("starts".to_string(), s.to_string())));
                query.extend(counts.iter().map(|c| ("counts".to_string(), c.to_string())));
            }
            if binary {
                query.push(("format".into(), "base64".into()));
            }
            paths.push(if query.is_empty() {
                path
            } else {
                format!("{path}?{}", query_string(&query))
            });
            reading.push((
                key,
                carriage.transport_stated.then_some(meta.transport_array_type),
                meta.logical_array_type,
                id.path_in_resource.clone(),
            ));
        }
        for ((key, kind, logical, path_in_resource), answer) in
            reading.into_iter().zip(self.get_all(paths).await?)
        {
            match answer {
                Ok(json) => match array(&json, kind, logical) {
                    Ok(array) => {
                        found.insert(key, array);
                    }
                    Err(why) => {
                        errors.insert(
                            key,
                            EtpError::EINVALID_ARGUMENT
                                .with_detail(format!("{path_in_resource}: {why}"))
                                .error_info(),
                        );
                    }
                },
                Err(error) => {
                    errors.insert(key, error);
                }
            }
        }
        let body = if sub {
            Message::GetDataSubarraysResponse(GetDataSubarraysResponse {
                data_subarrays: found,
            })
        } else {
            Message::GetDataArraysResponse(GetDataArraysResponse { data_arrays: found })
        };
        Ok(self.map_answer(request, body, errors))
    }

    // -- the writes -------------------------------------------------------

    /// `PUT /dataspaces/{id}/resources`, **one request per dataspace**, with
    /// the objects as JSON Energistics - the only form this API takes.
    /// An object handed over as XML is refused **by
    /// key and by name**: this client holds no registry to convert it, and
    /// sending the XML would be refused by the API with less to go on.
    async fn put_objects(
        &mut self,
        request: i64,
        asked: retp_types::protocol::store::PutDataObjects,
    ) -> Result<Vec<Delivery>, ClientError> {
        if asked.prune_contained_objects {
            return Err(unsupported(
                "pruneContainedObjects has no counterpart in the Reservoir DDMS REST API".to_string(),
            ));
        }
        let mut errors = BTreeMap::new();
        let mut success = BTreeMap::new();
        let mut groups: BTreeMap<String, Vec<(String, Json)>> = BTreeMap::new();
        for (key, object) in asked.data_objects {
            let uri = &object.resource.uri;
            let dataspace = match Target::of(uri) {
                Ok(Target::Object { dataspace, .. }) => dataspace,
                _ => {
                    errors.insert(
                        key,
                        EtpError::EINVALID_URI
                            .with_detail(format!("{uri} names no object"))
                            .error_info(),
                    );
                    continue;
                }
            };
            if object.format != "json" {
                errors.insert(
                    key,
                    EtpError::ENOSUPPORTEDFORMATS
                        .with_detail(format!(
                            "{uri}: the Reservoir DDMS REST API takes objects as JSON Energistics only, and this one is `{}`",
                            object.format
                        ))
                        .error_info(),
                );
                continue;
            }
            match serde_json::from_slice::<Json>(&object.data) {
                Ok(json) => groups.entry(dataspace).or_default().push((key, json)),
                Err(why) => {
                    errors.insert(
                        key,
                        EtpError::EINVALID_OBJECT
                            .with_detail(format!("{uri}: its data is not JSON: {why}"))
                            .error_info(),
                    );
                }
            }
        }
        for (dataspace, wanted) in groups {
            let path = format!("/dataspaces/{}/resources", encode(&dataspace));
            let body: Vec<Json> = wanted.iter().map(|(_, json)| json.clone()).collect();
            let response = self
                .send_one(Method::Put, path, serde_json::to_vec(&body).expect("JSON"))
                .await?;
            match accepted(&response) {
                Ok(()) => {
                    for (key, _) in wanted {
                        success.insert(key, PutResponse::default());
                    }
                }
                Err(error) => {
                    for (key, _) in wanted {
                        errors.insert(key, error.clone());
                    }
                }
            }
        }
        Ok(self.map_answer(
            request,
            Message::PutDataObjectsResponse(PutDataObjectsResponse { success }),
            errors,
        ))
    }

    /// `DELETE /dataspaces/{id}/resources/{type}/{guid}`, one request per
    /// object, sent together. The API says nothing of what went with it, so
    /// the answer names the object alone.
    async fn delete_objects(
        &mut self,
        request: i64,
        asked: retp_types::protocol::store::DeleteDataObjects,
    ) -> Result<Vec<Delivery>, ClientError> {
        if asked.prune_contained_objects {
            return Err(unsupported(
                "pruneContainedObjects has no counterpart in the Reservoir DDMS REST API".to_string(),
            ));
        }
        let mut errors = BTreeMap::new();
        let mut calls = Vec::new();
        let mut keyed = Vec::new();
        for (key, uri) in asked.uris {
            match Target::of(&uri) {
                Ok(Target::Object {
                    dataspace,
                    qualified_type,
                    uuid,
                }) => {
                    calls.push((
                        Method::Delete,
                        format!(
                            "/dataspaces/{}/resources/{qualified_type}/{uuid}",
                            encode(&dataspace)
                        ),
                        None,
                    ));
                    keyed.push((key, uri));
                }
                _ => {
                    errors.insert(
                        key,
                        EtpError::EINVALID_URI
                            .with_detail(format!("{uri} names no object"))
                            .error_info(),
                    );
                }
            }
        }
        let mut deleted_uris = BTreeMap::new();
        for ((key, uri), response) in keyed.into_iter().zip(self.send_all(calls).await?) {
            match status(&response) {
                Ok(()) => {
                    deleted_uris.insert(key, ArrayOfString { values: vec![uri] });
                }
                Err(error) => {
                    errors.insert(key, error);
                }
            }
        }
        Ok(self.map_answer(
            request,
            Message::DeleteDataObjectsResponse(DeleteDataObjectsResponse { deleted_uris }),
            errors,
        ))
    }

    /// `DELETE /dataspaces/{id}`, one request per dataspace, sent together.
    async fn delete_dataspaces(
        &mut self,
        request: i64,
        asked: retp_types::protocol::dataspace::DeleteDataspaces,
    ) -> Result<Vec<Delivery>, ClientError> {
        let mut errors = BTreeMap::new();
        let mut calls = Vec::new();
        let mut keyed = Vec::new();
        for (key, uri) in asked.uris {
            match Target::of(&uri) {
                Ok(Target::Dataspace(path)) => {
                    calls.push((Method::Delete, format!("/dataspaces/{}", encode(&path)), None));
                    keyed.push((key, uri));
                }
                _ => {
                    errors.insert(
                        key,
                        EtpError::EINVALID_URI
                            .with_detail(format!("{uri} names no dataspace"))
                            .error_info(),
                    );
                }
            }
        }
        let mut success = BTreeMap::new();
        for ((key, uri), response) in keyed.into_iter().zip(self.send_all(calls).await?) {
            match status(&response) {
                Ok(()) => {
                    success.insert(key, uri);
                }
                Err(error) => {
                    errors.insert(key, error);
                }
            }
        }
        Ok(self.map_answer(
            request,
            Message::DeleteDataspacesResponse(DeleteDataspacesResponse { success }),
            errors,
        ))
    }

    /// `POST /dataspaces` with `[{DataspaceId, Path, CustomData}]`. The
    /// `customData` - OSDU's owners, viewers and legal tags among them - goes
    /// as JSON; a value this binding cannot spell is refused by key and by
    /// name rather than dropped, since dropping the owners is exactly what a
    /// store then refuses with less to go on.
    async fn put_dataspaces(
        &mut self,
        request: i64,
        asked: retp_types::protocol::dataspace::PutDataspaces,
    ) -> Result<Vec<Delivery>, ClientError> {
        let mut errors = BTreeMap::new();
        let mut wanted: Vec<(String, String, Json)> = Vec::new();
        for (key, space) in asked.dataspaces {
            let Ok(Target::Dataspace(path)) = Target::of(&space.uri) else {
                errors.insert(
                    key,
                    EtpError::EINVALID_URI
                        .with_detail(format!("{} names no dataspace", space.uri))
                        .error_info(),
                );
                continue;
            };
            let mut custom = serde_json::Map::new();
            let mut refused = None;
            for (name, value) in &space.custom_data {
                match json_of_value(value) {
                    Some(json) => {
                        custom.insert(name.clone(), json);
                    }
                    None => {
                        refused = Some(format!(
                            "{}: customData `{name}` is of a kind this client cannot send as JSON",
                            space.uri
                        ));
                        break;
                    }
                }
            }
            if let Some(why) = refused {
                errors.insert(key, EtpError::EINVALID_ARGUMENT.with_detail(why).error_info());
                continue;
            }
            let body = serde_json::json!({ "DataspaceId": path, "Path": path, "CustomData": custom });
            wanted.push((key, space.uri, body));
        }
        let mut success = BTreeMap::new();
        if !wanted.is_empty() {
            let body: Vec<Json> = wanted.iter().map(|(_, _, json)| json.clone()).collect();
            let response = self
                .send_one(
                    Method::Post,
                    "/dataspaces".to_string(),
                    serde_json::to_vec(&body).expect("JSON"),
                )
                .await?;
            match status(&response) {
                Ok(()) => {
                    for (key, uri, _) in wanted {
                        success.insert(key, uri);
                    }
                }
                Err(error) => {
                    for (key, _, _) in wanted {
                        errors.insert(key, error.clone());
                    }
                }
            }
        }
        Ok(self.map_answer(
            request,
            Message::PutDataspacesResponse(PutDataspacesResponse { success }),
            errors,
        ))
    }

    /// `PUT /dataspaces/{id}/resources/arrays`, **one request per
    /// dataspace** - and one route for the three ETP messages (OSDU's
    /// Reservoir DMS 1.2 OpenAPI description): an item with `Data` is an
    /// array whole; without it, a **declaration** of its shape and type; with
    /// `Starts` and `Counts`, a **subarray**. `Data` takes a JSON list of
    /// numbers or a base64 string, so bytes go as base64. `ArrayType` is
    /// required, named as ETP names a transport.
    async fn put_arrays(
        &mut self,
        request: i64,
        asked: retp_types::protocol::data_array::PutDataArrays,
    ) -> Result<Vec<Delivery>, ClientError> {
        let items = asked.data_arrays.into_iter().map(|(key, put)| {
            let dimensions = put.array.dimensions.clone();
            let kind = transport_of(&put.array.data.item);
            (
                key,
                put.uid,
                dimensions,
                kind,
                Some(json_of_array(&put.array.data.item)),
                None,
            )
        });
        let success = self.put_array_items(items).await?;
        Ok(self.map_answer(
            request,
            Message::PutDataArraysResponse(PutDataArraysResponse { success: success.0 }),
            success.1,
        ))
    }

    /// A declaration: the shape and the type, no values. Remembered, so that
    /// the subarrays that follow can say the whole array's dimensions, which
    /// the API wants on every item.
    async fn put_uninitialized(
        &mut self,
        request: i64,
        asked: retp_types::protocol::data_array::PutUninitializedDataArrays,
    ) -> Result<Vec<Delivery>, ClientError> {
        for put in asked.data_arrays.values() {
            self.metadata.insert(
                (put.uid.uri.clone(), put.uid.path_in_resource.clone()),
                Carriage {
                    meta: put.metadata.clone(),
                    transport_stated: true,
                },
            );
        }
        let items = asked.data_arrays.into_iter().map(|(key, put)| {
            (
                key,
                put.uid,
                put.metadata.dimensions.clone(),
                put.metadata.transport_array_type,
                None,
                None,
            )
        });
        let success = self.put_array_items(items).await?;
        Ok(self.map_answer(
            request,
            Message::PutUninitializedDataArraysResponse(PutUninitializedDataArraysResponse {
                success: success.0,
            }),
            success.1,
        ))
    }

    /// A subarray: `Starts` and `Counts` beside the values - and the whole
    /// array's dimensions, which the API wants and the message does not
    /// carry. They come from the declaration made before, or from a metadata
    /// read; a subarray of an array this client knows nothing about is
    /// refused by key and by name rather than sent with dimensions made up.
    async fn put_subarrays(
        &mut self,
        request: i64,
        asked: retp_types::protocol::data_array::PutDataSubarrays,
    ) -> Result<Vec<Delivery>, ClientError> {
        let mut errors = BTreeMap::new();
        let mut items = Vec::new();
        for (key, put) in asked.data_subarrays {
            let known = self
                .metadata
                .get(&(put.uid.uri.clone(), put.uid.path_in_resource.clone()));
            let Some(carriage) = known else {
                errors.insert(
                    key,
                    EtpError::EINVALID_STATE
                        .with_detail(format!(
                            "{} {}: the API wants the whole array's dimensions on a subarray, and this client has not seen them - declare the array first",
                            put.uid.uri, put.uid.path_in_resource
                        ))
                        .error_info(),
                );
                continue;
            };
            let dimensions = carriage.meta.dimensions.clone();
            let kind = transport_of(&put.data.item);
            items.push((
                key,
                put.uid,
                dimensions,
                kind,
                Some(json_of_array(&put.data.item)),
                Some((put.starts, put.counts)),
            ));
        }
        let (success, more) = self.put_array_items(items.into_iter()).await?;
        errors.extend(more);
        Ok(self.map_answer(
            request,
            Message::PutDataSubarraysResponse(PutDataSubarraysResponse { success }),
            errors,
        ))
    }

    /// The one route, for items of the three kinds: keys grouped by
    /// dataspace, one `PUT` each, every key of a group answered together.
    #[allow(clippy::type_complexity)]
    async fn put_array_items(
        &mut self,
        items: impl Iterator<
            Item = (
                String,
                DataArrayIdentifier,
                Vec<i64>,
                AnyArrayType,
                Option<Json>,
                Option<(Vec<i64>, Vec<i64>)>,
            ),
        >,
    ) -> Result<(BTreeMap<String, String>, BTreeMap<String, ErrorInfo>), ClientError> {
        let mut errors = BTreeMap::new();
        let mut groups: BTreeMap<String, Vec<(String, Json)>> = BTreeMap::new();
        for (key, uid, dimensions, kind, data, region) in items {
            let (dataspace, qualified_type, uuid) = match Target::of(&uid.uri) {
                Ok(Target::Object {
                    dataspace,
                    qualified_type,
                    uuid,
                }) => (dataspace, qualified_type, uuid),
                _ => {
                    errors.insert(
                        key,
                        EtpError::EINVALID_URI
                            .with_detail(format!("{}: arrays hang from an object", uid.uri))
                            .error_info(),
                    );
                    continue;
                }
            };
            let mut body = serde_json::json!({
                "ContainerType": qualified_type,
                "ContainerUuid": uuid,
                "PathInResource": uid.path_in_resource,
                "Dimensions": dimensions,
                "ArrayType": kind.as_str(),
            });
            if let Some(data) = data {
                body["Data"] = data;
            }
            if let Some((starts, counts)) = region {
                body["Starts"] = serde_json::json!(starts);
                body["Counts"] = serde_json::json!(counts);
            }
            groups.entry(dataspace).or_default().push((key, body));
        }
        let mut success = BTreeMap::new();
        for (dataspace, wanted) in groups {
            let path = format!("/dataspaces/{}/resources/arrays", encode(&dataspace));
            let body: Vec<Json> = wanted.iter().map(|(_, json)| json.clone()).collect();
            let response = self
                .send_one(Method::Put, path, serde_json::to_vec(&body).expect("JSON"))
                .await?;
            match accepted(&response) {
                Ok(()) => {
                    for (key, _) in wanted {
                        success.insert(key, String::new());
                    }
                }
                Err(error) => {
                    for (key, _) in wanted {
                        errors.insert(key, error.clone());
                    }
                }
            }
        }
        Ok((success, errors))
    }

    /// `POST /dataspaces/{id}/transactions` - **one** dataspace, which is the
    /// one thing here that ETP's message can ask for and this API cannot do:
    /// two dataspaces are refused by name before anything leaves. The API
    /// answers an id; the ETP message carries a uuid, so an id that is not one
    /// is a refusal too - not a uuid made up to fit.
    async fn start_transaction(
        &mut self,
        request: i64,
        asked: retp_types::protocol::transaction::StartTransaction,
    ) -> Result<Vec<Delivery>, ClientError> {
        let spaces: Vec<&String> = asked.dataspace_uris.iter().filter(|u| !u.is_empty()).collect();
        let [space] = spaces.as_slice() else {
            return Err(ClientError::Invalid(EtpError::EINVALID_ARGUMENT.with_detail(
                format!(
                    "a Reservoir DDMS REST transaction covers exactly one dataspace, {} were asked",
                    spaces.len()
                ),
            )));
        };
        let Ok(Target::Dataspace(path)) = Target::of(space) else {
            return Err(ClientError::Invalid(
                EtpError::EINVALID_URI.with_detail(format!("{space} names no dataspace")),
            ));
        };
        if let Some(open) = &self.transaction {
            return Err(ClientError::Invalid(EtpError::EINVALID_STATE.with_detail(
                format!(
                    "a transaction is already open on {} ({}); commit or roll it back first",
                    open.dataspace, open.id
                ),
            )));
        }
        let response = self
            .send_one(
                Method::Post,
                format!("/dataspaces/{}/transactions", encode(&path)),
                b"{}".to_vec(),
            )
            .await?;
        let answer = match status(&response) {
            Ok(()) => {
                let id = serde_json::from_slice::<Json>(&response.body)
                    .ok()
                    .and_then(|j| j.as_str().map(str::to_string))
                    .unwrap_or_else(|| String::from_utf8_lossy(&response.body).trim().to_string());
                match uuid_of(&id) {
                    Some(uuid) => {
                        self.transaction = Some(RestTransaction {
                            dataspace: path,
                            id,
                            uuid,
                        });
                        StartTransactionResponse {
                            transaction_uuid: uuid,
                            successful: true,
                            failure_reason: String::new(),
                        }
                    }
                    // some servers answer `null`: the route exists and opens
                    // nothing. Said as *no transactions here* (`ENOTSUPPORTED`)
                    // rather than as a refusal, so that a push may go on
                    // without one instead of stopping at its first island.
                    None if id.is_empty() || id == "null" => {
                        return Ok(self.map_answer(
                            request,
                            Message::StartTransactionResponse(StartTransactionResponse::default()),
                            [(String::new(), EtpError::ENOTSUPPORTED.with_detail("the API named the transaction `null`: this server opens no transaction over REST").error_info())].into_iter().collect(),
                        ));
                    }
                    None => StartTransactionResponse {
                        transaction_uuid: Uuid::default(),
                        successful: false,
                        failure_reason: format!(
                            "the API named the transaction `{id}`, which is not a uuid this client can carry"
                        ),
                    },
                }
            }
            Err(error) => StartTransactionResponse {
                transaction_uuid: Uuid::default(),
                successful: false,
                failure_reason: error.message,
            },
        };
        Ok(vec![self.delivery(
            request,
            Message::StartTransactionResponse(answer),
            true,
        )])
    }

    /// `PUT` commits, `DELETE` rolls back - `…/transactions/{id}`, for the
    /// transaction this client has open and no other: asked about another
    /// uuid, it says so rather than ending the one it has.
    async fn end_transaction(
        &mut self,
        request: i64,
        uuid: Uuid,
        commit: bool,
    ) -> Result<Vec<Delivery>, ClientError> {
        let (successful, failure_reason) = match &self.transaction {
            Some(open) if open.uuid == uuid => {
                let path = format!(
                    "/dataspaces/{}/transactions/{}",
                    encode(&open.dataspace),
                    encode(&open.id)
                );
                let method = if commit { Method::Put } else { Method::Delete };
                let response = self.send_one(method, path, Vec::new()).await?;
                match accepted(&response) {
                    Ok(()) => {
                        self.transaction = None;
                        (true, String::new())
                    }
                    Err(error) => (false, error.message),
                }
            }
            Some(open) => (
                false,
                format!("the transaction open on {} is not {uuid:?}", open.dataspace),
            ),
            None => (false, "no transaction is open".to_string()),
        };
        let body = if commit {
            Message::CommitTransactionResponse(CommitTransactionResponse {
                transaction_uuid: uuid,
                successful,
                failure_reason,
            })
        } else {
            Message::RollbackTransactionResponse(RollbackTransactionResponse {
                transaction_uuid: uuid,
                successful,
                failure_reason,
            })
        };
        Ok(vec![self.delivery(request, body, true)])
    }

    /// One request with a body, and its response - a `send_all` of one.
    async fn send_one(
        &mut self,
        method: Method,
        path: String,
        body: Vec<u8>,
    ) -> Result<HttpResponse, ClientError> {
        let body = if body.is_empty() { None } else { Some(body) };
        let mut responses = self.send_all(vec![(method, path, body)]).await?;
        Ok(responses.pop().expect("one call, one response"))
    }

    /// The metadata of each of `ids`: those already known are not asked
    /// again, and the others are asked **together**.
    async fn metadata_of_all(
        &mut self,
        ids: &[DataArrayIdentifier],
    ) -> Result<Vec<Result<Carriage, ErrorInfo>>, ClientError> {
        let mut known: Vec<Option<Result<Carriage, ErrorInfo>>> = Vec::with_capacity(ids.len());
        let mut asking: Vec<(usize, (String, String))> = Vec::new();
        let mut paths = Vec::new();
        for (index, id) in ids.iter().enumerate() {
            let key = (id.uri.clone(), id.path_in_resource.clone());
            if let Some(meta) = self.metadata.get(&key) {
                known.push(Some(Ok(meta.clone())));
                continue;
            }
            match array_path(id) {
                Ok(path) => {
                    paths.push(format!("{path}/metadata"));
                    asking.push((index, key));
                    known.push(None);
                }
                Err(error) => known.push(Some(Err(error))),
            }
        }
        for ((index, key), answer) in asking.into_iter().zip(self.get_all(paths).await?) {
            known[index] = Some(match answer {
                Ok(json) => match metadata(&json) {
                    Some(meta) => {
                        self.metadata.insert(key, meta.clone());
                        Ok(meta)
                    }
                    None => Err(EtpError::EINVALID_ARGUMENT
                        .with_detail(format!(
                            "{}: the metadata names no logical array type this build knows",
                            ids[index].path_in_resource
                        ))
                        .error_info()),
                },
                Err(error) => Err(error),
            });
        }
        Ok(known
            .into_iter()
            .map(|meta| meta.expect("every array answered"))
            .collect())
    }

    // -- the wire ---------------------------------------------------------

    /// `GET` a path of the API: its JSON, or the ETP error its status means.
    async fn get(&mut self, path: String) -> Result<Result<Json, ErrorInfo>, ClientError> {
        let mut answers = self.get_all(vec![path]).await?;
        Ok(answers.pop().expect("one path, one answer"))
    }

    /// The same for a lot of paths, asked together.
    async fn get_all(&mut self, paths: Vec<String>) -> Result<Vec<Result<Json, ErrorInfo>>, ClientError> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }
        let calls = paths
            .iter()
            .map(|path| (Method::Get, path.clone(), None))
            .collect();
        let responses = self.send_all(calls).await?;
        let request = self.next_id - 2;
        paths
            .into_iter()
            .zip(responses)
            .map(|(path, response)| match status(&response) {
                Ok(()) => match serde_json::from_slice(&response.body) {
                    Ok(json) => Ok(Ok(json)),
                    Err(why) => Err(ClientError::AnswerRefused {
                        request,
                        incident: EtpError::EINVALID_MESSAGE.with_detail(format!("{path}: not JSON ({why})")),
                    }),
                },
                Err(error) => Ok(Err(error)),
            })
            .collect()
    }

    /// Send a request to the API. What means the connection is not there -
    /// unreachable, rate-limited, a server error, its ETP session gone - is a
    /// [`ClientError::Transport`], which a resumed operation retries; the
    /// credentials refused, [`ClientError::AuthorizationNeeded`].
    async fn send(
        &mut self,
        method: Method,
        path: String,
        body: Option<Vec<u8>>,
    ) -> Result<HttpResponse, ClientError> {
        let mut answers = self.send_all(vec![(method, path, body)]).await?;
        Ok(answers.pop().expect("one request, one answer"))
    }

    /// The same, for every call of one ETP message: they go to the transport
    /// together ([`Http::send_all`]), and come back in the order asked. The
    /// first that says the connection is not there refuses them all.
    async fn send_all(
        &mut self,
        calls: Vec<(Method, String, Option<Vec<u8>>)>,
    ) -> Result<Vec<HttpResponse>, ClientError> {
        let at_once = self.at_once;
        let requests: Vec<HttpRequest> = calls
            .iter()
            .map(|(method, path, body)| self.request_of(*method, path, body.clone()))
            .collect();
        let answers = self.http.send_all(requests, at_once).await;
        calls
            .into_iter()
            .zip(answers)
            .map(|((method, path, _), answer)| self.answer_of(method, &path, answer))
            .collect()
    }

    /// What a call is on the wire: the URL, the credentials, the partition.
    fn request_of(&self, method: Method, path: &str, body: Option<Vec<u8>>) -> HttpRequest {
        let mut headers = vec![("accept".to_string(), "application/json".to_string())];
        if let Some(credentials) = &self.credentials {
            headers.push(("authorization".into(), credentials.header().to_string()));
        }
        if let Some(partition) = &self.partition {
            headers.push(("data-partition-id".into(), partition.clone()));
        }
        if body.is_some() {
            headers.push(("content-type".into(), "application/json".into()));
        }
        // The open transaction rides on every request, reads included: the
        // API reads *inside* the transaction what was written in it.
        let mut url = format!("{}{path}", self.base);
        if let Some(transaction) = &self.transaction {
            url.push(if path.contains('?') { '&' } else { '?' });
            url.push_str("transactionId=");
            url.push_str(&encode(&transaction.id));
        }
        HttpRequest {
            method,
            url,
            headers,
            body,
        }
    }

    /// What an answer means for the session: the connection gone, the
    /// credentials refused, or an answer to read.
    fn answer_of(
        &self,
        method: Method,
        path: &str,
        answer: Result<HttpResponse, TransportError>,
    ) -> Result<HttpResponse, ClientError> {
        let response = answer?;
        match response.status {
            401 => Err(ClientError::AuthorizationNeeded {
                challenges: response.challenge.clone().into_iter().collect(),
            }),
            410 | 429 | 500..=599 => Err(ClientError::Transport(TransportError::new(format!(
                "{} {path}: HTTP {}: {}",
                verb(method),
                response.status,
                message_of(&response.body)
            )))),
            _ => Ok(response),
        }
    }

    fn delivery(&mut self, request: i64, body: Message, last: bool) -> Delivery {
        let (protocol, message_type) = body.header();
        let id = self.next_id;
        self.next_id += 2;
        Delivery {
            header: MessageHeader {
                protocol,
                message_type,
                correlation_id: request,
                message_id: id,
                message_flags: if last { Flags::FIN.0 } else { 0 },
            },
            extension: None,
            body: Body::Message(body),
            place: Place::Answers { request },
            last,
        }
    }

    /// A map request's answer: what came, then the errors key by key.
    fn map_answer(
        &mut self,
        request: i64,
        found: Message,
        errors: BTreeMap<String, ErrorInfo>,
    ) -> Vec<Delivery> {
        if errors.is_empty() {
            return vec![self.delivery(request, found, true)];
        }
        vec![
            self.delivery(request, found, false),
            self.delivery(
                request,
                Message::ProtocolException(ProtocolException { error: None, errors }),
                true,
            ),
        ]
    }
}

impl<H: Http> Caller for RestClient<H> {
    fn call<M: EtpMessage>(
        &mut self,
        request: &M,
    ) -> impl Future<Output = Result<Vec<Delivery>, ClientError>> {
        // The request, read back as the message it is: the translation goes
        // by what it asks, not by its Rust type.
        let mut out = Writer::new();
        request.write(&mut out);
        let message = dispatch::decode(M::PROTOCOL, M::MESSAGE_TYPE, &out.finish());
        async move {
            let message =
                message.map_err(|why| unsupported(format!("the request could not be read back: {why}")))?;
            self.answer(message).await
        }
    }

    fn negotiated(&self) -> Negotiated {
        self.negotiated.clone()
    }

    fn tolerance(&self) -> Tolerance {
        self.tolerance
    }

    fn at_once(&self) -> usize {
        self.at_once
    }
}

/// What the client serves, said as a negotiation would - there is no session
/// to negotiate: the protocols it emulates, as their customer, and the two
/// formats.
fn described(base: &str) -> Negotiated {
    let protocols = [3, 4, 9, 18, 24, 25]
        .into_iter()
        .map(|p| {
            (
                p,
                NegotiatedProtocol {
                    version: v12(),
                    role: Role::Customer,
                    ours: Capabilities::empty(Scope::Protocol),
                    theirs: Capabilities::empty(Scope::Protocol),
                },
            )
        })
        .collect();
    Negotiated {
        session_id: Uuid([0; 16]),
        peer_application: ("Reservoir DDMS REST API".into(), base.to_string()),
        protocols,
        data_objects: Vec::new(),
        formats: vec!["xml".into(), "json".into()],
        compression: None,
        ours: Capabilities::empty(Scope::Endpoint),
        theirs: Capabilities::empty(Scope::Endpoint),
        peer_time: 0,
        peer_earliest_retained: 0,
        deviations: Vec::new(),
    }
}

// -- where a URI points -----------------------------------------------------

enum Target {
    Dataspace(String),
    Object {
        dataspace: String,
        qualified_type: String,
        uuid: String,
    },
}

impl Target {
    fn of(uri: &str) -> Result<Target, ClientError> {
        let parsed = EtpUri::parse(uri).map_err(|why| {
            ClientError::Invalid(EtpError::EINVALID_URI.with_detail(format!("{uri}: {why}")))
        })?;
        let Some(dataspace) = parsed.dataspace.clone().filter(|d| !d.is_empty()) else {
            return Err(unsupported(format!(
                "{uri}: the REST API names every dataspace, and the default one (eml:///) has no name"
            )));
        };
        match (&parsed.domain, &parsed.version, &parsed.object_type, &parsed.uuid) {
            (Some(domain), Some(version), Some(object_type), Some(uuid)) => Ok(Target::Object {
                dataspace,
                qualified_type: format!("{domain}{version}.{object_type}"),
                uuid: uuid.clone(),
            }),
            _ if parsed.object_type.is_none() => Ok(Target::Dataspace(dataspace)),
            _ => Err(ClientError::Invalid(
                EtpError::EINVALID_URI.with_detail(format!("{uri}: no uuid")),
            )),
        }
    }
}

fn array_path(id: &DataArrayIdentifier) -> Result<String, ErrorInfo> {
    match Target::of(&id.uri) {
        Ok(Target::Object {
            dataspace,
            qualified_type,
            uuid,
        }) => Ok(format!(
            "/dataspaces/{}/resources/{qualified_type}/{uuid}/arrays/{}",
            encode(&dataspace),
            encode(&id.path_in_resource)
        )),
        _ => Err(EtpError::EINVALID_URI
            .with_detail(format!("{}: arrays hang from an object", id.uri))
            .error_info()),
    }
}

// -- reading the API's JSON -------------------------------------------------

fn dataspace(json: &Json) -> Dataspace {
    Dataspace {
        uri: text(json, "uri"),
        path: text(json, "path"),
        store_last_write: time(json, "storeLastWrite"),
        store_created: time(json, "storeCreated"),
        custom_data: custom(json),
    }
}

fn resource(json: &Json) -> Resource {
    Resource {
        uri: text(json, "uri"),
        alternate_uris: json
            .get("alternateUris")
            .and_then(Json::as_array)
            .map(|a| a.iter().filter_map(|u| u.as_str().map(str::to_string)).collect())
            .unwrap_or_default(),
        name: text(json, "name"),
        source_count: json.get("sourceCount").and_then(Json::as_i64).map(|n| n as i32),
        target_count: json.get("targetCount").and_then(Json::as_i64).map(|n| n as i32),
        last_changed: time(json, "lastChanged"),
        store_last_write: time(json, "storeLastWrite"),
        store_created: time(json, "storeCreated"),
        active_status: if text(json, "activeStatus") == "Inactive" {
            ActiveStatusKind::Inactive
        } else {
            ActiveStatusKind::Active
        },
        custom_data: custom(json),
    }
}

fn edge(json: &Json) -> Edge {
    let mut custom_data = BTreeMap::new();
    if let Some(path) = json.get("path").and_then(Json::as_str) {
        custom_data.insert(
            "path".to_string(),
            DataValue {
                item: DataValueItem::String(path.to_string()),
            },
        );
    }
    Edge {
        source_uri: text(json, "source"),
        target_uri: text(json, "target"),
        relationship_kind: RelationshipKind::Primary,
        custom_data,
    }
}

/// An array's metadata, and **whether the store named its transport type.**
///
/// The two Reservoir DDMS answer different shapes here. One sends the whole
/// of ETP's `DataArrayMetadata`; the other's `…/arrays/{path}/metadata` answers
/// its own leaner `ArrayMetadata`, which carries **no `transportArrayType` at
/// all** and spells `logicalArrayType` as a symbol rather than an ordinal.
/// Both are legitimate JSON bindings of the same fact, and reading only one
/// of them is what made every array of that server unreadable (reported as
/// *« unreadable metadata »* on every `points_patch0`).
///
/// So the transport is remembered as *stated or not*. Not stated does not
/// mean unknown: §13.2.2.1 names the transports a logical type may travel
/// in, and where the data's own shape is needed to choose between them, the
/// data is in hand by then (see [`array`]). What must not happen is a made-up
/// transport passed off as the store's word.
#[derive(Clone)]
struct Carriage {
    meta: DataArrayMetadata,
    transport_stated: bool,
}

fn metadata(json: &Json) -> Option<Carriage> {
    let dims = |key: &str| -> Vec<i64> {
        json.get(key)
            .and_then(Json::as_array)
            .map(|a| a.iter().filter_map(Json::as_i64).collect())
            .unwrap_or_default()
    };
    let logical: AnyLogicalArrayType = enumeration(json, "logicalArrayType")?;
    let stated: Option<AnyArrayType> = enumeration(json, "transportArrayType");
    let transport = match stated {
        Some(transport) => transport,
        None => implied_transport(logical)?,
    };
    Some(Carriage {
        meta: DataArrayMetadata {
            dimensions: dims("dimensions"),
            preferred_subarray_dimensions: dims("preferredSubarrayDimensions"),
            transport_array_type: transport,
            logical_array_type: logical,
            store_last_write: time(json, "storeLastWrite"),
            store_created: time(json, "storeCreated"),
            custom_data: custom(json),
        },
        transport_stated: stated.is_some(),
    })
}

/// An Avro enumeration as an API writes it: its **ordinal**, which is what an
/// Avro enumeration is on the wire, or its **symbol**, which is what a JSON
/// binding of the same enumeration naturally spells. One sends `4`, the other
/// sends `"arrayOfDouble"`, and the two mean the same thing.
///
/// Both go through the generated tables - the ordinal through the codec, the
/// symbol through `SYMBOLS` and `as_str` - so neither can fall out of step
/// with the schema, which a spelling table written here would.
fn enumeration<E: Symbols>(json: &Json, key: &str) -> Option<E> {
    match json.get(key)? {
        Json::Number(number) => by_ordinal(number.as_i64()?),
        Json::String(symbol) => E::SPELLINGS.iter().copied().find(|one| one.spelling() == symbol),
        _ => None,
    }
}

/// What a generated Avro enumeration offers for the lookup above. Two lines
/// per enumeration, and no list of names: the names are the generated ones.
trait Symbols: Avro + Copy + 'static {
    const SPELLINGS: &'static [Self];
    fn spelling(&self) -> &'static str;
}

impl Symbols for AnyArrayType {
    const SPELLINGS: &'static [Self] = &Self::SYMBOLS;
    fn spelling(&self) -> &'static str {
        self.as_str()
    }
}

impl Symbols for AnyLogicalArrayType {
    const SPELLINGS: &'static [Self] = &Self::SYMBOLS;
    fn spelling(&self) -> &'static str {
        self.as_str()
    }
}

/// An Avro enumeration from its ordinal - an Avro enumeration **is** its
/// ordinal on the wire: no table here to fall out of step with the schema.
fn by_ordinal<E: Avro>(ordinal: i64) -> Option<E> {
    let mut out = Writer::new();
    out.int(i32::try_from(ordinal).ok()?);
    E::read(&mut Reader::new(&out.finish())).ok()
}

/// A `DataArray` from `{uid, data: {data, dimensions}}`: base64 for numbers
/// and bytes - little-endian, as ETP's transport -, JSON for booleans and
/// strings. A count that is not the dimensions' product is refused.
///
/// `transport` is `None` when the store never named one ([`Carriage`]); the
/// logical type then implies it ([`implied_transport`]), and the count check
/// below is what keeps that honest - a width the bytes do not divide into is
/// refused, naming both numbers, rather than read as something else.
fn array(
    json: &Json,
    transport: Option<AnyArrayType>,
    logical: AnyLogicalArrayType,
) -> Result<DataArray, String> {
    let data = json.get("data").ok_or("no data")?;
    let dimensions: Vec<i64> = data
        .get("dimensions")
        .and_then(Json::as_array)
        .map(|a| a.iter().filter_map(Json::as_i64).collect())
        .ok_or("no dimensions")?;
    let count: i64 = dimensions.iter().product();
    let values = data.get("data").ok_or("no values")?;
    let implied = transport.is_none();
    let transport = match transport {
        Some(stated) => stated,
        None => implied_transport(logical).ok_or_else(|| {
            format!("{logical:?}: the store named no transport type, and the specification implies none")
        })?,
    };
    let item = match values {
        Json::String(encoded) => {
            let raw = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|why| format!("not base64: {why}"))?;
            let width = match transport {
                AnyArrayType::ArrayOfDouble | AnyArrayType::ArrayOfLong => 8,
                AnyArrayType::ArrayOfFloat | AnyArrayType::ArrayOfInt => 4,
                AnyArrayType::Bytes => 1,
                other => return Err(format!("{other:?} is not read from base64")),
            };
            if raw.len() % width != 0
                || transport != AnyArrayType::Bytes && (raw.len() / width) as i64 != count
            {
                let said = if implied {
                    format!(
                        ", read as {transport:?} because the store named no transport type for {logical:?}"
                    )
                } else {
                    String::new()
                };
                return Err(format!(
                    "{} bytes are not {count} values of {width} bytes{said}",
                    raw.len()
                ));
            }
            let chunks = raw.chunks_exact(width);
            match transport {
                AnyArrayType::ArrayOfDouble => AnyArrayItem::ArrayOfDouble(ArrayOfDouble {
                    values: chunks
                        .map(|c| f64::from_le_bytes(c.try_into().expect("8 bytes")))
                        .collect(),
                }),
                AnyArrayType::ArrayOfLong => AnyArrayItem::ArrayOfLong(ArrayOfLong {
                    values: chunks
                        .map(|c| i64::from_le_bytes(c.try_into().expect("8 bytes")))
                        .collect(),
                }),
                AnyArrayType::ArrayOfFloat => AnyArrayItem::ArrayOfFloat(ArrayOfFloat {
                    values: chunks
                        .map(|c| f32::from_le_bytes(c.try_into().expect("4 bytes")))
                        .collect(),
                }),
                AnyArrayType::ArrayOfInt => AnyArrayItem::ArrayOfInt(ArrayOfInt {
                    values: chunks
                        .map(|c| i32::from_le_bytes(c.try_into().expect("4 bytes")))
                        .collect(),
                }),
                _ => AnyArrayItem::Bytes(raw),
            }
        }
        Json::Array(items) => {
            if items.len() as i64 != count {
                return Err(format!("{} values for dimensions {dimensions:?}", items.len()));
            }
            match transport {
                AnyArrayType::ArrayOfBoolean => AnyArrayItem::ArrayOfBoolean(ArrayOfBoolean {
                    values: items
                        .iter()
                        .map(|v| v.as_bool().ok_or("not a boolean"))
                        .collect::<Result<_, _>>()?,
                }),
                AnyArrayType::ArrayOfString => AnyArrayItem::ArrayOfString(ArrayOfString {
                    values: items
                        .iter()
                        .map(|v| v.as_str().map(str::to_string).ok_or("not a string"))
                        .collect::<Result<_, _>>()?,
                }),
                // **`null` is a value, and it is the only one JSON has for
                // *no value*.** RFC 8259 has neither `NaN` nor `Infinity`, so
                // a store that writes a no-value correctly writes `null` -
                // which is what this client's own writer does
                // (`json_of_array`). Refusing it here lost the whole array of
                // any continuous property with an inactive cell, which is
                // most of them.
                AnyArrayType::ArrayOfDouble => AnyArrayItem::ArrayOfDouble(ArrayOfDouble {
                    values: items
                        .iter()
                        .map(|v| {
                            if v.is_null() {
                                Ok(f64::NAN)
                            } else {
                                v.as_f64().ok_or("not a number")
                            }
                        })
                        .collect::<Result<_, _>>()?,
                }),
                AnyArrayType::ArrayOfInt => AnyArrayItem::ArrayOfInt(ArrayOfInt {
                    values: items
                        .iter()
                        .map(|v| v.as_i64().and_then(|n| i32::try_from(n).ok()).ok_or("not an int"))
                        .collect::<Result<_, _>>()?,
                }),
                other => return Err(format!("{other:?} is not read from JSON")),
            }
        }
        _ => return Err("values neither base64 nor a list".into()),
    };
    Ok(DataArray {
        dimensions,
        data: AnyArray { item },
    })
}

/// The transport type a logical type implies, for a store that names none.
///
/// §13.2.2.1 says which transports a logical type may travel in, and for all
/// but the little-endian integers it allows **exactly one**. Where it allows
/// several, the odd one out is `bytes` - a packing this API does not use, its
/// own numbers arriving either as a JSON list or as base64 of the transport's
/// own width - so the numeric container is the one meant.
///
/// `None` only for `arrayOfCustom`, which the specification leaves open: there
/// is nothing to imply, and saying so is the honest answer.
///
/// **Base64 is not `bytes`.** It is how this REST binding writes the values of
/// any numeric transport, which is why the encoding says nothing about the
/// transport - a mistake worth naming, because it reads as if it did.
fn implied_transport(logical: AnyLogicalArrayType) -> Option<AnyArrayType> {
    let allowed = crate::arrays::allowed_transports(logical);
    allowed
        .iter()
        .copied()
        .find(|one| *one != AnyArrayType::Bytes)
        .or_else(|| allowed.first().copied())
}

/// An object asked of a plural read: its URI, and its uuid lowercase - what
/// the API's answers are keyed by.
type Asked = (String, String);

/// What came of a read, into the answer: an object found, one the server
/// holds without JSON, or one it does not hold at all.
fn take(
    (mut contents, no_json): (BTreeMap<String, Vec<u8>>, BTreeMap<String, Vec<u8>>),
    wanted: Vec<(String, Asked)>,
    format: &str,
    found: &mut BTreeMap<String, DataObject>,
    errors: &mut BTreeMap<String, ErrorInfo>,
) {
    for (key, (uri, uuid)) in wanted {
        let Some(data) = contents.remove(&uuid) else {
            let error = if no_json.contains_key(&uuid) {
                EtpError::ENOSUPPORTEDFORMATS.with_detail(format!(
                    "{uri}: the server holds it - its XML came back - but gives no JSON for it"
                ))
            } else {
                EtpError::ENOT_FOUND.with_detail(format!("{uri}: the server returned nothing for it"))
            };
            errors.insert(key, error.error_info());
            continue;
        };
        found.insert(
            key,
            DataObject {
                // The REST API gives the content, not the resource.
                resource: Resource {
                    uri,
                    ..Default::default()
                },
                format: format.to_string(),
                blob_id: None,
                data,
            },
        );
    }
}

/// One object's JSON, without what the API embeds in it.
fn strip_one(body: &[u8]) -> Vec<u8> {
    let Ok(mut json) = serde_json::from_slice::<Json>(body) else {
        return body.to_vec();
    };
    strip_embedded(&mut json);
    serde_json::to_vec(&json).unwrap_or_else(|_| body.to_vec())
}

/// What [`RestClient::json_objects`] brought; an object asked in neither map
/// is one the server does not hold.
#[derive(Default)]
struct JsonRead {
    /// Each object returned, by its lowercase uuid, as the API sent it.
    found: BTreeMap<String, Json>,
    /// Each object the server holds - its XML came back - and gives no JSON
    /// for, with that XML.
    no_json: BTreeMap<String, Vec<u8>>,
}

/// Each object of `set` into `read`: found if `got` has it, else without
/// JSON if the XML held it.
fn absorb(
    set: &[Asked],
    mut got: BTreeMap<String, Json>,
    read: &mut JsonRead,
    xml: &mut BTreeMap<String, Vec<u8>>,
) {
    for (_, uuid) in set {
        if let Some(json) = got.remove(uuid) {
            read.found.insert(uuid.clone(), json);
        } else if let Some(content) = xml.remove(uuid) {
            read.no_json.insert(uuid.clone(), content);
        }
    }
}

/// What a plural read in XML brought: each object's content by its
/// lowercase uuid, out of the `<DataObjects>` the API wraps the objects in -
/// not Energistics' -, each child its own document.
fn xml_contents(body: &[u8]) -> BTreeMap<String, Vec<u8>> {
    let mut out = BTreeMap::new();
    let Ok(text) = std::str::from_utf8(body) else {
        return out;
    };
    for child in children(text) {
        if let Some(uuid) = root_uuid(child) {
            out.insert(uuid.to_ascii_lowercase(), child.as_bytes().to_vec());
        }
    }
    out
}

/// The elements directly inside `<DataObjects>` - or the document itself
/// when the API sent no wrapper. A scanner of tags, not a parser: comments,
/// CDATA, processing instructions and quoted `>` are stepped over; what it
/// cannot follow, it does not return.
fn children(text: &str) -> Vec<&str> {
    let bytes = text.as_bytes();
    let Some(open) = text.find("<DataObjects") else {
        let body = text.trim_start();
        let body = match body.strip_prefix("<?xml") {
            Some(rest) => rest.find("?>").map_or("", |end| rest[end + 2..].trim()),
            None => body.trim(),
        };
        return if body.starts_with('<') {
            vec![body]
        } else {
            Vec::new()
        };
    };
    let Some(after) = text[open..].find('>').map(|i| open + i + 1) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut i = after;
    let mut depth = 0usize;
    let mut start = None;
    while i < bytes.len() {
        if bytes[i] != b'<' {
            i += 1;
            continue;
        }
        let rest = &text[i..];
        let skip = |end: &str| rest.find(end).map(|e| e + end.len());
        if rest.starts_with("<!--") {
            i += skip("-->").unwrap_or(rest.len());
        } else if rest.starts_with("<![CDATA[") {
            i += skip("]]>").unwrap_or(rest.len());
        } else if rest.starts_with("<?") {
            i += skip("?>").unwrap_or(rest.len());
        } else if rest.starts_with("</") {
            let end = i + skip(">").unwrap_or(rest.len());
            if depth == 0 {
                break; // </DataObjects>
            }
            depth -= 1;
            if depth == 0
                && let Some(s) = start.take()
            {
                out.push(&text[s..end]);
            }
            i = end;
        } else {
            // A start tag: to its `>`, quotes stepped over.
            let mut j = i + 1;
            let mut quote = None;
            while j < bytes.len() {
                match (quote, bytes[j]) {
                    (None, b'"' | b'\'') => quote = Some(bytes[j]),
                    (Some(q), c) if c == q => quote = None,
                    (None, b'>') => break,
                    _ => {}
                }
                j += 1;
            }
            let self_closing = j > 0 && bytes[j - 1] == b'/';
            if depth == 0 {
                start = Some(i);
            }
            if self_closing {
                if depth == 0
                    && let Some(s) = start.take()
                {
                    out.push(&text[s..=j.min(bytes.len() - 1)]);
                }
            } else {
                depth += 1;
            }
            i = j + 1;
        }
    }
    out
}

/// The `uuid` attribute of an element's start tag.
fn root_uuid(element: &str) -> Option<&str> {
    let tag = &element[..element.find('>')?];
    let at = tag
        .find(" uuid=\"")
        .map(|i| i + 7)
        .or_else(|| tag.find(" uuid='").map(|i| i + 7))?;
    let end = tag[at..].find(['"', '\''])?;
    Some(&tag[at..at + end])
}

/// Take out what the API embeds and Energistics' JSON does not have: the
/// `_data` of references and of array nodes.
fn strip_embedded(json: &mut Json) {
    match json {
        Json::Object(map) => {
            map.remove("_data");
            map.values_mut().for_each(strip_embedded);
        }
        Json::Array(items) => items.iter_mut().for_each(strip_embedded),
        _ => {}
    }
}

/// What [`RestClient::prefetch`] brought.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Prefetch {
    /// Each object - asked, or referenced by one that was - by its URI, as
    /// Energistics JSON.
    pub objects: BTreeMap<String, Vec<u8>>,
    /// The metadata of every array those objects hold.
    pub arrays: Vec<(DataArrayIdentifier, DataArrayMetadata)>,
    /// Each object asked that the server holds and gives no JSON for - so
    /// neither what it references nor its arrays came -, as its XML.
    pub without_json: BTreeMap<String, Vec<u8>>,
    /// What was asked and is not held - the API omits what it did not find.
    pub missing: Vec<String>,
}

/// Take the embedded content out of `object`, into `prefetch`: a referenced
/// object becomes one more object, addressed in `dataspace` - the one of the
/// object that references it (P11); an array node's metadata becomes an
/// array. Deep: a referenced object's own references are taken too.
fn unroll(object: &mut Json, dataspace: &str, prefetch: &mut Prefetch) {
    match object {
        Json::Object(map) => {
            if let Some(embedded) = map.remove("_data") {
                if let Some(meta) = embedded_array(&embedded) {
                    prefetch.arrays.push(meta);
                } else if let (Some(kind), Some(uuid)) = (
                    embedded.get("$type").and_then(Json::as_str),
                    embedded.get("Uuid").and_then(Json::as_str),
                ) {
                    let uri = format!("eml:///dataspace('{dataspace}')/{kind}({uuid})");
                    if !prefetch.objects.contains_key(&uri) {
                        let mut embedded = embedded.clone();
                        unroll(&mut embedded, dataspace, prefetch);
                        prefetch
                            .objects
                            .insert(uri, serde_json::to_vec(&embedded).expect("JSON"));
                    }
                }
            }
            for value in map.values_mut() {
                unroll(value, dataspace, prefetch);
            }
        }
        Json::Array(items) => items.iter_mut().for_each(|i| unroll(i, dataspace, prefetch)),
        _ => {}
    }
}

/// An array node's embedded metadata - `{LogicalArrayType, TransportArrayType,
/// Dimensions, …, Uid: {Uri, PathInResource}}` - as ETP says it.
fn embedded_array(json: &Json) -> Option<(DataArrayIdentifier, DataArrayMetadata)> {
    let uid = json.get("Uid")?;
    let id = DataArrayIdentifier {
        uri: uid.get("Uri")?.as_str()?.to_string(),
        path_in_resource: uid.get("PathInResource")?.as_str()?.to_string(),
    };
    let dims = |key: &str| -> Vec<i64> {
        json.get(key)
            .and_then(Json::as_array)
            .map(|a| a.iter().filter_map(Json::as_i64).collect())
            .unwrap_or_default()
    };
    let meta = DataArrayMetadata {
        dimensions: dims("Dimensions"),
        preferred_subarray_dimensions: dims("PreferredSubarrayDimensions"),
        transport_array_type: enumeration(json, "TransportArrayType")?,
        logical_array_type: enumeration(json, "LogicalArrayType")?,
        store_last_write: time(json, "StoreLastWrite"),
        store_created: time(json, "StoreCreated"),
        custom_data: BTreeMap::new(),
    };
    Some((id, meta))
}

fn text(json: &Json, key: &str) -> String {
    json.get(key)
        .and_then(Json::as_str)
        .unwrap_or_default()
        .to_string()
}

fn custom(json: &Json) -> BTreeMap<String, DataValue> {
    let Some(Json::Object(map)) = json.get("customData") else {
        return BTreeMap::new();
    };
    map.iter()
        .map(|(k, v)| {
            let item = match v {
                Json::Bool(b) => DataValueItem::Boolean(*b),
                Json::String(s) => DataValueItem::String(s.clone()),
                Json::Number(n) => match n.as_i64() {
                    Some(i) => DataValueItem::Long(i),
                    None => DataValueItem::Double(n.as_f64().unwrap_or(f64::NAN)),
                },
                Json::Null => DataValueItem::Null,
                other => DataValueItem::String(other.to_string()),
            };
            (k.clone(), DataValue { item })
        })
        .collect()
}

/// A `DataValue` as this API's JSON - the inverse of [`custom`], for what a
/// dataspace's `customData` carries: a flag, a number, a string, a list of
/// strings. Anything else is `None`, and the caller refuses by name.
fn json_of_value(value: &DataValue) -> Option<Json> {
    Some(match &value.item {
        DataValueItem::Boolean(b) => Json::Bool(*b),
        DataValueItem::Int(i) => Json::from(*i),
        DataValueItem::Long(l) => Json::from(*l),
        DataValueItem::Float(f) => Json::from(*f),
        DataValueItem::Double(d) => Json::from(*d),
        DataValueItem::String(s) => Json::String(s.clone()),
        DataValueItem::ArrayOfString(a) => {
            Json::Array(a.values.iter().map(|s| Json::String(s.clone())).collect())
        }
        DataValueItem::Null => Json::Null,
        _ => return None,
    })
}

/// An array's values as the API's `Data`: a JSON list of numbers, flags or
/// strings - or, for bytes, the base64 string the definition allows.
///
/// **A no-value goes out as `null`**, and there is no other valid answer:
/// RFC 8259 has neither `NaN` nor `Infinity`, and a store that writes them
/// bare writes JSON that strict parsers refuse (Python's `json` does exactly
/// that). `serde_json` turns a `NaN` into `null` of itself; what matters is
/// that the reader knows it means *no value* - which it does, because the
/// logical type says the array is doubles.
///
/// The cost is the reason `--rest` reads arrays in **base64** wherever the
/// API offers it: a double is 8 bytes there against some twenty characters
/// and a decimal conversion here, and base64 carries a `NaN` for free,
/// being the IEEE bytes themselves.
fn json_of_array(item: &AnyArrayItem) -> Json {
    match item {
        AnyArrayItem::ArrayOfBoolean(a) => Json::Array(a.values.iter().map(|b| Json::Bool(*b)).collect()),
        AnyArrayItem::ArrayOfInt(a) => Json::Array(a.values.iter().map(|v| Json::from(*v)).collect()),
        AnyArrayItem::ArrayOfLong(a) => Json::Array(a.values.iter().map(|v| Json::from(*v)).collect()),
        AnyArrayItem::ArrayOfFloat(a) => Json::Array(a.values.iter().map(|v| Json::from(*v)).collect()),
        AnyArrayItem::ArrayOfDouble(a) => Json::Array(a.values.iter().map(|v| Json::from(*v)).collect()),
        AnyArrayItem::ArrayOfString(a) => {
            Json::Array(a.values.iter().map(|s| Json::String(s.clone())).collect())
        }
        AnyArrayItem::Bytes(bytes) => Json::String(base64::engine::general_purpose::STANDARD.encode(bytes)),
    }
}

/// A write's answer: the status, and then the body - the definition answers
/// the `PUT`s with a **boolean**, and a 200 carrying `false` is a refusal
/// that a status alone would read as success.
fn accepted(response: &HttpResponse) -> Result<(), ErrorInfo> {
    status(response)?;
    match serde_json::from_slice::<Json>(&response.body) {
        Ok(Json::Bool(false)) => Err(EtpError::EREQUEST_DENIED
            .with_detail("the API answered false - refused, and it gave no reason".to_string())
            .error_info()),
        _ => Ok(()),
    }
}

/// The transport type an item travels as - named as the schema names it.
fn transport_of(item: &AnyArrayItem) -> AnyArrayType {
    match item {
        AnyArrayItem::ArrayOfBoolean(_) => AnyArrayType::ArrayOfBoolean,
        AnyArrayItem::ArrayOfInt(_) => AnyArrayType::ArrayOfInt,
        AnyArrayItem::ArrayOfLong(_) => AnyArrayType::ArrayOfLong,
        AnyArrayItem::ArrayOfFloat(_) => AnyArrayType::ArrayOfFloat,
        AnyArrayItem::ArrayOfDouble(_) => AnyArrayType::ArrayOfDouble,
        AnyArrayItem::ArrayOfString(_) => AnyArrayType::ArrayOfString,
        AnyArrayItem::Bytes(_) => AnyArrayType::Bytes,
    }
}

/// A uuid from its text, `8-4-4-4-12` hex - `None` for anything else, since
/// a uuid guessed from a string that is not one would be one the store never
/// issued.
pub fn uuid_of(text: &str) -> Option<Uuid> {
    let hex: String = text.trim().chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 || text.trim().len() != 36 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, pair) in hex.as_bytes().chunks(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(Uuid(out))
}

/// Microseconds since the epoch, from an ISO 8601 UTC time
/// (`2025-07-31T15:15:38.151Z`); 0 when absent.
fn time(json: &Json, key: &str) -> i64 {
    json.get(key)
        .and_then(Json::as_str)
        .and_then(micros_of)
        .unwrap_or(0)
}

/// `YYYY-MM-DDTHH:MM:SS[.fraction]Z` to microseconds since the epoch.
pub fn micros_of(iso: &str) -> Option<i64> {
    let (date, rest) = iso.split_once('T')?;
    let mut d = date.splitn(3, '-').map(|p| p.parse::<i64>());
    let (y, m, day) = (d.next()?.ok()?, d.next()?.ok()?, d.next()?.ok()?);
    let rest = rest.trim_end_matches('Z');
    let (clock, fraction) = rest.split_once('.').unwrap_or((rest, ""));
    let mut c = clock.splitn(3, ':').map(|p| p.parse::<i64>());
    let (h, min, s) = (c.next()?.ok()?, c.next()?.ok()?, c.next()?.ok()?);
    let micros: i64 = format!("{:0<6}", &fraction[..fraction.len().min(6)])
        .parse()
        .ok()?;
    // Days from the civil date - Howard Hinnant's algorithm.
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(((days * 86_400 + h * 3_600 + min * 60 + s) * 1_000_000) + micros)
}

/// Microseconds since the epoch to ISO 8601 UTC.
pub fn iso(micros: i64) -> String {
    let secs = micros.div_euclid(1_000_000);
    let (days, day_secs) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z",
        day_secs / 3_600,
        day_secs % 3_600 / 60,
        day_secs % 60,
        micros.rem_euclid(1_000_000) / 1_000
    )
}

// -- small things -----------------------------------------------------------

/// The ETP error an HTTP status means, for a request the server answered.
fn status(response: &HttpResponse) -> Result<(), ErrorInfo> {
    let why = || message_of(&response.body);
    match response.status {
        200..=299 => Ok(()),
        404 => Err(EtpError::ENOT_FOUND.with_detail(why()).error_info()),
        403 => Err(EtpError::EREQUEST_DENIED.with_detail(why()).error_info()),
        _ => Err(EtpError::EINVALID_ARGUMENT
            .with_detail(format!("HTTP {}: {}", response.status, why()))
            .error_info()),
    }
}

/// The API's own words for an error: its `message`, or the body.
fn message_of(body: &[u8]) -> String {
    serde_json::from_slice::<Json>(body)
        .ok()
        .and_then(|j| {
            j.get("message")
                .map(|m| m.as_str().map(str::to_string).unwrap_or_else(|| m.to_string()))
        })
        .unwrap_or_else(|| String::from_utf8_lossy(&body[..body.len().min(300)]).into_owned())
}

fn single(error: ErrorInfo) -> Message {
    Message::ProtocolException(ProtocolException {
        error: Some(error),
        errors: BTreeMap::new(),
    })
}

fn unsupported(why: String) -> ClientError {
    ClientError::Invalid(EtpError::ENOTSUPPORTED.with_detail(why))
}

fn unreadable(what: &str, json: &Json) -> ClientError {
    ClientError::AnswerRefused {
        request: 0,
        incident: EtpError::EINVALID_MESSAGE.with_detail(format!(
            "{what}: an answer of an unexpected shape ({})",
            &json.to_string()[..json.to_string().len().min(120)]
        )),
    }
}

pub(crate) fn verb(method: Method) -> &'static str {
    match method {
        Method::Get => "GET",
        Method::Post => "POST",
        Method::Put => "PUT",
        Method::Delete => "DELETE",
    }
}

/// `resqml20.obj_X` out of an object URI, `""` when it has none.
fn type_of(uri: &str) -> String {
    let slash = uri.rfind('/').map_or(0, |s| s + 1);
    uri[slash..].split('(').next().unwrap_or_default().to_string()
}

/// `resqml20.*` matches `resqml20.obj_X`, and every type matches itself.
fn covers(pattern: &str, qualified_type: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => qualified_type.starts_with(prefix),
        None => pattern == qualified_type,
    }
}

/// Percent-encoding for a path segment or a query value: everything but the
/// unreserved characters of RFC 3986 - `example/dataspace` is one segment.
fn encode(text: &str) -> String {
    let mut out = String::new();
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// A query string; a key keeps its `$` - OData's `$skip`, `$top`, `$format`.
fn query_string(pairs: &[(String, String)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", encode(k).replace("%24", "$"), encode(v)))
        .collect::<Vec<_>>()
        .join("&")
}
