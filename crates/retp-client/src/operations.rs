//! The first things a client asks a store: its dataspaces, what they hold,
//! and objects by URI.
//!
//! Enough to try a server from the command line (tier 3). The storage
//! bridge (`energyml-etp-store`, tier 4) builds on these; the command line
//! only prints what they return - it holds no logic of its own.

use std::collections::BTreeMap;

use retp_avro::{Avro, EtpMessage};
use retp_proto::capabilities::{self, names};
use retp_proto::negotiate::Negotiated;
use retp_proto::uris::{UriUse, check_uri};
use retp_proto::wire::chunk_objects;
use retp_proto::wire::{Body, Delivery, EtpError, Exception};
use retp_types::datatypes::ErrorInfo;
use retp_types::datatypes::Uuid;
use retp_types::datatypes::object::{
    ContextInfo, ContextScopeKind, DataObject, Dataspace, Edge, PutResponse, RelationshipKind, Resource,
    SupportedType,
};
use retp_types::dispatch::Message;
use retp_types::protocol::dataspace::{DeleteDataspaces, GetDataspaces, PutDataspaces};
use retp_types::protocol::discovery::GetResources;
use retp_types::protocol::store::{Chunk, DeleteDataObjects, GetDataObjects, PutDataObjects};
use retp_types::protocol::supported_types::GetSupportedTypes;
use retp_types::protocol::transaction::{CommitTransaction, RollbackTransaction, StartTransaction};

use crate::ClientError;
use crate::caller::{Blocking, run};

/// The Store protocol's number - where objects are written and deleted.
const STORE: i32 = 4;

/// The answer of a non-map request: what came, and the error that ended it
/// early, if one did - a terminating exception is **one** error for the whole
/// request, never one per item (§3.7.3 §4.c).
#[derive(Debug, Clone, PartialEq)]
pub struct Listed<T> {
    /// The items, in the order they arrived.
    pub items: Vec<T>,
    /// The exception that ended the answer, if any.
    pub ended_by: Option<ErrorInfo>,
}

/// The dataspaces of a store (Dataspace, protocol 24).
pub type DataspaceList = Listed<Dataspace>;

/// A transaction a store opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transaction {
    /// What the store calls it - the only thing that ends it.
    pub uuid: Uuid,
    /// The dataspaces it spans.
    pub dataspaces: Vec<String>,
    /// Whether it may only read.
    pub read_only: bool,
}

/// What a store said when asked to open one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Opened {
    /// It opened.
    Open(Transaction),
    /// It would not, and said why.
    ///
    /// **Not a `ClientError`**: `Invalid` means *not sent, session
    /// untouched*, and this one was sent and answered. Losing that
    /// difference would make a store's decision look like a bug of ours.
    Refused(String),
    /// The store has no transactions at all - it answered
    /// `ENOTSUPPORTED` or `EUNSUPPORTED_PROTOCOL`, or, over REST, named the
    /// transaction `null`. Apart from [`Opened::Refused`] because a caller
    /// may go on **without** one where it would not go on after a refusal:
    /// a refusal says *not now*, this says *never here*.
    Unsupported(String),
}

/// What a store said when asked to end one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ended {
    /// It went through.
    Done,
    /// The store said no, and why.
    ///
    /// On a **commit**, this means the writes of the transaction are *not*
    /// in the store - the one outcome a caller must never read as done.
    /// On a rollback, it means they may still be.
    Refused(String),
}

/// What `GetResources` answered: the resources, the edges between them when
/// they were asked for, and the error that ended the answer early, if one
/// did.
#[derive(Debug, Clone, PartialEq)]
pub struct ResourceList {
    /// The resources, in the order they arrived.
    pub resources: Vec<Resource>,
    /// The edges, when `include_edges` was set (`GetResourcesEdgesResponse`).
    pub edges: Vec<Edge>,
    /// The exception that ended the answer, if any.
    pub ended_by: Option<ErrorInfo>,
}

/// The data object types under a URI (SupportedTypes, protocol 25).
pub type SupportedTypeList = Listed<SupportedType>;

/// What to ask Discovery (§8.3.1): where to start, which way, how far, what.
#[derive(Debug, Clone, PartialEq)]
pub struct ResourceQuery {
    /// A canonical dataspace or object URI (`eml:///`,
    /// `eml:///dataspace('…')`, an object's).
    pub uri: String,
    /// How many levels of the graph.
    pub depth: i32,
    /// Which way from `uri`: its targets, its sources, itself…
    pub scope: ContextScopeKind,
    /// Only these types (`resqml20.obj_TriangulatedSetRepresentation`,
    /// `resqml20.*`); every type when empty.
    pub data_object_types: Vec<String>,
    /// Also the edges between the resources.
    pub include_edges: bool,
    /// The store fills `sourceCount` and `targetCount`.
    pub count_objects: bool,
    /// Only the resources of this activity status (`activeStatusFilter`,
    /// §8.3.1); none, the default, sends `null` - every status.
    pub active_status: Option<retp_types::datatypes::object::ActiveStatusKind>,
}

impl ResourceQuery {
    /// What `uri` holds, one level down - the targets of a dataspace are the
    /// objects in it.
    pub fn new(uri: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            depth: 1,
            scope: ContextScopeKind::Targets,
            data_object_types: Vec::new(),
            include_edges: false,
            count_objects: false,
            active_status: None,
        }
    }
}

fn terminating(message: &Message) -> Option<ErrorInfo> {
    match message {
        Message::ProtocolException(exception) => match Exception::from_message(exception).ok()? {
            Exception::Single(error) => Some(error),
            Exception::Map(_) => None,
        },
        _ => None,
    }
}

/// The operations, for any [`Caller`] - written once, and run on the
/// blocking client by the functions beside this module.
pub mod asynchronous {
    use super::*;
    use crate::caller::Caller;

    /// `GetDataspaces`: every dataspace the store has.
    pub async fn list_dataspaces<K: Caller>(client: &mut K) -> Result<DataspaceList, ClientError> {
        let mut list = Listed {
            items: Vec::new(),
            ended_by: None,
        };
        for delivery in client.call(&GetDataspaces::default()).await? {
            if let Body::Message(message) = &delivery.body {
                match message {
                    Message::GetDataspacesResponse(r) => list.items.extend(r.dataspaces.iter().cloned()),
                    other => list.ended_by = list.ended_by.take().or_else(|| terminating(other)),
                }
            }
        }
        Ok(list)
    }

    /// `GetResources` (§8.3.1): what a query's URI holds.
    ///
    /// Discovery takes canonical URIs only (§3.7.4.2), so anything else is
    /// refused **here**, by name, before a server is asked.
    pub async fn list_resources<K: Caller>(
        client: &mut K,
        query: &ResourceQuery,
    ) -> Result<ResourceList, ClientError> {
        if let Err(incident) = check_uri(&query.uri, UriUse::CanonicalOnly, false) {
            return Err(ClientError::Invalid(incident));
        }
        let request = GetResources {
            context: ContextInfo {
                uri: query.uri.clone(),
                depth: query.depth,
                data_object_types: query.data_object_types.clone(),
                navigable_edges: RelationshipKind::Primary,
                ..Default::default()
            },
            scope: query.scope,
            count_objects: query.count_objects,
            include_edges: query.include_edges,
            active_status_filter: query.active_status,
            ..Default::default()
        };
        let mut list = ResourceList {
            resources: Vec::new(),
            edges: Vec::new(),
            ended_by: None,
        };
        for delivery in client.call(&request).await? {
            if let Body::Message(message) = &delivery.body {
                match message {
                    Message::GetResourcesResponse(r) => list.resources.extend(r.resources.iter().cloned()),
                    Message::GetResourcesEdgesResponse(r) => list.edges.extend(r.edges.iter().cloned()),
                    other => list.ended_by = list.ended_by.take().or_else(|| terminating(other)),
                }
            }
        }
        Ok(list)
    }

    /// `GetSupportedTypes` (§22.3.1): the data object types found - or, with
    /// `return_empty_types`, supported - from `uri` in the direction `scope`,
    /// counted when `count_objects` is set.
    ///
    /// The URI is canonical and the scope `sources` or `targets`, "MUST" both:
    /// anything else is refused here, before the server is asked.
    pub async fn list_supported_types<K: Caller>(
        client: &mut K,
        uri: &str,
        scope: ContextScopeKind,
        count_objects: bool,
        return_empty_types: bool,
    ) -> Result<SupportedTypeList, ClientError> {
        if let Err(incident) = check_uri(uri, UriUse::CanonicalOnly, false) {
            return Err(ClientError::Invalid(incident));
        }
        if !matches!(scope, ContextScopeKind::Sources | ContextScopeKind::Targets) {
            return Err(ClientError::Invalid(EtpError::EINVALID_ARGUMENT.with_detail(
                "GetSupportedTypes goes toward the sources or the targets of its URI, nothing else (§22.3.1)",
            )));
        }
        let request = GetSupportedTypes {
            uri: uri.to_string(),
            scope,
            return_empty_types,
            count_objects,
        };
        let mut list = Listed {
            items: Vec::new(),
            ended_by: None,
        };
        for delivery in client.call(&request).await? {
            if let Body::Message(message) = &delivery.body {
                match message {
                    Message::GetSupportedTypesResponse(r) => {
                        list.items.extend(r.supported_types.iter().cloned())
                    }
                    other => list.ended_by = list.ended_by.take().or_else(|| terminating(other)),
                }
            }
        }
        Ok(list)
    }

    /// `GetDataObjects`: objects by URI, each answered on its own - found, or
    /// with its own error (a map request, §3.7.3 §4.b) - in `format`, which the
    /// session must have agreed (`xml`, `json`; §5.3.2): one it did not is
    /// refused here, before the server is asked.
    pub async fn get_data_objects<K: Caller>(
        client: &mut K,
        uris: &[String],
        format: &str,
    ) -> Result<BTreeMap<String, Result<DataObject, ErrorInfo>>, ClientError> {
        let negotiated = client.negotiated();
        let agreed = &negotiated.formats;
        if !agreed.iter().any(|f| f == format) {
            return Err(ClientError::Invalid(EtpError::EINVALID_ARGUMENT.with_detail(
                format!(
                    "format `{format}` was not agreed for this session ({})",
                    agreed.join(", ")
                ),
            )));
        }
        let keys: BTreeMap<String, String> = uris
            .iter()
            .enumerate()
            .map(|(i, u)| ((i + 1).to_string(), u.clone()))
            .collect();
        let request = GetDataObjects {
            uris: keys.clone(),
            format: format.to_string(),
        };
        Ok(objects_of(keys, client.call(&request).await?))
    }

    /// [`get_data_objects`], in handfuls of `batch` sent **together** -
    /// `at_once` requests in flight, each answered to its own. A handful the
    /// store refuses fails alone: its URIs carry the refusal, the others
    /// their objects. What a package or a dataspace is read with.
    pub async fn get_data_objects_in_batches<K: Caller>(
        client: &mut K,
        uris: &[String],
        format: &str,
        batch: usize,
    ) -> Result<BTreeMap<String, Result<DataObject, ErrorInfo>>, ClientError> {
        let negotiated = client.negotiated();
        if !negotiated.formats.iter().any(|f| f == format) {
            return Err(ClientError::Invalid(EtpError::EINVALID_ARGUMENT.with_detail(
                format!(
                    "format `{format}` was not agreed for this session ({})",
                    negotiated.formats.join(", ")
                ),
            )));
        }
        let batch = batch.max(1);
        let handfuls: Vec<BTreeMap<String, String>> = uris
            .chunks(batch)
            .map(|handful| {
                handful
                    .iter()
                    .enumerate()
                    .map(|(i, u)| ((i + 1).to_string(), u.clone()))
                    .collect()
            })
            .collect();
        let requests: Vec<GetDataObjects> = handfuls
            .iter()
            .map(|keys| GetDataObjects {
                uris: keys.clone(),
                format: format.to_string(),
            })
            .collect();
        let mut out = BTreeMap::new();
        for (keys, answer) in handfuls.into_iter().zip(client.call_all(&requests).await) {
            match answer {
                Ok(deliveries) => out.extend(objects_of(keys, deliveries)),
                Err(why) => {
                    let info = ErrorInfo {
                        message: why.to_string(),
                        code: -1,
                    };
                    for uri in keys.into_values() {
                        out.insert(uri, Err(info.clone()));
                    }
                }
            }
        }
        Ok(out)
    }

    /// The objects of one `GetDataObjects` answer, keyed back by URI.
    fn objects_of(
        keys: BTreeMap<String, String>,
        deliveries: Vec<Delivery>,
    ) -> BTreeMap<String, Result<DataObject, ErrorInfo>> {
        let mut answers: BTreeMap<String, Result<DataObject, ErrorInfo>> = BTreeMap::new();
        let mut ended_by = None;
        for delivery in deliveries {
            let Body::Message(message) = &delivery.body else {
                continue;
            };
            match message {
                Message::GetDataObjectsResponse(r) => {
                    for (key, object) in &r.data_objects {
                        answers.insert(key.clone(), Ok(object.clone()));
                    }
                }
                Message::ProtocolException(exception) => match Exception::from_message(exception) {
                    Ok(Exception::Map(errors)) => {
                        for (key, error) in errors {
                            answers.insert(key, Err(error));
                        }
                    }
                    Ok(Exception::Single(error)) => ended_by = Some(error),
                    Err(_) => {}
                },
                _ => {}
            }
        }
        // Keyed back by URI, and a key the answer never mentioned is said: the
        // request failed as a whole, or the server forgot it.
        let mut by_uri = BTreeMap::new();
        for (key, uri) in keys {
            let answer = answers.remove(&key).unwrap_or_else(|| {
                Err(ended_by.clone().unwrap_or_else(|| ErrorInfo {
                    message: "the server answered nothing for this URI".into(),
                    code: 0,
                }))
            });
            by_uri.insert(uri, answer);
        }
        by_uri
    }

    /// The answers of a keyed request, each back to what it was asked about.
    ///
    /// Every keyed operation of ETP has this shape - a map out, a map back,
    /// and a `ProtocolException` that is either **per key** (this one failed)
    /// or **single** (the whole request did). Written once because the two
    /// are easy to confuse: a single exception read as a per-key one loses
    /// the reason for every key at once, and nothing downstream can tell.
    ///
    /// A key the store never mentioned is **said**, never taken for success:
    /// a write whose outcome is unknown is not a write that worked.
    async fn keyed_answers<K: Caller, R: EtpMessage + Avro, T: Clone>(
        client: &mut K,
        request: &R,
        labels: BTreeMap<String, String>,
        take: impl FnMut(&Message, &mut BTreeMap<String, T>),
    ) -> Result<BTreeMap<String, Result<T, ErrorInfo>>, ClientError> {
        let deliveries = client.call(request).await?;
        Ok(sort_answers(deliveries, labels, take))
    }

    /// The answers of a keyed request, each back to what it was asked about.
    fn sort_answers<T: Clone>(
        deliveries: Vec<retp_proto::wire::Delivery>,
        labels: BTreeMap<String, String>,
        mut take: impl FnMut(&Message, &mut BTreeMap<String, T>),
    ) -> BTreeMap<String, Result<T, ErrorInfo>> {
        let mut found: BTreeMap<String, T> = BTreeMap::new();
        let mut failed: BTreeMap<String, ErrorInfo> = BTreeMap::new();
        let mut ended_by = None;
        for delivery in deliveries {
            let Body::Message(message) = &delivery.body else {
                continue;
            };
            match message {
                Message::ProtocolException(exception) => match Exception::from_message(exception) {
                    Ok(Exception::Map(errors)) => {
                        for (key, error) in errors {
                            failed.insert(key, error);
                        }
                    }
                    Ok(Exception::Single(error)) => ended_by = Some(error),
                    Err(_) => {}
                },
                other => take(other, &mut found),
            }
        }
        let mut by_label = BTreeMap::new();
        for (key, label) in labels {
            let answer = match (found.remove(&key), failed.remove(&key)) {
                (Some(value), _) => Ok(value),
                (None, Some(error)) => Err(error),
                (None, None) => Err(ended_by.clone().unwrap_or_else(|| ErrorInfo {
                    message: "the server answered nothing for this one".into(),
                    code: 0,
                })),
            };
            by_label.insert(label, answer);
        }
        by_label
    }

    /// [`keyed_answers`] for a request that leaves in parts - the message
    /// and the `Chunk`s of its BLOBs.
    async fn keyed_answers_parts<K: Caller, R: EtpMessage + Avro, P: EtpMessage + Avro, T: Clone>(
        client: &mut K,
        request: &R,
        parts: &[P],
        labels: BTreeMap<String, String>,
        take: impl FnMut(&Message, &mut BTreeMap<String, T>),
    ) -> Result<BTreeMap<String, Result<T, ErrorInfo>>, ClientError> {
        let deliveries = client.call_parts(request, parts).await?;
        Ok(sort_answers(deliveries, labels, take))
    }

    /// What a keyed request said of each of its `n` keys, `1..=n`: an error
    /// by key marks that key, a single exception marks them all, a message
    /// `is_the_answer` picks confirms the rest - and nothing said is not
    /// success: a write whose outcome nobody stated is the one a caller must
    /// not take for done.
    pub(crate) fn keyed_outcomes(
        deliveries: Vec<Delivery>,
        n: usize,
        is_the_answer: impl Fn(&Message) -> bool,
    ) -> Vec<Result<(), ErrorInfo>> {
        let mut answered = false;
        let mut by_key: BTreeMap<String, ErrorInfo> = BTreeMap::new();
        let mut all = None;
        for delivery in deliveries {
            let Body::Message(message) = &delivery.body else {
                continue;
            };
            match message {
                Message::ProtocolException(exception) => match Exception::from_message(exception) {
                    Ok(Exception::Map(errors)) => by_key.extend(errors),
                    Ok(Exception::Single(error)) => all = Some(error),
                    Err(_) => {}
                },
                other if is_the_answer(other) => answered = true,
                _ => {}
            }
        }
        (1..=n)
            .map(|k| {
                let key = k.to_string();
                if let Some(error) = by_key.get(&key) {
                    Err(error.clone())
                } else if let Some(error) = &all {
                    Err(error.clone())
                } else if answered {
                    Ok(())
                } else {
                    Err(nothing_said())
                }
            })
            .collect()
    }

    /// The failure of a request nobody stated the outcome of.
    pub(crate) fn nothing_said() -> ErrorInfo {
        ErrorInfo {
            message: "the store answered nothing for this request".into(),
            code: 0,
        }
    }

    /// `1`, `2`, … against what was asked - the keys ETP maps are made of.
    fn labelled<T>(items: &[T], of: impl Fn(&T) -> String) -> BTreeMap<String, String> {
        items
            .iter()
            .enumerate()
            .map(|(i, item)| ((i + 1).to_string(), of(item)))
            .collect()
    }

    /// How big one data object may be **in the store**: `MaxDataObjectSize`
    /// (§3.3.2.4), the endpoint's lowered by the Store protocol's, and `None`
    /// when no end states one. This is the cap that refuses: a store will not
    /// hold an object bigger than it says, however it is sent.
    ///
    /// What one **message** may carry is the other bound, and it does not
    /// refuse - it **cuts**: an object bigger than a message goes out in
    /// `Chunk`s ([`piece_size`]). The two were once one number, and
    /// the difference is what makes a 20 MB object sendable to a store that
    /// takes 16 MiB messages.
    ///
    /// One thing to know, found by a test: a capability set with nothing in
    /// it still **answers the specification's default** for a name the
    /// specification gives one to (`Capabilities::long`), so `max_payload` is
    /// the default 16 MiB even for the REST client, which states nothing and
    /// has no message at all.
    fn object_cap(negotiated: &Negotiated) -> Option<u64> {
        let endpoint = capabilities::smaller_of_two(
            negotiated.ours.long(names::MAX_DATA_OBJECT_SIZE),
            negotiated.theirs.long(names::MAX_DATA_OBJECT_SIZE),
        );
        let protocol = negotiated.protocols.get(&STORE).and_then(|p| {
            capabilities::smaller_of_two(
                p.ours.long(names::MAX_DATA_OBJECT_SIZE),
                p.theirs.long(names::MAX_DATA_OBJECT_SIZE),
            )
        });
        capabilities::data_object_size(endpoint, protocol, None).and_then(|size| u64::try_from(size).ok())
    }

    /// The bytes of object data one message may carry beside its header -
    /// the same reckoning arrays make ([`crate::arrays::message_budget`]),
    /// reused rather than rewritten. An object bigger than this leaves as
    /// `Chunk`s of this size.
    fn piece_size(negotiated: &Negotiated) -> usize {
        usize::try_from(crate::arrays::message_budget(negotiated, 16 * 1024 * 1024))
            .unwrap_or(usize::MAX)
            .max(1)
    }

    /// The BLOB id of an object about to leave in chunks: its own uuid, so
    /// that the id is unique among the objects of one message and names what
    /// it carries. An object whose URI yields no uuid - a store may address
    /// one by an alternate URI - takes a fold of its URI's bytes instead.
    fn blob_id_of(object: &DataObject) -> Uuid {
        let parsed = energyml_core::identifiers::EtpUri::parse(&object.resource.uri).ok();
        if let Some(uuid) = parsed.and_then(|u| u.uuid) {
            let hex: String = uuid.chars().filter(char::is_ascii_hexdigit).collect();
            if hex.len() == 32 {
                let mut out = [0u8; 16];
                let ok = hex.as_bytes().chunks(2).enumerate().all(|(i, pair)| {
                    std::str::from_utf8(pair)
                        .ok()
                        .and_then(|p| u8::from_str_radix(p, 16).ok())
                        .map(|b| out[i] = b)
                        .is_some()
                });
                if ok {
                    return Uuid(out);
                }
            }
        }
        // FNV-1a over the URI, twice with different seeds, for sixteen bytes.
        let mut out = [0u8; 16];
        for (half, seed) in [(0usize, 0xcbf2_9ce4_8422_2325u64), (8, 0x84222325_cbf29ce4u64)] {
            let mut h = seed;
            for b in object.resource.uri.bytes() {
                h ^= u64::from(b);
                h = h.wrapping_mul(0x0000_0100_0000_01b3);
            }
            out[half..half + 8].copy_from_slice(&h.to_le_bytes());
        }
        Uuid(out)
    }

    /// Write objects, and say what became of each.
    ///
    /// **`PutDataObjects` is an upsert** (§9.1.1, p.178): writing an object
    /// that is already there replaces it, and that is not a failure. Nothing
    /// here checks first - a read before every write would double the round
    /// trips to enforce a rule the store does not have.
    ///
    /// `prune_contained` asks the store to drop the contained objects a
    /// replacement leaves behind. Off by default, because dropping data is
    /// not something a caller should get without naming it.
    ///
    /// Nothing is guessed: a session that states no size is not guarded here,
    /// and the store says what it will not take (`object_budget`).
    ///
    /// Two things are refused **before anything leaves**, so that a refusal
    /// names its cause instead of arriving as a store's error: a format the
    /// session did not agree, and an object above the store's
    /// `MaxDataObjectSize`, which it would not hold however it were sent.
    ///
    /// An object bigger than one **message** is another matter: it goes out
    /// in `Chunk`s (§3.7.3.2) - its `data` emptied, a `blobId` set, and the
    /// bytes following in the same multipart request, the last chunk with the
    /// FIN. The store reassembles before its handler sees the object, as this
    /// client does on the way in.
    pub async fn put_data_objects<K: Caller>(
        client: &mut K,
        objects: &[DataObject],
        prune_contained: bool,
    ) -> Result<BTreeMap<String, Result<PutResponse, ErrorInfo>>, ClientError> {
        let negotiated = client.negotiated();
        let agreed = &negotiated.formats;
        for object in objects {
            if !agreed.contains(&object.format) {
                return Err(ClientError::Invalid(EtpError::EINVALID_ARGUMENT.with_detail(
                    format!(
                        "format `{}` was not agreed for this session ({})",
                        object.format,
                        agreed.join(", ")
                    ),
                )));
            }
            if let Some(cap) = object_cap(&negotiated)
                && object.data.len() as u64 > cap
            {
                return Err(ClientError::Invalid(EtpError::EINVALID_ARGUMENT.with_detail(format!(
                    "{} is {} bytes, more than the {cap} this store holds for one object (MaxDataObjectSize)",
                    object.resource.uri,
                    object.data.len()
                ))));
            }
        }
        let labels = labelled(objects, |object| object.resource.uri.clone());
        let mut request = PutDataObjects {
            data_objects: labels.keys().cloned().zip(objects.iter().cloned()).collect(),
            prune_contained_objects: prune_contained,
        };
        let piece = piece_size(&negotiated);
        let ids: Vec<Uuid> = request
            .data_objects
            .values()
            .filter(|o| o.data.len() > piece)
            .map(blob_id_of)
            .collect();
        let mut next = ids.into_iter();
        let chunks: Vec<Chunk> = chunk_objects(request.data_objects.values_mut(), piece, || {
            next.next().expect("one id per big object")
        });
        let take = |message: &Message, found: &mut BTreeMap<String, PutResponse>| {
            if let Message::PutDataObjectsResponse(r) = message {
                for (key, put) in &r.success {
                    found.insert(key.clone(), put.clone());
                }
            }
        };
        if chunks.is_empty() {
            keyed_answers(client, &request, labels, take).await
        } else {
            keyed_answers_parts(client, &request, &chunks, labels, take).await
        }
    }

    /// Delete objects, and say for each what the store actually deleted.
    ///
    /// The answer is a **list of URIs per key**, not a yes: deleting one
    /// object may take its contained objects with it, and the store is the
    /// only one that knows which. Reporting a bare success would hide a
    /// cascade nobody asked about.
    ///
    /// A store that refuses because the deletion would orphan contained
    /// objects answers `ENOCASCADE_DELETE` (4003) for that key, and it comes
    /// back as it was sent: it is a business refusal with something to do
    /// about it - ask again with `prune_contained` - not a breakdown.
    pub async fn delete_data_objects<K: Caller>(
        client: &mut K,
        uris: &[String],
        prune_contained: bool,
    ) -> Result<BTreeMap<String, Result<Vec<String>, ErrorInfo>>, ClientError> {
        let labels = labelled(uris, Clone::clone);
        let request = DeleteDataObjects {
            uris: labels
                .iter()
                .map(|(key, uri)| (key.clone(), uri.clone()))
                .collect(),
            prune_contained_objects: prune_contained,
        };
        keyed_answers(client, &request, labels, |message, found| {
            if let Message::DeleteDataObjectsResponse(r) = message {
                for (key, deleted) in &r.deleted_uris {
                    found.insert(key.clone(), deleted.values.clone());
                }
            }
        })
        .await
    }

    /// Open a transaction over these dataspaces (Transaction, protocol 18).
    ///
    /// A store answers with `successful` and a `failureReason`, **not** an
    /// exception, so a refusal is an ordinary answer that says no - and a
    /// client reading only the uuid would carry on writing into a transaction
    /// that was never opened.
    ///
    /// `read_only` asks for a consistent view rather than a unit of writing;
    /// `message` is what the store may show to a person looking at its locks.
    pub async fn start_transaction<K: Caller>(
        client: &mut K,
        dataspaces: &[String],
        read_only: bool,
        message: &str,
    ) -> Result<Opened, ClientError> {
        // §3.7.4.2: a request of this kind takes the canonical form whatever
        // was negotiated. Refused here, where the session is untouched, rather
        // than by a store that has already opened something.
        for uri in dataspaces {
            if let Err(incident) = check_uri(uri, UriUse::CanonicalOnly, false) {
                return Err(ClientError::Invalid(incident));
            }
        }
        let request = StartTransaction {
            read_only,
            message: message.to_string(),
            dataspace_uris: dataspaces.to_vec(),
        };
        let mut answer = None;
        let mut unsupported = None;
        for delivery in client.call(&request).await? {
            if let Body::Message(Message::StartTransactionResponse(r)) = &delivery.body {
                answer = Some(r.clone());
            }
            if let Some(exception) = delivery.exception() {
                let info = match exception {
                    retp_proto::wire::Exception::Single(info) => Some(info),
                    retp_proto::wire::Exception::Map(map) => map.into_values().next(),
                };
                if let Some(info) = info
                    && (info.code == EtpError::ENOTSUPPORTED.code()
                        || info.code == EtpError::EUNSUPPORTED_PROTOCOL.code())
                {
                    unsupported = Some(info.message);
                }
            }
        }
        if let Some(why) = unsupported {
            return Ok(Opened::Unsupported(said_why(&why)));
        }
        let Some(answer) = answer else {
            return Ok(Opened::Refused(
                "the store answered nothing about the transaction".into(),
            ));
        };
        if !answer.successful {
            return Ok(Opened::Refused(said_why(&answer.failure_reason)));
        }
        Ok(Opened::Open(Transaction {
            uuid: answer.transaction_uuid,
            dataspaces: dataspaces.to_vec(),
            read_only,
        }))
    }

    /// Commit a transaction: everything written in it becomes visible, or
    /// none of it does.
    ///
    /// A refusal here is the expensive one - the writes are gone -, so it is
    /// returned rather than swallowed. And an **interrupted** commit is
    /// neither: `ClientError::Interrupted` says the outcome is unknown, which
    /// is the rule of issue #201 and the only honest answer when the
    /// socket falls between the request and its response.
    pub async fn commit_transaction<K: Caller>(
        client: &mut K,
        transaction: &Transaction,
    ) -> Result<Ended, ClientError> {
        let request = CommitTransaction {
            transaction_uuid: transaction.uuid,
        };
        let mut answer = None;
        for delivery in client.call(&request).await? {
            if let Body::Message(Message::CommitTransactionResponse(r)) = &delivery.body {
                answer = Some((r.transaction_uuid, r.successful, r.failure_reason.clone()));
            }
        }
        Ok(ended(answer, transaction, "commit"))
    }

    /// Roll a transaction back: nothing written in it stays.
    pub async fn rollback_transaction<K: Caller>(
        client: &mut K,
        transaction: &Transaction,
    ) -> Result<Ended, ClientError> {
        let request = RollbackTransaction {
            transaction_uuid: transaction.uuid,
        };
        let mut answer = None;
        for delivery in client.call(&request).await? {
            if let Body::Message(Message::RollbackTransactionResponse(r)) = &delivery.body {
                answer = Some((r.transaction_uuid, r.successful, r.failure_reason.clone()));
            }
        }
        Ok(ended(answer, transaction, "rollback"))
    }

    /// What a commit or a rollback answered, read the same way for both -
    /// **and the uuid is checked**.
    ///
    /// A store answering about another transaction is answering about another
    /// transaction: believing it would report a commit that happened somewhere
    /// else. Nothing in the protocol makes that impossible, and one comparison
    /// makes it harmless.
    fn ended(answer: Option<(Uuid, bool, String)>, transaction: &Transaction, what: &str) -> Ended {
        let Some((uuid, successful, why)) = answer else {
            return Ended::Refused(format!("the store answered nothing about the {what}"));
        };
        if uuid != transaction.uuid {
            return Ended::Refused(format!(
                "the store answered about another transaction: {uuid:?} where {:?} was asked",
                transaction.uuid
            ));
        }
        if successful {
            Ended::Done
        } else {
            Ended::Refused(said_why(&why))
        }
    }

    /// A store's reason, or the fact that it gave none - never an empty
    /// string, which reads on screen as a refusal with nothing wrong.
    fn said_why(reason: &str) -> String {
        if reason.trim().is_empty() {
            "the store gave no reason".to_string()
        } else {
            reason.to_string()
        }
    }

    /// Create or replace dataspaces, and say what became of each.
    ///
    /// On an OSDU store the ACLs and legal tags travel in the dataspace's
    /// `custom_data`. Nothing is invented here:
    /// what the caller did not put in is not sent, and the store says what it
    /// requires.
    pub async fn put_dataspaces<K: Caller>(
        client: &mut K,
        dataspaces: &[Dataspace],
    ) -> Result<BTreeMap<String, Result<String, ErrorInfo>>, ClientError> {
        let labels = labelled(dataspaces, |space| space.uri.clone());
        let request = PutDataspaces {
            dataspaces: labels.keys().cloned().zip(dataspaces.iter().cloned()).collect(),
        };
        keyed_answers(client, &request, labels, |message, found| {
            if let Message::PutDataspacesResponse(r) = message {
                for (key, said) in &r.success {
                    found.insert(key.clone(), said.clone());
                }
            }
        })
        .await
    }

    /// Delete dataspaces by URI (`DeleteDataspaces`, protocol 24), each
    /// answered for itself: a store may hold one and refuse another - not
    /// empty, not the caller's.
    pub async fn delete_dataspaces<K: Caller>(
        client: &mut K,
        uris: &[String],
    ) -> Result<BTreeMap<String, Result<String, ErrorInfo>>, ClientError> {
        let labels = labelled(uris, Clone::clone);
        let request = DeleteDataspaces {
            uris: labels
                .iter()
                .map(|(key, uri)| (key.clone(), uri.clone()))
                .collect(),
        };
        keyed_answers(client, &request, labels, |message, found| {
            if let Message::DeleteDataspacesResponse(r) = message {
                for (key, said) in &r.success {
                    found.insert(key.clone(), said.clone());
                }
            }
        })
        .await
    }
}

/// [`asynchronous::list_dataspaces`], on the blocking client.
pub fn list_dataspaces<K: Blocking>(client: &mut K) -> Result<DataspaceList, ClientError> {
    run(asynchronous::list_dataspaces(client))
}

/// [`asynchronous::put_data_objects`], on the blocking client.
pub fn put_data_objects<K: Blocking>(
    client: &mut K,
    objects: &[DataObject],
    prune_contained: bool,
) -> Result<BTreeMap<String, Result<PutResponse, ErrorInfo>>, ClientError> {
    run(asynchronous::put_data_objects(client, objects, prune_contained))
}

/// [`asynchronous::delete_data_objects`], on the blocking client.
pub fn delete_data_objects<K: Blocking>(
    client: &mut K,
    uris: &[String],
    prune_contained: bool,
) -> Result<BTreeMap<String, Result<Vec<String>, ErrorInfo>>, ClientError> {
    run(asynchronous::delete_data_objects(client, uris, prune_contained))
}

/// [`asynchronous::start_transaction`], on the blocking client.
pub fn start_transaction<K: Blocking>(
    client: &mut K,
    dataspaces: &[String],
    read_only: bool,
    message: &str,
) -> Result<Opened, ClientError> {
    run(asynchronous::start_transaction(
        client, dataspaces, read_only, message,
    ))
}

/// [`asynchronous::commit_transaction`], on the blocking client.
pub fn commit_transaction<K: Blocking>(
    client: &mut K,
    transaction: &Transaction,
) -> Result<Ended, ClientError> {
    run(asynchronous::commit_transaction(client, transaction))
}

/// [`asynchronous::rollback_transaction`], on the blocking client.
pub fn rollback_transaction<K: Blocking>(
    client: &mut K,
    transaction: &Transaction,
) -> Result<Ended, ClientError> {
    run(asynchronous::rollback_transaction(client, transaction))
}

/// [`asynchronous::put_dataspaces`], on the blocking client.
pub fn put_dataspaces<K: Blocking>(
    client: &mut K,
    dataspaces: &[Dataspace],
) -> Result<BTreeMap<String, Result<String, ErrorInfo>>, ClientError> {
    run(asynchronous::put_dataspaces(client, dataspaces))
}

/// One `customData` value of a dataspace, from the text a person types:
/// `a,b` is a list of strings (OSDU's `owners`, `viewers`, `legaltags`), any
/// other text one string. The command line and the conformance suite read
/// `KEY=VALUE` through this, so both send the same value for the same text.
pub fn custom_data_value(value: &str) -> retp_types::datatypes::DataValue {
    use retp_types::datatypes::{ArrayOfString, DataValue, DataValueItem};
    let item = if value.contains(',') {
        DataValueItem::ArrayOfString(ArrayOfString {
            values: value.split(',').map(|v| v.trim().to_string()).collect(),
        })
    } else {
        DataValueItem::String(value.to_string())
    };
    DataValue { item }
}

/// [`asynchronous::delete_dataspaces`], on the blocking client.
pub fn delete_dataspaces<K: Blocking>(
    client: &mut K,
    uris: &[String],
) -> Result<BTreeMap<String, Result<String, ErrorInfo>>, ClientError> {
    run(asynchronous::delete_dataspaces(client, uris))
}

/// [`asynchronous::list_resources`], on the blocking client.
pub fn list_resources<K: Blocking>(
    client: &mut K,
    query: &ResourceQuery,
) -> Result<ResourceList, ClientError> {
    run(asynchronous::list_resources(client, query))
}

/// [`asynchronous::list_supported_types`], on the blocking client.
pub fn list_supported_types<K: Blocking>(
    client: &mut K,
    uri: &str,
    scope: ContextScopeKind,
    count_objects: bool,
    return_empty_types: bool,
) -> Result<SupportedTypeList, ClientError> {
    run(asynchronous::list_supported_types(
        client,
        uri,
        scope,
        count_objects,
        return_empty_types,
    ))
}

/// [`asynchronous::get_data_objects`], on the blocking client.
pub fn get_data_objects<K: Blocking>(
    client: &mut K,
    uris: &[String],
    format: &str,
) -> Result<BTreeMap<String, Result<DataObject, ErrorInfo>>, ClientError> {
    run(asynchronous::get_data_objects(client, uris, format))
}

/// The canonical URI of a dataspace given by its path - `demo`,
/// `carl/teapot` - or `path_or_uri` itself when it is a URI already. An empty
/// path is the default dataspace, `eml:///`.
pub fn dataspace_uri(path_or_uri: &str) -> String {
    if path_or_uri.starts_with("eml:") {
        path_or_uri.to_string()
    } else if path_or_uri.is_empty() {
        "eml:///".to_string()
    } else {
        format!("eml:///dataspace('{path_or_uri}')")
    }
}

/// A file name for the object at `uri`, in `format`: `<type>_<uuid>.<ext>`,
/// the object version appended when the URI has one -
/// `resqml20.obj_TriangulatedSetRepresentation_9298c0c3-….xml`. `None` for a
/// URI that names no object.
pub fn object_file_name(uri: &str, format: &str) -> Option<String> {
    let parsed = energyml_core::identifiers::EtpUri::parse(uri).ok()?;
    let (object_type, uuid) = (parsed.object_type?, parsed.uuid?);
    let family = match (parsed.domain, parsed.version) {
        (Some(domain), Some(version)) => format!("{domain}{version}."),
        _ => String::new(),
    };
    let version = parsed
        .object_version
        .map(|v| {
            format!(
                "_{}",
                v.replace(|c: char| !c.is_ascii_alphanumeric() && c != '.' && c != '-', "_")
            )
        })
        .unwrap_or_default();
    let extension = if format == "json" { "json" } else { "xml" };
    Some(format!("{family}{object_type}_{uuid}{version}.{extension}"))
}
