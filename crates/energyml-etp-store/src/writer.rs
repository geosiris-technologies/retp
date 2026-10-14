//! The writing traits on an ETP dataspace: [`EnergymlWriter`] and
//! [`EnergymlDataspaces`] for [`EtpStore`].
//!
//! Each method is one exchange of the client, like the reading half in the
//! crate root -
//!
//! | the trait | ETP |
//! |---|---|
//! | `put_objects`, `put_raw` | Store (4), `PutDataObjects` |
//! | `delete_objects` | Store (4), `DeleteDataObjects` |
//! | `begin`, `commit`, `rollback` | Transaction (18) |
//! | `put_arrays` | DataArray (9), `PutDataArrays`, and for an array too big for one message `PutUninitializedDataArrays` then `PutDataSubarrays` (the client's own cut) |
//! | `list_dataspaces`, `put_dataspaces`, `delete_dataspaces` | Dataspace (24) |
//!
//! # Refused by name
//!
//! - **Part of an array** (`start` or `count` set): `retp-client` cuts an array
//!   into blocks itself but offers no call that writes **one block the caller
//!   chose**, so the store says [`Error::Unsupported`] rather than rebuild the
//!   `PutDataSubarrays` exchange here (a second implementation of the same
//!   door).
//! - **Declaring an array before writing it** (`declare_arrays`): the same
//!   reason, `PutUninitializedDataArrays` is only reachable inside
//!   `write_arrays_with`. The trait's default refusal stands.
//! - **Another dataspace**: an `EtpStore` is bound to one. An object, array or
//!   transaction that names another is [`Error::BadUri`] or
//!   [`Error::NotFound`], never written through this store into somewhere else.
//!   Reaching another dataspace means building another `EtpStore` on the same
//!   session.
//!
//! # One result per item
//!
//! The same URI twice in one call is two upserts, in order: ETP keys a plural
//! answer by the URI, so the repeats are sent in later requests and each gets its
//! own answer (`unique_rounds`).
//!
//! # The listing cache
//!
//! Every request that leaves invalidates the cache `EtpStore::listing` keeps,
//! whatever the answer: a refusal or a session that fell says nothing about what
//! the dataspace holds now, and a stale listing makes `arrays_of` miss the
//! proxy of an object that was just written.

use std::collections::BTreeMap;

use async_trait::async_trait;
use energyml_core::identifiers::{self, EtpUri};
use energyml_core::identity;
use energyml_core::registry::{DynObject, to_part};
use energyml_core::xml::XmlWriteOptions;
use energyml_storage::array::{ArrayData, ArrayLocation};
use energyml_storage::dataspaces::EnergymlDataspaces;
use energyml_storage::error::{Error, ExternalDataReason, Result};
use energyml_storage::raw::RawObject;
use energyml_storage::resource::Dataspace;
use energyml_storage::storage::EnergymlStorage;
use energyml_storage::write::{EnergymlWriter, Put, Transaction, TransactionRequest, check_open};
use retp_client::arrays::{Array, ArrayFailure, Grouping, write_arrays_with};
use retp_client::notifications::uuid_text;
use retp_client::rest::uuid_of;
use retp_client::{
    ClientError, Clock, Ended, Opened, Transport, commit_transaction, dataspace_uri, delete_data_objects,
    delete_dataspaces, put_data_objects, put_dataspaces, rollback_transaction, start_transaction,
};
use retp_proto::uris::{UriUse, check_uri};
use retp_types::datatypes::data_array_types::DataArrayIdentifier;
use retp_types::datatypes::object::{DataObject, Dataspace as EtpDataspace, PutResponse, Resource};

use super::{DEFAULT_MESSAGE, EtpStore, KIND, qualified_type_of, refused, remote};
use crate::push::to_wire;

/// Positions of `keys` grouped in rounds so that no round holds a key twice,
/// and a key's later occurrences go in later rounds, in order. Plural ETP
/// answers are keyed by the URI: one request cannot carry the same URI twice.
fn unique_rounds(keys: &[String]) -> Vec<Vec<usize>> {
    let mut seen: BTreeMap<&str, usize> = BTreeMap::new();
    let mut rounds: Vec<Vec<usize>> = Vec::new();
    for (position, key) in keys.iter().enumerate() {
        let count = seen.entry(key.as_str()).or_insert(0);
        if rounds.len() <= *count {
            rounds.push(Vec::new());
        }
        rounds[*count].push(position);
        *count += 1;
    }
    rounds
}

/// What the server said of one object, as the trait says it.
fn put_of(uri: &str, response: PutResponse) -> Put {
    let mut put = Put::new(uri);
    put.created_contained = response.created_contained_object_uris;
    put.deleted_contained = response.deleted_contained_object_uris;
    put.joined_contained = response.joined_contained_object_uris;
    put.unjoined_contained = response.unjoined_contained_object_uris;
    put
}

impl<T: Transport, C: Clock> EtpStore<T, C> {
    /// Forget what the dataspace was last listed as holding.
    fn forget_listing(&self) {
        *self.listing.lock().unwrap_or_else(|p| p.into_inner()) = None;
    }

    /// Refuse a write inside a transaction that only reads.
    fn writable(&self) -> Result<()> {
        match &self.writer_transaction {
            Some(open) if open.request.read_only => Err(Error::Transaction {
                store: KIND,
                reason: format!("transaction `{}` only reads", open.id),
            }),
            _ => Ok(()),
        }
    }

    /// Whether `uri` - a dataspace URI, or the path of one - names this store's
    /// dataspace.
    fn names_own_dataspace(&self, uri: &str) -> bool {
        let given = dataspace_uri(uri);
        if given == self.dataspace {
            return true;
        }
        match (EtpUri::parse(&given), EtpUri::parse(&self.dataspace)) {
            (Ok(given), Ok(own)) => {
                given.object_type.is_none() && given.uuid.is_none() && given.dataspace == own.dataspace
            }
            _ => false,
        }
    }

    /// A canonical URI of this store's dataspace, parsed. Puts and deletes take
    /// canonical URIs whatever was negotiated (ETP 1.2 §3.7.4.2), and a writer
    /// is one dataspace: a URI of another is refused, never written through here.
    fn in_own_dataspace(&self, uri: &str) -> Result<EtpUri> {
        let bad = |reason: String| Error::BadUri {
            uri: uri.to_string(),
            reason,
        };
        check_uri(uri, UriUse::CanonicalOnly, false).map_err(|why| bad(why.to_string()))?;
        let parsed = EtpUri::parse(uri).map_err(|why| bad(why.to_string()))?;
        let own = EtpUri::parse(&self.dataspace).map_err(|why| bad(why.to_string()))?;
        if parsed.dataspace != own.dataspace {
            return Err(bad(format!(
                "it names another dataspace than this store's `{}`",
                self.dataspace
            )));
        }
        Ok(parsed)
    }

    /// The object as the server will hold it: its URI in this dataspace, and the
    /// ETP data object (XML, as `to_part` writes it).
    ///
    /// The URI is the short form - type and uuid, no `objectVersion` -, like
    /// `push` addresses an object. TODO(verify): whether an object that declares
    /// an `objectVersion` must be addressed by it in ETP 1.2 (Energistics
    /// Identifier Specification v5.0 §3.4.2).
    fn data_object_of(&self, object: &dyn DynObject) -> Result<DataObject> {
        let reflect = object.as_reflect();
        let uuid = identity::uuid_of_reflect(reflect)
            .filter(|uuid| identifiers::is_uuid(uuid))
            .ok_or_else(|| Error::Package("this object carries no usable uuid, so it has no URI".into()))?
            .to_ascii_lowercase();
        let qualified = reflect.type_info().qualified_type.ok_or_else(|| {
            Error::Package("this object's type has no qualified ETP name, so it has no URI".into())
        })?;
        let text = to_part(object, XmlWriteOptions::default())?;
        Ok(DataObject {
            resource: Resource {
                uri: self.uri_for(qualified, &uuid),
                name: identity::title_of_reflect(reflect)
                    .unwrap_or_default()
                    .to_string(),
                ..Default::default()
            },
            format: "xml".into(),
            blob_id: None,
            data: text.into_bytes(),
        })
    }

    /// The object as bytes, kept as received: the URI is the caller's (it must be
    /// a canonical object URI of this dataspace), the format and the bytes are
    /// sent untouched.
    fn raw_data_object_of(&self, raw: RawObject) -> Result<DataObject> {
        let parsed = self.in_own_dataspace(&raw.uri)?;
        if qualified_type_of(&parsed).is_none() || parsed.uuid.is_none() {
            return Err(Error::BadUri {
                uri: raw.uri,
                reason: "it names no data object".into(),
            });
        }
        Ok(DataObject {
            resource: Resource {
                uri: raw.uri,
                ..Default::default()
            },
            format: raw.format.as_etp().into(),
            blob_id: None,
            data: raw.bytes,
        })
    }

    /// One `PutDataObjects` and its answer, one result per object in order.
    ///
    /// A batch the client refuses before it leaves (a format the session did not
    /// agree, an object above `MaxDataObjectSize`) is `ClientError::Invalid`:
    /// nothing was sent, so the objects go one by one and only the offender
    /// fails.
    fn put_batch(&self, objects: &[DataObject], prune_contained: bool) -> Vec<Result<Put>> {
        let answered = put_data_objects(&mut *self.client(), objects, prune_contained);
        self.forget_listing();
        match answered {
            Ok(mut answers) => objects
                .iter()
                .map(|object| {
                    let uri = &object.resource.uri;
                    match answers.remove(uri) {
                        Some(Ok(put)) => Ok(put_of(uri, put)),
                        Some(Err(error)) => Err(refused(uri, &error)),
                        None => Err(remote(format!(
                            "{uri}: the server answered nothing for this object"
                        ))),
                    }
                })
                .collect(),
            Err(ClientError::Invalid(_)) if objects.len() > 1 => objects
                .iter()
                .flat_map(|object| self.put_batch(std::slice::from_ref(object), prune_contained))
                .collect(),
            Err(why) => objects.iter().map(|_| Err(remote(&why))).collect(),
        }
    }

    /// Send the objects that could be made ready, filling `results` at their
    /// positions.
    fn send_objects(
        &self,
        ready: Vec<(usize, DataObject)>,
        prune_contained: bool,
        results: &mut [Option<Result<Put>>],
    ) {
        let keys: Vec<String> = ready
            .iter()
            .map(|(_, object)| object.resource.uri.clone())
            .collect();
        let mut ready: Vec<Option<(usize, DataObject)>> = ready.into_iter().map(Some).collect();
        for round in unique_rounds(&keys) {
            let (slots, objects): (Vec<usize>, Vec<DataObject>) =
                round.iter().filter_map(|&r| ready[r].take()).unzip();
            for (slot, outcome) in slots.into_iter().zip(self.put_batch(&objects, prune_contained)) {
                results[slot] = Some(outcome);
            }
        }
    }

    /// The array as DataArray addresses it: the URI of the object whose resource
    /// holds it, and the path in that resource - an ETP-addressed location as it
    /// stands, or one `arrays_of` filled in (the object's URI is its `file`).
    /// The object must be in this dataspace.
    fn array_address(&self, location: &ArrayLocation) -> Result<DataArrayIdentifier> {
        let id = match location.etp_address() {
            Some((uri, path)) => DataArrayIdentifier {
                uri: uri.to_string(),
                path_in_resource: path.to_string(),
            },
            None => {
                if let Some(error) = Self::unresolved(location) {
                    return Err(error);
                }
                Self::identifier(location)
            }
        };
        self.in_own_dataspace(&id.uri)?;
        Ok(id)
    }

    /// The open transaction as the client names it, to end it.
    fn wire_transaction(&self, open: &Transaction) -> Result<retp_client::Transaction> {
        let uuid = uuid_of(&open.id).ok_or_else(|| Error::Transaction {
            store: KIND,
            reason: format!("`{}` is not a transaction uuid", open.id),
        })?;
        Ok(retp_client::Transaction {
            uuid,
            dataspaces: vec![self.dataspace.clone()],
            read_only: open.request.read_only,
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<T: Transport + Send, C: Clock + Send> EnergymlWriter for EtpStore<T, C> {
    async fn put_objects(
        &mut self,
        objects: Vec<Box<dyn DynObject>>,
        prune_contained: bool,
    ) -> Vec<Result<Put>> {
        let mut results: Vec<Option<Result<Put>>> = objects.iter().map(|_| None).collect();
        let mut ready = Vec::new();
        for (position, object) in objects.iter().enumerate() {
            match self
                .writable()
                .and_then(|()| self.data_object_of(object.as_ref()))
            {
                Ok(data_object) => ready.push((position, data_object)),
                Err(why) => results[position] = Some(Err(why)),
            }
        }
        if !ready.is_empty() {
            self.send_objects(ready, prune_contained, &mut results);
        }
        results
            .into_iter()
            .map(|one| one.unwrap_or_else(|| Err(remote("no answer was recorded for this object"))))
            .collect()
    }

    async fn put_raw(&mut self, objects: Vec<RawObject>, prune_contained: bool) -> Vec<Result<Put>> {
        let mut results: Vec<Option<Result<Put>>> = objects.iter().map(|_| None).collect();
        let mut ready = Vec::new();
        for (position, raw) in objects.into_iter().enumerate() {
            match self.writable().and_then(|()| self.raw_data_object_of(raw)) {
                Ok(data_object) => ready.push((position, data_object)),
                Err(why) => results[position] = Some(Err(why)),
            }
        }
        if !ready.is_empty() {
            self.send_objects(ready, prune_contained, &mut results);
        }
        results
            .into_iter()
            .map(|one| one.unwrap_or_else(|| Err(remote("no answer was recorded for this object"))))
            .collect()
    }

    async fn delete_objects(&mut self, uris: &[String], prune_contained: bool) -> Vec<Result<Vec<String>>> {
        let mut results: Vec<Option<Result<Vec<String>>>> = uris.iter().map(|_| None).collect();
        let mut ready: Vec<Option<(usize, String)>> = Vec::new();
        for (position, uri) in uris.iter().enumerate() {
            let checked = self.writable().and_then(|()| {
                let parsed = self.in_own_dataspace(uri)?;
                if parsed.uuid.is_none() || qualified_type_of(&parsed).is_none() {
                    return Err(Error::BadUri {
                        uri: uri.clone(),
                        reason: "it names no data object".into(),
                    });
                }
                Ok(())
            });
            match checked {
                Ok(()) => ready.push(Some((position, uri.clone()))),
                Err(why) => results[position] = Some(Err(why)),
            }
        }
        let keys: Vec<String> = ready.iter().flatten().map(|(_, uri)| uri.clone()).collect();
        for round in unique_rounds(&keys) {
            let (slots, asked): (Vec<usize>, Vec<String>) =
                round.iter().filter_map(|&r| ready[r].take()).unzip();
            let answered = delete_data_objects(&mut *self.client(), &asked, prune_contained);
            self.forget_listing();
            match answered {
                Ok(mut answers) => {
                    for (slot, uri) in slots.into_iter().zip(&asked) {
                        results[slot] = Some(match answers.remove(uri) {
                            // The URIs the server **actually** deleted, which may
                            // be more than the one asked (its contained objects).
                            Some(Ok(deleted)) => Ok(deleted),
                            Some(Err(error)) => Err(refused(uri, &error)),
                            None => Err(remote(format!("{uri}: the server answered nothing for this URI"))),
                        });
                    }
                }
                Err(why) => {
                    for slot in slots {
                        results[slot] = Some(Err(remote(&why)));
                    }
                }
            }
        }
        results
            .into_iter()
            .map(|one| one.unwrap_or_else(|| Err(remote("no answer was recorded for this URI"))))
            .collect()
    }

    async fn begin(&mut self, request: TransactionRequest) -> Result<Transaction> {
        if let Some(open) = &self.writer_transaction {
            return Err(Error::Transaction {
                store: KIND,
                reason: format!("transaction `{}` is already open", open.id),
            });
        }
        // The same as the EPC: a transaction of a writer covers the writer's
        // dataspace. Another one is not this store's to open a transaction on.
        if let Some(other) = request
            .dataspaces
            .iter()
            .find(|uri| !self.names_own_dataspace(uri))
        {
            return Err(Error::NotFound(other.clone()));
        }
        // ETP 1.2 §18.1.1.1 step 1.a: the customer includes the list of
        // dataspaces the transaction covers; an empty list would extend it to
        // all of them. The store's own is named. TODO(verify): the field text
        // says an empty STRING means the default dataspace; the canonical
        // `eml:///` is sent for it, which the client's own URI check requires.
        let opened = start_transaction(
            &mut *self.client(),
            std::slice::from_ref(&self.dataspace),
            request.read_only,
            &request.message,
        );
        match opened.map_err(remote)? {
            Opened::Open(opened) => {
                let transaction = Transaction::new(uuid_text(&opened.uuid), request);
                self.writer_transaction = Some(transaction.clone());
                Ok(transaction)
            }
            Opened::Refused(why) => Err(Error::Transaction {
                store: KIND,
                reason: format!("the server would not open the transaction: {why}"),
            }),
            // "Never here", apart from "not now": ETP 1.2 §18.
            Opened::Unsupported(_) => Err(Error::Unsupported {
                operation: "transactions (the server has none)",
                store: KIND,
            }),
        }
    }

    async fn commit(&mut self, transaction: &Transaction) -> Result<()> {
        check_open(self.writer_transaction.as_ref(), transaction, KIND)?;
        let open = self.writer_transaction.clone().expect("checked open above");
        let wire = self.wire_transaction(&open)?;
        let ended = commit_transaction(&mut *self.client(), &wire).map_err(remote);
        self.forget_listing();
        match ended? {
            Ended::Done => {
                self.writer_transaction = None;
                Ok(())
            }
            // ETP 1.2 §18.1.1.1 step 8.b.ii: a commit the store could not apply
            // leaves it in the state where every request of the transaction
            // was ignored - the transaction is over, and nothing of it is there.
            Ended::Refused(why) => {
                self.writer_transaction = None;
                Err(Error::Transaction {
                    store: KIND,
                    reason: format!("the commit was refused, nothing of the transaction is stored: {why}"),
                })
            }
        }
    }

    async fn rollback(&mut self, transaction: &Transaction) -> Result<()> {
        check_open(self.writer_transaction.as_ref(), transaction, KIND)?;
        let open = self.writer_transaction.clone().expect("checked open above");
        let wire = self.wire_transaction(&open)?;
        let ended = rollback_transaction(&mut *self.client(), &wire).map_err(remote);
        self.forget_listing();
        match ended? {
            Ended::Done => {
                self.writer_transaction = None;
                Ok(())
            }
            // TODO(verify): ETP 1.2 §18.1.1.1 step 6.b says the response "may
            // provide an optional failureReason" and nothing of the state after a
            // rollback that failed. The transaction is kept open here - the
            // writes may still be there - rather than declared over.
            Ended::Refused(why) => Err(Error::Transaction {
                store: KIND,
                reason: format!("the rollback was refused, the transaction is still open: {why}"),
            }),
        }
    }

    fn open_transaction(&self) -> Option<Transaction> {
        self.writer_transaction.clone()
    }

    async fn put_arrays(&mut self, arrays: &[(ArrayLocation, ArrayData)]) -> Vec<Result<()>> {
        let mut results: Vec<Option<Result<()>>> = arrays.iter().map(|_| None).collect();
        let mut slots = Vec::new();
        let mut writes: Vec<(DataArrayIdentifier, Array)> = Vec::new();
        for (position, (location, data)) in arrays.iter().enumerate() {
            let prepared = self.writable().and_then(|()| {
                // A block the caller chose is `PutDataSubarrays`, which the
                // client does not offer on its own (see the module doc).
                if !location.start.is_empty() || !location.count.is_empty() {
                    return Err(Error::Unsupported {
                        operation: "writing part of an array (PutDataSubarrays)",
                        store: KIND,
                    });
                }
                let id = self.array_address(location)?;
                // The shape and the values agree before anything leaves: a shape
                // that promises more numbers than there are is a plausible array
                // that nothing flags.
                let promised = data
                    .metadata
                    .dimensions
                    .iter()
                    .fold(1i64, |acc, d| acc.saturating_mul((*d).max(0)));
                if usize::try_from(promised).ok() != Some(data.values.len()) {
                    return Err(Error::ExternalData {
                        location: format!("{} {}", id.uri, id.path_in_resource),
                        reason: ExternalDataReason::ShapeDisagrees {
                            promised,
                            held: data.values.len(),
                        },
                    });
                }
                let array = to_wire(data).map_err(|why| Error::ExternalData {
                    location: format!("{} {}", id.uri, id.path_in_resource),
                    reason: ExternalDataReason::Backend(why),
                })?;
                Ok((id, array))
            });
            match prepared {
                Ok(write) => {
                    slots.push(position);
                    writes.push(write);
                }
                Err(why) => results[position] = Some(Err(why)),
            }
        }
        if !writes.is_empty() {
            // The client cuts an array that does not fit a message into blocks
            // itself, declaring it first (ETP 1.2 §13.1.2).
            let written = write_arrays_with(
                &mut *self.client(),
                &writes,
                DEFAULT_MESSAGE,
                Grouping::Together,
                |_| {},
            );
            self.forget_listing();
            match written {
                Ok(outcomes) => {
                    let mut outcomes = outcomes.into_iter();
                    for (slot, (id, _)) in slots.iter().zip(&writes) {
                        results[*slot] = Some(match outcomes.next() {
                            Some(Ok(())) => Ok(()),
                            Some(Err(failure)) => Err(array_error(id, &failure)),
                            None => Err(remote(format!(
                                "{} {}: the client reported nothing for this array",
                                id.uri, id.path_in_resource
                            ))),
                        });
                    }
                }
                Err(why) => {
                    for slot in &slots {
                        results[*slot] = Some(Err(remote(&why)));
                    }
                }
            }
        }
        results
            .into_iter()
            .map(|one| one.unwrap_or_else(|| Err(remote("no answer was recorded for this array"))))
            .collect()
    }
}

/// What the client said of one array, as the trait says it.
fn array_error(id: &DataArrayIdentifier, failure: &ArrayFailure) -> Error {
    Error::ExternalData {
        location: format!("{} {}", id.uri, id.path_in_resource),
        reason: ExternalDataReason::Backend(failure.to_string()),
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<T: Transport + Send, C: Clock + Send> EnergymlDataspaces for EtpStore<T, C> {
    /// ETP `GetDataspaces`: the one door the reading half already has.
    async fn list_dataspaces(&self) -> Result<Vec<Dataspace>> {
        <Self as EnergymlStorage>::dataspaces(self).await
    }

    async fn put_dataspaces(&mut self, dataspaces: Vec<Dataspace>) -> Vec<Result<()>> {
        let mut results: Vec<Option<Result<()>>> = dataspaces.iter().map(|_| None).collect();
        let mut ready: Vec<Option<(usize, EtpDataspace)>> = Vec::new();
        for (position, dataspace) in dataspaces.iter().enumerate() {
            match dataspace_record(&dataspace.uri) {
                // The store sets the timestamps (zero here is "not given").
                // TODO(verify): that zero is read as "not given" by servers.
                Ok(path) => ready.push(Some((
                    position,
                    EtpDataspace {
                        uri: dataspace.uri.clone(),
                        path,
                        store_last_write: 0,
                        store_created: 0,
                        custom_data: BTreeMap::new(),
                    },
                ))),
                Err(why) => results[position] = Some(Err(why)),
            }
        }
        let keys: Vec<String> = ready.iter().flatten().map(|(_, d)| d.uri.clone()).collect();
        for round in unique_rounds(&keys) {
            let (slots, sent): (Vec<usize>, Vec<EtpDataspace>) =
                round.iter().filter_map(|&r| ready[r].take()).unzip();
            let answered = put_dataspaces(&mut *self.client(), &sent);
            self.forget_listing();
            fill_dataspace_results(&mut results, slots, sent.iter().map(|d| d.uri.as_str()), answered);
        }
        finish(results)
    }

    async fn delete_dataspaces(&mut self, uris: &[String]) -> Vec<Result<()>> {
        let mut results: Vec<Option<Result<()>>> = uris.iter().map(|_| None).collect();
        let mut ready: Vec<Option<(usize, String)>> = Vec::new();
        for (position, uri) in uris.iter().enumerate() {
            match dataspace_record(uri) {
                Ok(_) => ready.push(Some((position, uri.clone()))),
                Err(why) => results[position] = Some(Err(why)),
            }
        }
        let keys: Vec<String> = ready.iter().flatten().map(|(_, uri)| uri.clone()).collect();
        for round in unique_rounds(&keys) {
            let (slots, asked): (Vec<usize>, Vec<String>) =
                round.iter().filter_map(|&r| ready[r].take()).unzip();
            let answered = delete_dataspaces(&mut *self.client(), &asked);
            self.forget_listing();
            fill_dataspace_results(&mut results, slots, asked.iter().map(String::as_str), answered);
        }
        finish(results)
    }

    /// This store's own dataspace is `self`; any other is
    /// [`Error::NotFound`]: an `EtpStore` is bound to one dataspace, and reaching
    /// another means building another `EtpStore` on the same session.
    fn open_dataspace(&mut self, uri: &str) -> Result<&mut dyn EnergymlWriter> {
        let parsed = EtpUri::parse(uri).map_err(|why| Error::BadUri {
            uri: uri.to_string(),
            reason: why.to_string(),
        })?;
        if parsed.uuid.is_some() || parsed.object_type.is_some() || !self.names_own_dataspace(uri) {
            return Err(Error::NotFound(uri.to_string()));
        }
        Ok(self)
    }
}

/// The ETP `path` of the dataspace a canonical dataspace URI names - the
/// default dataspace's is empty - or the refusal of a URI that is not one
/// (ETP 1.2, Dataspace protocol, `PutDataspaces`: *« The URIs MUST be
/// canonical Energistics dataspace URIs »*).
fn dataspace_record(uri: &str) -> Result<String> {
    let bad = |reason: String| Error::BadUri {
        uri: uri.to_string(),
        reason,
    };
    check_uri(uri, UriUse::CanonicalOnly, false).map_err(|why| bad(why.to_string()))?;
    let parsed = EtpUri::parse(uri).map_err(|why| bad(why.to_string()))?;
    if parsed.uuid.is_some() || parsed.object_type.is_some() {
        return Err(bad("it names a data object, not a dataspace".into()));
    }
    Ok(parsed.dataspace.unwrap_or_default())
}

/// One answer per dataspace asked: the server's refusal of one is that one's
/// own error.
fn fill_dataspace_results<'a>(
    results: &mut [Option<Result<()>>],
    slots: Vec<usize>,
    asked: impl Iterator<Item = &'a str>,
    answered: std::result::Result<
        BTreeMap<String, std::result::Result<String, retp_types::datatypes::ErrorInfo>>,
        ClientError,
    >,
) {
    match answered {
        Ok(mut answers) => {
            for (slot, uri) in slots.into_iter().zip(asked) {
                results[slot] = Some(match answers.remove(uri) {
                    Some(Ok(_)) => Ok(()),
                    Some(Err(error)) => Err(refused(uri, &error)),
                    None => Err(remote(format!(
                        "{uri}: the server answered nothing for this dataspace"
                    ))),
                });
            }
        }
        Err(why) => {
            for slot in slots {
                results[slot] = Some(Err(remote(&why)));
            }
        }
    }
}

/// Every position answered: one that was not is an error, never a success.
fn finish(results: Vec<Option<Result<()>>>) -> Vec<Result<()>> {
    results
        .into_iter()
        .map(|one| one.unwrap_or_else(|| Err(remote("no answer was recorded for this dataspace"))))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use energyml_storage::array::ArrayValues;

    fn keys(list: &[&str]) -> Vec<String> {
        list.iter().map(|k| k.to_string()).collect()
    }

    /// A key sent twice goes in two rounds, in order, and the others are not
    /// delayed.
    #[test]
    fn a_repeated_key_goes_in_a_later_round() {
        assert_eq!(
            unique_rounds(&keys(&["a", "b", "a", "a", "c"])),
            vec![vec![0, 1, 4], vec![2], vec![3]]
        );
        assert_eq!(unique_rounds(&keys(&[])), Vec::<Vec<usize>>::new());
    }

    /// A dataspace URI gives its path; an object URI is refused by name.
    #[test]
    fn a_dataspace_record_has_the_path_of_its_uri() {
        assert_eq!(dataspace_record("eml:///dataspace('a/b')").unwrap(), "a/b");
        assert!(matches!(
            dataspace_record(
                "eml:///dataspace('a')/resqml20.obj_LocalDepth3dCrs(aa000000-0000-4000-8000-000000000001)"
            ),
            Err(Error::BadUri { .. })
        ));
        assert!(matches!(dataspace_record("demo"), Err(Error::BadUri { .. })));
    }

    /// The conversion refuses what it cannot carry, by name, and keeps every
    /// integer width under a transport its logical type allows.
    #[test]
    fn every_element_type_travels_as_its_logical_type_allows() {
        use energyml_storage::array::ArrayMetadata;
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
                metadata: ArrayMetadata::new(vec![1], element_type.to_string()),
                values: values.clone(),
            };
            let wire = to_wire(&data).unwrap();
            assert!(
                allowed_transports(wire.logical).contains(&wire.transport),
                "{element_type}: {:?} as {:?}",
                wire.logical,
                wire.transport
            );
            assert_eq!(crate::values_of(&wire).unwrap(), values, "{element_type}");
        }
    }
}
