//! StoreNotification (5): what a store says **on its own** about the objects
//! a client watches - and, from a watermark, the catch-up a reconnection
//! needs.
//!
//! A subscription names a context, as Discovery names one (§23.34.15), and a
//! uuid the store repeats in every notification of it. The stream is read
//! apart from everything else that arrives unasked (`Caller::subscribe`,
//! `Caller::events`): [`notifications`] gives what came since the last
//! call, typed. Nothing here waits for ever: a shell polls between two
//! commands, a front end's worker awaits `next_events`.
//!
//! **The watermark.** A notification missed while the session was down is
//! gone; what ETP offers instead is Discovery with `storeLastWriteFilter`
//! and `GetDeletedResources` (§8.3.1, §8.3.4; the filter is *greater than*,
//! §3.12.8), both read against the
//! store's clock. [`changes_since`] asks both from a time - the last
//! `currentDateTime` heard from the store (`Client::watermark`) is the one
//! to give it - so that a client back from a cut asks for what it missed
//! rather than for everything.
use std::collections::BTreeMap;

use retp_proto::connection::Event;
use retp_proto::wire::{Body, Delivery};
use retp_types::datatypes::object::{
    ActiveStatusKind, ContextInfo, ContextScopeKind, DeletedResource, ObjectChange, Resource,
    SubscriptionInfo,
};
use retp_types::datatypes::{ErrorInfo, Uuid};
use retp_types::dispatch::Message;
use retp_types::protocol::discovery::{GetDeletedResources, GetResources};
use retp_types::protocol::store_notification::{SubscribeNotifications, UnsubscribeNotifications};

use crate::ClientError;
use crate::caller::{Blocking, run};

/// The protocol.
pub const STORE_NOTIFICATION: i32 = 5;

/// What to watch: a context and the uuid the subscription goes by.
#[derive(Debug, Clone, PartialEq)]
pub struct Watch {
    /// The context's URI - a dataspace, or an object.
    pub uri: String,
    /// How far from it, in the graph; 1 by default.
    pub depth: i32,
    /// Which side of the graph; targets or self by default, which watches
    /// the object and what it references.
    pub scope: ContextScopeKind,
    /// Only these types; every type when empty.
    pub data_object_types: Vec<String>,
    /// Whether a change carries the object's data, not only its resource.
    pub include_object_data: bool,
    /// The format the data comes in, when it does.
    pub format: String,
    /// The uuid the store repeats in every notification of this watch, and
    /// the one to end it by. The client's to choose (`native::random_uuid`).
    pub request_uuid: Uuid,
}

impl Watch {
    /// A watch of `uri` and what it references, one level down, without
    /// the objects' data.
    pub fn of(uri: impl Into<String>, request_uuid: Uuid) -> Self {
        Watch {
            uri: uri.into(),
            depth: 1,
            scope: ContextScopeKind::TargetsOrSelf,
            data_object_types: Vec::new(),
            include_object_data: false,
            format: "xml".into(),
            request_uuid,
        }
    }

    /// As the wire says it.
    pub fn info(&self) -> SubscriptionInfo {
        SubscriptionInfo {
            context: ContextInfo {
                uri: self.uri.clone(),
                depth: self.depth,
                data_object_types: self.data_object_types.clone(),
                ..Default::default()
            },
            scope: self.scope,
            request_uuid: self.request_uuid,
            include_object_data: self.include_object_data,
            format: self.format.clone(),
        }
    }
}

/// One thing the store said on its own, typed.
#[derive(Debug, Clone, PartialEq)]
pub enum Notification {
    /// An object was inserted or updated (§10.3.2).
    Changed {
        /// What changed, and when by the store's clock.
        change: ObjectChange,
        /// The watch it came for.
        subscription: Uuid,
    },
    /// An object was deleted (§10.3.3).
    Deleted {
        /// Its URI.
        uri: String,
        /// When, by the store's clock.
        change_time: i64,
        /// The watch it came for.
        subscription: Uuid,
    },
    /// The client may no longer read an object (§10.3.4).
    AccessRevoked {
        /// Its URI.
        uri: String,
        /// When.
        change_time: i64,
        /// The watch it came for.
        subscription: Uuid,
    },
    /// An object went active or inactive (§10.3.8).
    ActiveStatusChanged {
        /// The object.
        resource: Resource,
        /// Its status now.
        status: ActiveStatusKind,
        /// When.
        change_time: i64,
        /// The watch it came for.
        subscription: Uuid,
    },
    /// A subscription is over - the store ended it on its own (§10.3.5),
    /// or answered an unsubscribe.
    Ended {
        /// The store's reason.
        reason: String,
        /// The watch.
        subscription: Uuid,
    },
    /// The store lists the subscriptions it kept for a session that came
    /// back (§10.3.9).
    Unsolicited(Vec<SubscriptionInfo>),
    /// Something else of the protocol, or an exception: as delivered.
    Other(Delivery),
    /// Not a message of the protocol: the session's own event on this
    /// stream - a fault, a timeout -, as it came.
    Session(Event),
}

impl Notification {
    /// The watch it came for, when it names one.
    pub fn subscription(&self) -> Option<Uuid> {
        match self {
            Notification::Changed { subscription, .. }
            | Notification::Deleted { subscription, .. }
            | Notification::AccessRevoked { subscription, .. }
            | Notification::ActiveStatusChanged { subscription, .. }
            | Notification::Ended { subscription, .. } => Some(*subscription),
            _ => None,
        }
    }

    /// What one event of the stream is.
    pub fn of(event: Event) -> Notification {
        let Event::Delivered(delivery) = event else {
            return Notification::Session(event);
        };
        let Body::Message(message) = &delivery.body else {
            return Notification::Other(delivery);
        };
        match message {
            Message::ObjectChanged(m) => Notification::Changed {
                change: m.change.clone(),
                subscription: m.request_uuid,
            },
            Message::ObjectDeleted(m) => Notification::Deleted {
                uri: m.uri.clone(),
                change_time: m.change_time,
                subscription: m.request_uuid,
            },
            Message::ObjectAccessRevoked(m) => Notification::AccessRevoked {
                uri: m.uri.clone(),
                change_time: m.change_time,
                subscription: m.request_uuid,
            },
            Message::ObjectActiveStatusChanged(m) => Notification::ActiveStatusChanged {
                resource: m.resource.clone(),
                status: m.active_status,
                change_time: m.change_time,
                subscription: m.request_uuid,
            },
            Message::SubscriptionEnded(m) => Notification::Ended {
                reason: m.reason.clone(),
                subscription: m.request_uuid,
            },
            Message::UnsolicitedStoreNotifications(m) => Notification::Unsolicited(m.subscriptions.clone()),
            _ => Notification::Other(delivery),
        }
    }
}

/// What changed in a dataspace since a time - the catch-up.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Changes {
    /// Written - inserted or updated - since then, by the store's clock.
    pub written: Vec<Resource>,
    /// Deleted since then.
    pub deleted: Vec<DeletedResource>,
}

/// The text of a uuid, `8-4-4-4-12` hex lowercase - the one the store
/// prints, and [`crate::rest::uuid_of`] reads back.
pub fn uuid_text(uuid: &Uuid) -> String {
    let hex: Vec<String> = uuid.0.iter().map(|b| format!("{b:02x}")).collect();
    let hex = hex.concat();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

pub mod asynchronous {
    use super::*;
    use crate::caller::Caller;
    use crate::operations::asynchronous::keyed_outcomes;

    /// Subscribe to `watches`: one answer per watch, in the order given.
    /// From here on the protocol's stream is kept apart: read it with
    /// [`notifications`].
    pub async fn subscribe_notifications<K: Caller>(
        client: &mut K,
        watches: &[Watch],
    ) -> Result<Vec<Result<(), ErrorInfo>>, ClientError> {
        client.subscribe(STORE_NOTIFICATION);
        let request = SubscribeNotifications {
            request: watches
                .iter()
                .enumerate()
                .map(|(i, w)| ((i + 1).to_string(), w.info()))
                .collect(),
        };
        let deliveries = client.call(&request).await?;
        Ok(keyed_outcomes(deliveries, watches.len(), |m| {
            matches!(m, Message::SubscribeNotificationsResponse(_))
        }))
    }

    /// End a watch. The store answers `SubscriptionEnded` with its reason
    /// (§10.3.5) - or an exception, given back as the store's.
    pub async fn unsubscribe_notifications<K: Caller>(
        client: &mut K,
        subscription: Uuid,
    ) -> Result<Result<String, ErrorInfo>, ClientError> {
        let request = UnsubscribeNotifications {
            request_uuid: subscription,
        };
        for delivery in client.call(&request).await? {
            if let Body::Message(Message::SubscriptionEnded(ended)) = &delivery.body {
                return Ok(Ok(ended.reason.clone()));
            }
            if let Some(exception) = delivery.exception() {
                let info = match exception {
                    retp_proto::wire::Exception::Single(info) => info,
                    retp_proto::wire::Exception::Map(map) => match map.into_values().next() {
                        Some(info) => info,
                        None => continue,
                    },
                };
                return Ok(Err(info));
            }
        }
        Ok(Err(crate::operations::asynchronous::nothing_said()))
    }

    /// What the store said on its own since the last call - after giving
    /// the transport a moment. Read again and again; empty is *nothing
    /// yet*.
    pub async fn notifications<K: Caller>(client: &mut K) -> Result<Vec<Notification>, ClientError> {
        Ok(client
            .events(STORE_NOTIFICATION)
            .await?
            .into_iter()
            .map(Notification::of)
            .collect())
    }

    /// What changed in `dataspace_uri` since `since` (microseconds, the
    /// store's clock): the resources written since, and the ones deleted
    /// since - Discovery (3) with `storeLastWriteFilter`, and
    /// `GetDeletedResources`. `since` is the watermark of the session that
    /// was cut. Empty `data_object_types` is every type.
    pub async fn changes_since<K: Caller>(
        client: &mut K,
        dataspace_uri: &str,
        since: i64,
        data_object_types: &[String],
    ) -> Result<Changes, ClientError> {
        Ok(Changes {
            written: written_since(client, dataspace_uri, since, data_object_types).await?,
            deleted: deleted_since(client, dataspace_uri, since, data_object_types).await?,
        })
    }

    /// The half of [`changes_since`] every transport has: the resources of
    /// `dataspace_uri` written **after** `since` (the filter is strict,
    /// §3.12.8).
    pub async fn written_since<K: Caller>(
        client: &mut K,
        dataspace_uri: &str,
        since: i64,
        data_object_types: &[String],
    ) -> Result<Vec<Resource>, ClientError> {
        let listing = GetResources {
            context: ContextInfo {
                uri: dataspace_uri.to_string(),
                depth: 1,
                data_object_types: data_object_types.to_vec(),
                ..Default::default()
            },
            scope: ContextScopeKind::Targets,
            count_objects: false,
            store_last_write_filter: Some(since),
            active_status_filter: None,
            include_edges: false,
        };
        let mut written = Vec::new();
        for delivery in client.call(&listing).await? {
            if let Body::Message(Message::GetResourcesResponse(r)) = &delivery.body {
                written.extend(r.resources.iter().cloned());
            }
        }
        Ok(written)
    }

    /// The other half: what was deleted from `dataspace_uri` after `since`
    /// (§8.3.4). The Reservoir DDMS REST API has no route for it, and the
    /// REST client refuses it by name.
    pub async fn deleted_since<K: Caller>(
        client: &mut K,
        dataspace_uri: &str,
        since: i64,
        data_object_types: &[String],
    ) -> Result<Vec<DeletedResource>, ClientError> {
        let request = GetDeletedResources {
            dataspace_uri: dataspace_uri.to_string(),
            delete_time_filter: Some(since),
            data_object_types: data_object_types.to_vec(),
        };
        let mut deleted = Vec::new();
        for delivery in client.call(&request).await? {
            if let Body::Message(Message::GetDeletedResourcesResponse(r)) = &delivery.body {
                deleted.extend(r.deleted_resources.iter().cloned());
            }
        }
        Ok(deleted)
    }

    #[allow(dead_code)]
    fn _keyed(_: &BTreeMap<String, String>) {}
}

/// [`asynchronous::subscribe_notifications`], on the blocking client.
pub fn subscribe_notifications<K: Blocking>(
    client: &mut K,
    watches: &[Watch],
) -> Result<Vec<Result<(), ErrorInfo>>, ClientError> {
    run(asynchronous::subscribe_notifications(client, watches))
}

/// [`asynchronous::unsubscribe_notifications`], on the blocking client.
pub fn unsubscribe_notifications<K: Blocking>(
    client: &mut K,
    subscription: Uuid,
) -> Result<Result<String, ErrorInfo>, ClientError> {
    run(asynchronous::unsubscribe_notifications(client, subscription))
}

/// [`asynchronous::notifications`], on the blocking client.
pub fn notifications<K: Blocking>(client: &mut K) -> Result<Vec<Notification>, ClientError> {
    run(asynchronous::notifications(client))
}

/// [`asynchronous::written_since`], on the blocking client.
pub fn written_since<K: Blocking>(
    client: &mut K,
    dataspace_uri: &str,
    since: i64,
    data_object_types: &[String],
) -> Result<Vec<Resource>, ClientError> {
    run(asynchronous::written_since(
        client,
        dataspace_uri,
        since,
        data_object_types,
    ))
}

/// [`asynchronous::deleted_since`], on the blocking client.
pub fn deleted_since<K: Blocking>(
    client: &mut K,
    dataspace_uri: &str,
    since: i64,
    data_object_types: &[String],
) -> Result<Vec<DeletedResource>, ClientError> {
    run(asynchronous::deleted_since(
        client,
        dataspace_uri,
        since,
        data_object_types,
    ))
}

/// [`asynchronous::changes_since`], on the blocking client.
pub fn changes_since<K: Blocking>(
    client: &mut K,
    dataspace_uri: &str,
    since: i64,
    data_object_types: &[String],
) -> Result<Changes, ClientError> {
    run(asynchronous::changes_since(
        client,
        dataspace_uri,
        since,
        data_object_types,
    ))
}
