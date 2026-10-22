// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 84bd808714c5d32f2b7e91d897b5677012f16bafd221694662c8058e98c13fdc
//! `protocol::store_notification` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Protocol.StoreNotification.Chunk`.
///
/// ETP protocol 5, message 9, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.StoreNotification.Chunk")]
#[etp(protocol = 5, message_type = 9, sender = "store", roles = "store,customer", multipart)]
pub struct Chunk {
    /// `blobId`.
    pub blob_id: crate::datatypes::Uuid,
    /// `data`.
    pub data: Vec<u8>,
    /// `final`.
    pub r#final: bool,
}

/// `Energistics.Etp.v12.Protocol.StoreNotification.ObjectAccessRevoked`.
///
/// ETP protocol 5, message 5, sent by the store.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.StoreNotification.ObjectAccessRevoked")]
#[etp(protocol = 5, message_type = 5, sender = "store", roles = "store,customer")]
pub struct ObjectAccessRevoked {
    /// `uri`.
    pub uri: String,
    /// `changeTime`.
    pub change_time: i64,
    /// `requestUuid`.
    pub request_uuid: crate::datatypes::Uuid,
}

/// `Energistics.Etp.v12.Protocol.StoreNotification.ObjectActiveStatusChanged`.
///
/// ETP protocol 5, message 11, sent by the store.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.StoreNotification.ObjectActiveStatusChanged")]
#[etp(protocol = 5, message_type = 11, sender = "store", roles = "store,customer")]
pub struct ObjectActiveStatusChanged {
    /// `activeStatus`.
    pub active_status: crate::datatypes::object::ActiveStatusKind,
    /// `changeTime`.
    pub change_time: i64,
    /// `resource`.
    pub resource: crate::datatypes::object::Resource,
    /// `requestUuid`.
    pub request_uuid: crate::datatypes::Uuid,
}

/// `Energistics.Etp.v12.Protocol.StoreNotification.ObjectChanged`.
///
/// ETP protocol 5, message 2, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.StoreNotification.ObjectChanged")]
#[etp(protocol = 5, message_type = 2, sender = "store", roles = "store,customer", multipart)]
pub struct ObjectChanged {
    /// `change`.
    pub change: crate::datatypes::object::ObjectChange,
    /// `requestUuid`.
    pub request_uuid: crate::datatypes::Uuid,
}

/// `Energistics.Etp.v12.Protocol.StoreNotification.ObjectDeleted`.
///
/// ETP protocol 5, message 3, sent by the store.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.StoreNotification.ObjectDeleted")]
#[etp(protocol = 5, message_type = 3, sender = "store", roles = "store,customer")]
pub struct ObjectDeleted {
    /// `uri`.
    pub uri: String,
    /// `changeTime`.
    pub change_time: i64,
    /// `requestUuid`.
    pub request_uuid: crate::datatypes::Uuid,
}

/// `Energistics.Etp.v12.Protocol.StoreNotification.SubscribeNotifications`.
///
/// ETP protocol 5, message 6, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.StoreNotification.SubscribeNotifications")]
#[etp(protocol = 5, message_type = 6, sender = "customer", roles = "store,customer")]
pub struct SubscribeNotifications {
    /// `request`.
    pub request: std::collections::BTreeMap<String, crate::datatypes::object::SubscriptionInfo>,
}

/// `Energistics.Etp.v12.Protocol.StoreNotification.SubscribeNotificationsResponse`.
///
/// ETP protocol 5, message 10, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.StoreNotification.SubscribeNotificationsResponse")]
#[etp(protocol = 5, message_type = 10, sender = "store", roles = "store,customer", multipart)]
pub struct SubscribeNotificationsResponse {
    /// `success`.
    pub success: std::collections::BTreeMap<String, String>,
}

/// `Energistics.Etp.v12.Protocol.StoreNotification.SubscriptionEnded`.
///
/// ETP protocol 5, message 7, sent by the store.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.StoreNotification.SubscriptionEnded")]
#[etp(protocol = 5, message_type = 7, sender = "store", roles = "store,customer")]
pub struct SubscriptionEnded {
    /// `reason`.
    pub reason: String,
    /// `requestUuid`.
    pub request_uuid: crate::datatypes::Uuid,
}

/// `Energistics.Etp.v12.Protocol.StoreNotification.UnsolicitedStoreNotifications`.
///
/// ETP protocol 5, message 8, sent by the store.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.StoreNotification.UnsolicitedStoreNotifications")]
#[etp(protocol = 5, message_type = 8, sender = "store", roles = "store,customer")]
pub struct UnsolicitedStoreNotifications {
    /// `subscriptions`.
    pub subscriptions: Vec<crate::datatypes::object::SubscriptionInfo>,
}

/// `Energistics.Etp.v12.Protocol.StoreNotification.UnsubscribeNotifications`.
///
/// ETP protocol 5, message 4, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.StoreNotification.UnsubscribeNotifications")]
#[etp(protocol = 5, message_type = 4, sender = "customer", roles = "store,customer")]
pub struct UnsubscribeNotifications {
    /// `requestUuid`.
    pub request_uuid: crate::datatypes::Uuid,
}

