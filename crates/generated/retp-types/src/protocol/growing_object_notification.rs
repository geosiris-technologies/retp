// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 340bb1bd2c3610d260d81c9371284871112e00b0ef22a7e36d04310dc770a1f0
//! `protocol::growing_object_notification` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Protocol.GrowingObjectNotification.PartSubscriptionEnded`.
///
/// ETP protocol 7, message 8, sent by the store.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObjectNotification.PartSubscriptionEnded")]
#[etp(protocol = 7, message_type = 8, sender = "store", roles = "store,customer")]
pub struct PartSubscriptionEnded {
    /// `reason`.
    pub reason: String,
    /// `requestUuid`.
    pub request_uuid: crate::datatypes::Uuid,
}

/// `Energistics.Etp.v12.Protocol.GrowingObjectNotification.PartsChanged`.
///
/// ETP protocol 7, message 2, sent by the store.
///
/// 1 of 6 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObjectNotification.PartsChanged")]
#[etp(protocol = 7, message_type = 2, sender = "store", roles = "store,customer")]
pub struct PartsChanged {
    /// `uri`.
    pub uri: String,
    /// `requestUuid`.
    pub request_uuid: crate::datatypes::Uuid,
    /// `changeKind`.
    pub change_kind: crate::datatypes::object::ObjectChangeKind,
    /// `changeTime`.
    pub change_time: i64,
    /// `format`.
    #[avro(default = "")]
    pub format: String,
    /// `parts`.
    pub parts: Vec<crate::datatypes::object::ObjectPart>,
}

/// `Energistics.Etp.v12.Protocol.GrowingObjectNotification.PartsDeleted`.
///
/// ETP protocol 7, message 3, sent by the store.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObjectNotification.PartsDeleted")]
#[etp(protocol = 7, message_type = 3, sender = "store", roles = "store,customer")]
pub struct PartsDeleted {
    /// `uri`.
    pub uri: String,
    /// `requestUuid`.
    pub request_uuid: crate::datatypes::Uuid,
    /// `changeTime`.
    pub change_time: i64,
    /// `uids`.
    pub uids: Vec<String>,
}

/// `Energistics.Etp.v12.Protocol.GrowingObjectNotification.PartsReplacedByRange`.
///
/// ETP protocol 7, message 6, sent by the store. May be split across several messages.
///
/// 1 of 7 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObjectNotification.PartsReplacedByRange")]
#[etp(protocol = 7, message_type = 6, sender = "store", roles = "store,customer", multipart)]
pub struct PartsReplacedByRange {
    /// `uri`.
    pub uri: String,
    /// `requestUuid`.
    pub request_uuid: crate::datatypes::Uuid,
    /// `changeTime`.
    pub change_time: i64,
    /// `deletedInterval`.
    pub deleted_interval: crate::datatypes::object::IndexInterval,
    /// `includeOverlappingIntervals`.
    pub include_overlapping_intervals: bool,
    /// `format`.
    #[avro(default = "")]
    pub format: String,
    /// `parts`.
    pub parts: Vec<crate::datatypes::object::ObjectPart>,
}

/// `Energistics.Etp.v12.Protocol.GrowingObjectNotification.SubscribePartNotifications`.
///
/// ETP protocol 7, message 7, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObjectNotification.SubscribePartNotifications")]
#[etp(protocol = 7, message_type = 7, sender = "customer", roles = "store,customer")]
pub struct SubscribePartNotifications {
    /// `request`.
    pub request: std::collections::BTreeMap<String, crate::datatypes::object::SubscriptionInfo>,
}

/// `Energistics.Etp.v12.Protocol.GrowingObjectNotification.SubscribePartNotificationsResponse`.
///
/// ETP protocol 7, message 10, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObjectNotification.SubscribePartNotificationsResponse")]
#[etp(protocol = 7, message_type = 10, sender = "store", roles = "store,customer", multipart)]
pub struct SubscribePartNotificationsResponse {
    /// `success`.
    pub success: std::collections::BTreeMap<String, String>,
}

/// `Energistics.Etp.v12.Protocol.GrowingObjectNotification.UnsolicitedPartNotifications`.
///
/// ETP protocol 7, message 9, sent by the store.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObjectNotification.UnsolicitedPartNotifications")]
#[etp(protocol = 7, message_type = 9, sender = "store", roles = "store,customer")]
pub struct UnsolicitedPartNotifications {
    /// `subscriptions`.
    pub subscriptions: Vec<crate::datatypes::object::SubscriptionInfo>,
}

/// `Energistics.Etp.v12.Protocol.GrowingObjectNotification.UnsubscribePartNotification`.
///
/// ETP protocol 7, message 4, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObjectNotification.UnsubscribePartNotification")]
#[etp(protocol = 7, message_type = 4, sender = "customer", roles = "store,customer")]
pub struct UnsubscribePartNotification {
    /// `requestUuid`.
    pub request_uuid: crate::datatypes::Uuid,
}

