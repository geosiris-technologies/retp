// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 340bb1bd2c3610d260d81c9371284871112e00b0ef22a7e36d04310dc770a1f0
//! `protocol::channel_subscribe` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Protocol.ChannelSubscribe.CancelGetRanges`.
///
/// ETP protocol 21, message 11, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelSubscribe.CancelGetRanges")]
#[etp(protocol = 21, message_type = 11, sender = "customer", roles = "store,customer")]
pub struct CancelGetRanges {
    /// `requestUuid`.
    pub request_uuid: crate::datatypes::Uuid,
}

/// `Energistics.Etp.v12.Protocol.ChannelSubscribe.ChannelData`.
///
/// ETP protocol 21, message 4, sent by the store.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelSubscribe.ChannelData")]
#[etp(protocol = 21, message_type = 4, sender = "store", roles = "store,customer")]
pub struct ChannelData {
    /// `data`.
    pub data: Vec<crate::datatypes::channel_data::DataItem>,
}

/// `Energistics.Etp.v12.Protocol.ChannelSubscribe.ChannelsTruncated`.
///
/// ETP protocol 21, message 13, sent by the store.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelSubscribe.ChannelsTruncated")]
#[etp(protocol = 21, message_type = 13, sender = "store", roles = "store,customer")]
pub struct ChannelsTruncated {
    /// `channels`.
    pub channels: Vec<crate::datatypes::channel_data::TruncateInfo>,
    /// `changeTime`.
    pub change_time: i64,
}

/// `Energistics.Etp.v12.Protocol.ChannelSubscribe.GetChangeAnnotations`.
///
/// ETP protocol 21, message 14, sent by the customer.
///
/// 1 of 2 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelSubscribe.GetChangeAnnotations")]
#[etp(protocol = 21, message_type = 14, sender = "customer", roles = "store,customer")]
pub struct GetChangeAnnotations {
    /// `channels`.
    pub channels: std::collections::BTreeMap<String, crate::datatypes::channel_data::ChannelChangeRequestInfo>,
    /// `latestOnly`.
    #[avro(default = false)]
    pub latest_only: bool,
}

/// `Energistics.Etp.v12.Protocol.ChannelSubscribe.GetChangeAnnotationsResponse`.
///
/// ETP protocol 21, message 15, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelSubscribe.GetChangeAnnotationsResponse")]
#[etp(protocol = 21, message_type = 15, sender = "store", roles = "store,customer", multipart)]
pub struct GetChangeAnnotationsResponse {
    /// `changes`.
    pub changes: std::collections::BTreeMap<String, crate::datatypes::object::ChangeResponseInfo>,
}

/// `Energistics.Etp.v12.Protocol.ChannelSubscribe.GetChannelMetadata`.
///
/// ETP protocol 21, message 1, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelSubscribe.GetChannelMetadata")]
#[etp(protocol = 21, message_type = 1, sender = "customer", roles = "store,customer")]
pub struct GetChannelMetadata {
    /// `uris`.
    pub uris: std::collections::BTreeMap<String, String>,
}

/// `Energistics.Etp.v12.Protocol.ChannelSubscribe.GetChannelMetadataResponse`.
///
/// ETP protocol 21, message 2, sent by the store. May be split across several messages.
///
/// 1 of 1 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelSubscribe.GetChannelMetadataResponse")]
#[etp(protocol = 21, message_type = 2, sender = "store", roles = "store,customer", multipart)]
pub struct GetChannelMetadataResponse {
    /// `metadata`.
    #[avro(default = {})]
    pub metadata: std::collections::BTreeMap<String, crate::datatypes::channel_data::ChannelMetadataRecord>,
}

/// `Energistics.Etp.v12.Protocol.ChannelSubscribe.GetRanges`.
///
/// ETP protocol 21, message 9, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelSubscribe.GetRanges")]
#[etp(protocol = 21, message_type = 9, sender = "customer", roles = "store,customer")]
pub struct GetRanges {
    /// `requestUuid`.
    pub request_uuid: crate::datatypes::Uuid,
    /// `channelRanges`.
    pub channel_ranges: Vec<crate::datatypes::channel_data::ChannelRangeInfo>,
}

/// `Energistics.Etp.v12.Protocol.ChannelSubscribe.GetRangesResponse`.
///
/// ETP protocol 21, message 10, sent by the store. May be split across several messages.
///
/// 1 of 1 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelSubscribe.GetRangesResponse")]
#[etp(protocol = 21, message_type = 10, sender = "store", roles = "store,customer", multipart)]
pub struct GetRangesResponse {
    /// `data`.
    #[avro(default = [])]
    pub data: Vec<crate::datatypes::channel_data::DataItem>,
}

/// `Energistics.Etp.v12.Protocol.ChannelSubscribe.RangeReplaced`.
///
/// ETP protocol 21, message 6, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelSubscribe.RangeReplaced")]
#[etp(protocol = 21, message_type = 6, sender = "store", roles = "store,customer", multipart)]
pub struct RangeReplaced {
    /// `changeTime`.
    pub change_time: i64,
    /// `channelIds`.
    pub channel_ids: Vec<i64>,
    /// `changedInterval`.
    pub changed_interval: crate::datatypes::object::IndexInterval,
    /// `data`.
    pub data: Vec<crate::datatypes::channel_data::DataItem>,
}

/// `Energistics.Etp.v12.Protocol.ChannelSubscribe.SubscribeChannels`.
///
/// ETP protocol 21, message 3, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelSubscribe.SubscribeChannels")]
#[etp(protocol = 21, message_type = 3, sender = "customer", roles = "store,customer")]
pub struct SubscribeChannels {
    /// `channels`.
    pub channels: std::collections::BTreeMap<String, crate::datatypes::channel_data::ChannelSubscribeInfo>,
}

/// `Energistics.Etp.v12.Protocol.ChannelSubscribe.SubscribeChannelsResponse`.
///
/// ETP protocol 21, message 12, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelSubscribe.SubscribeChannelsResponse")]
#[etp(protocol = 21, message_type = 12, sender = "store", roles = "store,customer", multipart)]
pub struct SubscribeChannelsResponse {
    /// `success`.
    pub success: std::collections::BTreeMap<String, String>,
}

/// `Energistics.Etp.v12.Protocol.ChannelSubscribe.SubscriptionsStopped`.
///
/// ETP protocol 21, message 8, sent by the store. May be split across several messages.
///
/// 1 of 2 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelSubscribe.SubscriptionsStopped")]
#[etp(protocol = 21, message_type = 8, sender = "store", roles = "store,customer", multipart)]
pub struct SubscriptionsStopped {
    /// `reason`.
    pub reason: String,
    /// `channelIds`.
    #[avro(default = {})]
    pub channel_ids: std::collections::BTreeMap<String, i64>,
}

/// `Energistics.Etp.v12.Protocol.ChannelSubscribe.UnsubscribeChannels`.
///
/// ETP protocol 21, message 7, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelSubscribe.UnsubscribeChannels")]
#[etp(protocol = 21, message_type = 7, sender = "customer", roles = "store,customer")]
pub struct UnsubscribeChannels {
    /// `channelIds`.
    pub channel_ids: std::collections::BTreeMap<String, i64>,
}

