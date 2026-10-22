// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 84bd808714c5d32f2b7e91d897b5677012f16bafd221694662c8058e98c13fdc
//! `protocol::channel_data_load` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Protocol.ChannelDataLoad.ChannelData`.
///
/// ETP protocol 22, message 4, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelDataLoad.ChannelData")]
#[etp(protocol = 22, message_type = 4, sender = "customer", roles = "store,customer")]
pub struct ChannelData {
    /// `data`.
    pub data: Vec<crate::datatypes::channel_data::DataItem>,
}

/// `Energistics.Etp.v12.Protocol.ChannelDataLoad.ChannelsClosed`.
///
/// ETP protocol 22, message 7, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelDataLoad.ChannelsClosed")]
#[etp(protocol = 22, message_type = 7, sender = "store", roles = "store,customer", multipart)]
pub struct ChannelsClosed {
    /// `reason`.
    pub reason: String,
    /// `id`.
    pub id: std::collections::BTreeMap<String, i64>,
}

/// `Energistics.Etp.v12.Protocol.ChannelDataLoad.CloseChannels`.
///
/// ETP protocol 22, message 3, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelDataLoad.CloseChannels")]
#[etp(protocol = 22, message_type = 3, sender = "customer", roles = "store,customer")]
pub struct CloseChannels {
    /// `id`.
    pub id: std::collections::BTreeMap<String, i64>,
}

/// `Energistics.Etp.v12.Protocol.ChannelDataLoad.OpenChannels`.
///
/// ETP protocol 22, message 1, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelDataLoad.OpenChannels")]
#[etp(protocol = 22, message_type = 1, sender = "customer", roles = "store,customer")]
pub struct OpenChannels {
    /// `uris`.
    pub uris: std::collections::BTreeMap<String, String>,
}

/// `Energistics.Etp.v12.Protocol.ChannelDataLoad.OpenChannelsResponse`.
///
/// ETP protocol 22, message 2, sent by the store. May be split across several messages.
///
/// 1 of 1 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelDataLoad.OpenChannelsResponse")]
#[etp(protocol = 22, message_type = 2, sender = "store", roles = "store,customer", multipart)]
pub struct OpenChannelsResponse {
    /// `channels`.
    #[avro(default = {})]
    pub channels: std::collections::BTreeMap<String, crate::datatypes::channel_data::OpenChannelInfo>,
}

/// `Energistics.Etp.v12.Protocol.ChannelDataLoad.ReplaceRange`.
///
/// ETP protocol 22, message 6, sent by the customer. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelDataLoad.ReplaceRange")]
#[etp(protocol = 22, message_type = 6, sender = "customer", roles = "store,customer", multipart)]
pub struct ReplaceRange {
    /// `changedInterval`.
    pub changed_interval: crate::datatypes::object::IndexInterval,
    /// `channelIds`.
    pub channel_ids: Vec<i64>,
    /// `data`.
    pub data: Vec<crate::datatypes::channel_data::DataItem>,
}

/// `Energistics.Etp.v12.Protocol.ChannelDataLoad.ReplaceRangeResponse`.
///
/// ETP protocol 22, message 8, sent by the store.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelDataLoad.ReplaceRangeResponse")]
#[etp(protocol = 22, message_type = 8, sender = "store", roles = "store,customer")]
pub struct ReplaceRangeResponse {
    /// `channelChangeTime`.
    pub channel_change_time: i64,
}

/// `Energistics.Etp.v12.Protocol.ChannelDataLoad.TruncateChannels`.
///
/// ETP protocol 22, message 9, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelDataLoad.TruncateChannels")]
#[etp(protocol = 22, message_type = 9, sender = "customer", roles = "store,customer")]
pub struct TruncateChannels {
    /// `channels`.
    pub channels: std::collections::BTreeMap<String, crate::datatypes::channel_data::TruncateInfo>,
}

/// `Energistics.Etp.v12.Protocol.ChannelDataLoad.TruncateChannelsResponse`.
///
/// ETP protocol 22, message 10, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelDataLoad.TruncateChannelsResponse")]
#[etp(protocol = 22, message_type = 10, sender = "store", roles = "store,customer", multipart)]
pub struct TruncateChannelsResponse {
    /// `channelsTruncatedTime`.
    pub channels_truncated_time: std::collections::BTreeMap<String, i64>,
}

