// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 84bd808714c5d32f2b7e91d897b5677012f16bafd221694662c8058e98c13fdc
//! `protocol::channel_data_frame` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Protocol.ChannelDataFrame.CancelGetFrame`.
///
/// ETP protocol 2, message 5, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelDataFrame.CancelGetFrame")]
#[etp(protocol = 2, message_type = 5, sender = "customer", roles = "store,customer")]
pub struct CancelGetFrame {
    /// `requestUuid`.
    pub request_uuid: crate::datatypes::Uuid,
}

/// `Energistics.Etp.v12.Protocol.ChannelDataFrame.GetFrame`.
///
/// ETP protocol 2, message 3, sent by the customer.
///
/// 2 of 5 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelDataFrame.GetFrame")]
#[etp(protocol = 2, message_type = 3, sender = "customer", roles = "store,customer")]
pub struct GetFrame {
    /// `uri`.
    pub uri: String,
    /// `includeAllChannelSecondaryIndexes`.
    #[avro(default = false)]
    pub include_all_channel_secondary_indexes: bool,
    /// `requestedInterval`.
    pub requested_interval: crate::datatypes::object::IndexInterval,
    /// `requestUuid`.
    pub request_uuid: crate::datatypes::Uuid,
    /// `requestedSecondaryIntervals`.
    #[avro(default = [])]
    pub requested_secondary_intervals: Vec<crate::datatypes::object::IndexInterval>,
}

/// `Energistics.Etp.v12.Protocol.ChannelDataFrame.GetFrameMetadata`.
///
/// ETP protocol 2, message 1, sent by the customer.
///
/// 1 of 2 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelDataFrame.GetFrameMetadata")]
#[etp(protocol = 2, message_type = 1, sender = "customer", roles = "store,customer")]
pub struct GetFrameMetadata {
    /// `uri`.
    pub uri: String,
    /// `includeAllChannelSecondaryIndexes`.
    #[avro(default = false)]
    pub include_all_channel_secondary_indexes: bool,
}

/// `Energistics.Etp.v12.Protocol.ChannelDataFrame.GetFrameMetadataResponse`.
///
/// ETP protocol 2, message 2, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelDataFrame.GetFrameMetadataResponse")]
#[etp(protocol = 2, message_type = 2, sender = "store", roles = "store,customer", multipart)]
pub struct GetFrameMetadataResponse {
    /// `uri`.
    pub uri: String,
    /// `indexes`.
    pub indexes: Vec<crate::datatypes::channel_data::IndexMetadataRecord>,
    /// `channels`.
    pub channels: Vec<crate::datatypes::channel_data::FrameChannelMetadataRecord>,
}

/// `Energistics.Etp.v12.Protocol.ChannelDataFrame.GetFrameResponseHeader`.
///
/// ETP protocol 2, message 4, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelDataFrame.GetFrameResponseHeader")]
#[etp(protocol = 2, message_type = 4, sender = "store", roles = "store,customer", multipart)]
pub struct GetFrameResponseHeader {
    /// `channelUris`.
    pub channel_uris: Vec<String>,
    /// `indexes`.
    pub indexes: Vec<crate::datatypes::channel_data::IndexMetadataRecord>,
}

/// `Energistics.Etp.v12.Protocol.ChannelDataFrame.GetFrameResponseRows`.
///
/// ETP protocol 2, message 6, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelDataFrame.GetFrameResponseRows")]
#[etp(protocol = 2, message_type = 6, sender = "store", roles = "store,customer", multipart)]
pub struct GetFrameResponseRows {
    /// `frame`.
    pub frame: Vec<crate::datatypes::channel_data::FrameRow>,
}

