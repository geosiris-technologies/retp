// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 340bb1bd2c3610d260d81c9371284871112e00b0ef22a7e36d04310dc770a1f0
//! `protocol::channel_streaming` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Protocol.ChannelStreaming.ChannelData`.
///
/// ETP protocol 1, message 2, sent by the producer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelStreaming.ChannelData")]
#[etp(protocol = 1, message_type = 2, sender = "producer", roles = "producer,consumer")]
pub struct ChannelData {
    /// `data`.
    pub data: Vec<crate::datatypes::channel_data::DataItem>,
}

/// `Energistics.Etp.v12.Protocol.ChannelStreaming.ChannelMetadata`.
///
/// ETP protocol 1, message 1, sent by the producer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelStreaming.ChannelMetadata")]
#[etp(protocol = 1, message_type = 1, sender = "producer", roles = "producer,consumer")]
pub struct ChannelMetadata {
    /// `channels`.
    pub channels: Vec<crate::datatypes::channel_data::ChannelMetadataRecord>,
}

/// `Energistics.Etp.v12.Protocol.ChannelStreaming.StartStreaming`.
///
/// ETP protocol 1, message 3, sent by the consumer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelStreaming.StartStreaming")]
#[etp(protocol = 1, message_type = 3, sender = "consumer", roles = "producer,consumer")]
pub struct StartStreaming {
}

/// `Energistics.Etp.v12.Protocol.ChannelStreaming.StopStreaming`.
///
/// ETP protocol 1, message 4, sent by the consumer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelStreaming.StopStreaming")]
#[etp(protocol = 1, message_type = 4, sender = "consumer", roles = "producer,consumer")]
pub struct StopStreaming {
}

/// `Energistics.Etp.v12.Protocol.ChannelStreaming.TruncateChannels`.
///
/// ETP protocol 1, message 5, sent by the producer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.ChannelStreaming.TruncateChannels")]
#[etp(protocol = 1, message_type = 5, sender = "producer", roles = "producer,consumer")]
pub struct TruncateChannels {
    /// `channels`.
    pub channels: Vec<crate::datatypes::channel_data::TruncateInfo>,
}

