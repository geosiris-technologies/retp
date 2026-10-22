// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 84bd808714c5d32f2b7e91d897b5677012f16bafd221694662c8058e98c13fdc
//! `protocol::core` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Protocol.Core.Acknowledge`.
///
/// ETP protocol 0, message 1001, sent by the *.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Core.Acknowledge")]
#[etp(protocol = 0, message_type = 1001, sender = "*", roles = "client, server")]
pub struct Acknowledge {
}

/// `Energistics.Etp.v12.Protocol.Core.Authorize`.
///
/// ETP protocol 0, message 6, sent by the client,server.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Core.Authorize")]
#[etp(protocol = 0, message_type = 6, sender = "client,server", roles = "client, server")]
pub struct Authorize {
    /// `authorization`.
    pub authorization: String,
    /// `supplementalAuthorization`.
    pub supplemental_authorization: std::collections::BTreeMap<String, String>,
}

/// `Energistics.Etp.v12.Protocol.Core.AuthorizeResponse`.
///
/// ETP protocol 0, message 7, sent by the client,server.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Core.AuthorizeResponse")]
#[etp(protocol = 0, message_type = 7, sender = "client,server", roles = "client, server")]
pub struct AuthorizeResponse {
    /// `success`.
    pub success: bool,
    /// `challenges`.
    pub challenges: Vec<String>,
}

/// `Energistics.Etp.v12.Protocol.Core.CloseSession`.
///
/// ETP protocol 0, message 5, sent by the client,server.
///
/// 1 of 1 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Core.CloseSession")]
#[etp(protocol = 0, message_type = 5, sender = "client,server", roles = "client, server")]
pub struct CloseSession {
    /// `reason`.
    #[avro(default = "")]
    pub reason: String,
}

/// `Energistics.Etp.v12.Protocol.Core.OpenSession`.
///
/// ETP protocol 0, message 2, sent by the server.
///
/// 3 of 11 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Core.OpenSession")]
#[etp(protocol = 0, message_type = 2, sender = "server", roles = "client, server")]
pub struct OpenSession {
    /// `applicationName`.
    pub application_name: String,
    /// `applicationVersion`.
    pub application_version: String,
    /// `serverInstanceId`.
    pub server_instance_id: crate::datatypes::Uuid,
    /// `supportedProtocols`.
    pub supported_protocols: Vec<crate::datatypes::SupportedProtocol>,
    /// `supportedDataObjects`.
    pub supported_data_objects: Vec<crate::datatypes::SupportedDataObject>,
    /// `supportedCompression`.
    #[avro(default = "")]
    pub supported_compression: String,
    /// `supportedFormats`.
    #[avro(default = ["xml"])]
    pub supported_formats: Vec<String>,
    /// `currentDateTime`.
    pub current_date_time: i64,
    /// `earliestRetainedChangeTime`.
    pub earliest_retained_change_time: i64,
    /// `sessionId`.
    pub session_id: crate::datatypes::Uuid,
    /// `endpointCapabilities`.
    #[avro(default = {})]
    pub endpoint_capabilities: std::collections::BTreeMap<String, crate::datatypes::DataValue>,
}

/// `Energistics.Etp.v12.Protocol.Core.Ping`.
///
/// ETP protocol 0, message 8, sent by the client,server.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Core.Ping")]
#[etp(protocol = 0, message_type = 8, sender = "client,server", roles = "client, server")]
pub struct Ping {
    /// `currentDateTime`.
    pub current_date_time: i64,
}

/// `Energistics.Etp.v12.Protocol.Core.Pong`.
///
/// ETP protocol 0, message 9, sent by the client,server.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Core.Pong")]
#[etp(protocol = 0, message_type = 9, sender = "client,server", roles = "client, server")]
pub struct Pong {
    /// `currentDateTime`.
    pub current_date_time: i64,
}

/// `Energistics.Etp.v12.Protocol.Core.ProtocolException`.
///
/// ETP protocol 0, message 1000, sent by the *. May be split across several messages.
///
/// 1 of 2 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Core.ProtocolException")]
#[etp(protocol = 0, message_type = 1000, sender = "*", roles = "client, server", multipart)]
pub struct ProtocolException {
    /// `error`.
    pub error: Option<crate::datatypes::ErrorInfo>,
    /// `errors`.
    #[avro(default = {})]
    pub errors: std::collections::BTreeMap<String, crate::datatypes::ErrorInfo>,
}

/// `Energistics.Etp.v12.Protocol.Core.RequestSession`.
///
/// ETP protocol 0, message 1, sent by the client.
///
/// 4 of 11 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Core.RequestSession")]
#[etp(protocol = 0, message_type = 1, sender = "client", roles = "client, server")]
pub struct RequestSession {
    /// `applicationName`.
    pub application_name: String,
    /// `applicationVersion`.
    pub application_version: String,
    /// `clientInstanceId`.
    pub client_instance_id: crate::datatypes::Uuid,
    /// `requestedProtocols`.
    pub requested_protocols: Vec<crate::datatypes::SupportedProtocol>,
    /// `supportedDataObjects`.
    pub supported_data_objects: Vec<crate::datatypes::SupportedDataObject>,
    /// `supportedCompression`.
    #[avro(default = [])]
    pub supported_compression: Vec<String>,
    /// `supportedFormats`.
    #[avro(default = ["xml"])]
    pub supported_formats: Vec<String>,
    /// `currentDateTime`.
    pub current_date_time: i64,
    /// `earliestRetainedChangeTime`.
    pub earliest_retained_change_time: i64,
    /// `serverAuthorizationRequired`.
    #[avro(default = false)]
    pub server_authorization_required: bool,
    /// `endpointCapabilities`.
    #[avro(default = {})]
    pub endpoint_capabilities: std::collections::BTreeMap<String, crate::datatypes::DataValue>,
}

