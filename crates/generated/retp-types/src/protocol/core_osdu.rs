// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 84bd808714c5d32f2b7e91d897b5677012f16bafd221694662c8058e98c13fdc
//! `protocol::core_osdu` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Protocol.CoreOSDU.ResumeSession`.
///
/// ETP protocol 2400, message 1, sent by the client.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.CoreOSDU.ResumeSession")]
#[etp(protocol = 2400, message_type = 1, sender = "client", roles = "client, server")]
pub struct ResumeSession {
    /// `applicationName`.
    pub application_name: String,
    /// `applicationVersion`.
    pub application_version: String,
    /// `clientInstanceId`.
    pub client_instance_id: crate::datatypes::Uuid,
    /// `currentDateTime`.
    pub current_date_time: i64,
    /// `sessionId`.
    pub session_id: crate::datatypes::Uuid,
}

/// `Energistics.Etp.v12.Protocol.CoreOSDU.ResumeSessionResponse`.
///
/// ETP protocol 2400, message 2, sent by the server.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.CoreOSDU.ResumeSessionResponse")]
#[etp(protocol = 2400, message_type = 2, sender = "server", roles = "client, server")]
pub struct ResumeSessionResponse {
    /// `applicationName`.
    pub application_name: String,
    /// `applicationVersion`.
    pub application_version: String,
    /// `serverInstanceId`.
    pub server_instance_id: crate::datatypes::Uuid,
    /// `currentDateTime`.
    pub current_date_time: i64,
    /// `sessionId`.
    pub session_id: crate::datatypes::Uuid,
    /// `earliestRetainedChangeTime`.
    pub earliest_retained_change_time: i64,
}

