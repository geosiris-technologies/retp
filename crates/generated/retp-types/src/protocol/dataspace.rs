// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 84bd808714c5d32f2b7e91d897b5677012f16bafd221694662c8058e98c13fdc
//! `protocol::dataspace` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Protocol.Dataspace.DeleteDataspaces`.
///
/// ETP protocol 24, message 4, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Dataspace.DeleteDataspaces")]
#[etp(protocol = 24, message_type = 4, sender = "customer", roles = "store,customer")]
pub struct DeleteDataspaces {
    /// `uris`.
    pub uris: std::collections::BTreeMap<String, String>,
}

/// `Energistics.Etp.v12.Protocol.Dataspace.DeleteDataspacesResponse`.
///
/// ETP protocol 24, message 5, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Dataspace.DeleteDataspacesResponse")]
#[etp(protocol = 24, message_type = 5, sender = "store", roles = "store,customer", multipart)]
pub struct DeleteDataspacesResponse {
    /// `success`.
    pub success: std::collections::BTreeMap<String, String>,
}

/// `Energistics.Etp.v12.Protocol.Dataspace.GetDataspaces`.
///
/// ETP protocol 24, message 1, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Dataspace.GetDataspaces")]
#[etp(protocol = 24, message_type = 1, sender = "customer", roles = "store,customer")]
pub struct GetDataspaces {
    /// `storeLastWriteFilter`.
    pub store_last_write_filter: Option<i64>,
}

/// `Energistics.Etp.v12.Protocol.Dataspace.GetDataspacesResponse`.
///
/// ETP protocol 24, message 2, sent by the store. May be split across several messages.
///
/// 1 of 1 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Dataspace.GetDataspacesResponse")]
#[etp(protocol = 24, message_type = 2, sender = "store", roles = "store,customer", multipart)]
pub struct GetDataspacesResponse {
    /// `dataspaces`.
    #[avro(default = [])]
    pub dataspaces: Vec<crate::datatypes::object::Dataspace>,
}

/// `Energistics.Etp.v12.Protocol.Dataspace.PutDataspaces`.
///
/// ETP protocol 24, message 3, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Dataspace.PutDataspaces")]
#[etp(protocol = 24, message_type = 3, sender = "customer", roles = "store,customer")]
pub struct PutDataspaces {
    /// `dataspaces`.
    pub dataspaces: std::collections::BTreeMap<String, crate::datatypes::object::Dataspace>,
}

/// `Energistics.Etp.v12.Protocol.Dataspace.PutDataspacesResponse`.
///
/// ETP protocol 24, message 6, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Dataspace.PutDataspacesResponse")]
#[etp(protocol = 24, message_type = 6, sender = "store", roles = "store,customer", multipart)]
pub struct PutDataspacesResponse {
    /// `success`.
    pub success: std::collections::BTreeMap<String, String>,
}

