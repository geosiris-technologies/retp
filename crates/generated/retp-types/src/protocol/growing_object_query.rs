// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 84bd808714c5d32f2b7e91d897b5677012f16bafd221694662c8058e98c13fdc
//! `protocol::growing_object_query` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Protocol.GrowingObjectQuery.FindParts`.
///
/// ETP protocol 16, message 1, sent by the customer.
///
/// 1 of 2 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObjectQuery.FindParts")]
#[etp(protocol = 16, message_type = 1, sender = "customer", roles = "store,customer")]
pub struct FindParts {
    /// `uri`.
    pub uri: String,
    /// `format`.
    #[avro(default = "xml")]
    pub format: String,
}

/// `Energistics.Etp.v12.Protocol.GrowingObjectQuery.FindPartsResponse`.
///
/// ETP protocol 16, message 2, sent by the store. May be split across several messages.
///
/// 2 of 4 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObjectQuery.FindPartsResponse")]
#[etp(protocol = 16, message_type = 2, sender = "store", roles = "store,customer", multipart)]
pub struct FindPartsResponse {
    /// `uri`.
    pub uri: String,
    /// `serverSortOrder`.
    pub server_sort_order: String,
    /// `format`.
    #[avro(default = "xml")]
    pub format: String,
    /// `parts`.
    #[avro(default = [])]
    pub parts: Vec<crate::datatypes::object::ObjectPart>,
}

