// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 84bd808714c5d32f2b7e91d897b5677012f16bafd221694662c8058e98c13fdc
//! `protocol::supported_types` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Protocol.SupportedTypes.GetSupportedTypes`.
///
/// ETP protocol 25, message 1, sent by the customer.
///
/// 2 of 4 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.SupportedTypes.GetSupportedTypes")]
#[etp(protocol = 25, message_type = 1, sender = "customer", roles = "store,customer")]
pub struct GetSupportedTypes {
    /// `uri`.
    pub uri: String,
    /// `scope`.
    pub scope: crate::datatypes::object::ContextScopeKind,
    /// `returnEmptyTypes`.
    #[avro(default = false)]
    pub return_empty_types: bool,
    /// `countObjects`.
    #[avro(default = false)]
    pub count_objects: bool,
}

/// `Energistics.Etp.v12.Protocol.SupportedTypes.GetSupportedTypesResponse`.
///
/// ETP protocol 25, message 2, sent by the store. May be split across several messages.
///
/// 1 of 1 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.SupportedTypes.GetSupportedTypesResponse")]
#[etp(protocol = 25, message_type = 2, sender = "store", roles = "store,customer", multipart)]
pub struct GetSupportedTypesResponse {
    /// `supportedTypes`.
    #[avro(default = [])]
    pub supported_types: Vec<crate::datatypes::object::SupportedType>,
}

