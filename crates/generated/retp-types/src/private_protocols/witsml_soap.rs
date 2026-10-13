// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 340bb1bd2c3610d260d81c9371284871112e00b0ef22a7e36d04310dc770a1f0
//! `private_protocols::witsml_soap` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_AddToStore`.
///
/// ETP protocol 2100, message 1, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_AddToStore")]
#[etp(protocol = 2100, message_type = 1, sender = "customer", roles = "store,customer")]
pub struct WMLS_AddToStore {
    /// `WMLtypeIn`.
    #[avro(rename = "WMLtypeIn")]
    pub wm_ltype_in: String,
    /// `XMLin`.
    #[avro(rename = "XMLin")]
    pub xm_lin: String,
    /// `OptionsIn`.
    #[avro(rename = "OptionsIn")]
    pub options_in: String,
    /// `CapabilitiesIn`.
    #[avro(rename = "CapabilitiesIn")]
    pub capabilities_in: String,
}

/// `Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_AddToStoreResponse`.
///
/// ETP protocol 2100, message 2, sent by the store.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_AddToStoreResponse")]
#[etp(protocol = 2100, message_type = 2, sender = "store", roles = "store,customer")]
pub struct WMLS_AddToStoreResponse {
    /// `Result`.
    #[avro(rename = "Result")]
    pub result: i32,
    /// `SuppMsgOut`.
    #[avro(rename = "SuppMsgOut")]
    pub supp_msg_out: String,
}

/// `Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_DeleteFromStore`.
///
/// ETP protocol 2100, message 3, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_DeleteFromStore")]
#[etp(protocol = 2100, message_type = 3, sender = "customer", roles = "store,customer")]
pub struct WMLS_DeleteFromStore {
    /// `WMLtypeIn`.
    #[avro(rename = "WMLtypeIn")]
    pub wm_ltype_in: String,
    /// `XMLin`.
    #[avro(rename = "XMLin")]
    pub xm_lin: String,
    /// `OptionsIn`.
    #[avro(rename = "OptionsIn")]
    pub options_in: String,
    /// `CapabilitiesIn`.
    #[avro(rename = "CapabilitiesIn")]
    pub capabilities_in: String,
}

/// `Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_DeleteFromStoreResponse`.
///
/// ETP protocol 2100, message 4, sent by the store.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_DeleteFromStoreResponse")]
#[etp(protocol = 2100, message_type = 4, sender = "store", roles = "store,customer")]
pub struct WMLS_DeleteFromStoreResponse {
    /// `Result`.
    #[avro(rename = "Result")]
    pub result: i32,
    /// `SuppMsgOut`.
    #[avro(rename = "SuppMsgOut")]
    pub supp_msg_out: String,
}

/// `Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_GetBaseMsg`.
///
/// ETP protocol 2100, message 5, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_GetBaseMsg")]
#[etp(protocol = 2100, message_type = 5, sender = "customer", roles = "store,customer")]
pub struct WMLS_GetBaseMsg {
    /// `ReturnValueIn`.
    #[avro(rename = "ReturnValueIn")]
    pub return_value_in: i32,
}

/// `Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_GetBaseMsgResponse`.
///
/// ETP protocol 2100, message 6, sent by the store.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_GetBaseMsgResponse")]
#[etp(protocol = 2100, message_type = 6, sender = "store", roles = "store,customer")]
pub struct WMLS_GetBaseMsgResponse {
    /// `Result`.
    #[avro(rename = "Result")]
    pub result: String,
}

/// `Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_GetCap`.
///
/// ETP protocol 2100, message 7, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_GetCap")]
#[etp(protocol = 2100, message_type = 7, sender = "customer", roles = "store,customer")]
pub struct WMLS_GetCap {
    /// `OptionsIn`.
    #[avro(rename = "OptionsIn")]
    pub options_in: String,
}

/// `Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_GetCapResponse`.
///
/// ETP protocol 2100, message 8, sent by the store.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_GetCapResponse")]
#[etp(protocol = 2100, message_type = 8, sender = "store", roles = "store,customer")]
pub struct WMLS_GetCapResponse {
    /// `Result`.
    #[avro(rename = "Result")]
    pub result: i32,
    /// `CapabilitiesOut`.
    #[avro(rename = "CapabilitiesOut")]
    pub capabilities_out: String,
    /// `SuppMsgOut`.
    #[avro(rename = "SuppMsgOut")]
    pub supp_msg_out: String,
}

/// `Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_GetFromStore`.
///
/// ETP protocol 2100, message 9, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_GetFromStore")]
#[etp(protocol = 2100, message_type = 9, sender = "customer", roles = "store,customer")]
pub struct WMLS_GetFromStore {
    /// `WMLtypeIn`.
    #[avro(rename = "WMLtypeIn")]
    pub wm_ltype_in: String,
    /// `XMLin`.
    #[avro(rename = "XMLin")]
    pub xm_lin: String,
    /// `OptionsIn`.
    #[avro(rename = "OptionsIn")]
    pub options_in: String,
    /// `CapabilitiesIn`.
    #[avro(rename = "CapabilitiesIn")]
    pub capabilities_in: String,
}

/// `Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_GetFromStoreResponse`.
///
/// ETP protocol 2100, message 10, sent by the store.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_GetFromStoreResponse")]
#[etp(protocol = 2100, message_type = 10, sender = "store", roles = "store,customer")]
pub struct WMLS_GetFromStoreResponse {
    /// `Result`.
    #[avro(rename = "Result")]
    pub result: i32,
    /// `XMLout`.
    #[avro(rename = "XMLout")]
    pub xm_lout: String,
    /// `SuppMsgOut`.
    #[avro(rename = "SuppMsgOut")]
    pub supp_msg_out: String,
}

/// `Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_GetVersion`.
///
/// ETP protocol 2100, message 11, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_GetVersion")]
#[etp(protocol = 2100, message_type = 11, sender = "customer", roles = "store,customer")]
pub struct WMLS_GetVersion {
}

/// `Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_GetVersionResponse`.
///
/// ETP protocol 2100, message 12, sent by the store.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_GetVersionResponse")]
#[etp(protocol = 2100, message_type = 12, sender = "store", roles = "store,customer")]
pub struct WMLS_GetVersionResponse {
    /// `Result`.
    #[avro(rename = "Result")]
    pub result: String,
}

/// `Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_UpdateInStore`.
///
/// ETP protocol 2100, message 13, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_UpdateInStore")]
#[etp(protocol = 2100, message_type = 13, sender = "customer", roles = "store,customer")]
pub struct WMLS_UpdateInStore {
    /// `WMLtypeIn`.
    #[avro(rename = "WMLtypeIn")]
    pub wm_ltype_in: String,
    /// `XMLin`.
    #[avro(rename = "XMLin")]
    pub xm_lin: String,
    /// `OptionsIn`.
    #[avro(rename = "OptionsIn")]
    pub options_in: String,
    /// `CapabilitiesIn`.
    #[avro(rename = "CapabilitiesIn")]
    pub capabilities_in: String,
}

/// `Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_UpdateInStoreResponse`.
///
/// ETP protocol 2100, message 14, sent by the store.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.PrivateProtocols.WitsmlSoap.WMLS_UpdateInStoreResponse")]
#[etp(protocol = 2100, message_type = 14, sender = "store", roles = "store,customer")]
pub struct WMLS_UpdateInStoreResponse {
    /// `Result`.
    #[avro(rename = "Result")]
    pub result: i32,
    /// `SuppMsgOut`.
    #[avro(rename = "SuppMsgOut")]
    pub supp_msg_out: String,
}

