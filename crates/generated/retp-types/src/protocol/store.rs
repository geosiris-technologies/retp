// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 84bd808714c5d32f2b7e91d897b5677012f16bafd221694662c8058e98c13fdc
//! `protocol::store` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Protocol.Store.Chunk`.
///
/// ETP protocol 4, message 8, sent by the store,customer. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Store.Chunk")]
#[etp(protocol = 4, message_type = 8, sender = "store,customer", roles = "store,customer", multipart)]
pub struct Chunk {
    /// `blobId`.
    pub blob_id: crate::datatypes::Uuid,
    /// `data`.
    pub data: Vec<u8>,
    /// `final`.
    pub r#final: bool,
}

/// `Energistics.Etp.v12.Protocol.Store.DeleteDataObjects`.
///
/// ETP protocol 4, message 3, sent by the customer.
///
/// 1 of 2 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Store.DeleteDataObjects")]
#[etp(protocol = 4, message_type = 3, sender = "customer", roles = "store,customer")]
pub struct DeleteDataObjects {
    /// `uris`.
    pub uris: std::collections::BTreeMap<String, String>,
    /// `pruneContainedObjects`.
    #[avro(default = false)]
    pub prune_contained_objects: bool,
}

/// `Energistics.Etp.v12.Protocol.Store.DeleteDataObjectsResponse`.
///
/// ETP protocol 4, message 10, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Store.DeleteDataObjectsResponse")]
#[etp(protocol = 4, message_type = 10, sender = "store", roles = "store,customer", multipart)]
pub struct DeleteDataObjectsResponse {
    /// `deletedUris`.
    pub deleted_uris: std::collections::BTreeMap<String, crate::datatypes::ArrayOfString>,
}

/// `Energistics.Etp.v12.Protocol.Store.GetDataObjects`.
///
/// ETP protocol 4, message 1, sent by the customer.
///
/// 1 of 2 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Store.GetDataObjects")]
#[etp(protocol = 4, message_type = 1, sender = "customer", roles = "store,customer")]
pub struct GetDataObjects {
    /// `uris`.
    pub uris: std::collections::BTreeMap<String, String>,
    /// `format`.
    #[avro(default = "xml")]
    pub format: String,
}

/// `Energistics.Etp.v12.Protocol.Store.GetDataObjectsResponse`.
///
/// ETP protocol 4, message 4, sent by the store. May be split across several messages.
///
/// 1 of 1 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Store.GetDataObjectsResponse")]
#[etp(protocol = 4, message_type = 4, sender = "store", roles = "store,customer", multipart)]
pub struct GetDataObjectsResponse {
    /// `dataObjects`.
    #[avro(default = {})]
    pub data_objects: std::collections::BTreeMap<String, crate::datatypes::object::DataObject>,
}

/// `Energistics.Etp.v12.Protocol.Store.PutDataObjects`.
///
/// ETP protocol 4, message 2, sent by the customer. May be split across several messages.
///
/// 1 of 2 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Store.PutDataObjects")]
#[etp(protocol = 4, message_type = 2, sender = "customer", roles = "store,customer", multipart)]
pub struct PutDataObjects {
    /// `dataObjects`.
    pub data_objects: std::collections::BTreeMap<String, crate::datatypes::object::DataObject>,
    /// `pruneContainedObjects`.
    #[avro(default = false)]
    pub prune_contained_objects: bool,
}

/// `Energistics.Etp.v12.Protocol.Store.PutDataObjectsResponse`.
///
/// ETP protocol 4, message 9, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Store.PutDataObjectsResponse")]
#[etp(protocol = 4, message_type = 9, sender = "store", roles = "store,customer", multipart)]
pub struct PutDataObjectsResponse {
    /// `success`.
    pub success: std::collections::BTreeMap<String, crate::datatypes::object::PutResponse>,
}

