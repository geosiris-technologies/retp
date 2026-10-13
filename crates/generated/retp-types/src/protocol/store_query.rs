// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 340bb1bd2c3610d260d81c9371284871112e00b0ef22a7e36d04310dc770a1f0
//! `protocol::store_query` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Protocol.StoreQuery.Chunk`.
///
/// ETP protocol 14, message 3, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.StoreQuery.Chunk")]
#[etp(protocol = 14, message_type = 3, sender = "store", roles = "store,customer", multipart)]
pub struct Chunk {
    /// `blobId`.
    pub blob_id: crate::datatypes::Uuid,
    /// `data`.
    pub data: Vec<u8>,
    /// `final`.
    pub r#final: bool,
}

/// `Energistics.Etp.v12.Protocol.StoreQuery.FindDataObjects`.
///
/// ETP protocol 14, message 1, sent by the customer.
///
/// 1 of 5 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.StoreQuery.FindDataObjects")]
#[etp(protocol = 14, message_type = 1, sender = "customer", roles = "store,customer")]
pub struct FindDataObjects {
    /// `context`.
    pub context: crate::datatypes::object::ContextInfo,
    /// `scope`.
    pub scope: crate::datatypes::object::ContextScopeKind,
    /// `storeLastWriteFilter`.
    pub store_last_write_filter: Option<i64>,
    /// `activeStatusFilter`.
    pub active_status_filter: Option<crate::datatypes::object::ActiveStatusKind>,
    /// `format`.
    #[avro(default = "xml")]
    pub format: String,
}

/// `Energistics.Etp.v12.Protocol.StoreQuery.FindDataObjectsResponse`.
///
/// ETP protocol 14, message 2, sent by the store. May be split across several messages.
///
/// 1 of 2 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.StoreQuery.FindDataObjectsResponse")]
#[etp(protocol = 14, message_type = 2, sender = "store", roles = "store,customer", multipart)]
pub struct FindDataObjectsResponse {
    /// `dataObjects`.
    #[avro(default = [])]
    pub data_objects: Vec<crate::datatypes::object::DataObject>,
    /// `serverSortOrder`.
    pub server_sort_order: String,
}

