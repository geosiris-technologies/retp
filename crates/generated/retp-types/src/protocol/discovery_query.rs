// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 340bb1bd2c3610d260d81c9371284871112e00b0ef22a7e36d04310dc770a1f0
//! `protocol::discovery_query` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Protocol.DiscoveryQuery.FindResources`.
///
/// ETP protocol 13, message 1, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.DiscoveryQuery.FindResources")]
#[etp(protocol = 13, message_type = 1, sender = "customer", roles = "store,customer")]
pub struct FindResources {
    /// `context`.
    pub context: crate::datatypes::object::ContextInfo,
    /// `scope`.
    pub scope: crate::datatypes::object::ContextScopeKind,
    /// `storeLastWriteFilter`.
    pub store_last_write_filter: Option<i64>,
    /// `activeStatusFilter`.
    pub active_status_filter: Option<crate::datatypes::object::ActiveStatusKind>,
}

/// `Energistics.Etp.v12.Protocol.DiscoveryQuery.FindResourcesResponse`.
///
/// ETP protocol 13, message 2, sent by the store. May be split across several messages.
///
/// 1 of 2 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.DiscoveryQuery.FindResourcesResponse")]
#[etp(protocol = 13, message_type = 2, sender = "store", roles = "store,customer", multipart)]
pub struct FindResourcesResponse {
    /// `resources`.
    #[avro(default = [])]
    pub resources: Vec<crate::datatypes::object::Resource>,
    /// `serverSortOrder`.
    pub server_sort_order: String,
}

