// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 84bd808714c5d32f2b7e91d897b5677012f16bafd221694662c8058e98c13fdc
//! `protocol::discovery` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Protocol.Discovery.GetDeletedResources`.
///
/// ETP protocol 3, message 5, sent by the customer.
///
/// 1 of 3 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Discovery.GetDeletedResources")]
#[etp(protocol = 3, message_type = 5, sender = "customer", roles = "store,customer")]
pub struct GetDeletedResources {
    /// `dataspaceUri`.
    pub dataspace_uri: String,
    /// `deleteTimeFilter`.
    pub delete_time_filter: Option<i64>,
    /// `dataObjectTypes`.
    #[avro(default = [])]
    pub data_object_types: Vec<String>,
}

/// `Energistics.Etp.v12.Protocol.Discovery.GetDeletedResourcesResponse`.
///
/// ETP protocol 3, message 6, sent by the store. May be split across several messages.
///
/// 1 of 1 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Discovery.GetDeletedResourcesResponse")]
#[etp(protocol = 3, message_type = 6, sender = "store", roles = "store,customer", multipart)]
pub struct GetDeletedResourcesResponse {
    /// `deletedResources`.
    #[avro(default = [])]
    pub deleted_resources: Vec<crate::datatypes::object::DeletedResource>,
}

/// `Energistics.Etp.v12.Protocol.Discovery.GetResources`.
///
/// ETP protocol 3, message 1, sent by the customer.
///
/// 2 of 6 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Discovery.GetResources")]
#[etp(protocol = 3, message_type = 1, sender = "customer", roles = "store,customer")]
pub struct GetResources {
    /// `context`.
    pub context: crate::datatypes::object::ContextInfo,
    /// `scope`.
    pub scope: crate::datatypes::object::ContextScopeKind,
    /// `countObjects`.
    #[avro(default = false)]
    pub count_objects: bool,
    /// `storeLastWriteFilter`.
    pub store_last_write_filter: Option<i64>,
    /// `activeStatusFilter`.
    pub active_status_filter: Option<crate::datatypes::object::ActiveStatusKind>,
    /// `includeEdges`.
    #[avro(default = false)]
    pub include_edges: bool,
}

/// `Energistics.Etp.v12.Protocol.Discovery.GetResourcesEdgesResponse`.
///
/// ETP protocol 3, message 7, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Discovery.GetResourcesEdgesResponse")]
#[etp(protocol = 3, message_type = 7, sender = "store", roles = "store,customer", multipart)]
pub struct GetResourcesEdgesResponse {
    /// `edges`.
    pub edges: Vec<crate::datatypes::object::Edge>,
}

/// `Energistics.Etp.v12.Protocol.Discovery.GetResourcesResponse`.
///
/// ETP protocol 3, message 4, sent by the store. May be split across several messages.
///
/// 1 of 1 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.Discovery.GetResourcesResponse")]
#[etp(protocol = 3, message_type = 4, sender = "store", roles = "store,customer", multipart)]
pub struct GetResourcesResponse {
    /// `resources`.
    #[avro(default = [])]
    pub resources: Vec<crate::datatypes::object::Resource>,
}

