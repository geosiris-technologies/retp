// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 84bd808714c5d32f2b7e91d897b5677012f16bafd221694662c8058e98c13fdc
//! `protocol::growing_object` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Protocol.GrowingObject.DeleteParts`.
///
/// ETP protocol 6, message 1, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObject.DeleteParts")]
#[etp(protocol = 6, message_type = 1, sender = "customer", roles = "store,customer")]
pub struct DeleteParts {
    /// `uri`.
    pub uri: String,
    /// `uids`.
    pub uids: std::collections::BTreeMap<String, String>,
}

/// `Energistics.Etp.v12.Protocol.GrowingObject.DeletePartsResponse`.
///
/// ETP protocol 6, message 11, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObject.DeletePartsResponse")]
#[etp(protocol = 6, message_type = 11, sender = "store", roles = "store,customer", multipart)]
pub struct DeletePartsResponse {
    /// `success`.
    pub success: std::collections::BTreeMap<String, String>,
}

/// `Energistics.Etp.v12.Protocol.GrowingObject.GetChangeAnnotations`.
///
/// ETP protocol 6, message 19, sent by the customer.
///
/// 1 of 3 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObject.GetChangeAnnotations")]
#[etp(protocol = 6, message_type = 19, sender = "customer", roles = "store,customer")]
pub struct GetChangeAnnotations {
    /// `sinceChangeTime`.
    pub since_change_time: i64,
    /// `uris`.
    pub uris: std::collections::BTreeMap<String, String>,
    /// `latestOnly`.
    #[avro(default = false)]
    pub latest_only: bool,
}

/// `Energistics.Etp.v12.Protocol.GrowingObject.GetChangeAnnotationsResponse`.
///
/// ETP protocol 6, message 20, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObject.GetChangeAnnotationsResponse")]
#[etp(protocol = 6, message_type = 20, sender = "store", roles = "store,customer", multipart)]
pub struct GetChangeAnnotationsResponse {
    /// `changes`.
    pub changes: std::collections::BTreeMap<String, crate::datatypes::object::ChangeResponseInfo>,
}

/// `Energistics.Etp.v12.Protocol.GrowingObject.GetGrowingDataObjectsHeader`.
///
/// ETP protocol 6, message 14, sent by the customer.
///
/// 1 of 2 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObject.GetGrowingDataObjectsHeader")]
#[etp(protocol = 6, message_type = 14, sender = "customer", roles = "store,customer")]
pub struct GetGrowingDataObjectsHeader {
    /// `uris`.
    pub uris: std::collections::BTreeMap<String, String>,
    /// `format`.
    #[avro(default = "xml")]
    pub format: String,
}

/// `Energistics.Etp.v12.Protocol.GrowingObject.GetGrowingDataObjectsHeaderResponse`.
///
/// ETP protocol 6, message 15, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObject.GetGrowingDataObjectsHeaderResponse")]
#[etp(protocol = 6, message_type = 15, sender = "store", roles = "store,customer", multipart)]
pub struct GetGrowingDataObjectsHeaderResponse {
    /// `dataObjects`.
    pub data_objects: std::collections::BTreeMap<String, crate::datatypes::object::DataObject>,
}

/// `Energistics.Etp.v12.Protocol.GrowingObject.GetParts`.
///
/// ETP protocol 6, message 3, sent by the customer.
///
/// 1 of 3 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObject.GetParts")]
#[etp(protocol = 6, message_type = 3, sender = "customer", roles = "store,customer")]
pub struct GetParts {
    /// `uri`.
    pub uri: String,
    /// `format`.
    #[avro(default = "xml")]
    pub format: String,
    /// `uids`.
    pub uids: std::collections::BTreeMap<String, String>,
}

/// `Energistics.Etp.v12.Protocol.GrowingObject.GetPartsByRange`.
///
/// ETP protocol 6, message 4, sent by the customer.
///
/// 1 of 4 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObject.GetPartsByRange")]
#[etp(protocol = 6, message_type = 4, sender = "customer", roles = "store,customer")]
pub struct GetPartsByRange {
    /// `uri`.
    pub uri: String,
    /// `format`.
    #[avro(default = "xml")]
    pub format: String,
    /// `indexInterval`.
    pub index_interval: crate::datatypes::object::IndexInterval,
    /// `includeOverlappingIntervals`.
    pub include_overlapping_intervals: bool,
}

/// `Energistics.Etp.v12.Protocol.GrowingObject.GetPartsByRangeResponse`.
///
/// ETP protocol 6, message 10, sent by the store. May be split across several messages.
///
/// 1 of 3 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObject.GetPartsByRangeResponse")]
#[etp(protocol = 6, message_type = 10, sender = "store", roles = "store,customer", multipart)]
pub struct GetPartsByRangeResponse {
    /// `uri`.
    pub uri: String,
    /// `format`.
    #[avro(default = "xml")]
    pub format: String,
    /// `parts`.
    pub parts: Vec<crate::datatypes::object::ObjectPart>,
}

/// `Energistics.Etp.v12.Protocol.GrowingObject.GetPartsMetadata`.
///
/// ETP protocol 6, message 8, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObject.GetPartsMetadata")]
#[etp(protocol = 6, message_type = 8, sender = "customer", roles = "store,customer")]
pub struct GetPartsMetadata {
    /// `uris`.
    pub uris: std::collections::BTreeMap<String, String>,
}

/// `Energistics.Etp.v12.Protocol.GrowingObject.GetPartsMetadataResponse`.
///
/// ETP protocol 6, message 9, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObject.GetPartsMetadataResponse")]
#[etp(protocol = 6, message_type = 9, sender = "store", roles = "store,customer", multipart)]
pub struct GetPartsMetadataResponse {
    /// `metadata`.
    pub metadata: std::collections::BTreeMap<String, crate::datatypes::object::PartsMetadataInfo>,
}

/// `Energistics.Etp.v12.Protocol.GrowingObject.GetPartsResponse`.
///
/// ETP protocol 6, message 6, sent by the store. May be split across several messages.
///
/// 1 of 3 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObject.GetPartsResponse")]
#[etp(protocol = 6, message_type = 6, sender = "store", roles = "store,customer", multipart)]
pub struct GetPartsResponse {
    /// `uri`.
    pub uri: String,
    /// `format`.
    #[avro(default = "xml")]
    pub format: String,
    /// `parts`.
    pub parts: std::collections::BTreeMap<String, crate::datatypes::object::ObjectPart>,
}

/// `Energistics.Etp.v12.Protocol.GrowingObject.PutGrowingDataObjectsHeader`.
///
/// ETP protocol 6, message 16, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObject.PutGrowingDataObjectsHeader")]
#[etp(protocol = 6, message_type = 16, sender = "customer", roles = "store,customer")]
pub struct PutGrowingDataObjectsHeader {
    /// `dataObjects`.
    pub data_objects: std::collections::BTreeMap<String, crate::datatypes::object::DataObject>,
}

/// `Energistics.Etp.v12.Protocol.GrowingObject.PutGrowingDataObjectsHeaderResponse`.
///
/// ETP protocol 6, message 17, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObject.PutGrowingDataObjectsHeaderResponse")]
#[etp(protocol = 6, message_type = 17, sender = "store", roles = "store,customer", multipart)]
pub struct PutGrowingDataObjectsHeaderResponse {
    /// `success`.
    pub success: std::collections::BTreeMap<String, String>,
}

/// `Energistics.Etp.v12.Protocol.GrowingObject.PutParts`.
///
/// ETP protocol 6, message 5, sent by the customer.
///
/// 1 of 3 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObject.PutParts")]
#[etp(protocol = 6, message_type = 5, sender = "customer", roles = "store,customer")]
pub struct PutParts {
    /// `uri`.
    pub uri: String,
    /// `format`.
    #[avro(default = "xml")]
    pub format: String,
    /// `parts`.
    pub parts: std::collections::BTreeMap<String, crate::datatypes::object::ObjectPart>,
}

/// `Energistics.Etp.v12.Protocol.GrowingObject.PutPartsResponse`.
///
/// ETP protocol 6, message 13, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObject.PutPartsResponse")]
#[etp(protocol = 6, message_type = 13, sender = "store", roles = "store,customer", multipart)]
pub struct PutPartsResponse {
    /// `success`.
    pub success: std::collections::BTreeMap<String, String>,
}

/// `Energistics.Etp.v12.Protocol.GrowingObject.ReplacePartsByRange`.
///
/// ETP protocol 6, message 7, sent by the customer. May be split across several messages.
///
/// 1 of 5 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObject.ReplacePartsByRange")]
#[etp(protocol = 6, message_type = 7, sender = "customer", roles = "store,customer", multipart)]
pub struct ReplacePartsByRange {
    /// `uri`.
    pub uri: String,
    /// `deleteInterval`.
    pub delete_interval: crate::datatypes::object::IndexInterval,
    /// `includeOverlappingIntervals`.
    pub include_overlapping_intervals: bool,
    /// `format`.
    #[avro(default = "xml")]
    pub format: String,
    /// `parts`.
    pub parts: Vec<crate::datatypes::object::ObjectPart>,
}

/// `Energistics.Etp.v12.Protocol.GrowingObject.ReplacePartsByRangeResponse`.
///
/// ETP protocol 6, message 18, sent by the store.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.GrowingObject.ReplacePartsByRangeResponse")]
#[etp(protocol = 6, message_type = 18, sender = "store", roles = "store,customer")]
pub struct ReplacePartsByRangeResponse {
}

