// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 84bd808714c5d32f2b7e91d897b5677012f16bafd221694662c8058e98c13fdc
//! `protocol::data_array` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Protocol.DataArray.GetDataArrayMetadata`.
///
/// ETP protocol 9, message 6, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.DataArray.GetDataArrayMetadata")]
#[etp(protocol = 9, message_type = 6, sender = "customer", roles = "store,customer")]
pub struct GetDataArrayMetadata {
    /// `dataArrays`.
    pub data_arrays: std::collections::BTreeMap<String, crate::datatypes::data_array_types::DataArrayIdentifier>,
}

/// `Energistics.Etp.v12.Protocol.DataArray.GetDataArrayMetadataResponse`.
///
/// ETP protocol 9, message 7, sent by the store. May be split across several messages.
///
/// 1 of 1 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.DataArray.GetDataArrayMetadataResponse")]
#[etp(protocol = 9, message_type = 7, sender = "store", roles = "store,customer", multipart)]
pub struct GetDataArrayMetadataResponse {
    /// `arrayMetadata`.
    #[avro(default = {})]
    pub array_metadata: std::collections::BTreeMap<String, crate::datatypes::data_array_types::DataArrayMetadata>,
}

/// `Energistics.Etp.v12.Protocol.DataArray.GetDataArrays`.
///
/// ETP protocol 9, message 2, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.DataArray.GetDataArrays")]
#[etp(protocol = 9, message_type = 2, sender = "customer", roles = "store,customer")]
pub struct GetDataArrays {
    /// `dataArrays`.
    pub data_arrays: std::collections::BTreeMap<String, crate::datatypes::data_array_types::DataArrayIdentifier>,
}

/// `Energistics.Etp.v12.Protocol.DataArray.GetDataArraysResponse`.
///
/// ETP protocol 9, message 1, sent by the store. May be split across several messages.
///
/// 1 of 1 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.DataArray.GetDataArraysResponse")]
#[etp(protocol = 9, message_type = 1, sender = "store", roles = "store,customer", multipart)]
pub struct GetDataArraysResponse {
    /// `dataArrays`.
    #[avro(default = {})]
    pub data_arrays: std::collections::BTreeMap<String, crate::datatypes::data_array_types::DataArray>,
}

/// `Energistics.Etp.v12.Protocol.DataArray.GetDataSubarrays`.
///
/// ETP protocol 9, message 3, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.DataArray.GetDataSubarrays")]
#[etp(protocol = 9, message_type = 3, sender = "customer", roles = "store,customer")]
pub struct GetDataSubarrays {
    /// `dataSubarrays`.
    pub data_subarrays: std::collections::BTreeMap<String, crate::datatypes::data_array_types::GetDataSubarraysType>,
}

/// `Energistics.Etp.v12.Protocol.DataArray.GetDataSubarraysResponse`.
///
/// ETP protocol 9, message 8, sent by the store. May be split across several messages.
///
/// 1 of 1 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.DataArray.GetDataSubarraysResponse")]
#[etp(protocol = 9, message_type = 8, sender = "store", roles = "store,customer", multipart)]
pub struct GetDataSubarraysResponse {
    /// `dataSubarrays`.
    #[avro(default = {})]
    pub data_subarrays: std::collections::BTreeMap<String, crate::datatypes::data_array_types::DataArray>,
}

/// `Energistics.Etp.v12.Protocol.DataArray.PutDataArrays`.
///
/// ETP protocol 9, message 4, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.DataArray.PutDataArrays")]
#[etp(protocol = 9, message_type = 4, sender = "customer", roles = "store,customer")]
pub struct PutDataArrays {
    /// `dataArrays`.
    pub data_arrays: std::collections::BTreeMap<String, crate::datatypes::data_array_types::PutDataArraysType>,
}

/// `Energistics.Etp.v12.Protocol.DataArray.PutDataArraysResponse`.
///
/// ETP protocol 9, message 10, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.DataArray.PutDataArraysResponse")]
#[etp(protocol = 9, message_type = 10, sender = "store", roles = "store,customer", multipart)]
pub struct PutDataArraysResponse {
    /// `success`.
    pub success: std::collections::BTreeMap<String, String>,
}

/// `Energistics.Etp.v12.Protocol.DataArray.PutDataSubarrays`.
///
/// ETP protocol 9, message 5, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.DataArray.PutDataSubarrays")]
#[etp(protocol = 9, message_type = 5, sender = "customer", roles = "store,customer")]
pub struct PutDataSubarrays {
    /// `dataSubarrays`.
    pub data_subarrays: std::collections::BTreeMap<String, crate::datatypes::data_array_types::PutDataSubarraysType>,
}

/// `Energistics.Etp.v12.Protocol.DataArray.PutDataSubarraysResponse`.
///
/// ETP protocol 9, message 11, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.DataArray.PutDataSubarraysResponse")]
#[etp(protocol = 9, message_type = 11, sender = "store", roles = "store,customer", multipart)]
pub struct PutDataSubarraysResponse {
    /// `success`.
    pub success: std::collections::BTreeMap<String, String>,
}

/// `Energistics.Etp.v12.Protocol.DataArray.PutUninitializedDataArrays`.
///
/// ETP protocol 9, message 9, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.DataArray.PutUninitializedDataArrays")]
#[etp(protocol = 9, message_type = 9, sender = "customer", roles = "store,customer")]
pub struct PutUninitializedDataArrays {
    /// `dataArrays`.
    pub data_arrays: std::collections::BTreeMap<String, crate::datatypes::data_array_types::PutUninitializedDataArrayType>,
}

/// `Energistics.Etp.v12.Protocol.DataArray.PutUninitializedDataArraysResponse`.
///
/// ETP protocol 9, message 12, sent by the store. May be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Energistics.Etp.v12.Protocol.DataArray.PutUninitializedDataArraysResponse")]
#[etp(protocol = 9, message_type = 12, sender = "store", roles = "store,customer", multipart)]
pub struct PutUninitializedDataArraysResponse {
    /// `success`.
    pub success: std::collections::BTreeMap<String, String>,
}

