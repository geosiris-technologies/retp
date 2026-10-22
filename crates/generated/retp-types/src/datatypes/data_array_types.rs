// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 84bd808714c5d32f2b7e91d897b5677012f16bafd221694662c8058e98c13fdc
//! `datatypes::data_array_types` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Datatypes.DataArrayTypes.DataArray`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.DataArrayTypes.DataArray")]
pub struct DataArray {
    /// `dimensions`.
    pub dimensions: Vec<i64>,
    /// `data`.
    pub data: crate::datatypes::AnyArray,
}

/// `Energistics.Etp.v12.Datatypes.DataArrayTypes.DataArrayIdentifier`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.DataArrayTypes.DataArrayIdentifier")]
pub struct DataArrayIdentifier {
    /// `uri`.
    pub uri: String,
    /// `pathInResource`.
    pub path_in_resource: String,
}

/// `Energistics.Etp.v12.Datatypes.DataArrayTypes.DataArrayMetadata`.
///
/// 2 of 7 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.DataArrayTypes.DataArrayMetadata")]
pub struct DataArrayMetadata {
    /// `dimensions`.
    pub dimensions: Vec<i64>,
    /// `preferredSubarrayDimensions`.
    #[avro(default = [])]
    pub preferred_subarray_dimensions: Vec<i64>,
    /// `transportArrayType`.
    pub transport_array_type: crate::datatypes::AnyArrayType,
    /// `logicalArrayType`.
    pub logical_array_type: crate::datatypes::AnyLogicalArrayType,
    /// `storeLastWrite`.
    pub store_last_write: i64,
    /// `storeCreated`.
    pub store_created: i64,
    /// `customData`.
    #[avro(default = {})]
    pub custom_data: std::collections::BTreeMap<String, crate::datatypes::DataValue>,
}

/// `Energistics.Etp.v12.Datatypes.DataArrayTypes.GetDataSubarraysType`.
///
/// 2 of 3 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.DataArrayTypes.GetDataSubarraysType")]
pub struct GetDataSubarraysType {
    /// `uid`.
    pub uid: crate::datatypes::data_array_types::DataArrayIdentifier,
    /// `starts`.
    #[avro(default = [])]
    pub starts: Vec<i64>,
    /// `counts`.
    #[avro(default = [])]
    pub counts: Vec<i64>,
}

/// `Energistics.Etp.v12.Datatypes.DataArrayTypes.PutDataArraysType`.
///
/// 1 of 3 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.DataArrayTypes.PutDataArraysType")]
pub struct PutDataArraysType {
    /// `uid`.
    pub uid: crate::datatypes::data_array_types::DataArrayIdentifier,
    /// `array`.
    pub array: crate::datatypes::data_array_types::DataArray,
    /// `customData`.
    #[avro(default = {})]
    pub custom_data: std::collections::BTreeMap<String, crate::datatypes::DataValue>,
}

/// `Energistics.Etp.v12.Datatypes.DataArrayTypes.PutDataSubarraysType`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.DataArrayTypes.PutDataSubarraysType")]
pub struct PutDataSubarraysType {
    /// `uid`.
    pub uid: crate::datatypes::data_array_types::DataArrayIdentifier,
    /// `data`.
    pub data: crate::datatypes::AnyArray,
    /// `starts`.
    pub starts: Vec<i64>,
    /// `counts`.
    pub counts: Vec<i64>,
}

/// `Energistics.Etp.v12.Datatypes.DataArrayTypes.PutUninitializedDataArrayType`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.DataArrayTypes.PutUninitializedDataArrayType")]
pub struct PutUninitializedDataArrayType {
    /// `uid`.
    pub uid: crate::datatypes::data_array_types::DataArrayIdentifier,
    /// `metadata`.
    pub metadata: crate::datatypes::data_array_types::DataArrayMetadata,
}

