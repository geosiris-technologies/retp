// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 84bd808714c5d32f2b7e91d897b5677012f16bafd221694662c8058e98c13fdc
//! `datatypes::channel_data` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Datatypes.ChannelData.ChannelChangeRequestInfo`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ChannelData.ChannelChangeRequestInfo")]
pub struct ChannelChangeRequestInfo {
    /// `sinceChangeTime`.
    pub since_change_time: i64,
    /// `channelIds`.
    pub channel_ids: Vec<i64>,
}

/// `Energistics.Etp.v12.Datatypes.ChannelData.ChannelDataKind`.
///
/// The variant order **is** the encoding: an Avro enumeration travels as
/// its ordinal. `Default` is the first symbol: the schema declares none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ChannelData.ChannelDataKind")]
pub enum ChannelDataKind {
    /// `DateTime`.
    DateTime,
    /// `ElapsedTime`.
    ElapsedTime,
    /// `MeasuredDepth`.
    MeasuredDepth,
    /// `PassIndexedDepth`.
    PassIndexedDepth,
    /// `TrueVerticalDepth`.
    TrueVerticalDepth,
    /// `typeBoolean`.
    #[avro(symbol = "typeBoolean")]
    TypeBoolean,
    /// `typeInt`.
    #[avro(symbol = "typeInt")]
    TypeInt,
    /// `typeLong`.
    #[avro(symbol = "typeLong")]
    TypeLong,
    /// `typeFloat`.
    #[avro(symbol = "typeFloat")]
    TypeFloat,
    /// `typeDouble`.
    #[avro(symbol = "typeDouble")]
    TypeDouble,
    /// `typeString`.
    #[avro(symbol = "typeString")]
    TypeString,
    /// `typeBytes`.
    #[avro(symbol = "typeBytes")]
    TypeBytes,
}

/// `Energistics.Etp.v12.Datatypes.ChannelData.ChannelIndexKind`.
///
/// The variant order **is** the encoding: an Avro enumeration travels as
/// its ordinal. `Default` is the first symbol: the schema declares none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ChannelData.ChannelIndexKind")]
pub enum ChannelIndexKind {
    /// `DateTime`.
    DateTime,
    /// `ElapsedTime`.
    ElapsedTime,
    /// `MeasuredDepth`.
    MeasuredDepth,
    /// `TrueVerticalDepth`.
    TrueVerticalDepth,
    /// `PassIndexedDepth`.
    PassIndexedDepth,
    /// `Pressure`.
    Pressure,
    /// `Temperature`.
    Temperature,
    /// `Scalar`.
    Scalar,
}

/// `Energistics.Etp.v12.Datatypes.ChannelData.ChannelMetadataRecord`.
///
/// 2 of 13 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ChannelData.ChannelMetadataRecord")]
pub struct ChannelMetadataRecord {
    /// `uri`.
    pub uri: String,
    /// `id`.
    pub id: i64,
    /// `indexes`.
    pub indexes: Vec<crate::datatypes::channel_data::IndexMetadataRecord>,
    /// `channelName`.
    pub channel_name: String,
    /// `dataKind`.
    pub data_kind: crate::datatypes::channel_data::ChannelDataKind,
    /// `uom`.
    pub uom: String,
    /// `depthDatum`.
    pub depth_datum: String,
    /// `channelClassUri`.
    pub channel_class_uri: String,
    /// `status`.
    pub status: crate::datatypes::object::ActiveStatusKind,
    /// `source`.
    pub source: String,
    /// `axisVectorLengths`.
    pub axis_vector_lengths: Vec<i32>,
    /// `attributeMetadata`.
    #[avro(default = [])]
    pub attribute_metadata: Vec<crate::datatypes::AttributeMetadataRecord>,
    /// `customData`.
    #[avro(default = {})]
    pub custom_data: std::collections::BTreeMap<String, crate::datatypes::DataValue>,
}

/// `Energistics.Etp.v12.Datatypes.ChannelData.ChannelRangeInfo`.
///
/// 1 of 3 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ChannelData.ChannelRangeInfo")]
pub struct ChannelRangeInfo {
    /// `channelIds`.
    pub channel_ids: Vec<i64>,
    /// `interval`.
    pub interval: crate::datatypes::object::IndexInterval,
    /// `secondaryIntervals`.
    #[avro(default = [])]
    pub secondary_intervals: Vec<crate::datatypes::object::IndexInterval>,
}

/// `Energistics.Etp.v12.Datatypes.ChannelData.ChannelSubscribeInfo`.
///
/// 1 of 4 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ChannelData.ChannelSubscribeInfo")]
pub struct ChannelSubscribeInfo {
    /// `channelId`.
    pub channel_id: i64,
    /// `startIndex`.
    pub start_index: crate::datatypes::IndexValue,
    /// `dataChanges`.
    #[avro(default = true)]
    pub data_changes: bool,
    /// `requestLatestIndexCount`.
    pub request_latest_index_count: Option<i32>,
}

/// `Energistics.Etp.v12.Datatypes.ChannelData.DataItem`.
///
/// 1 of 4 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ChannelData.DataItem")]
pub struct DataItem {
    /// `channelId`.
    pub channel_id: i64,
    /// `indexes`.
    pub indexes: Vec<crate::datatypes::IndexValue>,
    /// `value`.
    pub value: crate::datatypes::DataValue,
    /// `valueAttributes`.
    #[avro(default = [])]
    pub value_attributes: Vec<crate::datatypes::DataAttribute>,
}

/// `Energistics.Etp.v12.Datatypes.ChannelData.FrameChannelMetadataRecord`.
///
/// 2 of 11 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ChannelData.FrameChannelMetadataRecord")]
pub struct FrameChannelMetadataRecord {
    /// `uri`.
    pub uri: String,
    /// `channelName`.
    pub channel_name: String,
    /// `dataKind`.
    pub data_kind: crate::datatypes::channel_data::ChannelDataKind,
    /// `uom`.
    pub uom: String,
    /// `depthDatum`.
    pub depth_datum: String,
    /// `channelPropertyKindUri`.
    pub channel_property_kind_uri: String,
    /// `status`.
    pub status: crate::datatypes::object::ActiveStatusKind,
    /// `source`.
    pub source: String,
    /// `axisVectorLengths`.
    pub axis_vector_lengths: Vec<i32>,
    /// `attributeMetadata`.
    #[avro(default = [])]
    pub attribute_metadata: Vec<crate::datatypes::AttributeMetadataRecord>,
    /// `customData`.
    #[avro(default = {})]
    pub custom_data: std::collections::BTreeMap<String, crate::datatypes::DataValue>,
}

/// `Energistics.Etp.v12.Datatypes.ChannelData.FramePoint`.
///
/// 1 of 2 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ChannelData.FramePoint")]
pub struct FramePoint {
    /// `value`.
    pub value: crate::datatypes::DataValue,
    /// `valueAttributes`.
    #[avro(default = [])]
    pub value_attributes: Vec<crate::datatypes::DataAttribute>,
}

/// `Energistics.Etp.v12.Datatypes.ChannelData.FrameRow`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ChannelData.FrameRow")]
pub struct FrameRow {
    /// `indexes`.
    pub indexes: Vec<crate::datatypes::IndexValue>,
    /// `points`.
    pub points: Vec<crate::datatypes::channel_data::FramePoint>,
}

/// `Energistics.Etp.v12.Datatypes.ChannelData.IndexDirection`.
///
/// The variant order **is** the encoding: an Avro enumeration travels as
/// its ordinal. `Default` is the first symbol: the schema declares none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ChannelData.IndexDirection")]
pub enum IndexDirection {
    /// `Increasing`.
    Increasing,
    /// `Decreasing`.
    Decreasing,
    /// `Unordered`.
    Unordered,
}

/// `Energistics.Etp.v12.Datatypes.ChannelData.IndexMetadataRecord`.
///
/// 5 of 8 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ChannelData.IndexMetadataRecord")]
pub struct IndexMetadataRecord {
    /// `indexKind`.
    #[avro(default = DateTime)]
    pub index_kind: crate::datatypes::channel_data::ChannelIndexKind,
    /// `interval`.
    pub interval: crate::datatypes::object::IndexInterval,
    /// `direction`.
    #[avro(default = Increasing)]
    pub direction: crate::datatypes::channel_data::IndexDirection,
    /// `name`.
    #[avro(default = "")]
    pub name: String,
    /// `uom`.
    pub uom: String,
    /// `depthDatum`.
    #[avro(default = "")]
    pub depth_datum: String,
    /// `indexPropertyKindUri`.
    pub index_property_kind_uri: String,
    /// `filterable`.
    #[avro(default = true)]
    pub filterable: bool,
}

/// `Energistics.Etp.v12.Datatypes.ChannelData.OpenChannelInfo`.
///
/// 2 of 3 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ChannelData.OpenChannelInfo")]
pub struct OpenChannelInfo {
    /// `metadata`.
    pub metadata: crate::datatypes::channel_data::ChannelMetadataRecord,
    /// `preferRealtime`.
    #[avro(default = true)]
    pub prefer_realtime: bool,
    /// `dataChanges`.
    #[avro(default = true)]
    pub data_changes: bool,
}

/// `Energistics.Etp.v12.Datatypes.ChannelData.PassDirection`.
///
/// The variant order **is** the encoding: an Avro enumeration travels as
/// its ordinal. `Default` is the first symbol: the schema declares none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ChannelData.PassDirection")]
pub enum PassDirection {
    /// `Up`.
    Up,
    /// `HoldingSteady`.
    HoldingSteady,
    /// `Down`.
    Down,
}

/// `Energistics.Etp.v12.Datatypes.ChannelData.PassIndexedDepth`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ChannelData.PassIndexedDepth")]
pub struct PassIndexedDepth {
    /// `pass`.
    pub pass: i64,
    /// `direction`.
    pub direction: crate::datatypes::channel_data::PassDirection,
    /// `depth`.
    pub depth: f64,
}

/// `Energistics.Etp.v12.Datatypes.ChannelData.TruncateInfo`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ChannelData.TruncateInfo")]
pub struct TruncateInfo {
    /// `channelId`.
    pub channel_id: i64,
    /// `newEndIndex`.
    pub new_end_index: crate::datatypes::IndexValue,
}

