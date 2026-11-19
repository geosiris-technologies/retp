// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 84bd808714c5d32f2b7e91d897b5677012f16bafd221694662c8058e98c13fdc
//! `datatypes` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

pub mod channel_data;
pub mod data_array_types;
pub mod object;

/// `Energistics.Etp.v12.Datatypes.AnyArray`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.AnyArray")]
pub struct AnyArray {
    /// `item`.
    pub item: crate::datatypes::AnyArrayItem,
}

/// The union at `Energistics.Etp.v12.Datatypes.AnyArray.item`.
///
/// The variant order **is** the encoding: a union travels as its branch
/// index, so reordering these would change the wire format. `Default` is
/// the first branch, the one an Avro default applies to.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(union, name = "Energistics.Etp.v12.Datatypes.AnyArray.item")]
pub enum AnyArrayItem {
    /// The `array_of_boolean` branch.
    ArrayOfBoolean(crate::datatypes::ArrayOfBoolean),
    /// The `array_of_int` branch.
    ArrayOfInt(crate::datatypes::ArrayOfInt),
    /// The `array_of_long` branch.
    ArrayOfLong(crate::datatypes::ArrayOfLong),
    /// The `array_of_float` branch.
    ArrayOfFloat(crate::datatypes::ArrayOfFloat),
    /// The `array_of_double` branch.
    ArrayOfDouble(crate::datatypes::ArrayOfDouble),
    /// The `array_of_string` branch.
    ArrayOfString(crate::datatypes::ArrayOfString),
    /// The `bytes` branch.
    Bytes(Vec<u8>),
}

/// `Energistics.Etp.v12.Datatypes.AnyArrayType`.
///
/// The variant order **is** the encoding: an Avro enumeration travels as
/// its ordinal. `Default` is the first symbol: the schema declares none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.AnyArrayType")]
pub enum AnyArrayType {
    /// `arrayOfBoolean`.
    #[avro(symbol = "arrayOfBoolean")]
    ArrayOfBoolean,
    /// `arrayOfInt`.
    #[avro(symbol = "arrayOfInt")]
    ArrayOfInt,
    /// `arrayOfLong`.
    #[avro(symbol = "arrayOfLong")]
    ArrayOfLong,
    /// `arrayOfFloat`.
    #[avro(symbol = "arrayOfFloat")]
    ArrayOfFloat,
    /// `arrayOfDouble`.
    #[avro(symbol = "arrayOfDouble")]
    ArrayOfDouble,
    /// `arrayOfString`.
    #[avro(symbol = "arrayOfString")]
    ArrayOfString,
    /// `bytes`.
    #[avro(symbol = "bytes")]
    Bytes,
}

/// `Energistics.Etp.v12.Datatypes.AnyLogicalArrayType`.
///
/// The variant order **is** the encoding: an Avro enumeration travels as
/// its ordinal. `Default` is the first symbol: the schema declares none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.AnyLogicalArrayType")]
pub enum AnyLogicalArrayType {
    /// `arrayOfBoolean`.
    #[avro(symbol = "arrayOfBoolean")]
    ArrayOfBoolean,
    /// `arrayOfInt8`.
    #[avro(symbol = "arrayOfInt8")]
    ArrayOfInt8,
    /// `arrayOfUInt8`.
    #[avro(symbol = "arrayOfUInt8")]
    ArrayOfUInt8,
    /// `arrayOfInt16LE`.
    #[avro(symbol = "arrayOfInt16LE")]
    ArrayOfInt16Le,
    /// `arrayOfInt32LE`.
    #[avro(symbol = "arrayOfInt32LE")]
    ArrayOfInt32Le,
    /// `arrayOfInt64LE`.
    #[avro(symbol = "arrayOfInt64LE")]
    ArrayOfInt64Le,
    /// `arrayOfUInt16LE`.
    #[avro(symbol = "arrayOfUInt16LE")]
    ArrayOfUInt16Le,
    /// `arrayOfUInt32LE`.
    #[avro(symbol = "arrayOfUInt32LE")]
    ArrayOfUInt32Le,
    /// `arrayOfUInt64LE`.
    #[avro(symbol = "arrayOfUInt64LE")]
    ArrayOfUInt64Le,
    /// `arrayOfFloat32LE`.
    #[avro(symbol = "arrayOfFloat32LE")]
    ArrayOfFloat32Le,
    /// `arrayOfDouble64LE`.
    #[avro(symbol = "arrayOfDouble64LE")]
    ArrayOfDouble64Le,
    /// `arrayOfInt16BE`.
    #[avro(symbol = "arrayOfInt16BE")]
    ArrayOfInt16Be,
    /// `arrayOfInt32BE`.
    #[avro(symbol = "arrayOfInt32BE")]
    ArrayOfInt32Be,
    /// `arrayOfInt64BE`.
    #[avro(symbol = "arrayOfInt64BE")]
    ArrayOfInt64Be,
    /// `arrayOfUInt16BE`.
    #[avro(symbol = "arrayOfUInt16BE")]
    ArrayOfUInt16Be,
    /// `arrayOfUInt32BE`.
    #[avro(symbol = "arrayOfUInt32BE")]
    ArrayOfUInt32Be,
    /// `arrayOfUInt64BE`.
    #[avro(symbol = "arrayOfUInt64BE")]
    ArrayOfUInt64Be,
    /// `arrayOfFloat32BE`.
    #[avro(symbol = "arrayOfFloat32BE")]
    ArrayOfFloat32Be,
    /// `arrayOfDouble64BE`.
    #[avro(symbol = "arrayOfDouble64BE")]
    ArrayOfDouble64Be,
    /// `arrayOfString`.
    #[avro(symbol = "arrayOfString")]
    ArrayOfString,
    /// `arrayOfCustom`.
    #[avro(symbol = "arrayOfCustom")]
    ArrayOfCustom,
}

/// `Energistics.Etp.v12.Datatypes.AnySparseArray`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.AnySparseArray")]
pub struct AnySparseArray {
    /// `slices`.
    pub slices: Vec<crate::datatypes::AnySubarray>,
}

/// `Energistics.Etp.v12.Datatypes.AnySubarray`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.AnySubarray")]
pub struct AnySubarray {
    /// `start`.
    pub start: i64,
    /// `slice`.
    pub slice: crate::datatypes::AnyArray,
}

/// `Energistics.Etp.v12.Datatypes.ArrayOfBoolean`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ArrayOfBoolean")]
pub struct ArrayOfBoolean {
    /// `values`.
    pub values: Vec<bool>,
}

/// `Energistics.Etp.v12.Datatypes.ArrayOfBytes`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ArrayOfBytes")]
pub struct ArrayOfBytes {
    /// `values`.
    pub values: Vec<Vec<u8>>,
}

/// `Energistics.Etp.v12.Datatypes.ArrayOfDouble`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ArrayOfDouble")]
pub struct ArrayOfDouble {
    /// `values`.
    pub values: Vec<f64>,
}

/// `Energistics.Etp.v12.Datatypes.ArrayOfFloat`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ArrayOfFloat")]
pub struct ArrayOfFloat {
    /// `values`.
    pub values: Vec<f32>,
}

/// `Energistics.Etp.v12.Datatypes.ArrayOfInt`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ArrayOfInt")]
pub struct ArrayOfInt {
    /// `values`.
    pub values: Vec<i32>,
}

/// `Energistics.Etp.v12.Datatypes.ArrayOfLong`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ArrayOfLong")]
pub struct ArrayOfLong {
    /// `values`.
    pub values: Vec<i64>,
}

/// `Energistics.Etp.v12.Datatypes.ArrayOfNullableBoolean`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ArrayOfNullableBoolean")]
pub struct ArrayOfNullableBoolean {
    /// `values`.
    pub values: Vec<Option<bool>>,
}

/// `Energistics.Etp.v12.Datatypes.ArrayOfNullableInt`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ArrayOfNullableInt")]
pub struct ArrayOfNullableInt {
    /// `values`.
    pub values: Vec<Option<i32>>,
}

/// `Energistics.Etp.v12.Datatypes.ArrayOfNullableLong`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ArrayOfNullableLong")]
pub struct ArrayOfNullableLong {
    /// `values`.
    pub values: Vec<Option<i64>>,
}

/// `Energistics.Etp.v12.Datatypes.ArrayOfString`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ArrayOfString")]
pub struct ArrayOfString {
    /// `values`.
    pub values: Vec<String>,
}

/// `Energistics.Etp.v12.Datatypes.AttributeMetadataRecord`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.AttributeMetadataRecord")]
pub struct AttributeMetadataRecord {
    /// `attributeId`.
    pub attribute_id: i32,
    /// `attributeName`.
    pub attribute_name: String,
    /// `dataKind`.
    pub data_kind: crate::datatypes::channel_data::ChannelDataKind,
    /// `uom`.
    pub uom: String,
    /// `depthDatum`.
    pub depth_datum: String,
    /// `attributePropertyKindUri`.
    pub attribute_property_kind_uri: String,
    /// `axisVectorLengths`.
    pub axis_vector_lengths: Vec<i32>,
}

/// `Energistics.Etp.v12.Datatypes.Contact`.
///
/// 4 of 4 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Contact")]
pub struct Contact {
    /// `organizationName`.
    #[avro(default = "")]
    pub organization_name: String,
    /// `contactName`.
    #[avro(default = "")]
    pub contact_name: String,
    /// `contactPhone`.
    #[avro(default = "")]
    pub contact_phone: String,
    /// `contactEmail`.
    #[avro(default = "")]
    pub contact_email: String,
}

/// `Energistics.Etp.v12.Datatypes.DataAttribute`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.DataAttribute")]
pub struct DataAttribute {
    /// `attributeId`.
    pub attribute_id: i32,
    /// `attributeValue`.
    pub attribute_value: crate::datatypes::DataValue,
}

/// `Energistics.Etp.v12.Datatypes.DataObjectCapabilityKind`.
///
/// The variant order **is** the encoding: an Avro enumeration travels as
/// its ordinal. `Default` is the first symbol: the schema declares none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.DataObjectCapabilityKind")]
pub enum DataObjectCapabilityKind {
    /// `ActiveTimeoutPeriod`.
    ActiveTimeoutPeriod,
    /// `MaxContainedDataObjectCount`.
    MaxContainedDataObjectCount,
    /// `MaxDataObjectSize`.
    MaxDataObjectSize,
    /// `OrphanedChildrenPrunedOnDelete`.
    OrphanedChildrenPrunedOnDelete,
    /// `SupportsGet`.
    SupportsGet,
    /// `SupportsPut`.
    SupportsPut,
    /// `SupportsDelete`.
    SupportsDelete,
    /// `MaxSecondaryIndexCount`.
    MaxSecondaryIndexCount,
}

/// `Energistics.Etp.v12.Datatypes.DataValue`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.DataValue")]
pub struct DataValue {
    /// `item`.
    pub item: crate::datatypes::DataValueItem,
}

/// The union at `Energistics.Etp.v12.Datatypes.DataValue.item`.
///
/// The variant order **is** the encoding: a union travels as its branch
/// index, so reordering these would change the wire format. `Default` is
/// the first branch, the one an Avro default applies to.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(union, name = "Energistics.Etp.v12.Datatypes.DataValue.item")]
pub enum DataValueItem {
    /// The `null` branch.
    Null,
    /// The `boolean` branch.
    Boolean(bool),
    /// The `int` branch.
    Int(i32),
    /// The `long` branch.
    Long(i64),
    /// The `float` branch.
    Float(f32),
    /// The `double` branch.
    Double(f64),
    /// The `string` branch.
    String(String),
    /// The `array_of_boolean` branch.
    ArrayOfBoolean(crate::datatypes::ArrayOfBoolean),
    /// The `array_of_nullable_boolean` branch.
    ArrayOfNullableBoolean(crate::datatypes::ArrayOfNullableBoolean),
    /// The `array_of_int` branch.
    ArrayOfInt(crate::datatypes::ArrayOfInt),
    /// The `array_of_nullable_int` branch.
    ArrayOfNullableInt(crate::datatypes::ArrayOfNullableInt),
    /// The `array_of_long` branch.
    ArrayOfLong(crate::datatypes::ArrayOfLong),
    /// The `array_of_nullable_long` branch.
    ArrayOfNullableLong(crate::datatypes::ArrayOfNullableLong),
    /// The `array_of_float` branch.
    ArrayOfFloat(crate::datatypes::ArrayOfFloat),
    /// The `array_of_double` branch.
    ArrayOfDouble(crate::datatypes::ArrayOfDouble),
    /// The `array_of_string` branch.
    ArrayOfString(crate::datatypes::ArrayOfString),
    /// The `array_of_bytes` branch.
    ArrayOfBytes(crate::datatypes::ArrayOfBytes),
    /// The `bytes` branch.
    Bytes(Vec<u8>),
    /// The `any_sparse_array` branch.
    AnySparseArray(crate::datatypes::AnySparseArray),
}

/// `Energistics.Etp.v12.Datatypes.EndpointCapabilityKind`.
///
/// The variant order **is** the encoding: an Avro enumeration travels as
/// its ordinal. `Default` is the first symbol: the schema declares none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.EndpointCapabilityKind")]
pub enum EndpointCapabilityKind {
    /// `ActiveTimeoutPeriod`.
    ActiveTimeoutPeriod,
    /// `AuthorizationDetails`.
    AuthorizationDetails,
    /// `ChangePropagationPeriod`.
    ChangePropagationPeriod,
    /// `ChangeRetentionPeriod`.
    ChangeRetentionPeriod,
    /// `MaxConcurrentMultipart`.
    MaxConcurrentMultipart,
    /// `MaxDataObjectSize`.
    MaxDataObjectSize,
    /// `MaxPartSize`.
    MaxPartSize,
    /// `MaxSessionClientCount`.
    MaxSessionClientCount,
    /// `MaxSessionGlobalCount`.
    MaxSessionGlobalCount,
    /// `MaxWebSocketFramePayloadSize`.
    MaxWebSocketFramePayloadSize,
    /// `MaxWebSocketMessagePayloadSize`.
    MaxWebSocketMessagePayloadSize,
    /// `MultipartMessageTimeoutPeriod`.
    MultipartMessageTimeoutPeriod,
    /// `ResponseTimeoutPeriod`.
    ResponseTimeoutPeriod,
    /// `RequestSessionTimeoutPeriod`.
    RequestSessionTimeoutPeriod,
    /// `SessionEstablishmentTimeoutPeriod`.
    SessionEstablishmentTimeoutPeriod,
    /// `SupportsAlternateRequestUris`.
    SupportsAlternateRequestUris,
    /// `SupportsMessageHeaderExtensions`.
    SupportsMessageHeaderExtensions,
}

/// `Energistics.Etp.v12.Datatypes.ErrorInfo`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ErrorInfo")]
pub struct ErrorInfo {
    /// `message`.
    pub message: String,
    /// `code`.
    pub code: i32,
}

/// `Energistics.Etp.v12.Datatypes.IndexValue`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.IndexValue")]
pub struct IndexValue {
    /// `item`.
    pub item: crate::datatypes::IndexValueItem,
}

/// The union at `Energistics.Etp.v12.Datatypes.IndexValue.item`.
///
/// The variant order **is** the encoding: a union travels as its branch
/// index, so reordering these would change the wire format. `Default` is
/// the first branch, the one an Avro default applies to.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(union, name = "Energistics.Etp.v12.Datatypes.IndexValue.item")]
pub enum IndexValueItem {
    /// The `null` branch.
    Null,
    /// The `long` branch.
    Long(i64),
    /// The `double` branch.
    Double(f64),
    /// The `pass_indexed_depth` branch.
    PassIndexedDepth(crate::datatypes::channel_data::PassIndexedDepth),
}

/// `Energistics.Etp.v12.Datatypes.MessageHeader`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.MessageHeader")]
pub struct MessageHeader {
    /// `protocol`.
    pub protocol: i32,
    /// `messageType`.
    pub message_type: i32,
    /// `correlationId`.
    pub correlation_id: i64,
    /// `messageId`.
    pub message_id: i64,
    /// `messageFlags`.
    pub message_flags: i32,
}

/// `Energistics.Etp.v12.Datatypes.MessageHeaderExtension`.
///
/// 1 of 1 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.MessageHeaderExtension")]
pub struct MessageHeaderExtension {
    /// `extension`.
    #[avro(default = {})]
    pub extension: std::collections::BTreeMap<String, crate::datatypes::DataValue>,
}

/// `Energistics.Etp.v12.Datatypes.Protocol`.
///
/// The variant order **is** the encoding: an Avro enumeration travels as
/// its ordinal. `Default` is the first symbol: the schema declares none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Protocol")]
pub enum Protocol {
    /// `Core`.
    Core,
    /// `ChannelStreaming`.
    ChannelStreaming,
    /// `ChannelDataFrame`.
    ChannelDataFrame,
    /// `Discovery`.
    Discovery,
    /// `Store`.
    Store,
    /// `StoreNotification`.
    StoreNotification,
    /// `GrowingObject`.
    GrowingObject,
    /// `GrowingObjectNotification`.
    GrowingObjectNotification,
    /// `DEPRECATED_8`.
    #[avro(symbol = "DEPRECATED_8")]
    Deprecated8,
    /// `DataArray`.
    DataArray,
    /// `RESERVED_10`.
    #[avro(symbol = "RESERVED_10")]
    Reserved10,
    /// `RESERVED_11`.
    #[avro(symbol = "RESERVED_11")]
    Reserved11,
    /// `RESERVED_12`.
    #[avro(symbol = "RESERVED_12")]
    Reserved12,
    /// `DiscoveryQuery`.
    DiscoveryQuery,
    /// `StoreQuery`.
    StoreQuery,
    /// `RESERVED_15`.
    #[avro(symbol = "RESERVED_15")]
    Reserved15,
    /// `GrowingObjectQuery`.
    GrowingObjectQuery,
    /// `RESERVED_17`.
    #[avro(symbol = "RESERVED_17")]
    Reserved17,
    /// `Transaction`.
    Transaction,
    /// `RESERVED_19`.
    #[avro(symbol = "RESERVED_19")]
    Reserved19,
    /// `RESERVED_20`.
    #[avro(symbol = "RESERVED_20")]
    Reserved20,
    /// `ChannelSubscribe`.
    ChannelSubscribe,
    /// `ChannelDataLoad`.
    ChannelDataLoad,
    /// `RESERVED_23`.
    #[avro(symbol = "RESERVED_23")]
    Reserved23,
    /// `Dataspace`.
    Dataspace,
    /// `SupportedTypes`.
    SupportedTypes,
}

/// `Energistics.Etp.v12.Datatypes.ProtocolCapabilityKind`.
///
/// The variant order **is** the encoding: an Avro enumeration travels as
/// its ordinal. `Default` is the first symbol: the schema declares none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ProtocolCapabilityKind")]
pub enum ProtocolCapabilityKind {
    /// `FrameChangeDetectionPeriod`.
    FrameChangeDetectionPeriod,
    /// `MaxDataArraySize`.
    MaxDataArraySize,
    /// `MaxDataObjectSize`.
    MaxDataObjectSize,
    /// `MaxFrameResponseRowCount`.
    MaxFrameResponseRowCount,
    /// `MaxIndexCount`.
    MaxIndexCount,
    /// `MaxRangeChannelCount`.
    MaxRangeChannelCount,
    /// `MaxRangeDataItemCount`.
    MaxRangeDataItemCount,
    /// `MaxResponseCount`.
    MaxResponseCount,
    /// `MaxStreamingChannelsSessionCount`.
    MaxStreamingChannelsSessionCount,
    /// `MaxSubscriptionSessionCount`.
    MaxSubscriptionSessionCount,
    /// `MaxTransactionCount`.
    MaxTransactionCount,
    /// `SupportsSecondaryIndexFiltering`.
    SupportsSecondaryIndexFiltering,
    /// `TransactionTimeoutPeriod`.
    TransactionTimeoutPeriod,
}

/// `Energistics.Etp.v12.Datatypes.ServerCapabilities`.
///
/// 4 of 9 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.ServerCapabilities")]
pub struct ServerCapabilities {
    /// `applicationName`.
    pub application_name: String,
    /// `applicationVersion`.
    pub application_version: String,
    /// `contactInformation`.
    pub contact_information: crate::datatypes::Contact,
    /// `supportedCompression`.
    #[avro(default = [])]
    pub supported_compression: Vec<String>,
    /// `supportedEncodings`.
    #[avro(default = ["binary"])]
    pub supported_encodings: Vec<String>,
    /// `supportedFormats`.
    #[avro(default = ["xml"])]
    pub supported_formats: Vec<String>,
    /// `supportedDataObjects`.
    pub supported_data_objects: Vec<crate::datatypes::SupportedDataObject>,
    /// `supportedProtocols`.
    pub supported_protocols: Vec<crate::datatypes::SupportedProtocol>,
    /// `endpointCapabilities`.
    #[avro(default = {})]
    pub endpoint_capabilities: std::collections::BTreeMap<String, crate::datatypes::DataValue>,
}

/// `Energistics.Etp.v12.Datatypes.SupportedDataObject`.
///
/// 1 of 2 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.SupportedDataObject")]
pub struct SupportedDataObject {
    /// `qualifiedType`.
    pub qualified_type: String,
    /// `dataObjectCapabilities`.
    #[avro(default = {})]
    pub data_object_capabilities: std::collections::BTreeMap<String, crate::datatypes::DataValue>,
}

/// `Energistics.Etp.v12.Datatypes.SupportedProtocol`.
///
/// 1 of 4 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.SupportedProtocol")]
pub struct SupportedProtocol {
    /// `protocol`.
    pub protocol: i32,
    /// `protocolVersion`.
    pub protocol_version: crate::datatypes::Version,
    /// `role`.
    pub role: String,
    /// `protocolCapabilities`.
    #[avro(default = {})]
    pub protocol_capabilities: std::collections::BTreeMap<String, crate::datatypes::DataValue>,
}

/// `Energistics.Etp.v12.Datatypes.Uuid` - 16 bytes, with no length on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Uuid")]
pub struct Uuid(pub [u8; 16]);

/// `Energistics.Etp.v12.Datatypes.Version`.
///
/// 4 of 4 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Version")]
pub struct Version {
    /// `major`.
    #[avro(default = 0)]
    pub major: i32,
    /// `minor`.
    #[avro(default = 0)]
    pub minor: i32,
    /// `revision`.
    #[avro(default = 0)]
    pub revision: i32,
    /// `patch`.
    #[avro(default = 0)]
    pub patch: i32,
}

