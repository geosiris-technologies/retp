// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 340bb1bd2c3610d260d81c9371284871112e00b0ef22a7e36d04310dc770a1f0
//! `datatypes::object` - generated from the ETP v1.2 Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.

#[allow(unused_imports)]
use retp_avro::{Avro, EtpMessage};

/// `Energistics.Etp.v12.Datatypes.Object.ActiveStatusKind`.
///
/// The variant order **is** the encoding: an Avro enumeration travels as
/// its ordinal. `Default` is the first symbol: the schema declares none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Object.ActiveStatusKind")]
pub enum ActiveStatusKind {
    /// `Active`.
    Active,
    /// `Inactive`.
    Inactive,
}

/// `Energistics.Etp.v12.Datatypes.Object.ChangeAnnotation`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Object.ChangeAnnotation")]
pub struct ChangeAnnotation {
    /// `changeTime`.
    pub change_time: i64,
    /// `interval`.
    pub interval: crate::datatypes::object::IndexInterval,
}

/// `Energistics.Etp.v12.Datatypes.Object.ChangeResponseInfo`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Object.ChangeResponseInfo")]
pub struct ChangeResponseInfo {
    /// `responseTimestamp`.
    pub response_timestamp: i64,
    /// `changes`.
    pub changes: std::collections::BTreeMap<String, Vec<crate::datatypes::object::ChangeAnnotation>>,
}

/// `Energistics.Etp.v12.Datatypes.Object.ContextInfo`.
///
/// 3 of 6 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Object.ContextInfo")]
pub struct ContextInfo {
    /// `uri`.
    pub uri: String,
    /// `depth`.
    pub depth: i32,
    /// `dataObjectTypes`.
    #[avro(default = [])]
    pub data_object_types: Vec<String>,
    /// `navigableEdges`.
    pub navigable_edges: crate::datatypes::object::RelationshipKind,
    /// `includeSecondaryTargets`.
    #[avro(default = false)]
    pub include_secondary_targets: bool,
    /// `includeSecondarySources`.
    #[avro(default = false)]
    pub include_secondary_sources: bool,
}

/// `Energistics.Etp.v12.Datatypes.Object.ContextScopeKind`.
///
/// The variant order **is** the encoding: an Avro enumeration travels as
/// its ordinal. `Default` is the first symbol: the schema declares none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Object.ContextScopeKind")]
pub enum ContextScopeKind {
    /// `self`.
    #[avro(symbol = "self")]
    Self_,
    /// `sources`.
    #[avro(symbol = "sources")]
    Sources,
    /// `targets`.
    #[avro(symbol = "targets")]
    Targets,
    /// `sourcesOrSelf`.
    #[avro(symbol = "sourcesOrSelf")]
    SourcesOrSelf,
    /// `targetsOrSelf`.
    #[avro(symbol = "targetsOrSelf")]
    TargetsOrSelf,
}

/// `Energistics.Etp.v12.Datatypes.Object.DataObject`.
///
/// 2 of 4 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Object.DataObject")]
pub struct DataObject {
    /// `resource`.
    pub resource: crate::datatypes::object::Resource,
    /// `format`.
    #[avro(default = "xml")]
    pub format: String,
    /// `blobId`.
    pub blob_id: Option<crate::datatypes::Uuid>,
    /// `data`.
    #[avro(default = "")]
    pub data: Vec<u8>,
}

/// `Energistics.Etp.v12.Datatypes.Object.Dataspace`.
///
/// 2 of 5 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Object.Dataspace")]
pub struct Dataspace {
    /// `uri`.
    pub uri: String,
    /// `path`.
    #[avro(default = "")]
    pub path: String,
    /// `storeLastWrite`.
    pub store_last_write: i64,
    /// `storeCreated`.
    pub store_created: i64,
    /// `customData`.
    #[avro(default = {})]
    pub custom_data: std::collections::BTreeMap<String, crate::datatypes::DataValue>,
}

/// `Energistics.Etp.v12.Datatypes.Object.DeletedResource`.
///
/// 1 of 3 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Object.DeletedResource")]
pub struct DeletedResource {
    /// `uri`.
    pub uri: String,
    /// `deletedTime`.
    pub deleted_time: i64,
    /// `customData`.
    #[avro(default = {})]
    pub custom_data: std::collections::BTreeMap<String, crate::datatypes::DataValue>,
}

/// `Energistics.Etp.v12.Datatypes.Object.Edge`.
///
/// 1 of 4 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Object.Edge")]
pub struct Edge {
    /// `sourceUri`.
    pub source_uri: String,
    /// `targetUri`.
    pub target_uri: String,
    /// `relationshipKind`.
    pub relationship_kind: crate::datatypes::object::RelationshipKind,
    /// `customData`.
    #[avro(default = {})]
    pub custom_data: std::collections::BTreeMap<String, crate::datatypes::DataValue>,
}

/// `Energistics.Etp.v12.Datatypes.Object.IndexInterval`.
///
/// 1 of 4 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Object.IndexInterval")]
pub struct IndexInterval {
    /// `startIndex`.
    pub start_index: crate::datatypes::IndexValue,
    /// `endIndex`.
    pub end_index: crate::datatypes::IndexValue,
    /// `uom`.
    pub uom: String,
    /// `depthDatum`.
    #[avro(default = "")]
    pub depth_datum: String,
}

/// `Energistics.Etp.v12.Datatypes.Object.ObjectChange`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Object.ObjectChange")]
pub struct ObjectChange {
    /// `changeKind`.
    pub change_kind: crate::datatypes::object::ObjectChangeKind,
    /// `changeTime`.
    pub change_time: i64,
    /// `dataObject`.
    pub data_object: crate::datatypes::object::DataObject,
}

/// `Energistics.Etp.v12.Datatypes.Object.ObjectChangeKind`.
///
/// The variant order **is** the encoding: an Avro enumeration travels as
/// its ordinal. `Default` is the first symbol: the schema declares none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Object.ObjectChangeKind")]
pub enum ObjectChangeKind {
    /// `insert`.
    #[avro(symbol = "insert")]
    Insert,
    /// `update`.
    #[avro(symbol = "update")]
    Update,
    /// `authorized`.
    #[avro(symbol = "authorized")]
    Authorized,
    /// `joined`.
    #[avro(symbol = "joined")]
    Joined,
    /// `unjoined`.
    #[avro(symbol = "unjoined")]
    Unjoined,
    /// `joinedSubscription`.
    #[avro(symbol = "joinedSubscription")]
    JoinedSubscription,
    /// `unjoinedSubscription`.
    #[avro(symbol = "unjoinedSubscription")]
    UnjoinedSubscription,
}

/// `Energistics.Etp.v12.Datatypes.Object.ObjectPart`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Object.ObjectPart")]
pub struct ObjectPart {
    /// `uid`.
    pub uid: String,
    /// `data`.
    pub data: Vec<u8>,
}

/// `Energistics.Etp.v12.Datatypes.Object.PartsMetadataInfo`.
///
/// 1 of 4 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Object.PartsMetadataInfo")]
pub struct PartsMetadataInfo {
    /// `uri`.
    pub uri: String,
    /// `name`.
    pub name: String,
    /// `index`.
    pub index: crate::datatypes::channel_data::IndexMetadataRecord,
    /// `customData`.
    #[avro(default = {})]
    pub custom_data: std::collections::BTreeMap<String, crate::datatypes::DataValue>,
}

/// `Energistics.Etp.v12.Datatypes.Object.PutResponse`.
///
/// 4 of 4 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Object.PutResponse")]
pub struct PutResponse {
    /// `createdContainedObjectUris`.
    #[avro(default = [])]
    pub created_contained_object_uris: Vec<String>,
    /// `deletedContainedObjectUris`.
    #[avro(default = [])]
    pub deleted_contained_object_uris: Vec<String>,
    /// `joinedContainedObjectUris`.
    #[avro(default = [])]
    pub joined_contained_object_uris: Vec<String>,
    /// `unjoinedContainedObjectUris`.
    #[avro(default = [])]
    pub unjoined_contained_object_uris: Vec<String>,
}

/// `Energistics.Etp.v12.Datatypes.Object.RelationshipKind`.
///
/// The variant order **is** the encoding: an Avro enumeration travels as
/// its ordinal. `Default` is the first symbol: the schema declares none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Object.RelationshipKind")]
pub enum RelationshipKind {
    /// `Primary`.
    Primary,
    /// `Secondary`.
    Secondary,
    /// `Both`.
    Both,
}

/// `Energistics.Etp.v12.Datatypes.Object.Resource`.
///
/// 4 of 10 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Object.Resource")]
pub struct Resource {
    /// `uri`.
    pub uri: String,
    /// `alternateUris`.
    #[avro(default = [])]
    pub alternate_uris: Vec<String>,
    /// `name`.
    pub name: String,
    /// `sourceCount`.
    #[avro(default = null)]
    pub source_count: Option<i32>,
    /// `targetCount`.
    #[avro(default = null)]
    pub target_count: Option<i32>,
    /// `lastChanged`.
    pub last_changed: i64,
    /// `storeLastWrite`.
    pub store_last_write: i64,
    /// `storeCreated`.
    pub store_created: i64,
    /// `activeStatus`.
    pub active_status: crate::datatypes::object::ActiveStatusKind,
    /// `customData`.
    #[avro(default = {})]
    pub custom_data: std::collections::BTreeMap<String, crate::datatypes::DataValue>,
}

/// `Energistics.Etp.v12.Datatypes.Object.SubscriptionInfo`.
///
/// 1 of 5 fields carry a default in the schema; the rest take the Avro zero.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Object.SubscriptionInfo")]
pub struct SubscriptionInfo {
    /// `context`.
    pub context: crate::datatypes::object::ContextInfo,
    /// `scope`.
    pub scope: crate::datatypes::object::ContextScopeKind,
    /// `requestUuid`.
    pub request_uuid: crate::datatypes::Uuid,
    /// `includeObjectData`.
    pub include_object_data: bool,
    /// `format`.
    #[avro(default = "xml")]
    pub format: String,
}

/// `Energistics.Etp.v12.Datatypes.Object.SupportedType`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Energistics.Etp.v12.Datatypes.Object.SupportedType")]
pub struct SupportedType {
    /// `dataObjectType`.
    pub data_object_type: String,
    /// `objectCount`.
    pub object_count: Option<i32>,
    /// `relationshipKind`.
    pub relationship_kind: crate::datatypes::object::RelationshipKind,
}

