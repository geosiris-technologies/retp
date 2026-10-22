// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 84bd808714c5d32f2b7e91d897b5677012f16bafd221694662c8058e98c13fdc
//! Every record's `Default` against the schema - the test `defaults_match_schema`.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.
//!
//! The expected bytes are **not** generated. `fastavro` wrote them, from the
//! defaults read straight out of each `.avsc` (`retp-tests/tests/
//! fixtures/defaults.py`), while the values here went through the generator,
//! an attribute and the derive. A default lost on the way is a byte that
//! differs.

use std::collections::BTreeMap;
use std::path::PathBuf;

use retp_avro::Avro;

/// How many records the schemas define.
const RECORD_COUNT: usize = 199;

fn entry<T: Avro + Default>() -> (&'static str, Vec<u8>) {
    (T::FULL_NAME, T::default().to_avro())
}

fn generated() -> BTreeMap<&'static str, Vec<u8>> {
    BTreeMap::from([
        entry::<retp_types::datatypes::AnyArray>(),
        entry::<retp_types::datatypes::AnySparseArray>(),
        entry::<retp_types::datatypes::AnySubarray>(),
        entry::<retp_types::datatypes::ArrayOfBoolean>(),
        entry::<retp_types::datatypes::ArrayOfBytes>(),
        entry::<retp_types::datatypes::ArrayOfDouble>(),
        entry::<retp_types::datatypes::ArrayOfFloat>(),
        entry::<retp_types::datatypes::ArrayOfInt>(),
        entry::<retp_types::datatypes::ArrayOfLong>(),
        entry::<retp_types::datatypes::ArrayOfNullableBoolean>(),
        entry::<retp_types::datatypes::ArrayOfNullableInt>(),
        entry::<retp_types::datatypes::ArrayOfNullableLong>(),
        entry::<retp_types::datatypes::ArrayOfString>(),
        entry::<retp_types::datatypes::AttributeMetadataRecord>(),
        entry::<retp_types::datatypes::channel_data::ChannelChangeRequestInfo>(),
        entry::<retp_types::datatypes::channel_data::ChannelMetadataRecord>(),
        entry::<retp_types::datatypes::channel_data::ChannelRangeInfo>(),
        entry::<retp_types::datatypes::channel_data::ChannelSubscribeInfo>(),
        entry::<retp_types::datatypes::channel_data::DataItem>(),
        entry::<retp_types::datatypes::channel_data::FrameChannelMetadataRecord>(),
        entry::<retp_types::datatypes::channel_data::FramePoint>(),
        entry::<retp_types::datatypes::channel_data::FrameRow>(),
        entry::<retp_types::datatypes::channel_data::IndexMetadataRecord>(),
        entry::<retp_types::datatypes::channel_data::OpenChannelInfo>(),
        entry::<retp_types::datatypes::channel_data::PassIndexedDepth>(),
        entry::<retp_types::datatypes::channel_data::TruncateInfo>(),
        entry::<retp_types::datatypes::Contact>(),
        entry::<retp_types::datatypes::data_array_types::DataArray>(),
        entry::<retp_types::datatypes::data_array_types::DataArrayIdentifier>(),
        entry::<retp_types::datatypes::data_array_types::DataArrayMetadata>(),
        entry::<retp_types::datatypes::data_array_types::GetDataSubarraysType>(),
        entry::<retp_types::datatypes::data_array_types::PutDataArraysType>(),
        entry::<retp_types::datatypes::data_array_types::PutDataSubarraysType>(),
        entry::<retp_types::datatypes::data_array_types::PutUninitializedDataArrayType>(),
        entry::<retp_types::datatypes::DataAttribute>(),
        entry::<retp_types::datatypes::DataValue>(),
        entry::<retp_types::datatypes::ErrorInfo>(),
        entry::<retp_types::datatypes::IndexValue>(),
        entry::<retp_types::datatypes::MessageHeader>(),
        entry::<retp_types::datatypes::MessageHeaderExtension>(),
        entry::<retp_types::datatypes::object::ChangeAnnotation>(),
        entry::<retp_types::datatypes::object::ChangeResponseInfo>(),
        entry::<retp_types::datatypes::object::ContextInfo>(),
        entry::<retp_types::datatypes::object::DataObject>(),
        entry::<retp_types::datatypes::object::Dataspace>(),
        entry::<retp_types::datatypes::object::DeletedResource>(),
        entry::<retp_types::datatypes::object::Edge>(),
        entry::<retp_types::datatypes::object::IndexInterval>(),
        entry::<retp_types::datatypes::object::ObjectChange>(),
        entry::<retp_types::datatypes::object::ObjectPart>(),
        entry::<retp_types::datatypes::object::PartsMetadataInfo>(),
        entry::<retp_types::datatypes::object::PutResponse>(),
        entry::<retp_types::datatypes::object::Resource>(),
        entry::<retp_types::datatypes::object::SubscriptionInfo>(),
        entry::<retp_types::datatypes::object::SupportedType>(),
        entry::<retp_types::datatypes::ServerCapabilities>(),
        entry::<retp_types::datatypes::SupportedDataObject>(),
        entry::<retp_types::datatypes::SupportedProtocol>(),
        entry::<retp_types::datatypes::Version>(),
        entry::<retp_types::private_protocols::witsml_soap::WMLS_AddToStore>(),
        entry::<retp_types::private_protocols::witsml_soap::WMLS_AddToStoreResponse>(),
        entry::<retp_types::private_protocols::witsml_soap::WMLS_DeleteFromStore>(),
        entry::<retp_types::private_protocols::witsml_soap::WMLS_DeleteFromStoreResponse>(),
        entry::<retp_types::private_protocols::witsml_soap::WMLS_GetBaseMsg>(),
        entry::<retp_types::private_protocols::witsml_soap::WMLS_GetBaseMsgResponse>(),
        entry::<retp_types::private_protocols::witsml_soap::WMLS_GetCap>(),
        entry::<retp_types::private_protocols::witsml_soap::WMLS_GetCapResponse>(),
        entry::<retp_types::private_protocols::witsml_soap::WMLS_GetFromStore>(),
        entry::<retp_types::private_protocols::witsml_soap::WMLS_GetFromStoreResponse>(),
        entry::<retp_types::private_protocols::witsml_soap::WMLS_GetVersion>(),
        entry::<retp_types::private_protocols::witsml_soap::WMLS_GetVersionResponse>(),
        entry::<retp_types::private_protocols::witsml_soap::WMLS_UpdateInStore>(),
        entry::<retp_types::private_protocols::witsml_soap::WMLS_UpdateInStoreResponse>(),
        entry::<retp_types::protocol::channel_data_frame::CancelGetFrame>(),
        entry::<retp_types::protocol::channel_data_frame::GetFrame>(),
        entry::<retp_types::protocol::channel_data_frame::GetFrameMetadata>(),
        entry::<retp_types::protocol::channel_data_frame::GetFrameMetadataResponse>(),
        entry::<retp_types::protocol::channel_data_frame::GetFrameResponseHeader>(),
        entry::<retp_types::protocol::channel_data_frame::GetFrameResponseRows>(),
        entry::<retp_types::protocol::channel_data_load::ChannelData>(),
        entry::<retp_types::protocol::channel_data_load::ChannelsClosed>(),
        entry::<retp_types::protocol::channel_data_load::CloseChannels>(),
        entry::<retp_types::protocol::channel_data_load::OpenChannels>(),
        entry::<retp_types::protocol::channel_data_load::OpenChannelsResponse>(),
        entry::<retp_types::protocol::channel_data_load::ReplaceRange>(),
        entry::<retp_types::protocol::channel_data_load::ReplaceRangeResponse>(),
        entry::<retp_types::protocol::channel_data_load::TruncateChannels>(),
        entry::<retp_types::protocol::channel_data_load::TruncateChannelsResponse>(),
        entry::<retp_types::protocol::channel_streaming::ChannelData>(),
        entry::<retp_types::protocol::channel_streaming::ChannelMetadata>(),
        entry::<retp_types::protocol::channel_streaming::StartStreaming>(),
        entry::<retp_types::protocol::channel_streaming::StopStreaming>(),
        entry::<retp_types::protocol::channel_streaming::TruncateChannels>(),
        entry::<retp_types::protocol::channel_subscribe::CancelGetRanges>(),
        entry::<retp_types::protocol::channel_subscribe::ChannelData>(),
        entry::<retp_types::protocol::channel_subscribe::ChannelsTruncated>(),
        entry::<retp_types::protocol::channel_subscribe::GetChangeAnnotations>(),
        entry::<retp_types::protocol::channel_subscribe::GetChangeAnnotationsResponse>(),
        entry::<retp_types::protocol::channel_subscribe::GetChannelMetadata>(),
        entry::<retp_types::protocol::channel_subscribe::GetChannelMetadataResponse>(),
        entry::<retp_types::protocol::channel_subscribe::GetRanges>(),
        entry::<retp_types::protocol::channel_subscribe::GetRangesResponse>(),
        entry::<retp_types::protocol::channel_subscribe::RangeReplaced>(),
        entry::<retp_types::protocol::channel_subscribe::SubscribeChannels>(),
        entry::<retp_types::protocol::channel_subscribe::SubscribeChannelsResponse>(),
        entry::<retp_types::protocol::channel_subscribe::SubscriptionsStopped>(),
        entry::<retp_types::protocol::channel_subscribe::UnsubscribeChannels>(),
        entry::<retp_types::protocol::core::Acknowledge>(),
        entry::<retp_types::protocol::core::Authorize>(),
        entry::<retp_types::protocol::core::AuthorizeResponse>(),
        entry::<retp_types::protocol::core::CloseSession>(),
        entry::<retp_types::protocol::core::OpenSession>(),
        entry::<retp_types::protocol::core::Ping>(),
        entry::<retp_types::protocol::core::Pong>(),
        entry::<retp_types::protocol::core::ProtocolException>(),
        entry::<retp_types::protocol::core::RequestSession>(),
        entry::<retp_types::protocol::core_osdu::ResumeSession>(),
        entry::<retp_types::protocol::core_osdu::ResumeSessionResponse>(),
        entry::<retp_types::protocol::data_array::GetDataArrayMetadata>(),
        entry::<retp_types::protocol::data_array::GetDataArrayMetadataResponse>(),
        entry::<retp_types::protocol::data_array::GetDataArrays>(),
        entry::<retp_types::protocol::data_array::GetDataArraysResponse>(),
        entry::<retp_types::protocol::data_array::GetDataSubarrays>(),
        entry::<retp_types::protocol::data_array::GetDataSubarraysResponse>(),
        entry::<retp_types::protocol::data_array::PutDataArrays>(),
        entry::<retp_types::protocol::data_array::PutDataArraysResponse>(),
        entry::<retp_types::protocol::data_array::PutDataSubarrays>(),
        entry::<retp_types::protocol::data_array::PutDataSubarraysResponse>(),
        entry::<retp_types::protocol::data_array::PutUninitializedDataArrays>(),
        entry::<retp_types::protocol::data_array::PutUninitializedDataArraysResponse>(),
        entry::<retp_types::protocol::dataspace::DeleteDataspaces>(),
        entry::<retp_types::protocol::dataspace::DeleteDataspacesResponse>(),
        entry::<retp_types::protocol::dataspace::GetDataspaces>(),
        entry::<retp_types::protocol::dataspace::GetDataspacesResponse>(),
        entry::<retp_types::protocol::dataspace::PutDataspaces>(),
        entry::<retp_types::protocol::dataspace::PutDataspacesResponse>(),
        entry::<retp_types::protocol::discovery::GetDeletedResources>(),
        entry::<retp_types::protocol::discovery::GetDeletedResourcesResponse>(),
        entry::<retp_types::protocol::discovery::GetResources>(),
        entry::<retp_types::protocol::discovery::GetResourcesEdgesResponse>(),
        entry::<retp_types::protocol::discovery::GetResourcesResponse>(),
        entry::<retp_types::protocol::discovery_query::FindResources>(),
        entry::<retp_types::protocol::discovery_query::FindResourcesResponse>(),
        entry::<retp_types::protocol::growing_object::DeleteParts>(),
        entry::<retp_types::protocol::growing_object::DeletePartsResponse>(),
        entry::<retp_types::protocol::growing_object::GetChangeAnnotations>(),
        entry::<retp_types::protocol::growing_object::GetChangeAnnotationsResponse>(),
        entry::<retp_types::protocol::growing_object::GetGrowingDataObjectsHeader>(),
        entry::<retp_types::protocol::growing_object::GetGrowingDataObjectsHeaderResponse>(),
        entry::<retp_types::protocol::growing_object::GetParts>(),
        entry::<retp_types::protocol::growing_object::GetPartsByRange>(),
        entry::<retp_types::protocol::growing_object::GetPartsByRangeResponse>(),
        entry::<retp_types::protocol::growing_object::GetPartsMetadata>(),
        entry::<retp_types::protocol::growing_object::GetPartsMetadataResponse>(),
        entry::<retp_types::protocol::growing_object::GetPartsResponse>(),
        entry::<retp_types::protocol::growing_object::PutGrowingDataObjectsHeader>(),
        entry::<retp_types::protocol::growing_object::PutGrowingDataObjectsHeaderResponse>(),
        entry::<retp_types::protocol::growing_object::PutParts>(),
        entry::<retp_types::protocol::growing_object::PutPartsResponse>(),
        entry::<retp_types::protocol::growing_object::ReplacePartsByRange>(),
        entry::<retp_types::protocol::growing_object::ReplacePartsByRangeResponse>(),
        entry::<retp_types::protocol::growing_object_notification::PartSubscriptionEnded>(),
        entry::<retp_types::protocol::growing_object_notification::PartsChanged>(),
        entry::<retp_types::protocol::growing_object_notification::PartsDeleted>(),
        entry::<retp_types::protocol::growing_object_notification::PartsReplacedByRange>(),
        entry::<retp_types::protocol::growing_object_notification::SubscribePartNotifications>(),
        entry::<retp_types::protocol::growing_object_notification::SubscribePartNotificationsResponse>(),
        entry::<retp_types::protocol::growing_object_notification::UnsolicitedPartNotifications>(),
        entry::<retp_types::protocol::growing_object_notification::UnsubscribePartNotification>(),
        entry::<retp_types::protocol::growing_object_query::FindParts>(),
        entry::<retp_types::protocol::growing_object_query::FindPartsResponse>(),
        entry::<retp_types::protocol::store::Chunk>(),
        entry::<retp_types::protocol::store::DeleteDataObjects>(),
        entry::<retp_types::protocol::store::DeleteDataObjectsResponse>(),
        entry::<retp_types::protocol::store::GetDataObjects>(),
        entry::<retp_types::protocol::store::GetDataObjectsResponse>(),
        entry::<retp_types::protocol::store::PutDataObjects>(),
        entry::<retp_types::protocol::store::PutDataObjectsResponse>(),
        entry::<retp_types::protocol::store_notification::Chunk>(),
        entry::<retp_types::protocol::store_notification::ObjectAccessRevoked>(),
        entry::<retp_types::protocol::store_notification::ObjectActiveStatusChanged>(),
        entry::<retp_types::protocol::store_notification::ObjectChanged>(),
        entry::<retp_types::protocol::store_notification::ObjectDeleted>(),
        entry::<retp_types::protocol::store_notification::SubscribeNotifications>(),
        entry::<retp_types::protocol::store_notification::SubscribeNotificationsResponse>(),
        entry::<retp_types::protocol::store_notification::SubscriptionEnded>(),
        entry::<retp_types::protocol::store_notification::UnsolicitedStoreNotifications>(),
        entry::<retp_types::protocol::store_notification::UnsubscribeNotifications>(),
        entry::<retp_types::protocol::store_query::Chunk>(),
        entry::<retp_types::protocol::store_query::FindDataObjects>(),
        entry::<retp_types::protocol::store_query::FindDataObjectsResponse>(),
        entry::<retp_types::protocol::supported_types::GetSupportedTypes>(),
        entry::<retp_types::protocol::supported_types::GetSupportedTypesResponse>(),
        entry::<retp_types::protocol::transaction::CommitTransaction>(),
        entry::<retp_types::protocol::transaction::CommitTransactionResponse>(),
        entry::<retp_types::protocol::transaction::RollbackTransaction>(),
        entry::<retp_types::protocol::transaction::RollbackTransactionResponse>(),
        entry::<retp_types::protocol::transaction::StartTransaction>(),
        entry::<retp_types::protocol::transaction::StartTransactionResponse>(),
    ])
}

#[test]
fn defaults_match_schema() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../retp-tests/tests/fixtures/defaults");
    let mut generated = generated();
    assert_eq!(generated.len(), RECORD_COUNT, "two records share a full name");
    let mut failures = Vec::new();
    let mut seen = 0;
    let listing = std::fs::read_dir(&dir)
        .unwrap_or_else(|why| panic!("reading {}: {why}", dir.display()));
    for file in listing {
        let path = file.expect("a directory entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("avro") {
            continue;
        }
        let name = path.file_stem().and_then(|s| s.to_str()).expect("a UTF-8 name").to_string();
        let expected = std::fs::read(&path).expect("a fixture");
        seen += 1;
        match generated.remove(name.as_str()) {
            None => failures.push(format!("{name}: fastavro has a fixture and the crate has no type")),
            Some(bytes) if bytes != expected => failures.push(format!(
                "{name}: the default encodes to {bytes:02x?}, fastavro wrote {expected:02x?}"
            )),
            Some(_) => {}
        }
    }
    for name in generated.keys() {
        failures.push(format!("{name}: no fixture - rerun defaults.py"));
    }
    assert_eq!(seen, RECORD_COUNT, "the fixtures and the schemas disagree about how many records there are");
    assert!(failures.is_empty(), "{} of {seen} records:
{}", failures.len(), failures.join("
"));
}
