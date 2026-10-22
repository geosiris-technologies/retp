// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 84bd808714c5d32f2b7e91d897b5677012f16bafd221694662c8058e98c13fdc
//! One encoding round trip per ETP message - generated, because 138 of
//! these is not a thing anyone writes by hand.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.
//!
//! Each test encodes the message's default value, decodes it back, and
//! compares. What it catches is a writer and a reader that disagree about
//! one field - the failure that a hand-written sample would miss on the
//! 137 messages nobody thought to sample.
//!
//! It does **not** prove the encoding is Avro; `retp-avro`'s own
//! cross-audit against an independent implementation does that.

use retp_avro::Avro;

#[test]
fn protocol_core_request_session() {
    let original = <retp_types::protocol::core::RequestSession>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::core::RequestSession>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::core::RequestSession>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::core::RequestSession>::FULL_NAME);
}

#[test]
fn protocol_core_open_session() {
    let original = <retp_types::protocol::core::OpenSession>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::core::OpenSession>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::core::OpenSession>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::core::OpenSession>::FULL_NAME);
}

#[test]
fn protocol_core_close_session() {
    let original = <retp_types::protocol::core::CloseSession>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::core::CloseSession>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::core::CloseSession>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::core::CloseSession>::FULL_NAME);
}

#[test]
fn protocol_core_authorize() {
    let original = <retp_types::protocol::core::Authorize>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::core::Authorize>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::core::Authorize>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::core::Authorize>::FULL_NAME);
}

#[test]
fn protocol_core_authorize_response() {
    let original = <retp_types::protocol::core::AuthorizeResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::core::AuthorizeResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::core::AuthorizeResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::core::AuthorizeResponse>::FULL_NAME);
}

#[test]
fn protocol_core_ping() {
    let original = <retp_types::protocol::core::Ping>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::core::Ping>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::core::Ping>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::core::Ping>::FULL_NAME);
}

#[test]
fn protocol_core_pong() {
    let original = <retp_types::protocol::core::Pong>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::core::Pong>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::core::Pong>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::core::Pong>::FULL_NAME);
}

#[test]
fn protocol_core_protocol_exception() {
    let original = <retp_types::protocol::core::ProtocolException>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::core::ProtocolException>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::core::ProtocolException>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::core::ProtocolException>::FULL_NAME);
}

#[test]
fn protocol_core_acknowledge() {
    let original = <retp_types::protocol::core::Acknowledge>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::core::Acknowledge>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::core::Acknowledge>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::core::Acknowledge>::FULL_NAME);
}

#[test]
fn protocol_channel_streaming_channel_metadata() {
    let original = <retp_types::protocol::channel_streaming::ChannelMetadata>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_streaming::ChannelMetadata>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_streaming::ChannelMetadata>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_streaming::ChannelMetadata>::FULL_NAME);
}

#[test]
fn protocol_channel_streaming_channel_data() {
    let original = <retp_types::protocol::channel_streaming::ChannelData>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_streaming::ChannelData>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_streaming::ChannelData>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_streaming::ChannelData>::FULL_NAME);
}

#[test]
fn protocol_channel_streaming_start_streaming() {
    let original = <retp_types::protocol::channel_streaming::StartStreaming>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_streaming::StartStreaming>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_streaming::StartStreaming>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_streaming::StartStreaming>::FULL_NAME);
}

#[test]
fn protocol_channel_streaming_stop_streaming() {
    let original = <retp_types::protocol::channel_streaming::StopStreaming>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_streaming::StopStreaming>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_streaming::StopStreaming>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_streaming::StopStreaming>::FULL_NAME);
}

#[test]
fn protocol_channel_streaming_truncate_channels() {
    let original = <retp_types::protocol::channel_streaming::TruncateChannels>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_streaming::TruncateChannels>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_streaming::TruncateChannels>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_streaming::TruncateChannels>::FULL_NAME);
}

#[test]
fn protocol_channel_data_frame_get_frame_metadata() {
    let original = <retp_types::protocol::channel_data_frame::GetFrameMetadata>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_data_frame::GetFrameMetadata>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_data_frame::GetFrameMetadata>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_data_frame::GetFrameMetadata>::FULL_NAME);
}

#[test]
fn protocol_channel_data_frame_get_frame_metadata_response() {
    let original = <retp_types::protocol::channel_data_frame::GetFrameMetadataResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_data_frame::GetFrameMetadataResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_data_frame::GetFrameMetadataResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_data_frame::GetFrameMetadataResponse>::FULL_NAME);
}

#[test]
fn protocol_channel_data_frame_get_frame() {
    let original = <retp_types::protocol::channel_data_frame::GetFrame>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_data_frame::GetFrame>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_data_frame::GetFrame>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_data_frame::GetFrame>::FULL_NAME);
}

#[test]
fn protocol_channel_data_frame_get_frame_response_header() {
    let original = <retp_types::protocol::channel_data_frame::GetFrameResponseHeader>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_data_frame::GetFrameResponseHeader>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_data_frame::GetFrameResponseHeader>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_data_frame::GetFrameResponseHeader>::FULL_NAME);
}

#[test]
fn protocol_channel_data_frame_cancel_get_frame() {
    let original = <retp_types::protocol::channel_data_frame::CancelGetFrame>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_data_frame::CancelGetFrame>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_data_frame::CancelGetFrame>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_data_frame::CancelGetFrame>::FULL_NAME);
}

#[test]
fn protocol_channel_data_frame_get_frame_response_rows() {
    let original = <retp_types::protocol::channel_data_frame::GetFrameResponseRows>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_data_frame::GetFrameResponseRows>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_data_frame::GetFrameResponseRows>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_data_frame::GetFrameResponseRows>::FULL_NAME);
}

#[test]
fn protocol_discovery_get_resources() {
    let original = <retp_types::protocol::discovery::GetResources>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::discovery::GetResources>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::discovery::GetResources>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::discovery::GetResources>::FULL_NAME);
}

#[test]
fn protocol_discovery_get_resources_response() {
    let original = <retp_types::protocol::discovery::GetResourcesResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::discovery::GetResourcesResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::discovery::GetResourcesResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::discovery::GetResourcesResponse>::FULL_NAME);
}

#[test]
fn protocol_discovery_get_deleted_resources() {
    let original = <retp_types::protocol::discovery::GetDeletedResources>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::discovery::GetDeletedResources>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::discovery::GetDeletedResources>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::discovery::GetDeletedResources>::FULL_NAME);
}

#[test]
fn protocol_discovery_get_deleted_resources_response() {
    let original = <retp_types::protocol::discovery::GetDeletedResourcesResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::discovery::GetDeletedResourcesResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::discovery::GetDeletedResourcesResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::discovery::GetDeletedResourcesResponse>::FULL_NAME);
}

#[test]
fn protocol_discovery_get_resources_edges_response() {
    let original = <retp_types::protocol::discovery::GetResourcesEdgesResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::discovery::GetResourcesEdgesResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::discovery::GetResourcesEdgesResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::discovery::GetResourcesEdgesResponse>::FULL_NAME);
}

#[test]
fn protocol_store_get_data_objects() {
    let original = <retp_types::protocol::store::GetDataObjects>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::store::GetDataObjects>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::store::GetDataObjects>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::store::GetDataObjects>::FULL_NAME);
}

#[test]
fn protocol_store_put_data_objects() {
    let original = <retp_types::protocol::store::PutDataObjects>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::store::PutDataObjects>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::store::PutDataObjects>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::store::PutDataObjects>::FULL_NAME);
}

#[test]
fn protocol_store_delete_data_objects() {
    let original = <retp_types::protocol::store::DeleteDataObjects>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::store::DeleteDataObjects>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::store::DeleteDataObjects>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::store::DeleteDataObjects>::FULL_NAME);
}

#[test]
fn protocol_store_get_data_objects_response() {
    let original = <retp_types::protocol::store::GetDataObjectsResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::store::GetDataObjectsResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::store::GetDataObjectsResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::store::GetDataObjectsResponse>::FULL_NAME);
}

#[test]
fn protocol_store_chunk() {
    let original = <retp_types::protocol::store::Chunk>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::store::Chunk>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::store::Chunk>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::store::Chunk>::FULL_NAME);
}

#[test]
fn protocol_store_put_data_objects_response() {
    let original = <retp_types::protocol::store::PutDataObjectsResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::store::PutDataObjectsResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::store::PutDataObjectsResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::store::PutDataObjectsResponse>::FULL_NAME);
}

#[test]
fn protocol_store_delete_data_objects_response() {
    let original = <retp_types::protocol::store::DeleteDataObjectsResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::store::DeleteDataObjectsResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::store::DeleteDataObjectsResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::store::DeleteDataObjectsResponse>::FULL_NAME);
}

#[test]
fn protocol_store_notification_object_changed() {
    let original = <retp_types::protocol::store_notification::ObjectChanged>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::store_notification::ObjectChanged>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::store_notification::ObjectChanged>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::store_notification::ObjectChanged>::FULL_NAME);
}

#[test]
fn protocol_store_notification_object_deleted() {
    let original = <retp_types::protocol::store_notification::ObjectDeleted>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::store_notification::ObjectDeleted>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::store_notification::ObjectDeleted>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::store_notification::ObjectDeleted>::FULL_NAME);
}

#[test]
fn protocol_store_notification_unsubscribe_notifications() {
    let original = <retp_types::protocol::store_notification::UnsubscribeNotifications>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::store_notification::UnsubscribeNotifications>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::store_notification::UnsubscribeNotifications>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::store_notification::UnsubscribeNotifications>::FULL_NAME);
}

#[test]
fn protocol_store_notification_object_access_revoked() {
    let original = <retp_types::protocol::store_notification::ObjectAccessRevoked>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::store_notification::ObjectAccessRevoked>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::store_notification::ObjectAccessRevoked>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::store_notification::ObjectAccessRevoked>::FULL_NAME);
}

#[test]
fn protocol_store_notification_subscribe_notifications() {
    let original = <retp_types::protocol::store_notification::SubscribeNotifications>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::store_notification::SubscribeNotifications>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::store_notification::SubscribeNotifications>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::store_notification::SubscribeNotifications>::FULL_NAME);
}

#[test]
fn protocol_store_notification_subscription_ended() {
    let original = <retp_types::protocol::store_notification::SubscriptionEnded>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::store_notification::SubscriptionEnded>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::store_notification::SubscriptionEnded>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::store_notification::SubscriptionEnded>::FULL_NAME);
}

#[test]
fn protocol_store_notification_unsolicited_store_notifications() {
    let original = <retp_types::protocol::store_notification::UnsolicitedStoreNotifications>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::store_notification::UnsolicitedStoreNotifications>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::store_notification::UnsolicitedStoreNotifications>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::store_notification::UnsolicitedStoreNotifications>::FULL_NAME);
}

#[test]
fn protocol_store_notification_chunk() {
    let original = <retp_types::protocol::store_notification::Chunk>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::store_notification::Chunk>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::store_notification::Chunk>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::store_notification::Chunk>::FULL_NAME);
}

#[test]
fn protocol_store_notification_subscribe_notifications_response() {
    let original = <retp_types::protocol::store_notification::SubscribeNotificationsResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::store_notification::SubscribeNotificationsResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::store_notification::SubscribeNotificationsResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::store_notification::SubscribeNotificationsResponse>::FULL_NAME);
}

#[test]
fn protocol_store_notification_object_active_status_changed() {
    let original = <retp_types::protocol::store_notification::ObjectActiveStatusChanged>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::store_notification::ObjectActiveStatusChanged>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::store_notification::ObjectActiveStatusChanged>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::store_notification::ObjectActiveStatusChanged>::FULL_NAME);
}

#[test]
fn protocol_growing_object_delete_parts() {
    let original = <retp_types::protocol::growing_object::DeleteParts>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object::DeleteParts>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object::DeleteParts>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object::DeleteParts>::FULL_NAME);
}

#[test]
fn protocol_growing_object_get_parts() {
    let original = <retp_types::protocol::growing_object::GetParts>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object::GetParts>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object::GetParts>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object::GetParts>::FULL_NAME);
}

#[test]
fn protocol_growing_object_get_parts_by_range() {
    let original = <retp_types::protocol::growing_object::GetPartsByRange>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object::GetPartsByRange>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object::GetPartsByRange>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object::GetPartsByRange>::FULL_NAME);
}

#[test]
fn protocol_growing_object_put_parts() {
    let original = <retp_types::protocol::growing_object::PutParts>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object::PutParts>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object::PutParts>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object::PutParts>::FULL_NAME);
}

#[test]
fn protocol_growing_object_get_parts_response() {
    let original = <retp_types::protocol::growing_object::GetPartsResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object::GetPartsResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object::GetPartsResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object::GetPartsResponse>::FULL_NAME);
}

#[test]
fn protocol_growing_object_replace_parts_by_range() {
    let original = <retp_types::protocol::growing_object::ReplacePartsByRange>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object::ReplacePartsByRange>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object::ReplacePartsByRange>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object::ReplacePartsByRange>::FULL_NAME);
}

#[test]
fn protocol_growing_object_get_parts_metadata() {
    let original = <retp_types::protocol::growing_object::GetPartsMetadata>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object::GetPartsMetadata>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object::GetPartsMetadata>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object::GetPartsMetadata>::FULL_NAME);
}

#[test]
fn protocol_growing_object_get_parts_metadata_response() {
    let original = <retp_types::protocol::growing_object::GetPartsMetadataResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object::GetPartsMetadataResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object::GetPartsMetadataResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object::GetPartsMetadataResponse>::FULL_NAME);
}

#[test]
fn protocol_growing_object_get_parts_by_range_response() {
    let original = <retp_types::protocol::growing_object::GetPartsByRangeResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object::GetPartsByRangeResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object::GetPartsByRangeResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object::GetPartsByRangeResponse>::FULL_NAME);
}

#[test]
fn protocol_growing_object_delete_parts_response() {
    let original = <retp_types::protocol::growing_object::DeletePartsResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object::DeletePartsResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object::DeletePartsResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object::DeletePartsResponse>::FULL_NAME);
}

#[test]
fn protocol_growing_object_put_parts_response() {
    let original = <retp_types::protocol::growing_object::PutPartsResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object::PutPartsResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object::PutPartsResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object::PutPartsResponse>::FULL_NAME);
}

#[test]
fn protocol_growing_object_get_growing_data_objects_header() {
    let original = <retp_types::protocol::growing_object::GetGrowingDataObjectsHeader>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object::GetGrowingDataObjectsHeader>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object::GetGrowingDataObjectsHeader>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object::GetGrowingDataObjectsHeader>::FULL_NAME);
}

#[test]
fn protocol_growing_object_get_growing_data_objects_header_response() {
    let original = <retp_types::protocol::growing_object::GetGrowingDataObjectsHeaderResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object::GetGrowingDataObjectsHeaderResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object::GetGrowingDataObjectsHeaderResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object::GetGrowingDataObjectsHeaderResponse>::FULL_NAME);
}

#[test]
fn protocol_growing_object_put_growing_data_objects_header() {
    let original = <retp_types::protocol::growing_object::PutGrowingDataObjectsHeader>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object::PutGrowingDataObjectsHeader>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object::PutGrowingDataObjectsHeader>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object::PutGrowingDataObjectsHeader>::FULL_NAME);
}

#[test]
fn protocol_growing_object_put_growing_data_objects_header_response() {
    let original = <retp_types::protocol::growing_object::PutGrowingDataObjectsHeaderResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object::PutGrowingDataObjectsHeaderResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object::PutGrowingDataObjectsHeaderResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object::PutGrowingDataObjectsHeaderResponse>::FULL_NAME);
}

#[test]
fn protocol_growing_object_replace_parts_by_range_response() {
    let original = <retp_types::protocol::growing_object::ReplacePartsByRangeResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object::ReplacePartsByRangeResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object::ReplacePartsByRangeResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object::ReplacePartsByRangeResponse>::FULL_NAME);
}

#[test]
fn protocol_growing_object_get_change_annotations() {
    let original = <retp_types::protocol::growing_object::GetChangeAnnotations>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object::GetChangeAnnotations>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object::GetChangeAnnotations>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object::GetChangeAnnotations>::FULL_NAME);
}

#[test]
fn protocol_growing_object_get_change_annotations_response() {
    let original = <retp_types::protocol::growing_object::GetChangeAnnotationsResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object::GetChangeAnnotationsResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object::GetChangeAnnotationsResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object::GetChangeAnnotationsResponse>::FULL_NAME);
}

#[test]
fn protocol_growing_object_notification_parts_changed() {
    let original = <retp_types::protocol::growing_object_notification::PartsChanged>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object_notification::PartsChanged>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object_notification::PartsChanged>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object_notification::PartsChanged>::FULL_NAME);
}

#[test]
fn protocol_growing_object_notification_parts_deleted() {
    let original = <retp_types::protocol::growing_object_notification::PartsDeleted>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object_notification::PartsDeleted>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object_notification::PartsDeleted>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object_notification::PartsDeleted>::FULL_NAME);
}

#[test]
fn protocol_growing_object_notification_unsubscribe_part_notification() {
    let original = <retp_types::protocol::growing_object_notification::UnsubscribePartNotification>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object_notification::UnsubscribePartNotification>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object_notification::UnsubscribePartNotification>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object_notification::UnsubscribePartNotification>::FULL_NAME);
}

#[test]
fn protocol_growing_object_notification_parts_replaced_by_range() {
    let original = <retp_types::protocol::growing_object_notification::PartsReplacedByRange>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object_notification::PartsReplacedByRange>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object_notification::PartsReplacedByRange>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object_notification::PartsReplacedByRange>::FULL_NAME);
}

#[test]
fn protocol_growing_object_notification_subscribe_part_notifications() {
    let original = <retp_types::protocol::growing_object_notification::SubscribePartNotifications>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object_notification::SubscribePartNotifications>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object_notification::SubscribePartNotifications>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object_notification::SubscribePartNotifications>::FULL_NAME);
}

#[test]
fn protocol_growing_object_notification_part_subscription_ended() {
    let original = <retp_types::protocol::growing_object_notification::PartSubscriptionEnded>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object_notification::PartSubscriptionEnded>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object_notification::PartSubscriptionEnded>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object_notification::PartSubscriptionEnded>::FULL_NAME);
}

#[test]
fn protocol_growing_object_notification_unsolicited_part_notifications() {
    let original = <retp_types::protocol::growing_object_notification::UnsolicitedPartNotifications>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object_notification::UnsolicitedPartNotifications>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object_notification::UnsolicitedPartNotifications>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object_notification::UnsolicitedPartNotifications>::FULL_NAME);
}

#[test]
fn protocol_growing_object_notification_subscribe_part_notifications_response() {
    let original = <retp_types::protocol::growing_object_notification::SubscribePartNotificationsResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object_notification::SubscribePartNotificationsResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object_notification::SubscribePartNotificationsResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object_notification::SubscribePartNotificationsResponse>::FULL_NAME);
}

#[test]
fn protocol_data_array_get_data_arrays_response() {
    let original = <retp_types::protocol::data_array::GetDataArraysResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::data_array::GetDataArraysResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::data_array::GetDataArraysResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::data_array::GetDataArraysResponse>::FULL_NAME);
}

#[test]
fn protocol_data_array_get_data_arrays() {
    let original = <retp_types::protocol::data_array::GetDataArrays>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::data_array::GetDataArrays>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::data_array::GetDataArrays>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::data_array::GetDataArrays>::FULL_NAME);
}

#[test]
fn protocol_data_array_get_data_subarrays() {
    let original = <retp_types::protocol::data_array::GetDataSubarrays>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::data_array::GetDataSubarrays>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::data_array::GetDataSubarrays>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::data_array::GetDataSubarrays>::FULL_NAME);
}

#[test]
fn protocol_data_array_put_data_arrays() {
    let original = <retp_types::protocol::data_array::PutDataArrays>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::data_array::PutDataArrays>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::data_array::PutDataArrays>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::data_array::PutDataArrays>::FULL_NAME);
}

#[test]
fn protocol_data_array_put_data_subarrays() {
    let original = <retp_types::protocol::data_array::PutDataSubarrays>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::data_array::PutDataSubarrays>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::data_array::PutDataSubarrays>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::data_array::PutDataSubarrays>::FULL_NAME);
}

#[test]
fn protocol_data_array_get_data_array_metadata() {
    let original = <retp_types::protocol::data_array::GetDataArrayMetadata>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::data_array::GetDataArrayMetadata>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::data_array::GetDataArrayMetadata>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::data_array::GetDataArrayMetadata>::FULL_NAME);
}

#[test]
fn protocol_data_array_get_data_array_metadata_response() {
    let original = <retp_types::protocol::data_array::GetDataArrayMetadataResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::data_array::GetDataArrayMetadataResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::data_array::GetDataArrayMetadataResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::data_array::GetDataArrayMetadataResponse>::FULL_NAME);
}

#[test]
fn protocol_data_array_get_data_subarrays_response() {
    let original = <retp_types::protocol::data_array::GetDataSubarraysResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::data_array::GetDataSubarraysResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::data_array::GetDataSubarraysResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::data_array::GetDataSubarraysResponse>::FULL_NAME);
}

#[test]
fn protocol_data_array_put_uninitialized_data_arrays() {
    let original = <retp_types::protocol::data_array::PutUninitializedDataArrays>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::data_array::PutUninitializedDataArrays>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::data_array::PutUninitializedDataArrays>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::data_array::PutUninitializedDataArrays>::FULL_NAME);
}

#[test]
fn protocol_data_array_put_data_arrays_response() {
    let original = <retp_types::protocol::data_array::PutDataArraysResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::data_array::PutDataArraysResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::data_array::PutDataArraysResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::data_array::PutDataArraysResponse>::FULL_NAME);
}

#[test]
fn protocol_data_array_put_data_subarrays_response() {
    let original = <retp_types::protocol::data_array::PutDataSubarraysResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::data_array::PutDataSubarraysResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::data_array::PutDataSubarraysResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::data_array::PutDataSubarraysResponse>::FULL_NAME);
}

#[test]
fn protocol_data_array_put_uninitialized_data_arrays_response() {
    let original = <retp_types::protocol::data_array::PutUninitializedDataArraysResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::data_array::PutUninitializedDataArraysResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::data_array::PutUninitializedDataArraysResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::data_array::PutUninitializedDataArraysResponse>::FULL_NAME);
}

#[test]
fn protocol_discovery_query_find_resources() {
    let original = <retp_types::protocol::discovery_query::FindResources>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::discovery_query::FindResources>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::discovery_query::FindResources>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::discovery_query::FindResources>::FULL_NAME);
}

#[test]
fn protocol_discovery_query_find_resources_response() {
    let original = <retp_types::protocol::discovery_query::FindResourcesResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::discovery_query::FindResourcesResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::discovery_query::FindResourcesResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::discovery_query::FindResourcesResponse>::FULL_NAME);
}

#[test]
fn protocol_store_query_find_data_objects() {
    let original = <retp_types::protocol::store_query::FindDataObjects>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::store_query::FindDataObjects>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::store_query::FindDataObjects>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::store_query::FindDataObjects>::FULL_NAME);
}

#[test]
fn protocol_store_query_find_data_objects_response() {
    let original = <retp_types::protocol::store_query::FindDataObjectsResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::store_query::FindDataObjectsResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::store_query::FindDataObjectsResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::store_query::FindDataObjectsResponse>::FULL_NAME);
}

#[test]
fn protocol_store_query_chunk() {
    let original = <retp_types::protocol::store_query::Chunk>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::store_query::Chunk>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::store_query::Chunk>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::store_query::Chunk>::FULL_NAME);
}

#[test]
fn protocol_growing_object_query_find_parts() {
    let original = <retp_types::protocol::growing_object_query::FindParts>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object_query::FindParts>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object_query::FindParts>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object_query::FindParts>::FULL_NAME);
}

#[test]
fn protocol_growing_object_query_find_parts_response() {
    let original = <retp_types::protocol::growing_object_query::FindPartsResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::growing_object_query::FindPartsResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::growing_object_query::FindPartsResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::growing_object_query::FindPartsResponse>::FULL_NAME);
}

#[test]
fn protocol_transaction_start_transaction() {
    let original = <retp_types::protocol::transaction::StartTransaction>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::transaction::StartTransaction>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::transaction::StartTransaction>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::transaction::StartTransaction>::FULL_NAME);
}

#[test]
fn protocol_transaction_start_transaction_response() {
    let original = <retp_types::protocol::transaction::StartTransactionResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::transaction::StartTransactionResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::transaction::StartTransactionResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::transaction::StartTransactionResponse>::FULL_NAME);
}

#[test]
fn protocol_transaction_commit_transaction() {
    let original = <retp_types::protocol::transaction::CommitTransaction>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::transaction::CommitTransaction>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::transaction::CommitTransaction>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::transaction::CommitTransaction>::FULL_NAME);
}

#[test]
fn protocol_transaction_rollback_transaction() {
    let original = <retp_types::protocol::transaction::RollbackTransaction>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::transaction::RollbackTransaction>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::transaction::RollbackTransaction>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::transaction::RollbackTransaction>::FULL_NAME);
}

#[test]
fn protocol_transaction_commit_transaction_response() {
    let original = <retp_types::protocol::transaction::CommitTransactionResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::transaction::CommitTransactionResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::transaction::CommitTransactionResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::transaction::CommitTransactionResponse>::FULL_NAME);
}

#[test]
fn protocol_transaction_rollback_transaction_response() {
    let original = <retp_types::protocol::transaction::RollbackTransactionResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::transaction::RollbackTransactionResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::transaction::RollbackTransactionResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::transaction::RollbackTransactionResponse>::FULL_NAME);
}

#[test]
fn protocol_channel_subscribe_get_channel_metadata() {
    let original = <retp_types::protocol::channel_subscribe::GetChannelMetadata>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_subscribe::GetChannelMetadata>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_subscribe::GetChannelMetadata>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_subscribe::GetChannelMetadata>::FULL_NAME);
}

#[test]
fn protocol_channel_subscribe_get_channel_metadata_response() {
    let original = <retp_types::protocol::channel_subscribe::GetChannelMetadataResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_subscribe::GetChannelMetadataResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_subscribe::GetChannelMetadataResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_subscribe::GetChannelMetadataResponse>::FULL_NAME);
}

#[test]
fn protocol_channel_subscribe_subscribe_channels() {
    let original = <retp_types::protocol::channel_subscribe::SubscribeChannels>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_subscribe::SubscribeChannels>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_subscribe::SubscribeChannels>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_subscribe::SubscribeChannels>::FULL_NAME);
}

#[test]
fn protocol_channel_subscribe_channel_data() {
    let original = <retp_types::protocol::channel_subscribe::ChannelData>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_subscribe::ChannelData>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_subscribe::ChannelData>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_subscribe::ChannelData>::FULL_NAME);
}

#[test]
fn protocol_channel_subscribe_range_replaced() {
    let original = <retp_types::protocol::channel_subscribe::RangeReplaced>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_subscribe::RangeReplaced>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_subscribe::RangeReplaced>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_subscribe::RangeReplaced>::FULL_NAME);
}

#[test]
fn protocol_channel_subscribe_unsubscribe_channels() {
    let original = <retp_types::protocol::channel_subscribe::UnsubscribeChannels>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_subscribe::UnsubscribeChannels>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_subscribe::UnsubscribeChannels>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_subscribe::UnsubscribeChannels>::FULL_NAME);
}

#[test]
fn protocol_channel_subscribe_subscriptions_stopped() {
    let original = <retp_types::protocol::channel_subscribe::SubscriptionsStopped>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_subscribe::SubscriptionsStopped>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_subscribe::SubscriptionsStopped>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_subscribe::SubscriptionsStopped>::FULL_NAME);
}

#[test]
fn protocol_channel_subscribe_get_ranges() {
    let original = <retp_types::protocol::channel_subscribe::GetRanges>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_subscribe::GetRanges>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_subscribe::GetRanges>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_subscribe::GetRanges>::FULL_NAME);
}

#[test]
fn protocol_channel_subscribe_get_ranges_response() {
    let original = <retp_types::protocol::channel_subscribe::GetRangesResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_subscribe::GetRangesResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_subscribe::GetRangesResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_subscribe::GetRangesResponse>::FULL_NAME);
}

#[test]
fn protocol_channel_subscribe_cancel_get_ranges() {
    let original = <retp_types::protocol::channel_subscribe::CancelGetRanges>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_subscribe::CancelGetRanges>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_subscribe::CancelGetRanges>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_subscribe::CancelGetRanges>::FULL_NAME);
}

#[test]
fn protocol_channel_subscribe_subscribe_channels_response() {
    let original = <retp_types::protocol::channel_subscribe::SubscribeChannelsResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_subscribe::SubscribeChannelsResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_subscribe::SubscribeChannelsResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_subscribe::SubscribeChannelsResponse>::FULL_NAME);
}

#[test]
fn protocol_channel_subscribe_channels_truncated() {
    let original = <retp_types::protocol::channel_subscribe::ChannelsTruncated>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_subscribe::ChannelsTruncated>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_subscribe::ChannelsTruncated>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_subscribe::ChannelsTruncated>::FULL_NAME);
}

#[test]
fn protocol_channel_subscribe_get_change_annotations() {
    let original = <retp_types::protocol::channel_subscribe::GetChangeAnnotations>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_subscribe::GetChangeAnnotations>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_subscribe::GetChangeAnnotations>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_subscribe::GetChangeAnnotations>::FULL_NAME);
}

#[test]
fn protocol_channel_subscribe_get_change_annotations_response() {
    let original = <retp_types::protocol::channel_subscribe::GetChangeAnnotationsResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_subscribe::GetChangeAnnotationsResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_subscribe::GetChangeAnnotationsResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_subscribe::GetChangeAnnotationsResponse>::FULL_NAME);
}

#[test]
fn protocol_channel_data_load_open_channels() {
    let original = <retp_types::protocol::channel_data_load::OpenChannels>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_data_load::OpenChannels>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_data_load::OpenChannels>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_data_load::OpenChannels>::FULL_NAME);
}

#[test]
fn protocol_channel_data_load_open_channels_response() {
    let original = <retp_types::protocol::channel_data_load::OpenChannelsResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_data_load::OpenChannelsResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_data_load::OpenChannelsResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_data_load::OpenChannelsResponse>::FULL_NAME);
}

#[test]
fn protocol_channel_data_load_close_channels() {
    let original = <retp_types::protocol::channel_data_load::CloseChannels>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_data_load::CloseChannels>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_data_load::CloseChannels>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_data_load::CloseChannels>::FULL_NAME);
}

#[test]
fn protocol_channel_data_load_channel_data() {
    let original = <retp_types::protocol::channel_data_load::ChannelData>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_data_load::ChannelData>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_data_load::ChannelData>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_data_load::ChannelData>::FULL_NAME);
}

#[test]
fn protocol_channel_data_load_replace_range() {
    let original = <retp_types::protocol::channel_data_load::ReplaceRange>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_data_load::ReplaceRange>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_data_load::ReplaceRange>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_data_load::ReplaceRange>::FULL_NAME);
}

#[test]
fn protocol_channel_data_load_channels_closed() {
    let original = <retp_types::protocol::channel_data_load::ChannelsClosed>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_data_load::ChannelsClosed>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_data_load::ChannelsClosed>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_data_load::ChannelsClosed>::FULL_NAME);
}

#[test]
fn protocol_channel_data_load_replace_range_response() {
    let original = <retp_types::protocol::channel_data_load::ReplaceRangeResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_data_load::ReplaceRangeResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_data_load::ReplaceRangeResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_data_load::ReplaceRangeResponse>::FULL_NAME);
}

#[test]
fn protocol_channel_data_load_truncate_channels() {
    let original = <retp_types::protocol::channel_data_load::TruncateChannels>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_data_load::TruncateChannels>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_data_load::TruncateChannels>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_data_load::TruncateChannels>::FULL_NAME);
}

#[test]
fn protocol_channel_data_load_truncate_channels_response() {
    let original = <retp_types::protocol::channel_data_load::TruncateChannelsResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::channel_data_load::TruncateChannelsResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::channel_data_load::TruncateChannelsResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::channel_data_load::TruncateChannelsResponse>::FULL_NAME);
}

#[test]
fn protocol_dataspace_get_dataspaces() {
    let original = <retp_types::protocol::dataspace::GetDataspaces>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::dataspace::GetDataspaces>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::dataspace::GetDataspaces>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::dataspace::GetDataspaces>::FULL_NAME);
}

#[test]
fn protocol_dataspace_get_dataspaces_response() {
    let original = <retp_types::protocol::dataspace::GetDataspacesResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::dataspace::GetDataspacesResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::dataspace::GetDataspacesResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::dataspace::GetDataspacesResponse>::FULL_NAME);
}

#[test]
fn protocol_dataspace_put_dataspaces() {
    let original = <retp_types::protocol::dataspace::PutDataspaces>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::dataspace::PutDataspaces>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::dataspace::PutDataspaces>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::dataspace::PutDataspaces>::FULL_NAME);
}

#[test]
fn protocol_dataspace_delete_dataspaces() {
    let original = <retp_types::protocol::dataspace::DeleteDataspaces>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::dataspace::DeleteDataspaces>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::dataspace::DeleteDataspaces>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::dataspace::DeleteDataspaces>::FULL_NAME);
}

#[test]
fn protocol_dataspace_delete_dataspaces_response() {
    let original = <retp_types::protocol::dataspace::DeleteDataspacesResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::dataspace::DeleteDataspacesResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::dataspace::DeleteDataspacesResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::dataspace::DeleteDataspacesResponse>::FULL_NAME);
}

#[test]
fn protocol_dataspace_put_dataspaces_response() {
    let original = <retp_types::protocol::dataspace::PutDataspacesResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::dataspace::PutDataspacesResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::dataspace::PutDataspacesResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::dataspace::PutDataspacesResponse>::FULL_NAME);
}

#[test]
fn protocol_supported_types_get_supported_types() {
    let original = <retp_types::protocol::supported_types::GetSupportedTypes>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::supported_types::GetSupportedTypes>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::supported_types::GetSupportedTypes>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::supported_types::GetSupportedTypes>::FULL_NAME);
}

#[test]
fn protocol_supported_types_get_supported_types_response() {
    let original = <retp_types::protocol::supported_types::GetSupportedTypesResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::supported_types::GetSupportedTypesResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::supported_types::GetSupportedTypesResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::supported_types::GetSupportedTypesResponse>::FULL_NAME);
}

#[test]
fn private_protocols_witsml_soap_wmls_add_to_store() {
    let original = <retp_types::private_protocols::witsml_soap::WMLS_AddToStore>::default();
    let bytes = original.to_avro();
    let back = <retp_types::private_protocols::witsml_soap::WMLS_AddToStore>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::private_protocols::witsml_soap::WMLS_AddToStore>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::private_protocols::witsml_soap::WMLS_AddToStore>::FULL_NAME);
}

#[test]
fn private_protocols_witsml_soap_wmls_add_to_store_response() {
    let original = <retp_types::private_protocols::witsml_soap::WMLS_AddToStoreResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::private_protocols::witsml_soap::WMLS_AddToStoreResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::private_protocols::witsml_soap::WMLS_AddToStoreResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::private_protocols::witsml_soap::WMLS_AddToStoreResponse>::FULL_NAME);
}

#[test]
fn private_protocols_witsml_soap_wmls_delete_from_store() {
    let original = <retp_types::private_protocols::witsml_soap::WMLS_DeleteFromStore>::default();
    let bytes = original.to_avro();
    let back = <retp_types::private_protocols::witsml_soap::WMLS_DeleteFromStore>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::private_protocols::witsml_soap::WMLS_DeleteFromStore>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::private_protocols::witsml_soap::WMLS_DeleteFromStore>::FULL_NAME);
}

#[test]
fn private_protocols_witsml_soap_wmls_delete_from_store_response() {
    let original = <retp_types::private_protocols::witsml_soap::WMLS_DeleteFromStoreResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::private_protocols::witsml_soap::WMLS_DeleteFromStoreResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::private_protocols::witsml_soap::WMLS_DeleteFromStoreResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::private_protocols::witsml_soap::WMLS_DeleteFromStoreResponse>::FULL_NAME);
}

#[test]
fn private_protocols_witsml_soap_wmls_get_base_msg() {
    let original = <retp_types::private_protocols::witsml_soap::WMLS_GetBaseMsg>::default();
    let bytes = original.to_avro();
    let back = <retp_types::private_protocols::witsml_soap::WMLS_GetBaseMsg>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::private_protocols::witsml_soap::WMLS_GetBaseMsg>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::private_protocols::witsml_soap::WMLS_GetBaseMsg>::FULL_NAME);
}

#[test]
fn private_protocols_witsml_soap_wmls_get_base_msg_response() {
    let original = <retp_types::private_protocols::witsml_soap::WMLS_GetBaseMsgResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::private_protocols::witsml_soap::WMLS_GetBaseMsgResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::private_protocols::witsml_soap::WMLS_GetBaseMsgResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::private_protocols::witsml_soap::WMLS_GetBaseMsgResponse>::FULL_NAME);
}

#[test]
fn private_protocols_witsml_soap_wmls_get_cap() {
    let original = <retp_types::private_protocols::witsml_soap::WMLS_GetCap>::default();
    let bytes = original.to_avro();
    let back = <retp_types::private_protocols::witsml_soap::WMLS_GetCap>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::private_protocols::witsml_soap::WMLS_GetCap>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::private_protocols::witsml_soap::WMLS_GetCap>::FULL_NAME);
}

#[test]
fn private_protocols_witsml_soap_wmls_get_cap_response() {
    let original = <retp_types::private_protocols::witsml_soap::WMLS_GetCapResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::private_protocols::witsml_soap::WMLS_GetCapResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::private_protocols::witsml_soap::WMLS_GetCapResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::private_protocols::witsml_soap::WMLS_GetCapResponse>::FULL_NAME);
}

#[test]
fn private_protocols_witsml_soap_wmls_get_from_store() {
    let original = <retp_types::private_protocols::witsml_soap::WMLS_GetFromStore>::default();
    let bytes = original.to_avro();
    let back = <retp_types::private_protocols::witsml_soap::WMLS_GetFromStore>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::private_protocols::witsml_soap::WMLS_GetFromStore>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::private_protocols::witsml_soap::WMLS_GetFromStore>::FULL_NAME);
}

#[test]
fn private_protocols_witsml_soap_wmls_get_from_store_response() {
    let original = <retp_types::private_protocols::witsml_soap::WMLS_GetFromStoreResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::private_protocols::witsml_soap::WMLS_GetFromStoreResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::private_protocols::witsml_soap::WMLS_GetFromStoreResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::private_protocols::witsml_soap::WMLS_GetFromStoreResponse>::FULL_NAME);
}

#[test]
fn private_protocols_witsml_soap_wmls_get_version() {
    let original = <retp_types::private_protocols::witsml_soap::WMLS_GetVersion>::default();
    let bytes = original.to_avro();
    let back = <retp_types::private_protocols::witsml_soap::WMLS_GetVersion>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::private_protocols::witsml_soap::WMLS_GetVersion>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::private_protocols::witsml_soap::WMLS_GetVersion>::FULL_NAME);
}

#[test]
fn private_protocols_witsml_soap_wmls_get_version_response() {
    let original = <retp_types::private_protocols::witsml_soap::WMLS_GetVersionResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::private_protocols::witsml_soap::WMLS_GetVersionResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::private_protocols::witsml_soap::WMLS_GetVersionResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::private_protocols::witsml_soap::WMLS_GetVersionResponse>::FULL_NAME);
}

#[test]
fn private_protocols_witsml_soap_wmls_update_in_store() {
    let original = <retp_types::private_protocols::witsml_soap::WMLS_UpdateInStore>::default();
    let bytes = original.to_avro();
    let back = <retp_types::private_protocols::witsml_soap::WMLS_UpdateInStore>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::private_protocols::witsml_soap::WMLS_UpdateInStore>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::private_protocols::witsml_soap::WMLS_UpdateInStore>::FULL_NAME);
}

#[test]
fn private_protocols_witsml_soap_wmls_update_in_store_response() {
    let original = <retp_types::private_protocols::witsml_soap::WMLS_UpdateInStoreResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::private_protocols::witsml_soap::WMLS_UpdateInStoreResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::private_protocols::witsml_soap::WMLS_UpdateInStoreResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::private_protocols::witsml_soap::WMLS_UpdateInStoreResponse>::FULL_NAME);
}

#[test]
fn protocol_core_osdu_resume_session() {
    let original = <retp_types::protocol::core_osdu::ResumeSession>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::core_osdu::ResumeSession>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::core_osdu::ResumeSession>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::core_osdu::ResumeSession>::FULL_NAME);
}

#[test]
fn protocol_core_osdu_resume_session_response() {
    let original = <retp_types::protocol::core_osdu::ResumeSessionResponse>::default();
    let bytes = original.to_avro();
    let back = <retp_types::protocol::core_osdu::ResumeSessionResponse>::from_avro(&bytes)
        .unwrap_or_else(|why| panic!("{}: {why}", <retp_types::protocol::core_osdu::ResumeSessionResponse>::FULL_NAME));
    assert_eq!(back, original, "{}", <retp_types::protocol::core_osdu::ResumeSessionResponse>::FULL_NAME);
}

