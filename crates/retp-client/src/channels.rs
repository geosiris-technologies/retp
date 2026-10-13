//! Channels: ChannelSubscribe (21), where a customer asks a store for the
//! channels it holds and subscribes to their new values; and
//! ChannelStreaming (1), where a producer describes its channels and
//! streams them to a consumer that only says *start* and *stop* (§6).
//!
//! Both are read the way notifications are: the protocol's stream kept
//! apart (`Caller::subscribe`), and [`channel_events`] giving what came
//! since the last call, typed as one [`ChannelEvent`] per message. What a
//! WITSML log becomes on the wire - the growing part of a well - is a
//! `DataItem` per index per channel; assembling them into a table is the
//! caller's, above this crate, where the domain lives.
use retp_proto::connection::Event;
use retp_proto::wire::{Body, Delivery};
use retp_types::datatypes::channel_data::{
    ChannelMetadataRecord, ChannelRangeInfo, ChannelSubscribeInfo, DataItem, TruncateInfo,
};
use retp_types::datatypes::object::IndexInterval;
use retp_types::datatypes::{ErrorInfo, Uuid};
use retp_types::dispatch::Message;
use retp_types::protocol::channel_streaming::{StartStreaming, StopStreaming};
use retp_types::protocol::channel_subscribe::{
    GetChannelMetadata, GetRanges, SubscribeChannels, UnsubscribeChannels,
};

use crate::ClientError;
use crate::caller::{Blocking, run};

/// ChannelStreaming, the producer's stream.
pub const CHANNEL_STREAMING: i32 = 1;
/// ChannelSubscribe, the store's channels.
pub const CHANNEL_SUBSCRIBE: i32 = 21;

/// One thing a store or a producer sent on its own about channels.
#[derive(Debug, Clone, PartialEq)]
pub enum ChannelEvent {
    /// New values - of subscribed channels (21, §19.3.5) or of a stream
    /// (1, §6.3.4): one item per channel per index.
    Data(Vec<DataItem>),
    /// A producer describes the channels it streams (1, §6.3.3): the ids
    /// its data items carry, and what they are.
    Metadata(Vec<ChannelMetadataRecord>),
    /// Values already sent were replaced over an interval (21, §19.3.7).
    RangeReplaced {
        /// When, by the store's clock.
        change_time: i64,
        /// Which channels.
        channel_ids: Vec<i64>,
        /// The interval replaced.
        interval: IndexInterval,
        /// The values now.
        data: Vec<DataItem>,
    },
    /// Channels were cut back to an index (21, §19.3.6; 1, §6.3.5).
    Truncated {
        /// Which, and to where.
        channels: Vec<TruncateInfo>,
        /// When, by the store's clock - the stream (1) does not say.
        change_time: Option<i64>,
    },
    /// The store stopped subscriptions (21, §19.3.9) - on its own, or as
    /// the answer to an unsubscribe.
    Stopped {
        /// Why.
        reason: String,
        /// Which channels.
        channel_ids: Vec<i64>,
    },
    /// Something else of the protocol, or an exception: as delivered.
    Other(Delivery),
    /// Not a message of the protocol: the session's own event on this
    /// stream, as it came.
    Session(Event),
}

impl ChannelEvent {
    /// What one event of a channel stream is.
    pub fn of(event: Event) -> ChannelEvent {
        let Event::Delivered(delivery) = event else {
            return ChannelEvent::Session(event);
        };
        let Body::Message(message) = &delivery.body else {
            return ChannelEvent::Other(delivery);
        };
        match message {
            Message::ChannelSubscribeChannelData(m) => ChannelEvent::Data(m.data.clone()),
            Message::ChannelStreamingChannelData(m) => ChannelEvent::Data(m.data.clone()),
            Message::ChannelMetadata(m) => ChannelEvent::Metadata(m.channels.clone()),
            Message::RangeReplaced(m) => ChannelEvent::RangeReplaced {
                change_time: m.change_time,
                channel_ids: m.channel_ids.clone(),
                interval: m.changed_interval.clone(),
                data: m.data.clone(),
            },
            Message::ChannelsTruncated(m) => ChannelEvent::Truncated {
                channels: m.channels.clone(),
                change_time: Some(m.change_time),
            },
            Message::ChannelStreamingTruncateChannels(m) => ChannelEvent::Truncated {
                channels: m.channels.clone(),
                change_time: None,
            },
            Message::SubscriptionsStopped(m) => ChannelEvent::Stopped {
                reason: m.reason.clone(),
                channel_ids: m.channel_ids.values().copied().collect(),
            },
            _ => ChannelEvent::Other(delivery),
        }
    }
}

pub mod asynchronous {
    use super::*;
    use crate::caller::Caller;
    use crate::operations::asynchronous::keyed_outcomes;

    /// The channels at these URIs - a log, a channel set, a channel -, one
    /// answer per URI in the order asked (21, §19.3.1). The ids in the
    /// records are what every data item names.
    pub async fn channel_metadata<K: Caller>(
        client: &mut K,
        uris: &[String],
    ) -> Result<Vec<Result<ChannelMetadataRecord, ErrorInfo>>, ClientError> {
        let request = GetChannelMetadata {
            uris: uris
                .iter()
                .enumerate()
                .map(|(i, u)| ((i + 1).to_string(), u.clone()))
                .collect(),
        };
        let deliveries = client.call(&request).await?;
        let mut found = std::collections::BTreeMap::new();
        for delivery in &deliveries {
            if let Body::Message(Message::GetChannelMetadataResponse(r)) = &delivery.body {
                found.extend(r.metadata.iter().map(|(k, v)| (k.clone(), v.clone())));
            }
        }
        let outcomes = keyed_outcomes(deliveries, uris.len(), |m| {
            matches!(m, Message::GetChannelMetadataResponse(_))
        });
        Ok(outcomes
            .into_iter()
            .enumerate()
            .map(|(i, outcome)| {
                let key = (i + 1).to_string();
                outcome.and_then(|()| {
                    found.remove(&key).ok_or_else(|| ErrorInfo {
                        message: format!("the store answered nothing about channel {key}"),
                        code: 0,
                    })
                })
            })
            .collect())
    }

    /// Subscribe to channels by id (21, §19.3.3): one answer each, in the
    /// order asked. From here on the protocol's stream is kept apart: read
    /// it with [`subscribed_channel_events`].
    pub async fn subscribe_channels<K: Caller>(
        client: &mut K,
        channels: &[ChannelSubscribeInfo],
    ) -> Result<Vec<Result<(), ErrorInfo>>, ClientError> {
        client.subscribe(CHANNEL_SUBSCRIBE);
        let request = SubscribeChannels {
            channels: channels
                .iter()
                .enumerate()
                .map(|(i, c)| ((i + 1).to_string(), c.clone()))
                .collect(),
        };
        let deliveries = client.call(&request).await?;
        Ok(keyed_outcomes(deliveries, channels.len(), |m| {
            matches!(m, Message::SubscribeChannelsResponse(_))
        }))
    }

    /// Unsubscribe channels by id (21, §19.3.8). The store answers
    /// `SubscriptionsStopped`; one outcome per id, in the order asked.
    pub async fn unsubscribe_channels<K: Caller>(
        client: &mut K,
        channel_ids: &[i64],
    ) -> Result<Vec<Result<(), ErrorInfo>>, ClientError> {
        let request = UnsubscribeChannels {
            channel_ids: channel_ids
                .iter()
                .enumerate()
                .map(|(i, id)| ((i + 1).to_string(), *id))
                .collect(),
        };
        let deliveries = client.call(&request).await?;
        Ok(keyed_outcomes(deliveries, channel_ids.len(), |m| {
            matches!(m, Message::SubscriptionsStopped(_))
        }))
    }

    /// Values over index intervals (21, §19.3.10), every part of the answer
    /// gathered; `request_uuid` is what a `CancelGetRanges` would name.
    pub async fn get_ranges<K: Caller>(
        client: &mut K,
        request_uuid: Uuid,
        ranges: &[ChannelRangeInfo],
    ) -> Result<Result<Vec<DataItem>, ErrorInfo>, ClientError> {
        let request = GetRanges {
            request_uuid,
            channel_ranges: ranges.to_vec(),
        };
        let mut data = Vec::new();
        let mut answered = false;
        for delivery in client.call(&request).await? {
            if let Body::Message(Message::GetRangesResponse(r)) = &delivery.body {
                answered = true;
                data.extend(r.data.iter().cloned());
            }
            if let Some(exception) = delivery.exception() {
                let info = match exception {
                    retp_proto::wire::Exception::Single(info) => info,
                    retp_proto::wire::Exception::Map(map) => match map.into_values().next() {
                        Some(info) => info,
                        None => continue,
                    },
                };
                return Ok(Err(info));
            }
        }
        Ok(if answered {
            Ok(data)
        } else {
            Err(crate::operations::asynchronous::nothing_said())
        })
    }

    /// Ask a producer to stream (1, §6.3.1): a message that expects no
    /// answer - what follows comes on its own, read with
    /// [`streamed_channel_events`].
    pub async fn start_streaming<K: Caller>(client: &mut K) -> Result<(), ClientError> {
        client.subscribe(CHANNEL_STREAMING);
        client.tell(&StartStreaming {}).await
    }

    /// Ask a producer to stop (1, §6.3.2). No answer either.
    pub async fn stop_streaming<K: Caller>(client: &mut K) -> Result<(), ClientError> {
        client.tell(&StopStreaming {}).await
    }

    /// What came on `protocol` since the last call - [`CHANNEL_SUBSCRIBE`]
    /// or [`CHANNEL_STREAMING`] -, typed.
    pub async fn channel_events<K: Caller>(
        client: &mut K,
        protocol: i32,
    ) -> Result<Vec<ChannelEvent>, ClientError> {
        Ok(client
            .events(protocol)
            .await?
            .into_iter()
            .map(ChannelEvent::of)
            .collect())
    }

    /// [`channel_events`] of the subscribed channels (21).
    pub async fn subscribed_channel_events<K: Caller>(
        client: &mut K,
    ) -> Result<Vec<ChannelEvent>, ClientError> {
        channel_events(client, CHANNEL_SUBSCRIBE).await
    }

    /// [`channel_events`] of the stream (1).
    pub async fn streamed_channel_events<K: Caller>(
        client: &mut K,
    ) -> Result<Vec<ChannelEvent>, ClientError> {
        channel_events(client, CHANNEL_STREAMING).await
    }
}

/// [`asynchronous::channel_metadata`], on the blocking client.
pub fn channel_metadata<K: Blocking>(
    client: &mut K,
    uris: &[String],
) -> Result<Vec<Result<ChannelMetadataRecord, ErrorInfo>>, ClientError> {
    run(asynchronous::channel_metadata(client, uris))
}

/// [`asynchronous::subscribe_channels`], on the blocking client.
pub fn subscribe_channels<K: Blocking>(
    client: &mut K,
    channels: &[ChannelSubscribeInfo],
) -> Result<Vec<Result<(), ErrorInfo>>, ClientError> {
    run(asynchronous::subscribe_channels(client, channels))
}

/// [`asynchronous::unsubscribe_channels`], on the blocking client.
pub fn unsubscribe_channels<K: Blocking>(
    client: &mut K,
    channel_ids: &[i64],
) -> Result<Vec<Result<(), ErrorInfo>>, ClientError> {
    run(asynchronous::unsubscribe_channels(client, channel_ids))
}

/// [`asynchronous::get_ranges`], on the blocking client.
pub fn get_ranges<K: Blocking>(
    client: &mut K,
    request_uuid: Uuid,
    ranges: &[ChannelRangeInfo],
) -> Result<Result<Vec<DataItem>, ErrorInfo>, ClientError> {
    run(asynchronous::get_ranges(client, request_uuid, ranges))
}

/// [`asynchronous::start_streaming`], on the blocking client.
pub fn start_streaming<K: Blocking>(client: &mut K) -> Result<(), ClientError> {
    run(asynchronous::start_streaming(client))
}

/// [`asynchronous::stop_streaming`], on the blocking client.
pub fn stop_streaming<K: Blocking>(client: &mut K) -> Result<(), ClientError> {
    run(asynchronous::stop_streaming(client))
}

/// [`asynchronous::channel_events`], on the blocking client.
pub fn channel_events<K: Blocking>(client: &mut K, protocol: i32) -> Result<Vec<ChannelEvent>, ClientError> {
    run(asynchronous::channel_events(client, protocol))
}
