//! The reads beyond the first object: every GET a protocol the session
//! agreed has, tried on data the store already holds.
//!
//! Every testable GET must be tested, even on data that is not the suite's:
//! a read modifies nothing, so it is not critical. A read changes nothing on the server, so it is asked of what
//! is there - the objects Discovery listed, the channels among them - and a
//! protocol that was not agreed is *not applicable*, a read that found
//! nothing to ask about *not checked*, never a pass.

use energyml_core::identifiers::EtpUri;
use retp_client::channels::{channel_metadata, get_ranges};
use retp_client::notifications::{Watch, subscribe_notifications, unsubscribe_notifications};
use retp_client::{get_data_objects, native};
use retp_proto::wire::Body;
use retp_types::datatypes::channel_data::ChannelRangeInfo;
use retp_types::datatypes::object::{ContextScopeKind, Resource};
use retp_types::dispatch::Message;
use retp_types::protocol::discovery::{GetDeletedResources, GetResources};

use crate::checks::{ask, exception_of, failed, not_applicable, resources_of, timed};
use crate::{Finding, Report, Seen, Session, Verdict};

/// How many listed objects one GetDataObjects asks for at once.
const SEVERAL: usize = 5;
/// How many channels GetChannelMetadata asks about.
const CHANNELS: usize = 3;

/// The reads, on `space` (the dataspace read) and the `resources` it listed.
pub fn run<K: Session>(
    client: &mut K,
    seen: &mut Seen,
    report: &mut Report,
    space: Option<&str>,
    resources: &[Resource],
) {
    let negotiated = client.session();
    let has = |protocol: i32| negotiated.protocols.contains_key(&protocol);
    let objects: Vec<&Resource> = resources
        .iter()
        .filter(|r| EtpUri::parse(&r.uri).ok().and_then(|u| u.uuid).is_some())
        .collect();

    // --- Discovery (3): the tombstones, and an object's neighbours -------------
    if has(3) {
        // Sent by `ask`, not by the client's `deleted_since`, which reads a
        // refusal - or no answer - as « nothing deleted ».
        report.findings.push(match space {
            Some(space) => {
                let request = GetDeletedResources {
                    dataspace_uri: space.to_string(),
                    delete_time_filter: None,
                    data_object_types: Vec::new(),
                };
                match ask(client, seen, &request) {
                    Ok(answer) => {
                        let deleted: Option<usize> = answer
                            .iter()
                            .filter_map(|d| match &d.body {
                                Body::Message(Message::GetDeletedResourcesResponse(r)) => Some(r.deleted_resources.len()),
                                _ => None,
                            })
                            .reduce(|a, b| a + b);
                        match (exception_of(&answer), deleted) {
                            (Some(said), _) => Finding::new("discovery.deleted", "§8.3.4", "GetDeletedResources answers with the data objects deleted from a dataspace", Some(3), Verdict::Fail, format!("{space}: refused: {said}")),
                            (None, Some(n)) => Finding::new(
                                "discovery.deleted",
                                "§8.3.4",
                                "GetDeletedResources answers with the data objects deleted from a dataspace - an empty array when none was",
                                Some(3),
                                Verdict::Pass,
                                format!("{space}: {n} deleted resource(s)"),
                            ),
                            (None, None) => Finding::new("discovery.deleted", "§8.3.4", "GetDeletedResources is answered", Some(3), Verdict::Fail, format!("{space}: neither answered nor refused")),
                        }
                    }
                    Err(why) => failed("discovery.deleted", "§8.3.4", "The store MUST answer GetDeletedResources", Some(3), why),
                }
            }
            None => Finding::new("discovery.deleted", "§8.3.4", "The store MUST answer GetDeletedResources", Some(3), Verdict::NotChecked, "no dataspace to ask about"),
        });
        report.findings.push(match objects.first() {
            Some(first) => neighbours(client, seen, &first.uri),
            None => Finding::new(
                "discovery.neighbours",
                "§8.3.1",
                "GetResources on an object answers with its sources and its targets",
                Some(3),
                Verdict::NotChecked,
                "no object listed to start from",
            ),
        });
    }

    // --- Store (4): several objects at once, and JSON -------------------------
    if has(4) {
        let uris: Vec<String> = objects.iter().take(SEVERAL).map(|r| r.uri.clone()).collect();
        report.findings.push(if uris.len() < 2 {
            Finding::new("store.get.several", "§9.3.1, §3.7.3", "GetDataObjects answers every key of a map of several URIs", Some(4), Verdict::NotChecked, format!("{} object(s) listed: not enough to ask for several", uris.len()))
        } else {
            match timed(seen, "GetDataObjects", || get_data_objects(client, &uris, "xml")) {
                Ok(answers) => {
                    let missing: Vec<String> = uris
                        .iter()
                        .filter_map(|uri| match answers.get(uri) {
                            Some(Ok(object)) if !object.data.is_empty() => None,
                            Some(Ok(_)) => Some(format!("{uri}: no data")),
                            Some(Err(why)) => Some(format!("{uri}: {} ({})", why.message, why.code)),
                            None => Some(format!("{uri}: neither answered nor refused")),
                        })
                        .collect();
                    Finding::new(
                        "store.get.several",
                        "§9.3.1, §3.7.3",
                        "GetDataObjects answers every key of a map of several URIs, each object listed by Discovery returned with its data",
                        Some(4),
                        if missing.is_empty() { Verdict::Pass } else { Verdict::Fail },
                        if missing.is_empty() { format!("{} object(s) in one request, all returned", uris.len()) } else { missing.join("; ") },
                    )
                }
                Err(why) => failed("store.get.several", "§9.3.1", "The store MUST answer GetDataObjects", Some(4), why),
            }
        });
        let json = negotiated.formats.iter().any(|f| f.eq_ignore_ascii_case("json"));
        report.findings.push(match (json, objects.first()) {
            (false, _) => Finding::new(
                "store.get.json",
                "§9.3.1, §3.12.4",
                "GetDataObjects in a format the session agreed",
                Some(4),
                Verdict::NotApplicable,
                "json was not agreed for this session",
            ),
            (true, None) => Finding::new(
                "store.get.json",
                "§9.3.1, §3.12.4",
                "GetDataObjects in a format the session agreed",
                Some(4),
                Verdict::NotChecked,
                "no object listed to ask for",
            ),
            (true, Some(first)) => {
                let uri = first.uri.clone();
                match timed(seen, "GetDataObjects", || {
                    get_data_objects(client, std::slice::from_ref(&uri), "json")
                }) {
                    Ok(answers) => match answers.get(&uri) {
                        Some(Ok(object)) => {
                            let parsed = serde_json::from_slice::<serde_json::Value>(&object.data).is_ok();
                            Finding::new(
                                "store.get.json",
                                "§9.3.1, §3.12.4",
                                "An object asked in json, an agreed format, comes back as JSON",
                                Some(4),
                                if parsed && object.format.eq_ignore_ascii_case("json") {
                                    Verdict::Pass
                                } else {
                                    Verdict::Fail
                                },
                                format!(
                                    "{uri}: {} bytes, format `{}`{}",
                                    object.data.len(),
                                    object.format,
                                    if parsed { "" } else { ", not JSON" }
                                ),
                            )
                        }
                        Some(Err(why)) => Finding::new(
                            "store.get.json",
                            "§9.3.1",
                            "An object asked in json, an agreed format, comes back",
                            Some(4),
                            Verdict::Fail,
                            format!("{uri}: refused: {} ({})", why.message, why.code),
                        ),
                        None => Finding::new(
                            "store.get.json",
                            "§9.3.1, §3.7.3 §4.d",
                            "Each key is answered",
                            Some(4),
                            Verdict::Fail,
                            format!("{uri}: neither answered nor refused"),
                        ),
                    },
                    Err(why) => failed(
                        "store.get.json",
                        "§9.3.1",
                        "The store MUST answer GetDataObjects",
                        Some(4),
                        why,
                    ),
                }
            }
        });
    }

    // --- ChannelSubscribe (21): a channel's metadata, then its first index ---
    if has(21) {
        channel_reads(client, seen, report, &objects);
    } else {
        report
            .findings
            .push(not_applicable("channels.metadata", "§19", Some(21)));
    }

    // --- StoreNotification (5): a watch opened and closed ---------------------
    if has(5) {
        report.findings.push(match space {
            Some(space) => {
                let watch = Watch::of(space, native::random_uuid());
                match timed(seen, "SubscribeNotifications", || subscribe_notifications(client, std::slice::from_ref(&watch))) {
                    Ok(mut answers) => match answers.pop() {
                        Some(Ok(())) => {
                            let ended = timed(seen, "UnsubscribeNotifications", || unsubscribe_notifications(client, watch.request_uuid));
                            let (verdict, said) = match ended {
                                Ok(Ok(_)) => (Verdict::Pass, format!("{space}: subscribed, then unsubscribed")),
                                Ok(Err(why)) => (Verdict::Fail, format!("{space}: subscribed; the unsubscription was refused: {} ({})", why.message, why.code)),
                                Err(why) => (Verdict::Fail, format!("{space}: subscribed; the unsubscription was not answered: {why}")),
                            };
                            Finding::new("notification.subscribe", "§10.3.6, §10.3.1", "SubscribeNotifications on a dataspace is answered, and UnsubscribeNotifications ends it", Some(5), verdict, said)
                        }
                        Some(Err(why)) => Finding::new("notification.subscribe", "§10.3.6", "SubscribeNotifications on a dataspace is answered", Some(5), Verdict::Fail, format!("{space}: refused: {} ({})", why.message, why.code)),
                        None => Finding::new("notification.subscribe", "§10.3.6", "SubscribeNotifications is answered", Some(5), Verdict::Fail, format!("{space}: no answer")),
                    },
                    Err(why) => failed("notification.subscribe", "§10.3.6", "The store MUST answer SubscribeNotifications", Some(5), why),
                }
            }
            None => Finding::new("notification.subscribe", "§10.3.6", "SubscribeNotifications on a dataspace is answered", Some(5), Verdict::NotChecked, "no dataspace to watch"),
        });
    } else {
        report
            .findings
            .push(not_applicable("notification.subscribe", "§10", Some(5)));
    }
}

/// GetResources from an object, both ways: what it references, what
/// references it.
fn neighbours<K: Session>(client: &mut K, seen: &mut Seen, uri: &str) -> Finding {
    let mut said = Vec::new();
    let mut failure = None;
    for scope in [ContextScopeKind::Targets, ContextScopeKind::Sources] {
        let request = GetResources {
            scope,
            ..resources_of(uri, 1)
        };
        match ask(client, seen, &request) {
            Ok(answer) => {
                let listed: Option<usize> = answer
                    .iter()
                    .filter_map(|d| match &d.body {
                        Body::Message(Message::GetResourcesResponse(r)) => Some(r.resources.len()),
                        _ => None,
                    })
                    .reduce(|a, b| a + b);
                match (exception_of(&answer), listed) {
                    (Some(why), _) => failure = Some(format!("{scope:?}: refused: {why}")),
                    (None, Some(n)) => said.push(format!("{n} {scope:?}")),
                    (None, None) => failure = Some(format!("{scope:?}: neither answered nor refused")),
                }
            }
            Err(why) => failure = Some(format!("{scope:?}: not answered: {why}")),
        }
    }
    Finding::new(
        "discovery.neighbours",
        "§8.3.1",
        "GetResources on an object answers with its targets and its sources",
        Some(3),
        if failure.is_none() {
            Verdict::Pass
        } else {
            Verdict::Fail
        },
        match failure {
            None => format!("{uri}: {}", said.join(", ")),
            Some(why) => format!("{uri}: {why}"),
        },
    )
}

/// GetChannelMetadata on channels Discovery listed, then GetRanges on the
/// first index of the first one - a single index, so that a channel of a
/// million rows costs one row.
fn channel_reads<K: Session>(client: &mut K, seen: &mut Seen, report: &mut Report, objects: &[&Resource]) {
    let uris: Vec<String> = objects
        .iter()
        .filter(|r| {
            EtpUri::parse(&r.uri)
                .ok()
                .and_then(|u| u.object_type)
                .is_some_and(|t| t == "Channel")
        })
        .take(CHANNELS)
        .map(|r| r.uri.clone())
        .collect();
    if uris.is_empty() {
        report.findings.push(Finding::new(
            "channels.metadata",
            "§19.3.1",
            "GetChannelMetadata answers for the channels of the store",
            Some(21),
            Verdict::NotChecked,
            "no Channel among the objects listed",
        ));
        return;
    }
    let metadata = match timed(seen, "GetChannelMetadata", || channel_metadata(client, &uris)) {
        Ok(answers) => answers,
        Err(why) => {
            report.findings.push(failed(
                "channels.metadata",
                "§19.3.1",
                "The store MUST answer GetChannelMetadata",
                Some(21),
                why,
            ));
            return;
        }
    };
    let refused: Vec<String> = uris
        .iter()
        .zip(&metadata)
        .filter_map(|(uri, answer)| {
            answer
                .as_ref()
                .err()
                .map(|why| format!("{uri}: {} ({})", why.message, why.code))
        })
        .collect();
    let found: Vec<_> = metadata
        .iter()
        .filter_map(|answer| answer.as_ref().ok())
        .collect();
    report.findings.push(Finding::new(
        "channels.metadata",
        "§19.3.1",
        "GetChannelMetadata answers with the metadata and channel id of each channel asked about",
        Some(21),
        if refused.is_empty() {
            Verdict::Pass
        } else {
            Verdict::Fail
        },
        if refused.is_empty() {
            format!(
                "{} channel(s): {}",
                found.len(),
                found
                    .iter()
                    .map(|m| format!("{} (id {}, {} index(es))", m.channel_name, m.id, m.indexes.len()))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else {
            refused.join("; ")
        },
    ));
    let Some(first) = found.iter().find(|m| !m.indexes.is_empty()) else {
        report.findings.push(Finding::new(
            "channels.ranges",
            "§19.3.10",
            "GetRanges answers with the data of a channel over an interval",
            Some(21),
            Verdict::NotChecked,
            "no channel with an index to ask a range of",
        ));
        return;
    };
    let mut interval = first.indexes[0].interval.clone();
    interval.end_index = interval.start_index.clone();
    let range = ChannelRangeInfo {
        channel_ids: vec![first.id],
        interval,
        secondary_intervals: Vec::new(),
    };
    report.findings.push(
        match timed(seen, "GetRanges", || {
            get_ranges(client, native::random_uuid(), std::slice::from_ref(&range))
        }) {
            Ok(Ok(items)) => {
                let others = items.iter().filter(|item| item.channel_id != first.id).count();
                Finding::new(
                    "channels.ranges",
                    "§19.3.10",
                    "GetRanges answers with the data of the channels asked for, over the interval asked",
                    Some(21),
                    if others == 0 { Verdict::Pass } else { Verdict::Fail },
                    if others == 0 {
                        format!(
                            "{} (id {}): {} data item(s) at its first index",
                            first.channel_name,
                            first.id,
                            items.len()
                        )
                    } else {
                        format!("{} item(s) of channels not asked for", others)
                    },
                )
            }
            Ok(Err(why)) => Finding::new(
                "channels.ranges",
                "§19.3.10",
                "GetRanges answers for a channel of the store",
                Some(21),
                Verdict::Fail,
                format!("{}: refused: {} ({})", first.channel_name, why.message, why.code),
            ),
            Err(why) => failed(
                "channels.ranges",
                "§19.3.10",
                "The store MUST answer GetRanges",
                Some(21),
                why,
            ),
        },
    );
}
