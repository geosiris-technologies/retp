//! `rc/etp-1.2/correlation.json` - which message answers which - held against
//! the schemas.
//!
//! The pairing is not in the `.avsc`, so it is declared, from the "Correlation
//! Id Usage" line of each message in the specification. A declared table can be
//! wrong; these tests say where, by confronting it with what the schemas *do*
//! say: that a message exists, in which protocol, who sends it, and whether it
//! may be split. Two sources, and neither is this crate.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde_json::Value;

/// One message, as its schema declares it.
#[derive(Debug, Clone)]
struct Message {
    protocol: i64,
    senders: BTreeSet<String>,
    multipart: bool,
}

/// The schema root: `ETP_SCHEMAS` if set, else where it lives today.
fn root() -> PathBuf {
    std::env::var_os("ETP_SCHEMAS")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../rc/etp-1.2"))
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|why| panic!("{}: {why}", dir.display())) {
        let path = entry.expect("a directory entry").path();
        if path.is_dir() {
            walk(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("avsc") {
            out.push(path);
        }
    }
}

/// Every message of the schemas, keyed `Namespace.Name`.
fn messages() -> BTreeMap<String, Message> {
    let mut files = Vec::new();
    walk(&root().join("Energistics"), &mut files);
    let mut out = BTreeMap::new();
    for path in files {
        let schema: Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("a schema")).expect("JSON");
        let Some(protocol) = schema.get("protocol") else {
            continue;
        };
        let namespace = schema["namespace"].as_str().expect("a namespace");
        let key = format!(
            "{}.{}",
            namespace.rsplit('.').next().unwrap(),
            schema["name"].as_str().unwrap()
        );
        let senders = schema["senderRole"]
            .as_str()
            .expect("senderRole")
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        let message = Message {
            protocol: protocol.as_str().expect("a string").parse().expect("a number"),
            senders,
            multipart: schema["multipartFlag"].as_bool().expect("multipartFlag"),
        };
        assert!(out.insert(key.clone(), message).is_none(), "{key} twice");
    }
    assert_eq!(out.len(), 138, "the schemas define 138 messages");
    out
}

fn table() -> Value {
    let path = root().join("correlation.json");
    serde_json::from_str(&std::fs::read_to_string(&path).expect("correlation.json")).expect("JSON")
}

fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .expect("an array")
        .iter()
        .map(|v| v.as_str().expect("a string").to_string())
        .collect()
}

/// Response → the request(s) it answers.
fn responses(table: &Value) -> BTreeMap<String, Vec<String>> {
    table["responses"]
        .as_object()
        .expect("responses")
        .iter()
        .map(|(k, v)| (k.clone(), strings(&v["to"])))
        .collect()
}

/// Chunk → the multipart message(s) it travels in.
fn chunks(table: &Value) -> BTreeMap<String, Vec<String>> {
    table["chunks"]
        .as_object()
        .expect("chunks")
        .iter()
        .map(|(k, v)| (k.clone(), strings(&v["partOf"])))
        .collect()
}

#[test]
fn request_response_table_is_honest() {
    let messages = messages();
    let table = table();
    let responses = responses(&table);
    let chunks = chunks(&table);
    let universal = strings(&table["universal"]);
    let also = strings(&table["alsoNotification"]);
    let private: BTreeSet<i64> = table["privateProtocols"]
        .as_array()
        .expect("privateProtocols")
        .iter()
        .map(|p| p.as_i64().expect("a number"))
        .collect();
    let requests: BTreeSet<&String> = responses.values().flatten().collect();

    let mut failures = Vec::new();
    let mut exists = |name: &str, role: &str| {
        if !messages.contains_key(name) {
            failures.push(format!("{name}, {role}, is no message of the schemas"));
        }
    };
    for (response, to) in &responses {
        exists(response, "a response");
        for request in to {
            exists(request, "a request");
        }
    }
    for (chunk, part_of) in &chunks {
        exists(chunk, "a chunk");
        for carrier in part_of {
            exists(carrier, "a chunk's carrier");
        }
    }
    for name in universal.iter().chain(&also) {
        exists(name, "a listed message");
    }

    for (response, to) in &responses {
        for request in to {
            if responses.contains_key(request) {
                failures.push(format!(
                    "{response} answers {request}, which is itself a response"
                ));
            }
            if let (Some(a), Some(b)) = (messages.get(response), messages.get(request))
                && a.protocol != b.protocol
            {
                failures.push(format!(
                    "{response} (protocol {}) answers {request} (protocol {})",
                    a.protocol, b.protocol
                ));
            }
        }
    }
    for name in &also {
        if !responses.contains_key(name) {
            failures.push(format!(
                "{name} is listed as also a notification but answers nothing"
            ));
        }
    }

    // Every message is classified exactly once - the private protocols, which
    // the specification does not cover, not at all.
    for (name, message) in &messages {
        let classes = [
            responses.contains_key(name),
            chunks.contains_key(name),
            universal.contains(name),
        ]
        .iter()
        .filter(|&&c| c)
        .count();
        if private.contains(&message.protocol) {
            if classes > 0 || requests.contains(name) {
                failures.push(format!(
                    "{name} is in a private protocol, which the table cannot source"
                ));
            }
        } else if classes > 1 {
            failures.push(format!("{name} is classified {classes} times"));
        } else if classes == 1 && requests.contains(name) {
            failures.push(format!("{name} is both a request and something else"));
        }
    }

    // An erratum names what the specification says and what it meant, and the
    // first must not exist - otherwise it is not an erratum but a rewrite.
    for erratum in table["errata"].as_array().expect("errata") {
        let message = erratum["message"].as_str().expect("message");
        let namespace = message.split('.').next().unwrap();
        let says = format!("{namespace}.{}", erratum["specSays"].as_str().expect("specSays"));
        let means = format!("{namespace}.{}", erratum["means"].as_str().expect("means"));
        if messages.contains_key(&says) {
            failures.push(format!(
                "{message}: the erratum rewrites `{says}`, which does exist"
            ));
        }
        if !messages.contains_key(&means) {
            failures.push(format!(
                "{message}: the erratum reads `{means}`, which does not exist"
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "{} problem(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
    assert!(
        responses.len() > 40 && requests.len() > 30,
        "a table this small proves nothing"
    );
}

/// A response comes from the other side of the conversation.
///
/// The schemas say who sends what; the table says who answers whom. A request
/// sent by the `customer` answered by a message the `customer` sends would be
/// a pairing that cannot happen. The Core messages that either side sends
/// (`client,server`) are the one exception, and it is symmetric.
#[test]
fn a_response_comes_from_the_other_side() {
    let messages = messages();
    let mut failures = Vec::new();
    for (response, to) in responses(&table()) {
        for request in to {
            // A missing name is `request_response_table_is_honest`'s to report.
            let (Some(a), Some(b)) = (messages.get(&response), messages.get(&request)) else {
                continue;
            };
            let both: BTreeSet<String> = ["client", "server"].map(String::from).into();
            if a.senders == both && b.senders == both {
                continue;
            }
            if !a.senders.is_disjoint(&b.senders) {
                failures.push(format!(
                    "{response} (sent by {:?}) answers {request} (sent by {:?})",
                    a.senders, b.senders
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A `Chunk` travels inside a multipart message, sent by whoever sends that.
#[test]
fn a_chunk_travels_in_a_multipart_message() {
    let messages = messages();
    let mut failures = Vec::new();
    for (chunk, part_of) in chunks(&table()) {
        for carrier in part_of {
            let (Some(c), Some(m)) = (messages.get(&chunk), messages.get(&carrier)) else {
                continue;
            };
            if !m.multipart {
                failures.push(format!("{chunk} travels in {carrier}, which may not be split"));
            }
            if !m.senders.is_subset(&c.senders) {
                failures.push(format!(
                    "{carrier} is sent by {:?}, and its {chunk} only by {:?}",
                    m.senders, c.senders
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Where a response is **not** `<request>Response`, and why - the pairs a
/// naming guess would get wrong, each read in the specification.
///
/// Every other response follows the convention, and must: a table entry that
/// breaks it without being listed here is a typo, not an exception.
#[test]
fn the_unconventional_pairs_are_the_ones_the_specification_names() {
    let listed: BTreeMap<&str, &str> = [
        ("Core.OpenSession", "Core.RequestSession"), // §5.3.2
        ("Core.Pong", "Core.Ping"),                  // §5.3.6
        (
            "ChannelDataFrame.GetFrameResponseHeader",
            "ChannelDataFrame.GetFrame",
        ), // §7.3.2
        (
            "ChannelDataFrame.GetFrameResponseRows",
            "ChannelDataFrame.GetFrame",
        ), // §7.3.3
        ("Discovery.GetResourcesEdgesResponse", "Discovery.GetResources"), // §8.3.3
        (
            "StoreNotification.SubscriptionEnded",
            "StoreNotification.UnsubscribeNotifications",
        ), // §10.3.5
        (
            "GrowingObjectNotification.PartSubscriptionEnded",
            "GrowingObjectNotification.UnsubscribePartNotification",
        ), // §12.3.7
        (
            "ChannelSubscribe.SubscriptionsStopped",
            "ChannelSubscribe.UnsubscribeChannels",
        ), // §19.3.9
        ("ChannelDataLoad.ChannelsClosed", "ChannelDataLoad.CloseChannels"), // §20.3.9
    ]
    .into();
    let mut failures = Vec::new();
    let responses = responses(&table());
    for (response, to) in &responses {
        let [request] = to.as_slice() else {
            failures.push(format!("{response} answers {} requests", to.len()));
            continue;
        };
        let conventional = format!("{request}Response") == *response;
        match listed.get(response.as_str()) {
            Some(expected) if *expected != request => failures.push(format!(
                "{response} answers {request}; the specification says {expected}"
            )),
            Some(_) if conventional => failures.push(format!("{response} is conventional after all")),
            Some(_) => {}
            None if !conventional => failures.push(format!(
                "{response} answers {request}: not the convention, and not a listed exception"
            )),
            None => {}
        }
    }
    for name in listed.keys() {
        if !responses.contains_key(*name) {
            failures.push(format!("{name} is a listed exception the table no longer has"));
        }
    }
    // And the convention in the other direction: a message called
    // `…Response` that the table does not classify as one is a missed entry.
    for name in messages().keys() {
        if name.ends_with("Response") && !responses.contains_key(name) && !name.starts_with("WitsmlSoap.") {
            failures.push(format!(
                "{name} is named like a response and the table does not have it"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
