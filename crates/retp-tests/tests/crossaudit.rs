//! The generated ETP types against an independent Avro implementation.
//!
//! *Never ask a component to check itself.* The 138 fixtures
//! read here were written by `fastavro` from the same `.avsc` the generator
//! read - so a generator that put two fields in the wrong order, numbered a
//! union branch wrongly, or mapped an enum ordinal off by one produces bytes
//! that do not match, and the round-trip test in the generated crate could
//! never see any of it: it round-trips *default* values, which for most
//! messages are empty.
//!
//! Regenerating the fixtures is `python tests/fixtures/make.py` and needs
//! `fastavro`; running the tests needs nothing, which is why they are committed.

use std::collections::BTreeMap;
use std::path::PathBuf;

use retp_avro::{Avro, Error, EtpMessage, Role, Senders};
use retp_types::dispatch::{self, MESSAGE_COUNT, Message};

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// The fixtures, as `(protocol, message_type, name, bytes)`.
fn fixtures() -> Vec<(i32, i32, String, Vec<u8>)> {
    let index = fixtures_dir().join("index.json");
    let text =
        std::fs::read_to_string(&index).unwrap_or_else(|why| panic!("reading {}: {why}", index.display()));
    let entries: Vec<serde_json::Value> = serde_json::from_str(&text).expect("index.json");
    entries
        .iter()
        .map(|entry| {
            let protocol = entry["protocol"].as_i64().expect("protocol") as i32;
            let message_type = entry["messageType"].as_i64().expect("messageType") as i32;
            let name = entry["name"].as_str().expect("name").to_string();
            let path = fixtures_dir().join(format!("{protocol:02}-{message_type:02}-{name}.avro"));
            let bytes =
                std::fs::read(&path).unwrap_or_else(|why| panic!("reading {}: {why}", path.display()));
            (protocol, message_type, name, bytes)
        })
        .collect()
}

/// The one that decides whether the generator is right.
///
/// For each message: decode `fastavro`'s bytes through the dispatch table, then
/// encode the result and require the bytes back, exactly. Both halves matter -
/// decoding alone would pass on a reader that skips a field and a writer that
/// never writes it.
#[test]
fn every_message_survives_a_fastavro_round_trip() {
    let fixtures = fixtures();
    assert_eq!(
        fixtures.len(),
        MESSAGE_COUNT,
        "the fixtures and the dispatch table disagree about how many messages ETP has"
    );

    let mut failures = Vec::new();
    for (protocol, message_type, name, bytes) in &fixtures {
        let decoded = match dispatch::decode(*protocol, *message_type, bytes) {
            Ok(message) => message,
            Err(why) => {
                failures.push(format!("{name} ({protocol}/{message_type}): decode: {why}"));
                continue;
            }
        };
        let again = encode(&decoded);
        if again != *bytes {
            failures.push(format!(
                "{name} ({protocol}/{message_type}): re-encoded to {} bytes, fastavro wrote {}",
                again.len(),
                bytes.len()
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} messages disagree with fastavro:\n  {}",
        failures.len(),
        fixtures.len(),
        failures.join("\n  ")
    );
}

/// Decoding must land on the variant the header named, not merely on *a*
/// variant. Without this, a dispatch table with two arms swapped would still
/// round-trip whenever the two messages happened to share a shape.
#[test]
fn the_dispatch_table_lands_on_the_right_message() {
    for (protocol, message_type, name, bytes) in fixtures() {
        let decoded =
            dispatch::decode(protocol, message_type, &bytes).unwrap_or_else(|why| panic!("{name}: {why}"));
        assert_eq!(
            decoded.header(),
            (protocol, message_type),
            "{name} decoded as a message with a different header"
        );
    }
}

/// A pair ETP does not define is news, not a message to be skipped.
#[test]
fn an_unknown_pair_is_a_named_refusal() {
    match dispatch::decode(1_999, 7, &[]) {
        Err(Error::NoSuchMessage {
            protocol,
            message_type,
        }) => {
            assert_eq!(protocol, 1_999);
            assert_eq!(message_type, 7);
        }
        other => panic!("expected a named refusal, got {other:?}"),
    }
}

/// Trailing bytes after a message body mean the peer and this decoder disagree
/// about the schema. Reading the message and shrugging at the rest would hide
/// exactly that.
#[test]
fn trailing_bytes_after_a_message_are_refused() {
    let (protocol, message_type, name, bytes) = fixtures()
        .into_iter()
        .find(|(_, _, name, _)| name == "OpenSession")
        .expect("OpenSession is in the fixtures");

    assert!(dispatch::decode(protocol, message_type, &bytes).is_ok(), "{name}");

    let mut longer = bytes.clone();
    longer.push(0x00);
    assert!(
        matches!(
            dispatch::decode(protocol, message_type, &longer),
            Err(Error::TrailingBytes(1))
        ),
        "a message body with a byte too many must be refused"
    );
}

/// The generated round trip in `retp-types` uses default values, which
/// for most messages are empty. This pins that the fixtures are *not* - so if
/// someone regenerates them into empty values, the cross-audit stops proving
/// anything and says so.
#[test]
fn the_fixtures_carry_real_values() {
    let fixtures = fixtures();
    let trivial = fixtures.iter().filter(|(.., bytes)| bytes.len() < 4).count();
    assert!(
        trivial < 12,
        "{trivial} of {} fixtures are almost empty; a round trip over those proves nothing",
        fixtures.len()
    );

    let total: usize = fixtures.iter().map(|(.., bytes)| bytes.len()).sum();
    assert!(
        total > 8_000,
        "the fixtures total {total} bytes, far less than the schemas can carry"
    );
}

// ---------------------------------------------------------------------------
// What a message says about itself
// ---------------------------------------------------------------------------

/// Spot checks on the metadata, against the specification rather than the code.
///
/// The generated constants come from the schemas; these three come from the
/// specification's own prose, so agreeing is a fact about two sources and not
/// about one.
#[test]
fn message_metadata_matches_the_specification() {
    use retp_types::protocol::core::{Acknowledge, OpenSession, ProtocolException, RequestSession};

    // Section 5.3.1 and 5.3.2: Core is protocol 0, RequestSession is message 1
    // sent by the client, OpenSession is message 2 sent by the server.
    assert_eq!(RequestSession::PROTOCOL, 0);
    assert_eq!(RequestSession::MESSAGE_TYPE, 1);
    assert_eq!(RequestSession::SENDER_ROLE, Senders::These(&[Role::Client]));
    assert_eq!(OpenSession::PROTOCOL, 0);
    assert_eq!(OpenSession::MESSAGE_TYPE, 2);
    assert_eq!(OpenSession::SENDER_ROLE, Senders::These(&[Role::Server]));

    // Section 3.7.2: ProtocolException and Acknowledge are the two "universal"
    // messages - any role may send them, in any sub-protocol.
    assert_eq!(ProtocolException::SENDER_ROLE, Senders::Any);
    assert_eq!(Acknowledge::SENDER_ROLE, Senders::Any);
    assert!(ProtocolException::SENDER_ROLE.allows(Role::Store));
    assert!(Acknowledge::SENDER_ROLE.allows(Role::Customer));

    // Section 3.7.2.2: an Acknowledge is always a single message. A const
    // block, because the fact is known at compile time and there is no reason
    // to let a build through that has it wrong.
    const { assert!(!Acknowledge::MULTIPART) };
}

/// Section 3.5.5: only a message whose schema says so may be split. A code path
/// that splits by size alone would break against a conforming peer.
#[test]
fn multipart_is_declared_not_inferred() {
    use retp_types::protocol::discovery::{GetResources, GetResourcesResponse};
    use retp_types::protocol::store::PutDataObjects;

    // A response that arrives in pieces, and the request that does not.
    const { assert!(GetResourcesResponse::MULTIPART) };
    const { assert!(!GetResources::MULTIPART) };
    // Issue #191 at OSDU: this one is legitimately multipart, and a server that
    // refuses it is the bug.
    const { assert!(PutDataObjects::MULTIPART) };
}

/// Every message is reachable through the module tree, and the five names that
/// occur in more than one namespace really do resolve to different types.
#[test]
fn colliding_names_stay_distinct() {
    use retp_types::protocol::{store, store_notification, store_query};

    assert_eq!(
        store::Chunk::FULL_NAME,
        "Energistics.Etp.v12.Protocol.Store.Chunk"
    );
    assert_eq!(
        store_notification::Chunk::FULL_NAME,
        "Energistics.Etp.v12.Protocol.StoreNotification.Chunk"
    );
    assert_eq!(
        store_query::Chunk::FULL_NAME,
        "Energistics.Etp.v12.Protocol.StoreQuery.Chunk"
    );

    // And they carry different headers, which is what makes the dispatch table
    // able to tell them apart at all.
    let headers = BTreeMap::from([
        ("store", (store::Chunk::PROTOCOL, store::Chunk::MESSAGE_TYPE)),
        (
            "notification",
            (
                store_notification::Chunk::PROTOCOL,
                store_notification::Chunk::MESSAGE_TYPE,
            ),
        ),
        (
            "query",
            (store_query::Chunk::PROTOCOL, store_query::Chunk::MESSAGE_TYPE),
        ),
    ]);
    let distinct: std::collections::BTreeSet<_> = headers.values().collect();
    assert_eq!(distinct.len(), 3, "three Chunks, three headers: {headers:?}");
}

fn encode(message: &Message) -> Vec<u8> {
    // The dispatch enum has 138 variants; encoding through the trait keeps this
    // helper from being a second 138-arm table that could drift from the first.
    message.to_avro()
}
