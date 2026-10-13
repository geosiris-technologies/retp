//! Tier 1 - the wire, against the specification.
//!
//! Each test is named after the specification section it holds the wire to. Two wires talk to each other here - a client and a server -
//! and where a peer must misbehave, the bytes are written by hand, because a
//! well-behaved `Wire` refuses to send them.

use std::collections::BTreeMap;

use retp_avro::{Avro, Role};
use retp_proto::wire::{
    Body, CodeEntry, Config, ConfigRefusal, Delivery, ErrorRegistry, EtpError, Exception, Flags, Known,
    MalformedException, MessageIds, Options, Origin, Outgoing, Place, Received, RegistryRefusal, SendRefusal,
    Side, Tolerance, Wire, chunk_objects, split_to_fit,
};
use retp_types::datatypes::object::{DataObject, Resource};
use retp_types::datatypes::{
    DataValue, DataValueItem, ErrorInfo, MessageHeader, MessageHeaderExtension, Uuid,
};
use retp_types::dispatch::Message;
use retp_types::protocol::core::{Acknowledge, Ping, ProtocolException};
use retp_types::protocol::discovery::GetResources;
use retp_types::protocol::store::{
    Chunk, GetDataObjects, GetDataObjectsResponse, PutDataObjects, PutDataObjectsResponse,
};

// ---------------------------------------------------------------------------
// Two ends of a connection
// ---------------------------------------------------------------------------

fn client_config() -> Config {
    Config::new()
        .protocol(3, Role::Customer)
        .unwrap()
        .protocol(4, Role::Customer)
        .unwrap()
}

fn server_config() -> Config {
    Config::new()
        .protocol(3, Role::Store)
        .unwrap()
        .protocol(4, Role::Store)
        .unwrap()
}

fn client() -> Wire {
    Wire::new(Side::Client, client_config())
}

fn server() -> Wire {
    Wire::new(Side::Server, server_config())
}

/// The one exception among the replies, with its header.
fn exception_in(received: &Received) -> (MessageHeader, ErrorInfo) {
    let found: Vec<_> = received
        .replies
        .iter()
        .filter(|out| out.header.message_type == 1000)
        .collect();
    assert_eq!(found.len(), 1, "one ProtocolException expected: {received:#?}");
    (found[0].header.clone(), single_error(found[0]))
}

fn body_of(out: &Outgoing) -> &[u8] {
    &out.bytes[out.header.to_avro().len()..]
}

fn single_error(out: &Outgoing) -> ErrorInfo {
    match Exception::from_message(&ProtocolException::from_avro(body_of(out)).unwrap()).unwrap() {
        Exception::Single(error) => error,
        Exception::Map(map) => panic!("a map exception: {map:?}"),
    }
}

fn map_errors(out: &Outgoing) -> BTreeMap<String, ErrorInfo> {
    match Exception::from_message(&ProtocolException::from_avro(body_of(out)).unwrap()).unwrap() {
        Exception::Map(map) => map,
        Exception::Single(error) => panic!("a single exception: {error:?}"),
    }
}

/// A message written by hand - for a peer that breaks a rule on purpose.
fn raw(protocol: i32, message_type: i32, correlation: i64, id: i64, flags: Flags, body: &[u8]) -> Vec<u8> {
    let mut bytes = MessageHeader {
        protocol,
        message_type,
        correlation_id: correlation,
        message_id: id,
        message_flags: flags.0,
    }
    .to_avro();
    bytes.extend_from_slice(body);
    bytes
}

fn object(uri: &str, data: Vec<u8>) -> DataObject {
    DataObject {
        resource: Resource {
            uri: uri.to_string(),
            ..Default::default()
        },
        data,
        ..Default::default()
    }
}

/// A data object that carries a `blobId` **and** its data breaks §3.7.3.2
/// §1.b. Its data is there, so a tolerant end takes it and ignores the
/// `blobId` - a server met in the field does it on every object; a strict end
/// refuses. And a refused answer **ends the request it answers**
/// (`Received::refused_answer`): without it, the caller waited for an answer
/// that had already come and gone, until the timeout.
#[test]
fn a_blob_id_beside_its_data_is_taken_when_tolerant() {
    for tolerance in [Tolerance::Tolerant, Tolerance::Strict] {
        let mut client = Wire::new(Side::Client, client_config().tolerance(tolerance));
        let request = client.request(&get_data_objects()).unwrap().header.message_id;
        let mut both = object("eml:///a", b"<obj/>".to_vec());
        both.blob_id = Some(uuid(7));
        let answer = GetDataObjectsResponse {
            data_objects: BTreeMap::from([("1".to_string(), both)]),
        };
        let received = client.receive(&raw(4, 4, request, 1, Flags::FIN, &answer.to_avro()));
        match tolerance {
            Tolerance::Tolerant => {
                assert!(
                    received.replies.is_empty() && received.refused_answer.is_none(),
                    "{received:#?}"
                );
                let delivery = received.delivery.expect("delivered at once: no Chunk is awaited");
                let Body::Message(Message::GetDataObjectsResponse(response)) = &delivery.body else {
                    panic!("{delivery:#?}");
                };
                assert_eq!(response.data_objects["1"].data, b"<obj/>");
                assert_eq!(response.data_objects["1"].blob_id, None);
            }
            Tolerance::Strict => {
                let (_, error) = exception_in(&received);
                assert_eq!(error.code, EtpError::EINVALID_ARGUMENT.code());
                let (ended, why) = received.refused_answer.clone().expect("the request is ended");
                assert_eq!((ended, why.code()), (request, EtpError::EINVALID_ARGUMENT.code()));
                assert!(!client.is_awaiting(request), "and no longer awaited");
            }
        }
    }
}

/// An exception numbered -1 is attached to nothing - a server met in the field sends them -
/// but with one request out, it can only be about that one: a
/// tolerant end ends that request with what the exception says, instead of
/// waiting for the timeout. Not when strict, not for correlation 0 (an
/// unattached notice, §3.7.2.1 §3.c), and not with two requests out: that
/// would be a guess.
#[test]
fn a_misnumbered_exception_ends_the_only_request_out() {
    let body = Exception::Single(ErrorInfo {
        message: "Internal server error".into(),
        code: -1,
    })
    .into_message()
    .to_avro();
    for (tolerance, correlation, requests, ends) in [
        (Tolerance::Tolerant, -1, 1, true),
        (Tolerance::Strict, -1, 1, false),
        (Tolerance::Tolerant, 0, 1, false),
        (Tolerance::Tolerant, -1, 2, false),
    ] {
        let mut client = Wire::new(Side::Client, client_config().tolerance(tolerance));
        let first = client.request(&get_data_objects()).unwrap().header.message_id;
        for _ in 1..requests {
            client.request(&get_data_objects()).unwrap();
        }
        let received = client.receive(&raw(0, 1000, correlation, -1, Flags::FIN, &body));
        let case = format!("{tolerance:?}, correlation {correlation}, {requests} out");
        assert!(
            received.replies.is_empty(),
            "{case}: an exception is never answered"
        );
        match &received.refused_answer {
            Some((request, why)) => {
                assert!(ends, "{case}: {why}");
                assert_eq!(*request, first, "{case}");
                assert!(why.message().contains("Internal server error"), "{case}: {why}");
                assert!(!client.is_awaiting(first), "{case}");
            }
            None => assert!(!ends && client.is_awaiting(first), "{case}"),
        }
    }
}

fn get_data_objects() -> GetDataObjects {
    GetDataObjects {
        uris: BTreeMap::from([("1".to_string(), "eml:///a".to_string())]),
        ..Default::default()
    }
}

/// The client asks, the server receives: the server's view of the request.
fn requested(client: &mut Wire, server: &mut Wire) -> Delivery {
    let out = client.request(&get_data_objects()).unwrap();
    let received = server.receive(&out.bytes);
    assert!(received.replies.is_empty(), "{received:#?}");
    received.delivery.expect("the request is delivered")
}

// ---------------------------------------------------------------------------
// Message ids - §3.5.4.1 §5
// ---------------------------------------------------------------------------

#[test]
fn message_ids_are_strictly_increasing() {
    let mut client = client();
    let ids: Vec<i64> = (0..5)
        .map(|_| client.notify(&Ping::default()).unwrap().header.message_id)
        .collect();
    assert!(ids.windows(2).all(|w| w[0] < w[1]), "{ids:?}");

    // And the receiver holds the peer to it: an id that goes back is refused.
    let mut server = server();
    let ping = Ping::default().to_avro();
    assert!(
        server
            .receive(&raw(0, 8, 0, 10, Flags::FIN, &ping))
            .replies
            .is_empty()
    );
    let (header, error) = exception_in(&server.receive(&raw(0, 8, 0, 6, Flags::FIN, &ping)));
    assert_eq!(error.code, EtpError::EINVALID_STATE.code(), "{}", error.message);
    assert_eq!(header.correlation_id, 6);
}

#[test]
fn client_ids_are_even_and_nonzero() {
    let mut ids = MessageIds::new(Side::Client);
    let taken: Vec<i64> = (0..4).map(|_| ids.issue()).collect();
    assert_eq!(taken, [2, 4, 6, 8]);
    assert!(!Side::Client.may_use(0) && !Side::Client.may_use(3));
}

/// The one test of the list that fails if `retp-proto` was written for a
/// client: the same counter, configured as a server.
#[test]
fn server_ids_are_odd() {
    let mut ids = MessageIds::new(Side::Server);
    let taken: Vec<i64> = (0..4).map(|_| ids.issue()).collect();
    assert_eq!(taken, [1, 3, 5, 7]);
    let mut server = server();
    let mut client = client();
    let request = requested(&mut client, &mut server);
    let out = server
        .respond(&request, &GetDataObjectsResponse::default())
        .unwrap();
    assert_eq!(out.header.message_id % 2, 1);
    // And the client refuses an even id from the server.
    let (_, error) = exception_in(&client.receive(&raw(0, 8, 0, 4, Flags::FIN, &[])));
    assert_eq!(error.code, EtpError::EINVALID_STATE.code());
}

// ---------------------------------------------------------------------------
// Multipart - §3.7.3.1, issue #191
// ---------------------------------------------------------------------------

/// Six `PutDataObjects`, one correlation, the FIN on the sixth only - the
/// case OSDU's server refused (#191). Refusing it here would be the same fault.
#[test]
fn multipart_shares_one_correlation_id() {
    let mut client = client();
    let mut server = server();
    let mut put = client.begin(4, true, Options::default());
    let mut sent = Vec::new();
    for n in 0..6 {
        let message = PutDataObjects {
            data_objects: BTreeMap::from([(format!("{n}"), object(&format!("eml:///o{n}"), vec![n]))]),
            ..Default::default()
        };
        if n < 5 {
            sent.push(client.part(&mut put, &message).unwrap());
        } else {
            sent.extend(client.last(&mut put, &message).unwrap());
        }
    }
    let first = sent[0].header.message_id;
    let correlations: Vec<i64> = sent.iter().map(|o| o.header.correlation_id).collect();
    assert_eq!(correlations, [0, first, first, first, first, first]);
    let fins: Vec<bool> = sent.iter().map(|o| o.flags().contains(Flags::FIN)).collect();
    assert_eq!(fins, [false, false, false, false, false, true]);

    let mut deliveries = Vec::new();
    for out in &sent {
        let received = server.receive(&out.bytes);
        assert!(
            received.replies.is_empty(),
            "the server refused a legal part: {received:#?}"
        );
        deliveries.push(received.delivery.unwrap());
    }
    let places: Vec<(Place, bool)> = deliveries.iter().map(|d| (d.place, d.last)).collect();
    assert_eq!(places[0], (Place::Opens, false));
    assert!(
        places[1..5]
            .iter()
            .all(|p| *p == (Place::Continues { first }, false))
    );
    assert_eq!(places[5], (Place::Continues { first }, true));

    // The response correlates to the FIRST part (§3.7.3.1 §7.b).
    let response = server
        .respond(&deliveries[5], &PutDataObjectsResponse::default())
        .unwrap();
    assert_eq!(response.header.correlation_id, first);
    let answered = client.receive(&response.bytes).delivery.unwrap();
    assert_eq!(answered.place, Place::Answers { request: first });
    assert!(!client.is_awaiting(first), "the FIN closed the request");
}

#[test]
fn multipart_refused_when_flag_is_false() {
    let mut client = client();
    let mut action = client.begin(4, true, Options::default());
    let refused = client.part(&mut action, &get_data_objects()).unwrap_err();
    assert!(matches!(refused, SendRefusal::NotMultipart { message } if message.ends_with("GetDataObjects")));
    // Alone, the same message goes: it is one message, not a part.
    assert_eq!(client.last(&mut action, &get_data_objects()).unwrap().len(), 1);
}

#[test]
fn map_keys_unique_across_a_multipart() {
    let mut client = client();
    let mut server = server();
    let request = requested(&mut client, &mut server);
    let mut response = server
        .begin_response(&request)
        .for_keys(["1".to_string(), "2".to_string()]);
    response.answered("1").unwrap();
    server
        .part(&mut response, &GetDataObjectsResponse::default())
        .unwrap();
    assert_eq!(response.answered("1"), Err(SendRefusal::KeyTwice("1".into())));
    assert_eq!(response.answered("9"), Err(SendRefusal::UnknownKey("9".into())));
}

#[test]
fn empty_final_part_closes_a_multipart_response() {
    let mut client = client();
    let mut server = server();
    let request = requested(&mut client, &mut server);
    let id = request.header.message_id;
    let mut response = server.begin_response(&request);
    let full = GetDataObjectsResponse {
        data_objects: BTreeMap::from([("1".to_string(), object("eml:///a", b"<a/>".to_vec()))]),
    };
    let part = server.part(&mut response, &full).unwrap();
    let end = server
        .last(&mut response, &GetDataObjectsResponse::default())
        .unwrap();
    assert!(!client.receive(&part.bytes).delivery.unwrap().last);
    let closing = client.receive(&end[0].bytes).delivery.unwrap();
    assert!(closing.last, "an empty message with the FIN closes the response");
    assert!(!client.is_awaiting(id));
    assert_eq!(server.part(&mut response, &full), Err(SendRefusal::Finished));
}

#[test]
fn nothing_is_attached_after_fin() {
    let mut client = client();
    let mut server = server();
    let request = requested(&mut client, &mut server);
    let id = request.header.message_id;
    let out = server
        .respond(&request, &GetDataObjectsResponse::default())
        .unwrap();
    client.receive(&out.bytes);
    // A server that goes on after its FIN.
    let late = raw(
        4,
        4,
        id,
        99,
        Flags::FIN,
        &GetDataObjectsResponse::default().to_avro(),
    );
    let received = client.receive(&late);
    assert!(received.delivery.is_none());
    let (header, error) = exception_in(&received);
    assert_eq!(error.code, EtpError::EINVALID_STATE.code());
    assert!(error.message.contains("closed by its FIN"), "{}", error.message);
    assert_eq!((header.protocol, header.correlation_id), (4, 99));
}

// ---------------------------------------------------------------------------
// Acknowledge - §3.7.2.2
// ---------------------------------------------------------------------------

#[test]
fn ack_is_never_acked() {
    let mut client = client();
    let ask = Options {
        ack: true,
        ..Default::default()
    };
    assert_eq!(
        client.notify_with(&Acknowledge {}, &ask),
        Err(SendRefusal::AckOfAck)
    );
    // Nor answered with one when a peer asks anyway.
    let mut server = server();
    let ping = server.notify(&Ping::default()).unwrap();
    client.receive(&ping.bytes);
    let acknowledge = raw(
        0,
        1001,
        ping.header.message_id,
        2,
        Flags::FIN | Flags::ACK_REQUESTED,
        &[],
    );
    let mut other = server.clone();
    let received = other.receive(&acknowledge);
    assert!(
        received.replies.iter().all(|r| r.header.message_type != 1001),
        "{received:#?}"
    );
}

#[test]
fn ack_only_when_requested() {
    let mut client = client();
    let mut server = server();
    let quiet = client.notify(&Ping::default()).unwrap();
    assert!(server.receive(&quiet.bytes).replies.is_empty());

    let ask = Options {
        ack: true,
        ..Default::default()
    };
    let loud = client.request_with(&GetResources::default(), &ask).unwrap();
    let received = server.receive(&loud.bytes);
    assert_eq!(received.replies.len(), 1);
    let ack = &received.replies[0].header;
    assert_eq!(
        (ack.protocol, ack.message_type, ack.correlation_id),
        (3, 1001, loud.header.message_id)
    );
    assert!(Flags(ack.message_flags).contains(Flags::FIN));
    assert!(!Flags(ack.message_flags).contains(Flags::ACK_REQUESTED));
    let back = client.receive(&received.replies[0].bytes).delivery.unwrap();
    assert_eq!(
        back.place,
        Place::Acknowledges {
            message: loud.header.message_id
        }
    );
}

#[test]
fn ack_is_sent_before_the_refusal() {
    let mut server = server();
    // Protocol 5 was not negotiated; the sender asked for an Ack.
    let received = server.receive(&raw(5, 6, 0, 2, Flags::FIN | Flags::ACK_REQUESTED, &[]));
    let types: Vec<i32> = received.replies.iter().map(|r| r.header.message_type).collect();
    assert_eq!(types, [1001, 1000], "the Ack first, then the exception");
    assert_eq!(
        single_error(&received.replies[1]).code,
        EtpError::EUNSUPPORTED_PROTOCOL.code()
    );
}

// ---------------------------------------------------------------------------
// ProtocolException - §3.7.2.1
// ---------------------------------------------------------------------------

#[test]
fn exception_header_falls_back_to_zero() {
    let mut server = server();
    let received = server.receive(&[0xFF, 0xFF, 0xFF]);
    let (header, error) = exception_in(&received);
    assert_eq!(error.code, EtpError::EINVALID_MESSAGE.code());
    assert_eq!(
        (header.protocol, header.correlation_id),
        (0, 0),
        "nothing of the errant message is known"
    );
}

#[test]
fn exception_is_never_compressed() {
    let mut server = server();
    let mut replies = Vec::new();
    replies.extend(server.receive(&[0xFF]).replies);
    replies.extend(
        server
            .receive(&raw(0, 8, 0, 2, Flags::FIN | Flags::COMPRESSED, &[]))
            .replies,
    );
    replies.extend(server.receive(&raw(9, 1, 0, 4, Flags::FIN, &[])).replies);
    replies.push(server.unattached(&EtpError::EBACKPRESSURE_WARNING.incident()));
    assert!(replies.len() >= 4);
    for reply in replies {
        assert_eq!(reply.header.message_type, 1000);
        assert!(!reply.flags().contains(Flags::COMPRESSED));
    }
}

#[test]
fn compressed_body_is_refused_by_name() {
    let mut server = server();
    for (protocol, message_type, body) in [(0, 8, Vec::new()), (4, 1, get_data_objects().to_avro())] {
        let id = 2 * (protocol as i64 + 1);
        let received = server.receive(&raw(
            protocol,
            message_type,
            0,
            id,
            Flags::FIN | Flags::COMPRESSED,
            &body,
        ));
        let (header, error) = exception_in(&received);
        assert_eq!(
            error.code,
            EtpError::ECOMPRESSION_NOTSUPPORTED.code(),
            "{}",
            error.message
        );
        assert_eq!(header.correlation_id, id);
        assert!(received.delivery.is_none());
    }
}

#[test]
fn protocol_exception_has_error_xor_errors() {
    let both = ProtocolException {
        error: Some(EtpError::ENOT_FOUND.incident().error_info()),
        errors: BTreeMap::from([("1".to_string(), EtpError::ENOT_FOUND.incident().error_info())]),
    };
    assert_eq!(Exception::from_message(&both), Err(MalformedException::Both));
    assert_eq!(
        Exception::from_message(&ProtocolException::default()),
        Err(MalformedException::Neither)
    );

    // Received, a malformed exception is a fault - and not answered: an
    // exception answered with an exception can loop.
    let mut client = client();
    let received = client.receive(&raw(0, 1000, 0, 1, Flags::FIN, &both.to_avro()));
    assert!(received.replies.is_empty());
    assert!(received.delivery.is_none());
    assert_eq!(received.fault.unwrap().code(), EtpError::EINVALID_MESSAGE.code());
}

// ---------------------------------------------------------------------------
// Map responses - §3.7.3
// ---------------------------------------------------------------------------

fn keys(n: usize) -> Vec<String> {
    (1..=n).map(|k| k.to_string()).collect()
}

#[test]
fn map_error_is_not_terminating() {
    let mut client = client();
    let mut server = server();
    let request = requested(&mut client, &mut server);

    let mut partial = server.begin_response(&request).for_keys(keys(3));
    partial.answered("1").unwrap();
    partial.answered("3").unwrap();
    partial
        .failed("2", &EtpError::ENOT_FOUND.with_detail("eml:///b"))
        .unwrap();
    let out = server
        .last(&mut partial, &GetDataObjectsResponse::default())
        .unwrap();
    assert_eq!(out.len(), 2);
    assert!(
        !out[0].flags().contains(Flags::FIN),
        "map errors do not end the response"
    );
    assert_eq!(map_errors(&out[0]).keys().collect::<Vec<_>>(), ["2"]);
    assert!(out[1].flags().contains(Flags::FIN));

    let request = requested(&mut client, &mut server);
    let mut whole = server.begin_response(&request).for_keys(keys(3));
    let out = server
        .fail(
            &mut whole,
            &EtpError::EREQUEST_DENIED.with_detail("the store lost its database"),
        )
        .unwrap();
    assert_eq!(out.len(), 1);
    assert!(out[0].flags().contains(Flags::FIN));
    assert_eq!(single_error(&out[0]).code, EtpError::EREQUEST_DENIED.code());
    // On the client side, the two are two different types, not one shape.
    let delivered = client.receive(&out[0].bytes).delivery.unwrap();
    assert!(matches!(delivered.exception(), Some(Exception::Single(_))));
}

#[test]
fn every_map_key_is_answered_once() {
    let mut client = client();
    let mut server = server();
    let request = requested(&mut client, &mut server);
    let mut response = server.begin_response(&request).for_keys(keys(3));
    response.answered("1").unwrap();
    let out = server
        .last(&mut response, &GetDataObjectsResponse::default())
        .unwrap();
    let owed = map_errors(&out[0]);
    assert_eq!(
        owed.keys().collect::<Vec<_>>(),
        ["2", "3"],
        "the forgotten keys are answered, by name"
    );
    assert!(owed.values().all(|e| e.code == EtpError::EREQUEST_DENIED.code()));
    assert!(out[1].flags().contains(Flags::FIN));
}

#[test]
fn terminal_exception_comes_after_every_sendable_response() {
    let mut client = client();
    let mut server = server();
    let request = requested(&mut client, &mut server);
    let mut response = server.begin_response(&request).for_keys(keys(3));
    response.failed("2", &EtpError::ENOT_FOUND.incident()).unwrap();
    let out = server
        .fail(&mut response, &EtpError::ETIMED_OUT.incident())
        .unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(map_errors(&out[0]).len(), 1, "the map error goes first");
    assert!(!out[0].flags().contains(Flags::FIN));
    assert_eq!(single_error(&out[1]).code, EtpError::ETIMED_OUT.code());
    assert!(out[1].flags().contains(Flags::FIN));
}

#[test]
fn response_count_is_cut_only_after_the_maximum() {
    let mut client = client();
    let mut server = server();
    let request = requested(&mut client, &mut server);
    let mut response = server.begin_response(&request).with_max_items(3);
    response.counted(2).unwrap();
    assert_eq!(
        server.cut(&mut response),
        Err(SendRefusal::CutTooEarly { sent: 2, max: 3 })
    );
    response.counted(1).unwrap();
    assert_eq!(
        response.counted(1),
        Err(SendRefusal::OverResponseCount { max: 3 })
    );
    let cut = server.cut(&mut response).unwrap();
    assert_eq!(single_error(&cut).code, EtpError::ERESPONSECOUNT_EXCEEDED.code());
    assert!(cut.flags().contains(Flags::FIN));
}

// ---------------------------------------------------------------------------
// Error codes - §24
// ---------------------------------------------------------------------------

#[test]
fn error_enum_matches_the_declared_table() {
    // `ETP_SCHEMAS` names the schema root; the default is where it lives today.
    let root = std::env::var_os("ETP_SCHEMAS")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../rc/etp-1.2"));
    let path = root.join("error-codes.json");
    let table: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let declared: Vec<(i64, String, String)> = table["codes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["code"].as_i64().unwrap(),
                c["name"].as_str().unwrap().to_string(),
                c["notes"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    let ours: Vec<(i64, String, String)> = EtpError::ALL
        .iter()
        .map(|e| {
            (
                i64::from(e.code()),
                e.name().to_string(),
                e.description().to_string(),
            )
        })
        .collect();
    assert_eq!(ours, declared);
    assert_eq!(ours.len(), 37);
    let codes: std::collections::BTreeSet<_> = ours.iter().map(|o| o.0).collect();
    let names: std::collections::BTreeSet<_> = ours.iter().map(|o| o.1.clone()).collect();
    assert_eq!((codes.len(), names.len()), (37, 37), "a code or a name twice");
}

#[test]
fn unknown_error_code_is_named_not_swallowed() {
    let registry = ErrorRegistry::new();
    assert_eq!(registry.lookup(11), Known::Energistics(EtpError::ENOT_FOUND));
    assert_eq!(
        registry.lookup(-7),
        Known::Unknown {
            code: -7,
            suspicious: false
        }
    );
    assert!(
        registry.incident(-7, None).is_none(),
        "an endpoint does not send what it cannot name"
    );
}

#[test]
fn custom_codes_are_negative() {
    let registry = ErrorRegistry::new();
    assert_eq!(
        registry.lookup(33),
        Known::Unknown {
            code: 33,
            suspicious: true
        }
    );
    assert_eq!(
        registry.lookup(7777),
        Known::Unknown {
            code: 7777,
            suspicious: true
        }
    );
}

#[test]
fn error_registry_refuses_a_positive_code_without_a_source() {
    let mut registry = ErrorRegistry::new();
    let entry = |code, origin| CodeEntry {
        code,
        name: "EOURS".into(),
        description: "ours".into(),
        origin,
    };
    assert_eq!(
        registry.register(entry(7777, Origin::Custom)),
        Err(RegistryRefusal::PositiveWithoutSource { code: 7777 })
    );
    assert_eq!(
        registry.register(entry(
            -3,
            Origin::Domain {
                publisher: "RESQML".into()
            }
        )),
        Err(RegistryRefusal::NegativeDomainCode { code: -3 })
    );
    registry.register(entry(-3, Origin::Custom)).unwrap();
    registry
        .register(entry(
            9001,
            Origin::Domain {
                publisher: "WITSML ETP guide".into(),
            },
        ))
        .unwrap();
    assert!(matches!(registry.lookup(-3), Known::Registered(e) if e.name == "EOURS"));
    assert_eq!(
        registry.register(entry(-3, Origin::Custom)),
        Err(RegistryRefusal::Duplicate { code: -3 })
    );
    assert_eq!(
        registry.incident(-3, Some("here".into())).unwrap().message(),
        "EOURS: here"
    );
}

#[test]
fn error_registry_never_overrides_a_spec_code() {
    let mut registry = ErrorRegistry::new();
    for code in [3, 5001] {
        let refused = registry.register(CodeEntry {
            code,
            name: "EMINE".into(),
            description: "mine".into(),
            origin: Origin::Domain {
                publisher: "anyone".into(),
            },
        });
        assert!(
            matches!(refused, Err(RegistryRefusal::OverridesSpec { existing }) if existing.code() == code)
        );
    }
    assert_eq!(
        registry.lookup(3),
        Known::Energistics(EtpError::EINVALID_MESSAGETYPE)
    );
}

#[test]
fn incident_message_is_name_then_detail() {
    let detailed = EtpError::EREQUEST_DENIED.with_detail("Cannot change the unit of measure for a channel");
    assert_eq!(
        detailed.message(),
        "EREQUEST_DENIED: Cannot change the unit of measure for a channel"
    );
    assert_eq!(detailed.error_info().code, 6);
    let bare = EtpError::EREQUEST_DENIED.incident();
    assert_eq!(
        bare.message(),
        format!("EREQUEST_DENIED: {}", EtpError::EREQUEST_DENIED.description())
    );
}

// ---------------------------------------------------------------------------
// Routing - §3.5.4.2, §3.6.1
// ---------------------------------------------------------------------------

#[test]
fn receiving_what_our_session_role_sends_is_einvalid_messagetype() {
    let mut client = client();
    assert!(matches!(
        client.notify(&GetDataObjectsResponse::default()),
        Err(SendRefusal::RoleMayNotSend {
            role: Role::Customer,
            ..
        })
    ));
    // A customer that sends it anyway, to a store.
    let mut server = server();
    let received = server.receive(&raw(
        4,
        4,
        0,
        2,
        Flags::FIN,
        &GetDataObjectsResponse::default().to_avro(),
    ));
    let (_, error) = exception_in(&received);
    assert_eq!(
        error.code,
        EtpError::EINVALID_MESSAGETYPE.code(),
        "{}",
        error.message
    );
    assert!(error.message.contains("customer"), "{}", error.message);
}

#[test]
fn the_client_may_be_the_store() {
    // The connection role and the session role are two things.
    let config = Config::new().protocol(4, Role::Store).unwrap();
    let mut client = Wire::new(Side::Client, config);
    assert!(client.notify(&GetDataObjectsResponse::default()).is_ok());
    assert_eq!(
        Config::new().protocol(4, Role::Client).unwrap_err(),
        ConfigRefusal::NotASessionRole(Role::Client)
    );
}

#[test]
fn raw_handlers_only_for_private_protocols() {
    assert_eq!(
        Config::new().private_protocol(1999).unwrap_err(),
        ConfigRefusal::NotPrivate(1999)
    );
    assert_eq!(
        Config::new().protocol(2000, Role::Store).unwrap_err(),
        ConfigRefusal::Private(2000)
    );

    let config = server_config().private_protocol(2100).unwrap();
    let mut server = Wire::new(Side::Server, config);
    let received = server.receive(&raw(2100, 1, 0, 2, Flags::FIN, &[1, 2, 3]));
    let delivery = received.delivery.unwrap();
    assert_eq!(delivery.body, Body::Private(vec![1, 2, 3]));

    let received = server.receive(&raw(2200, 1, 0, 4, Flags::FIN, &[]));
    assert_eq!(
        exception_in(&received).1.code,
        EtpError::EUNSUPPORTED_PROTOCOL.code()
    );
}

// ---------------------------------------------------------------------------
// Header extensions - §3.6.2
// ---------------------------------------------------------------------------

fn extension(key: &str) -> MessageHeaderExtension {
    MessageHeaderExtension {
        extension: BTreeMap::from([(
            key.to_string(),
            DataValue {
                item: DataValueItem::Boolean(true),
            },
        )]),
    }
}

#[test]
fn header_extension_unknown_keys_are_ignored() {
    let mut client = Wire::new(Side::Client, client_config().header_extensions(true));
    let mut server = Wire::new(Side::Server, server_config().header_extensions(true));
    let options = Options {
        extension: Some(extension("x-nobody-knows-this")),
        ..Default::default()
    };
    let out = client.request_with(&get_data_objects(), &options).unwrap();
    assert!(out.flags().contains(Flags::EXTENSION));
    let received = server.receive(&out.bytes);
    assert!(received.replies.is_empty(), "{received:#?}");
    let delivery = received.delivery.unwrap();
    assert_eq!(delivery.extension, Some(extension("x-nobody-knows-this")));
    assert!(matches!(delivery.body, Body::Message(Message::GetDataObjects(_))));
}

#[test]
fn header_extension_flag_without_extension_is_einvalid_message() {
    let body = get_data_objects().to_avro();
    // Not negotiated - and a perfectly readable extension, so that the refusal
    // cannot come from anything but the negotiation.
    let mut well_formed = extension("a").to_avro();
    well_formed.extend_from_slice(&body);
    let mut server = server();
    let received = server.receive(&raw(4, 1, 0, 2, Flags::FIN | Flags::EXTENSION, &well_formed));
    let (_, error) = exception_in(&received);
    assert_eq!(error.code, EtpError::EINVALID_MESSAGE.code());
    assert!(error.message.contains("none was negotiated"), "{}", error.message);

    let mut server = Wire::new(Side::Server, server_config().header_extensions(true));
    // Announced, and omitted: the body is read as the extension and nothing
    // sensible is left.
    let received = server.receive(&raw(4, 1, 0, 2, Flags::FIN | Flags::EXTENSION, &body));
    assert_eq!(exception_in(&received).1.code, EtpError::EINVALID_MESSAGE.code());
    // Two of them.
    let mut two = extension("a").to_avro();
    two.extend_from_slice(&extension("b").to_avro());
    two.extend_from_slice(&body);
    let received = server.receive(&raw(4, 1, 0, 4, Flags::FIN | Flags::EXTENSION, &two));
    assert_eq!(exception_in(&received).1.code, EtpError::EINVALID_MESSAGE.code());
}

// ---------------------------------------------------------------------------
// Size - §3.3.2.9, §3.5.6
// ---------------------------------------------------------------------------

#[test]
fn oversized_message_is_split_or_refused() {
    let limit = 600;
    let mut client = Wire::new(Side::Client, client_config().max_payload(limit));
    let objects: Vec<(String, DataObject)> = (0..20)
        .map(|n| (n.to_string(), object(&format!("eml:///o{n}"), vec![b'x'; 60])))
        .collect();
    let whole = PutDataObjects {
        data_objects: objects.iter().cloned().collect(),
        ..Default::default()
    };
    assert!(matches!(
        client.clone().request(&whole),
        Err(SendRefusal::Oversized { limit: 600, .. })
    ));

    // Multipart: split, and every part goes.
    let parts = split_to_fit(objects, limit, |items| PutDataObjects {
        data_objects: items.into_iter().collect(),
        ..Default::default()
    })
    .unwrap();
    assert!(parts.len() > 1);
    assert_eq!(parts.iter().map(|p| p.data_objects.len()).sum::<usize>(), 20);
    let mut put = client.begin(4, true, Options::default());
    let (last, rest) = parts.split_last().unwrap();
    for part in rest {
        assert!(client.part(&mut put, part).unwrap().bytes.len() <= limit);
    }
    assert!(client.last(&mut put, last).unwrap()[0].bytes.len() <= limit);

    // Not multipart: refused by name, never sent too big.
    let many = GetDataObjects {
        uris: (0..40)
            .map(|n| (n.to_string(), format!("eml:///some/long/uri/{n}")))
            .collect(),
        ..Default::default()
    };
    assert!(
        matches!(client.request(&many), Err(SendRefusal::Oversized { message, .. }) if message.ends_with("GetDataObjects"))
    );

    // And one item too big for any message is named.
    let refused = split_to_fit(vec![vec![0u8; 5000]], limit, |items| Chunk {
        data: items.concat(),
        ..Default::default()
    })
    .unwrap_err();
    assert_eq!(refused.index, 0);
}

// ---------------------------------------------------------------------------
// BLOBs - §3.7.3.2
// ---------------------------------------------------------------------------

fn uuid(n: u8) -> Uuid {
    Uuid([n; 16])
}

#[test]
fn chunks_of_two_objects_never_interleave() {
    let mut objects = [
        object("eml:///a", vec![1; 250]),
        object("eml:///b", vec![2; 150]),
        object("eml:///c", vec![3; 10]),
    ];
    let mut next = 0;
    let chunks: Vec<Chunk> = chunk_objects(objects.iter_mut(), 100, || {
        next += 1;
        uuid(next)
    });
    let sequence: Vec<(u8, bool)> = chunks.iter().map(|c| (c.blob_id.0[0], c.r#final)).collect();
    assert_eq!(
        sequence,
        [(1, false), (1, false), (1, true), (2, false), (2, true)]
    );
    assert!(objects[0].data.is_empty() && objects[1].data.is_empty());
    assert_eq!(objects[2].data.len(), 10, "small enough: sent inline");
    assert_eq!(objects[2].blob_id, None);
}

#[test]
fn chunks_are_reassembled_before_the_handler() {
    let mut client = client();
    let mut server = server();
    let request = requested(&mut client, &mut server);
    let data: Vec<u8> = (0..1000u32).map(|n| (n % 251) as u8).collect();
    let mut objects = [object("eml:///big", data.clone())];
    let chunks: Vec<Chunk> = chunk_objects(objects.iter_mut(), 300, || uuid(7));
    assert_eq!(chunks.len(), 4);
    let carrier = GetDataObjectsResponse {
        data_objects: BTreeMap::from([("1".to_string(), objects[0].clone())]),
    };

    let mut response = server.begin_response(&request);
    let mut out = vec![server.part(&mut response, &carrier).unwrap()];
    let (last, rest) = chunks.split_last().unwrap();
    for chunk in rest {
        out.push(server.part(&mut response, chunk).unwrap());
    }
    out.extend(server.last(&mut response, last).unwrap());

    let mut deliveries = Vec::new();
    for message in &out {
        let received = client.receive(&message.bytes);
        assert!(received.replies.is_empty(), "{received:#?}");
        deliveries.extend(received.delivery);
    }
    assert_eq!(deliveries.len(), 1, "the handler sees the object, never a chunk");
    let delivery = &deliveries[0];
    assert!(delivery.last);
    assert_eq!(
        delivery.place,
        Place::Answers {
            request: request.header.message_id
        }
    );
    let Body::Message(Message::GetDataObjectsResponse(response)) = &delivery.body else {
        panic!("{delivery:#?}");
    };
    let object = &response.data_objects["1"];
    assert_eq!(object.data, data);
    assert_eq!(object.blob_id, None);
}
