//! Tier 2 - the session, against the specification.
//!
//! Two [`Connection`]s talk to each other: a client and a server, each
//! instantiated with its side and its handlers (ADR-0001). Time is a number
//! the test moves; nothing here waits.

use std::collections::{BTreeMap, VecDeque};

use retp_avro::{Avro, Role};
use retp_proto::capabilities::{self, Capabilities, DEFINITIONS, Problem, Scope, Value, names};
use retp_proto::connection::{
    Authorization, Backpressure, Connection, Declined, Event, Forward, Handled, Handler, Responder, State,
    Step,
};
use retp_proto::continuity::{Continuity, Lifecycle};
use retp_proto::credentials::Credentials;
use retp_proto::describe::{OfferDescription, data_object};
use retp_proto::negotiate::{
    Offer, OfferRefusal, ProtocolOffer, Tolerance, accept_open_session, open_session, v12,
};
use retp_proto::probe::{self, HandshakeSettings, Profile};
use retp_proto::uris::{UriUse, check_uri, is_canonical};
use retp_proto::wire::{Delivery, EtpError, Exception, Flags, Options, Outgoing, Place, SendRefusal};
use retp_types::datatypes::object::DataObject;
use retp_types::datatypes::{
    Contact, DataValue, DataValueItem, MessageHeader, ServerCapabilities, SupportedDataObject,
    SupportedProtocol, Uuid,
};
use retp_types::dispatch::Message;
use retp_types::protocol::core::{ProtocolException, RequestSession};
use retp_types::protocol::discovery::{GetDeletedResources, GetResources, GetResourcesResponse};
use retp_types::protocol::store::PutDataObjects;

const S: i64 = 1_000_000;
const T0: i64 = 1_760_000_000 * S;

// ---------------------------------------------------------------------------
// Two ends
// ---------------------------------------------------------------------------

fn data_objects(types: &[&str]) -> Vec<SupportedDataObject> {
    types
        .iter()
        .map(|t| SupportedDataObject {
            qualified_type: t.to_string(),
            data_object_capabilities: BTreeMap::new(),
        })
        .collect()
}

fn client_offer() -> Offer {
    let mut offer = Offer::new("energyml-rust tests", "0.1", Uuid([1; 16]));
    offer.data_objects = data_objects(&["resqml20.*"]);
    offer
}

fn server_offer() -> Offer {
    let mut offer = Offer::new("a test store", "0.1", Uuid([2; 16]));
    offer.data_objects = data_objects(&["resqml20.*", "eml20.*"]);
    offer
}

/// A Discovery store: answers `GetResources`, declines the rest.
struct Discovery;

impl Handler for Discovery {
    fn offer(&self) -> ProtocolOffer {
        ProtocolOffer::v12(3, Role::Store)
    }

    fn handle(&mut self, delivery: &Delivery, responder: &mut Responder<'_>) -> Result<Handled, Declined> {
        match &delivery.body {
            retp_proto::wire::Body::Message(Message::GetResources(_)) => {
                let out = responder
                    .wire()
                    .respond(delivery, &GetResourcesResponse::default())
                    .unwrap();
                responder.send(out);
                Ok(Handled::Answered)
            }
            _ => Err(Declined("this store does not keep deleted resources".into())),
        }
    }
}

/// A Store (4) that takes parts and says nothing until the last.
struct Store;

impl Handler for Store {
    fn offer(&self) -> ProtocolOffer {
        ProtocolOffer::v12(4, Role::Store)
    }

    fn handle(&mut self, _: &Delivery, _: &mut Responder<'_>) -> Result<Handled, Declined> {
        Ok(Handled::Answered)
    }
}

fn client() -> Connection {
    Connection::client(
        client_offer(),
        vec![
            Box::new(Forward(ProtocolOffer::v12(3, Role::Customer))),
            Box::new(Forward(ProtocolOffer::v12(4, Role::Customer))),
        ],
        None,
        T0,
    )
    .unwrap()
}

fn server_with(offer: Offer, authorizer: Option<Box<dyn retp_proto::connection::Authorizer>>) -> Connection {
    Connection::server(
        offer,
        vec![Box::new(Discovery), Box::new(Store)],
        authorizer,
        Uuid([0xAB; 16]),
        T0,
    )
    .unwrap()
}

fn server() -> Connection {
    server_with(server_offer(), None)
}

/// Carry every message both ways until both ends are quiet; what each learned.
fn exchange(
    client: &mut Connection,
    server: &mut Connection,
    first: Step,
    now: i64,
) -> (Vec<Event>, Vec<Event>) {
    let mut to_server: VecDeque<Outgoing> = first.send.into();
    let mut to_client: VecDeque<Outgoing> = VecDeque::new();
    let (mut client_events, mut server_events) = (first.events, Vec::new());
    for _ in 0..100 {
        if let Some(out) = to_server.pop_front() {
            let step = server.receive(&out.bytes, now);
            server_events.extend(step.events);
            to_client.extend(step.send);
        } else if let Some(out) = to_client.pop_front() {
            let step = client.receive(&out.bytes, now);
            client_events.extend(step.events);
            to_server.extend(step.send);
        } else {
            return (client_events, server_events);
        }
    }
    panic!("the two ends never went quiet");
}

fn opened() -> (Connection, Connection) {
    let mut client = client();
    let mut server = server();
    let start = client.start(T0);
    let (c, s) = exchange(&mut client, &mut server, start, T0);
    assert!(c.contains(&Event::Opened), "{c:#?}");
    assert!(s.contains(&Event::Opened), "{s:#?}");
    (client, server)
}

fn single_error(out: &Outgoing) -> retp_types::datatypes::ErrorInfo {
    let body = &out.bytes[out.header.to_avro().len()..];
    match Exception::from_message(&ProtocolException::from_avro(body).unwrap()).unwrap() {
        Exception::Single(error) => error,
        Exception::Map(map) => panic!("{map:?}"),
    }
}

fn exceptions(step: &Step) -> Vec<(MessageHeader, i32)> {
    step.send
        .iter()
        .filter(|o| o.header.message_type == 1000)
        .map(|o| (o.header.clone(), single_error(o).code))
        .collect()
}

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

#[test]
fn session_opens_between_two_connections() {
    let (client, server) = opened();
    let negotiated = client.negotiated().unwrap();
    assert_eq!(negotiated.protocols.keys().collect::<Vec<_>>(), [&3, &4]);
    assert_eq!(negotiated.protocols[&3].role, Role::Customer);
    assert_eq!(server.negotiated().unwrap().protocols[&3].role, Role::Store);
    assert_eq!(negotiated.session_id, Uuid([0xAB; 16]));
    assert_eq!(negotiated.data_objects[0].qualified_type, "resqml20.*");
    assert_eq!((client.state(), server.state()), (State::Open, State::Open));
}

// ---------------------------------------------------------------------------
// The common connection
// ---------------------------------------------------------------------------

#[test]
fn ping_is_answered_natively_in_both_roles() {
    let (mut client, mut server) = opened();
    let check = |from: &mut Connection, to: &mut Connection| {
        let ping = from.ping(T0 + S);
        let step = to.receive(&ping.bytes, T0 + 2 * S);
        assert_eq!(step.send.len(), 1, "{step:#?}");
        let pong = &step.send[0].header;
        assert_eq!(
            (pong.protocol, pong.message_type, pong.correlation_id),
            (0, 9, ping.header.message_id)
        );
    };
    check(&mut client, &mut server);
    check(&mut server, &mut client);
}

#[test]
fn acknowledge_is_sent_when_the_flag_asks() {
    let (mut client, mut server) = opened();
    let ask = Options {
        ack: true,
        ..Default::default()
    };
    let out = client
        .wire()
        .request_with(&GetResources::default(), &ask)
        .unwrap();
    let step = server.receive(&out.bytes, T0);
    let types: Vec<(i32, i32)> = step
        .send
        .iter()
        .map(|o| (o.header.message_type, o.header.message_flags))
        .collect();
    assert_eq!(types[0].0, 1001, "the Ack first: {types:?}");
    assert_eq!(types[0].1 & 0x10, 0, "an Ack never asks for one");
    assert_eq!(types[1].0, 4, "then the response");
}

#[test]
fn unhandled_protocol_is_answered_eunsupported_protocol() {
    let (_, mut server) = opened();
    let step = server.receive(&raw(5, 6, 0, 1_000_000, Flags::FIN, &[]), T0);
    assert_eq!(exceptions(&step)[0].1, EtpError::EUNSUPPORTED_PROTOCOL.code());
}

#[test]
fn unknown_message_type_is_answered_einvalid_messagetype() {
    let (_, mut server) = opened();
    let step = server.receive(&raw(3, 99, 0, 1_000_000, Flags::FIN, &[]), T0);
    assert_eq!(exceptions(&step)[0].1, EtpError::EINVALID_MESSAGETYPE.code());
    assert_eq!(
        server.state(),
        State::Open,
        "a named exception, not a fallen connection"
    );
}

#[test]
fn declined_operation_is_answered_enotsupported() {
    let (mut client, mut server) = opened();
    let out = client.request(&GetDeletedResources::default(), T0).unwrap();
    let step = server.receive(&out.bytes, T0);
    let found = exceptions(&step);
    assert_eq!(found[0].1, EtpError::ENOTSUPPORTED.code());
    assert_eq!(found[0].0.correlation_id, out.header.message_id);
    // And the client hears it as the answer to its request.
    let heard = client.receive(&step.send[0].bytes, T0);
    assert!(
        matches!(&heard.events[0], Event::Delivered(d) if d.place == Place::Answers { request: out.header.message_id })
    );
}

#[test]
fn a_role_cannot_send_what_the_spec_reserves_to_the_other() {
    let (mut client, _) = opened();
    assert!(matches!(
        client.notify(&GetResourcesResponse::default()),
        Err(SendRefusal::RoleMayNotSend {
            role: Role::Customer,
            ..
        })
    ));
}

#[test]
fn we_only_announce_what_we_fully_implement() {
    // An offer that claims three protocols, and a connection that implements one.
    let offer = client_offer()
        .protocol(ProtocolOffer::v12(3, Role::Customer))
        .unwrap()
        .protocol(ProtocolOffer::v12(4, Role::Customer))
        .unwrap()
        .protocol(ProtocolOffer::v12(5, Role::Customer))
        .unwrap();
    let mut client = Connection::client(
        offer,
        vec![Box::new(Forward(ProtocolOffer::v12(3, Role::Customer)))],
        None,
        T0,
    )
    .unwrap();
    let start = client.start(T0);
    let body = &start.send[0].bytes[start.send[0].header.to_avro().len()..];
    let request = RequestSession::from_avro(body).unwrap();
    let asked: Vec<i32> = request.requested_protocols.iter().map(|p| p.protocol).collect();
    assert_eq!(asked, [3], "the offer's own list is not what is announced");
}

#[test]
fn core_is_not_listed_in_supported_protocols() {
    assert_eq!(
        client_offer()
            .protocol(ProtocolOffer::v12(0, Role::Customer))
            .unwrap_err(),
        OfferRefusal::CoreListed
    );
    let (client, _) = opened();
    let request = client.offer().request_session(T0, 0);
    assert!(request.requested_protocols.iter().all(|p| p.protocol != 0));
    // A client that lists it anyway is refused by a strict server - a
    // tolerant one ignores the entry (`a_request_session_is_taken_as_meant_when_tolerant`).
    let mut bad = request.clone();
    bad.requested_protocols.push(SupportedProtocol {
        protocol: 0,
        protocol_version: v12(),
        role: "server".into(),
        protocol_capabilities: BTreeMap::new(),
    });
    let mut strict = with_protocols(server_offer(), &[(3, Role::Store)]);
    strict.tolerance = Tolerance::Strict;
    let refused = open_session(&strict, &bad, Uuid([9; 16]), T0, 0).unwrap_err();
    assert_eq!(refused.code(), EtpError::EINVALID_ARGUMENT.code());
}

fn with_protocols(mut offer: Offer, protocols: &[(i32, Role)]) -> Offer {
    for (protocol, role) in protocols {
        offer = offer.protocol(ProtocolOffer::v12(*protocol, *role)).unwrap();
    }
    offer
}

// ---------------------------------------------------------------------------
// Negotiation - §5.2.1.1, §5.2.2
// ---------------------------------------------------------------------------

#[test]
fn inconsistent_session_roles_are_einvalid_operation() {
    let server = with_protocols(server_offer(), &[(4, Role::Store)]);
    let mut request = with_protocols(client_offer(), &[(4, Role::Customer)]).request_session(T0, 0);
    request.requested_protocols.push(SupportedProtocol {
        protocol: 5,
        protocol_version: v12(),
        role: "customer".into(),
        protocol_capabilities: BTreeMap::new(),
    });
    let refused = open_session(&server, &request, Uuid([9; 16]), T0, 0).unwrap_err();
    assert_eq!(refused.code(), EtpError::EINVALID_OPERATION.code(), "{refused}");
    // And an offer cannot hold both.
    assert!(matches!(
        with_protocols(client_offer(), &[(4, Role::Customer)]).protocol(ProtocolOffer::v12(5, Role::Store)),
        Err(OfferRefusal::Inconsistent(Role::Customer, Role::Store))
    ));
}

#[test]
fn open_session_changing_version_or_role_is_invalid() {
    let client = with_protocols(client_offer(), &[(3, Role::Customer)]);
    let request = client.request_session(T0, 0);
    let good = open_session(
        &with_protocols(server_offer(), &[(3, Role::Store)]),
        &request,
        Uuid([9; 16]),
        T0,
        0,
    )
    .unwrap()
    .0;
    assert!(accept_open_session(&client, &request, &good).is_ok());

    let mut role = good.clone();
    role.supported_protocols[0].role = "customer".into();
    let mut version = good.clone();
    version.supported_protocols[0].protocol_version.minor = 1;
    // What cannot be taken as meant, even by a tolerant client: the server
    // claiming the role this client plays, another minor version.
    for (what, open) in [
        ("the role this client plays itself", role.clone()),
        ("version 1.1.0.0", version.clone()),
    ] {
        let refused = accept_open_session(&client, &request, &open).unwrap_err();
        assert_eq!(refused.code(), EtpError::EREQUEST_DENIED.code(), "{what}");
        assert!(refused.message().contains(what), "{what}: {}", refused.message());
    }
    // Both faults are named together, not the first alone.
    let mut both = role;
    both.supported_protocols.push(SupportedProtocol {
        protocol_version: version.supported_protocols[0].protocol_version.clone(),
        ..version.supported_protocols[0].clone()
    });
    let refused = accept_open_session(&client, &request, &both).unwrap_err();
    assert!(
        refused.message().contains("role") && refused.message().contains("version 1.1.0.0"),
        "{}",
        refused.message()
    );
}

/// An RDDMS server's `OpenSession`, as an older ETP implementation wrote it: Core listed, role
/// `server` in every protocol, two protocols not requested, compression
/// `string`, a data object type without its `.*`. Tolerant, the session opens
/// with each fault named - the rule it breaks, and what was made of it;
/// strict, it is refused with all of them.
#[test]
fn an_rddms_open_session_is_taken_with_every_deviation_named() {
    let mut client = with_protocols(client_offer(), &[(3, Role::Customer), (4, Role::Customer)]);
    let request = client.request_session(T0, 0);
    let protocol = |number: i32, role: &str| SupportedProtocol {
        protocol: number,
        protocol_version: v12(),
        role: role.into(),
        protocol_capabilities: BTreeMap::new(),
    };
    let mut deviant = open_session(
        &with_protocols(server_offer(), &[(3, Role::Store), (4, Role::Store)]),
        &request,
        Uuid([9; 16]),
        T0,
        0,
    )
    .unwrap()
    .0;
    deviant.supported_protocols = vec![
        protocol(0, "server"),
        protocol(3, "server"),
        protocol(4, "server"),
        protocol(9, "server"),
        protocol(25, "server"),
    ];
    deviant.supported_compression = "string".into();
    deviant.supported_data_objects = data_objects(&["resqml20", "not a type"]);

    let negotiated = accept_open_session(&client, &request, &deviant).unwrap();
    assert_eq!(negotiated.protocols.keys().copied().collect::<Vec<_>>(), [3, 4]);
    assert!(
        negotiated.protocols.values().all(|p| p.role == Role::Customer),
        "this client's role is its own"
    );
    assert_eq!(negotiated.compression, None);
    let types: Vec<&str> = negotiated
        .data_objects
        .iter()
        .map(|d| d.qualified_type.as_str())
        .collect();
    assert_eq!(types, ["resqml20.*"]);
    let rules: Vec<&str> = negotiated.deviations.iter().map(|d| d.rule).collect();
    assert_eq!(
        rules,
        [
            "§5.2.1.1 step 9.b.iv",
            "§5.2.2 row 5.2",
            "§5.2.2 row 5.2",
            "§5.2.2 row 5.2",
            "§5.2.2 row 5.2",
            "§5.3.2 OpenSession.supportedCompression",
            "§23.20 SupportedDataObject.qualifiedType",
            "§23.20 SupportedDataObject.qualifiedType",
        ]
    );
    let said: Vec<String> = negotiated.deviations.iter().map(ToString::to_string).collect();
    assert!(
        said[1].contains("`server`") && said[1].contains("taken as `store`"),
        "{said:#?}"
    );
    assert!(
        said[3].contains("DataArray (protocol 9)") && said[3].contains("not agreed"),
        "{said:#?}"
    );
    assert!(
        said[6].contains("read as `resqml20.*`") && said[7].contains("dropped"),
        "{said:#?}"
    );

    client.tolerance = Tolerance::Strict;
    let refused = accept_open_session(&client, &request, &deviant).unwrap_err();
    for fault in ["Core", "`server`", "protocol 25", "`string`", "`resqml20`"] {
        assert!(
            refused.message().contains(fault),
            "{fault}: {}",
            refused.message()
        );
    }
}

/// The server agrees to a type this client never asked for - an RDDMS server answers
/// `resqml20` whatever is asked. The agreed types are an intersection
/// (§5.2.2 row 6.1), so it is a deviation; it is taken all the same, this
/// client reading whatever it is sent. A type the request covers is none -
/// in either direction. Strict, it is refused.
#[test]
fn a_type_agreed_but_not_requested_is_taken_and_named() {
    let mut client = with_protocols(client_offer(), &[(3, Role::Customer)]);
    client.data_objects = data_objects(&["eml20.*", "resqml22.obj_Asked"]);
    let request = client.request_session(T0, 0);
    let mut open = open_session(
        &with_protocols(server_offer(), &[(3, Role::Store)]),
        &request,
        Uuid([9; 16]),
        T0,
        0,
    )
    .unwrap()
    .0;
    open.supported_data_objects =
        data_objects(&["eml20.obj_EpcExternalPartReference", "resqml22.*", "resqml20.*"]);

    let negotiated = accept_open_session(&client, &request, &open).unwrap();
    let types: Vec<&str> = negotiated
        .data_objects
        .iter()
        .map(|d| d.qualified_type.as_str())
        .collect();
    assert_eq!(
        types,
        ["eml20.obj_EpcExternalPartReference", "resqml22.*", "resqml20.*"],
        "all agreed"
    );
    let said: Vec<String> = negotiated.deviations.iter().map(ToString::to_string).collect();
    assert_eq!(said.len(), 1, "{said:#?}");
    assert!(
        said[0].contains("`resqml20.*`, which was not requested") && said[0].contains("§5.2.2 row 6.1"),
        "{said:#?}"
    );

    client.tolerance = Tolerance::Strict;
    let refused = accept_open_session(&client, &request, &open).unwrap_err();
    assert!(
        refused.message().contains("`resqml20.*`"),
        "{}",
        refused.message()
    );
}

/// A client that reads any type asks for whatever the server announces:
/// widened to the server's list, its session agrees to all of it - the
/// intersection (§5.2.2 row 6.1) loses nothing. What the offer covers
/// already is not added twice; a family without its `.*` is read with it,
/// what is no type is left out - both named, neither refused.
#[test]
fn an_offer_widened_to_the_server_agrees_to_all_it_has() {
    let server = with_protocols(server_offer(), &[(3, Role::Store)]);
    let agreed = |client: &Offer| {
        let request = client.request_session(T0, 0);
        let open = open_session(&server, &request, Uuid([9; 16]), T0, 0).unwrap().0;
        let negotiated = accept_open_session(client, &request, &open).unwrap();
        let mut types: Vec<String> = negotiated
            .data_objects
            .iter()
            .map(|d| d.qualified_type.clone())
            .collect();
        types.sort();
        types
    };
    let client = with_protocols(client_offer(), &[(3, Role::Customer)]);
    assert_eq!(
        agreed(&client),
        ["resqml20.*"],
        "the client's list is the narrower"
    );

    let (widened, what) = client.widened_to(&server.data_objects);
    assert_eq!(what.added, ["eml20.*"]);
    assert!(what.deviations.is_empty());
    assert_eq!(agreed(&widened), ["eml20.*", "resqml20.*"]);

    let (_, what) = client_offer().widened_to(&data_objects(&[
        "resqml20.obj_Covered",
        "witsml20",
        "not a type",
        "eml21.DataAssuranceRecord",
    ]));
    assert_eq!(what.added, ["witsml20.*", "eml21.DataAssuranceRecord"]);
    let said: Vec<String> = what.deviations.iter().map(ToString::to_string).collect();
    assert_eq!(said.len(), 2, "{said:#?}");
    assert!(
        said[0].contains("read as `witsml20.*`") && said[1].contains("`not a type`"),
        "{said:#?}"
    );
}

/// The server side tolerates the same way: a `RequestSession` listing Core,
/// or asking for a role no sub-protocol has, is taken as meant - strict, it
/// is refused as before.
#[test]
fn a_request_session_is_taken_as_meant_when_tolerant() {
    let client = with_protocols(client_offer(), &[(3, Role::Customer)]);
    let mut request = client.request_session(T0, 0);
    request.requested_protocols[0].role = "server".into();
    request.requested_protocols.insert(
        0,
        SupportedProtocol {
            protocol: 0,
            protocol_version: v12(),
            role: "server".into(),
            protocol_capabilities: BTreeMap::new(),
        },
    );
    let mut server = with_protocols(server_offer(), &[(3, Role::Store)]);
    let (open, negotiated) = open_session(&server, &request, Uuid([9; 16]), T0, 0).unwrap();
    assert_eq!(open.supported_protocols.len(), 1);
    assert_eq!(open.supported_protocols[0].role, "store");
    assert_eq!(negotiated.deviations.len(), 2, "{:#?}", negotiated.deviations);

    server.tolerance = Tolerance::Strict;
    let refused = open_session(&server, &request, Uuid([9; 16]), T0, 0).unwrap_err();
    assert_eq!(refused.code(), EtpError::EINVALID_ARGUMENT.code());
}

/// An answer the wire refuses ends its request at once, as an event the
/// caller waits for - and the offer's tolerance is the wire's: the same
/// answer (a `blobId` beside its data) is delivered to a tolerant client and
/// ends the request of a strict one. Neither waits for the timeout.
#[test]
fn a_refused_answer_ends_its_request_at_once() {
    for tolerance in [Tolerance::Tolerant, Tolerance::Strict] {
        let mut offer = client_offer();
        offer.tolerance = tolerance;
        let mut client = Connection::client(
            offer,
            vec![Box::new(Forward(ProtocolOffer::v12(4, Role::Customer)))],
            None,
            T0,
        )
        .unwrap();
        let mut server = server();
        let start = client.start(T0);
        let (c, _) = exchange(&mut client, &mut server, start, T0);
        assert!(c.contains(&Event::Opened));
        let request = client
            .request(
                &retp_types::protocol::store::GetDataObjects {
                    uris: BTreeMap::from([("1".to_string(), "eml:///a".to_string())]),
                    ..Default::default()
                },
                T0,
            )
            .unwrap()
            .header
            .message_id;
        let mut both = DataObject {
            data: b"<obj/>".to_vec(),
            ..Default::default()
        };
        both.blob_id = Some(Uuid([7; 16]));
        let answer = retp_types::protocol::store::GetDataObjectsResponse {
            data_objects: BTreeMap::from([("1".to_string(), both)]),
        };
        let step = client.receive(&raw(4, 4, request, 101, Flags::FIN, &answer.to_avro()), T0);
        let refused = step.events.iter().find_map(|e| match e {
            Event::AnswerRefused { request: r, incident } => Some((*r, incident.code())),
            _ => None,
        });
        match tolerance {
            Tolerance::Tolerant => {
                assert_eq!(refused, None, "{:#?}", step.events);
                assert!(
                    step.events
                        .iter()
                        .any(|e| matches!(e, Event::Delivered(d) if d.place == Place::Answers { request }))
                );
            }
            Tolerance::Strict => {
                assert_eq!(
                    refused,
                    Some((request, EtpError::EINVALID_ARGUMENT.code())),
                    "{:#?}",
                    step.events
                );
            }
        }
        let later = client.tick(T0 + 400 * S);
        assert!(
            !later.events.iter().any(|e| matches!(e, Event::TimedOut { .. })),
            "{:#?}",
            later.events
        );
    }
}

/// An `OpenSession` that lists Core breaks §5.2.1.1 step 9.b.iv and agrees
/// to nothing more for it - Core is spoken anyway. So the entry is skipped
/// and **named**, and the session opens: a server met in the field
/// does it. A list with Core alone still agrees to no protocol.
#[test]
fn a_core_entry_in_open_session_is_ignored_and_named() {
    let client = with_protocols(client_offer(), &[(3, Role::Customer)]);
    let request = client.request_session(T0, 0);
    let good = open_session(
        &with_protocols(server_offer(), &[(3, Role::Store)]),
        &request,
        Uuid([9; 16]),
        T0,
        0,
    )
    .unwrap()
    .0;
    assert!(
        accept_open_session(&client, &request, &good)
            .unwrap()
            .deviations
            .is_empty()
    );

    let core = SupportedProtocol {
        protocol: 0,
        protocol_version: v12(),
        role: "server".into(),
        protocol_capabilities: BTreeMap::new(),
    };
    let mut with_core = good.clone();
    with_core.supported_protocols.insert(0, core.clone());
    let negotiated = accept_open_session(&client, &request, &with_core).unwrap();
    assert_eq!(negotiated.protocols.keys().copied().collect::<Vec<_>>(), [3]);
    assert_eq!(negotiated.deviations.len(), 1);
    assert_eq!(
        negotiated.deviations[0].rule, "§5.2.1.1 step 9.b.iv",
        "{:?}",
        negotiated.deviations
    );

    let mut only_core = good.clone();
    only_core.supported_protocols = vec![core];
    let refused = accept_open_session(&client, &request, &only_core).unwrap_err();
    assert!(
        refused.message().contains("none of the requested protocols"),
        "{}",
        refused.message()
    );
}

#[test]
fn unopenable_session_closes_cleanly() {
    for bad in ["unreadable", "role changed"] {
        let mut client = client();
        let start = client.start(T0);
        let request_id = start.send[0].header.message_id;
        let body = match bad {
            "unreadable" => vec![0xFF, 0xFF],
            _ => {
                let request = client.offer().request_session(T0, 0);
                let mut open = open_session(
                    &with_protocols(server_offer(), &[(3, Role::Store), (4, Role::Store)]),
                    &request,
                    Uuid([9; 16]),
                    T0,
                    0,
                )
                .unwrap()
                .0;
                open.supported_protocols[0].role = "customer".into();
                open.to_avro()
            }
        };
        let step = client.receive(&raw(0, 2, request_id, 1, Flags::FIN, &body), T0);
        let sent: Vec<(i32, i32)> = step
            .send
            .iter()
            .map(|o| (o.header.protocol, o.header.message_type))
            .collect();
        assert_eq!(
            sent,
            [(0, 1000), (0, 5)],
            "{bad}: the exception, then CloseSession"
        );
        assert_eq!(
            single_error(&step.send[0]).code,
            EtpError::EREQUEST_DENIED.code(),
            "{bad}"
        );
        assert!(step.close, "{bad}");
        assert_eq!(client.state(), State::Closed);
    }
}

/// An offer is described in a few lines - in code, or in JSON - and the two
/// say the same thing, down to what the `RequestSession` carries.
#[test]
fn an_offer_is_described_the_same_in_code_and_in_json() {
    let base = || Offer::new("an application", "1.0", Uuid([1; 16]));
    let in_code = base()
        .with_types(["resqml20.*"])
        .unwrap()
        .with_type(
            "eml20.*",
            data_object(&[("SupportsPut", Value::Boolean(false))]).unwrap(),
        )
        .unwrap()
        .with_formats(["xml", "json"])
        .unwrap()
        .with_capability(names::MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE, Value::Long(8_000_000))
        .unwrap()
        .with_tolerance(Tolerance::Strict);
    let description = OfferDescription::parse(
        r#"{
            "types": ["resqml20.*", { "type": "eml20.*", "capabilities": { "SupportsPut": false } }],
            "formats": ["xml", "json"],
            "protocols": [3, 4],
            "endpoint": { "MaxWebSocketMessagePayloadSize": 8000000 },
            "tolerance": "strict"
        }"#,
    )
    .unwrap();
    assert_eq!(description.protocols, Some(vec![3, 4]));
    let in_json = description.apply(base()).unwrap();
    assert_eq!(in_code, in_json);

    let request = in_json.request_session(T0, 0);
    let types: Vec<&str> = request
        .supported_data_objects
        .iter()
        .map(|d| d.qualified_type.as_str())
        .collect();
    assert_eq!(types, ["resqml20.*", "eml20.*"]);
    assert_eq!(
        request.supported_data_objects[1].data_object_capabilities["SupportsPut"].item,
        DataValueItem::Boolean(false)
    );
    assert!(
        request.supported_data_objects[0]
            .data_object_capabilities
            .is_empty()
    );
    assert_eq!(request.supported_formats, ["xml", "json"]);
    assert_eq!(
        request.endpoint_capabilities["MaxWebSocketMessagePayloadSize"].item,
        DataValueItem::Long(8_000_000)
    );
}

/// What an offer would refuse from a peer, it does not announce: each is
/// refused by name - in code and in JSON.
#[test]
fn an_offer_announces_nothing_it_would_refuse() {
    let base = || Offer::new("an application", "1.0", Uuid([1; 16]));
    let refused = [
        (
            base().with_types(["resqml20"]).unwrap_err(),
            "`resqml20.*` names the family",
        ),
        (
            base().with_types(["not a type"]).unwrap_err(),
            "not a qualified data object type",
        ),
        (base().with_formats(["yaml"]).unwrap_err(), "`yaml`"),
        (
            base().with_formats(Vec::<&str>::new()).unwrap_err(),
            "at least one",
        ),
        (
            base()
                .with_capability("MaxWebSocketMessagePayloadSizes", Value::Long(1))
                .unwrap_err(),
            "defines no such",
        ),
        (
            base()
                .with_capability(names::MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE, Value::Boolean(true))
                .unwrap_err(),
            "makes it a Long",
        ),
        (
            data_object(&[("MaxDataObjectSize", Value::Long(10))]).unwrap_err(),
            "outside the bounds",
        ),
        (
            base()
                .with_type("resqml20.*", Capabilities::empty(Scope::Endpoint))
                .unwrap_err(),
            "Endpoint capabilities",
        ),
    ];
    for (refusal, says) in &refused {
        assert!(refusal.to_string().contains(says), "{says}: {refusal}");
    }

    for (json, says) in [
        (r#"{"type": ["resqml20.*"]}"#, "unknown key `type`"),
        (
            r#"{"types": [{"type": "resqml20.*", "capabilities": {"SupportsPut": "no"}}]}"#,
            "true or false",
        ),
        (
            r#"{"types": [{"type": "resqml20.*", "capability": {}}]}"#,
            "unknown key `capability`",
        ),
        (
            r#"{"endpoint": {"MaxWebSocketMessagePayloadSize": "big"}}"#,
            "a whole number",
        ),
        (r#"{"endpoint": {"NoSuchCapability": 1}}"#, "defines no such"),
        (r#"{"tolerance": "maybe"}"#, r#""tolerant" or "strict""#),
        (r#"{"protocols": ["three"]}"#, "protocol numbers"),
        ("[1]", "a JSON object"),
    ] {
        let refusal = OfferDescription::parse(json).unwrap_err();
        assert!(refusal.to_string().contains(says), "{json}: {refusal}");
    }
    let family = OfferDescription::parse(r#"{"types": ["resqml20"]}"#).unwrap();
    assert!(
        family.apply(base()).is_err(),
        "what parses is still checked when applied"
    );
}

/// A quiet session is kept alive by a `Ping` - by a client, every 30 s by
/// default, never before; anything crossing puts the next one off. Its `Pong`
/// is the connection's own: no event, the watermark moves. A server does not
/// ping unless told to.
#[test]
fn a_client_keeps_a_quiet_session_alive_and_a_server_does_not() {
    let is_ping = |step: &Step| {
        step.send
            .iter()
            .any(|o| (o.header.protocol, o.header.message_type) == (0, 8))
    };
    let mut client = client();
    let mut server = server();
    assert_eq!(client.keepalive(), Some(std::time::Duration::from_secs(30)));
    assert_eq!(server.keepalive(), None);
    let start = client.start(T0);
    let (c, _) = exchange(&mut client, &mut server, start, T0);
    assert!(c.contains(&Event::Opened));

    assert!(!is_ping(&client.tick(T0 + 29 * S)), "not before its period");
    let ping = client.tick(T0 + 31 * S);
    assert!(
        is_ping(&ping),
        "{:?}",
        ping.send
            .iter()
            .map(|o| (o.header.protocol, o.header.message_type))
            .collect::<Vec<_>>()
    );
    let pong = server.receive(&ping.send[0].bytes, T0 + 31 * S);
    let heard = client.receive(&pong.send[0].bytes, T0 + 32 * S);
    assert!(
        heard.events.is_empty(),
        "the keepalive's Pong is not the application's: {:?}",
        heard.events
    );
    assert_eq!(
        client.watermark(),
        Some(T0 + 31 * S),
        "the server's clock, as the Pong said it"
    );

    // Activity puts the next one off: a request sent…
    let asked = client
        .request(
            &GetResources {
                context: Default::default(),
                ..Default::default()
            },
            T0 + 50 * S,
        )
        .unwrap();
    assert!(
        !is_ping(&client.tick(T0 + 70 * S)),
        "20 s after the request, its answer not yet in"
    );
    // … and a message received.
    let answer = server.receive(&asked.bytes, T0 + 70 * S);
    for out in &answer.send {
        client.receive(&out.bytes, T0 + 75 * S);
    }
    assert!(!is_ping(&client.tick(T0 + 95 * S)), "20 s after the answer");
    assert!(is_ping(&client.tick(T0 + 106 * S)));

    // A server, by default: never.
    assert!(!is_ping(&server.tick(T0 + 1000 * S)));
    server.set_keepalive(Some(std::time::Duration::from_secs(10)));
    assert!(is_ping(&server.tick(T0 + 1011 * S)), "when told to");

    // A client told not to: never.
    client.set_keepalive(None);
    assert!(!is_ping(&client.tick(T0 + 5000 * S)));
}

/// `next_deadline` names the moment `tick` acts: nothing a microsecond
/// before, the act at it. Whoever drives a connection can then sleep until
/// it instead of ticking at a fixed interval.
#[test]
fn the_next_deadline_is_when_tick_acts_and_none_when_nothing_is_due() {
    let is_ping = |step: &Step| {
        step.send
            .iter()
            .any(|o| (o.header.protocol, o.header.message_type) == (0, 8))
    };
    let (mut client, mut server) = opened();

    // A quiet server, no keepalive, nothing asked: nothing to wake up for.
    assert_eq!(server.next_deadline(), None);

    // The client's keepalive.
    let ping_at = client.next_deadline().expect("a client keeps its session alive");
    assert!(!is_ping(&client.tick(ping_at - 1)), "not a microsecond before");
    let ping = client.tick(ping_at);
    assert!(is_ping(&ping), "at the deadline");
    let pong = server.receive(&ping.send[0].bytes, ping_at);
    client.receive(&pong.send[0].bytes, ping_at);

    // A request out: its response timeout is due before a second keepalive
    // only once the keepalive has gone out and is itself waiting.
    let asked_at = ping_at + 1000;
    client
        .request(
            &GetResources {
                context: Default::default(),
                ..Default::default()
            },
            asked_at,
        )
        .unwrap();
    let keepalive_at = client.next_deadline().unwrap();
    assert!(keepalive_at > asked_at, "activity puts the keepalive off");
    let second = client.tick(keepalive_at);
    assert!(is_ping(&second));
    // The keepalive's Ping is never answered: the request's timeout and the
    // Ping's are what is left, and the earlier is the request's.
    let timeout_at = client.next_deadline().unwrap();
    assert!(
        client.tick(timeout_at - 1).events.is_empty(),
        "no timeout a microsecond before"
    );
    let late = client.tick(timeout_at);
    assert!(
        late.events.iter().any(|e| matches!(e, Event::TimedOut { .. })),
        "{:?}",
        late.events
    );

    // A closed session has nothing due.
    let closing = client.close("done");
    exchange(&mut client, &mut server, closing, timeout_at);
    assert_eq!(client.state(), retp_proto::connection::State::Closed);
    assert_eq!(client.next_deadline(), None);
}

/// Every line logged in this test binary - one global subscriber per process.
static LOGGED: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

struct Capture;

/// Reads the `message` field of an event: the text, whatever the other fields.
struct Text(String);

impl tracing::field::Visit for Text {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut text = Text(String::new());
        event.record(&mut text);
        LOGGED
            .lock()
            .unwrap()
            .push(format!("{} {}", event.metadata().level(), text.0));
    }
}

/// Tolerated is not unseen: what a tolerant connection takes as meant reaches
/// the log as a warning, with the peer's name - whatever the application does
/// with the session.
#[test]
fn deviations_reach_the_log() {
    use tracing_subscriber::layer::SubscriberExt;
    // One global subscriber per process: already installed is fine.
    let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Capture));
    let mut client = client();
    let start = client.start(T0);
    let request_id = start.send[0].header.message_id;
    let request = client.offer().request_session(T0, 0);
    let mut open = open_session(
        &with_protocols(server_offer(), &[(3, Role::Store), (4, Role::Store)]),
        &request,
        Uuid([9; 16]),
        T0,
        0,
    )
    .unwrap()
    .0;
    open.application_name = "a deviant store".into();
    open.supported_compression = "string".into();
    let step = client.receive(&raw(0, 2, request_id, 1, Flags::FIN, &open.to_avro()), T0);
    assert!(step.events.contains(&Event::Opened), "{:#?}", step.events);
    let logged = LOGGED.lock().unwrap();
    assert!(
        logged
            .iter()
            .any(|l| l.starts_with("WARN a deviant store:") && l.contains("compression `string`")),
        "{logged:#?}"
    );
}

#[test]
fn payload_limit_is_the_smaller_of_two() {
    let mut small = server_offer();
    small.endpoint = small
        .endpoint
        .set(names::MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE, Value::Long(2000))
        .unwrap();
    let mut large = client_offer();
    large.endpoint = large
        .endpoint
        .set(names::MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE, Value::Long(1_000_000))
        .unwrap();
    let mut client = Connection::client(
        large,
        vec![Box::new(Forward(ProtocolOffer::v12(4, Role::Customer)))],
        None,
        T0,
    )
    .unwrap();
    let mut server = server_with(small, None);
    let start = client.start(T0);
    exchange(&mut client, &mut server, start, T0);
    assert_eq!(client.negotiated().unwrap().max_payload(), Some(2000));
    assert_eq!(server.negotiated().unwrap().max_payload(), Some(2000));
    // And the client's wire holds to the server's limit, not its own.
    let big = PutDataObjects {
        data_objects: BTreeMap::from([(
            "1".to_string(),
            DataObject {
                data: vec![0; 5000],
                ..Default::default()
            },
        )]),
        ..Default::default()
    };
    assert!(matches!(
        client.request(&big, T0),
        Err(SendRefusal::Oversized { limit: 2000, .. })
    ));
}

// ---------------------------------------------------------------------------
// Authorization - §5.2.1.1 steps 2-6, §3.13.5
// ---------------------------------------------------------------------------

fn bearer(token: &str) -> Credentials {
    Credentials::bearer(token).unwrap()
}

fn requiring(expires_at: i64) -> Option<Box<dyn retp_proto::connection::Authorizer>> {
    Some(Box::new(move |authorization: &str, _now: i64| {
        if authorization == "Bearer good" || authorization == "Bearer renewed" {
            let expires_at = if authorization == "Bearer renewed" {
                expires_at + 1000 * S
            } else {
                expires_at
            };
            Authorization::Granted {
                expires_at: Some(expires_at),
            }
        } else {
            Authorization::Challenge(vec![
                r#"Bearer authz_server="https://auth.example", scope="etp""#.into(),
            ])
        }
    }))
}

#[test]
fn authorize_round_trip_then_resend_request_session() {
    let mut client = client();
    let mut server = server_with(server_offer(), requiring(T0 + 3600 * S));
    let start = client.start(T0);
    let first_request = start.send[0].header.message_id;
    let (events, _) = exchange(&mut client, &mut server, start, T0);
    let challenges = events
        .iter()
        .find_map(|e| match e {
            Event::AuthorizationNeeded { challenges } => Some(challenges.clone()),
            _ => None,
        })
        .expect("the client is told how to get a token");
    assert!(challenges[0].contains("authz_server"));
    assert_eq!(client.state(), State::Authorizing);

    let authorize = client.authorize(&bearer("good"), T0);
    let mut sent_by_client = vec![authorize.clone()];
    let step = server.receive(&authorize.bytes, T0);
    let response = client.receive(&step.send[0].bytes, T0);
    sent_by_client.extend(response.send.iter().cloned());
    // The RequestSession goes again, with a new id, once authorized.
    let again = &response.send[0].header;
    assert_eq!((again.protocol, again.message_type), (0, 1));
    assert!(again.message_id > first_request);
    let (c, s) = exchange(&mut client, &mut server, response, T0);
    assert!(c.contains(&Event::Opened) && s.contains(&Event::Opened));
}

#[test]
fn expiring_authorization_is_renewed_or_closes() {
    for renew in [true, false] {
        let mut client = Connection::client(
            client_offer(),
            vec![Box::new(Forward(ProtocolOffer::v12(3, Role::Customer)))],
            Some(bearer("good")),
            T0,
        )
        .unwrap();
        let mut server = server_with(server_offer(), requiring(T0 + 120 * S)).warn_expiring_before(60);
        let start = client.start(T0);
        let (c, _) = exchange(&mut client, &mut server, start, T0);
        assert!(
            c.contains(&Event::Opened),
            "the token given up front is used: {c:#?}"
        );

        assert!(server.tick(T0 + 30 * S).send.is_empty());
        let warning = server.tick(T0 + 61 * S);
        let (header, code) = exceptions(&warning)[0].clone();
        assert_eq!(
            (code, header.protocol, header.correlation_id),
            (EtpError::EAUTHORIZATION_EXPIRING.code(), 0, 0)
        );
        let heard = client.receive(&warning.send[0].bytes, T0 + 61 * S);
        assert_eq!(heard.events, [Event::AuthorizationExpiring]);

        if renew {
            let authorize = client.authorize(&bearer("renewed"), T0 + 62 * S);
            let answer = server.receive(&authorize.bytes, T0 + 62 * S);
            client.receive(&answer.send[0].bytes, T0 + 62 * S);
            assert!(
                server.tick(T0 + 200 * S).send.is_empty(),
                "renewed: the session goes on"
            );
            assert_eq!(server.state(), State::Open);
        } else {
            let end = server.tick(T0 + 120 * S);
            let sent: Vec<i32> = end.send.iter().map(|o| o.header.message_type).collect();
            assert_eq!(sent, [1000, 5], "EAUTHORIZATION_EXPIRED, then CloseSession");
            assert_eq!(
                single_error(&end.send[0]).code,
                EtpError::EAUTHORIZATION_EXPIRED.code()
            );
            assert!(end.close);
        }
    }
}

// ---------------------------------------------------------------------------
// Time - the connection never reads a clock
// ---------------------------------------------------------------------------

#[test]
fn timeouts_read_the_clock_they_are_given() {
    let (mut client, _) = opened();
    let out = client.request(&GetResources::default(), T0).unwrap();
    assert!(client.tick(T0 + 299 * S).events.is_empty());
    let step = client.tick(T0 + 301 * S);
    assert!(
        matches!(&step.events[0], Event::TimedOut { request, incident } if *request == out.header.message_id && incident.code() == EtpError::ETIMED_OUT.code()),
        "{step:#?}"
    );
    // And nothing in the crate reads the system clock.
    let mut files = Vec::new();
    collect(
        std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src")),
        &mut files,
    );
    assert!(files.len() > 5);
    for file in files {
        let text = std::fs::read_to_string(&file).unwrap();
        for clock in ["SystemTime::now", "Instant::now", "chrono::Utc::now"] {
            assert!(
                !text.contains(clock),
                "{} reads the clock ({clock})",
                file.display()
            );
        }
    }
}

fn collect(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

fn put(n: u8) -> PutDataObjects {
    PutDataObjects {
        data_objects: BTreeMap::from([(
            n.to_string(),
            DataObject {
                data: vec![n],
                ..Default::default()
            },
        )]),
        ..Default::default()
    }
}

/// A Ping waits no longer than any request: a Pong that never comes back
/// correlated to it - one correlated to 0, say - is `ETIMED_OUT`
/// after ResponseTimeoutPeriod; one that does answer it ends the wait.
#[test]
fn a_ping_without_its_pong_times_out() {
    let (mut client, mut server) = opened();
    let ping = client.ping(T0);
    let timed_out = |step: &retp_proto::connection::Step, id: i64| {
        step.events
            .iter()
            .any(|e| matches!(e, Event::TimedOut { request, .. } if *request == id))
    };
    assert!(!timed_out(&client.tick(T0 + 299 * S), ping.header.message_id));
    let step = client.tick(T0 + 301 * S);
    assert!(timed_out(&step, ping.header.message_id), "{step:#?}");
    // Answered, it is watched no more.
    let answered = client.ping(T0 + 400 * S);
    let pong = server.receive(&answered.bytes, T0 + 401 * S);
    client.receive(&pong.send[0].bytes, T0 + 402 * S);
    assert!(!timed_out(
        &client.tick(T0 + 1000 * S),
        answered.header.message_id
    ));
}

#[test]
fn multipart_gap_over_the_timeout_is_etimed_out() {
    let (mut client, mut server) = opened();
    let mut action = client.wire().begin(4, true, Options::default());
    let first = client.wire().part(&mut action, &put(1)).unwrap();
    let second = client.wire().part(&mut action, &put(2)).unwrap();
    server.receive(&first.bytes, T0);
    server.receive(&second.bytes, T0 + 10 * S);
    assert!(
        server.tick(T0 + 60 * S).send.is_empty(),
        "50 s since the last part: within the 60 s"
    );
    let step = server.tick(T0 + 71 * S);
    let (header, code) = exceptions(&step)[0].clone();
    assert_eq!(
        (code, header.correlation_id),
        (EtpError::ETIMED_OUT.code(), first.header.message_id)
    );
    // The action is over: a late part is refused.
    let late = client.wire().last(&mut action, &put(3)).unwrap();
    let refused = server.receive(&late[0].bytes, T0 + 72 * S);
    assert_eq!(exceptions(&refused)[0].1, EtpError::EINVALID_STATE.code());
}

#[test]
fn concurrent_multipart_over_the_limit_is_cancelled() {
    let mut offer = server_offer();
    offer.endpoint = offer
        .endpoint
        .set(names::MAX_CONCURRENT_MULTIPART, Value::Long(1))
        .unwrap();
    let mut client = client();
    let mut server = server_with(offer, None);
    let start = client.start(T0);
    exchange(&mut client, &mut server, start, T0);

    let mut one = client.wire().begin(4, true, Options::default());
    let mut two = client.wire().begin(4, true, Options::default());
    let a = client.wire().part(&mut one, &put(1)).unwrap();
    let b = client.wire().part(&mut two, &put(2)).unwrap();
    assert!(exceptions(&server.receive(&a.bytes, T0)).is_empty());
    let step = server.receive(&b.bytes, T0);
    let (header, code) = exceptions(&step)[0].clone();
    assert_eq!(
        (code, header.correlation_id),
        (EtpError::EMULTIPART_CANCELLED.code(), b.header.message_id)
    );
}

#[test]
fn backpressure_warns_then_closes() {
    let (_, server) = opened();
    let mut server = server.with_backpressure(Backpressure {
        warn_at: 100,
        limit_at: 200,
    });
    assert!(server.pressure(50).send.is_empty());
    let warn = server.pressure(120);
    let (header, code) = exceptions(&warn)[0].clone();
    assert_eq!(
        (code, header.protocol, header.correlation_id),
        (EtpError::EBACKPRESSURE_WARNING.code(), 0, 0)
    );
    assert!(server.pressure(130).send.is_empty(), "warned once");
    let end = server.pressure(250);
    let sent: Vec<i32> = end.send.iter().map(|o| o.header.message_type).collect();
    assert_eq!(sent, [1000, 5]);
    assert_eq!(
        single_error(&end.send[0]).code,
        EtpError::EBACKPRESSURE_LIMIT_EXCEEDED.code()
    );
    assert!(end.close);
}

#[test]
fn watermark_advances_on_pong() {
    let (mut client, mut server) = opened();
    for later in [100, 5000] {
        let ping = client.ping(T0 + later * S);
        let pong = server.receive(&ping.bytes, T0 + (later + 1) * S);
        client.receive(&pong.send[0].bytes, T0 + (later + 2) * S);
        assert_eq!(
            client.watermark(),
            Some(T0 + (later + 1) * S),
            "the server's clock, from its Pong"
        );
    }
}

// ---------------------------------------------------------------------------
// Continuity - #232
// ---------------------------------------------------------------------------

#[test]
fn session_id_is_never_reused_to_reconnect() {
    let (client, _) = opened();
    let negotiated = client.negotiated().unwrap();
    let kept = Continuity::keep(negotiated, client.watermark().unwrap());
    assert!(
        !format!("{kept:?}").to_lowercase().contains("session"),
        "{kept:?}"
    );
    // The next session's opening carries nothing of the last one's id.
    let mut next = self::client();
    let start = next.start(T0 + 3600 * S);
    let bytes = &start.send[0].bytes;
    assert!(
        !bytes.windows(16).any(|w| w == [0xAB; 16]),
        "the old session id is in the new RequestSession"
    );
}

#[test]
fn resume_replays_authorization() {
    let mut path = Vec::new();
    let mut state = Lifecycle::Interrupted;
    while state != Lifecycle::Open {
        state = state.next();
        path.push(state);
    }
    assert_eq!(
        path,
        [
            Lifecycle::Probing,
            Lifecycle::Connecting,
            Lifecycle::Authorizing,
            Lifecycle::Negotiating,
            Lifecycle::CatchingUp,
            Lifecycle::Open
        ]
    );
}

fn continuity(watermark: i64, earliest: i64) -> Continuity {
    Continuity {
        watermark,
        retention_seconds: 86_400,
        earliest_retained: earliest,
        // The capability's own default (§3.3.2.2).
        propagation_seconds: 5,
        protocols: BTreeMap::new(),
    }
}

/// **A catch-up asks from before the watermark, not from it.** A change made
/// just before the cut may only have become visible just after, and asking
/// from the watermark exactly steps over it - so the store's
/// `ChangePropagationPeriod` comes off first (§26.6.3 §3.a). Asking too early
/// costs a few objects read twice, which is free: a read is idempotent.
#[test]
fn the_propagation_delay_comes_off_the_watermark() {
    assert_eq!(continuity(T0, 0).window(T0 + 3600 * S), Ok(T0 - 5 * S));
}

/// And the **same moved time** decides whether it is too late. A watermark
/// inside the window by a second is outside it once the delay is taken off:
/// asking from there would be answered `ERETENTION_PERIOD_EXCEEDED` (5001),
/// so it is a full reload, said here rather than discovered on the wire.
#[test]
fn the_propagation_delay_decides_the_retention_too() {
    let too_late = continuity(T0, T0 - S).window(T0 + 60 * S).unwrap_err();
    assert_eq!(too_late.oldest_kept, T0 - S);
    assert_eq!(too_late.watermark, T0);
}

#[test]
fn catch_up_beyond_retention_is_named() {
    assert_eq!(continuity(T0, 0).window(T0 + 3600 * S), Ok(T0 - 5 * S));
    let too_late = continuity(T0, 0).window(T0 + 2 * 86_400 * S).unwrap_err();
    assert_eq!(
        too_late.incident().code(),
        EtpError::ERETENTION_PERIOD_EXCEEDED.code()
    );
    assert!(too_late.incident().message().contains("full reload"));
}

#[test]
fn earliest_retained_shortens_the_window() {
    // Within the retention period, but the server lost what came before it
    // restarted.
    let lost = continuity(T0, T0 + S).window(T0 + 3600 * S).unwrap_err();
    assert_eq!(lost.oldest_kept, T0 + S);
}

// ---------------------------------------------------------------------------
// Capabilities
// ---------------------------------------------------------------------------

#[test]
fn capability_table_matches_the_kind_enums() {
    // `ETP_SCHEMAS` names the schema root; the default is where it lives today.
    let root = std::env::var_os("ETP_SCHEMAS")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../rc/etp-1.2"));
    let path = root.join("capabilities.json");
    let table: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    for (scope, key) in [
        (Scope::Endpoint, "endpoint"),
        (Scope::Protocol, "protocol"),
        (Scope::DataObject, "dataObject"),
    ] {
        let mut kinds: Vec<String> = table["kinds"][key]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        let mut described: Vec<String> = table["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|c| c["scopes"].as_array().unwrap().iter().any(|s| s == key))
            .map(|c| c["name"].as_str().unwrap().to_string())
            .collect();
        kinds.sort();
        described.sort();
        assert_eq!(
            kinds, described,
            "{key}: the Avro enumeration and the prose tables disagree"
        );
        let mut ours: Vec<String> = DEFINITIONS
            .iter()
            .filter(|d| d.scopes.contains(&scope))
            .map(|d| d.name.to_string())
            .collect();
        ours.sort();
        assert_eq!(ours, kinds, "{key}: DEFINITIONS and the declared table disagree");
    }
    // And every value agrees, not only the names.
    for c in table["capabilities"].as_array().unwrap() {
        let name = c["name"].as_str().unwrap();
        let d = DEFINITIONS.iter().find(|d| d.name == name).unwrap();
        assert_eq!(d.min, c["min"].as_i64(), "{name}.min");
        assert_eq!(d.max, c["max"].as_i64(), "{name}.max");
        let default = match d.default {
            Some(capabilities::DefaultValue::Long(v)) => serde_json::json!(v),
            Some(capabilities::DefaultValue::Boolean(v)) => serde_json::json!(v),
            None => serde_json::Value::Null,
        };
        assert_eq!(default, c["default"], "{name}.default");
    }
}

fn long(value: i64) -> DataValue {
    DataValue {
        item: DataValueItem::Long(value),
    }
}

#[test]
fn out_of_bounds_capability_is_named() {
    let map = BTreeMap::from([
        (names::MULTIPART_MESSAGE_TIMEOUT_PERIOD.to_string(), long(90)),
        (
            names::RESPONSE_TIMEOUT_PERIOD.to_string(),
            DataValue {
                item: DataValueItem::String("300".into()),
            },
        ),
    ]);
    let read = Capabilities::read(&map, Scope::Endpoint);
    assert_eq!(
        read.long(names::MULTIPART_MESSAGE_TIMEOUT_PERIOD),
        Some(60),
        "brought to the bound"
    );
    assert!(read.problems().contains(&Problem::OutOfBounds {
        name: names::MULTIPART_MESSAGE_TIMEOUT_PERIOD.into(),
        sent: 90,
        used: 60
    }));
    assert_eq!(
        read.long(names::RESPONSE_TIMEOUT_PERIOD),
        Some(300),
        "a string is not guessed at: the default"
    );
    assert!(
        read.problems()
            .iter()
            .any(|p| matches!(p, Problem::WrongType { name, .. } if name == names::RESPONSE_TIMEOUT_PERIOD))
    );
}

#[test]
fn unknown_capabilities_are_ignored_not_fatal() {
    let map = BTreeMap::from([
        (
            "x-osdu-data-partition".to_string(),
            DataValue {
                item: DataValueItem::String("opendes".into()),
            },
        ),
        (
            names::MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE.to_string(),
            long(4_000_000),
        ),
    ]);
    let read = Capabilities::read(&map, Scope::Endpoint);
    assert_eq!(read.unknown(), ["x-osdu-data-partition"]);
    assert!(read.problems().is_empty());
    assert_eq!(
        read.long(names::MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE),
        Some(4_000_000)
    );

    // And a session opens with one.
    let mut offer = server_offer();
    offer.endpoint = Capabilities::read(&map, Scope::Endpoint);
    let mut client = client();
    let mut server = server_with(offer, None);
    let start = client.start(T0);
    let (c, _) = exchange(&mut client, &mut server, start, T0);
    assert!(c.contains(&Event::Opened));
}

#[test]
fn data_object_size_precedence() {
    use capabilities::data_object_size;
    assert_eq!(
        data_object_size(Some(1_000_000), None, Some(100_000)),
        Some(100_000),
        "the object overrides the endpoint"
    );
    assert_eq!(
        data_object_size(Some(1_000_000), None, Some(5_000_000)),
        Some(5_000_000),
        "upwards too"
    );
    assert_eq!(
        data_object_size(Some(1_000_000), Some(256_000), Some(5_000_000)),
        Some(256_000),
        "the protocol lowers"
    );
    assert_eq!(
        data_object_size(Some(1_000_000), Some(5_000_000), None),
        Some(1_000_000),
        "and never raises"
    );
}

#[test]
fn alternate_uri_refused_without_the_capability() {
    let canonical = "eml:///resqml20.obj_HorizonInterpretation(0c6ec4d7-2af1-4f7a-8d8f-2e7f3a3b9c11)";
    // With a version, still canonical (§25.3.10) - the first version of this
    // test took it for an alternate form.
    let versioned =
        "eml:///resqml20.obj_HorizonInterpretation(uuid=0c6ec4d7-2af1-4f7a-8d8f-2e7f3a3b9c11,version='2')";
    // The primary key named without a version: alternate (§25.3.9 item 7).
    let alternate = "eml:///resqml20.obj_HorizonInterpretation(uuid=0c6ec4d7-2af1-4f7a-8d8f-2e7f3a3b9c11)";
    for uri in [canonical, versioned] {
        assert!(check_uri(uri, UriUse::Request, false).is_ok(), "{uri}");
        assert!(check_uri(uri, UriUse::CanonicalOnly, false).is_ok(), "{uri}");
    }
    assert_eq!(
        check_uri(alternate, UriUse::Request, false).unwrap_err().code(),
        EtpError::EINVALID_URI.code()
    );
    assert!(
        check_uri(alternate, UriUse::Request, true).is_ok(),
        "negotiated: allowed"
    );
    assert_eq!(
        check_uri(alternate, UriUse::CanonicalOnly, true)
            .unwrap_err()
            .code(),
        EtpError::EINVALID_URI.code(),
        "negotiated, and still refused where the canonical form is required"
    );
}

// ---------------------------------------------------------------------------
// Before the WebSocket - §4.3
// ---------------------------------------------------------------------------

#[test]
fn well_known_url_is_derived_not_configured() {
    let v12 = "?GetVersion=etp12.energistics.org";
    assert_eq!(
        probe::server_capabilities_url("wss://h:8080/x").unwrap(),
        format!("https://h:8080/x/.well-known/etp-server-capabilities{v12}")
    );
    assert_eq!(
        probe::server_capabilities_url("ws://etp.sample.org:8080").unwrap(),
        format!("http://etp.sample.org:8080/.well-known/etp-server-capabilities{v12}")
    );
    assert_eq!(
        probe::server_capabilities_url("wss://h/api/etp/?data-partition-id=x#here").unwrap(),
        format!("https://h/api/etp/.well-known/etp-server-capabilities{v12}")
    );
    assert!(probe::server_capabilities_url("https://h").is_err());
    assert!(probe::server_capabilities_url("wss:///nohost").is_err());
}

fn sample_capabilities() -> ServerCapabilities {
    ServerCapabilities {
        application_name: "a store".into(),
        application_version: "3.1".into(),
        contact_information: Contact {
            organization_name: "Example Org".into(),
            ..Default::default()
        },
        supported_compression: vec!["gzip".into()],
        supported_encodings: vec!["binary".into()],
        supported_formats: vec!["xml".into(), "json".into()],
        supported_data_objects: data_objects(&["resqml20.*"]),
        supported_protocols: vec![SupportedProtocol {
            protocol: 3,
            protocol_version: v12(),
            role: "store".into(),
            protocol_capabilities: BTreeMap::from([("MaxResponseCount".to_string(), long(10_000))]),
        }],
        endpoint_capabilities: BTreeMap::from([
            (
                names::MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE.to_string(),
                long(16_777_216),
            ),
            (
                names::AUTHORIZATION_DETAILS.to_string(),
                DataValue {
                    item: DataValueItem::ArrayOfString(retp_types::datatypes::ArrayOfString {
                        values: vec![r#"Bearer authz_server="https://a", scope="s""#.into()],
                    }),
                },
            ),
        ]),
    }
}

#[test]
fn an_offer_publishes_capabilities_a_client_reads_back() {
    // What a server serves at `.well-known`, read by the probe a client runs:
    // the same offer, both ways.
    let offer = Offer::new("a store", "3.1", Uuid([7; 16]))
        .with_types(["resqml20.*", "eml20.*"])
        .unwrap()
        .with_capability(names::MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE, Value::Long(1 << 20))
        .unwrap()
        .protocol(ProtocolOffer::v12(3, Role::Store))
        .unwrap()
        .protocol(ProtocolOffer::v12(4, Role::Store))
        .unwrap();
    let published = offer.server_capabilities();
    let read = probe::parse_server_capabilities(&published.to_avro(), Some("avro/binary")).unwrap();
    assert_eq!(read, published);
    assert_eq!(read.application_name, "a store");
    assert_eq!(read.supported_encodings, ["binary"]);
    assert_eq!(read.supported_formats, offer.formats);
    let roles: Vec<(i32, &str)> = read
        .supported_protocols
        .iter()
        .map(|p| (p.protocol, p.role.as_str()))
        .collect();
    assert_eq!(
        roles,
        [(3, "store"), (4, "store")],
        "each protocol in the role the server plays"
    );
    let types: Vec<&str> = read
        .supported_data_objects
        .iter()
        .map(|d| d.qualified_type.as_str())
        .collect();
    assert_eq!(types, ["resqml20.*", "eml20.*"]);
    assert!(
        read.endpoint_capabilities
            .contains_key(names::MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE)
    );
}

#[test]
fn server_capabilities_parses_binary_and_json() {
    let expected = sample_capabilities();
    let binary = expected.to_avro();
    assert_eq!(
        probe::parse_server_capabilities(&binary, Some("avro/binary")).unwrap(),
        expected
    );
    assert_eq!(
        probe::parse_server_capabilities(&binary, None).unwrap(),
        expected,
        "sniffed"
    );

    // Avro's JSON encoding, unions spelled out.
    let avro_json = serde_json::json!({
        "applicationName": "a store",
        "applicationVersion": "3.1",
        "contactInformation": {"organizationName": "Example Org", "contactName": "", "contactPhone": "", "contactEmail": ""},
        "supportedCompression": ["gzip"],
        "supportedEncodings": ["binary"],
        "supportedFormats": ["xml", "json"],
        "supportedDataObjects": [{"qualifiedType": "resqml20.*", "dataObjectCapabilities": {}}],
        "supportedProtocols": [{
            "protocol": 3,
            "protocolVersion": {"major": 1, "minor": 2, "revision": 0, "patch": 0},
            "role": "store",
            "protocolCapabilities": {"MaxResponseCount": {"item": {"long": 10000}}}
        }],
        "endpointCapabilities": {
            "MaxWebSocketMessagePayloadSize": {"item": {"long": 16777216}},
            "AuthorizationDetails": {"item": {"Energistics.Etp.v12.Datatypes.ArrayOfString": {"values": ["Bearer authz_server=\"https://a\", scope=\"s\""]}}}
        }
    });
    let text = serde_json::to_vec(&avro_json).unwrap();
    assert_eq!(
        probe::parse_server_capabilities(&text, Some("application/json; charset=utf-8")).unwrap(),
        expected
    );
    assert_eq!(
        probe::parse_server_capabilities(&text, None).unwrap(),
        expected,
        "sniffed"
    );

    // A body that is neither is refused, by encoding.
    let refused = probe::parse_server_capabilities(b"\x03", Some("avro/binary")).unwrap_err();
    assert_eq!(refused.encoding, "avro/binary");
}

#[test]
fn browser_profile_uses_query_string() {
    let settings = HandshakeSettings {
        max_message: Some(1_000_000),
        ..Default::default()
    };
    let browser = probe::handshake("wss://h/etp", Profile::Browser, &settings).unwrap();
    assert!(browser.headers.is_empty(), "a browser sets no header");
    assert_eq!(
        browser.url,
        "wss://h/etp?etp-encoding=binary&maxwebsocketmessagepayloadsize=1000000"
    );
    assert_eq!(browser.subprotocols, ["etp12.energistics.org"]);

    let native = probe::handshake("wss://h/etp", Profile::Native, &settings).unwrap();
    assert_eq!(native.url, "wss://h/etp");
    assert!(native.headers.contains(&("etp-encoding".into(), "binary".into())));
    assert!(
        native.headers.iter().all(|(k, _)| k == &k.to_lowercase()),
        "header names in lower case (§4.3 step 4.c)"
    );
}

#[test]
fn browser_profile_never_puts_the_token_in_the_url() {
    let settings = HandshakeSettings {
        authorization: Some(bearer("s3cr3t-token")),
        ..Default::default()
    };
    let browser = probe::handshake("wss://h/etp", Profile::Browser, &settings).unwrap();
    assert!(!browser.url.contains("s3cr3t"), "{}", browser.url);
    assert_eq!(browser.authorize_after_connecting, Some(bearer("s3cr3t-token")));
    let native = probe::handshake("wss://h/etp", Profile::Native, &settings).unwrap();
    assert!(!native.url.contains("s3cr3t"));
    assert!(
        native
            .headers
            .contains(&("authorization".into(), "Bearer s3cr3t-token".into()))
    );
}

/// Basic credentials are RFC 7617's: the two examples of its §2 and §2.1, and
/// a pair whose base64 needs `+` and `/` - worked out with Python's `base64`,
/// not with the crate that writes it - so that the URL-safe alphabet, which
/// would pass the RFC's two, cannot pass this.
#[test]
fn basic_credentials_are_what_rfc_7617_writes() {
    for (user, password, header) in [
        ("Aladdin", "open sesame", "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="),
        ("test", "123\u{a3}", "Basic dGVzdDoxMjPCow=="),
        ("ilab", "?>~~?", "Basic aWxhYjo/Pn5+Pw=="),
    ] {
        let credentials = Credentials::basic(user, password).unwrap();
        assert_eq!(credentials.header(), header);
        assert_eq!(credentials.scheme(), "Basic");
    }
    let colon = Credentials::basic("ad:min", "x").unwrap_err();
    assert!(colon.0.contains("RFC 7617"), "{colon}");
    assert!(Credentials::basic("admin", "line\nbreak").is_err());
    // A password may hold a colon: only the first one separates.
    assert!(Credentials::basic("admin", "a:b").is_ok());
}

/// A bearer token is the token alone, in RFC 6750's syntax; and neither kind
/// of credentials shows its secret through `Debug`.
#[test]
fn bearer_credentials_refuse_what_is_not_a_token() {
    assert_eq!(bearer("abc-._~+/0==").header(), "Bearer abc-._~+/0==");
    let doubled = Credentials::bearer("Bearer abc").unwrap_err();
    assert!(doubled.0.contains("alone"), "{doubled}");
    for pasted in ["", "abc def", "abc\n", "\"abc\"", "=abc"] {
        assert!(
            Credentials::bearer(pasted).is_err(),
            "{pasted:?} is not a b64token"
        );
    }
    let shown = format!(
        "{:?} {:?}",
        bearer("s3cr3t"),
        Credentials::basic("u", "s3cr3t").unwrap()
    );
    assert!(
        !shown.contains("s3cr3t") && !shown.contains("dTpzM2NyM3Q"),
        "{shown}"
    );
    assert_eq!(shown, "Credentials(Bearer \u{2026}) Credentials(Basic \u{2026})");
}

/// `Authorize` carries the header value **as it is** (§5.3.4: *« the content
/// of the equivalent HTTP Authorization header (NOT just a token) »*): a
/// server that takes Basic sees `Basic …`, not `Bearer Basic …`, and so does
/// the native handshake.
#[test]
fn authorize_carries_the_whole_header_value() {
    let basic = Credentials::basic("Aladdin", "open sesame").unwrap();
    let mut client = Connection::client(
        client_offer(),
        vec![Box::new(Forward(ProtocolOffer::v12(3, Role::Customer)))],
        Some(basic.clone()),
        T0,
    )
    .unwrap();
    let heard = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = heard.clone();
    let authorizer: Box<dyn retp_proto::connection::Authorizer> =
        Box::new(move |authorization: &str, _now: i64| {
            seen.lock().unwrap().push(authorization.to_string());
            if authorization == "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==" {
                Authorization::Granted { expires_at: None }
            } else {
                Authorization::Challenge(vec!["Basic realm=\"etp\"".into()])
            }
        });
    let mut server = server_with(server_offer(), Some(authorizer));
    let start = client.start(T0);
    let (c, s) = exchange(&mut client, &mut server, start, T0);
    assert!(c.contains(&Event::Opened) && s.contains(&Event::Opened), "{c:#?}");
    assert_eq!(*heard.lock().unwrap(), ["Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="]);

    let settings = HandshakeSettings {
        authorization: Some(basic),
        ..Default::default()
    };
    let native = probe::handshake("wss://h/etp", Profile::Native, &settings).unwrap();
    assert!(native.headers.contains(&(
        "authorization".into(),
        "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==".into()
    )));
}

/// Every example §25.3.6-25.3.10 gives of a canonical URI is canonical, and
/// every alternate form §25.3.9 lists is not - nor the authority of the
/// Identifier Specification v4.0 (`eml://witsml14/…`, §6.2), which v5.0 and
/// ETP v1.2 no longer have.
#[test]
fn canonical_uris_are_the_ones_the_specification_lists() {
    for uri in [
        "eml:///",
        "eml:///dataspace('/folder-name/project-name')",
        "eml:///dataspace('rdms-db')",
        "eml:///witsml20.Well(ec8c3f16-1454-4f36-ae10-27d2a2680cf2)",
        "eml:///witsml20.Well(uuid=ec8c3f16-1454-4f36-ae10-27d2a2680cf2,version='1.0')",
        "eml:///dataspace('/folder-name/project-name')/resqml20.obj_HorizonInterpretation(uuid=421a7a05-033a-450d-bcef-051352023578,version='2.0')",
        "eml:///witsml20.Well(ec8c3f16-1454-4f36-ae10-27d2a2680cf2)/witsml20.Wellbore?query",
        "eml:///witsml20.Well?query",
        "eml:///dataspace('/folder-name/project-name')/resqml20.obj_HorizonInterpretation?query",
    ] {
        assert!(is_canonical(uri), "the specification calls `{uri}` canonical");
    }
    for uri in [
        "eml:/witsml20.Well(ec8c3f16-1454-4f36-ae10-27d2a2680cf2)",
        "eml:///witsml20.Well(ec8c3f16-1454-4f36-ae10-27d2a2680cf2)/witsml20.Wellbore(81bb7920-fa42-48cb-b9ac-38031e2703a8)",
        "eml:///witsml20.Well/witsml20.Wellbore",
        "eml:///resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)#hash",
        "eml:///dataspace('rdms-db')?$filter=Name eq 'mydb'",
        "eml:///witsml20.Channel(53b3bf2b-3aa3-458d-b40c-9a4cb754210e)/ChannelClass/Title",
        "eml:///witsml20.Well(uuid=ec8c3f16-1454-4f36-ae10-27d2a2680cf2)",
        "eml:///witsml20.ChannelSet(2c0f6ef2-cc54-4104-8523-0f0fbaba3661)/witsml20.Channel(Mnemonic='HKLD')",
        "eml:///witsml14.well(uid='abc')/witsml14.wellbore(uid='def')",
        "eml://witsml14/well(uid)",
        "eml://api/well(1234567890)",
    ] {
        assert!(
            !is_canonical(uri),
            "`{uri}` is an alternate form, not a canonical one"
        );
    }
}
