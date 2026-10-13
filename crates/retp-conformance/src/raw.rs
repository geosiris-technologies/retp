//! The checks that need a wire of their own: what a server answers to a
//! message it must refuse.
//!
//! A client never sends these - the wire refuses to emit a message of a
//! protocol the session did not agree, and never writes an unreadable
//! header. So they are sent **around** the client: a `Connection` driven by
//! hand over a bare [`Transport`], its session opened the ordinary way, then
//! the faults on purpose and the exception each must draw (§3.5.3 §2.b.i,
//! §5.2.2, §3.5.4.2 §3). The ids stay the wire's: a forged message takes the
//! id of a `Ping` the wire just issued, so that what follows is still
//! numbered in order.
//!
//! The session ends by dropping the socket rather than by `CloseSession`:
//! after a fault, what the server holds of the session is exactly what the
//! checks are about, and a clean close on top would test nothing more.

use std::time::Duration;

use retp_avro::{Avro, EtpMessage, Role, Writer};
use retp_client::{Clock, Transport};
use retp_proto::connection::{Connection, Event, Forward};
use retp_proto::credentials::Credentials;
use retp_proto::negotiate::{Offer, ProtocolOffer};
use retp_proto::wire::{EtpError, Exception, Place};
use retp_types::datatypes::MessageHeader;
use retp_types::datatypes::data_array_types::DataArrayIdentifier;
use retp_types::datatypes::object::ContextScopeKind;
use retp_types::datatypes::object::Dataspace;
use retp_types::protocol::data_array::GetDataArrayMetadata;
use retp_types::protocol::dataspace::{GetDataspaces, PutDataspaces};
use retp_types::protocol::store::{GetDataObjects, PutDataObjects};
use retp_types::protocol::supported_types::GetSupportedTypes;
use retp_types::protocol::transaction::StartTransaction;

use crate::{Finding, Verdict};

/// How long one fault may wait for its exception.
const PATIENCE: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(100);

/// Open a session over `transport` with Core and Discovery alone, then send
/// the three faults and read what came back.
pub fn checks<T: Transport, C: Clock>(
    mut transport: T,
    clock: C,
    offer: Offer,
    credentials: Option<Credentials>,
) -> Vec<Finding> {
    let mut out = Vec::new();
    // Discovery only: 24 is then a protocol the session did **not** agree,
    // which is what the first fault needs.
    let handlers: Vec<Box<dyn retp_proto::connection::Handler>> =
        vec![Box::new(Forward(ProtocolOffer::v12(3, Role::Customer)))];
    let mut connection = match Connection::client(offer, handlers, credentials, clock.now()) {
        Ok(connection) => connection,
        Err(why) => {
            out.push(Finding::new(
                "raw.session",
                "§5.2.1.1",
                "A session opens with Core and Discovery alone",
                None,
                Verdict::NotChecked,
                format!("the raw session's offer was refused before sending: {why}"),
            ));
            return out;
        }
    };
    let step = connection.start(clock.now());
    match pump(&mut transport, &mut connection, &clock, step, |event| {
        matches!(event, Event::Opened)
    }) {
        Some(_) => {}
        None => {
            out.push(Finding::new(
                "raw.session",
                "§5.2.1.1",
                "A session opens with Core and Discovery alone",
                None,
                Verdict::NotChecked,
                "the raw session did not open in time; the faults could not be sent",
            ));
            return out;
        }
    }

    // --- 1. A request of each protocol the session did not agree ----------------
    // This session agreed Discovery alone, so every other protocol is one it
    // did not - whether the server announces it or not: §5.2.2 §4 asks the
    // same refusal of both. One harmless request each (a read; a transaction
    // read-only), so that a server that wrongly serves it changes nothing.
    let absent = "eml:///eml23.PropertyKind(0f0e0d0c-0b0a-4908-8706-050403020100)".to_string();
    out.push(unsupported(
        &mut transport,
        &mut connection,
        &clock,
        24,
        "raw.unsupported-protocol",
        &GetDataspaces {
            store_last_write_filter: None,
        },
    ));
    out.push(unsupported(
        &mut transport,
        &mut connection,
        &clock,
        4,
        "raw.unsupported-protocol.store",
        &GetDataObjects {
            uris: [("1".to_string(), absent.clone())].into_iter().collect(),
            format: "xml".into(),
        },
    ));
    out.push(unsupported(
        &mut transport,
        &mut connection,
        &clock,
        9,
        "raw.unsupported-protocol.data-array",
        &GetDataArrayMetadata {
            data_arrays: [(
                "1".to_string(),
                DataArrayIdentifier {
                    uri: absent.clone(),
                    path_in_resource: "/energyml-conformance/nobody-has-this".into(),
                },
            )]
            .into_iter()
            .collect(),
        },
    ));
    out.push(unsupported(
        &mut transport,
        &mut connection,
        &clock,
        18,
        "raw.unsupported-protocol.transaction",
        &StartTransaction {
            read_only: true,
            message: "retp-conformance: a protocol not negotiated".into(),
            dataspace_uris: vec!["eml:///".into()],
        },
    ));
    // And the requests that create, which a store must refuse the same way
    // (the creation commands are tried on a protocol
    // not negotiated, the deletions never). Fresh names under the suite's
    // prefix; a store that wrongly takes one is told what it may now hold.
    let stamp = format!("{:012x}", clock.now() & 0xffff_ffff_ffff);
    let space = format!("example/etp-conformance-unagreed-{stamp}");
    let mut created = unsupported(
        &mut transport,
        &mut connection,
        &clock,
        24,
        "raw.unsupported-protocol.put-dataspaces",
        &PutDataspaces {
            dataspaces: [(
                "1".to_string(),
                Dataspace {
                    uri: format!("eml:///dataspace('{space}')"),
                    path: space.clone(),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
        },
    );
    if created.said.starts_with("answered as if it were fine") {
        created.said.push_str(&format!(" - eml:///dataspace('{space}') may now be on the server; the suite deletes nothing it was not able to create through a session that agreed it"));
    }
    out.push(created);
    let (object_uri, object) = crate::checks::feature("eml:///", &format!("0f0e0d0c-0b0a-4908-8706-{stamp}"));
    let mut put = unsupported(
        &mut transport,
        &mut connection,
        &clock,
        4,
        "raw.unsupported-protocol.put-data-objects",
        &PutDataObjects {
            data_objects: [("1".to_string(), object)].into_iter().collect(),
            prune_contained_objects: false,
        },
    );
    if put.said.starts_with("answered as if it were fine") {
        put.said
            .push_str(&format!(" - {object_uri} may now be on the server"));
    }
    out.push(put);
    out.push(unsupported(
        &mut transport,
        &mut connection,
        &clock,
        25,
        "raw.unsupported-protocol.supported-types",
        &GetSupportedTypes {
            uri: "eml:///".into(),
            scope: ContextScopeKind::Self_,
            return_empty_types: false,
            count_objects: false,
        },
    ));

    // --- 2. A message type Core does not have ------------------------------------
    let ping = connection.ping(clock.now());
    let forged = MessageHeader {
        protocol: 0,
        message_type: 99,
        correlation_id: 0,
        message_id: ping.header.message_id,
        message_flags: ping.header.message_flags,
    };
    let mut writer = Writer::new();
    forged.write(&mut writer);
    let step = retp_proto::connection::Step {
        send: vec![retp_proto::wire::Outgoing {
            header: forged,
            bytes: writer.finish(),
        }],
        events: Vec::new(),
        close: false,
    };
    out.push(expect(
        &mut transport,
        &mut connection,
        &clock,
        step,
        ping.header.message_id,
        EtpError::EINVALID_MESSAGETYPE,
        "raw.invalid-message-type",
        "§3.5.4.2 §3, §24",
        "A messageType unknown for the protocol MUST be answered with EINVALID_MESSAGETYPE (3)",
    ));

    // --- 3. A header that does not read ------------------------------------------
    let garbage = retp_proto::connection::Step {
        send: vec![retp_proto::wire::Outgoing {
            header: MessageHeader {
                protocol: 0,
                message_type: 0,
                correlation_id: 0,
                message_id: 0,
                message_flags: 0,
            },
            bytes: vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        }],
        events: Vec::new(),
        close: false,
    };
    let mut seen: Option<(MessageHeader, Exception)> = None;
    pump(&mut transport, &mut connection, &clock, garbage, |event| {
        if let Event::Delivered(delivery) = event
            && let Some(exception) = delivery.exception()
        {
            seen = Some((delivery.header.clone(), exception));
            return true;
        }
        false
    });
    out.push(match seen {
        Some((header, Exception::Single(info))) if info.code == EtpError::EINVALID_MESSAGE.code() => {
            let placed = header.protocol == 0 && header.correlation_id == 0;
            Finding::new(
                "raw.invalid-message",
                "§3.5.3 §2.b.i",
                "A header that does not deserialize MUST be answered with EINVALID_MESSAGE (19), in a ProtocolException whose header has protocol 0 and correlationId 0",
                None,
                if placed { Verdict::Pass } else { Verdict::Fail },
                if placed { "EINVALID_MESSAGE (19), protocol 0, correlation 0".into() } else { format!("EINVALID_MESSAGE (19), but sent with protocol {} and correlation {}", header.protocol, header.correlation_id) },
            )
        }
        Some((_, exception)) => Finding::new("raw.invalid-message", "§3.5.3 §2.b.i", "A header that does not deserialize is EINVALID_MESSAGE (19)", None, Verdict::Fail, format!("{} instead", describe(&exception))),
        None => Finding::new("raw.invalid-message", "§3.5.3 §2.b.i", "A header that does not deserialize is EINVALID_MESSAGE (19)", None, Verdict::Fail, "no ProtocolException came within the patience - or the server dropped the connection without one"),
    });
    transport.close();
    out
}

/// A request of `protocol`, which this session did not agree, sent all the
/// same - the wire widened for it - and the EUNSUPPORTED_PROTOCOL it must draw.
fn unsupported<T: Transport, C: Clock, M: EtpMessage>(
    transport: &mut T,
    connection: &mut Connection,
    clock: &C,
    protocol: i32,
    check: &'static str,
    request: &M,
) -> Finding {
    const SECTION: &str = "§5.2.2 §4";
    const RULE: &str =
        "A message of a protocol that was not negotiated MUST be answered with EUNSUPPORTED_PROTOCOL (4)";
    // Widened once per protocol: a second request of the same one finds it
    // there already.
    if let Ok(widened) = connection
        .wire()
        .config()
        .clone()
        .protocol(protocol, Role::Customer)
    {
        connection.wire().reconfigure(widened);
    }
    match connection.request(request, clock.now()) {
        Ok(outgoing) => {
            let id = outgoing.header.message_id;
            let step = retp_proto::connection::Step {
                send: vec![outgoing],
                events: Vec::new(),
                close: false,
            };
            let mut finding = expect(
                transport,
                connection,
                clock,
                step,
                id,
                EtpError::EUNSUPPORTED_PROTOCOL,
                check,
                SECTION,
                RULE,
            );
            finding.protocol = Some(protocol);
            finding
        }
        Err(why) => Finding::new(
            check,
            SECTION,
            RULE,
            Some(protocol),
            Verdict::NotChecked,
            format!("the wire would not send it: {why}"),
        ),
    }
}

/// Send a step, then read until `stop` says the awaited event came - or the
/// patience runs out. What arrived meanwhile is handed back.
fn pump<T: Transport, C: Clock>(
    transport: &mut T,
    connection: &mut Connection,
    clock: &C,
    step: retp_proto::connection::Step,
    mut stop: impl FnMut(&Event) -> bool,
) -> Option<Vec<Event>> {
    let mut pending = step;
    let mut gathered = Vec::new();
    let started = clock.now();
    loop {
        for outgoing in pending.send.drain(..) {
            if transport.send(&outgoing.bytes).is_err() {
                return None;
            }
        }
        let mut done = false;
        for event in pending.events.drain(..) {
            done |= stop(&event);
            gathered.push(event);
        }
        if done {
            return Some(gathered);
        }
        if pending.close || clock.now() - started > PATIENCE.as_micros() as i64 {
            return None;
        }
        pending = match transport.receive(POLL) {
            Ok(Some(bytes)) => connection.receive(&bytes, clock.now()),
            Ok(None) => connection.tick(clock.now()),
            Err(_) => return None,
        };
    }
}

/// Send a fault that carries a request id and expect the exception about it.
#[allow(clippy::too_many_arguments)]
fn expect<T: Transport, C: Clock>(
    transport: &mut T,
    connection: &mut Connection,
    clock: &C,
    step: retp_proto::connection::Step,
    id: i64,
    expected: EtpError,
    check: &'static str,
    section: &'static str,
    rule: &'static str,
) -> Finding {
    let mut code: Option<(i32, String)> = None;
    let mut positive = false;
    pump(transport, connection, clock, step, |event| match event {
        Event::AnswerRefused { request, incident } if *request == id => {
            code = Some((incident.code(), incident.message()));
            true
        }
        Event::Delivered(delivery) => match (delivery.place, delivery.exception()) {
            (Place::Answers { request }, Some(exception)) if request == id => {
                code = Some(first_code(&exception));
                true
            }
            (Place::Answers { request }, None) if request == id => {
                positive = true;
                true
            }
            (Place::Stray { correlation }, Some(exception)) if correlation == id => {
                code = Some(first_code(&exception));
                true
            }
            _ => false,
        },
        _ => false,
    });
    match code {
        Some((got, _)) if got == expected.code() => Finding::new(
            check,
            section,
            rule,
            None,
            Verdict::Pass,
            format!("{} ({})", expected.name(), expected.code()),
        ),
        Some((got, message)) => Finding::new(
            check,
            section,
            rule,
            None,
            Verdict::Fail,
            format!(
                "{} instead of {} ({}): {message}",
                EtpError::from_code(got)
                    .map(|e| format!("{} ({got})", e.name()))
                    .unwrap_or_else(|| format!("code {got}")),
                expected.name(),
                expected.code()
            ),
        ),
        None if positive => Finding::new(
            check,
            section,
            rule,
            None,
            Verdict::Fail,
            format!(
                "answered as if it were fine, instead of {} ({})",
                expected.name(),
                expected.code()
            ),
        ),
        None => Finding::new(
            check,
            section,
            rule,
            None,
            Verdict::Fail,
            format!(
                "no ProtocolException came within the patience, instead of {} ({})",
                expected.name(),
                expected.code()
            ),
        ),
    }
}

fn first_code(exception: &Exception) -> (i32, String) {
    match exception {
        Exception::Single(info) => (info.code, info.message.clone()),
        Exception::Map(map) => map
            .values()
            .next()
            .map(|info| (info.code, info.message.clone()))
            .unwrap_or((0, "an empty errors map".into())),
    }
}

fn describe(exception: &Exception) -> String {
    let (code, message) = first_code(exception);
    format!(
        "{} ({code}): {message}",
        EtpError::from_code(code)
            .map(|e| e.name().to_string())
            .unwrap_or_else(|| "unknown".into())
    )
}
