//! Receiving, in the order §3.5.3 and §3.5.4.2 impose.
//!
//! 1. the whole WebSocket message - the transport's job, before this;
//! 2. the header; unreadable → `EINVALID_MESSAGE`, protocol 0, correlation 0;
//! 3. the `Acknowledge`, if asked for, **before anything else**;
//! 4. the message id: of the peer's parity, above the last;
//! 5. compression, then the header extension;
//! 6. the route: protocol negotiated, message type known, sent by the role
//!    that may send it;
//! 7. the correlation: a new action, a part of an open one, a response to a
//!    request of ours - or refused;
//! 8. the body;
//! 9. BLOBs: a message that announces some is held until its `Chunk`s are in.
//!
//! A step that refuses answers with a `ProtocolException` about the message -
//! except about an exception, which is never answered with one: that goes to
//! [`Received::fault`].

use std::collections::BTreeMap;

use retp_avro::{Avro, Reader};
use retp_types::datatypes::{MessageHeader, MessageHeaderExtension};
use retp_types::dispatch::{self, Message};

use super::chunks::{as_chunk, objects_mut};
use super::send::{ACKNOWLEDGE, PROTOCOL_EXCEPTION};
use retp_types::protocol::core::ProtocolException;

use super::{
    Body, Delivery, EtpError, Exception, FIRST_PRIVATE_PROTOCOL, Flags, Held, Incident, Open, Place,
    Received, Tolerance, Wire,
};

/// What becomes of the correlation table once the body has decoded.
enum Commit {
    Nothing,
    OpenAction,
    CloseAction(i64),
    CloseRequest(i64),
}

impl Wire {
    /// Take in one complete WebSocket message.
    pub fn receive(&mut self, bytes: &[u8]) -> Received {
        let mut received = Received::default();
        let mut input = Reader::new(bytes);
        let header = match MessageHeader::read(&mut input) {
            Ok(header) => header,
            Err(why) => {
                let incident = EtpError::EINVALID_MESSAGE
                    .with_detail(format!("the message header could not be read: {why}"));
                received.replies.push(self.unattached(&incident));
                return received;
            }
        };
        let rest = &bytes[bytes.len() - input.remaining()..];
        let flags = Flags(header.message_flags);
        log::debug!(target: "retp_proto", "received {} ({} bytes)", super::describe(&header), bytes.len());

        // §3.7.2.2 §3.b: immediately, before any other processing - so the
        // Ack precedes even an exception about this very message. Never for
        // an Acknowledge: that would be the endless loop.
        if flags.contains(Flags::ACK_REQUESTED) && header.message_type != ACKNOWLEDGE {
            received.replies.push(self.acknowledge(&header));
        }

        match self.accept(&header, flags, rest) {
            Ok(delivery) => received.delivery = delivery,
            Err(incident) if header.message_type == PROTOCOL_EXCEPTION => {
                // What the peer meant to say still helps whoever reads the log.
                let decoded = ProtocolException::from_avro(rest)
                    .ok()
                    .filter(|_| !flags.contains(Flags::COMPRESSED) && !flags.contains(Flags::EXTENSION))
                    .and_then(|e| Exception::from_message(&e).ok());
                let said = match &decoded {
                    Some(Exception::Single(error)) => format!(" - it says: {} {}", error.code, error.message),
                    Some(Exception::Map(errors)) => {
                        format!(" - it holds {} errors, one per key", errors.len())
                    }
                    None => String::new(),
                };
                log::warn!(target: "retp_proto", "a {} could not be taken: {incident}{said}", super::describe(&header));
                // Numbered -1 - as a server met in the field does -, it is attached to nothing;
                // but with one request of ours out, it can only be about that
                // one. Tolerant, it ends that request with what it says,
                // rather than let it wait for its timeout. Correlation 0 is
                // not this: that is how an unattached notice is sent
                // (§3.7.2.1 §3.c).
                let only = (self.awaiting.len() == 1).then(|| *self.awaiting.keys().next().expect("one"));
                if let (Tolerance::Tolerant, true, Some(request), Some(Exception::Single(error))) =
                    (self.config.tolerance, header.correlation_id < 0, only, &decoded)
                {
                    log::warn!(
                        target: "retp_proto",
                        "taken as the answer to request #{request}, the only one out"
                    );
                    self.abandon(request);
                    let told = super::ErrorRegistry::new()
                        .incident(error.code, Some(error.message.clone()))
                        .unwrap_or_else(|| {
                            EtpError::EREQUEST_DENIED
                                .with_detail(format!("error {}: {}", error.code, error.message))
                        });
                    received.refused_answer = Some((request, told));
                }
                received.fault = Some(incident);
            }
            Err(incident) => {
                log::warn!(target: "retp_proto", "refused {}: {incident}", super::describe(&header));
                received.replies.push(self.exception(&header, &incident));
                if self.awaiting.contains_key(&header.correlation_id) {
                    self.abandon(header.correlation_id);
                    received.refused_answer = Some((header.correlation_id, incident));
                }
            }
        }
        received
    }

    fn accept(
        &mut self,
        header: &MessageHeader,
        flags: Flags,
        rest: &[u8],
    ) -> Result<Option<Delivery>, Incident> {
        let id = header.message_id;
        let peer = self.side.peer();
        if !peer.may_use(id) {
            return Err(EtpError::EINVALID_STATE.with_detail(format!(
                "message id {id} is not one the {} may use: non-zero, {} (§3.5.4.1 §5.c)",
                peer.role().as_str(),
                if matches!(peer, super::Side::Client) {
                    "even"
                } else {
                    "odd"
                },
            )));
        }
        if id <= self.last_peer_id {
            return Err(EtpError::EINVALID_STATE.with_detail(format!(
                "message id {id} follows {}: ids are strictly increasing (§3.5.4.1 §5.b)",
                self.last_peer_id
            )));
        }
        self.last_peer_id = id;

        if flags.contains(Flags::COMPRESSED) {
            return Err(
                EtpError::ECOMPRESSION_NOTSUPPORTED.with_detail(if header.protocol == 0 {
                    "a Core message is never compressed (§3.5.5)"
                } else {
                    "compression was not negotiated for this session"
                }),
            );
        }

        let mut body = rest;
        let extension = if flags.contains(Flags::EXTENSION) {
            if !self.config.header_extensions {
                return Err(EtpError::EINVALID_MESSAGE
                    .with_detail("a header extension arrived, and none was negotiated (§3.6.2 §3.c)"));
            }
            let mut input = Reader::new(body);
            let extension = MessageHeaderExtension::read(&mut input).map_err(|why| {
                EtpError::EINVALID_MESSAGE
                    .with_detail(format!("the header extension could not be read: {why}"))
            })?;
            body = &body[body.len() - input.remaining()..];
            // Keys it does not understand are ignored, with no error (§3.6.2 §3.b):
            // the extension is handed over as it came.
            Some(extension)
        } else {
            None
        };

        match header.message_type {
            ACKNOWLEDGE => return self.acknowledgement(header, flags, extension, body),
            PROTOCOL_EXCEPTION => return self.protocol_exception(header, flags, extension, body),
            _ => {}
        }

        let protocol = header.protocol;
        if protocol >= FIRST_PRIVATE_PROTOCOL {
            if !self.config.private.contains(&protocol) {
                return Err(EtpError::EUNSUPPORTED_PROTOCOL
                    .with_detail(format!("private protocol {protocol} was not negotiated")));
            }
            let (place, commit) = self.correlate(header, flags, true)?;
            self.commit(header, commit);
            return Ok(Some(Delivery {
                header: header.clone(),
                extension,
                body: Body::Private(body.to_vec()),
                place,
                last: flags.contains(Flags::FIN),
            }));
        }
        let peer_role = self.peer_role(protocol).ok_or_else(|| {
            EtpError::EUNSUPPORTED_PROTOCOL.with_detail(format!("protocol {protocol} was not negotiated"))
        })?;
        let info = dispatch::info(protocol, header.message_type).ok_or_else(|| {
            EtpError::EINVALID_MESSAGETYPE.with_detail(format!(
                "protocol {protocol} has no message type {}",
                header.message_type
            ))
        })?;
        if !info.sender_role.allows(peer_role) {
            return Err(EtpError::EINVALID_MESSAGETYPE.with_detail(format!(
                "{} is not sent by the {}, which the peer is in protocol {protocol}",
                info.name,
                peer_role.as_str()
            )));
        }

        let mut correlation_header = header.clone();
        if as_chunk_type(protocol, header.message_type) {
            // §3.7.3.2 §2.b correlates a Chunk to the message that announced
            // its BLOB; §9.3.7 and §3.7.3.1 §7 to the request. Both are read.
            if let Some(held) = self
                .held
                .iter()
                .find(|h| h.delivery.header.message_id == header.correlation_id)
            {
                correlation_header.correlation_id = held.group;
            }
        }
        let (place, commit) = self.correlate(&correlation_header, flags, info.multipart)?;

        let mut message = dispatch::decode(protocol, header.message_type, body).map_err(|why| {
            EtpError::EINVALID_MESSAGE
                .with_detail(format!("the body of {} could not be read: {why}", info.name))
        })?;
        let last = flags.contains(Flags::FIN);

        if let Some((blob, data, final_piece)) = as_chunk(&message) {
            let group = group_of(place, id);
            let Some(index) = self
                .held
                .iter()
                .position(|h| h.blobs.contains_key(&blob.0) && h.group == group)
            else {
                return Err(EtpError::EINVALID_STATE
                    .with_detail("a Chunk for a BLOB that no message of this action announced (§3.7.3.2)"));
            };
            let entry = self.held[index].blobs.get_mut(&blob.0).expect("found above");
            if entry.1 {
                return Err(EtpError::EINVALID_STATE
                    .with_detail("a Chunk after the final one of its BLOB (§3.7.3.2 §2.c.i)"));
            }
            entry.0.extend_from_slice(data);
            entry.1 = final_piece;
            self.commit(header, commit);
            if !self.held[index].blobs.values().all(|(_, done)| *done) {
                return Ok(None);
            }
            let mut held = self.held.remove(index);
            if let Body::Message(carrier) = &mut held.delivery.body {
                for object in objects_mut(carrier) {
                    if let Some(blob) = object.blob_id.take() {
                        object.data = held
                            .blobs
                            .remove(&blob.0)
                            .map(|(bytes, _)| bytes)
                            .unwrap_or_default();
                    }
                }
            }
            held.delivery.last = last;
            return Ok(Some(held.delivery));
        }

        let mut blobs = BTreeMap::new();
        for object in objects_mut(&mut message) {
            if let Some(blob) = &object.blob_id {
                if !object.data.is_empty() {
                    if self.config.tolerance == Tolerance::Strict {
                        return Err(EtpError::EINVALID_ARGUMENT
                            .with_detail("a data object with a blobId carries its data too; it must be empty (§3.7.3.2 §1.b)"));
                    }
                    // The data is there: it is the object, and the blobId
                    // announces nothing that will come.
                    log::warn!(
                        target: "retp_proto",
                        "{}: a data object carries a blobId and its data too (§3.7.3.2 §1.b); the data is taken, the blobId ignored",
                        super::describe(header)
                    );
                    object.blob_id = None;
                    continue;
                }
                blobs.insert(blob.0, (Vec::new(), false));
            }
        }
        self.commit(header, commit);
        let delivery = Delivery {
            header: header.clone(),
            extension,
            body: Body::Message(message),
            place,
            last,
        };
        if blobs.is_empty() {
            return Ok(Some(delivery));
        }
        if last {
            return Err(EtpError::EINVALID_STATE.with_detail(
                "a message announces BLOBs and carries the FIN: its Chunks could not follow (§3.7.3.2 §3)",
            ));
        }
        self.held.push(Held {
            group: group_of(place, id),
            delivery,
            blobs,
        });
        Ok(None)
    }

    /// Where a message belongs, and what its FIN will change - decided before
    /// the body is read, applied only once it has been.
    fn correlate(
        &self,
        header: &MessageHeader,
        flags: Flags,
        multipart: bool,
    ) -> Result<(Place, Commit), Incident> {
        let correlation = header.correlation_id;
        let fin = flags.contains(Flags::FIN);
        if correlation == 0 {
            if fin {
                return Ok((Place::Opens, Commit::Nothing));
            }
            if !multipart {
                return Err(EtpError::EINVALID_STATE
                    .with_detail("a message that may not be split arrived without the FIN (§3.5.4.1 §7.a)"));
            }
            return Ok((Place::Opens, Commit::OpenAction));
        }
        if let Some(open) = self.open.get(&correlation) {
            if open.protocol != header.protocol {
                return Err(EtpError::EINVALID_STATE.with_detail(format!(
                    "a part in protocol {} of an action opened in protocol {}",
                    header.protocol, open.protocol
                )));
            }
            let commit = if fin {
                Commit::CloseAction(correlation)
            } else {
                Commit::Nothing
            };
            return Ok((Place::Continues { first: correlation }, commit));
        }
        if let Some(protocol) = self.awaiting.get(&correlation) {
            if *protocol != header.protocol {
                return Err(EtpError::EINVALID_STATE.with_detail(format!(
                    "a response in protocol {} to a request of protocol {protocol}",
                    header.protocol
                )));
            }
            let commit = if fin {
                Commit::CloseRequest(correlation)
            } else {
                Commit::Nothing
            };
            return Ok((Place::Answers { request: correlation }, commit));
        }
        Err(EtpError::EINVALID_STATE.with_detail(format!(
            "correlation {correlation} is not open: no action or request of that id is in progress - never opened, or closed by its FIN (§3.7.3.1 §7.c)"
        )))
    }

    fn commit(&mut self, header: &MessageHeader, commit: Commit) {
        match commit {
            Commit::Nothing => {}
            Commit::OpenAction => {
                self.open.insert(
                    header.message_id,
                    Open {
                        protocol: header.protocol,
                    },
                );
            }
            Commit::CloseAction(first) => {
                self.open.remove(&first);
            }
            Commit::CloseRequest(request) => {
                self.awaiting.remove(&request);
            }
        }
    }

    fn acknowledgement(
        &mut self,
        header: &MessageHeader,
        flags: Flags,
        extension: Option<MessageHeaderExtension>,
        body: &[u8],
    ) -> Result<Option<Delivery>, Incident> {
        let message = dispatch::decode(0, ACKNOWLEDGE, body).map_err(|why| {
            EtpError::EINVALID_MESSAGE
                .with_detail(format!("the body of an Acknowledge could not be read: {why}"))
        })?;
        let acknowledged = header.correlation_id;
        if !self.ids.issued(acknowledged) {
            return Err(EtpError::EINVALID_STATE.with_detail(format!(
                "an Acknowledge of message {acknowledged}, which this end never sent"
            )));
        }
        Ok(Some(Delivery {
            header: header.clone(),
            extension,
            body: Body::Message(message),
            place: Place::Acknowledges {
                message: acknowledged,
            },
            last: flags.contains(Flags::FIN),
        }))
    }

    fn protocol_exception(
        &mut self,
        header: &MessageHeader,
        flags: Flags,
        extension: Option<MessageHeaderExtension>,
        body: &[u8],
    ) -> Result<Option<Delivery>, Incident> {
        let message = dispatch::decode(0, PROTOCOL_EXCEPTION, body).map_err(|why| {
            EtpError::EINVALID_MESSAGE.with_detail(format!(
                "the body of a ProtocolException could not be read: {why}"
            ))
        })?;
        if let Message::ProtocolException(exception) = &message {
            Exception::from_message(exception)
                .map_err(|malformed| EtpError::EINVALID_MESSAGE.with_detail(malformed.to_string()))?;
        }
        let correlation = header.correlation_id;
        let fin = flags.contains(Flags::FIN);
        let place = if correlation == 0 {
            Place::Unattached
        } else if self.awaiting.contains_key(&correlation) {
            if fin {
                self.awaiting.remove(&correlation);
            }
            Place::Answers { request: correlation }
        } else if self.ids.issued(correlation) {
            Place::About { message: correlation }
        } else {
            Place::Stray { correlation }
        };
        Ok(Some(Delivery {
            header: header.clone(),
            extension,
            body: Body::Message(message),
            place,
            last: fin,
        }))
    }
}

/// Whether `(protocol, message_type)` is one of the three `Chunk`s.
fn as_chunk_type(protocol: i32, message_type: i32) -> bool {
    matches!((protocol, message_type), (4, 8) | (5, 9) | (14, 3))
}

/// The key of the action a message belongs to.
fn group_of(place: Place, id: i64) -> i64 {
    match place {
        Place::Continues { first } => first,
        Place::Answers { request } => request,
        _ => id,
    }
}
