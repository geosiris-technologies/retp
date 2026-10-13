//! What was sent and what came back, in ETP's JSON, for the report's appendix.
//!
//! The report gets a place with the JSON, or at least readable, versions of
//! the requests sent and the answers received - a provider asking « which URI
//! did you send for this check? » reads it there. Each request is shown in ETP's JSON encoding (the Avro
//! JSON encoding, ETP v1.2 §3.8.1, `Avro::to_json`) - the message the Avro
//! body carried, field for field - and each answer its header and its body,
//! the same way. A long array is shown as its first values and its count, a
//! long string (an object's XML) cut, so that an appendix of a hundred
//! exchanges stays readable.

use retp_avro::EtpMessage;
use retp_avro::json::Value;
use retp_proto::wire::{Body, Delivery};

/// At most this many characters of one message.
const MOST: usize = 4000;
/// At most this many messages of one answer.
const PARTS: usize = 12;
/// An array longer than this is shown as its first values and its count.
const LIST: usize = 16;
/// A string longer than this is cut.
const TEXT: usize = 600;

/// One request and what came back.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Exchange {
    /// `Energistics.Etp.v12.Protocol.Discovery.GetResources`.
    pub request: String,
    /// The message sent, in ETP's JSON.
    pub sent: String,
    /// Each message received, its header first.
    pub received: Vec<String>,
    /// How it ended: answered, refused, not answered and why.
    pub outcome: String,
}

/// The message as it left, in ETP's JSON.
pub fn sent<M: EtpMessage>(request: &M) -> String {
    shown(request.to_json())
}

/// A message received: header, then body.
pub fn received(delivery: &Delivery) -> String {
    let h = &delivery.header;
    let head = format!(
        "protocol {} messageType {} messageId {} correlationId {} flags {:#04x}{}",
        h.protocol,
        h.message_type,
        h.message_id,
        h.correlation_id,
        h.message_flags,
        if delivery.last { " (FIN)" } else { "" }
    );
    let body = match &delivery.body {
        Body::Message(message) => shown(message.to_json()),
        Body::Private(bytes) => format!("{} bytes of a private protocol", bytes.len()),
    };
    format!("{head}\n{body}")
}

/// The answers of one exchange, at most `PARTS` of them.
pub fn all_received(deliveries: &[Delivery]) -> Vec<String> {
    let mut out: Vec<String> = deliveries.iter().take(PARTS).map(received).collect();
    if deliveries.len() > PARTS {
        out.push(format!("… and {} more message(s)", deliveries.len() - PARTS));
    }
    out
}

/// A JSON value as the appendix shows it: [`shortened`], pretty-printed, cut
/// at `MOST` characters.
pub fn shown(value: Value) -> String {
    let text = serde_json::to_string_pretty(&shortened(value)).unwrap_or_default();
    if text.chars().count() > MOST {
        let cut: String = text.chars().take(MOST).collect();
        return format!("{cut}\n… (cut at {MOST} characters)");
    }
    text
}

/// Long arrays reduced to their first values and their count, long strings
/// cut - what the appendix shows, never what was sent.
pub fn shortened(value: Value) -> Value {
    match value {
        Value::Array(items) if items.len() > LIST => {
            let count = items.len();
            let mut kept: Vec<Value> = items.into_iter().take(8).map(shortened).collect();
            kept.push(Value::String(format!("… ({count} values)")));
            Value::Array(kept)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(shortened).collect()),
        Value::Object(map) => Value::Object(map.into_iter().map(|(k, v)| (k, shortened(v))).collect()),
        Value::String(text) if text.chars().count() > TEXT => {
            let count = text.chars().count();
            let cut: String = text.chars().take(TEXT).collect();
            Value::String(format!("{cut}… ({count} characters)"))
        }
        other => other,
    }
}
