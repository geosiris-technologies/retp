//! The headers a server wants in addition to the protocol's own.
//!
//! Which headers a server expects is a property of **the server**, like its
//! URL: a routing key, a tenant, a data partition, an API key. They are given
//! once, with the [`Endpoint`](crate::native::Endpoint), and go with every
//! request to it - the WebSocket handshake, the probe of
//! `/.well-known/etp-server-capabilities`, each call of the REST API.
//!
//! This module is the one door they come through: a name or a value that
//! cannot be sent is refused here, by name, before anything is connected.

use std::fmt;

use retp_proto::probe::{Handshake, Profile};
use zeroize::Zeroize;

/// The headers the client writes itself, in the WebSocket handshake or in an
/// HTTP request. One given as an extra header would be sent twice, or would
/// replace what the client meant to send.
const OWN: [&str; 11] = [
    "accept",
    "authorization",
    "connection",
    "content-length",
    "content-type",
    "etp-encoding",
    "host",
    "maxwebsocketframepayloadsize",
    "maxwebsocketmessagepayloadsize",
    "transfer-encoding",
    "upgrade",
];

/// The WebSocket handshake's own headers (RFC 6455 §11.3).
const OWN_PREFIX: &str = "sec-websocket-";

/// Whether a header says by its name that it carries a secret (`x-api-key`,
/// `cookie`, `x-auth-token`…).
///
/// Such a header is never written as a literal in a file, and never in a URL.
pub fn sensitive_header(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    ["authorization", "cookie", "key", "token", "secret", "password"]
        .iter()
        .any(|word| lower.contains(word))
}

/// Why a header is not sent. It names the header, never its value.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum HeaderRefusal {
    /// Not a header name: empty, or a character that is not a letter, a
    /// digit, `-` or `_`. The text given is not kept: what is not a name
    /// may be a value, written where the name goes.
    Name,
    /// The value of this header holds a character that cannot be sent: a
    /// control character (a line break would start another header), or
    /// one that is not ASCII.
    Value(String),
    /// A header the client writes itself (`authorization`, `etp-encoding`,
    /// `sec-websocket-*`…).
    Own(String),
    /// This header is given twice, whatever the case of its letters.
    Twice(String),
    /// This header carries a secret, and the handshake of a front end has no
    /// header to put it in: it would be written in the URL.
    InTheUrl(String),
}

impl fmt::Display for HeaderRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HeaderRefusal::Name => f.write_str(
                "a header name is letters, digits, `-` and `_`: what was given as a name is not one (it is not shown: it may hold a value)",
            ),
            HeaderRefusal::Value(name) => write!(
                f,
                "the value of header `{name}` holds a character that cannot be sent: a control character (a line break for instance) or one that is not ASCII"
            ),
            HeaderRefusal::Own(name) if name.eq_ignore_ascii_case("authorization") => write!(
                f,
                "`{name}` is the header of the credentials: give credentials (a token, a login), not an extra header"
            ),
            HeaderRefusal::Own(name) => write!(
                f,
                "header `{name}` is written by the client itself: it cannot be given as an extra header"
            ),
            HeaderRefusal::Twice(name) => write!(f, "header `{name}` is given twice"),
            HeaderRefusal::InTheUrl(name) => write!(
                f,
                "header `{name}` carries a secret, and the handshake of a front end writes its attributes in the URL: it is not sent that way"
            ),
        }
    }
}

impl std::error::Error for HeaderRefusal {}

#[derive(Clone, PartialEq, Eq)]
struct Extra {
    name: String,
    value: String,
    secret: bool,
}

impl Drop for Extra {
    fn drop(&mut self) {
        if self.secret {
            self.value.zeroize();
        }
    }
}

/// The headers a server wants in addition, each one checked when it is added.
///
/// A header is a name and a value. The value is sent as given; the name is
/// sent in lower case by the transports of this crate (HTTP does not tell
/// the cases apart). What is refused ([`HeaderRefusal`]): a name that is not
/// letters, digits, `-` and `_`; a header the client writes itself; the same
/// name twice, whatever the case; a value with a control character or a
/// character that is not ASCII.
///
/// **A value may be a secret** (an API key). One added with
/// [`ExtraHeaders::with_secret`], or whose name says so
/// ([`sensitive_header`]), is never written in a URL, and is wiped from memory
/// when this list is dropped - the copy a transport makes to send it is the
/// transport's. The `Debug` of this type shows the names only.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ExtraHeaders(Vec<Extra>);

impl ExtraHeaders {
    /// No header.
    pub fn new() -> Self {
        Self::default()
    }

    /// These headers, in this order.
    pub fn from_pairs<N, V>(pairs: impl IntoIterator<Item = (N, V)>) -> Result<Self, HeaderRefusal>
    where
        N: Into<String>,
        V: Into<String>,
    {
        pairs
            .into_iter()
            .try_fold(Self::new(), |headers, (name, value)| headers.with(name, value))
    }

    /// One more header. Its value is held as a secret when its name says it
    /// is one ([`sensitive_header`]).
    pub fn with(self, name: impl Into<String>, value: impl Into<String>) -> Result<Self, HeaderRefusal> {
        let name = name.into();
        let secret = sensitive_header(&name);
        self.push(name, value.into(), secret)
    }

    /// One more header, whose value is a secret whatever its name - one read
    /// from a secret store, for instance.
    pub fn with_secret(
        self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, HeaderRefusal> {
        self.push(name.into(), value.into(), true)
    }

    fn push(mut self, name: String, value: String, secret: bool) -> Result<Self, HeaderRefusal> {
        // The value is taken first, so that it is wiped on every refusal too.
        let extra = Extra { name, value, secret };
        let name = extra.name.as_str();
        let token = !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'));
        if !token {
            return Err(HeaderRefusal::Name);
        }
        let lower = name.to_ascii_lowercase();
        if OWN.contains(&lower.as_str()) || lower.starts_with(OWN_PREFIX) {
            return Err(HeaderRefusal::Own(name.to_string()));
        }
        if self.0.iter().any(|other| other.name.eq_ignore_ascii_case(name)) {
            return Err(HeaderRefusal::Twice(name.to_string()));
        }
        // What a header value may hold and every transport can write
        // (RFC 9110 §5.5): visible ASCII, the space and the tab. A byte
        // above 0x7f is obsolete text there, and a WebSocket handshake
        // refuses it after the connection is made.
        let sendable = extra
            .value
            .bytes()
            .all(|byte| byte == b'\t' || (0x20..=0x7e).contains(&byte));
        if !sendable {
            return Err(HeaderRefusal::Value(name.to_string()));
        }
        self.0.push(extra);
        Ok(self)
    }

    /// These headers, then `others` - one list, or the refusal of a header
    /// both give.
    pub fn and(self, others: &ExtraHeaders) -> Result<Self, HeaderRefusal> {
        others.0.iter().try_fold(self, |headers, extra| {
            headers.push(extra.name.clone(), extra.value.clone(), extra.secret)
        })
    }

    /// Whether one of them is held as a secret. A request that carries one
    /// follows no redirection: the header would go to another host.
    pub fn has_secret(&self) -> bool {
        self.0.iter().any(|extra| extra.secret)
    }

    /// Whether there is none.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// How many there are.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Their names, in the order given.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(|extra| extra.name.as_str())
    }

    /// Each header as `(name, value)`, for a transport to send - secrets
    /// among them: never print what this gives.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|extra| (extra.name.as_str(), extra.value.as_str()))
    }

    /// `handshake` with these headers, where its profile can put them: a
    /// header for a program, the query string for a front end (§4.3.2).
    ///
    /// A secret is never written in a URL: under [`Profile::Browser`] it is
    /// refused, by name.
    pub fn onto(&self, handshake: Handshake) -> Result<Handshake, HeaderRefusal> {
        self.fit(handshake.profile)?;
        Ok(self.0.iter().fold(handshake, |handshake, extra| {
            handshake.with_attribute(&extra.name, &extra.value)
        }))
    }

    /// Whether a handshake of this profile can carry these headers, with
    /// nothing built: what [`ExtraHeaders::onto`] would refuse.
    pub fn fit(&self, profile: Profile) -> Result<(), HeaderRefusal> {
        match self.0.iter().find(|extra| extra.secret) {
            Some(extra) if profile == Profile::Browser => Err(HeaderRefusal::InTheUrl(extra.name.clone())),
            _ => Ok(()),
        }
    }
}

impl fmt::Debug for ExtraHeaders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.names()).finish()
    }
}
