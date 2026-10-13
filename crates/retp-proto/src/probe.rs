//! Before the WebSocket: where the server describes itself, and how to ask
//! it for a connection (§4.3, p.97-102).
//!
//! No HTTP here either - the transport fetches; this says **what** to fetch,
//! **what** to send in the handshake, and reads what comes back.

use std::collections::BTreeMap;
use std::fmt;

use retp_avro::Avro;
use retp_types::datatypes::{
    ArrayOfString, Contact, DataValue, DataValueItem, ServerCapabilities, SupportedDataObject,
    SupportedProtocol, Version,
};
use serde_json::Value as Json;

use crate::credentials::Credentials;

/// The `sec-websocket-protocol` of ETP v1.2 (§4.3 step 4).
pub const SUBPROTOCOL: &str = "etp12.energistics.org";

/// The path a server describes itself at, relative to its WebSocket URL.
pub const WELL_KNOWN: &str = ".well-known/etp-server-capabilities";

/// A URL this module cannot work from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UrlRefusal {
    /// The URL given.
    pub url: String,
    /// Why.
    pub why: &'static str,
}

impl fmt::Display for UrlRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "`{}`: {}", self.url, self.why)
    }
}

impl std::error::Error for UrlRefusal {}

/// `ws://` or `wss://`, the rest, and whether it is TLS.
fn split(url: &str) -> Result<(bool, &str), UrlRefusal> {
    let refuse = |why| UrlRefusal {
        url: url.to_string(),
        why,
    };
    let (tls, rest) = if let Some(rest) = url.strip_prefix("wss://") {
        (true, rest)
    } else if let Some(rest) = url.strip_prefix("ws://") {
        (false, rest)
    } else {
        return Err(refuse("an ETP endpoint is a ws:// or wss:// URL"));
    };
    if rest.is_empty() || rest.starts_with('/') {
        return Err(refuse("the URL has no host"));
    }
    Ok((tls, rest))
}

/// The URL of the server's `ServerCapabilities` for ETP v1.2.
///
/// Derived from the WebSocket URL, never asked for (§4.3.1 §2): the scheme
/// follows its "TLS-ness" - `wss` gives `https`, `ws` gives `http` - the path
/// is kept, the query and fragment (which are the handshake's) are not. And
/// `GetVersion=etp12.energistics.org` is **always** there: without it a server
/// answers for ETP 1.1, or with HTTP 400 if it only speaks 1.2 (§4.3.1 §3.e).
pub fn server_capabilities_url(websocket: &str) -> Result<String, UrlRefusal> {
    let (tls, rest) = split(websocket)?;
    let rest = rest.split(['?', '#']).next().unwrap_or(rest);
    let base = rest.trim_end_matches('/');
    let scheme = if tls { "https" } else { "http" };
    Ok(format!("{scheme}://{base}/{WELL_KNOWN}?GetVersion={SUBPROTOCOL}"))
}

/// Where the client runs, which decides how the handshake says things.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// A program that can set request headers.
    Native,
    /// A browser: the HTML5 WebSocket API sets no header but
    /// `sec-websocket-protocol`, so ETP's attributes go in the query string
    /// (§4.3.2).
    Browser,
}

/// What the handshake is to carry.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HandshakeSettings {
    /// `MaxWebSocketMessagePayloadSize` - the smaller of ours and the
    /// server's, when the probe gave its own (§4.3 step 2.b).
    pub max_message: Option<i64>,
    /// `MaxWebSocketFramePayloadSize`, likewise.
    pub max_frame: Option<i64>,
    /// The credentials, if the client has them before connecting.
    pub authorization: Option<Credentials>,
}

/// The WebSocket request to make.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    /// The URL to open, query string included.
    pub url: String,
    /// `sec-websocket-protocol`.
    pub subprotocols: Vec<&'static str>,
    /// Request headers - lower case, as §4.3 step 4.c asks. Empty for a browser.
    pub headers: Vec<(String, String)>,
    /// The credentials to send in an `Authorize` once the WebSocket is open,
    /// because this profile cannot carry them in the handshake.
    pub authorize_after_connecting: Option<Credentials>,
    /// Who makes it - which decides where an attribute can go.
    pub profile: Profile,
}

impl Handshake {
    /// One more attribute, where this profile can put it: a header for a
    /// program, the query string for a browser (§4.3.2) - `data-partition-id`
    /// for OSDU. Never credentials: those go in `Authorize`.
    pub fn with_attribute(mut self, name: &str, value: &str) -> Self {
        match self.profile {
            Profile::Native => self.headers.push((name.to_ascii_lowercase(), value.to_string())),
            Profile::Browser => {
                let separator = if self.url.contains('?') { '&' } else { '?' };
                self.url = format!("{}{separator}{}={}", self.url, encode(name), encode(value));
            }
        }
        self
    }
}

/// The handshake for `websocket`, as `profile` can make it.
///
/// **Credentials never go in a URL.** A browser cannot set an `authorization`
/// header, and a query string ends up in the server's logs; so a browser sends
/// its credentials in `Authorize`, once connected - the application-level
/// authorization §4.1.4.1 makes mandatory for browsers. The encoding is always
/// `binary`: `json` "is for internal testing and debugging only" (§4.3).
pub fn handshake(
    websocket: &str,
    profile: Profile,
    settings: &HandshakeSettings,
) -> Result<Handshake, UrlRefusal> {
    split(websocket)?;
    let mut attributes: Vec<(String, String)> = vec![("etp-encoding".into(), "binary".into())];
    if let Some(size) = settings.max_message {
        attributes.push(("maxwebsocketmessagepayloadsize".into(), size.to_string()));
    }
    if let Some(size) = settings.max_frame {
        attributes.push(("maxwebsocketframepayloadsize".into(), size.to_string()));
    }
    match profile {
        Profile::Native => {
            let mut headers = attributes;
            if let Some(credentials) = &settings.authorization {
                headers.push(("authorization".into(), credentials.header().to_string()));
            }
            Ok(Handshake {
                url: websocket.to_string(),
                subprotocols: vec![SUBPROTOCOL],
                headers,
                authorize_after_connecting: None,
                profile,
            })
        }
        Profile::Browser => {
            let query = attributes
                .iter()
                .map(|(k, v)| format!("{}={}", encode(k), encode(v)))
                .collect::<Vec<_>>()
                .join("&");
            let separator = if websocket.contains('?') { '&' } else { '?' };
            Ok(Handshake {
                url: format!("{websocket}{separator}{query}"),
                subprotocols: vec![SUBPROTOCOL],
                headers: Vec::new(),
                authorize_after_connecting: settings.authorization.clone(),
                profile,
            })
        }
    }
}

/// Percent-encoding for a query string (§4.3.2 §2): everything but the
/// unreserved characters of RFC 3986.
fn encode(text: &str) -> String {
    let mut out = String::new();
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Why a `ServerCapabilities` could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeRefusal {
    /// Which encoding was read.
    pub encoding: &'static str,
    /// What went wrong, and where.
    pub why: String,
}

impl fmt::Display for ProbeRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the ServerCapabilities ({}) could not be read: {}",
            self.encoding, self.why
        )
    }
}

impl std::error::Error for ProbeRefusal {}

/// Read the body of the probe.
///
/// `avro/binary` is the default every server MUST support; `application/json`
/// is Avro's JSON encoding (§4.3.1 §4.a). Without a content type the first
/// byte decides, and it can: a JSON document opens with `{`, and a binary one
/// opens with the length of `applicationName`, which `{` would make negative.
pub fn parse_server_capabilities(
    body: &[u8],
    content_type: Option<&str>,
) -> Result<ServerCapabilities, ProbeRefusal> {
    let json = match content_type.map(|c| c.split(';').next().unwrap_or(c).trim().to_ascii_lowercase()) {
        Some(c) if c == "application/json" => true,
        Some(c) if c == "avro/binary" || c == "application/octet-stream" => false,
        _ => body.iter().find(|b| !b.is_ascii_whitespace()) == Some(&b'{'),
    };
    if !json {
        return ServerCapabilities::from_avro(body).map_err(|why| ProbeRefusal {
            encoding: "avro/binary",
            why: why.to_string(),
        });
    }
    let value: Json = serde_json::from_slice(body).map_err(|why| ProbeRefusal {
        encoding: "application/json",
        why: why.to_string(),
    })?;
    from_json(&value).map_err(|why| ProbeRefusal {
        encoding: "application/json",
        why,
    })
}

// ---------------------------------------------------------------------------
// Avro's JSON encoding, read tolerantly where the encoding is ambiguous
// ---------------------------------------------------------------------------

fn field<'a>(object: &'a serde_json::Map<String, Json>, name: &str) -> Option<&'a Json> {
    object.get(name).filter(|v| !v.is_null())
}

fn string(object: &serde_json::Map<String, Json>, name: &str) -> Result<String, String> {
    match field(object, name) {
        None => Ok(String::new()),
        Some(Json::String(s)) => Ok(s.clone()),
        Some(other) => Err(format!("`{name}` is {other}, not a string")),
    }
}

fn strings(
    object: &serde_json::Map<String, Json>,
    name: &str,
    default: &[&str],
) -> Result<Vec<String>, String> {
    match field(object, name) {
        None => Ok(default.iter().map(|s| s.to_string()).collect()),
        Some(Json::Array(items)) => items
            .iter()
            .map(|i| {
                i.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| format!("`{name}` holds {i}, not a string"))
            })
            .collect(),
        Some(other) => Err(format!("`{name}` is {other}, not an array")),
    }
}

fn int(object: &serde_json::Map<String, Json>, name: &str) -> Result<i32, String> {
    match field(object, name) {
        None => Ok(0),
        Some(v) => v
            .as_i64()
            .and_then(|n| i32::try_from(n).ok())
            .ok_or_else(|| format!("`{name}` is {v}, not an int")),
    }
}

fn object<'a>(value: &'a Json, what: &str) -> Result<&'a serde_json::Map<String, Json>, String> {
    value
        .as_object()
        .ok_or_else(|| format!("{what} is {value}, not an object"))
}

/// A `DataValue`, from Avro JSON (`{"item": {"long": 5}}`) or plain JSON
/// (`5`): servers write both, and the value is the same either way.
fn data_value(value: &Json) -> Result<DataValue, String> {
    let item = match value {
        Json::Object(map) if map.len() == 1 && map.contains_key("item") => return data_value(&map["item"]),
        Json::Null => DataValueItem::Null,
        Json::Bool(b) => DataValueItem::Boolean(*b),
        Json::Number(n) => match n.as_i64() {
            Some(v) => DataValueItem::Long(v),
            None => DataValueItem::Double(n.as_f64().unwrap_or_default()),
        },
        Json::String(s) => DataValueItem::String(s.clone()),
        Json::Array(items) if items.iter().all(Json::is_string) => {
            DataValueItem::ArrayOfString(ArrayOfString {
                values: items
                    .iter()
                    .filter_map(|i| i.as_str().map(str::to_string))
                    .collect(),
            })
        }
        Json::Object(map) if map.len() == 1 => {
            let (branch, inner) = map.iter().next().expect("one entry");
            let branch = branch.rsplit('.').next().unwrap_or(branch);
            match (branch, inner) {
                ("boolean", Json::Bool(b)) => DataValueItem::Boolean(*b),
                ("int" | "long", Json::Number(n)) if n.is_i64() => {
                    DataValueItem::Long(n.as_i64().unwrap_or_default())
                }
                ("float" | "double", Json::Number(n)) => {
                    DataValueItem::Double(n.as_f64().unwrap_or_default())
                }
                ("string", Json::String(s)) => DataValueItem::String(s.clone()),
                ("ArrayOfString", Json::Object(array)) => DataValueItem::ArrayOfString(ArrayOfString {
                    values: strings(array, "values", &[])?,
                }),
                _ => {
                    return Err(format!(
                        "a DataValue `{branch}` holding {inner} is not one this client reads"
                    ));
                }
            }
        }
        other => return Err(format!("{other} is not a DataValue")),
    };
    Ok(DataValue { item })
}

fn capabilities(
    object: &serde_json::Map<String, Json>,
    name: &str,
) -> Result<BTreeMap<String, DataValue>, String> {
    match field(object, name) {
        None => Ok(BTreeMap::new()),
        Some(Json::Object(map)) => map
            .iter()
            .map(|(k, v)| {
                data_value(v)
                    .map(|d| (k.clone(), d))
                    .map_err(|why| format!("`{name}.{k}`: {why}"))
            })
            .collect(),
        Some(other) => Err(format!("`{name}` is {other}, not a map")),
    }
}

fn from_json(value: &Json) -> Result<ServerCapabilities, String> {
    let root = object(value, "the document")?;
    let contact = match field(root, "contactInformation") {
        None => Contact::default(),
        Some(c) => {
            let c = object(c, "`contactInformation`")?;
            Contact {
                organization_name: string(c, "organizationName")?,
                contact_name: string(c, "contactName")?,
                contact_phone: string(c, "contactPhone")?,
                contact_email: string(c, "contactEmail")?,
            }
        }
    };
    let mut data_objects = Vec::new();
    if let Some(items) = field(root, "supportedDataObjects") {
        for item in items.as_array().ok_or("`supportedDataObjects` is not an array")? {
            let item = object(item, "a supported data object")?;
            data_objects.push(SupportedDataObject {
                qualified_type: string(item, "qualifiedType")?,
                data_object_capabilities: capabilities(item, "dataObjectCapabilities")?,
            });
        }
    }
    let mut protocols = Vec::new();
    if let Some(items) = field(root, "supportedProtocols") {
        for item in items.as_array().ok_or("`supportedProtocols` is not an array")? {
            let item = object(item, "a supported protocol")?;
            let version = match field(item, "protocolVersion") {
                None => Version::default(),
                Some(v) => {
                    let v = object(v, "`protocolVersion`")?;
                    Version {
                        major: int(v, "major")?,
                        minor: int(v, "minor")?,
                        revision: int(v, "revision")?,
                        patch: int(v, "patch")?,
                    }
                }
            };
            protocols.push(SupportedProtocol {
                protocol: int(item, "protocol")?,
                protocol_version: version,
                role: string(item, "role")?,
                protocol_capabilities: capabilities(item, "protocolCapabilities")?,
            });
        }
    }
    Ok(ServerCapabilities {
        application_name: string(root, "applicationName")?,
        application_version: string(root, "applicationVersion")?,
        contact_information: contact,
        supported_compression: strings(root, "supportedCompression", &[])?,
        supported_encodings: strings(root, "supportedEncodings", &["binary"])?,
        supported_formats: strings(root, "supportedFormats", &["xml"])?,
        supported_data_objects: data_objects,
        supported_protocols: protocols,
        endpoint_capabilities: capabilities(root, "endpointCapabilities")?,
    })
}
