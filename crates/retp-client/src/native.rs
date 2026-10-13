//! The native transport: a blocking WebSocket over rustls, and the HTTP probe.
//!
//! What goes over the network lives here and nowhere else in the crate, and
//! none of it is compiled for wasm. TLS is **rustls with `ring`**: the
//! specification defines no encryption of its own and relies on TLS 1.2 or
//! more (§4.1.1 §1.a), which rustls speaks and no less. The trusted roots are
//! Mozilla's (`webpki-roots`).

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use retp_avro::EtpMessage;
use retp_proto::connection::Handler;
use retp_proto::credentials::Credentials;
use retp_proto::describe::Widened;
use retp_proto::negotiate::Offer;
use retp_proto::probe::{self, Handshake, HandshakeSettings, Profile};
use retp_types::datatypes::ServerCapabilities;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{
    ClientConfig, ClientConnection, DigitallySignedStruct, RootCertStore, SignatureScheme, StreamOwned,
};
use tungstenite::client::IntoClientRequest;
use tungstenite::http::HeaderValue;
use tungstenite::{Message, WebSocket};

use crate::rest::{Http, HttpRequest, HttpResponse, Method, RestClient};
use crate::{Client, ClientError, ServerProfile, Settings, SystemClock, Transport, TransportError};

/// Whether the server's certificate is checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CertificateCheck {
    /// Checked against Mozilla's roots, its name and its dates - always, unless
    /// asked otherwise.
    #[default]
    Verify,
    /// **Not checked**: an expired, self-signed or misnamed certificate is
    /// accepted, and so is anyone in the middle presenting one. The session is
    /// still encrypted, and the handshake signatures are still verified - but
    /// against a key nobody vouched for. For a server whose certificate is
    /// known to be wrong, never as a default.
    Skip,
}

impl CertificateCheck {
    /// What the user is told when the check is off.
    pub fn warning(&self, url: &str) -> Option<String> {
        match self {
            CertificateCheck::Verify => None,
            CertificateCheck::Skip => Some(format!(
                "the certificate of {url} is not checked: the session is encrypted, but to a server nobody \
                 vouched for - the credentials go to whoever answers"
            )),
        }
    }
}

/// A TCP connection, encrypted or not.
enum Stream {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
}

impl Stream {
    fn tcp(&self) -> &TcpStream {
        match self {
            Stream::Plain(s) => s,
            Stream::Tls(s) => &s.sock,
        }
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(s) => s.read(buf),
            Stream::Tls(s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(s) => s.write(buf),
            Stream::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Plain(s) => s.flush(),
            Stream::Tls(s) => s.flush(),
        }
    }
}

/// The verifier of [`CertificateCheck::Skip`]: any certificate is taken, and
/// the handshake signatures are still checked with the provider's algorithms -
/// so the peer at least holds the key of the certificate it shows.
#[derive(Debug)]
struct AnyCertificate(Arc<CryptoProvider>);

impl ServerCertVerifier for AnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

fn tls_config(check: CertificateCheck) -> Arc<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("ring supports TLS 1.2 and 1.3");
    let config = match check {
        CertificateCheck::Verify => {
            let mut roots = RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            builder.with_root_certificates(roots).with_no_client_auth()
        }
        CertificateCheck::Skip => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AnyCertificate(provider)))
            .with_no_client_auth(),
    };
    Arc::new(config)
}

/// The HTTP client of the probe and of the logins: HTTP errors come back as
/// responses, so their bodies - which say *why* - can be read.
pub(crate) fn http_agent(check: CertificateCheck) -> ureq::Agent {
    let tls = ureq::tls::TlsConfig::builder()
        .disable_verification(check == CertificateCheck::Skip)
        .build();
    ureq::Agent::config_builder()
        .tls_config(tls)
        .http_status_as_error(false)
        .timeout_connect(Some(CONNECT_TIMEOUT))
        .timeout_global(Some(Duration::from_secs(60)))
        .build()
        .new_agent()
}

/// A server: where it is, its habits, and how the client gets in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// The server's WebSocket URL.
    pub url: String,
    /// Its habits.
    pub profile: ServerProfile,
    /// The credentials, if the server wants some.
    pub credentials: Option<Credentials>,
    /// Whether its certificate is checked.
    pub certificates: CertificateCheck,
    /// How the handshake is made: [`Profile::Native`] puts credentials and
    /// attributes in headers; [`Profile::Browser`] does what a front end is
    /// limited to - attributes in the query string, credentials in an
    /// `Authorize` once connected (§4.1.4.1, §4.3.2) -, which tells from
    /// here whether a server will take a front end.
    pub handshake: Profile,
}

impl Endpoint {
    /// A server that asks for nothing, with its certificate checked.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            profile: ServerProfile::Standard,
            credentials: None,
            certificates: CertificateCheck::Verify,
            handshake: Profile::Native,
        }
    }
}

/// A WebSocket to an ETP server.
pub struct WebSocketTransport {
    socket: WebSocket<Stream>,
    closed: bool,
}

impl WebSocketTransport {
    /// Open the WebSocket a [`Handshake`] describes.
    pub fn connect(handshake: &Handshake, certificates: CertificateCheck) -> Result<Self, TransportError> {
        let mut request = handshake
            .url
            .as_str()
            .into_client_request()
            .map_err(|why| TransportError::new(format!("{}: {why}", handshake.url)))?;
        let uri = request.uri().clone();
        let tls = uri.scheme_str() == Some("wss");
        let host = uri
            .host()
            .ok_or_else(|| TransportError::new(format!("{}: no host", handshake.url)))?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();
        let port = uri.port_u16().unwrap_or(if tls { 443 } else { 80 });
        let headers = request.headers_mut();
        let protocols = handshake.subprotocols.join(", ");
        headers.insert("sec-websocket-protocol", header(&protocols)?);
        for (name, value) in &handshake.headers {
            let name = tungstenite::http::HeaderName::from_bytes(name.as_bytes())
                .map_err(|why| TransportError::new(format!("header `{name}`: {why}")))?;
            headers.insert(name, header(value)?);
        }

        let tcp = connect_tcp(&host, port)?;
        tcp.set_nodelay(true).ok();
        let stream = if tls {
            let name = ServerName::try_from(host.clone())
                .map_err(|why| TransportError::new(format!("`{host}` is not a name TLS can check: {why}")))?;
            let connection = ClientConnection::new(tls_config(certificates), name)
                .map_err(|why| TransportError::new(format!("TLS with {host}: {why}")))?;
            Stream::Tls(Box::new(StreamOwned::new(connection, tcp)))
        } else {
            Stream::Plain(tcp)
        };
        let (socket, response) =
            tungstenite::client(request, stream).map_err(|why| refused_handshake(&handshake.url, why))?;
        let agreed = response
            .headers()
            .get("sec-websocket-protocol")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if agreed != probe::SUBPROTOCOL {
            return Err(TransportError::new(format!(
                "the server agreed to `{agreed}` as its WebSocket protocol, not `{}`",
                probe::SUBPROTOCOL
            )));
        }
        Ok(Self {
            socket,
            closed: false,
        })
    }
}

/// A failed handshake, said as what it means: an HTTP 401 or 403 is about
/// the credentials, and says so with the server's challenge (§4.1.3).
fn refused_handshake(
    url: &str,
    why: tungstenite::HandshakeError<tungstenite::ClientHandshake<Stream>>,
) -> TransportError {
    let tungstenite::HandshakeError::Failure(error) = why else {
        return TransportError::new(format!("the WebSocket handshake with {url} was interrupted"));
    };
    if let tungstenite::Error::Http(response) = &error {
        let status = response.status();
        let challenge = response
            .headers()
            .get_all("www-authenticate")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .collect::<Vec<_>>()
            .join(" | ");
        let said = match status.as_u16() {
            401 if challenge.is_empty() => " - credentials missing or refused".to_string(),
            401 => format!(" - credentials missing or refused; the server asks for: {challenge}"),
            403 => " - the credentials were understood, and do not give access here".to_string(),
            _ => String::new(),
        };
        return TransportError::new(format!(
            "the WebSocket handshake with {url} was answered HTTP {status}{said}"
        ));
    }
    TransportError::new(format!("the WebSocket handshake with {url} failed: {error}"))
}

/// How long a TCP connection may take to open. The system's own wait on an
/// address that does not answer is twenty seconds or more on Windows, for a
/// failure that says nothing more after twenty seconds than after ten.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Every address the name resolves to, each tried in turn.
fn connect_tcp(host: &str, port: u16) -> Result<TcpStream, TransportError> {
    use std::net::ToSocketAddrs;
    let addresses: Vec<_> = (host, port)
        .to_socket_addrs()
        .map_err(|why| TransportError::new(format!("resolving {host}: {why}")))?
        .collect();
    let mut last = None;
    for address in &addresses {
        match TcpStream::connect_timeout(address, CONNECT_TIMEOUT) {
            Ok(stream) => return Ok(stream),
            Err(why) => last = Some(why),
        }
    }
    Err(TransportError::new(match last {
        Some(why) => format!("connecting to {host}:{port}: {why}"),
        None => format!("{host} resolves to no address"),
    }))
}

fn header(value: &str) -> Result<HeaderValue, TransportError> {
    HeaderValue::from_str(value).map_err(|why| TransportError::new(format!("header value: {why}")))
}

impl Transport for WebSocketTransport {
    fn send(&mut self, message: &[u8]) -> Result<(), TransportError> {
        self.socket
            .send(Message::Binary(message.to_vec().into()))
            .map_err(|why| TransportError::new(format!("sending: {why}")))
    }

    fn receive(&mut self, wait: Duration) -> Result<Option<Vec<u8>>, TransportError> {
        let wait = wait.max(Duration::from_millis(1));
        self.socket
            .get_ref()
            .tcp()
            .set_read_timeout(Some(wait))
            .map_err(|why| TransportError::new(format!("setting the read timeout: {why}")))?;
        loop {
            match self.socket.read() {
                Ok(Message::Binary(bytes)) => return Ok(Some(bytes.to_vec())),
                Ok(Message::Text(text)) => {
                    return Err(TransportError::new(format!(
                        "a text message arrived ({} bytes); ETP v1.2 in binary encoding sends binary messages only",
                        text.len()
                    )));
                }
                Ok(Message::Close(frame)) => {
                    self.closed = true;
                    return Err(TransportError::new(match frame {
                        Some(f) => format!("the server closed the WebSocket: {} {}", f.code, f.reason),
                        None => "the server closed the WebSocket".to_string(),
                    }));
                }
                // Ping, Pong and raw frames are the WebSocket's own business.
                Ok(_) => continue,
                Err(tungstenite::Error::Io(e))
                    if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) =>
                {
                    return Ok(None);
                }
                Err(why) => {
                    self.closed = true;
                    return Err(TransportError::new(format!("receiving: {why}")));
                }
            }
        }
    }

    fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            let _ = self.socket.close(None);
            let _ = self.socket.flush();
        }
    }
}

/// Why the probe failed.
#[derive(Debug)]
pub enum ProbeError {
    /// The URL is not an ETP endpoint's.
    Url(probe::UrlRefusal),
    /// The HTTP request failed - refused, unreachable, an error status.
    Http(String),
    /// The body is not a `ServerCapabilities`.
    Body(probe::ProbeRefusal),
}

impl std::fmt::Display for ProbeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProbeError::Url(why) => write!(f, "{why}"),
            ProbeError::Http(why) => write!(f, "{why}"),
            ProbeError::Body(why) => write!(f, "{why}"),
        }
    }
}

impl std::error::Error for ProbeError {}

/// `GET …/.well-known/etp-server-capabilities` for `endpoint` (§4.3.1): what
/// it speaks, its limits, how to get a token.
pub fn fetch_server_capabilities(endpoint: &Endpoint) -> Result<(String, ServerCapabilities), ProbeError> {
    let url = probe::server_capabilities_url(&endpoint.url).map_err(ProbeError::Url)?;
    let mut request = http_agent(endpoint.certificates)
        .get(&url)
        .header("accept", "avro/binary, application/json;q=0.5");
    if let ServerProfile::Rddms { data_partition } = &endpoint.profile {
        request = request.header("data-partition-id", data_partition);
    }
    if let Some(credentials) = &endpoint.credentials {
        request = request.header("authorization", credentials.header());
    }
    let mut response = request
        .call()
        .map_err(|why| ProbeError::Http(format!("GET {url}: {why}")))?;
    let status = response.status();
    if !status.is_success() {
        let challenge = response
            .headers()
            .get("www-authenticate")
            .and_then(|v| v.to_str().ok())
            .map(|c| format!("; the server asks for: {c}"))
            .unwrap_or_default();
        return Err(ProbeError::Http(format!("GET {url}: HTTP {status}{challenge}")));
    }
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let body = response
        .body_mut()
        .read_to_vec()
        .map_err(|why| ProbeError::Http(format!("GET {url}: reading the body: {why}")))?;
    let capabilities =
        probe::parse_server_capabilities(&body, content_type.as_deref()).map_err(ProbeError::Body)?;
    Ok((url, capabilities))
}

/// What [`fit_to_server`] learnt of the server before the session.
#[derive(Debug)]
pub enum Fitted {
    /// The server answered the probe: the types added to the offer, and
    /// what it announced wrong.
    Widened(Widened),
    /// It did not - some servers want a bearer token even for the probe -:
    /// the offer is left as it was.
    NotProbed(ProbeError),
}

/// Fit an offer to the server: probe what it supports (§4.3.1) and ask for
/// every data object type it announces as well ([`Offer::widened_to`]), so
/// that the session agrees to all the server has. What is added is logged
/// at `info`, what the server announces wrong at `warn`, a probe that fails
/// at `info`: a session opens without it.
pub fn fit_to_server(endpoint: &Endpoint, offer: Offer) -> (Offer, Fitted) {
    match fetch_server_capabilities(endpoint) {
        Ok((_, capabilities)) => {
            let (offer, widened) = offer.widened_to(&capabilities.supported_data_objects);
            for deviation in &widened.deviations {
                log::warn!(target: "retp_proto", "{}: {deviation}", capabilities.application_name);
            }
            if !widened.added.is_empty() {
                log::info!(
                    target: "retp_proto",
                    "the server supports {} as well: asked for too",
                    widened.added.join(", ")
                );
            }
            (offer, Fitted::Widened(widened))
        }
        Err(why) => {
            log::info!(
                target: "retp_proto",
                "the server's data object types are not known before the session ({why}): the offer is left as it is"
            );
            (offer, Fitted::NotProbed(why))
        }
    }
}

/// The REST transport's HTTP, natively: `ureq`, blocking - a request is over
/// at its first poll, as for the blocking client. Its agent waits longer than
/// the probe's: an array's block is a large answer.
pub struct UreqHttp {
    agent: ureq::Agent,
}

impl UreqHttp {
    /// An HTTP client checking certificates as `check` says.
    pub fn new(check: CertificateCheck) -> Self {
        let tls = ureq::tls::TlsConfig::builder()
            .disable_verification(check == CertificateCheck::Skip)
            .build();
        let agent = ureq::Agent::config_builder()
            .tls_config(tls)
            .http_status_as_error(false)
            .timeout_connect(Some(CONNECT_TIMEOUT))
            .timeout_global(Some(Duration::from_secs(300)))
            .build()
            .new_agent();
        Self { agent }
    }

    fn send_now(&self, request: &HttpRequest) -> Result<HttpResponse, TransportError> {
        let failed = |why: ureq::Error| TransportError::new(format!("{}: {why}", request.url));
        let mut response = match (request.method, &request.body) {
            (Method::Get, _) => {
                let mut r = self.agent.get(&request.url);
                for (k, v) in &request.headers {
                    r = r.header(k, v);
                }
                r.call()
            }
            (Method::Delete, _) => {
                let mut r = self.agent.delete(&request.url);
                for (k, v) in &request.headers {
                    r = r.header(k, v);
                }
                r.call()
            }
            (method, body) => {
                let mut r = if method == Method::Post {
                    self.agent.post(&request.url)
                } else {
                    self.agent.put(&request.url)
                };
                for (k, v) in &request.headers {
                    r = r.header(k, v);
                }
                r.send(body.as_deref().unwrap_or_default())
            }
        }
        .map_err(failed)?;
        let status = response.status().as_u16();
        let challenge = response
            .headers()
            .get("www-authenticate")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let body = response
            .body_mut()
            .with_config()
            .limit(u64::MAX)
            .read_to_vec()
            .map_err(|why| TransportError::new(format!("{}: reading the body: {why}", request.url)))?;
        Ok(HttpResponse {
            status,
            challenge,
            body,
        })
    }
}

impl Http for UreqHttp {
    fn send(&mut self, request: HttpRequest) -> impl Future<Output = Result<HttpResponse, TransportError>> {
        std::future::ready(self.send_now(&request))
    }

    /// `at_once` threads take the requests from the same queue, so that a
    /// long one does not hold the others back; each answer goes back to its
    /// place. `ureq`'s agent is shared: its connection pool is what makes a
    /// second request to the same host cheap.
    fn send_all(
        &mut self,
        requests: Vec<HttpRequest>,
        at_once: usize,
    ) -> impl Future<Output = Vec<Result<HttpResponse, TransportError>>> {
        let http = &*self;
        let next = std::sync::atomic::AtomicUsize::new(0);
        let done = std::sync::Mutex::new(Vec::with_capacity(requests.len()));
        let lanes = at_once.max(1).min(requests.len());
        std::thread::scope(|threads| {
            for _ in 0..lanes {
                threads.spawn(|| {
                    loop {
                        let index = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let Some(request) = requests.get(index) else { break };
                        let answer = http.send_now(request);
                        done.lock().expect("no sender panicked").push((index, answer));
                    }
                });
            }
        });
        let mut answers = done.into_inner().expect("no sender panicked");
        answers.sort_by_key(|(index, _)| *index);
        std::future::ready(answers.into_iter().map(|(_, answer)| answer).collect())
    }
}

impl crate::rest::BlockingHttp for UreqHttp {}

/// A client of the Reservoir DDMS REST API: `endpoint.url` is the API's base
/// (`https://…/api/reservoir-ddms/v2`), and its credentials, OSDU partition
/// and certificate check go with every request.
pub fn rest_client(endpoint: &Endpoint) -> RestClient<UreqHttp> {
    let partition = match &endpoint.profile {
        ServerProfile::Rddms { data_partition } => Some(data_partition.clone()),
        ServerProfile::Standard => None,
    };
    RestClient::new(
        UreqHttp::new(endpoint.certificates),
        &endpoint.url,
        endpoint.credentials.clone(),
        partition,
    )
}

/// A server, by either transport - what the command line holds: the same
/// commands run on both. What one transport
/// does not have is refused by name.
pub enum Remote {
    /// ETP over a WebSocket.
    Etp(Box<Client<WebSocketTransport, SystemClock>>),
    /// The Reservoir DDMS REST API.
    Rest(Box<RestClient<UreqHttp>>),
}

impl Remote {
    /// Whether it goes by REST.
    pub fn is_rest(&self) -> bool {
        matches!(self, Remote::Rest(_))
    }

    /// What the session agreed - for REST, what the client serves.
    pub fn negotiated(&self) -> retp_proto::negotiate::Negotiated {
        crate::Caller::negotiated(self)
    }

    /// A `Ping` and its `Pong`; over REST, `GET /health/info`.
    pub fn ping(&mut self) -> Result<Duration, ClientError> {
        match self {
            Remote::Etp(client) => client.ping(),
            Remote::Rest(rest) => {
                let started = std::time::Instant::now();
                crate::caller::run(rest.alive())?;
                Ok(started.elapsed())
            }
        }
    }

    /// What the REST server says of its build; `None` over a WebSocket.
    pub fn health(&mut self) -> Option<Result<String, ClientError>> {
        match self {
            Remote::Etp(_) => None,
            Remote::Rest(rest) => Some(crate::caller::run(rest.alive())),
        }
    }

    /// Objects, what they reference and their arrays' metadata, in one
    /// request - the REST API's own ([`RestClient::prefetch`]); no ETP
    /// message carries it.
    pub fn prefetch(&mut self, uris: &[String], arrays: bool) -> Result<crate::rest::Prefetch, ClientError> {
        match self {
            Remote::Rest(rest) => crate::caller::run(rest.prefetch(uris, arrays)),
            Remote::Etp(_) => Err(ClientError::Invalid(retp_proto::wire::EtpError::ENOTSUPPORTED.with_detail(
                "a prefetch is the Reservoir DDMS REST API's own: use --rest, or ask the objects and their arrays one request at a time",
            ))),
        }
    }

    /// Let `wait` go by listening; over REST there is nothing to listen to.
    pub fn idle(&mut self, wait: Duration) -> Result<(), ClientError> {
        match self {
            Remote::Etp(client) => client.idle(wait),
            Remote::Rest(_) => {
                std::thread::sleep(wait);
                Ok(())
            }
        }
    }

    /// Ask for an `Acknowledge` with every request; the REST API has none.
    pub fn ask_acknowledge(&mut self, on: bool) -> Result<(), ClientError> {
        match self {
            Remote::Etp(client) => {
                client.ask_acknowledge(on);
                Ok(())
            }
            Remote::Rest(_) if !on => Ok(()),
            Remote::Rest(_) => Err(ClientError::Invalid(
                retp_proto::wire::EtpError::ENOTSUPPORTED
                    .with_detail("the REST API sends no acknowledgement: use the ETP WebSocket"),
            )),
        }
    }

    /// Whether requests ask for an `Acknowledge`.
    pub fn asks_acknowledge(&self) -> bool {
        matches!(self, Remote::Etp(client) if client.asks_acknowledge())
    }

    /// The acknowledgements heard since the last call - none over REST.
    pub fn acknowledgements(&mut self) -> Vec<crate::Acknowledged> {
        match self {
            Remote::Etp(client) => client.acknowledgements(),
            Remote::Rest(_) => Vec::new(),
        }
    }

    /// What arrived unasked - nothing does over REST, which only answers.
    pub fn unsolicited(&mut self) -> Vec<retp_proto::connection::Event> {
        match self {
            Remote::Etp(client) => client.unsolicited(),
            Remote::Rest(_) => Vec::new(),
        }
    }

    /// The last `currentDateTime` heard from the server - the watermark a
    /// catch-up starts from; none over REST.
    pub fn watermark(&self) -> Option<i64> {
        match self {
            Remote::Etp(client) => client.watermark(),
            Remote::Rest(_) => None,
        }
    }

    /// The events of a subscribed protocol, after `wait` at most - nothing
    /// over REST, which hears nothing unasked.
    pub fn events(
        &mut self,
        protocol: i32,
        wait: Duration,
    ) -> Result<Vec<retp_proto::connection::Event>, ClientError> {
        match self {
            Remote::Etp(client) => client.events(protocol, wait),
            Remote::Rest(_) => Ok(Vec::new()),
        }
    }

    /// Keep a quiet session alive; a REST client has no session.
    pub fn set_keepalive(&mut self, period: Option<Duration>) {
        if let Remote::Etp(client) = self {
            client.set_keepalive(period);
        }
    }

    /// End the session; a REST client has none to end.
    pub fn close(self, reason: &str) -> Result<(), ClientError> {
        match self {
            Remote::Etp(client) => client.close(reason),
            Remote::Rest(_) => Ok(()),
        }
    }
}

impl crate::Caller for Remote {
    async fn call<M: retp_avro::EtpMessage>(
        &mut self,
        request: &M,
    ) -> Result<Vec<retp_proto::wire::Delivery>, ClientError> {
        match self {
            Remote::Etp(client) => crate::Caller::call(client.as_mut(), request).await,
            Remote::Rest(rest) => crate::Caller::call(rest.as_mut(), request).await,
        }
    }

    async fn call_all<M: retp_avro::EtpMessage>(
        &mut self,
        requests: &[M],
    ) -> Vec<Result<Vec<retp_proto::wire::Delivery>, ClientError>> {
        match self {
            Remote::Etp(client) => crate::Caller::call_all(client.as_mut(), requests).await,
            Remote::Rest(rest) => crate::Caller::call_all(rest.as_mut(), requests).await,
        }
    }

    fn at_once(&self) -> usize {
        match self {
            Remote::Etp(client) => crate::Caller::at_once(client.as_ref()),
            Remote::Rest(rest) => crate::Caller::at_once(rest.as_ref()),
        }
    }

    async fn tell<M: EtpMessage>(&mut self, message: &M) -> Result<(), ClientError> {
        match self {
            Remote::Etp(client) => crate::Caller::tell(client.as_mut(), message).await,
            Remote::Rest(rest) => crate::Caller::tell(rest.as_mut(), message).await,
        }
    }

    fn subscribe(&mut self, protocol: i32) {
        if let Remote::Etp(client) = self {
            client.subscribe(protocol);
        }
    }

    fn unsubscribe(&mut self, protocol: i32) {
        if let Remote::Etp(client) = self {
            client.unsubscribe(protocol);
        }
    }

    async fn events(&mut self, protocol: i32) -> Result<Vec<retp_proto::connection::Event>, ClientError> {
        match self {
            Remote::Etp(client) => crate::Caller::events(client.as_mut(), protocol).await,
            Remote::Rest(rest) => crate::Caller::events(rest.as_mut(), protocol).await,
        }
    }

    fn watermark(&self) -> Option<i64> {
        match self {
            Remote::Etp(client) => client.watermark(),
            Remote::Rest(_) => None,
        }
    }

    fn negotiated(&self) -> retp_proto::negotiate::Negotiated {
        match self {
            Remote::Etp(client) => crate::Caller::negotiated(client.as_ref()),
            Remote::Rest(rest) => crate::Caller::negotiated(rest.as_ref()),
        }
    }

    fn tolerance(&self) -> retp_proto::wire::Tolerance {
        match self {
            Remote::Etp(client) => client.tolerance(),
            Remote::Rest(rest) => crate::Caller::tolerance(rest.as_ref()),
        }
    }
}

impl crate::caller::Blocking for Remote {
    fn end(self, reason: &str) {
        let _ = self.close(reason);
    }
}

/// Everything a connection from the command line needs.
pub struct Connect {
    /// The server.
    pub endpoint: Endpoint,
    /// What this client asks for.
    pub offer: Offer,
    /// The protocols it implements.
    pub handlers: Vec<Box<dyn Handler>>,
    /// The smaller of our and the server's `MaxWebSocketMessagePayloadSize`,
    /// if the probe gave the server's.
    pub max_message: Option<i64>,
}

/// Open a WebSocket and a session over it.
pub fn connect(connect: Connect) -> Result<Client<WebSocketTransport, SystemClock>, ClientError> {
    let settings = HandshakeSettings {
        max_message: connect.max_message,
        max_frame: None,
        authorization: connect.endpoint.credentials.clone(),
    };
    let handshake = probe::handshake(&connect.endpoint.url, connect.endpoint.handshake, &settings)
        .map_err(|why| ClientError::Transport(TransportError::new(why.to_string())))?;
    let handshake = connect.endpoint.profile.adapt(handshake);
    let transport = WebSocketTransport::connect(&handshake, connect.endpoint.certificates)?;
    Client::open(
        transport,
        SystemClock,
        connect.offer,
        connect.handlers,
        connect.endpoint.credentials,
        Settings::default(),
    )
}

/// A [`Connector`](crate::resume::Connector) for the native transport:
/// `open` opens a session **from the start** - login, handshake, negotiation
/// -, and a pause is a sleep.
pub struct Reopen<F>(pub F);

impl<F, S, E> crate::resume::Connector for Reopen<F>
where
    F: FnMut() -> Result<S, E>,
    S: crate::caller::Blocking,
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Session = S;

    fn connect(&mut self) -> Result<S, Box<dyn std::error::Error + Send + Sync>> {
        (self.0)().map_err(Into::into)
    }

    fn pause(&mut self, how_long: Duration) {
        std::thread::sleep(how_long);
    }
}

/// A UUID of version 4, from the randomness `std` seeds its hash maps with -
/// enough for a `clientInstanceId`, which only has to differ between two
/// instances (§5.3.1), with no crate for it.
pub fn random_uuid() -> retp_types::datatypes::Uuid {
    use std::hash::{BuildHasher, Hasher};
    let mut bytes = [0u8; 16];
    for (half, chunk) in bytes.chunks_mut(8).enumerate() {
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_usize(half);
        hasher.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos()),
        );
        chunk.copy_from_slice(&hasher.finish().to_le_bytes());
    }
    bytes[6] = (bytes[6] & 0x0F) | 0x40;
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
    retp_types::datatypes::Uuid(bytes)
}
