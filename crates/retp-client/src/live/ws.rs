//! The asynchronous WebSocket of the live session: tokio, tokio-rustls and
//! tokio-tungstenite, opened as `native::WebSocketTransport::connect` opens
//! the blocking one - same request, same headers, same sub-protocol check.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use retp_proto::probe::{self, Handshake};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{self, Message};

use crate::TransportError;
use crate::native::{CertificateCheck, header, tls_config};

/// How long a TCP connection may take to open (as for the blocking client).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long closing the WebSocket may take before it is dropped.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(2);

/// A TCP connection, encrypted or not.
pub(super) enum Stream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for Stream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Stream::Tls(s) => Pin::new(&mut **s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Stream::Tls(s) => Pin::new(&mut **s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_flush(cx),
            Stream::Tls(s) => Pin::new(&mut **s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Stream::Tls(s) => Pin::new(&mut **s).poll_shutdown(cx),
        }
    }
}

/// An open WebSocket to an ETP server.
pub(super) struct Ws {
    inner: WebSocketStream<Stream>,
}

impl Ws {
    /// Open the WebSocket a [`Handshake`] describes.
    pub(super) async fn open(handshake: &Handshake, check: CertificateCheck) -> Result<Self, TransportError> {
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

        let tcp = connect_tcp(&host, port).await?;
        tcp.set_nodelay(true).ok();
        let stream = if tls {
            let name = ServerName::try_from(host.clone())
                .map_err(|why| TransportError::new(format!("`{host}` is not a name TLS can check: {why}")))?;
            let secured = TlsConnector::from(tls_config(check))
                .connect(name, tcp)
                .await
                .map_err(|why| TransportError::new(format!("TLS with {host}: {why}")))?;
            Stream::Tls(Box::new(secured))
        } else {
            Stream::Plain(tcp)
        };
        let (inner, response) = tokio_tungstenite::client_async(request, stream)
            .await
            .map_err(|why| refused_handshake(&handshake.url, why))?;
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
        Ok(Self { inner })
    }

    /// Send one binary message, flushed.
    pub(super) async fn send(&mut self, message: Vec<u8>) -> Result<(), TransportError> {
        self.inner
            .send(Message::Binary(message.into()))
            .await
            .map_err(|why| TransportError::new(format!("sending: {why}")))
    }

    /// The next binary message. Cancel-safe: dropping the future loses nothing.
    /// Ping and Pong frames are left to the WebSocket layer.
    pub(super) async fn receive(&mut self) -> Result<Vec<u8>, TransportError> {
        loop {
            match self.inner.next().await {
                Some(Ok(Message::Binary(bytes))) => return Ok(bytes.to_vec()),
                Some(Ok(Message::Text(text))) => {
                    return Err(TransportError::new(format!(
                        "a text message arrived ({} bytes); ETP v1.2 in binary encoding sends binary messages only",
                        text.len()
                    )));
                }
                Some(Ok(Message::Close(frame))) => {
                    return Err(TransportError::new(match frame {
                        Some(f) => format!("the server closed the WebSocket: {} {}", f.code, f.reason),
                        None => "the server closed the WebSocket".to_string(),
                    }));
                }
                Some(Ok(_)) => continue,
                Some(Err(why)) => return Err(TransportError::new(format!("receiving: {why}"))),
                None => return Err(TransportError::new("the WebSocket is closed")),
            }
        }
    }

    /// Close the WebSocket, waiting a little for the server's answer.
    pub(super) async fn close(&mut self) {
        let _ = tokio::time::timeout(CLOSE_TIMEOUT, self.inner.close(None)).await;
    }
}

/// Every address the name resolves to, each tried in turn.
async fn connect_tcp(host: &str, port: u16) -> Result<TcpStream, TransportError> {
    let addresses = tokio::net::lookup_host((host, port))
        .await
        .map_err(|why| TransportError::new(format!("resolving {host}: {why}")))?;
    let mut last = None;
    for address in addresses {
        match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(address)).await {
            Ok(Ok(stream)) => return Ok(stream),
            Ok(Err(why)) => last = Some(why.to_string()),
            Err(_) => last = Some("timed out".to_string()),
        }
    }
    Err(TransportError::new(match last {
        Some(why) => format!("connecting to {host}:{port}: {why}"),
        None => format!("{host} resolves to no address"),
    }))
}

/// A failed handshake, said as what it means (the same words as
/// `native::refused_handshake`, which takes the blocking handshake's error
/// type and so cannot be called here): an HTTP 401 or 403 is about the
/// credentials, and says so with the server's challenge (§4.1.3).
fn refused_handshake(url: &str, error: tungstenite::Error) -> TransportError {
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
