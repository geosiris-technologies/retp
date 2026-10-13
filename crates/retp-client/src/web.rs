//! The transports a front end has: `fetch` for the REST API, and the browser's
//! own WebSocket for ETP - the two doors a front end has.
//!
//! Conversion only, as is asked of anything bound to
//! JavaScript: what could be *wrong* - which URL, which headers, what an
//! answer means - is in [`crate::rest`] and in the engine, both of which a
//! native test exercises. What lives here is the call itself.

use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;

use retp_proto::connection::Handler;
use retp_proto::credentials::Credentials;
use retp_proto::negotiate::Offer;
use retp_proto::probe::Handshake;
use wasm_bindgen::prelude::Closure;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;

use crate::rest::{Http, HttpRequest, HttpResponse, verb};
use crate::{AsyncClient, ClientError, Clock, Sink, TransportError, browser_fetch_hint};

/// The REST transport in a front end: every request is a `fetch`. A front end reaches
/// OSDU's Reservoir DDMS this way - the only door some OSDU services leave open to it.
///
/// `fetch` starts the moment it is called, so several requests of one ETP
/// message run at the same time without a thread: [`Http::send_all`] starts a
/// window of them, then awaits them in order.
#[derive(Default)]
pub struct FetchHttp {
    /// What `credentials` the requests carry: `"omit"` unless told otherwise -
    /// a token in the `Authorization` header needs no cookie, and asking
    /// for cookies would make the server's CORS answer stricter.
    credentials: Option<String>,
}

impl FetchHttp {
    /// A transport sending no cookie.
    pub fn new() -> Self {
        Self::default()
    }

    /// Send the front end's cookies with each request (`credentials: "include"`) -
    /// for a server that authorizes by session rather than by token. The
    /// server must then name the front end's origin in
    /// `access-control-allow-origin`: `*` is refused by the browser with
    /// credentials.
    pub fn with_cookies(mut self) -> Self {
        self.credentials = Some("include".into());
        self
    }
}

impl Http for FetchHttp {
    fn send(&mut self, request: HttpRequest) -> impl Future<Output = Result<HttpResponse, TransportError>> {
        let credentials = self.credentials.clone();
        async move {
            match started(&request, credentials.as_deref()) {
                Ok(pending) => answer(&request.url, pending).await,
                Err(why) => Err(why),
            }
        }
    }

    /// `at_once` requests in flight, and every answer in the order asked.
    fn send_all(
        &mut self,
        requests: Vec<HttpRequest>,
        at_once: usize,
    ) -> impl Future<Output = Vec<Result<HttpResponse, TransportError>>> {
        let credentials = self.credentials.clone();
        async move {
            let mut answers = Vec::with_capacity(requests.len());
            for window in requests.chunks(at_once.max(1)) {
                // Calling `fetch` starts it: the whole window is in flight
                // before the first answer is awaited.
                let pending: Vec<_> = window
                    .iter()
                    .map(|request| started(request, credentials.as_deref()))
                    .collect();
                for (request, pending) in window.iter().zip(pending) {
                    answers.push(match pending {
                        Ok(pending) => answer(&request.url, pending).await,
                        Err(why) => Err(why),
                    });
                }
            }
            answers
        }
    }
}

/// `fetch(request)`, started.
fn started(request: &HttpRequest, credentials: Option<&str>) -> Result<js_sys::Promise, TransportError> {
    let failed = |why: JsValue| fetch_failed(&request.url, &why);
    let headers = web_sys::Headers::new().map_err(failed)?;
    for (name, value) in &request.headers {
        headers.append(name, value).map_err(failed)?;
    }
    let options = web_sys::RequestInit::new();
    options.set_method(verb(request.method));
    options.set_headers(&headers);
    if let Some(credentials) = credentials {
        options.set_credentials(match credentials {
            "include" => web_sys::RequestCredentials::Include,
            _ => web_sys::RequestCredentials::SameOrigin,
        });
    }
    if let Some(body) = &request.body {
        options.set_body(&js_sys::Uint8Array::from(body.as_slice()).into());
    }
    let request = web_sys::Request::new_with_str_and_init(&request.url, &options).map_err(failed)?;
    fetcher(&request).map_err(failed)
}

/// `fetch` wherever the code runs: a window, or a worker - the front end reads in
/// a worker, and neither global has the other's.
fn fetcher(request: &web_sys::Request) -> Result<js_sys::Promise, JsValue> {
    let global = js_sys::global();
    if let Some(window) = global.dyn_ref::<web_sys::Window>() {
        return Ok(window.fetch_with_request(request));
    }
    if let Some(worker) = global.dyn_ref::<web_sys::WorkerGlobalScope>() {
        return Ok(worker.fetch_with_request(request));
    }
    Err(JsValue::from_str("no fetch here: neither a window nor a worker"))
}

/// What a started request answered: its status, the challenge a `401`
/// carries, and its bytes. An answer of any status is an answer; only a
/// request that never arrived is a [`TransportError`].
async fn answer(url: &str, pending: js_sys::Promise) -> Result<HttpResponse, TransportError> {
    let failed = |why: JsValue| fetch_failed(url, &why);
    let response: web_sys::Response = JsFuture::from(pending).await.map_err(failed)?.unchecked_into();
    let challenge = response.headers().get("www-authenticate").ok().flatten();
    let body = JsFuture::from(response.array_buffer().map_err(failed)?)
        .await
        .map_err(failed)?;
    Ok(HttpResponse {
        status: response.status(),
        challenge,
        body: js_sys::Uint8Array::new(&body).to_vec(),
    })
}

/// What a JavaScript error says, for a message a person reads.
fn said(why: &JsValue) -> String {
    why.as_string()
        .or_else(|| why.dyn_ref::<js_sys::Error>().map(|error| error.message().into()))
        .unwrap_or_else(|| format!("{why:?}"))
}

/// A `fetch` that failed, as a [`TransportError`] a person reads: the
/// browser's own message, plus [`browser_fetch_hint`]'s guess when that
/// message is the generic one a browser gives for any network-level failure.
fn fetch_failed(url: &str, why: &JsValue) -> TransportError {
    let message = said(why);
    match browser_fetch_hint(&message) {
        Some(hint) => TransportError::new(format!("{url}: {message} ({hint})")),
        None => TransportError::new(format!("{url}: {message}")),
    }
}

/// The front end's clock: `Date.now()`, in microseconds - the unit ETP's
/// `currentDateTime` is in. `SystemClock` cannot be used here: reading the
/// system time panics in a browser.
#[derive(Debug, Clone, Copy, Default)]
pub struct BrowserClock;

impl Clock for BrowserClock {
    fn now(&self) -> i64 {
        // Milliseconds since the epoch, as a f64 exact up to 2^53 ms.
        (js_sys::Date::now() * 1000.0) as i64
    }
}

/// An ETP session over the browser's own WebSocket, for the servers that
/// take a front end (some RDDMS servers do, some OSDU services do
/// not). The credentials and ETP's attributes travel in the URL's query
/// string, which [`Handshake`] with [`Profile::Browser`] already builds
/// (§4.3.2) - a front end cannot set a header.
///
/// [`Profile::Browser`]: retp_proto::probe::Profile::Browser
pub struct WebSocketSession {
    client: Rc<AsyncClient<BrowserClock>>,
    socket: web_sys::WebSocket,
    /// The event listeners, kept alive as long as the session: dropping a
    /// `Closure` unregisters it.
    _listening: Vec<Closure<dyn FnMut(JsValue)>>,
}

impl WebSocketSession {
    /// Open the socket and start the session on it. The session is not open
    /// yet when this returns: await [`AsyncClient::opened`] on
    /// [`WebSocketSession::client`].
    pub fn open(
        handshake: &Handshake,
        offer: Offer,
        handlers: Vec<Box<dyn Handler>>,
        credentials: Option<Credentials>,
    ) -> Result<Self, ClientError> {
        let failed = |why: JsValue| {
            ClientError::Transport(TransportError::new(format!("{}: {}", handshake.url, said(&why))))
        };
        let protocols = js_sys::Array::new();
        for protocol in &handshake.subprotocols {
            protocols.push(&JsValue::from_str(protocol));
        }
        let socket = web_sys::WebSocket::new_with_str_sequence(&handshake.url, &protocols).map_err(failed)?;
        // ETP is binary from end to end (§3.5): ask for the frames as bytes.
        socket.set_binary_type(web_sys::BinaryType::Arraybuffer);
        let waiting = Rc::new(RefCell::new(Vec::new()));
        let sink = WebSocketSink {
            socket: socket.clone(),
            waiting: Rc::clone(&waiting),
        };
        let client = Rc::new(AsyncClient::start(
            Box::new(sink),
            BrowserClock,
            offer,
            handlers,
            credentials,
        )?);

        let mut listening = Vec::new();
        // Open: what the session said before the socket was there goes now.
        let opened = {
            let socket = socket.clone();
            listen(&socket.clone(), "open", move |_| {
                for message in waiting.borrow_mut().drain(..) {
                    let _ = socket.send_with_u8_array(&message);
                }
            })
        };
        let arrived = {
            let client = Rc::clone(&client);
            listen(&socket, "message", move |event: JsValue| {
                let data = event.unchecked_into::<web_sys::MessageEvent>().data();
                match data.dyn_ref::<js_sys::ArrayBuffer>() {
                    Some(buffer) => client.received(&js_sys::Uint8Array::new(buffer).to_vec()),
                    // ETP carries no text frame: one means the other end is
                    // not speaking ETP, and nothing that follows is readable.
                    None => client.lost(TransportError::new(
                        "a frame that is not binary: this is not an ETP session",
                    )),
                }
            })
        };
        let closed = {
            let client = Rc::clone(&client);
            listen(&socket, "close", move |event: JsValue| {
                let event = event.unchecked_into::<web_sys::CloseEvent>();
                client.lost(TransportError::new(format!(
                    "the connection closed: {} {}",
                    event.code(),
                    event.reason()
                )));
            })
        };
        let failed_socket = {
            let client = Rc::clone(&client);
            listen(&socket, "error", move |_| {
                // A browser tells a front end nothing of why: no status, no body.
                client.lost(TransportError::new(
                    "the connection failed; a page is not told why",
                ));
            })
        };
        listening.extend([opened, arrived, closed, failed_socket]);
        Ok(Self {
            client,
            socket,
            _listening: listening,
        })
    }

    /// The session itself: every operation of the crate takes it.
    pub fn client(&self) -> &Rc<AsyncClient<BrowserClock>> {
        &self.client
    }

    /// Say goodbye, then close the socket.
    pub fn close(&self, reason: &str) {
        self.client.close(reason);
        let _ = self.socket.close();
    }
}

/// One listener, registered and given back so that it stays alive.
fn listen(
    socket: &web_sys::WebSocket,
    event: &str,
    what: impl FnMut(JsValue) + 'static,
) -> Closure<dyn FnMut(JsValue)> {
    let listener = Closure::<dyn FnMut(JsValue)>::new(what);
    let _ = socket.add_event_listener_with_callback(event, listener.as_ref().unchecked_ref());
    listener
}

/// What the session writes on the socket. A message written before the
/// socket is open waits for it: the session speaks first (`RequestSession`),
/// and a browser refuses a send before the handshake is done.
struct WebSocketSink {
    socket: web_sys::WebSocket,
    waiting: Rc<RefCell<Vec<Vec<u8>>>>,
}

impl Sink for WebSocketSink {
    fn send(&mut self, message: &[u8]) -> Result<(), TransportError> {
        if self.socket.ready_state() == web_sys::WebSocket::CONNECTING {
            self.waiting.borrow_mut().push(message.to_vec());
            return Ok(());
        }
        self.socket
            .send_with_u8_array(message)
            .map_err(|why| TransportError::new(format!("sending {} bytes: {}", message.len(), said(&why))))
    }

    fn close(&mut self) {
        let _ = self.socket.close();
    }
}
