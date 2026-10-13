//! The native REST transport against a server written here: what
//! `Http::send_all` promises - every answer in the order asked, and no more
//! than `at_once` requests in flight - is something only a server can show,
//! a fake having nothing to run at the same time.
//!
//! The server counts how many requests it holds at once, so the check is on
//! what actually happened on the wire, not on how long it took.

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use retp_client::native::{CertificateCheck, UreqHttp};
use retp_client::rest::{Http, HttpRequest, Method};

/// How long the server takes to answer, so that requests overlap visibly.
const DELAY: Duration = Duration::from_millis(120);

/// A server answering `GET /<n>` with `<n>`, `DELAY` later, and remembering
/// the most requests it ever held at the same time.
struct Server {
    port: u16,
    at_most: Arc<AtomicUsize>,
}

impl Server {
    fn start() -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
        let port = listener.local_addr().expect("an address").port();
        let holding = Arc::new(AtomicUsize::new(0));
        let at_most = Arc::new(AtomicUsize::new(0));
        let server = Server {
            port,
            at_most: Arc::clone(&at_most),
        };
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let (holding, at_most) = (Arc::clone(&holding), Arc::clone(&at_most));
                std::thread::spawn(move || answer(stream, &holding, &at_most));
            }
        });
        server
    }

    fn url(&self, path: usize) -> String {
        format!("http://127.0.0.1:{}/{path}", self.port)
    }

    fn at_most(&self) -> usize {
        self.at_most.load(Ordering::Relaxed)
    }

    fn forget(&self) {
        self.at_most.store(0, Ordering::Relaxed);
    }
}

/// One connection: every request on it, answered after `DELAY`, the body
/// being the path asked for. Kept open, as a client may send more on it.
fn answer(stream: TcpStream, holding: &AtomicUsize, at_most: &AtomicUsize) {
    let mut reader = BufReader::new(stream.try_clone().expect("the same socket"));
    let mut stream = stream;
    loop {
        let mut request = String::new();
        if reader.read_line(&mut request).unwrap_or(0) == 0 {
            return;
        }
        let path = request
            .split_whitespace()
            .nth(1)
            .unwrap_or("/")
            .trim_start_matches('/')
            .to_string();
        loop {
            let mut header = String::new();
            if reader.read_line(&mut header).unwrap_or(0) == 0 || header.trim().is_empty() {
                break;
            }
        }
        let now = holding.fetch_add(1, Ordering::SeqCst) + 1;
        at_most.fetch_max(now, Ordering::SeqCst);
        std::thread::sleep(DELAY);
        holding.fetch_sub(1, Ordering::SeqCst);
        let answer = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\n\r\n{path}",
            path.len()
        );
        if stream.write_all(answer.as_bytes()).is_err() {
            return;
        }
        let _ = stream.flush();
    }
}

fn get(server: &Server, path: usize) -> HttpRequest {
    HttpRequest {
        method: Method::Get,
        url: server.url(path),
        headers: Vec::new(),
        body: None,
    }
}

/// A future of this transport is over at its first poll.
fn now<F: std::future::Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    {
        std::task::Poll::Ready(output) => output,
        std::task::Poll::Pending => panic!("the native transport answers at its first poll"),
    }
}

/// Eight requests with four allowed at once: the server sees several at a
/// time, never more than four, the answers come back in the order asked, and
/// it all takes less than doing them one after the other - which `at_once`
/// of 1 does, and the server sees one at a time.
#[test]
fn the_transport_runs_at_most_at_once_requests_and_answers_in_order() {
    let server = Server::start();
    let mut http = UreqHttp::new(CertificateCheck::Verify);
    let asked: Vec<HttpRequest> = (0..8).map(|path| get(&server, path)).collect();
    let bodies = |answers: Vec<Result<retp_client::rest::HttpResponse, _>>| -> Vec<String> {
        answers
            .into_iter()
            .map(|answer| String::from_utf8(answer.expect("an answer").body).expect("text"))
            .collect::<Vec<_>>()
    };
    let expected: Vec<String> = (0..8).map(|path| path.to_string()).collect();

    let answers = now(http.send_all(asked.clone(), 4));
    assert_eq!(bodies(answers), expected, "in the order asked");
    let at_once = server.at_most();
    assert!(
        (2..=4).contains(&at_once),
        "several at a time, four at most: {at_once}"
    );

    server.forget();
    let answers = now(http.send_all(asked, 1));
    assert_eq!(bodies(answers), expected);
    assert_eq!(server.at_most(), 1, "one at a time");

    // **What was measured here was the machine, not the contract.** An
    // assertion `together * 2 < one_by_one` failed during a parallel
    // compilation - 852 ms against 966 ms - and then passed three times in a
    // row on an idle machine. It is a rule this project has already written
    // down elsewhere: *a stopwatch in a test measures the machine*.
    //
    // And it proved nothing more: what the parallelism **is** can be read in
    // `at_most` - four in flight on one side, one on the other - which the two
    // assertions above hold exactly, regardless of load. A transport that
    // answered out of order or opened five requests turns them red; a loaded
    // runner does not.
}
