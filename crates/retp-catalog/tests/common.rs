//! What the tests share: a sandbox folder, a secret source in memory, a
//! capture of the `log` lines, a scan of a folder for a text, and a token
//! endpoint on a local socket.

#![allow(dead_code)]

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, Once};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use retp_catalog::{Catalog, Location, RefStatus, Secret, SecretError, SecretRef, SecretSource};

/// The stand-in for a secret. It is made up, it is valid nowhere, and it is the
/// text every leak test looks for.
pub const CANARY: &str = "CANARY-7f3a-must-appear-nowhere";

/// A folder of the test's own: `config.toml`, `servers/` (the catalogue),
/// `secrets/` (outside the catalogue, where the files secrets point at live).
pub struct Sandbox {
    pub root: PathBuf,
}

impl Sandbox {
    pub fn new(label: &str) -> Self {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let root = std::env::temp_dir().join(format!("retp-catalog-test-{}-{label}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("secrets")).expect("the sandbox is created");
        Sandbox { root }
    }

    pub fn location(&self) -> Location {
        Location::new(self.root.join("config.toml"), None)
    }

    pub fn servers(&self) -> PathBuf {
        self.root.join("servers")
    }

    pub fn secrets(&self) -> PathBuf {
        self.root.join("secrets")
    }

    /// A catalogue initialised in the sandbox.
    pub fn catalog(&self) -> Catalog {
        Catalog::init(&self.location(), &self.servers(), false).expect("the catalogue is initialised")
    }

    /// A secret file (`0600` on Unix) in the sandbox's `secrets/`; the path.
    pub fn secret_file(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.secrets().join(name);
        std::fs::write(&path, contents).expect("the secret file is written");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("mode 0600");
        }
        path
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// A secret source that reads a map, keyed by the command-line form of the reference.
pub struct MemorySecrets(pub HashMap<String, String>);

impl MemorySecrets {
    pub fn of(pairs: &[(&str, &str)]) -> Self {
        MemorySecrets(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }
}

impl SecretSource for MemorySecrets {
    fn read(&self, reference: &SecretRef) -> Result<Secret, SecretError> {
        self.0
            .get(&reference.to_text())
            .map(|value| Secret::new(value.clone()))
            .ok_or_else(|| SecretError::Unavailable {
                reason: format!("{} is not in the memory source", reference.to_text()),
            })
    }

    fn inspect(&self, reference: &SecretRef) -> RefStatus {
        RefStatus {
            resolves: self.0.contains_key(&reference.to_text()),
            detail: "memory".into(),
        }
    }
}

// ---- the log ----------------------------------------------------------------

static LINES: Mutex<Vec<String>> = Mutex::new(Vec::new());

struct Capture;

/// Every field of an event, the message among them: a value could leak through
/// a structured field as well as through the text.
struct Fields(String);

impl tracing::field::Visit for Fields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.push_str(&format!(" {}={value:?}", field.name()));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.push_str(&format!(" {}={value}", field.name()));
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut fields = Fields(String::new());
        event.record(&mut fields);
        if let Ok(mut lines) = LINES.lock() {
            lines.push(format!("{}:{}", event.metadata().target(), fields.0));
        }
    }
}

/// Capture every event, at every level, with all its fields: what `--verbose`
/// would show, and more.
pub fn capture_log() {
    use tracing_subscriber::layer::SubscriberExt;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Capture));
    });
}

/// Everything logged so far in this process.
pub fn logged() -> String {
    LINES.lock().map(|lines| lines.join("\n")).unwrap_or_default()
}

// ---- scanning ----------------------------------------------------------------

/// The files under `dir` whose bytes contain `needle`. Independent of the code
/// under test: it reads bytes and compares them.
pub fn files_containing(dir: &Path, needle: &str) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(folder) = pending.pop() {
        let Ok(read) = std::fs::read_dir(&folder) else {
            continue;
        };
        for item in read.flatten() {
            let path = item.path();
            if path.is_dir() {
                pending.push(path);
            } else if let Ok(bytes) = std::fs::read(&path)
                && bytes
                    .windows(needle.len())
                    .any(|window| window == needle.as_bytes())
            {
                found.push(path);
            }
        }
    }
    found
}

// ---- a token endpoint ---------------------------------------------------------

/// What the fake token endpoint answers (always HTTP 401, `invalid_client`).
#[derive(Clone, Copy)]
pub enum Reply {
    /// A plain refusal.
    Plain,
    /// A refusal whose explanation repeats the request body: the worst case
    /// the redaction is for (a server that echoes what it was sent).
    EchoBody,
}

pub struct FakeToken {
    pub url: String,
    handle: Option<JoinHandle<()>>,
}

impl FakeToken {
    /// Answer `connections` requests on 127.0.0.1, then stop.
    pub fn start(reply: Reply, connections: usize) -> FakeToken {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a local port");
        let port = listener.local_addr().expect("an address").port();
        let handle = thread::spawn(move || {
            for stream in listener.incoming().take(connections) {
                let Ok(mut stream) = stream else { continue };
                let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                let body = read_request(&mut stream);
                let description = match reply {
                    Reply::Plain => "refused".to_string(),
                    Reply::EchoBody => body,
                };
                let payload =
                    serde_json::json!({ "error": "invalid_client", "error_description": description })
                        .to_string();
                let response = format!(
                    "HTTP/1.1 401 Unauthorized\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    payload.len(),
                    payload
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        FakeToken {
            url: format!("http://127.0.0.1:{port}/token"),
            handle: Some(handle),
        }
    }

    /// A URL nothing listens on: the connection is refused.
    pub fn closed() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a local port");
        let port = listener.local_addr().expect("an address").port();
        drop(listener);
        format!("http://127.0.0.1:{port}/token")
    }
}

impl Drop for FakeToken {
    fn drop(&mut self) {
        // The thread ends after its connections; a test that made fewer leaves
        // it waiting, which the end of the process settles.
        let _ = self.handle.take();
    }
}

/// Read one HTTP request and return its body.
fn read_request(stream: &mut std::net::TcpStream) -> String {
    let mut data = Vec::new();
    let mut buffer = [0u8; 2048];
    let mut header_end = None;
    while header_end.is_none() {
        match stream.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(n) => data.extend_from_slice(&buffer[..n]),
        }
        header_end = data.windows(4).position(|w| w == b"\r\n\r\n");
    }
    let Some(end) = header_end else {
        return String::new();
    };
    let head = String::from_utf8_lossy(&data[..end]).to_ascii_lowercase();
    let length: usize = head
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0);
    let mut body = data[end + 4..].to_vec();
    while body.len() < length {
        match stream.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(n) => body.extend_from_slice(&buffer[..n]),
        }
    }
    String::from_utf8_lossy(&body).to_string()
}
