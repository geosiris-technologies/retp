//! Where what arrives unasked goes.
//!
//! A [`Destination`] is handed each arrival - a notification, a session event - on the
//! dispatcher thread of a live session, never on the session task, so a slow destination
//! never delays an answer nor the keepalive. Four are provided: [`JsonLines`] (a file, one
//! JSON object per line), [`Console`] (one human line), [`Callback`] (a closure) and
//! [`Queue`] (a bounded channel the application reads).
//!
//! The JSON of a message is the Avro JSON encoding the generated types already give
//! (`Message::to_json`); nothing is re-encoded here.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Mutex, MutexGuard};

use retp_proto::connection::Event;
use retp_proto::wire::{Body, Delivery};
use serde_json::{Value, json};

use crate::clock::{Clock, SystemClock};
use crate::notifications::{Notification, uuid_text};

/// Where what arrives unasked goes. Called on the dispatcher thread, never on the session
/// task: a slow destination never delays an answer nor the keepalive.
pub trait Destination: Send + Sync {
    /// One arrival, in the order the session received it.
    fn deliver(&self, event: &Event) -> Result<(), String>;
    /// `count` arrivals were dropped (the queue was full) before the next one delivered.
    fn dropped(&self, count: u64) {
        let _ = count;
    }
}

// -- shared helpers ----------------------------------------------------------

/// Lock, recovering from a poisoned mutex: a panic elsewhere must not silence a destination.
fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Microseconds since the epoch as RFC 3339 UTC with microseconds
/// (`2026-10-15T08:30:00.123456Z`). The date arithmetic is `rest::iso`'s; it prints
/// milliseconds, so the whole second goes through it and the six fraction digits are
/// appended here.
fn stamp(micros: i64) -> String {
    let whole = micros - micros.rem_euclid(1_000_000);
    let seconds = crate::rest::iso(whole);
    let seconds = seconds.strip_suffix(".000Z").unwrap_or(&seconds);
    format!("{seconds}.{:06}Z", micros.rem_euclid(1_000_000))
}

/// The moment of the call, as [`stamp`] writes it.
fn received_now() -> String {
    stamp(SystemClock.now())
}

/// The short name of a delivered message (after the last dot of its Avro full name), or
/// `None` for a private protocol's message, which has no schema here.
fn message_name(delivery: &Delivery) -> Option<String> {
    let Body::Message(message) = &delivery.body else {
        return None;
    };
    let (protocol, message_type) = message.header();
    let info = retp_types::dispatch::info(protocol, message_type)?;
    Some(info.name.rsplit('.').next().unwrap_or(info.name).to_string())
}

/// One event as the JSON object a [`JsonLines`] destination writes, received now: the one
/// way an arrival is put in JSON, for a front end (the Python bridge) as for the file.
pub fn as_json(event: &Event) -> Value {
    record(event, &received_now())
}

/// One event as the line a [`Console`] destination prints, without its newline: the one way
/// an arrival is said in words, for a front end (the command line) as for the console.
pub fn as_line(event: &Event) -> String {
    line_of(event)
}

/// The record of one event: the JSON object of a JSON Lines destination.
fn record(event: &Event, received: &str) -> Value {
    match event {
        Event::Delivered(delivery) => {
            let subscription = Notification::of(event.clone())
                .subscription()
                .map(|uuid| uuid_text(&uuid));
            match &delivery.body {
                Body::Message(message) => json!({
                    "received": received,
                    "protocol": delivery.header.protocol,
                    "message": message_name(delivery),
                    "subscription": subscription,
                    "body": message.to_json(),
                }),
                Body::Private(bytes) => json!({
                    "received": received,
                    "protocol": delivery.header.protocol,
                    "message": Value::Null,
                    "subscription": Value::Null,
                    "body": Value::Null,
                    "private_bytes": bytes.len(),
                }),
            }
        }
        Event::Opened => json!({"received": received, "event": "opened"}),
        Event::AuthorizationNeeded { challenges } => {
            json!({"received": received, "event": "authorization_needed", "challenges": challenges})
        }
        Event::AuthorizationExpiring => json!({"received": received, "event": "authorization_expiring"}),
        Event::TimedOut { request, incident } => json!({
            "received": received,
            "event": "timed_out",
            "request": request,
            "incident": incident.to_string(),
        }),
        Event::Refused(incident) => {
            json!({"received": received, "event": "refused", "incident": incident.to_string()})
        }
        Event::CapabilityProblems(problems) => json!({
            "received": received,
            "event": "capability_problems",
            "problems": problems.iter().map(|p| format!("{p:?}")).collect::<Vec<_>>(),
        }),
        Event::AnswerRefused { request, incident } => json!({
            "received": received,
            "event": "answer_refused",
            "request": request,
            "incident": incident.to_string(),
        }),
        Event::Closed { reason } => json!({"received": received, "event": "closed", "reason": reason}),
        Event::Fault(incident) => {
            json!({"received": received, "event": "fault", "incident": incident.to_string()})
        }
    }
}

/// A session event in words, as the CLI's `said_notices` words the four it says.
fn notice(event: &Event) -> String {
    match event {
        Event::Opened => "the session is open".to_string(),
        Event::AuthorizationNeeded { challenges } => {
            format!("the server wants credentials: {}", challenges.join("; "))
        }
        Event::AuthorizationExpiring => "the server says the authorization runs out soon".to_string(),
        Event::TimedOut { request, incident } => format!("request #{request} timed out: {incident}"),
        Event::Refused(incident) => format!("the session was refused: {incident}"),
        Event::CapabilityProblems(problems) => {
            format!("the server sent {} capability problem(s)", problems.len())
        }
        Event::AnswerRefused { request, incident } => {
            format!("the answer to request #{request} was refused: {incident}")
        }
        Event::Closed { reason } => format!("the server closed the session: {reason}"),
        Event::Fault(incident) => format!("the server sent what could not be taken: {incident}"),
        Event::Delivered(_) => "a message was delivered".to_string(),
    }
}

/// The human line of one event, without its newline. A notification is tab separated as the
/// CLI's `notifications` command prints it (`changed`, `deleted`, `revoked`, `status`,
/// `ended`, `kept`, `other`); a session event is `notice: ...`.
fn line_of(event: &Event) -> String {
    use crate::rest::iso;
    match Notification::of(event.clone()) {
        Notification::Changed { change, subscription } => format!(
            "changed\t{}\t{:?}\t{}\t{}",
            uuid_text(&subscription),
            change.change_kind,
            iso(change.change_time),
            change.data_object.resource.uri
        ),
        Notification::Deleted {
            uri,
            change_time,
            subscription,
        } => format!(
            "deleted\t{}\t{}\t{uri}",
            uuid_text(&subscription),
            iso(change_time)
        ),
        Notification::AccessRevoked {
            uri,
            change_time,
            subscription,
        } => format!(
            "revoked\t{}\t{}\t{uri}",
            uuid_text(&subscription),
            iso(change_time)
        ),
        Notification::ActiveStatusChanged {
            resource,
            status,
            change_time,
            subscription,
        } => format!(
            "status\t{}\t{status:?}\t{}\t{}",
            uuid_text(&subscription),
            iso(change_time),
            resource.uri
        ),
        Notification::Ended { reason, subscription } => {
            format!("ended\t{}\t{reason}", uuid_text(&subscription))
        }
        Notification::Unsolicited(kept) => {
            format!("kept\t{} subscription(s) the store still holds", kept.len())
        }
        Notification::Other(delivery) => match message_name(&delivery) {
            Some(name) => format!("other\t{name}"),
            None => format!("other\tprivate protocol {}", delivery.header.protocol),
        },
        Notification::Session(event) => format!("notice: {}", notice(&event)),
    }
}

// -- JSON Lines --------------------------------------------------------------

/// A file of JSON Lines: one object per arrival, written in a single call and flushed, so a
/// reader never sees a partial line.
///
/// An arrival is `{"received", "protocol", "message", "subscription", "body"}`, a session
/// event `{"received", "event", ...}`, a drop report `{"received", "dropped"}`. `received`
/// is the moment of the call, RFC 3339 UTC with microseconds.
pub struct JsonLines {
    file: Mutex<File>,
}

impl JsonLines {
    /// Create the file, truncating what was there.
    pub fn create(path: impl AsRef<Path>) -> std::io::Result<JsonLines> {
        Ok(JsonLines {
            file: Mutex::new(File::create(path)?),
        })
    }

    /// Open the file for appending, creating it when absent; what was there is kept.
    pub fn append(path: impl AsRef<Path>) -> std::io::Result<JsonLines> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(JsonLines {
            file: Mutex::new(file),
        })
    }

    /// One line, one `write_all`, then a flush.
    fn write(&self, value: &Value) -> Result<(), String> {
        let mut line = serde_json::to_string(value).map_err(|e| format!("cannot encode the record: {e}"))?;
        line.push('\n');
        let mut file = locked(&self.file);
        file.write_all(line.as_bytes())
            .and_then(|()| file.flush())
            .map_err(|e| format!("cannot write the JSON Lines file: {e}"))
    }
}

impl Destination for JsonLines {
    fn deliver(&self, event: &Event) -> Result<(), String> {
        self.write(&record(event, &received_now()))
    }

    fn dropped(&self, count: u64) {
        // `dropped` cannot fail by its signature; a write error here shows at the next
        // `deliver`, which fails on the same file.
        let _ = self.write(&json!({"received": received_now(), "dropped": count}));
    }
}

// -- Console -----------------------------------------------------------------

/// One human line per arrival on a writer (the standard output, usually), flushed after
/// each line. A notification reads as the CLI's `notifications` command prints it
/// (`changed <subscription> <kind> <time> <uri>`, tab separated); a session event is a
/// `notice:` sentence.
pub struct Console {
    out: Mutex<Box<dyn Write + Send>>,
}

impl Console {
    /// The standard output.
    pub fn stdout() -> Console {
        Console::new(Box::new(std::io::stdout()))
    }

    /// Any writer.
    pub fn new(out: Box<dyn Write + Send>) -> Console {
        Console { out: Mutex::new(out) }
    }

    /// One line, one `write_all`, then a flush.
    fn say(&self, mut line: String) -> Result<(), String> {
        line.push('\n');
        let mut out = locked(&self.out);
        out.write_all(line.as_bytes())
            .and_then(|()| out.flush())
            .map_err(|e| format!("cannot write the console line: {e}"))
    }
}

impl Destination for Console {
    fn deliver(&self, event: &Event) -> Result<(), String> {
        self.say(line_of(event))
    }

    fn dropped(&self, count: u64) {
        let _ = self.say(format!("notice: {count} arrival(s) dropped, the queue was full"));
    }
}

// -- Callback ----------------------------------------------------------------

/// The closure a [`Callback`] holds.
type OnArrival = Box<dyn Fn(&Event) -> Result<(), String> + Send + Sync + 'static>;

/// A closure called with each arrival. Its `Err` removes the destination from the session
/// (and is reported once), so return `Ok` for anything that is not fatal.
pub struct Callback {
    call: OnArrival,
}

impl Callback {
    /// Call `f` for each arrival, on the dispatcher thread.
    pub fn new(f: impl Fn(&Event) -> Result<(), String> + Send + Sync + 'static) -> Callback {
        Callback { call: Box::new(f) }
    }
}

impl Destination for Callback {
    fn deliver(&self, event: &Event) -> Result<(), String> {
        (self.call)(event)
    }
}

// -- Queue -------------------------------------------------------------------

/// A bounded queue the application reads from another thread. When it is full the newest
/// arrival is dropped and counted ([`Queue::lost`]); the oldest stay, in order.
pub struct Queue {
    sender: SyncSender<Event>,
    lost: AtomicU64,
}

impl Queue {
    /// A queue of `capacity` events, and the receiver to read them from. A `capacity` of 0
    /// is a rendezvous channel, which drops everything unless a reader is already waiting;
    /// ask for at least 1.
    pub fn bounded(capacity: usize) -> (Queue, Receiver<Event>) {
        let (sender, receiver) = sync_channel(capacity);
        (
            Queue {
                sender,
                lost: AtomicU64::new(0),
            },
            receiver,
        )
    }

    /// How many arrivals were dropped so far: those this queue found full, and those the
    /// session told it about (`Destination::dropped`).
    pub fn lost(&self) -> u64 {
        self.lost.load(Ordering::Relaxed)
    }
}

impl Destination for Queue {
    fn deliver(&self, event: &Event) -> Result<(), String> {
        match self.sender.try_send(event.clone()) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.lost.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(TrySendError::Disconnected(_)) => Err("the queue's receiver was dropped".to_string()),
        }
    }

    fn dropped(&self, count: u64) {
        self.lost.fetch_add(count, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    use retp_proto::wire::{EtpError, Place};
    use retp_types::datatypes::object::{DataObject, ObjectChange, ObjectChangeKind, Resource};
    use retp_types::datatypes::{MessageHeader, Uuid};
    use retp_types::dispatch::Message;
    use retp_types::protocol::store_notification::{ObjectChanged, ObjectDeleted};

    use super::*;

    const SUBSCRIPTION: Uuid = Uuid([
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
    ]);
    const SUBSCRIPTION_TEXT: &str = "01234567-89ab-cdef-0123-456789abcdef";
    const URI: &str = "eml:///dataspace('demo')/resqml20.TriangulatedSetRepresentation(11111111-2222-3333-4444-555555555555)";

    fn delivered(message: Message) -> Event {
        Event::Delivered(Delivery {
            header: MessageHeader {
                protocol: 5,
                message_type: 2,
                correlation_id: 0,
                message_id: 2,
                message_flags: 0,
            },
            extension: None,
            body: Body::Message(message),
            place: Place::Opens,
            last: true,
        })
    }

    fn changed() -> Event {
        let resource = Resource {
            uri: URI.to_string(),
            ..Default::default()
        };
        let data_object = DataObject {
            resource,
            ..Default::default()
        };
        delivered(Message::ObjectChanged(ObjectChanged {
            change: ObjectChange {
                change_kind: ObjectChangeKind::Update,
                // 2023-11-14T22:13:20.123456Z
                change_time: 1_700_000_000_123_456,
                data_object,
            },
            request_uuid: SUBSCRIPTION,
        }))
    }

    fn deleted() -> Event {
        delivered(Message::ObjectDeleted(ObjectDeleted {
            uri: URI.to_string(),
            change_time: 1_700_000_000_000_000,
            request_uuid: SUBSCRIPTION,
        }))
    }

    fn closed() -> Event {
        Event::Closed {
            reason: "bye".to_string(),
        }
    }

    /// A file path of this test run, removed on drop.
    struct Scratch(std::path::PathBuf);

    impl Scratch {
        fn new(name: &str) -> Scratch {
            static COUNTER: AtomicUsize = AtomicUsize::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            Scratch(std::env::temp_dir().join(format!("retp-destination-{}-{n}-{name}", std::process::id())))
        }

        fn lines(&self) -> Vec<String> {
            std::fs::read_to_string(&self.0)
                .unwrap_or_default()
                .lines()
                .map(str::to_string)
                .collect()
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    /// A writer whose bytes the test can read back.
    #[derive(Clone, Default)]
    struct Shared(Arc<Mutex<Vec<u8>>>);

    impl Write for Shared {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            locked(&self.0).extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn parsed(line: &str) -> Value {
        serde_json::from_str(line).expect("every line is JSON")
    }

    #[test]
    fn stamp_has_microseconds_and_the_right_date() {
        assert_eq!(stamp(1_700_000_000_123_456), "2023-11-14T22:13:20.123456Z");
        assert_eq!(stamp(1_700_000_000_000_007), "2023-11-14T22:13:20.000007Z");
        let now = received_now();
        assert_eq!(now.len(), 27, "{now}");
        assert!(now.ends_with('Z'));
    }

    #[test]
    fn json_lines_writes_one_json_line_per_event() {
        let file = Scratch::new("lines.jsonl");
        let lines = JsonLines::create(&file.0).unwrap();
        lines.deliver(&changed()).unwrap();
        lines.deliver(&deleted()).unwrap();
        lines.deliver(&closed()).unwrap();
        let written = file.lines();
        assert_eq!(written.len(), 3);

        let first = parsed(&written[0]);
        assert_eq!(first["protocol"], 5);
        assert_eq!(first["message"], "ObjectChanged");
        assert_eq!(first["subscription"], SUBSCRIPTION_TEXT);
        assert!(first["body"].is_object(), "{first}");
        let received = first["received"].as_str().unwrap();
        assert_eq!(received.len(), 27);
        assert!(received.ends_with('Z'));

        let second = parsed(&written[1]);
        assert_eq!(second["message"], "ObjectDeleted");
        assert_eq!(second["subscription"], SUBSCRIPTION_TEXT);
        assert!(second["body"].is_object());

        let third = parsed(&written[2]);
        assert_eq!(third["event"], "closed");
        assert_eq!(third["reason"], "bye");
        assert!(third.get("body").is_none());
    }

    #[test]
    fn a_message_without_a_subscription_says_null() {
        let file = Scratch::new("null.jsonl");
        let lines = JsonLines::create(&file.0).unwrap();
        let ping = delivered(Message::Ping(retp_types::protocol::core::Ping {
            current_date_time: 1,
        }));
        lines.deliver(&ping).unwrap();
        let line = parsed(&file.lines()[0]);
        assert_eq!(line["message"], "Ping");
        assert!(line["subscription"].is_null());
    }

    #[test]
    fn a_private_body_is_named_by_its_size_only() {
        let file = Scratch::new("private.jsonl");
        let lines = JsonLines::create(&file.0).unwrap();
        let event = Event::Delivered(Delivery {
            header: MessageHeader {
                protocol: 4000,
                message_type: 1,
                correlation_id: 0,
                message_id: 2,
                message_flags: 0,
            },
            extension: None,
            body: Body::Private(vec![1, 2, 3]),
            place: Place::Opens,
            last: true,
        });
        lines.deliver(&event).unwrap();
        let line = parsed(&file.lines()[0]);
        assert_eq!(line["protocol"], 4000);
        assert!(line["message"].is_null());
        assert_eq!(line["private_bytes"], 3);
    }

    #[test]
    fn session_events_carry_their_name_in_snake_case() {
        let file = Scratch::new("events.jsonl");
        let lines = JsonLines::create(&file.0).unwrap();
        let incident = EtpError::ALL[0].incident();
        let events = [
            (Event::Opened, "opened"),
            (Event::AuthorizationExpiring, "authorization_expiring"),
            (
                Event::TimedOut {
                    request: 7,
                    incident: incident.clone(),
                },
                "timed_out",
            ),
            (Event::Fault(incident), "fault"),
        ];
        for (event, _) in &events {
            lines.deliver(event).unwrap();
        }
        for (line, (_, name)) in file.lines().iter().zip(&events) {
            assert_eq!(parsed(line)["event"], *name);
        }
        assert_eq!(parsed(&file.lines()[2])["request"], 7);
    }

    #[test]
    fn dropped_is_written_as_a_line() {
        let file = Scratch::new("dropped.jsonl");
        let lines = JsonLines::create(&file.0).unwrap();
        lines.dropped(5);
        let written = file.lines();
        assert_eq!(written.len(), 1);
        let line = parsed(&written[0]);
        assert_eq!(line["dropped"], 5);
        assert_eq!(line["received"].as_str().unwrap().len(), 27);
    }

    #[test]
    fn append_keeps_what_was_there_and_create_truncates() {
        let file = Scratch::new("append.jsonl");
        std::fs::write(&file.0, "{\"before\":1}\n").unwrap();
        JsonLines::append(&file.0).unwrap().deliver(&closed()).unwrap();
        let written = file.lines();
        assert_eq!(written.len(), 2);
        assert_eq!(written[0], "{\"before\":1}");
        assert_eq!(parsed(&written[1])["event"], "closed");

        JsonLines::create(&file.0).unwrap();
        assert!(file.lines().is_empty(), "create truncates");
    }

    #[test]
    fn console_prints_the_changed_object_line() {
        let out = Shared::default();
        let console = Console::new(Box::new(out.clone()));
        console.deliver(&changed()).unwrap();
        console.deliver(&deleted()).unwrap();
        console.deliver(&closed()).unwrap();
        console.dropped(2);
        let text = String::from_utf8(locked(&out.0).clone()).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 4, "{text}");
        assert_eq!(
            lines[0],
            format!("changed\t{SUBSCRIPTION_TEXT}\tUpdate\t2023-11-14T22:13:20.123Z\t{URI}")
        );
        assert_eq!(
            lines[1],
            format!("deleted\t{SUBSCRIPTION_TEXT}\t2023-11-14T22:13:20.000Z\t{URI}")
        );
        assert_eq!(lines[2], "notice: the server closed the session: bye");
        assert!(lines[3].contains("2 arrival(s) dropped"));
    }

    #[test]
    fn callback_gets_every_event_and_its_error_comes_back() {
        let seen = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&seen);
        let callback = Callback::new(move |event| {
            counter.fetch_add(1, Ordering::Relaxed);
            match event {
                Event::Closed { .. } => Err("done".to_string()),
                _ => Ok(()),
            }
        });
        assert_eq!(callback.deliver(&changed()), Ok(()));
        assert_eq!(callback.deliver(&closed()), Err("done".to_string()));
        assert_eq!(seen.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn a_full_queue_drops_the_newest_and_counts_it() {
        let (queue, receiver) = Queue::bounded(2);
        queue.deliver(&changed()).unwrap();
        queue.deliver(&deleted()).unwrap();
        // Full: the third and fourth are dropped, not an error.
        assert_eq!(queue.deliver(&closed()), Ok(()));
        assert_eq!(queue.deliver(&closed()), Ok(()));
        assert_eq!(queue.lost(), 2);
        queue.dropped(3);
        assert_eq!(queue.lost(), 5);

        assert_eq!(receiver.try_recv().unwrap(), changed());
        assert_eq!(receiver.try_recv().unwrap(), deleted());
        assert_eq!(receiver.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty));

        // Room again: delivery resumes.
        queue.deliver(&closed()).unwrap();
        assert_eq!(receiver.try_recv().unwrap(), closed());
    }

    #[test]
    fn a_queue_whose_receiver_is_gone_is_an_error() {
        let (queue, receiver) = Queue::bounded(1);
        drop(receiver);
        assert!(queue.deliver(&closed()).is_err());
    }
}
