//! Checks an ETP v1.2 server against what the specification makes
//! mandatory, and says what it does well and what it does not.
//!
//! The aim is a suite of tests that runs in one command against an ETP
//! server, to give feedback to a provider who offers a server to test. The
//! design is ADR-0005; this crate is **apart** from the client
//! on purpose - it uses `retp-proto` and `retp-client` and nothing
//! uses it -, so that what a server is judged on never leaks into what the
//! client tolerates.
//!
//! # What a check is
//!
//! One rule of the specification, cited with its section, one observation
//! of the server, and a [`Verdict`]. Four verdicts, and the difference
//! between them is the whole point of the report:
//!
//! - **pass** - observed, as the rule says;
//! - **fail** - observed, against the rule; the finding says what was seen;
//! - **tolerated** - against the rule, but its meaning was not in doubt and
//!   the client took it as meant (`retp-proto`'s deviations): a fault to
//!   report, not one that stops a session;
//! - **not applicable** - the rule is about a protocol or a feature the
//!   server did not announce; §3.1.1.2 §3 makes a protocol mandatory *in
//!   full* only once it is announced;
//! - **not checked** - the suite has no check for it yet, or could not get
//!   to it (no dataspace to look in, no object to read).
//!
//! A refusal to guess runs through it: a check that cannot observe answers
//! *not checked*, never *pass* - a green report on a server nobody could
//! reach would be the worst thing this could produce.

use std::collections::BTreeMap;
use std::fmt;

use std::time::Duration;

use retp_client::native::Remote;
use retp_client::{Acknowledged, Blocking, Client, ClientError, Clock, Transport};
use retp_proto::connection::Event;
use retp_proto::negotiate::Negotiated;
use retp_proto::wire::{Delivery, Place};
use retp_types::datatypes::MessageHeader;

pub mod checks;
pub mod exchanges;
pub mod raw;
pub mod reads;
pub mod renewing;
pub mod samples;

/// What the checks need of a session, whichever transport carries it.
///
/// The WebSocket client has all of it; the REST client - `Remote::Rest` -
/// has no session to speak of, no acknowledgements and no wire of its own,
/// and says so through [`Session::is_rest`]: the checks about those answer
/// *not applicable* rather than judging the REST client's own bookkeeping
/// as if it were the server's.
pub trait Session: Blocking {
    /// Whether this is the Reservoir DDMS REST API rather than ETP.
    fn is_rest(&self) -> bool;
    /// What the session was opened with - or, over REST, what the client
    /// emulates.
    fn session(&self) -> Negotiated;
    /// A `Ping` and its `Pong` - over REST, the API's health.
    fn ping(&mut self) -> Result<Duration, ClientError>;
    fn ask_acknowledge(&mut self, on: bool);
    fn acknowledgements(&mut self) -> Vec<Acknowledged>;
    fn unsolicited(&mut self) -> Vec<Event>;
    /// Each time the session was opened again after a request timed out
    /// (`renewing::Renewing`), said once and taken; none for a session that
    /// does not reopen.
    fn renewals(&mut self) -> Vec<String> {
        Vec::new()
    }
    /// Each request sent and what came back, readable, since the last take
    /// (`renewing::Renewing` records them); none for a session that does not.
    fn exchanges(&mut self) -> Vec<exchanges::Exchange> {
        Vec::new()
    }
    /// How many times the session was opened again: 0 for the first. A
    /// session's message ids are its own, and start over in the next one.
    fn generation(&self) -> usize {
        0
    }
}

impl<T: Transport, C: Clock> Session for Client<T, C> {
    fn is_rest(&self) -> bool {
        false
    }

    fn session(&self) -> Negotiated {
        self.negotiated().clone()
    }

    fn ping(&mut self) -> Result<Duration, ClientError> {
        Client::ping(self)
    }

    fn ask_acknowledge(&mut self, on: bool) {
        Client::ask_acknowledge(self, on)
    }

    fn acknowledgements(&mut self) -> Vec<Acknowledged> {
        Client::acknowledgements(self)
    }

    fn unsolicited(&mut self) -> Vec<Event> {
        Client::unsolicited(self)
    }
}

impl Session for Remote {
    fn is_rest(&self) -> bool {
        Remote::is_rest(self)
    }

    fn session(&self) -> Negotiated {
        Remote::negotiated(self)
    }

    fn ping(&mut self) -> Result<Duration, ClientError> {
        Remote::ping(self)
    }

    fn ask_acknowledge(&mut self, on: bool) {
        let _ = Remote::ask_acknowledge(self, on);
    }

    fn acknowledgements(&mut self) -> Vec<Acknowledged> {
        Remote::acknowledgements(self)
    }

    fn unsolicited(&mut self) -> Vec<Event> {
        Remote::unsolicited(self)
    }
}

/// What a check concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Verdict {
    /// Observed, as the rule says.
    Pass,
    /// Observed, against the rule.
    Fail,
    /// Against the rule, taken as meant - reported, not fatal.
    Tolerated,
    /// About something the server did not announce.
    NotApplicable,
    /// Not observed: no check yet, or nothing to observe it on.
    NotChecked,
}

impl Verdict {
    /// The word the report prints.
    pub fn word(self) -> &'static str {
        match self {
            Verdict::Pass => "pass",
            Verdict::Fail => "FAIL",
            Verdict::Tolerated => "tolerated",
            Verdict::NotApplicable => "n/a",
            Verdict::NotChecked => "not checked",
        }
    }
}

/// One rule, and what the server did about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    /// A stable id, `core.open-session.session-id`.
    pub id: &'static str,
    /// The specification's section, `§5.2.2`.
    pub section: &'static str,
    /// The rule, in the specification's words or close to them.
    pub rule: &'static str,
    /// The protocol it belongs to, `None` for the rules of chapters 3 to 5
    /// that hold for every session.
    pub protocol: Option<i32>,
    pub verdict: Verdict,
    /// What was observed - the evidence, or why nothing could be.
    pub said: String,
}

impl Finding {
    fn new(
        id: &'static str,
        section: &'static str,
        rule: &'static str,
        protocol: Option<i32>,
        verdict: Verdict,
        said: impl Into<String>,
    ) -> Self {
        Finding {
            id,
            section,
            rule,
            protocol,
            verdict,
            said: said.into(),
        }
    }
}

/// What a run is told.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// Also the checks that write: a dataspace of the suite's own is
    /// created, written into, read back, emptied and deleted. Off by
    /// default - a provider's server is not ours to write on unasked.
    pub write: bool,
    /// The dataspace to read in, when the server holds several; the first
    /// listed otherwise.
    pub dataspace: Option<String>,
    /// The path of the dataspace the writing checks create -
    /// `example/etp-conformance-<seconds>` otherwise. A name the server
    /// already lists is **never** written into: the suite writes only in a
    /// dataspace it created itself.
    pub write_dataspace: Option<String>,
    /// The `customData` the created dataspace is put with - what an OSDU
    /// store asks for (`legaltags`, `owners`, `viewers`).
    pub custom_data: BTreeMap<String, retp_types::datatypes::DataValue>,
    /// When the suite may not create a dataspace: an **existing** one to
    /// write its samples into instead. Only the suite's own objects are
    /// deleted there, never the dataspace.
    pub write_fallback: Option<String>,
}

/// One request the run sent, and how it went.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    /// The message, `GetDataObjects`.
    pub request: String,
    pub took: Duration,
    /// How it ended.
    pub outcome: CallOutcome,
}

/// How a request ended - three different news.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallOutcome {
    /// Answered without an error.
    Answered,
    /// Answered with an error (a ProtocolException, or a key refused).
    Refused,
    /// Not answered: the transport, a timeout, a session gone.
    Failed,
}

/// What the requests of one message cost, over a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestStat {
    pub request: String,
    pub count: usize,
    pub answered: usize,
    pub refused: usize,
    pub failed: usize,
    pub min: Duration,
    pub median: Duration,
    pub max: Duration,
}

/// One object the writing checks sent, and what became of it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Upload {
    /// `resqml22.PointSetRepresentation`.
    pub qualified_type: String,
    pub uri: String,
    /// Whether the session's data object types cover it - a type the server
    /// did not announce is sent too, to see what it does with it.
    pub announced: bool,
    /// `accepted`, or the refusal.
    pub put: String,
    /// `same object`, or what came back.
    pub read_back: String,
    /// What happened to its array, when it has one.
    pub array: String,
    /// `deleted`, or why not.
    pub deleted: String,
}

/// Who wrote the report, and how far to trust a failure - at the head of
/// every report, Markdown and JSON alike. The reports say they were written
/// by this tool, and that a failure may be the tool's own as well as the
/// server's.
pub const ABOUT: &str = "This report was written by retp-conformance, a tool that \
checks an ETP v1.2 server against what the \
specification makes mandatory: each finding names the rule, its section, and what was observed. \
A failure reported here may come from this tool's own implementation rather than from the server; \
if you believe one does, please open an issue on the project's repository.";

/// The report: every finding, and what it adds up to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    /// The server, as it named itself - `applicationName applicationVersion`.
    pub server: String,
    /// The URL that was checked.
    pub url: String,
    /// The protocols the session agreed, by number.
    pub protocols: Vec<i32>,
    pub findings: Vec<Finding>,
    /// How the suite got in - a certificate it could not verify, a
    /// dataspace it fell back on. Said first: it changes how the rest reads.
    pub notes: Vec<String>,
    /// The data object types the session agreed.
    pub data_objects: Vec<String>,
    /// Every request sent, by message.
    pub requests: Vec<RequestStat>,
    /// What the writing checks sent.
    pub uploads: Vec<Upload>,
    /// Every request sent and what came back, readable - the appendix.
    pub exchanges: Vec<exchanges::Exchange>,
}

/// The capabilities the report sums up, and the checks each one reads.
const CAPABILITIES: [(&str, &[&str]); 18] = [
    ("TLS certificate a client can verify", &["tls.chain"]),
    ("Open a session", &["core.open-session.session-id"]),
    ("Ping / Pong", &["core.ping-pong"]),
    ("List dataspaces", &["dataspace.list"]),
    ("List resources (Discovery)", &["discovery.dataspace"]),
    (
        "Error codes on bad requests",
        &[
            "discovery.invalid-uri",
            "discovery.not-found",
            "store.get.not-found",
            "data-array.not-found",
        ],
    ),
    ("Read data objects (Store)", &["store.get"]),
    (
        "Array metadata (DataArray)",
        &["data-array.metadata", "write.array.metadata"],
    ),
    ("Read arrays (DataArray)", &["data-array.read"]),
    (
        "Read sub-arrays (DataArray)",
        &["data-array.subarray", "write.array.subarray"],
    ),
    ("Supported types", &["supported-types.list"]),
    (
        "Transactions",
        &[
            "transaction.start-rollback",
            "transaction.rollback-leaves-nothing",
            "transaction.commit-keeps",
        ],
    ),
    ("Create a dataspace", &["write.dataspace.put"]),
    (
        "Write data objects",
        &["write.object.put", "write.samples.announced"],
    ),
    ("Write arrays", &["write.array.put", "write.array.read-back"]),
    ("Write arrays in blocks (DataArray)", &["write.array.blocks"]),
    (
        "Delete data objects",
        &["write.object.delete", "write.samples.delete"],
    ),
    ("Delete a dataspace", &["write.dataspace.delete"]),
];

impl Report {
    /// How many of each verdict.
    pub fn tally(&self) -> BTreeMap<Verdict, usize> {
        let mut out = BTreeMap::new();
        for finding in &self.findings {
            *out.entry(finding.verdict).or_insert(0) += 1;
        }
        out
    }

    /// Whether anything failed outright.
    pub fn has_failures(&self) -> bool {
        self.findings.iter().any(|one| one.verdict == Verdict::Fail)
    }

    /// What the server supports, capability by capability, read off the
    /// findings: `yes` (every observation passed or was tolerated), `partly`
    /// (some passed, some failed), `no` (observed, and failed), `not
    /// announced`, or `not tested` - never `yes` on nothing observed.
    pub fn support(&self) -> Vec<(&'static str, &'static str, String)> {
        CAPABILITIES
            .iter()
            .map(|(label, ids)| {
                let ours: Vec<&Finding> = self.findings.iter().filter(|one| ids.contains(&one.id)).collect();
                let good = ours
                    .iter()
                    .filter(|one| matches!(one.verdict, Verdict::Pass | Verdict::Tolerated))
                    .count();
                let bad = ours.iter().filter(|one| one.verdict == Verdict::Fail).count();
                let absent = ours.iter().any(|one| one.verdict == Verdict::NotApplicable);
                let status = match (good, bad) {
                    (0, 0) if absent => "not announced",
                    (0, 0) => "not tested",
                    (_, 0) => "yes",
                    (0, _) => "no",
                    _ => "partly",
                };
                let checks: Vec<String> = ours
                    .iter()
                    .map(|one| format!("`{}` {}", one.id, one.verdict.word()))
                    .collect();
                (*label, status, checks.join(", "))
            })
            .collect()
    }

    /// The requests of a run, by message: count, outcomes, response times.
    pub fn requests_of(calls: &[Call]) -> Vec<RequestStat> {
        let mut by: BTreeMap<&str, Vec<&Call>> = BTreeMap::new();
        for call in calls {
            by.entry(call.request.as_str()).or_default().push(call);
        }
        by.into_iter()
            .map(|(request, calls)| {
                let mut times: Vec<Duration> = calls.iter().map(|c| c.took).collect();
                times.sort();
                RequestStat {
                    request: request.to_string(),
                    count: calls.len(),
                    answered: calls
                        .iter()
                        .filter(|c| c.outcome == CallOutcome::Answered)
                        .count(),
                    refused: calls.iter().filter(|c| c.outcome == CallOutcome::Refused).count(),
                    failed: calls.iter().filter(|c| c.outcome == CallOutcome::Failed).count(),
                    min: times[0],
                    median: times[times.len() / 2],
                    max: times[times.len() - 1],
                }
            })
            .collect()
    }

    /// The report as Markdown - what goes to a provider.
    pub fn markdown(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("# ETP v1.2 conformance - {}\n\n", self.server));
        out.push_str(&format!("> {ABOUT}\n\n"));
        out.push_str(&format!("Checked: `{}`\n\n", self.url));
        if !self.protocols.is_empty() {
            let listed: Vec<String> = self.protocols.iter().map(|p| p.to_string()).collect();
            out.push_str(&format!("Protocols agreed: {}\n\n", listed.join(", ")));
        }
        if !self.data_objects.is_empty() {
            out.push_str(&format!(
                "Data object types agreed: {}\n\n",
                self.data_objects.join(", ")
            ));
        }
        if !self.notes.is_empty() {
            out.push_str("## How the suite connected\n\n");
            for note in &self.notes {
                out.push_str(&format!("- {}\n", cell(note)));
            }
            out.push('\n');
        }
        out.push_str("## What this server supports\n\n| capability | supported | from |\n|---|---|---|\n");
        for (label, status, from) in self.support() {
            out.push_str(&format!("| {label} | **{status}** | {} |\n", cell(&from)));
        }
        out.push('\n');
        if !self.requests.is_empty() {
            out.push_str("## Requests sent and response times\n\n| request | sent | answered | refused | not answered | min (ms) | median (ms) | max (ms) |\n|---|---|---|---|---|---|---|---|\n");
            for one in &self.requests {
                out.push_str(&format!(
                    "| `{}` | {} | {} | {} | {} | {} | {} | {} |\n",
                    one.request,
                    one.count,
                    one.answered,
                    one.refused,
                    one.failed,
                    one.min.as_millis(),
                    one.median.as_millis(),
                    one.max.as_millis()
                ));
            }
            out.push('\n');
        }
        if !self.uploads.is_empty() {
            out.push_str("## Objects the suite wrote, then deleted\n\n| type | announced by the server | put | read back | array | deleted |\n|---|---|---|---|---|---|\n");
            for one in &self.uploads {
                out.push_str(&format!(
                    "| `{}` | {} | {} | {} | {} | {} |\n",
                    one.qualified_type,
                    if one.announced { "yes" } else { "no" },
                    cell(&one.put),
                    cell(&one.read_back),
                    cell(&one.array),
                    cell(&one.deleted)
                ));
            }
            out.push('\n');
        }
        let tally = self.tally();
        let count = |verdict: Verdict| tally.get(&verdict).copied().unwrap_or(0);
        out.push_str(&format!(
            "**{} pass, {} fail, {} tolerated, {} not applicable, {} not checked.**\n\n",
            count(Verdict::Pass),
            count(Verdict::Fail),
            count(Verdict::Tolerated),
            count(Verdict::NotApplicable),
            count(Verdict::NotChecked)
        ));
        // Failures first: they are what a provider opens the report for.
        for (title, verdict) in [
            ("What is not conformant", Verdict::Fail),
            ("What was taken as meant, and should be fixed", Verdict::Tolerated),
            ("What is conformant", Verdict::Pass),
            ("Not checked", Verdict::NotChecked),
            ("Not applicable", Verdict::NotApplicable),
        ] {
            let ours: Vec<&Finding> = self
                .findings
                .iter()
                .filter(|one| one.verdict == verdict)
                .collect();
            if ours.is_empty() {
                continue;
            }
            out.push_str(&format!("## {title}\n\n"));
            out.push_str("| check | section | rule | observed |\n|---|---|---|---|\n");
            for one in ours {
                out.push_str(&format!(
                    "| `{}` | {} | {} | {} |\n",
                    one.id,
                    one.section,
                    cell(one.rule),
                    cell(&one.said)
                ));
            }
            out.push('\n');
        }
        if !self.exchanges.is_empty() {
            out.push_str("## Requests sent and messages received\n\n");
            out.push_str(
                "Every request of the session and every message that came back, header first, in ETP's JSON encoding \
(the Avro JSON encoding, ETP v1.2 §3.8.1). A long array is shown as its first values and its count, a long string cut.\n\n",
            );
            for (n, one) in self.exchanges.iter().enumerate() {
                let name = one.request.rsplit('.').next().unwrap_or(&one.request);
                out.push_str(&format!(
                    "### {}. `{name}` - {}\n\n```json\n{}\n```\n\n",
                    n + 1,
                    one.outcome,
                    one.sent
                ));
                for answer in &one.received {
                    out.push_str(&format!("Received:\n\n```text\n{answer}\n```\n\n"));
                }
            }
        }
        out
    }

    /// The report as JSON - for a tool, or a diff between two runs.
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "about": ABOUT,
            "server": self.server,
            "url": self.url,
            "protocols": self.protocols,
            "tally": self.tally().iter().map(|(v, n)| (v.word().to_string(), *n)).collect::<BTreeMap<_, _>>(),
            "notes": self.notes,
            "dataObjects": self.data_objects,
            "support": self.support().iter().map(|(label, status, _)| serde_json::json!({ "capability": label, "supported": status })).collect::<Vec<_>>(),
            "requests": self.requests.iter().map(|one| serde_json::json!({
                "request": one.request,
                "sent": one.count,
                "answered": one.answered,
                "refused": one.refused,
                "notAnswered": one.failed,
                "minMs": one.min.as_millis() as u64,
                "medianMs": one.median.as_millis() as u64,
                "maxMs": one.max.as_millis() as u64,
            })).collect::<Vec<_>>(),
            "exchanges": self.exchanges.iter().map(|one| serde_json::json!({
                "request": one.request,
                "outcome": one.outcome,
                "sent": one.sent,
                "received": one.received,
            })).collect::<Vec<_>>(),
            "uploads": self.uploads.iter().map(|one| serde_json::json!({
                "type": one.qualified_type,
                "uri": one.uri,
                "announced": one.announced,
                "put": one.put,
                "readBack": one.read_back,
                "array": one.array,
                "deleted": one.deleted,
            })).collect::<Vec<_>>(),
            "findings": self.findings.iter().map(|one| serde_json::json!({
                "id": one.id,
                "section": one.section,
                "rule": one.rule,
                "protocol": one.protocol,
                "verdict": one.verdict.word(),
                "observed": one.said,
            })).collect::<Vec<_>>(),
        })
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for one in &self.findings {
            writeln!(
                f,
                "{:<12} {:<40} {:<10} {}",
                one.verdict.word(),
                one.id,
                one.section,
                one.said
            )?;
        }
        Ok(())
    }
}

/// A Markdown table cell: no pipe, no newline.
fn cell(text: &str) -> String {
    text.replace('|', "\\|").replace('\n', " ")
}

/// Every header the server sent during a run, in the order they arrived -
/// what the rules on message ids and correlation are checked against.
#[derive(Debug, Default)]
pub struct Seen {
    /// `(header, where the wire placed it, whether it was the last of its action)`.
    pub headers: Vec<(MessageHeader, Place, bool)>,
    /// What arrived unasked and correlated to nothing of ours - an exception
    /// with `correlationId` 0 about a request, say.
    pub stray: Vec<String>,
    /// The longest a request waited for its answer, and which one.
    pub slowest: Option<(std::time::Duration, String)>,
    /// Every request, in the order sent.
    pub calls: Vec<Call>,
    /// The session the last headers came on, and where in `headers` each
    /// later session began: ids are unique and increasing **within a
    /// session** (§3.5.4.1), and a session opened again after a timeout
    /// (`renewing`) starts them over.
    pub session: Option<usize>,
    /// The index in `headers` where each session after the first began.
    pub sessions_from: Vec<usize>,
}

impl Seen {
    /// Note the deliveries of one call.
    pub fn note(&mut self, deliveries: &[Delivery]) {
        for delivery in deliveries {
            self.headers
                .push((delivery.header.clone(), delivery.place, delivery.last));
        }
    }

    /// Note how long one call took, and what it was.
    pub fn timed(&mut self, took: std::time::Duration, what: &str) {
        if self.slowest.as_ref().is_none_or(|(before, _)| took > *before) {
            self.slowest = Some((took, what.to_string()));
        }
    }

    /// Note one request: its message (the short name, `GetDataObjects`),
    /// how long it took and how it ended.
    pub fn record(&mut self, request: &str, took: Duration, outcome: CallOutcome) {
        let short = request.rsplit('.').next().unwrap_or(request).to_string();
        self.timed(took, request);
        self.calls.push(Call {
            request: short,
            took,
            outcome,
        });
    }
}

/// What the session said of itself, for the report's head.
pub fn describe(negotiated: &Negotiated) -> (String, Vec<i32>) {
    let (name, version) = &negotiated.peer_application;
    (
        format!("{name} {version}").trim().to_string(),
        negotiated.protocols.keys().copied().collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(id: &'static str, verdict: Verdict) -> Finding {
        Finding::new(id, "§1", "a rule | with a pipe", None, verdict, "seen\nhere")
    }

    /// The report puts failures first and counts every verdict: a provider
    /// reads the first table, and a tally that lost a verdict would hide a
    /// class of findings from the head line.
    #[test]
    fn the_report_leads_with_failures_and_counts_everything() {
        let report = Report {
            server: "x 1".into(),
            url: "wss://x".into(),
            protocols: vec![3, 4],
            findings: vec![
                a("ok", Verdict::Pass),
                a("bad", Verdict::Fail),
                a("meh", Verdict::Tolerated),
                a("na", Verdict::NotApplicable),
                a("nc", Verdict::NotChecked),
            ],
            ..Default::default()
        };
        let text = report.markdown();
        assert!(
            text.contains("**1 pass, 1 fail, 1 tolerated, 1 not applicable, 1 not checked.**"),
            "{text}"
        );
        let fail_at = text.find("What is not conformant").unwrap();
        let pass_at = text.find("What is conformant").unwrap();
        assert!(fail_at < pass_at);
        // A pipe in a rule does not break the table, a newline does not break the row.
        assert!(text.contains("a rule \\| with a pipe"));
        assert!(text.contains("seen here"));
        assert!(report.has_failures());
        assert_eq!(report.json()["tally"]["FAIL"], 1);
    }
}
