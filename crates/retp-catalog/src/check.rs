//! `server test`: the verdict of each step, in the order one fails.
//!
//! *definition readable* then *every reference resolves* then *`/.well-known`
//! reachable* then *credentials obtained* then *session opened*. The steps
//! that need a network are the caller's ([`Network`]); the sequence, the
//! redaction and the record are the catalogue's, so a test drives all of it
//! with a fake network.

use std::time::{SystemTime, UNIX_EPOCH};

use retp_client::native::Endpoint;

use crate::catalog::Catalog;
use crate::error::CatalogError;
use crate::resolve::Overrides;
use crate::secret::SecretSource;

/// The two steps that need a server. Errors are words, which the catalogue
/// redacts before it shows or records them.
pub trait Network {
    /// Fetch `/.well-known/etp-server-capabilities`; the answer is a line such as `Name 1.2`.
    fn well_known(&self, endpoint: &Endpoint) -> Result<String, String>;

    /// Open a session with the credentials in `endpoint` (or reach the REST
    /// API's health route), then close it.
    fn session(&self, endpoint: Endpoint, rest: bool) -> Result<String, String>;
}

/// What a step came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// It worked.
    Passed(String),
    /// It did not.
    Failed(String),
    /// It was not tried.
    Skipped(String),
}

/// One step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    /// `definition`, `references`, `well-known`, `token`, `session`.
    pub name: &'static str,
    /// How it went.
    pub outcome: Outcome,
}

/// The steps of one test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestReport {
    /// The server.
    pub entry: String,
    /// Its steps, in order.
    pub steps: Vec<Step>,
}

impl TestReport {
    /// Whether no step failed.
    pub fn passed(&self) -> bool {
        !self.steps.iter().any(|s| matches!(s.outcome, Outcome::Failed(_)))
    }
}

const STEPS: [&str; 5] = ["definition", "references", "well-known", "token", "session"];

impl Catalog {
    /// Test a server. It never prints; the report holds words that name
    /// references and causes, and no secret value.
    pub fn run_test(&self, word: &str, secrets: &dyn SecretSource, network: &dyn Network) -> TestReport {
        let mut report = TestReport {
            entry: word.to_string(),
            steps: Vec::new(),
        };
        let push = |report: &mut TestReport, name: &'static str, outcome: Outcome| {
            report.steps.push(Step { name, outcome });
        };
        let skip_rest = |report: &mut TestReport| {
            for name in STEPS.iter().skip(report.steps.len()).copied() {
                report.steps.push(Step {
                    name,
                    outcome: Outcome::Skipped("an earlier step failed".into()),
                });
            }
        };

        let found = match self.find(word) {
            Ok(found) => found,
            Err(error) => {
                push(&mut report, "definition", Outcome::Failed(error.to_string()));
                skip_rest(&mut report);
                return report;
            }
        };
        report.entry = found.name.to_string();
        push(
            &mut report,
            "definition",
            Outcome::Passed(format!("readable, method {}", found.entry.auth.method())),
        );

        let described = found.entry.describe(found.name, secrets);
        let unresolved: Vec<String> = described
            .references
            .iter()
            .filter(|r| !r.status.resolves)
            .map(|r| format!("{} ({}): {}", r.field, r.reference, r.status.detail))
            .collect();
        if !unresolved.is_empty() {
            push(&mut report, "references", Outcome::Failed(unresolved.join("; ")));
            skip_rest(&mut report);
            return report;
        }
        let resolved = match found.entry.resolve(found.name, secrets, &Overrides::default()) {
            Ok(resolved) => resolved,
            Err(error) => {
                push(&mut report, "references", Outcome::Failed(error.to_string()));
                skip_rest(&mut report);
                return report;
            }
        };
        push(
            &mut report,
            "references",
            Outcome::Passed(format!("{} reference(s) resolve", described.references.len())),
        );

        if resolved.rest() {
            push(
                &mut report,
                "well-known",
                Outcome::Skipped("the REST API has no capabilities document".into()),
            );
        } else {
            match network.well_known(&resolved.endpoint(None)) {
                Ok(said) => push(&mut report, "well-known", Outcome::Passed(resolved.redact(&said))),
                Err(why) => {
                    push(&mut report, "well-known", Outcome::Failed(resolved.redact(&why)));
                    skip_rest(&mut report);
                    return report;
                }
            }
        }

        let opened = match resolved.obtain_endpoint() {
            Ok(opened) => opened,
            Err(error) => {
                push(
                    &mut report,
                    "token",
                    Outcome::Failed(resolved.redact(&error.to_string())),
                );
                skip_rest(&mut report);
                return report;
            }
        };
        let credentials = opened
            .notes
            .iter()
            .find_map(|note| note.strip_prefix("credentials: "))
            .unwrap_or("no credentials needed")
            .to_string();
        push(&mut report, "token", Outcome::Passed(credentials));

        match network.session(opened.endpoint, resolved.rest()) {
            Ok(said) => push(&mut report, "session", Outcome::Passed(resolved.redact(&said))),
            Err(why) => push(&mut report, "session", Outcome::Failed(resolved.redact(&why))),
        }
        report
    }

    /// Write `state/last.json`: when, and each step's verdict and words.
    pub fn record_test(&self, report: &TestReport) -> Result<(), CatalogError> {
        let dir = self.state_dir(&report.entry)?;
        let steps: Vec<serde_json::Value> = report
            .steps
            .iter()
            .map(|step| {
                let (status, detail) = match &step.outcome {
                    Outcome::Passed(d) => ("passed", d),
                    Outcome::Failed(d) => ("failed", d),
                    Outcome::Skipped(d) => ("skipped", d),
                };
                serde_json::json!({ "name": step.name, "status": status, "detail": detail })
            })
            .collect();
        let at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let record = serde_json::json!({
            "schema": 1,
            "tested_at_unix": at,
            "passed": report.passed(),
            "steps": steps,
        });
        let file = dir.join("last.json");
        let text = serde_json::to_string_pretty(&record).unwrap_or_default();
        std::fs::write(&file, text).map_err(|e| CatalogError::io(format!("writing {}", file.display()), &e))
    }
}
