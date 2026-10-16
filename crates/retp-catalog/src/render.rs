//! The text of `server show`, `server list` and `server test`.
//!
//! Strings, not prints: the command line prints them, and a test reads
//! exactly what a person would see.

use std::fmt::Write as _;

use crate::catalog::Listed;
use crate::check::{Outcome, TestReport};
use crate::entry::{Handshake, Tls};
use crate::resolve::Description;

/// `server show`: everything the definition says, references and whether they
/// resolve included, no value.
pub fn show(d: &Description) -> String {
    let mut out = String::new();
    let _ = write!(out, "{}", d.name);
    if !d.aliases.is_empty() {
        let _ = write!(out, " (alias: {})", d.aliases.join(", "));
    }
    out.push('\n');
    let mut line = |label: &str, value: &str| {
        let _ = writeln!(out, "  {label:<14}{value}");
    };
    if let Some(text) = &d.description {
        line("description", text);
    }
    if let Some(url) = &d.etp_url {
        line("etp url", url);
    }
    if let Some(url) = &d.rest_url {
        line("rest url", url);
    }
    line("transport", d.transport.as_str());
    if let Some(partition) = &d.partition {
        line("partition", partition);
    }
    line(
        "certificate",
        match d.tls {
            Tls::Verify => "checked",
            Tls::Skip => "NOT checked",
        },
    );
    line(
        "handshake",
        match d.handshake {
            Handshake::Native => "native",
            Handshake::Browser => "browser",
        },
    );
    line("protected", if d.protected { "yes" } else { "no" });
    line("auth", d.method);
    for (key, value) in &d.fields {
        line(&format!("  {key}"), value);
    }
    for reference in &d.references {
        let state = if reference.status.resolves {
            "resolves"
        } else {
            "DOES NOT RESOLVE"
        };
        line(
            &format!("  {}", reference.field),
            &format!("{}  ({state}: {})", reference.reference, reference.status.detail),
        );
    }
    if !d.headers.is_empty() {
        line("headers", "");
        for (name, how) in &d.headers {
            line(&format!("  {name}"), how);
        }
    }
    if !d.defaults.is_empty() {
        line("write defaults", "");
        for (key, value) in &d.defaults {
            line(&format!("  {key}"), value);
        }
    }
    out
}

/// `server list`: one row per server.
pub fn list(rows: &[Listed]) -> String {
    if rows.is_empty() {
        return "no server in the catalogue yet: add one with `retp server add`\n".to_string();
    }
    let width = rows.iter().map(|r| r.name.len()).max().unwrap_or(4).max(4);
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{:<width$}  {:<16}  {:<8}  URL",
        "NAME", "AUTH", "SECRETS"
    );
    for row in rows {
        let mut name = row.name.clone();
        if row.is_default {
            name.push('*');
        }
        if let Some(problem) = &row.problem {
            let _ = writeln!(out, "{name:<width$}  unusable: {problem}");
            continue;
        }
        let (total, missing) = row.references;
        let secrets = match (total, missing) {
            (0, _) => "-".to_string(),
            (_, 0) => "ok".to_string(),
            (_, n) => format!("{n} missing"),
        };
        let mut url = row.url.clone();
        if row.protected {
            url.push_str("  [protected]");
        }
        if !row.aliases.is_empty() {
            let _ = write!(url, "  (alias: {})", row.aliases.join(", "));
        }
        let _ = writeln!(out, "{name:<width$}  {:<16}  {secrets:<8}  {url}", row.method);
    }
    out.push_str("* is the default server\n");
    out
}

/// `server test`: one line per step.
pub fn test(report: &TestReport) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "{}", report.entry);
    for step in &report.steps {
        let (mark, detail) = match &step.outcome {
            Outcome::Passed(d) => ("ok", d),
            Outcome::Failed(d) => ("FAILED", d),
            Outcome::Skipped(d) => ("skipped", d),
        };
        let _ = writeln!(out, "  {:<11}{mark}: {detail}", step.name);
    }
    out
}
