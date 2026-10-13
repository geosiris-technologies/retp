//! `retp-conformance wss://host/etp` - one command, one report.
//!
//! The command reaches the server three ways, in order, and each way is a
//! finding of its own before any check of the session runs: the
//! `.well-known/etp-server-capabilities` document (§4.3.1), the TLS
//! handshake with the certificate **checked** (§4.1.1 - a server that does
//! not serve its chain refuses every strict client), then the session. Then
//! `retp_conformance::checks::run`, and the report to stdout, or to
//! a file as Markdown or JSON.

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::Parser;
use retp_client::login::Choice;
use retp_client::native::{self, CertificateCheck, Connect, Endpoint, Remote, WebSocketTransport};
use retp_client::{ServerProfile, SystemClock, writing_client};
use retp_conformance::renewing::Renewing;
use retp_conformance::{Finding, Options, Report, Verdict, checks, raw};
use retp_proto::credentials::Credentials;
use retp_proto::probe::{HandshakeSettings, Profile, handshake};

/// Check an ETP v1.2 server against what the specification makes mandatory.
#[derive(Parser)]
#[command(name = "retp-conformance", version)]
struct Args {
    /// The server's WebSocket URL: `wss://host[:port]/path` - or, with
    /// `--rest`, its REST API's: `https://host/api/reservoir-ddms/v2`.
    url: String,
    /// Check OSDU's Reservoir DDMS through its REST API instead of the ETP
    /// WebSocket: the same checks where the API has a counterpart; what only
    /// the wire has (`.well-known`, acknowledgements, message headers, the
    /// faults) is *not applicable*.
    #[arg(long)]
    rest: bool,
    /// A bearer token. Read from `ETP_TOKEN` when no other login is given.
    #[arg(long, conflicts_with_all = ["user", "token_url", "azure_tenant", "cognito_region"])]
    token: Option<String>,
    /// HTTP Basic's user - or, with `--token-url`, the password grant's, or
    /// with `--cognito-region`, the Cognito user. Read from `ETP_USER` when
    /// not given.
    #[arg(long)]
    user: Option<String>,
    /// The password of `--user`. Read from `ETP_PASSWORD` when not given -
    /// which keeps it off the command line and out of the shell's history.
    #[arg(long)]
    password: Option<String>,
    /// An OAuth 2.0 token endpoint: a token for the user and its password
    /// (the password grant), or by client credentials when no user is given.
    #[arg(long, conflicts_with_all = ["azure_tenant", "cognito_region"])]
    token_url: Option<String>,
    /// An Azure AD tenant: a token from its endpoint, by client credentials.
    #[arg(long, conflicts_with = "cognito_region")]
    azure_tenant: Option<String>,
    /// The AWS region of a Cognito user pool.
    #[arg(long)]
    cognito_region: Option<String>,
    /// The OAuth or Cognito app client's id.
    #[arg(long)]
    client_id: Option<String>,
    /// Its secret. Read from `ETP_CLIENT_SECRET` when not given.
    #[arg(long)]
    client_secret: Option<String>,
    /// The scope to ask the token for.
    #[arg(long)]
    oauth_scope: Option<String>,
    /// An OSDU data partition (`data-partition-id`).
    #[arg(long)]
    partition: Option<String>,
    /// Do not check the server's certificate. The TLS check is then *not
    /// checked* rather than failed.
    #[arg(long)]
    insecure: bool,
    /// Check the certificate, and when the server's cannot be verified, say
    /// so (`tls.chain` fails) and run the rest without checking it - one
    /// command gives the whole report instead of stopping at the handshake.
    #[arg(long, conflicts_with = "insecure")]
    tls_fallback: bool,
    /// Also the checks that write: a dataspace of the suite's own, created,
    /// written into, emptied and deleted. Nothing of the server's is touched.
    #[arg(long)]
    write: bool,
    /// With `--write`: the path of the dataspace to create
    /// (`example/etp-conformance-<seconds>` otherwise). A dataspace the
    /// server already lists is never written into.
    #[arg(long, value_name = "PATH", requires = "write")]
    write_dataspace: Option<String>,
    /// With `--write`: a `customData` entry of the created dataspace,
    /// `KEY=VALUE` (`a,b` is a list) - what an OSDU store asks for:
    /// `legaltags=…`, `owners=…`, `viewers=…`. Repeatable.
    #[arg(long = "dataspace-custom", value_name = "KEY=VALUE", requires = "write")]
    dataspace_custom: Vec<String>,
    /// With `--write`: when the server refuses to let the suite create its
    /// dataspace, write the samples into this **existing** one instead. Only
    /// the objects the suite put there are deleted; the dataspace never is.
    #[arg(long = "write-fallback-dataspace", value_name = "PATH", requires = "write")]
    write_fallback: Option<String>,
    /// The dataspace to read in, by path or URI; the first listed otherwise.
    #[arg(long)]
    dataspace: Option<String>,
    /// The data object types to ask for (`resqml20.*`, `eml23.*`…); every
    /// release this build knows otherwise.
    #[arg(long = "type", value_name = "QUALIFIED_TYPE")]
    types: Vec<String>,
    /// Write the report here - Markdown, or JSON when the name ends in `.json`.
    #[arg(long, short)]
    out: Option<PathBuf>,
    /// Also write the report as JSON here, beside `--out` - for a script
    /// that reads the verdicts and a person who reads the Markdown.
    #[arg(long, value_name = "PATH")]
    json: Option<PathBuf>,
}

fn main() -> Result<ExitCode> {
    let args = Args::parse();
    let mut report = Report {
        url: args.url.clone(),
        ..Default::default()
    };
    let options = options_of(&args)?;

    // The login is chosen as `energyml etp` chooses it (`login::Choice`), and
    // a token is asked for before anything else: a refusal there is not the
    // ETP server's, and the report says so instead of failing a check.
    let choice = Choice {
        token: args.token.clone(),
        user: args.user.clone(),
        password: args.password.clone(),
        token_url: args.token_url.clone(),
        azure_tenant: args.azure_tenant.clone(),
        cognito_region: args.cognito_region.clone(),
        client_id: args.client_id.clone(),
        client_secret: args.client_secret.clone(),
        oauth_scope: args.oauth_scope.clone(),
    };
    let login = choice
        .login(|name| std::env::var(name).ok())
        .map_err(|why| anyhow::anyhow!("{why}"))?;
    let credentials: Option<Credentials> = match login.map(|login| login.obtain()).transpose() {
        Ok(obtained) => obtained.map(|o| o.credentials),
        Err(why) => {
            report.notes.push(format!("No token could be obtained from the authorization server, so the server itself was not reached: {why}"));
            report.findings.push(Finding {
                id: "login.token",
                section: "§4.1.2",
                rule: "The credentials the server's authorization server hands out are obtained before connecting",
                protocol: None,
                verdict: Verdict::NotChecked,
                said: format!("{why}"),
            });
            return finish(&report, args.out.as_deref(), args.json.as_deref());
        }
    };
    let mut certificates = if args.insecure {
        CertificateCheck::Skip
    } else {
        CertificateCheck::Verify
    };
    let mut endpoint = Endpoint {
        url: args.url.clone(),
        profile: match &args.partition {
            Some(data_partition) => ServerProfile::Rddms {
                data_partition: data_partition.clone(),
            },
            None => ServerProfile::Standard,
        },
        credentials,
        certificates,
        handshake: Profile::Native,
    };
    // A certificate the client could not verify, when `--tls-fallback` let
    // the run go on without it: the `tls.chain` finding says it, and so
    // does the report's head.
    let mut tls_failure: Option<String> = None;

    if args.rest {
        return over_rest(&args, endpoint, report, options);
    }

    // --- §4.3.1: the server says what it is before any connection --------------
    let mut fetched = native::fetch_server_capabilities(&endpoint);
    if let Err(why) = &fetched
        && args.tls_fallback
        && is_certificate_error(&why.to_string())
    {
        tls_failure = Some(why.to_string());
        certificates = CertificateCheck::Skip;
        endpoint.certificates = certificates;
        fetched = native::fetch_server_capabilities(&endpoint);
    }
    match fetched {
        Ok((where_, capabilities)) => {
            report.findings.push(Finding {
                id: "well-known.reachable",
                section: "§4.3.1",
                rule: "A server MUST provide its ServerCapabilities at /.well-known/etp-server-capabilities, at the scheme matching the WebSocket's",
                protocol: None,
                verdict: Verdict::Pass,
                said: format!("{where_}: {} {}", capabilities.application_name, capabilities.application_version),
            });
            let protocols: Vec<i32> = capabilities
                .supported_protocols
                .iter()
                .map(|p| p.protocol)
                .collect();
            report.findings.push(Finding {
                id: "well-known.protocols",
                section: "§3.1.1.2 §4.a",
                rule: "Protocols that a server supports MUST be listed in its ServerCapabilities record",
                protocol: None,
                verdict: if protocols.is_empty() {
                    Verdict::Fail
                } else {
                    Verdict::Pass
                },
                said: format!("{protocols:?}"),
            });
            let details = capabilities
                .endpoint_capabilities
                .contains_key("AuthorizationDetails");
            report.findings.push(Finding {
                id: "well-known.authorization-details",
                section: "§4.1.2 §3, §5.2.3",
                rule: "An ETP server MUST have the AuthorizationDetails endpoint capability",
                protocol: None,
                verdict: if details { Verdict::Pass } else { Verdict::Fail },
                said: if details {
                    "present".into()
                } else {
                    format!(
                        "absent; endpoint capabilities: {:?}",
                        capabilities.endpoint_capabilities.keys().collect::<Vec<_>>()
                    )
                },
            });
            let formats_ok = capabilities
                .supported_formats
                .iter()
                .any(|f| f.eq_ignore_ascii_case("xml"));
            report.findings.push(Finding {
                id: "well-known.formats",
                section: "§4.3.1, §23 ServerCapabilities",
                rule: "supportedFormats lists xml, the format every store supports",
                protocol: None,
                verdict: if formats_ok { Verdict::Pass } else { Verdict::Fail },
                said: format!("{:?}", capabilities.supported_formats),
            });
        }
        Err(why) => {
            // §4.3.1 §5.a.i: the document may be protected, and then the 401
            // MUST carry a WWW-Authenticate challenge. That is conformant -
            // and the document is still not read, so what follows says so.
            let text = why.to_string();
            let protected = text.contains("401") && text.contains("the server asks for");
            report.findings.push(Finding {
                id: "well-known.reachable",
                section: "§4.3.1",
                rule: "A server MUST provide its ServerCapabilities at /.well-known/etp-server-capabilities - protected at most by an HTTP challenge",
                protocol: None,
                verdict: if protected { Verdict::Pass } else { Verdict::Fail },
                said: if protected { format!("protected, with a challenge: {text}") } else { text },
            });
            for id in [
                "well-known.protocols",
                "well-known.authorization-details",
                "well-known.formats",
            ] {
                report.findings.push(Finding {
                    id,
                    section: "§4.3.1",
                    rule: "What the ServerCapabilities document says",
                    protocol: None,
                    verdict: Verdict::NotChecked,
                    said: "the document could not be read with these credentials".into(),
                });
            }
        }
    }

    // --- §4.1.1: TLS, with the certificate checked ---------------------------------
    let (name, version) = ("retp-conformance", env!("CARGO_PKG_VERSION"));
    let open = |endpoint: &Endpoint| {
        let (offer, handlers) = writing_client(name, version, native::random_uuid(), &args.types);
        let offer = if args.types.is_empty() {
            native::fit_to_server(endpoint, offer).0
        } else {
            offer
        };
        let offer = with_patience(offer);
        // The raw session's offer: the same, under another instance id.
        let mut for_raw = offer.clone();
        for_raw.instance_id = native::random_uuid();
        (
            native::connect(Connect {
                endpoint: endpoint.clone(),
                offer,
                handlers,
                max_message: None,
            }),
            for_raw,
        )
    };
    let (mut opened, mut for_raw) = open(&endpoint);
    if let Err(why) = &opened
        && args.tls_fallback
        && tls_failure.is_none()
        && is_certificate_error(&why.to_string())
    {
        tls_failure = Some(why.to_string());
        certificates = CertificateCheck::Skip;
        endpoint.certificates = certificates;
        (opened, for_raw) = open(&endpoint);
    }
    let tls_finding = |verdict: Verdict, said: String| Finding {
        id: "tls.chain",
        section: "§4.1.1 §a",
        rule: "A server supporting TLS MUST support v1.2 or greater, with a certificate a client can verify - the chain served in full",
        protocol: None,
        verdict,
        said,
    };
    let client = match opened {
        Ok(client) => {
            report.findings.push(match &tls_failure {
                Some(why) => {
                    report.notes.push(format!(
                        "The server's certificate could not be verified ({why}); with --tls-fallback the rest of the run went on without checking it."
                    ));
                    tls_finding(Verdict::Fail, format!("{why} - the run went on without checking the certificate (--tls-fallback)"))
                }
                None if args.insecure => tls_finding(Verdict::NotChecked, "--insecure: the certificate was not checked".into()),
                None => tls_finding(Verdict::Pass, "the handshake verified the certificate".into()),
            });
            client
        }
        Err(why) => {
            let text = why.to_string();
            let chain = is_certificate_error(&text);
            report.findings.push(Finding {
                id: if chain { "tls.chain" } else { "core.open-session" },
                section: if chain { "§4.1.1 §a" } else { "§5.2.1.1" },
                rule: if chain {
                    "A server supporting TLS serves a certificate a client can verify - the chain in full, intermediates included"
                } else {
                    "A client that connects and sends RequestSession is answered with OpenSession, or an error that says why"
                },
                protocol: None,
                verdict: Verdict::Fail,
                said: if chain && !args.tls_fallback { format!("{text} - `--tls-fallback` would run the rest without checking it") } else { text },
            });
            return finish(&report, args.out.as_deref(), args.json.as_deref());
        }
    };

    // The checks run on a session opened again after a request times out
    // (`renewing`), and the report is written as it goes: a run stopped from
    // outside leaves what it had seen.
    let again = endpoint.clone();
    let mut session = Renewing::new(client, native::Reopen(|| open(&again).0));
    let before = report.clone();
    let mut progress = |partial: &Report| {
        let mut so_far = before.clone();
        absorb(&mut so_far, partial.clone());
        let _ = write_reports(&so_far, args.out.as_deref(), args.json.as_deref());
    };
    let checked = checks::run_with(&mut session, &options, &args.url, &mut progress);
    absorb(&mut report, checked);
    let client = session.into_inner();
    if let Err(why) = client.close("conformance checked") {
        report.findings.push(Finding {
            id: "core.close-session",
            section: "§5.2.1.2",
            rule: "The receiver of CloseSession MUST terminate the session and do a clean shutdown of the WebSocket",
            protocol: None,
            verdict: Verdict::Fail,
            said: format!("{why}"),
        });
    } else {
        report.findings.push(Finding {
            id: "core.close-session",
            section: "§5.2.1.2",
            rule: "The receiver of CloseSession MUST terminate the session and do a clean shutdown of the WebSocket",
            protocol: None,
            verdict: Verdict::Pass,
            said: "closed cleanly".into(),
        });
    }
    // --- A wire of its own, for the faults a client never sends -------------------
    let settings = HandshakeSettings {
        max_message: None,
        max_frame: None,
        authorization: endpoint.credentials.clone(),
    };
    let _ = write_reports(&report, args.out.as_deref(), args.json.as_deref());
    match handshake(&args.url, Profile::Native, &settings) {
        Ok(shake) => match WebSocketTransport::connect(&shake, certificates) {
            Ok(transport) => report.findings.extend(raw::checks(
                transport,
                SystemClock,
                for_raw,
                endpoint.credentials.clone(),
            )),
            Err(why) => report.findings.push(Finding {
                id: "raw.session",
                section: "§5.2.1.1",
                rule: "A second WebSocket opens for the faults a client never sends",
                protocol: None,
                verdict: Verdict::NotChecked,
                said: format!("the second connection failed: {why}"),
            }),
        },
        Err(why) => report.findings.push(Finding {
            id: "raw.session",
            section: "§4.3",
            rule: "The WebSocket handshake",
            protocol: None,
            verdict: Verdict::NotChecked,
            said: format!("{why}"),
        }),
    }
    // --- The credentials in Authorize instead of the handshake's headers -------
    // §4.3.2: a client that cannot set headers - a front end - opens the WebSocket
    // with its attributes in the query string and sends its credentials in
    // an `Authorize` (§5.3.4) once connected. Tried on every server, with the
    // same credentials: what a front end could do with it.
    report.findings.push(match &endpoint.credentials {
        None => Finding {
            id: "core.authorize",
            section: "§4.3.2, §5.3.4",
            rule: "A session opens with the credentials sent in Authorize, as a client that cannot set headers sends them",
            protocol: None,
            verdict: Verdict::NotApplicable,
            said: "no credentials to send".into(),
        },
        Some(_) => {
            let mut browser = endpoint.clone();
            browser.handshake = Profile::Browser;
            match open(&browser).0 {
                Ok(other) => {
                    let _ = other.close("conformance checked: Authorize");
                    Finding {
                        id: "core.authorize",
                        section: "§4.3.2, §5.3.4",
                        rule: "A session opens with the credentials sent in Authorize, as a client that cannot set headers sends them",
                        protocol: None,
                        verdict: Verdict::Pass,
                        said: "the session opened with the ETP attributes in the query string and the credentials in Authorize".into(),
                    }
                }
                Err(why) => Finding {
                    id: "core.authorize",
                    section: "§4.3.2, §5.3.4",
                    rule: "A session opens with the credentials sent in Authorize, as a client that cannot set headers sends them",
                    protocol: None,
                    verdict: Verdict::Fail,
                    said: format!("{why}"),
                },
            }
        }
    });
    finish(&report, args.out.as_deref(), args.json.as_deref())
}

/// The same checks over the Reservoir DDMS REST API.
fn over_rest(args: &Args, endpoint: Endpoint, mut report: Report, options: Options) -> Result<ExitCode> {
    for (id, section, rule) in [
        (
            "well-known.reachable",
            "§4.3.1",
            "The ServerCapabilities document",
        ),
        (
            "raw.unsupported-protocol",
            "§5.2.2 §4",
            "The faults a client never sends",
        ),
    ] {
        report.findings.push(Finding {
            id,
            section,
            rule,
            protocol: None,
            verdict: Verdict::NotApplicable,
            said: "REST: the API has no counterpart".into(),
        });
    }
    let insecure = args.insecure;
    let mut remote = Remote::Rest(Box::new(native::rest_client(&endpoint)));
    // The first request is the TLS handshake's witness, as the WebSocket's
    // opening is over ETP.
    match retp_conformance::Session::ping(&mut remote) {
        Ok(_) => report.findings.push(Finding {
            id: "tls.chain",
            section: "§4.1.1 §a",
            rule: "A server supporting TLS serves a certificate a client can verify - the chain in full",
            protocol: None,
            verdict: if insecure {
                Verdict::NotChecked
            } else {
                Verdict::Pass
            },
            said: if insecure {
                "--insecure: the certificate was not checked".into()
            } else {
                "the handshake verified the certificate".into()
            },
        }),
        Err(why) => {
            let text = why.to_string();
            if is_certificate_error(&text) {
                report.findings.push(Finding {
                    id: "tls.chain",
                    section: "§4.1.1 §a",
                    rule: "A server supporting TLS serves a certificate a client can verify",
                    protocol: None,
                    verdict: Verdict::Fail,
                    said: text,
                });
                return finish(&report, args.out.as_deref(), args.json.as_deref());
            }
            // This client speaks the REST API OSDU defines (Reservoir DDMS);
            // a server with another REST API is not at fault for not
            // answering it.
            return rest_not_connected(report, args, &text);
        }
    }
    // A health route answers on many REST APIs; the dataspaces are the
    // Reservoir DDMS's own. When they do not come, the server speaks another
    // API, and checking it against this one would judge this client: one
    // line, the server's words.
    let listed = retp_client::list_dataspaces(&mut remote);
    let refusal = match &listed {
        Ok(list) => list
            .ended_by
            .as_ref()
            .map(|error| format!("{} ({})", error.message, error.code)),
        Err(why) => Some(why.to_string()),
    };
    if let Some(text) = refusal {
        return rest_not_connected(
            report,
            args,
            &format!("the health route answered, the dataspaces did not: {text}"),
        );
    }
    let session = checks::run(&mut remote, &options, &args.url);
    absorb(&mut report, session);
    finish(&report, args.out.as_deref(), args.json.as_deref())
}

/// The one line a REST run says when this client could not talk to the
/// server's API - perhaps because of the client - and no verdict against it.
fn rest_not_connected(mut report: Report, args: &Args, text: &str) -> Result<ExitCode> {
    report.notes.push(format!(
        "Could not connect through the REST API - perhaps because this client speaks OSDU's Reservoir DDMS REST API and the server another one. The server said: {text}"
    ));
    report.findings.push(Finding {
        id: "rest.connect",
        section: "Reservoir DDMS REST API",
        rule: "The client connects through the REST API",
        protocol: None,
        verdict: Verdict::NotChecked,
        said: format!("could not connect, perhaps because of this client: {text}"),
    });
    finish(&report, args.out.as_deref(), args.json.as_deref())
}

/// What the session's checks found, into the report the command began.
fn absorb(report: &mut Report, session: Report) {
    report.server = session.server;
    report.protocols = session.protocols;
    report.data_objects = session.data_objects;
    report.findings.extend(session.findings);
    report.notes.extend(session.notes);
    report.requests = session.requests;
    report.uploads = session.uploads;
    report.exchanges = session.exchanges;
}

/// How long the suite waits for an answer, in seconds.
///
/// `ResponseTimeoutPeriod` is the waiting side's own capability (§3.3.2.3),
/// 300 s by default: some servers answer a `Ping` with a `Pong` correlated to 0, so the
/// Ping waited five minutes, the run outlasted the folder script's limit,
/// and no report was written. A minute is a long time for an
/// answer, and the suite says what did not come.
const RESPONSE_PATIENCE: i64 = 60;

/// The offer, announcing [`RESPONSE_PATIENCE`].
fn with_patience(mut offer: retp_proto::negotiate::Offer) -> retp_proto::negotiate::Offer {
    if let Ok(endpoint) = offer.endpoint.clone().set(
        retp_proto::capabilities::names::RESPONSE_TIMEOUT_PERIOD,
        retp_proto::capabilities::Value::Long(RESPONSE_PATIENCE),
    ) {
        offer.endpoint = endpoint;
    }
    offer
}

/// The run's options, from the command line.
fn options_of(args: &Args) -> Result<Options> {
    let mut custom_data = std::collections::BTreeMap::new();
    for entry in &args.dataspace_custom {
        let (key, value) = entry
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("--dataspace-custom takes KEY=VALUE, not `{entry}`"))?;
        custom_data.insert(key.to_string(), retp_client::custom_data_value(value));
    }
    Ok(Options {
        write: args.write,
        dataspace: args.dataspace.clone(),
        write_dataspace: args.write_dataspace.clone(),
        custom_data,
        write_fallback: args.write_fallback.clone(),
    })
}

/// Whether a connection failed on the server's certificate - what
/// `--tls-fallback` steps past.
fn is_certificate_error(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("certificate") || lower.contains("unknownissuer") || lower.contains("invalid peer")
}

/// The report out, and the exit code: 1 when anything failed.
fn finish(
    report: &Report,
    out: Option<&std::path::Path>,
    json: Option<&std::path::Path>,
) -> Result<ExitCode> {
    write_reports(report, out, json)?;
    match out {
        Some(path) => {
            eprintln!("{}", report);
            println!("{}", path.display());
        }
        None => print!("{}", report.markdown()),
    }
    Ok(if report.has_failures() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}

/// The report into its files - at the end, and as the run goes.
fn write_reports(
    report: &Report,
    out: Option<&std::path::Path>,
    json: Option<&std::path::Path>,
) -> Result<()> {
    if let Some(path) = json {
        std::fs::write(path, serde_json::to_string_pretty(&report.json())?)
            .with_context(|| format!("writing {}", path.display()))?;
    }
    match out {
        Some(path) if path.extension().is_some_and(|e| e.eq_ignore_ascii_case("json")) => {
            std::fs::write(path, serde_json::to_string_pretty(&report.json())?)
                .with_context(|| format!("writing {}", path.display()))?;
        }
        Some(path) => {
            std::fs::write(path, report.markdown()).with_context(|| format!("writing {}", path.display()))?;
        }
        None => {}
    }
    Ok(())
}
