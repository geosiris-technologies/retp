//! `subscribe`, `subscriptions` and `unsubscribe`: notifications on the live session of the
//! shell.
//!
//! A subscription is a store notification subscription with a destination: the console of the
//! shell, or a file (JSON Lines, or the console's words). What arrives is formatted by the
//! destinations of `retp_client::live`, never here. Only a live ETP session can subscribe;
//! the REST API sends no notifications, and the blocking client has nothing to listen with
//! between two commands.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Result, anyhow, bail};
use clap::{Args, ValueEnum};
use retp_client::live::{Console, Destination, JsonLines, RouteId};
use retp_client::native;
use retp_client::notifications::{Watch, uuid_text};
use retp_proto::connection::Event;

use super::Scope;

/// How a file destination writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Format {
    /// One JSON object per line (the Avro JSON encoding of the message).
    Jsonl,
    /// One human line per arrival, as the console prints it.
    Text,
}

/// What `subscribe` takes: the same context options as `watch`, and where to send what comes.
#[derive(Args)]
pub struct SubscribeArgs {
    /// The context: a dataspace, or an object, by URI.
    pub uri: String,
    /// How far from it in the graph.
    #[arg(long, default_value_t = 1)]
    pub depth: i32,
    /// Which side of the graph.
    #[arg(long, value_enum, default_value = "targets-or-self")]
    pub scope: Scope,
    /// Only these types (`resqml20.*`); every type otherwise.
    #[arg(long, value_name = "QUALIFIED_TYPE")]
    pub types: Vec<String>,
    /// Have each change carry the object's data, not only its resource.
    #[arg(long)]
    pub with_data: bool,
    /// Where to send what arrives: `console` (above the prompt), or a file path.
    #[arg(long, value_name = "PATH|console", default_value = "console")]
    pub to: String,
    /// How a file is written; JSON Lines by default. The console is always text.
    #[arg(long, value_enum)]
    pub format: Option<Format>,
    /// Append to an existing file instead of truncating it.
    #[arg(long)]
    pub append: bool,
}

/// What `unsubscribe` takes.
#[derive(Args)]
pub struct UnsubscribeArgs {
    /// The subscription's uuid, as `subscribe` printed it.
    pub id: String,
}

/// Where a subscription sends what arrives, as a value (what `--to` and `--format` say).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Target {
    /// The shell's console.
    Console,
    /// A file, written in `format`; `append` keeps what the file held.
    File {
        /// The file.
        path: PathBuf,
        /// How it is written.
        format: Format,
        /// Whether an existing file is kept.
        append: bool,
    },
}

impl Target {
    /// Read `--to`, `--format` and `--append`. The console takes no JSON Lines and no
    /// `--append`: refused by name rather than ignored.
    pub fn of(args: &SubscribeArgs) -> Result<Target> {
        if args.to == "console" {
            if args.format == Some(Format::Jsonl) {
                bail!("the console is always text: `--format jsonl` needs `--to <PATH>`");
            }
            if args.append {
                bail!("`--append` needs `--to <PATH>`");
            }
            return Ok(Target::Console);
        }
        if args.to.is_empty() {
            bail!("`--to` is empty: give a path, or `console`");
        }
        Ok(Target::File {
            path: PathBuf::from(&args.to),
            format: args.format.unwrap_or(Format::Jsonl),
            append: args.append,
        })
    }

    /// The target in a few words, as `subscribe` and `subscriptions` print it.
    pub fn describe(&self) -> String {
        match self {
            Target::Console => "console".to_string(),
            Target::File { path, format, .. } => {
                let format = match format {
                    Format::Jsonl => "jsonl",
                    Format::Text => "text",
                };
                format!("{} ({format})", path.display())
            }
        }
    }
}

/// A destination that counts what it hands to the real one.
pub struct Counted {
    inner: Arc<dyn Destination>,
    arrivals: Arc<AtomicU64>,
}

impl Counted {
    /// Wrap `inner`; the counter is shared, to be read while the destination lives in the
    /// session.
    pub fn new(inner: Arc<dyn Destination>) -> (Counted, Arc<AtomicU64>) {
        let arrivals = Arc::new(AtomicU64::new(0));
        (
            Counted {
                inner,
                arrivals: Arc::clone(&arrivals),
            },
            arrivals,
        )
    }
}

impl Destination for Counted {
    fn deliver(&self, event: &Event) -> Result<(), String> {
        self.arrivals.fetch_add(1, Ordering::Relaxed);
        self.inner.deliver(event)
    }

    fn dropped(&self, count: u64) {
        self.inner.dropped(count);
    }
}

/// One subscription the shell keeps.
pub struct Entry {
    /// The subscription's uuid, as text.
    pub uuid: String,
    /// The context it watches.
    pub uri: String,
    /// Where it sends what arrives.
    pub target: Target,
    /// The route of its destination in the session.
    pub route: RouteId,
    arrivals: Arc<AtomicU64>,
}

impl Entry {
    /// How many arrivals the destination has had so far.
    pub fn count(&self) -> u64 {
        self.arrivals.load(Ordering::Relaxed)
    }
}

/// What the shell keeps between commands.
#[derive(Default)]
pub struct Subscriptions {
    entries: Vec<Entry>,
}

impl Subscriptions {
    /// None yet.
    pub fn new() -> Subscriptions {
        Subscriptions::default()
    }

    /// How many are held.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether none is held.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Forget every entry (a new session starts with none: its routes are gone with the old).
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

/// The destination `--to` names: the console given, or a file opened as asked.
fn destination(target: &Target, console: &Arc<dyn Destination>) -> Result<Arc<dyn Destination>> {
    Ok(match target {
        Target::Console => Arc::clone(console),
        Target::File { path, format, append } => match format {
            Format::Jsonl => Arc::new(
                if *append {
                    JsonLines::append(path)
                } else {
                    JsonLines::create(path)
                }
                .map_err(|e| anyhow!("cannot open `{}`: {e}", path.display()))?,
            ),
            Format::Text => {
                let file = if *append {
                    std::fs::OpenOptions::new().create(true).append(true).open(path)
                } else {
                    std::fs::File::create(path)
                }
                .map_err(|e| anyhow!("cannot open `{}`: {e}", path.display()))?;
                Arc::new(Console::new(Box::new(file)))
            }
        },
    })
}

/// `subscribe`: watch a context and send what comes to a destination, until `unsubscribe`
/// or the end of the session.
pub fn subscribe(
    session: &mut native::Remote,
    args: &SubscribeArgs,
    subs: &mut Subscriptions,
    console: Arc<dyn Destination>,
) -> Result<()> {
    let live = match session {
        native::Remote::Rest(_) => {
            bail!("the REST API sends no notifications: subscribe needs an ETP WebSocket session")
        }
        native::Remote::Etp(_) => {
            bail!("subscribe needs a live session: this ETP session is the blocking client")
        }
        native::Remote::Live(live) => live,
    };
    let target = Target::of(args)?;
    let (counted, arrivals) = Counted::new(destination(&target, &console)?);
    let mut watch = Watch::of(args.uri.clone(), native::random_uuid());
    watch.depth = args.depth;
    watch.scope = args.scope.kind();
    watch.data_object_types = args.types.clone();
    watch.include_object_data = args.with_data;
    let uuid = uuid_text(&watch.request_uuid);
    match live.subscribe_to(&watch, Arc::new(counted))? {
        Ok(route) => {
            println!("subscribed\t{uuid}\t{}\t-> {}", args.uri, target.describe());
            subs.entries.push(Entry {
                uuid,
                uri: args.uri.clone(),
                target,
                route,
                arrivals,
            });
        }
        Err(error) => eprintln!("refused: {} ({})", error.message, error.code),
    }
    Ok(())
}

/// `subscriptions`: one line each (uuid, uri, destination, arrivals), then what the session
/// dropped, if anything.
pub fn subscriptions(session: &native::Remote, subs: &Subscriptions) {
    if subs.is_empty() {
        println!("no subscription");
    }
    for entry in &subs.entries {
        println!(
            "{}\t{}\t{}\t{}",
            entry.uuid,
            entry.uri,
            entry.target.describe(),
            entry.count()
        );
    }
    if let native::Remote::Live(live) = session {
        let dropped = live.dropped();
        if dropped > 0 {
            println!("dropped\t{dropped}");
        }
    }
}

/// `unsubscribe`: end a subscription of this shell, and its route.
pub fn unsubscribe(
    session: &mut native::Remote,
    args: &UnsubscribeArgs,
    subs: &mut Subscriptions,
) -> Result<()> {
    let native::Remote::Live(live) = session else {
        bail!("unsubscribe needs a live ETP session");
    };
    let uuid = retp_client::rest::uuid_of(&args.id)
        .ok_or_else(|| anyhow!("`{}` is not a uuid (8-4-4-4-12 hex)", args.id))?;
    let text = uuid_text(&uuid);
    let Some(at) = subs.entries.iter().position(|entry| entry.uuid == text) else {
        bail!("no subscription {text} in this shell (`subscriptions` lists them)");
    };
    let route = subs.entries[at].route;
    match live.unsubscribe_from(uuid, route)? {
        Ok(reason) => {
            subs.entries.remove(at);
            println!("ended\t{reason}");
        }
        Err(error) => eprintln!("refused: {} ({})", error.message, error.code),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Parser)]
    struct Wrap {
        #[command(flatten)]
        args: SubscribeArgs,
    }

    fn parse(words: &[&str]) -> SubscribeArgs {
        let mut all = vec!["subscribe"];
        all.extend_from_slice(words);
        Wrap::try_parse_from(all).expect("parses").args
    }

    #[test]
    fn defaults_are_the_console_at_depth_one() {
        let args = parse(&["eml:///dataspace('demo')"]);
        assert_eq!(args.depth, 1);
        assert_eq!(args.to, "console");
        assert!(args.types.is_empty() && !args.with_data && !args.append);
        assert_eq!(args.format, None);
        assert_eq!(Target::of(&args).unwrap(), Target::Console);
    }

    #[test]
    fn a_path_is_json_lines_unless_text_is_asked() {
        let args = parse(&["eml:///", "--to", "out.jsonl"]);
        assert_eq!(
            Target::of(&args).unwrap(),
            Target::File {
                path: PathBuf::from("out.jsonl"),
                format: Format::Jsonl,
                append: false
            }
        );
        let args = parse(&["eml:///", "--to", "out.txt", "--format", "text", "--append"]);
        assert_eq!(
            Target::of(&args).unwrap(),
            Target::File {
                path: PathBuf::from("out.txt"),
                format: Format::Text,
                append: true
            }
        );
    }

    #[test]
    fn the_console_refuses_json_lines_and_append() {
        let args = parse(&["eml:///", "--format", "jsonl"]);
        assert!(Target::of(&args).is_err());
        let args = parse(&["eml:///", "--append"]);
        assert!(Target::of(&args).is_err());
        // Text on the console is what it does anyway.
        let args = parse(&["eml:///", "--format", "text"]);
        assert_eq!(Target::of(&args).unwrap(), Target::Console);
    }

    #[test]
    fn options_are_read_as_watch_reads_them() {
        let args = parse(&[
            "eml:///x",
            "--depth",
            "3",
            "--types",
            "resqml20.*",
            "--types",
            "eml20.*",
            "--with-data",
            "--scope",
            "sources",
        ]);
        assert_eq!(args.depth, 3);
        assert_eq!(args.types, vec!["resqml20.*", "eml20.*"]);
        assert!(args.with_data);
        assert!(Wrap::try_parse_from(["subscribe", "eml:///x", "--scope", "nowhere"]).is_err());
    }

    #[test]
    fn target_is_described_for_the_listing() {
        assert_eq!(Target::Console.describe(), "console");
        let file = Target::File {
            path: PathBuf::from("a.jsonl"),
            format: Format::Jsonl,
            append: false,
        };
        assert_eq!(file.describe(), "a.jsonl (jsonl)");
    }

    #[test]
    fn the_wrapper_counts_what_it_hands_on() {
        struct Sink(AtomicU64);
        impl Destination for Sink {
            fn deliver(&self, _: &Event) -> Result<(), String> {
                self.0.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }
        let sink = Arc::new(Sink(AtomicU64::new(0)));
        let (counted, arrivals) = Counted::new(sink.clone());
        for _ in 0..3 {
            counted.deliver(&Event::Opened).unwrap();
        }
        assert_eq!(arrivals.load(Ordering::Relaxed), 3);
        assert_eq!(sink.0.load(Ordering::Relaxed), 3);
    }
}
