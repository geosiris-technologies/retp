//! A push's words, as a Python caller gives them, and its report, as the
//! bridge flattens it - with no server: what a push does is the library's,
//! tested in `energyml-etp-store`; what is tested here is the conversion.

use std::path::PathBuf;
use std::time::Duration;

use std::collections::BTreeMap;

use energyml_etp_store::push::{
    Cause, Island, Options, Plan, Refusal, Report, Said, SkipReason, Skipped, Split, SplitIsland, Stop,
    StopKind, Stopped,
};
use retp_client::resume::Came;
use retp_python::session::{Kind, PushChoices, PushPlan, PushReport, parse_split, pause_of, plan_epc};

/// `split=` takes the command line's two words, and `None` is no split; any
/// other word is refused by name, with the words it could have been.
#[test]
fn split_takes_the_command_lines_words() {
    assert_eq!(parse_split(None).unwrap(), Split::Off);
    assert_eq!(parse_split(Some("halve")).unwrap(), Split::Halve);
    assert_eq!(parse_split(Some("single")).unwrap(), Split::Single);
    let refused = parse_split(Some("sideways")).unwrap_err();
    assert_eq!(refused.kind, Kind::BadArgument);
    assert!(
        refused.message.contains("sideways") && refused.message.contains("halve"),
        "{}",
        refused.message
    );
}

/// `reconnect_pause=` is seconds; a pause that is not one is refused by
/// name, not taken for no pause.
#[test]
fn a_reconnect_pause_that_is_no_pause_is_refused() {
    assert_eq!(pause_of(None).unwrap(), None);
    assert_eq!(pause_of(Some(0.5)).unwrap(), Some(Duration::from_millis(500)));
    for bad in [-1.0, f64::NAN, f64::INFINITY] {
        let refused = pause_of(Some(bad)).unwrap_err();
        assert_eq!(refused.kind, Kind::BadArgument, "{bad}");
    }
}

/// The words reach the library's options under the same meaning, and none
/// given is the library's default - a push as it always went.
#[test]
fn the_push_words_are_the_librarys_options() {
    let registry = energyml_schemas::all();
    let plain = PushChoices::default();
    let options = plain.options(&registry, None);
    let default = Options::default();
    assert!(!options.skip_present && options.journal.is_none());
    assert_eq!(options.split, default.split);
    assert_eq!(options.split_limit, default.split_limit);
    assert_eq!(plain.reconnect, 0, "no reconnection unless asked");

    let asked = PushChoices {
        skip_present: true,
        journal: Some(PathBuf::from("push.jsonl")),
        split: Split::Single,
        split_limit: Some(3),
        ..PushChoices::default()
    };
    let options = asked.options(&registry, None);
    assert!(options.skip_present);
    assert_eq!(options.journal, Some(std::path::Path::new("push.jsonl")));
    assert_eq!(options.split, Split::Single);
    assert_eq!(options.split_limit, 3);
}

/// A journal that names no file is refused before anything is read.
#[test]
fn a_journal_that_names_no_file_is_refused() {
    let choices = PushChoices {
        journal: Some(PathBuf::new()),
        ..PushChoices::default()
    };
    assert_eq!(choices.check().unwrap_err().kind, Kind::BadArgument);
}

/// What was skipped, the splits and the new sessions cross as the library
/// said them; a push that gave up says so, and says the island's outcome is
/// unknown.
#[test]
fn the_report_carries_what_was_skipped_split_and_reopened() {
    let refusal = Refusal {
        uri: "eml:///dataspace('d')/resqml20.obj_LocalDepth3dCrs(1)".into(),
        array: None,
        code: Some(6),
        message: "too many objects at once".into(),
    };
    let report = Report {
        islands: vec![
            Island {
                uris: vec!["a".into()],
                bytes: 1,
                arrays: 0,
                units: vec![1],
            },
            Island {
                uris: vec!["b".into()],
                bytes: 1,
                arrays: 0,
                units: vec![1],
            },
        ],
        sent: 1,
        stopped: Some(Stopped {
            island: 1,
            why: Stop::GaveUp {
                attempts: 3,
                last: "the connection fell".into(),
            },
            said: None,
        }),
        skipped: vec![
            Skipped {
                uri: "eml:///dataspace('d')/x.A(1)".into(),
                why: SkipReason::Present,
            },
            Skipped {
                uri: "eml:///dataspace('d')/x.B(2)".into(),
                why: SkipReason::Journal,
            },
        ],
        splits: vec![SplitIsland {
            island: 0,
            uris: vec!["a".into(), "b".into()],
            into: 2,
            why: "too many objects at once".into(),
            refusals: vec![refusal.clone()],
        }],
        reconnections: 2,
        reopened: vec![
            Came::Resumed,
            Came::Fresh {
                refused: Some("ENOT_FOUND: Session not found or expired (11)".into()),
            },
        ],
        ..Report::default()
    };
    let flat = PushReport::of(report);
    assert_eq!(
        flat.skipped,
        [
            ("eml:///dataspace('d')/x.A(1)".to_string(), "present"),
            ("eml:///dataspace('d')/x.B(2)".to_string(), "journal"),
        ]
    );
    assert_eq!(flat.splits.len(), 1);
    assert_eq!(flat.splits[0].refusals, [refusal]);
    assert_eq!(flat.reconnections, 2);
    assert_eq!(
        flat.reopened,
        [
            Came::Resumed,
            Came::Fresh {
                refused: Some("ENOT_FOUND: Session not found or expired (11)".into()),
            },
        ],
        "how each new session came back crosses as the library said it"
    );
    assert_eq!(flat.islands[0].state, "committed");
    assert_eq!(flat.islands[1].state, "refused");
    let stopped = flat.stopped.as_deref().unwrap();
    assert!(
        stopped.contains("island 2") && stopped.contains("3 time(s)") && stopped.contains("unknown"),
        "{stopped}"
    );
    let stop = flat.stop.as_ref().expect("the stop, sorted");
    assert_eq!((stop.island, stop.kind), (1, StopKind::GaveUp));
    assert_eq!(stop.message, "the connection fell");
    assert_eq!(stop.cause, None, "a stop that is no refusal names no cause");
    assert!(!flat.complete());
}

/// **The sorted stop crosses as the library sorted it**, beside the
/// sentence: a refused commit's own words, without the hint the sentence
/// carries, their cause, and the candidates of the island that stopped -
/// never those of another island.
#[test]
fn the_report_carries_the_stop_sorted_beside_the_sentence() {
    let report = Report {
        islands: vec![
            Island {
                uris: vec!["crs".into()],
                units: vec![1],
                ..Island::default()
            },
            Island {
                uris: vec!["rep".into(), "prop".into()],
                units: vec![1, 1],
                ..Island::default()
            },
        ],
        sent: 1,
        stopped: Some(Stopped {
            island: 1,
            why: Stop::Refused(
                "the commit was refused: 2 Missing reference(s) - 3 reference(s) leave the source (...)"
                    .into(),
            ),
            said: Some(Said {
                code: None,
                message: "2 Missing reference(s)".into(),
            }),
        }),
        dangling: vec![
            ("crs".into(), "elsewhere-0".into()),
            ("prop".into(), "kind".into()),
            ("rep".into(), "elsewhere-1".into()),
        ],
        ..Report::default()
    };
    let flat = PushReport::of(report);
    assert!(flat.stopped.as_deref().unwrap().contains("leave the source"));
    let stop = flat.stop.as_ref().unwrap();
    assert_eq!(stop.kind, StopKind::Refused);
    assert_eq!(stop.message, "2 Missing reference(s)", "the store's words alone");
    assert_eq!(
        stop.cause,
        Some(Cause::MissingReference {
            count: 2,
            space: None,
            named: Vec::new(),
            more: 0,
        })
    );
    assert_eq!(
        stop.candidates,
        [
            ("prop".to_string(), "kind".to_string()),
            ("rep".to_string(), "elsewhere-1".to_string()),
        ],
        "only what leaves the island that stopped"
    );
}

/// **A plan crosses with its islands in the rows of a report**, `planned`,
/// each with its destination URIs in the order of its source URIs, its
/// units and its cycles.
#[test]
fn a_plan_crosses_with_its_destinations_and_its_cycles() {
    let plan = Plan {
        dataspace: "eml:///dataspace('d')".into(),
        islands: vec![Island {
            uris: vec!["a".into(), "b".into(), "c".into()],
            bytes: 30,
            arrays: 0,
            units: vec![2, 1],
        }],
        destinations: [
            ("a".to_string(), "eml:///dataspace('d')/x.A(1)".to_string()),
            ("b".to_string(), "eml:///dataspace('d')/x.B(2)".to_string()),
            ("c".to_string(), "eml:///dataspace('d')/x.C(3)".to_string()),
        ]
        .into_iter()
        .collect::<BTreeMap<_, _>>(),
        skipped: vec![Skipped {
            uri: "eml:///dataspace('d')/x.D(4)".into(),
            why: SkipReason::Journal,
        }],
        ..Plan::default()
    };
    let flat = PushPlan::of(plan);
    assert_eq!(flat.dataspace, "eml:///dataspace('d')");
    let island = &flat.islands[0];
    assert_eq!(island.state, "planned");
    assert_eq!(island.objects, 3);
    assert_eq!(island.units, [2, 1]);
    assert_eq!(island.cycles, [vec!["a".to_string(), "b".to_string()]]);
    assert_eq!(
        island.destinations.as_deref().unwrap(),
        [
            "eml:///dataspace('d')/x.A(1)",
            "eml:///dataspace('d')/x.B(2)",
            "eml:///dataspace('d')/x.C(3)"
        ]
    );
    assert_eq!(
        flat.skipped,
        [("eml:///dataspace('d')/x.D(4)".to_string(), "journal")]
    );
}

/// **A plan needs no session, and refuses what needs one**: a package that
/// is not there is a local failure; `skip_present` is refused by the
/// library, by name, as a bad argument - before the package is read.
#[test]
fn a_plan_refuses_what_needs_the_destination() {
    let missing = std::env::temp_dir().join(format!("retp-no-such-package-{}.epc", std::process::id()));
    let missing = missing.to_str().unwrap();
    let refused = plan_epc(missing, "demo", &PushChoices::default()).unwrap_err();
    assert_eq!(refused.kind, Kind::Local, "{}", refused.message);

    let skipping = PushChoices {
        skip_present: true,
        ..PushChoices::default()
    };
    let empty = energyml_storage::extract::empty_package(None).unwrap();
    let registry = std::sync::Arc::new(energyml_schemas::all());
    let store = energyml_storage::EpcStore::from_reader(
        Box::new(std::io::Cursor::new(empty)),
        "the plan's tests",
        registry.clone(),
    )
    .unwrap();
    let refused = retp_python::session::plan_store(&store, &registry, "demo", &skipping).unwrap_err();
    assert_eq!(refused.kind, Kind::BadArgument);
    assert!(refused.message.contains("skip_present"), "{}", refused.message);

    // The same empty package planned without it: nothing to send, said.
    let plan = retp_python::session::plan_store(&store, &registry, "demo", &PushChoices::default()).unwrap();
    assert!(plan.islands.is_empty() && plan.left_out.is_empty());
    assert_eq!(plan.dataspace, "eml:///dataspace('demo')");
}

/// `resume_session=` is the library's `Resuming::ask`, off unless given;
/// without `reconnect` it would mean nothing, and is refused by name.
#[test]
fn resume_session_is_the_connectors_ask_and_needs_reconnect() {
    let open = || Ok::<(), ()>(());
    let resume = |_lost: retp_proto::connection::Resumable| Ok::<(), ()>(());

    let plain = PushChoices {
        reconnect: 2,
        ..PushChoices::default()
    };
    assert!(!plain.resume_session, "off unless asked");
    assert!(!plain.resuming(open, resume).ask);
    assert!(plain.check().is_ok());

    let asked = PushChoices {
        reconnect: 2,
        resume_session: true,
        ..PushChoices::default()
    };
    assert!(asked.resuming(open, resume).ask);
    assert!(asked.check().is_ok());

    let alone = PushChoices {
        resume_session: true,
        ..PushChoices::default()
    };
    let refused = alone.check().unwrap_err();
    assert_eq!(refused.kind, Kind::BadArgument);
    assert!(refused.message.contains("reconnect"), "{}", refused.message);
}
