//! The checks, over an open session.
//!
//! Every request goes through `ask`, which keeps the headers the server
//! sent ([`Seen`]): the rules on message ids, correlation and the FIN bit
//! are checked over **everything** the run received, not over one request
//! chosen to pass. And every check answers with a [`Finding`] - a check
//! that could not observe says *not checked*, never *pass*.
//!
//! The order is the order a customer works in (§8): the session, then the
//! dataspaces, then a dataspace's content, then one object - each step
//! feeding the next what it found, and saying *not checked* downstream when
//! it found nothing.

use std::collections::BTreeMap;

use retp_avro::EtpMessage;
use retp_client::{
    ClientError, Ended, Opened, commit_transaction, dataspace_uri, delete_data_objects, delete_dataspaces,
    put_data_objects, put_dataspaces, rollback_transaction, start_transaction,
};
use retp_proto::capabilities::names;
use retp_proto::uris::{UriUse, check_uri};
use retp_proto::wire::{Body, Delivery, EtpError, Exception};
use retp_types::datatypes::object::{
    ActiveStatusKind, ContextInfo, ContextScopeKind, DataObject, Dataspace, RelationshipKind, Resource,
};
use retp_types::datatypes::{ErrorInfo, Uuid};
use retp_types::dispatch::Message;
use retp_types::protocol::dataspace::GetDataspaces;
use retp_types::protocol::discovery::GetResources;
use retp_types::protocol::store::GetDataObjects;
use retp_types::protocol::supported_types::GetSupportedTypes;
use retp_types::protocol::transaction::StartTransaction;

use energyml_core::identifiers::EtpUri;
use energyml_core::registry::ReadMode;
use energyml_etp_store::{DEFAULT_MESSAGE, read_object};
use energyml_storage::array::{ProxyTarget, locate_with};
use retp_client::arrays::{Array, ArrayValues, Block, array_metadata, read_arrays, read_regions};
use retp_proto::connection::Event;
use retp_proto::wire::Place;
use retp_types::datatypes::data_array_types::{
    DataArrayIdentifier, DataArrayMetadata, PutDataSubarraysType, PutUninitializedDataArrayType,
};
use retp_types::datatypes::{AnyArray, AnyArrayItem, AnyLogicalArrayType, ArrayOfDouble};
use retp_types::protocol::data_array::{GetDataArrayMetadata, PutDataSubarrays, PutUninitializedDataArrays};

use crate::samples::{self, Sample};
use crate::{CallOutcome, Finding, Options, Report, Seen, Session, Upload, Verdict, describe};

/// A uuid nobody holds: what *not found* is asked with.
const NOBODY: &str = "0f0e0d0c-0b0a-4908-8706-050403020100";

/// Run every check over an open session and write the report.
pub fn run<K: Session>(client: &mut K, options: &Options, url: &str) -> Report {
    run_with(client, options, url, &mut |_| {})
}

/// [`run`], handing the report as it stands to `progress` before each part:
/// a run stopped from outside - a server that never answers - leaves what it
/// had seen, instead of nothing.
pub fn run_with<K: Session>(
    client: &mut K,
    options: &Options,
    url: &str,
    progress: &mut dyn FnMut(&Report),
) -> Report {
    let negotiated = client.session();
    let rest = client.is_rest();
    let (server, protocols) = describe(&negotiated);
    let mut report = Report {
        server,
        url: url.to_string(),
        protocols: protocols.clone(),
        data_objects: negotiated
            .data_objects
            .iter()
            .map(|one| one.qualified_type.clone())
            .collect(),
        ..Default::default()
    };
    let mut seen = Seen::default();
    let has = |protocol: i32| negotiated.protocols.contains_key(&protocol);

    // --- Core: what the opening said -------------------------------------
    let nil = negotiated.session_id == Uuid([0; 16]);
    report.findings.push(Finding::new(
        "core.open-session.session-id",
        "§5.2.2",
        "A server MUST assign a unique ID (a UUID) to an ETP session",
        None,
        if rest {
            Verdict::NotApplicable
        } else if nil {
            Verdict::Fail
        } else {
            Verdict::Pass
        },
        if rest {
            "REST: no ETP session is opened".to_string()
        } else if nil {
            "the sessionId is the nil UUID".to_string()
        } else {
            format!("sessionId {}", hex(&negotiated.session_id))
        },
    ));
    report.findings.push(Finding::new(
        "core.open-session.protocols",
        "§5.2.2, §5.3.2",
        "OpenSession lists a subset of the requested protocols, at the requested versions and roles, without Core",
        None,
        Verdict::Pass,
        format!("{} protocol(s) agreed: {:?}", protocols.len(), protocols),
    ));
    let empty_objects = negotiated.data_objects.is_empty();
    report.findings.push(Finding::new(
        "core.open-session.data-objects",
        "§5.2.2",
        "The server lists the data objects it supports; an empty list must refuse the session with ENOSUPPORTEDDATAOBJECTTYPES",
        None,
        if rest { Verdict::NotApplicable } else if empty_objects { Verdict::Fail } else { Verdict::Pass },
        if rest { "REST: no OpenSession to list them in".to_string() } else { format!("{} data object type(s) agreed", negotiated.data_objects.len()) },
    ));
    let has_xml = negotiated.formats.iter().any(|f| f.eq_ignore_ascii_case("xml"));
    report.findings.push(Finding::new(
        "core.open-session.formats",
        "§5.3.1, §5.3.2",
        "supportedFormats MUST be populated, and `xml` is the format every store supports",
        None,
        if has_xml { Verdict::Pass } else { Verdict::Fail },
        format!("formats agreed: {:?}", negotiated.formats),
    ));
    for deviation in &negotiated.deviations {
        report.findings.push(Finding::new(
            "core.open-session.deviation",
            "§5.3.2",
            deviation.rule,
            None,
            Verdict::Tolerated,
            format!("{} - taken as: {}", deviation.what, deviation.taken_as),
        ));
    }

    report.exchanges.extend(client.exchanges());
    progress(&report);
    // --- Core: Ping / Pong ---------------------------------------------------
    match client.ping() {
        Ok(took) => report.findings.push(Finding::new(
            "core.ping-pong",
            "§5.2.1, §5.3.6",
            "The endpoint that receives a Ping MUST respond with a Pong, correlated to it",
            None,
            Verdict::Pass,
            if rest {
                format!("REST: the API's health answered in {} ms", took.as_millis())
            } else {
                format!("Pong in {} ms", took.as_millis())
            },
        )),
        Err(why) => report.findings.push(Finding::new(
            "core.ping-pong",
            "§5.2.1, §5.3.6",
            "The endpoint that receives a Ping MUST respond with a Pong, correlated to it",
            None,
            Verdict::Fail,
            format!("no Pong: {why}"),
        )),
    }

    report.exchanges.extend(client.exchanges());
    progress(&report);
    // --- Dataspaces (24) --------------------------------------------------------
    let mut spaces: Vec<Dataspace> = Vec::new();
    if has(24) {
        match ask(
            client,
            &mut seen,
            &GetDataspaces {
                store_last_write_filter: None,
            },
        ) {
            Ok(answer) => {
                for delivery in &answer {
                    if let Body::Message(Message::GetDataspacesResponse(r)) = &delivery.body {
                        spaces.extend(r.dataspaces.iter().cloned());
                    }
                }
                match exception_of(&answer) {
                    Some(said) => report.findings.push(Finding::new(
                        "dataspace.list",
                        "§21.2.1",
                        "The store MUST answer GetDataspaces with its dataspaces",
                        Some(24),
                        Verdict::Fail,
                        format!("refused: {said}"),
                    )),
                    None => {
                        let bad: Vec<&str> = spaces
                            .iter()
                            .filter(|space| check_uri(&space.uri, UriUse::CanonicalOnly, false).is_err())
                            .map(|space| space.uri.as_str())
                            .collect();
                        report.findings.push(Finding::new(
                            "dataspace.list",
                            "§21.2.1, §3.7.4",
                            "Every dataspace comes with its canonical URI",
                            Some(24),
                            if bad.is_empty() {
                                Verdict::Pass
                            } else {
                                Verdict::Fail
                            },
                            if bad.is_empty() {
                                format!(
                                    "{} dataspace(s): {:?}",
                                    spaces.len(),
                                    spaces.iter().map(|s| s.path.as_str()).collect::<Vec<_>>()
                                )
                            } else {
                                format!("URIs that are not canonical: {bad:?}")
                            },
                        ));
                        report.findings.push(timestamps(
                            "dataspace.timestamps",
                            Some(24),
                            spaces
                                .iter()
                                .map(|s| (s.uri.as_str(), s.store_created, s.store_last_write)),
                        ));
                    }
                }
            }
            Err(why) => report.findings.push(failed(
                "dataspace.list",
                "§21.2.1",
                "The store MUST answer GetDataspaces",
                Some(24),
                why,
            )),
        }
    } else {
        report
            .findings
            .push(not_applicable("dataspace.list", "§21", Some(24)));
    }

    // Which dataspace to look in: the one asked for, the first listed, and when none
    // is listed - or Dataspace (24) is not spoken - the default one, which a
    // store MUST support (§21.2.2 row 1, §8.2.2 « "eml:///" refers to the
    // default dataspace … A default dataspace ALWAYS exists, but it might be
    // empty »). A WITSML store keeps its wells there.
    let chosen: Option<String> = match &options.dataspace {
        Some(asked) => Some(dataspace_uri(asked)),
        None => Some(
            spaces
                .first()
                .map(|s| s.uri.clone())
                .unwrap_or_else(|| "eml:///".to_string()),
        ),
    };

    report.exchanges.extend(client.exchanges());
    progress(&report);
    // --- Discovery (3) ------------------------------------------------------------
    let mut resources: Vec<Resource> = Vec::new();
    if has(3) {
        // A malformed URI, and one that names nothing: the two refusals §8.2.1
        // names, with their codes.
        expect_error(
            client,
            &mut seen,
            &mut report,
            "discovery.invalid-uri",
            "§8.2.1",
            "If the request contains a malformed URI, the store MUST send error EINVALID_URI (9)",
            Some(3),
            &resources_of("this is not a uri", 1),
            EtpError::EINVALID_URI,
        );
        expect_error(
            client,
            &mut seen,
            &mut report,
            "discovery.not-found",
            "§8.2.1",
            "If the dataspace or data object does not exist, the store MUST send error ENOT_FOUND (11)",
            Some(3),
            &resources_of("eml:///dataspace('energyml-conformance-nobody-has-this')", 1),
            EtpError::ENOT_FOUND,
        );
        match &chosen {
            Some(space) => match listing_with_filter_fallback(client, &mut seen, &mut report, space) {
                Ok(answer) => {
                    for delivery in &answer {
                        if let Body::Message(Message::GetResourcesResponse(r)) = &delivery.body {
                            resources.extend(r.resources.iter().cloned());
                        }
                    }
                    match exception_of(&answer) {
                        Some(said) => report.findings.push(Finding::new(
                            "discovery.dataspace",
                            "§8.2.1",
                            "The store MUST answer GetResources on a dataspace with its resources, an empty array when it holds none",
                            Some(3),
                            Verdict::Fail,
                            format!("{space}: refused: {said}"),
                        )),
                        None => {
                            let bad: Vec<&str> = resources
                                .iter()
                                .filter(|r| check_uri(&r.uri, UriUse::CanonicalOnly, false).is_err())
                                .map(|r| r.uri.as_str())
                                .take(5)
                                .collect();
                            report.findings.push(Finding::new(
                                "discovery.dataspace",
                                "§8.2.1, §3.7.4",
                                "Each Resource returned MUST specify a canonical Energistics URI",
                                Some(3),
                                if bad.is_empty() { Verdict::Pass } else { Verdict::Fail },
                                if bad.is_empty() {
                                    format!("{space}: {} resource(s)", resources.len())
                                } else {
                                    format!("{space}: URIs that are not canonical, e.g. {bad:?}")
                                },
                            ));
                            report.findings.push(timestamps(
                                "discovery.timestamps",
                                Some(3),
                                resources.iter().map(|r| (r.uri.as_str(), r.store_created, r.store_last_write)),
                            ));
                        }
                    }
                }
                Err(why) => report.findings.push(failed(
                    "discovery.dataspace",
                    "§8.2.1",
                    "The store MUST answer GetResources on a dataspace",
                    Some(3),
                    why,
                )),
            },
            None => report.findings.push(Finding::new(
                "discovery.dataspace",
                "§8.2.1",
                "The store MUST answer GetResources on a dataspace",
                Some(3),
                Verdict::NotChecked,
                "no dataspace to look in: the store listed none, and none was named",
            )),
        }
    } else {
        report
            .findings
            .push(not_applicable("discovery.dataspace", "§8", Some(3)));
    }

    report.exchanges.extend(client.exchanges());
    progress(&report);
    // --- Store (4): one object, and one that is not there -------------------------
    if has(4) {
        match (resources.first(), &chosen) {
            (Some(first), Some(space)) => {
                let uri = first.uri.clone();
                let path = EtpUri::parse(space).ok().and_then(|parsed| parsed.dataspace);
                let absent = EtpUri::object(path.as_deref(), "eml23.PropertyKind", NOBODY);
                let request = GetDataObjects {
                    uris: [("1".to_string(), uri.clone()), ("2".to_string(), absent.clone())]
                        .into_iter()
                        .collect(),
                    format: "xml".into(),
                };
                match ask(client, &mut seen, &request) {
                    Ok(answer) => {
                        let mut got: BTreeMap<String, DataObject> = BTreeMap::new();
                        let mut errors: BTreeMap<String, ErrorInfo> = BTreeMap::new();
                        for delivery in &answer {
                            match (&delivery.body, delivery.exception()) {
                                (Body::Message(Message::GetDataObjectsResponse(r)), _) => {
                                    got.extend(r.data_objects.clone())
                                }
                                (_, Some(Exception::Map(map))) => errors.extend(map),
                                (_, Some(Exception::Single(one))) => {
                                    errors.insert("*".into(), one);
                                }
                                _ => {}
                            }
                        }
                        // The one that exists.
                        match got.get("1") {
                            Some(object) if !object.data.is_empty() => report.findings.push(Finding::new(
                                "store.get",
                                "§9.2.1.1",
                                "For the URIs it returns data objects for, the store MUST send GetDataObjectsResponse map messages keyed as the request",
                                Some(4),
                                Verdict::Pass,
                                format!("{uri}: {} bytes of {}", object.data.len(), object.format),
                            )),
                            Some(_) => report.findings.push(Finding::new(
                                "store.get",
                                "§9.2.1.1",
                                "A data object returned carries its data",
                                Some(4),
                                Verdict::Fail,
                                format!("{uri}: an empty data field, and no blobId"),
                            )),
                            None => report.findings.push(Finding::new(
                                "store.get",
                                "§9.2.1.1",
                                "The store MUST return all data objects that it can",
                                Some(4),
                                Verdict::Fail,
                                format!("{uri}: listed by Discovery, not returned by Store - {}", errors.get("1").map(|e| e.message.clone()).unwrap_or_else(|| "no error either".into())),
                            )),
                        }
                        // The one that does not.
                        match errors.get("2") {
                            Some(error) if error.code == EtpError::ENOT_FOUND.code() => report.findings.push(Finding::new(
                                "store.get.not-found",
                                "§9.2.1.1 §3, §3.7.3 §4",
                                "For the URIs it does not return, the store MUST send a map ProtocolException keyed as the request, ENOT_FOUND (11) for an absent object",
                                Some(4),
                                Verdict::Pass,
                                format!("ENOT_FOUND for {absent}"),
                            )),
                            Some(error) => report.findings.push(Finding::new(
                                "store.get.not-found",
                                "§9.2.1.1 §3",
                                "An absent object is ENOT_FOUND (11)",
                                Some(4),
                                Verdict::Fail,
                                format!("{absent}: error {} ({}) instead of 11", error.code, error.message),
                            )),
                            None => report.findings.push(Finding::new(
                                "store.get.not-found",
                                "§9.2.1.1 §3, §3.7.3 §4.d",
                                "Each key of a map request MUST appear in a positive response or in the errors map",
                                Some(4),
                                Verdict::Fail,
                                if got.contains_key("2") {
                                    format!("{absent}: an object came back for a uuid nobody holds")
                                } else if let Some(all) = errors.get("*") {
                                    format!("a terminating error instead of a keyed one: {} ({})", all.message, all.code)
                                } else {
                                    format!("{absent}: neither answered nor refused - the key vanished")
                                },
                            )),
                        }
                    }
                    Err(why) => report.findings.push(failed(
                        "store.get",
                        "§9.2.1.1",
                        "The store MUST answer GetDataObjects",
                        Some(4),
                        why,
                    )),
                }
            }
            _ => report.findings.push(Finding::new(
                "store.get",
                "§9.2.1.1",
                "The store MUST answer GetDataObjects",
                Some(4),
                Verdict::NotChecked,
                "no object to ask for: Discovery listed none",
            )),
        }
    } else {
        report.findings.push(not_applicable("store.get", "§9", Some(4)));
    }

    report.exchanges.extend(client.exchanges());
    progress(&report);
    // --- DataArray (9): one array found on a listed object ------------------------
    if has(9) {
        // The dataspace in hand first, then the others the store listed: an
        // array that cannot be found, or fails, in one dataspace is looked
        // for in the next, so one broken dataset does not stand for the
        // whole protocol.
        let mut candidates: Vec<(String, Option<Vec<Resource>>)> = Vec::new();
        if let Some(space) = &chosen {
            candidates.push((space.clone(), Some(resources.clone())));
        }
        for space in &spaces {
            if candidates.iter().all(|(uri, _)| uri != &space.uri) {
                candidates.push((space.uri.clone(), None));
            }
        }
        data_array_checks(client, &mut seen, &mut report, &candidates, has(3));
    } else {
        report
            .findings
            .push(not_applicable("data-array.metadata", "§13", Some(9)));
    }

    report.exchanges.extend(client.exchanges());
    progress(&report);
    // --- SupportedTypes (25) ------------------------------------------------------
    if has(25) {
        match &chosen {
            Some(space) => {
                let request = GetSupportedTypes {
                    uri: space.clone(),
                    scope: ContextScopeKind::Targets,
                    return_empty_types: false,
                    count_objects: true,
                };
                match ask(client, &mut seen, &request) {
                    Ok(answer) => {
                        let mut types = Vec::new();
                        for delivery in &answer {
                            if let Body::Message(Message::GetSupportedTypesResponse(r)) = &delivery.body {
                                types.extend(r.supported_types.iter().cloned());
                            }
                        }
                        match exception_of(&answer) {
                            Some(said) => report.findings.push(Finding::new(
                                "supported-types.list",
                                "§22.2.1",
                                "The store MUST answer GetSupportedTypes for a dataspace",
                                Some(25),
                                Verdict::Fail,
                                format!("refused: {said}"),
                            )),
                            None => {
                                let expected = !resources.is_empty();
                                let fine = !expected || !types.is_empty();
                                report.findings.push(Finding::new(
                                    "supported-types.list",
                                    "§22.2.1",
                                    "GetSupportedTypes on a dataspace that holds objects names their types",
                                    Some(25),
                                    if fine { Verdict::Pass } else { Verdict::Fail },
                                    format!(
                                        "{} type(s) for {space}{}",
                                        types.len(),
                                        if expected { "" } else { " (the dataspace is empty)" }
                                    ),
                                ));
                            }
                        }
                    }
                    Err(why) => report.findings.push(failed(
                        "supported-types.list",
                        "§22.2.1",
                        "The store MUST answer GetSupportedTypes",
                        Some(25),
                        why,
                    )),
                }
            }
            None => report.findings.push(Finding::new(
                "supported-types.list",
                "§22.2.1",
                "The store MUST answer GetSupportedTypes",
                Some(25),
                Verdict::NotChecked,
                "no dataspace to look in",
            )),
        }
    } else {
        report
            .findings
            .push(not_applicable("supported-types.list", "§22", Some(25)));
    }

    report.exchanges.extend(client.exchanges());
    progress(&report);
    // --- Transaction (18): a read-only one, opened and rolled back ---------------
    if has(18) {
        match &chosen {
            Some(space) => {
                match start_transaction(client, std::slice::from_ref(space), true, "retp-conformance") {
                    Ok(Opened::Open(transaction)) => {
                        // A second one, while the first is open: MaxTransactionCount
                        // is 1, and the store must say so with the code the
                        // specification names.
                        let second = StartTransaction {
                            read_only: true,
                            message: "retp-conformance: a second transaction".into(),
                            dataspace_uris: vec![space.clone()],
                        };
                        report.findings.push(match ask(client, &mut seen, &second) {
                        Ok(answer) => {
                            let codes: Vec<i32> = answer.iter().filter_map(|d| d.exception()).map(|e| match e {
                                Exception::Single(info) => info.code,
                                Exception::Map(map) => map.values().next().map(|i| i.code).unwrap_or(0),
                            }).collect();
                            let opened_anyway = answer.iter().any(|d| matches!(&d.body, Body::Message(Message::StartTransactionResponse(r)) if r.successful));
                            let refused_softly = answer.iter().any(|d| matches!(&d.body, Body::Message(Message::StartTransactionResponse(r)) if !r.successful));
                            if codes.contains(&EtpError::EMAX_TRANSACTIONS_EXCEEDED.code()) {
                                Finding::new("transaction.second-refused", "§18.1.1.1 §1.c.i", "A second transaction while one is open MUST be denied with EMAX_TRANSACTIONS_EXCEEDED (15)", Some(18), Verdict::Pass, "EMAX_TRANSACTIONS_EXCEEDED (15)")
                            } else if opened_anyway {
                                Finding::new("transaction.second-refused", "§18.1.1.1 §1.c.i", "A second transaction while one is open MUST be denied", Some(18), Verdict::Fail, "a second transaction was opened: MaxTransactionCount is 1")
                            } else if refused_softly {
                                Finding::new("transaction.second-refused", "§18.1.1.1 §1.c.i", "A second transaction is denied with error EMAX_TRANSACTIONS_EXCEEDED (15)", Some(18), Verdict::Tolerated, "refused with successful=false in StartTransactionResponse rather than with the error code")
                            } else {
                                Finding::new("transaction.second-refused", "§18.1.1.1 §1.c.i", "A second transaction is denied with EMAX_TRANSACTIONS_EXCEEDED (15)", Some(18), Verdict::Fail, format!("error codes {codes:?} instead of 15"))
                            }
                        }
                        Err(why) => failed("transaction.second-refused", "§18.1.1.1 §1.c.i", "A second transaction is denied", Some(18), why),
                    });
                        let ended = rollback_transaction(client, &transaction);
                        report.findings.push(Finding::new(
                        "transaction.start-rollback",
                        "§18.2.1",
                        "A read-only transaction opens with a transactionUuid, and a RollbackTransaction on it is answered",
                        Some(18),
                        match &ended {
                            Ok(Ended::Done) => Verdict::Pass,
                            _ => Verdict::Fail,
                        },
                        match ended {
                            Ok(Ended::Done) => format!("opened {} and rolled back", hex(&transaction.uuid)),
                            Ok(Ended::Refused(why)) => format!("opened, rollback refused: {why}"),
                            Err(why) => format!("opened, rollback not answered: {why}"),
                        },
                    ));
                    }
                    Ok(Opened::Refused(why)) => report.findings.push(Finding::new(
                        "transaction.start-rollback",
                        "§18.2.1",
                        "A read-only transaction on one dataspace opens",
                        Some(18),
                        Verdict::Fail,
                        format!("refused: {why}"),
                    )),
                    Ok(Opened::Unsupported(why)) => report.findings.push(Finding::new(
                        "transaction.start-rollback",
                        "§18.2.1, §3.1.1.2 §3",
                        "A store that announces Transaction (18) supports it in full",
                        Some(18),
                        if rest {
                            Verdict::NotApplicable
                        } else {
                            Verdict::Fail
                        },
                        if rest {
                            format!("REST: {why}")
                        } else {
                            format!("announced, and then: {why}")
                        },
                    )),
                    Err(why) => report.findings.push(failed(
                        "transaction.start-rollback",
                        "§18.2.1",
                        "The store MUST answer StartTransaction",
                        Some(18),
                        why,
                    )),
                }
            }
            None => report.findings.push(Finding::new(
                "transaction.start-rollback",
                "§18.2.1",
                "A read-only transaction opens",
                Some(18),
                Verdict::NotChecked,
                "no dataspace to open it on",
            )),
        }
    } else {
        report
            .findings
            .push(not_applicable("transaction.start-rollback", "§18", Some(18)));
    }

    report.exchanges.extend(client.exchanges());
    progress(&report);
    // --- Every other read an agreed protocol has, on what the store holds -------
    crate::reads::run(client, &mut seen, &mut report, chosen.as_deref(), &resources);

    report.exchanges.extend(client.exchanges());
    progress(&report);
    // --- Core: acknowledgements ----------------------------------------------------
    if rest {
        report.findings.push(Finding::new(
            "core.acknowledge",
            "§3.7.2.2",
            "An Acknowledge answers a flagged request",
            None,
            Verdict::NotApplicable,
            "REST: no message flags, no Acknowledge",
        ));
    } else if has(24) {
        client.ask_acknowledge(true);
        let asked = ask(
            client,
            &mut seen,
            &GetDataspaces {
                store_last_write_filter: None,
            },
        );
        client.ask_acknowledge(false);
        let acks = client.acknowledgements();
        report.findings.push(match asked {
            Ok(answer) => {
                let request = answer.first().map(|d| d.header.correlation_id);
                match request {
                    Some(id) if acks.iter().any(|ack| ack.message_id == id) => Finding::new(
                        "core.acknowledge",
                        "§3.7.2.2",
                        "When the 0x10 flag asks for it, the receiver MUST send an Acknowledge correlated to the request, besides its response",
                        None,
                        Verdict::Pass,
                        format!("Acknowledge for request #{id}"),
                    ),
                    Some(id) => Finding::new(
                        "core.acknowledge",
                        "§3.7.2.2",
                        "When the 0x10 flag asks for it, the receiver MUST send an Acknowledge correlated to the request",
                        None,
                        Verdict::Fail,
                        format!("no Acknowledge for request #{id} ({} received for other ids)", acks.len()),
                    ),
                    None => Finding::new("core.acknowledge", "§3.7.2.2", "An Acknowledge answers a flagged request", None, Verdict::NotChecked, "the request got no answer to read the id from"),
                }
            }
            Err(why) => Finding::new("core.acknowledge", "§3.7.2.2", "An Acknowledge answers a flagged request", None, Verdict::Fail, format!("the flagged request failed: {why}")),
        });
    }

    report.exchanges.extend(client.exchanges());
    progress(&report);
    // --- Writing, when asked ---------------------------------------------------------
    if options.write {
        write_checks(
            client,
            &mut seen,
            &mut report,
            &negotiated.data_objects,
            options,
            &spaces,
        );
    } else {
        report.findings.push(Finding::new(
            "write.*",
            "§9.2.1.2, §9.2.1.3, §21.2.2",
            "PutDataspaces, PutDataObjects, DeleteDataObjects, DeleteDataspaces",
            None,
            Verdict::NotChecked,
            "the checks that write were not asked for (`--write`)",
        ));
    }

    report.exchanges.extend(client.exchanges());
    progress(&report);
    // --- The wire, over everything received ----------------------------------------
    let period = negotiated
        .ours
        .long(names::RESPONSE_TIMEOUT_PERIOD)
        .unwrap_or(300)
        .max(1) as u64;
    report.findings.push(match &seen.slowest {
        Some((took, what)) => {
            let within = took.as_secs() < period;
            Finding::new(
                "wire.response-time",
                "§3.3.2.3 §1, §3.7.3 §c",
                "A store MUST send the response, or its first part, no later than the customer's ResponseTimeoutPeriod",
                None,
                if within { Verdict::Pass } else { Verdict::Fail },
                format!("slowest: {what} in {} ms, against {period} s", took.as_millis()),
            )
        }
        None => Finding::new("wire.response-time", "§3.3.2.3 §1", "A store answers within the customer's ResponseTimeoutPeriod", None, Verdict::NotChecked, "nothing was asked"),
    });
    for event in client.unsolicited() {
        if let Event::Delivered(delivery) = event
            && matches!(delivery.place, Place::Stray { .. })
        {
            seen.stray.push(format!(
                "message #{} of protocol {} correlated to {}, which names no request of ours",
                delivery.header.message_id, delivery.header.protocol, delivery.header.correlation_id
            ));
        }
    }
    if rest {
        // The headers over REST are the client's own bookkeeping, not the
        // server's: judging them would judge this crate.
        for (id, section) in [
            ("wire.message-ids", "§3.5.4.1"),
            ("wire.correlation", "§3.5.4.1 §4.c"),
            ("wire.fin", "§3.7.3 §6"),
        ] {
            report.findings.push(Finding::new(
                id,
                section,
                "A rule of the ETP wire",
                None,
                Verdict::NotApplicable,
                "REST: no ETP message header travels",
            ));
        }
    } else {
        report.findings.extend(wire_findings(&seen));
    }
    // Each time the session was opened again after a timeout, said.
    report.notes.extend(client.renewals());
    report.requests = Report::requests_of(&seen.calls);
    report.exchanges.extend(client.exchanges());
    progress(&report);
    report
}

/// GetResources on a dataspace, `activeStatusFilter` null - every status, as
/// the schema allows (`["null", ActiveStatusKind]`, §8.3.1). A store that
/// cannot read the `null` (one vendor's server answers « Error while trying
/// deserialize … GetResources ») is asked again with `active`, then
/// `inactive`, and what those answer is the listing the reads go on with.
/// Either way `discovery.null-filter` says how the store took the `null`,
/// so that the handling of nulls appears in the report too.
fn listing_with_filter_fallback<K: Session>(
    client: &mut K,
    seen: &mut Seen,
    report: &mut Report,
    space: &str,
) -> Result<Vec<Delivery>, ClientError> {
    const RULE: &str = "GetResources with activeStatusFilter null - every status, which the schema allows - is answered like any other";
    let answer = ask(client, seen, &resources_of(space, 1))?;
    let Some(refusal) = exception_of(&answer) else {
        report.findings.push(Finding::new(
            "discovery.null-filter",
            "§8.3.1",
            RULE,
            Some(3),
            Verdict::Pass,
            format!("{space}: answered with activeStatusFilter null"),
        ));
        return Ok(answer);
    };
    // A refusal for the listing's size is not about the null: some servers say
    // ERESPONSECOUNT_EXCEEDED for a dataspace of more than 10 000 objects.
    let for_size = answer.iter().any(|d| match d.exception() {
        Some(Exception::Single(info)) => info.code == EtpError::ERESPONSECOUNT_EXCEEDED.code(),
        Some(Exception::Map(map)) => map
            .values()
            .any(|info| info.code == EtpError::ERESPONSECOUNT_EXCEEDED.code()),
        None => false,
    });
    let mut both = Vec::new();
    let mut taken = Vec::new();
    for status in [ActiveStatusKind::Active, ActiveStatusKind::Inactive] {
        let request = GetResources {
            active_status_filter: Some(status),
            ..resources_of(space, 1)
        };
        if let Ok(again) = ask(client, seen, &request)
            && exception_of(&again).is_none()
        {
            taken.push(format!("{status:?}"));
            both.extend(again);
        }
    }
    if taken.is_empty() {
        // Not the null: refused all the same. The listing's own finding says it.
        return Ok(answer);
    }
    report.findings.push(if for_size {
        Finding::new(
            "discovery.null-filter",
            "§8.3.1",
            RULE,
            Some(3),
            Verdict::NotChecked,
            format!(
                "{space}: the listing was refused for its size ({refusal}), not for the null; answered with {} - the checks that follow use that listing",
                taken.join(" and ")
            ),
        )
    } else {
        Finding::new(
            "discovery.null-filter",
            "§8.3.1",
            RULE,
            Some(3),
            Verdict::Fail,
            format!("{space}: refused with activeStatusFilter null ({refusal}), answered with {}; the checks that follow use that listing", taken.join(" and ")),
        )
    });
    Ok(both)
}

/// DataArray, on the first array the first objects of a dataspace declare -
/// in the dataspace in hand, and when it gives nothing or fails, in the next
/// one the store listed (eight at most).
///
/// Finding an array is not ETP's: the object is read with the registry and
/// `energyml_storage::array::locate_with` says where its numbers hang - a
/// RESQML 2.0.1 proxy resolved through the listing, or the object itself
/// from 2.2 on -, the same way the bridge does. Then three rules: the
/// metadata comes for the array (§13.2.1), an absent path is `ENOT_FOUND`
/// by key (§13.2.1, §3.7.3 §4.d), and the values read whole count what the
/// metadata declared (§13.2.1.1). The real array and the absent path are
/// asked in **two** requests: a store that breaks on the absent one must not
/// hide that it serves the real one.
fn data_array_checks<K: Session>(
    client: &mut K,
    seen: &mut Seen,
    report: &mut Report,
    candidates: &[(String, Option<Vec<Resource>>)],
    discovery: bool,
) {
    let mut tried: Vec<String> = Vec::new();
    // The findings of the first dataspace where an array was found but went
    // wrong: kept to be reported when no other dataspace does better.
    let mut first_failure: Option<Vec<Finding>> = None;
    for (space, listed) in candidates.iter().take(8) {
        let resources = match listed {
            Some(resources) => resources.clone(),
            None if discovery => match ask(client, seen, &resources_of(space, 1)) {
                Ok(answer) => answer
                    .iter()
                    .filter_map(|d| match &d.body {
                        Body::Message(Message::GetResourcesResponse(r)) => Some(r.resources.clone()),
                        _ => None,
                    })
                    .flatten()
                    .collect(),
                Err(why) => {
                    tried.push(format!("{space}: not listed ({why})"));
                    continue;
                }
            },
            None => continue,
        };
        if resources.is_empty() {
            tried.push(format!("{space}: empty"));
            continue;
        }
        match array_attempt(client, seen, space, &resources) {
            Attempt::NoArray(looked) => {
                tried.push(format!("{space}: no array on the first {looked} object(s) read"))
            }
            Attempt::Tried { findings, read } => {
                if read {
                    tried.push(format!("{space}: array read"));
                    report.findings.extend(findings);
                    report.findings.push(Finding::new(
                        "data-array.dataspaces",
                        "§13",
                        "The DataArray checks ran on an array the store holds",
                        Some(9),
                        Verdict::Pass,
                        tried.join("; "),
                    ));
                    return;
                }
                tried.push(format!("{space}: array found, not read"));
                if first_failure.is_none() {
                    first_failure = Some(findings);
                }
            }
        }
    }
    match first_failure {
        Some(findings) => report.findings.extend(findings),
        None => report.findings.push(Finding::new(
            "data-array.metadata",
            "§13.2.1",
            "The store MUST answer GetDataArrayMetadata for an array an object declares",
            Some(9),
            Verdict::NotChecked,
            "no array found in any dataspace tried",
        )),
    }
    report.findings.push(Finding::new(
        "data-array.dataspaces",
        "§13",
        "The DataArray checks ran on an array the store holds",
        Some(9),
        Verdict::NotChecked,
        if tried.is_empty() {
            "no dataspace to look in".to_string()
        } else {
            tried.join("; ")
        },
    ));
}

/// What one dataspace gave the DataArray checks.
enum Attempt {
    /// No array on the objects read - how many were.
    NoArray(usize),
    /// An array was found: the findings, and whether its values were read.
    Tried { findings: Vec<Finding>, read: bool },
}

fn array_attempt<K: Session>(
    client: &mut K,
    seen: &mut Seen,
    space: &str,
    resources: &[Resource],
) -> Attempt {
    let registry = energyml_schemas::all();
    let resolver = |proxy_uuid: &str| {
        resources
            .iter()
            .find(|r| {
                EtpUri::parse(&r.uri)
                    .ok()
                    .and_then(|u| u.uuid)
                    .is_some_and(|u| u.eq_ignore_ascii_case(proxy_uuid))
            })
            .map(|r| ProxyTarget::file(r.uri.clone()))
    };
    // The first array found, the objects whose type carries arrays read first
    // (representations, properties, logs, grids, trajectories) - sixty at
    // most: a dataspace of two thousand objects is not read to find one
    // array, and a WITSML store lists its wells before its logs.
    let carries_arrays = |resource: &Resource| {
        let kind = EtpUri::parse(&resource.uri)
            .ok()
            .and_then(|u| u.object_type)
            .unwrap_or_default()
            .to_ascii_lowercase();
        [
            "representation",
            "property",
            "log",
            "grid",
            "trajectory",
            "frame",
            "array",
        ]
        .iter()
        .any(|word| kind.contains(word))
    };
    let mut ordered: Vec<&Resource> = resources.iter().filter(|r| carries_arrays(r)).collect();
    ordered.extend(resources.iter().filter(|r| !carries_arrays(r)));
    let mut found: Option<(String, DataArrayIdentifier)> = None;
    let mut looked = 0;
    for resource in ordered.into_iter().take(60) {
        let request = GetDataObjects {
            uris: [("1".to_string(), resource.uri.clone())].into_iter().collect(),
            format: "xml".into(),
        };
        let Ok(answer) = ask(client, seen, &request) else {
            break;
        };
        let Some(object) = answer.iter().find_map(|d| match &d.body {
            Body::Message(Message::GetDataObjectsResponse(r)) => r.data_objects.get("1").cloned(),
            _ => None,
        }) else {
            continue;
        };
        looked += 1;
        let Ok((read, _)) = read_object(&registry, ReadMode::Tolerant, &resource.uri, &object.data) else {
            continue;
        };
        let own = resource.uri.clone();
        for location in locate_with(read.as_reflect(), &resolver) {
            let uri = if location.proxy_uuid.is_none() {
                own.clone()
            } else {
                location.file.clone()
            };
            if uri.is_empty() {
                continue;
            }
            found = Some((
                resource.uri.clone(),
                DataArrayIdentifier {
                    uri,
                    path_in_resource: location.path_in_file.clone(),
                },
            ));
            break;
        }
        if found.is_some() {
            break;
        }
    }
    let Some((object_uri, id)) = found else {
        return Attempt::NoArray(looked);
    };
    let mut findings = Vec::new();
    // The array alone.
    let request = GetDataArrayMetadata {
        data_arrays: [("1".to_string(), id.clone())].into_iter().collect(),
    };
    let mut declared: Option<Vec<i64>> = None;
    match ask(client, seen, &request) {
        Ok(answer) => {
            let mut errors: BTreeMap<String, ErrorInfo> = BTreeMap::new();
            for delivery in &answer {
                match (&delivery.body, delivery.exception()) {
                    (Body::Message(Message::GetDataArrayMetadataResponse(r)), _) => {
                        if let Some(meta) = r.array_metadata.get("1") {
                            declared = Some(meta.dimensions.clone());
                        }
                    }
                    (_, Some(Exception::Map(map))) => errors.extend(map),
                    (_, Some(Exception::Single(one))) => {
                        errors.insert("*".into(), one);
                    }
                    _ => {}
                }
            }
            findings.push(match &declared {
                Some(dimensions) if !dimensions.is_empty() && dimensions.iter().all(|d| *d > 0) => Finding::new(
                    "data-array.metadata",
                    "§13.2.1",
                    "GetDataArrayMetadata answers, by key, the dimensions and types of an array an object declares",
                    Some(9),
                    Verdict::Pass,
                    format!("{} {}: dimensions {dimensions:?}", id.uri, id.path_in_resource),
                ),
                Some(dimensions) => Finding::new("data-array.metadata", "§13.2.1", "The metadata of an array states its dimensions", Some(9), Verdict::Fail, format!("dimensions {dimensions:?}")),
                None => Finding::new(
                    "data-array.metadata",
                    "§13.2.1",
                    "GetDataArrayMetadata answers for an array an object declares",
                    Some(9),
                    Verdict::Fail,
                    format!("{} {} (declared by {object_uri}): {}", id.uri, id.path_in_resource, errors.get("1").or(errors.get("*")).map(|e| format!("{} ({})", e.message, e.code)).unwrap_or_else(|| "neither answered nor refused".into())),
                ),
            });
        }
        Err(why) => findings.push(Finding::new(
            "data-array.metadata",
            "§13.2.1",
            "The store MUST answer GetDataArrayMetadata",
            Some(9),
            Verdict::Fail,
            format!(
                "{} {} (declared by {object_uri}): not answered: {why}",
                id.uri, id.path_in_resource
            ),
        )),
    }
    // A path nobody holds, on its own.
    let absent = DataArrayIdentifier {
        uri: id.uri.clone(),
        path_in_resource: format!("/energyml-conformance/nobody-has-this-{}", stamp_hex()),
    };
    let request = GetDataArrayMetadata {
        data_arrays: [("1".to_string(), absent)].into_iter().collect(),
    };
    findings.push(match ask(client, seen, &request) {
        Ok(answer) => {
            let mut errors: BTreeMap<String, ErrorInfo> = BTreeMap::new();
            for delivery in &answer {
                match delivery.exception() {
                    Some(Exception::Map(map)) => errors.extend(map),
                    Some(Exception::Single(one)) => {
                        errors.insert("*".into(), one);
                    }
                    None => {}
                }
            }
            match errors.get("1") {
                Some(error) if error.code == EtpError::ENOT_FOUND.code() => Finding::new(
                    "data-array.not-found",
                    "§13.2.1, §3.7.3 §4.d",
                    "An array path nobody holds is ENOT_FOUND (11), by key",
                    Some(9),
                    Verdict::Pass,
                    "ENOT_FOUND for the absent path",
                ),
                Some(error) => Finding::new(
                    "data-array.not-found",
                    "§13.2.1",
                    "An absent array is ENOT_FOUND (11)",
                    Some(9),
                    Verdict::Fail,
                    format!("error {} ({}) instead of 11", error.code, error.message),
                ),
                None => Finding::new(
                    "data-array.not-found",
                    "§13.2.1, §3.7.3 §4.d",
                    "Each key of a map request is answered, positively or in the errors map",
                    Some(9),
                    Verdict::Fail,
                    if let Some(all) = errors.get("*") {
                        format!(
                            "a terminating error instead of a keyed one: {} ({})",
                            all.message, all.code
                        )
                    } else {
                        "the absent path was neither answered nor refused".into()
                    },
                ),
            }
        }
        Err(why) => Finding::new(
            "data-array.not-found",
            "§13.2.1",
            "An absent array is ENOT_FOUND (11), by key",
            Some(9),
            Verdict::Fail,
            format!("not answered: {why}"),
        ),
    });
    let Some(dimensions) = declared else {
        return Attempt::Tried {
            findings,
            read: false,
        };
    };
    let expected: i64 = dimensions.iter().product();
    let mut read = false;
    let mut whole: Option<Vec<f64>> = None;
    match timed(seen, "GetDataArrays", || read_arrays(client, std::slice::from_ref(&id), DEFAULT_MESSAGE, |_| {})) {
        Ok(mut answers) => findings.push(match answers.pop() {
            Some(Ok(array)) => {
                let counted = match energyml_etp_store::values_of(&array) {
                    Ok(values) => values.len() as i64,
                    Err(_) => -1,
                };
                read = counted == expected;
                whole = array.values.to_f64s(array.logical).filter(|_| read);
                Finding::new(
                    "data-array.read",
                    "§13.2.1.1, §13.2.2",
                    "GetDataArrays (or sub-arrays, when it does not fit a message) returns the values the metadata declared",
                    Some(9),
                    if counted == expected { Verdict::Pass } else { Verdict::Fail },
                    if counted == expected {
                        format!("{counted} value(s) read in {} message(s), as {dimensions:?} declares", array.blocks.max(1))
                    } else if counted < 0 {
                        format!("the values came in a transport this suite cannot count ({:?})", array.transport)
                    } else {
                        format!("{counted} value(s) read, {expected} declared by {dimensions:?}")
                    },
                )
            }
            Some(Err(why)) => Finding::new("data-array.read", "§13.2.1.1", "An array whose metadata was given reads", Some(9), Verdict::Fail, format!("{why:?}")),
            None => Finding::new("data-array.read", "§13.2.1.1", "An array whose metadata was given reads", Some(9), Verdict::Fail, "no answer for it"),
        }),
        Err(why) => findings.push(failed("data-array.read", "§13.2.1.1", "The store MUST answer GetDataArrays", Some(9), why)),
    }
    if let Some(whole) = whole {
        findings.push(subarray_of_held(client, seen, &id, &dimensions, &whole));
    }
    let _ = space;
    Attempt::Tried { findings, read }
}

/// `GetDataSubarrays` on an array the store holds (§13.3.3), set against
/// the whole array read just before: the middle half of the first dimension,
/// all of the others, must be the same values at the same place. Two ways
/// the store serves one array, each a check on the other.
fn subarray_of_held<K: Session>(
    client: &mut K,
    seen: &mut Seen,
    id: &DataArrayIdentifier,
    dimensions: &[i64],
    whole: &[f64],
) -> Finding {
    let full: Vec<u64> = dimensions.iter().map(|d| (*d).max(0) as u64).collect();
    let first = full[0] / 4;
    let count = (full[0] / 2).max(1);
    let inner: u64 = full[1..].iter().product();
    let mut starts = vec![0; full.len()];
    starts[0] = first;
    let mut counts = full.clone();
    counts[0] = count;
    let region = Block { starts, counts };
    let expected = &whole[(first * inner) as usize..((first + count) * inner) as usize];
    let rule =
        "GetDataSubarrays returns the part of an array asked for: the values the whole array holds there";
    let asked = format!(
        "{} {}: indices {first}..{} of the first dimension of {full:?}",
        id.uri,
        id.path_in_resource,
        first + count
    );
    match timed(seen, "GetDataSubarrays", || {
        read_regions(client, &[(id.clone(), Some(region))], DEFAULT_MESSAGE, |_| {})
    }) {
        Ok(mut back) => match back.pop() {
            Some(Ok(read)) => match read.values.to_f64s(read.logical) {
                // NaN is a value a sub-array must return too: compared bit for bit.
                Some(numbers)
                    if numbers.len() == expected.len()
                        && numbers
                            .iter()
                            .zip(expected)
                            .all(|(a, b)| a.to_bits() == b.to_bits()) =>
                {
                    Finding::new(
                        "data-array.subarray",
                        "§13.3.3, §13.2.1.1",
                        rule,
                        Some(9),
                        Verdict::Pass,
                        format!("{asked}: {} value(s), equal to the whole array's", numbers.len()),
                    )
                }
                Some(numbers) => Finding::new(
                    "data-array.subarray",
                    "§13.3.3, §13.2.1.1",
                    rule,
                    Some(9),
                    Verdict::Fail,
                    format!(
                        "{asked}: {} value(s) returned, {} expected, or not those of the whole array",
                        numbers.len(),
                        expected.len()
                    ),
                ),
                None => Finding::new(
                    "data-array.subarray",
                    "§13.3.3",
                    rule,
                    Some(9),
                    Verdict::NotChecked,
                    format!(
                        "{asked}: returned in a form this suite cannot compare ({:?})",
                        read.transport
                    ),
                ),
            },
            Some(Err(why)) => Finding::new(
                "data-array.subarray",
                "§13.3.3",
                rule,
                Some(9),
                Verdict::Fail,
                format!("{asked}: refused: {why:?}"),
            ),
            None => Finding::new(
                "data-array.subarray",
                "§13.3.3",
                rule,
                Some(9),
                Verdict::Fail,
                format!("{asked}: neither answered nor refused"),
            ),
        },
        Err(why) => failed(
            "data-array.subarray",
            "§13.3.3",
            "The store MUST answer GetDataSubarrays",
            Some(9),
            why,
        ),
    }
}

/// When the suite cannot make a dataspace of its own, its samples go into
/// an existing one: the one `--write-fallback-dataspace` names, else the
/// first listed whose name says `test`, else the default dataspace
/// `eml:///`, which every store MUST support (§21.2.2 row 1). Only what the
/// suite put there is deleted afterwards - its uuid prefix, its dataspace
/// (`delete_ours`) - and the dataspace never. The rule: when no dataspace can
/// be created, look for a dataspace with "test" in its name, or else the
/// default dataspace `eml:///`, and upload the suite's files there, to be
/// deleted at the end. Careful: when no dataspace was created, never delete a
/// dataspace that is not the suite's.
fn write_into_existing<K: Session>(
    client: &mut K,
    seen: &mut Seen,
    report: &mut Report,
    data_objects: &[retp_types::datatypes::SupportedDataObject],
    options: &Options,
    listed: &[Dataspace],
    why: &str,
) {
    let agreed: Vec<i32> = client.session().protocols.keys().copied().collect();
    let has = |protocol: i32| agreed.contains(&protocol);
    let (into, how) = match &options.write_fallback {
        Some(path) => {
            let into = dataspace_uri(path);
            if into != "eml:///" && !listed.iter().any(|space| space.uri == into) {
                report.notes.push(format!(
                    "{why}, and the fallback {into} is not one the server lists: no object was written."
                ));
                return;
            }
            (into, "as asked (--write-fallback-dataspace)".to_string())
        }
        None => match listed
            .iter()
            .find(|space| space.path.to_ascii_lowercase().contains("test"))
        {
            Some(space) => (
                space.uri.clone(),
                "the first listed dataspace whose name says \"test\"".to_string(),
            ),
            None => (
                "eml:///".to_string(),
                "the default dataspace, no listed one saying \"test\"".to_string(),
            ),
        },
    };
    if !has(4) {
        report.notes.push(format!(
            "{why}, and Store (4) was not agreed: no object was written."
        ));
        return;
    }
    report.notes.push(format!(
        "{why}; it wrote its samples into the existing {into} ({how}) and deleted only what it had put there (uuids starting with {}). The dataspace itself was not touched.",
        samples::PREFIX
    ));
    let agreed_types: Vec<String> = data_objects
        .iter()
        .map(|one| one.qualified_type.clone())
        .collect();
    sample_checks(client, seen, report, &into, &agreed_types, has(3), has(9));
    report.findings.push(Finding::new(
        "write.dataspace.delete",
        "§21.2.2",
        "The suite deletes only a dataspace it created",
        Some(24),
        Verdict::NotChecked,
        format!("{into} is not the suite's: it is not deleted"),
    ));
}

/// The checks that write: a dataspace of the suite's own, one object into it,
/// read back, deleted, the dataspace deleted. Nothing of the server's is
/// touched, and the dataspace is deleted even when a step in between fails.
fn write_checks<K: Session>(
    client: &mut K,
    seen: &mut Seen,
    report: &mut Report,
    data_objects: &[retp_types::datatypes::SupportedDataObject],
    options: &Options,
    listed: &[Dataspace],
) {
    let agreed: Vec<i32> = client.session().protocols.keys().copied().collect();
    let has = |protocol: i32| agreed.contains(&protocol);
    if !has(24) {
        report
            .findings
            .push(not_applicable("write.dataspace", "§21", Some(24)));
        write_into_existing(
            client,
            seen,
            report,
            data_objects,
            options,
            listed,
            "Dataspace (24) was not agreed, so the suite could not create a dataspace of its own",
        );
        return;
    }
    let name = options
        .write_dataspace
        .clone()
        .unwrap_or_else(|| format!("example/etp-conformance-{}", stamp()));
    let uri = dataspace_uri(&name);
    let name = uri
        .strip_prefix("eml:///dataspace('")
        .and_then(|rest| rest.strip_suffix("')"))
        .unwrap_or(&name)
        .to_string();
    // **Never into a dataspace that exists.** What the suite deletes must be
    // what it made, and a dataspace it did not create holds what it did not.
    if listed.iter().any(|space| space.uri == uri || space.path == name) {
        report.findings.push(Finding::new(
            "write.dataspace.put",
            "§21.2.2",
            "The suite writes only in a dataspace it creates",
            Some(24),
            Verdict::NotChecked,
            format!("{uri} already exists: nothing was written, nothing deleted - give another name"),
        ));
        report.notes.push(format!("No writing check ran: {uri} already exists on the server, and the suite writes only in a dataspace it creates itself."));
        return;
    }
    let space = Dataspace {
        uri: uri.clone(),
        path: name.clone(),
        custom_data: options.custom_data.clone(),
        ..Default::default()
    };
    let created = match timed(seen, "PutDataspaces", || {
        put_dataspaces(client, std::slice::from_ref(&space))
    }) {
        Ok(answers) => match answers.get(&uri) {
            Some(Ok(_)) => {
                report.findings.push(Finding::new(
                    "write.dataspace.put",
                    "§21.2.2",
                    "PutDataspaces creates the dataspace and answers for it by key",
                    Some(24),
                    Verdict::Pass,
                    format!("created {uri}"),
                ));
                true
            }
            Some(Err(why)) => {
                // The hint is for an OSDU store, which needs them; said to
                // any other, it sends its provider after the wrong thing.
                let name = client.session().peer_application.0.to_ascii_lowercase();
                let osdu = client.is_rest()
                    || ["osdu", "ddms", "reservoir"]
                        .iter()
                        .any(|word| name.contains(word));
                let hint = if osdu && options.custom_data.is_empty() {
                    " - an OSDU store wants owners, viewers and legal tags in customData"
                } else {
                    ""
                };
                // Code 0 is the client's: the key was in no answer at all.
                let what = if why.code == 0 {
                    "neither created nor refused"
                } else {
                    "refused"
                };
                report.findings.push(Finding::new(
                    "write.dataspace.put",
                    "§21.2.2",
                    "PutDataspaces creates the dataspace",
                    Some(24),
                    Verdict::Fail,
                    format!("{uri}: {what}: {} ({}){hint}", why.message, why.code),
                ));
                false
            }
            None => {
                report.findings.push(Finding::new(
                    "write.dataspace.put",
                    "§21.2.2, §3.7.3 §4.d",
                    "Each key of a map request is answered",
                    Some(24),
                    Verdict::Fail,
                    format!("{uri}: neither created nor refused"),
                ));
                false
            }
        },
        Err(why) => {
            report.findings.push(failed(
                "write.dataspace.put",
                "§21.2.2",
                "The store MUST answer PutDataspaces",
                Some(24),
                why,
            ));
            false
        }
    };
    if !created {
        write_into_existing(
            client,
            seen,
            report,
            data_objects,
            options,
            listed,
            "The suite could not create a dataspace of its own",
        );
        return;
    }
    report.notes.push(format!(
        "The writing checks worked in {uri}, a dataspace the suite created for this run; it deleted only objects it had put there itself (uuids starting with {})",
        samples::PREFIX
    ));

    // The object: a RESQML 2.0.1 feature, the smallest object every store
    // that speaks resqml20 takes. Without resqml20, the object checks stay
    // unchecked rather than guessing another release's schema.
    // Over REST no type is negotiated - the API takes any object's JSON -
    // so the feature is written there too.
    let speaks_resqml20 = client.is_rest()
        || data_objects
            .iter()
            .any(|one| one.qualified_type.starts_with("resqml20."));
    if has(4) && speaks_resqml20 {
        let object_uuid = format!("{}-{}", &NOBODY[..23], stamp_hex());
        let object_uri = format!("{uri}/resqml20.obj_GeneticBoundaryFeature({object_uuid})");
        let xml = format!(
            "<resqml:obj_GeneticBoundaryFeature xmlns:resqml=\"http://www.energistics.org/energyml/data/resqmlv2\" \
xmlns:eml=\"http://www.energistics.org/energyml/data/commonv2\" uuid=\"{object_uuid}\" schemaVersion=\"2.0\">\
<eml:Citation><eml:Title>retp-conformance</eml:Title><eml:Originator>retp-conformance</eml:Originator>\
<eml:Creation>2026-09-17T00:00:00Z</eml:Creation><eml:Format>retp-conformance</eml:Format></eml:Citation>\
<resqml:GeneticBoundaryKind>horizon</resqml:GeneticBoundaryKind></resqml:obj_GeneticBoundaryFeature>"
        );
        let object = DataObject {
            resource: Resource {
                uri: object_uri.clone(),
                name: "retp-conformance".into(),
                ..Default::default()
            },
            format: "xml".into(),
            blob_id: None,
            data: xml.clone().into_bytes(),
        };
        let put = timed(seen, "PutDataObjects", || {
            put_data_objects(client, std::slice::from_ref(&object), false)
        });
        let written = matches!(
            put.as_ref().map(|answers| answers.get(&object_uri)),
            Ok(Some(Ok(_)))
        );
        report.findings.push(Finding::new(
            "write.object.put",
            "§9.2.1.2",
            "PutDataObjects adds a data object that does not exist yet and answers for it by key",
            Some(4),
            if written { Verdict::Pass } else { Verdict::Fail },
            match &put {
                Ok(answers) => match answers.get(&object_uri) {
                    Some(Ok(_)) => format!("put {object_uri}"),
                    Some(Err(why)) => format!("{object_uri}: refused: {} ({})", why.message, why.code),
                    None => format!("{object_uri}: neither put nor refused"),
                },
                Err(why) => format!("not answered: {why}"),
            },
        ));
        if written {
            // Read back: the same bytes, or at least the same object.
            let request = GetDataObjects {
                uris: [("1".to_string(), object_uri.clone())].into_iter().collect(),
                format: "xml".into(),
            };
            match ask(client, seen, &request) {
                Ok(answer) => {
                    let mut back: Option<DataObject> = None;
                    for delivery in &answer {
                        if let Body::Message(Message::GetDataObjectsResponse(r)) = &delivery.body {
                            back = r.data_objects.get("1").cloned();
                        }
                    }
                    report.findings.push(match back {
                        Some(object) if String::from_utf8_lossy(&object.data).contains(&object_uuid) => Finding::new(
                            "write.object.read-back",
                            "§9.2.1.2 §2.b-c",
                            "A put object is stored as sent - a put is a complete replacement, and the store MUST NOT change what is not store-managed",
                            Some(4),
                            Verdict::Pass,
                            format!("{} bytes back", object.data.len()),
                        ),
                        Some(_) => Finding::new("write.object.read-back", "§9.2.1.2", "A put object reads back as itself", Some(4), Verdict::Fail, "the object read back does not carry the uuid that was put"),
                        None => Finding::new("write.object.read-back", "§9.2.1.2", "A put object reads back", Some(4), Verdict::Fail, format!("{object_uri}: not returned after PutDataObjects - {}", exception_of(&answer).unwrap_or_default())),
                    });
                }
                Err(why) => report.findings.push(failed(
                    "write.object.read-back",
                    "§9.2.1.2",
                    "A put object reads back",
                    Some(4),
                    why,
                )),
            }
            // Its resource: storeCreated and storeLastWrite set together.
            if has(3)
                && let Ok(answer) = ask(client, seen, &resources_of(&uri, 1))
            {
                let mut found = None;
                for delivery in &answer {
                    if let Body::Message(Message::GetResourcesResponse(r)) = &delivery.body {
                        found = found
                            .or_else(|| r.resources.iter().find(|r| r.uri.contains(&object_uuid)).cloned());
                    }
                }
                report.findings.push(match found {
                        Some(resource) => {
                            let fine = resource.store_created > 0 && resource.store_last_write >= resource.store_created;
                            Finding::new(
                                "write.object.timestamps",
                                "§3.12.5.2 §a, §f",
                                "When a data object is created, the store MUST set storeCreated and storeLastWrite to the same timestamp; storeLastWrite is never earlier than storeCreated",
                                Some(3),
                                if fine { Verdict::Pass } else { Verdict::Fail },
                                format!("storeCreated {} storeLastWrite {}", resource.store_created, resource.store_last_write),
                            )
                        }
                        None => Finding::new("write.object.timestamps", "§8.2.1", "A put object is discoverable in its dataspace", Some(3), Verdict::Fail, format!("{object_uri}: put, not listed by Discovery")),
                    });
            }
            // Delete it, and it is gone - unless the store has added objects
            // of its own to the dataspace meanwhile (some stores record an
            // `obj_Activity` per write, which references what was written):
            // then nothing is deleted, and the report says why.
            let made = if has(3) {
                store_made(client, seen, &uri, &[])
            } else {
                Vec::new()
            };
            let deleted = if made.is_empty() {
                Some(delete_ours(client, seen, &uri, &object_uri))
            } else {
                None
            };
            if deleted.is_none() {
                report.findings.push(Finding::new(
                    "write.object.delete",
                    "§9.2.1.3",
                    "DeleteDataObjects deletes the data object and answers for it by key",
                    Some(4),
                    Verdict::NotChecked,
                    format!("{object_uri} kept: {}", kept_because(&made)),
                ));
                note_kept(report, &uri, &made);
            }
            if let Some(deleted) = deleted {
                match deleted {
                    Ok(answers) => {
                        let gone = matches!(answers.get(&object_uri), Some(Ok(_)));
                        report.findings.push(Finding::new(
                            "write.object.delete",
                            "§9.2.1.3",
                            "DeleteDataObjects deletes the data object and answers for it by key",
                            Some(4),
                            if gone { Verdict::Pass } else { Verdict::Fail },
                            match answers.get(&object_uri) {
                                Some(Ok(_)) => format!("deleted {object_uri}"),
                                Some(Err(why)) => format!("refused: {} ({})", why.message, why.code),
                                None => "neither deleted nor refused".to_string(),
                            },
                        ));
                        if gone {
                            expect_error(
                                client,
                                seen,
                                report,
                                "write.object.gone",
                                "§9.2.1.3, §9.2.1.1 §3",
                                "A deleted data object is ENOT_FOUND (11) afterwards",
                                Some(4),
                                &GetDataObjects {
                                    uris: [("1".to_string(), object_uri.clone())].into_iter().collect(),
                                    format: "xml".into(),
                                },
                                EtpError::ENOT_FOUND,
                            );
                        }
                    }
                    Err(why) => report.findings.push(failed(
                        "write.object.delete",
                        "§9.2.1.3",
                        "The store MUST answer DeleteDataObjects",
                        Some(4),
                        why,
                    )),
                }
            }
        }
    } else {
        report.findings.push(Finding::new(
            "write.object.put",
            "§9.2.1.2",
            "PutDataObjects adds a data object",
            Some(4),
            if has(4) {
                Verdict::NotChecked
            } else {
                Verdict::NotApplicable
            },
            if has(4) {
                "the session agreed no resqml20 type, and this suite writes a resqml20 feature"
            } else {
                "Store (4) was not agreed"
            },
        ));
    }

    // A small package of several releases, two arrays among it.
    if has(4) {
        let agreed_types: Vec<String> = data_objects
            .iter()
            .map(|one| one.qualified_type.clone())
            .collect();
        sample_checks(client, seen, report, &uri, &agreed_types, has(3), has(9));
    }

    // Transactions that write: what a rollback leaves, what a commit keeps.
    if has(18) && has(4) && speaks_resqml20 {
        transaction_checks(client, seen, report, &uri);
    } else if has(18) {
        report.findings.push(Finding::new(
            "transaction.rollback-leaves-nothing",
            "§18.1.1.1 §6",
            "A rolled-back transaction leaves the store as it was",
            Some(18),
            Verdict::NotChecked,
            "needs Store (4) and a resqml20 type to write",
        ));
    }

    // The dataspace goes - only when nothing in it is somebody else's. The
    // suite created it, so nothing should be; a store that let another
    // client write there in the meantime keeps the dataspace and its objects.
    if has(3) {
        let foreign: Vec<String> = match ask(client, seen, &resources_of(&uri, 1)) {
            Ok(answer) => answer
                .iter()
                .filter_map(|d| match &d.body {
                    Body::Message(Message::GetResourcesResponse(r)) => Some(r.resources.clone()),
                    _ => None,
                })
                .flatten()
                .map(|r| r.uri)
                .filter(|held| !is_ours(&uri, held))
                .collect(),
            Err(_) => Vec::new(),
        };
        if !foreign.is_empty() {
            report.findings.push(Finding::new(
                "write.dataspace.delete",
                "§21.2.2",
                "The suite deletes only a dataspace that holds nothing but what it wrote",
                Some(24),
                Verdict::NotChecked,
                format!("{uri} holds {} object(s) the suite did not write, e.g. {:?}: the dataspace is left in place", foreign.len(), foreign.iter().take(3).collect::<Vec<_>>()),
            ));
            report.notes.push(format!(
                "{uri} was left on the server: it holds objects the suite did not write."
            ));
            return;
        }
    }
    match timed(seen, "DeleteDataspaces", || {
        delete_dataspaces(client, std::slice::from_ref(&uri))
    }) {
        Ok(answers) => {
            let gone = matches!(answers.get(&uri), Some(Ok(_)));
            report.findings.push(Finding::new(
                "write.dataspace.delete",
                "§21.2.2",
                "DeleteDataspaces deletes the dataspace and answers for it by key",
                Some(24),
                if gone { Verdict::Pass } else { Verdict::Fail },
                match answers.get(&uri) {
                    Some(Ok(_)) => format!("deleted {uri}"),
                    Some(Err(why)) => format!(
                        "{uri}: refused: {} ({}) - the dataspace is left on the server",
                        why.message, why.code
                    ),
                    None => format!("{uri}: neither deleted nor refused"),
                },
            ));
            if gone
                && let Ok(answer) = ask(
                    client,
                    seen,
                    &GetDataspaces {
                        store_last_write_filter: None,
                    },
                )
            {
                let still = answer.iter().any(|d| matches!(&d.body, Body::Message(Message::GetDataspacesResponse(r)) if r.dataspaces.iter().any(|s| s.uri == uri)));
                report.findings.push(Finding::new(
                    "write.dataspace.gone",
                    "§21.2.2",
                    "A deleted dataspace is no longer listed",
                    Some(24),
                    if still { Verdict::Fail } else { Verdict::Pass },
                    if still {
                        format!("{uri} is still listed")
                    } else {
                        format!("{uri} is not listed any more")
                    },
                ));
            }
        }
        Err(why) => report.findings.push(failed(
            "write.dataspace.delete",
            "§21.2.2",
            "The store MUST answer DeleteDataspaces",
            Some(24),
            why,
        )),
    }
}

/// The URIs a dataspace lists, or `None` when it would not say.
fn listing<K: Session>(client: &mut K, seen: &mut Seen, space: &str) -> Option<Vec<String>> {
    let answer = ask(client, seen, &resources_of(space, 1)).ok()?;
    if exception_of(&answer).is_some() {
        return None;
    }
    Some(
        answer
            .iter()
            .filter_map(|d| match &d.body {
                Body::Message(Message::GetResourcesResponse(r)) => Some(r.resources.clone()),
                _ => None,
            })
            .flatten()
            .map(|r| r.uri)
            .collect(),
    )
}

/// What appeared in `space` since `before` that the suite did not write -
/// what the store made of its own accord (some stores record an
/// `obj_Activity` per transaction, referencing what it carried).
fn store_made<K: Session>(client: &mut K, seen: &mut Seen, space: &str, before: &[String]) -> Vec<String> {
    listing(client, seen, space)
        .unwrap_or_default()
        .into_iter()
        .filter(|uri| !before.contains(uri) && !is_ours(space, uri))
        .collect()
}

/// Why the suite deleted nothing: what the store added, by type.
fn kept_because(made: &[String]) -> String {
    let mut by_type: BTreeMap<String, usize> = BTreeMap::new();
    for uri in made {
        let qualified = uri
            .rsplit('/')
            .next()
            .and_then(|last| last.split('(').next())
            .unwrap_or("?")
            .to_string();
        *by_type.entry(qualified).or_insert(0) += 1;
    }
    let types: Vec<String> = by_type.iter().map(|(t, n)| format!("{n} {t}")).collect();
    format!(
        "while the suite wrote, the store added {} object(s) of its own to the dataspace ({}); they reference what the suite wrote, so deleting it would break them, and they are not the suite's to delete - nothing was deleted",
        made.len(),
        types.join(", ")
    )
}

/// The report's head says it once per dataspace.
fn note_kept(report: &mut Report, space: &str, made: &[String]) {
    let note = format!(
        "Nothing the suite wrote in {space} was deleted: {}.",
        kept_because(made)
    );
    if !report
        .notes
        .iter()
        .any(|one| one.starts_with(&format!("Nothing the suite wrote in {space}")))
    {
        report.notes.push(note);
    }
}

/// Whether `uri` is something the suite wrote: in its dataspace, with its
/// uuid prefix. The one test every delete goes through.
fn is_ours(space: &str, uri: &str) -> bool {
    // The dataspaces compared parsed, not as text: in the default dataspace
    // (`eml:///`) an object's URI has no segment of it to start with.
    let (Ok(space), Ok(object)) = (EtpUri::parse(space), EtpUri::parse(uri)) else {
        return false;
    };
    space.object_type.is_none()
        && space.dataspace == object.dataspace
        && object
            .uuid
            .is_some_and(|uuid| uuid.to_ascii_lowercase().starts_with(samples::PREFIX))
}

/// The only way the suite deletes an object: one it wrote, in the
/// dataspace it created. Anything else is refused here, never sent.
fn delete_ours<K: Session>(
    client: &mut K,
    seen: &mut Seen,
    space: &str,
    uri: &str,
) -> Result<BTreeMap<String, Result<Vec<String>, ErrorInfo>>, ClientError> {
    if !is_ours(space, uri) {
        let mut refused = BTreeMap::new();
        refused.insert(
            uri.to_string(),
            Err(ErrorInfo {
                message: format!("not deleted: {uri} is not an object this suite wrote in {space}"),
                code: EtpError::EREQUEST_DENIED.code(),
            }),
        );
        return Ok(refused);
    }
    timed(seen, "DeleteDataObjects", || {
        delete_data_objects(client, &[uri.to_string()], false)
    })
}

/// The samples of [`samples::samples`] into the suite's dataspace: each put
/// on its own, so that one refusal does not carry off the others; read
/// back; the arrays written and read back; listed; then deleted, the
/// referencing objects first.
#[allow(clippy::too_many_arguments)]
fn sample_checks<K: Session>(
    client: &mut K,
    seen: &mut Seen,
    report: &mut Report,
    space: &str,
    agreed: &[String],
    discovery: bool,
    arrays: bool,
) {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0);
    let mut uploads: Vec<Upload> = Vec::new();
    let mut created: Vec<String> = Vec::new();
    let mut array_results: Vec<(String, Verdict, String)> = Vec::new();
    // On each array written and read back: its metadata, a sub-array of it;
    // and, for the one written block by block, whether that way worked.
    let mut metadata_results: Vec<(String, Verdict, String)> = Vec::new();
    let mut subarray_results: Vec<(String, Verdict, String)> = Vec::new();
    let mut blocks_results: Vec<(String, Verdict, String)> = Vec::new();
    // What the dataspace held before the suite wrote: a fallback dataspace
    // holds other people's objects, and only what *appears* while the suite
    // writes, and is not the suite's, is the store's own doing.
    let before: Vec<String> = if discovery {
        listing(client, seen, space).unwrap_or_default()
    } else {
        Vec::new()
    };
    let transactional = client.session().protocols.contains_key(&18);
    let mut without_transaction: Option<String> = None;
    for Sample {
        qualified_type,
        uri,
        object,
        array,
        in_blocks,
    } in samples::samples(space, stamp)
    {
        let mut upload = Upload {
            qualified_type: qualified_type.clone(),
            uri: uri.clone(),
            announced: samples::announced(&qualified_type, agreed),
            ..Default::default()
        };
        // One transaction per object and its array, as a client sends a
        // package (`energyml_etp_store::push`): a store may take writes only
        // inside one, and checks references at the commit.
        let mut transaction = None;
        if transactional && without_transaction.is_none() {
            match timed(seen, "StartTransaction", || {
                start_transaction(client, &[space.to_string()], false, "retp-conformance: samples")
            }) {
                Ok(Opened::Open(opened)) => transaction = Some(opened),
                Ok(Opened::Refused(why)) | Ok(Opened::Unsupported(why)) => without_transaction = Some(why),
                Err(why) => without_transaction = Some(why.to_string()),
            }
        }
        let put = timed(seen, "PutDataObjects", || {
            put_data_objects(client, std::slice::from_ref(&object), false)
        });
        let mut accepted = matches!(put.as_ref().map(|answers| answers.get(&uri)), Ok(Some(Ok(_))));
        upload.put = match &put {
            Ok(answers) => match answers.get(&uri) {
                Some(Ok(_)) => "accepted".into(),
                Some(Err(why)) => format!("refused: {} ({})", why.message, why.code),
                None => "neither accepted nor refused".into(),
            },
            Err(why) => format!("not answered: {why}"),
        };
        // The array goes with its object, in the same transaction.
        let array_written: Option<(DataArrayIdentifier, Result<(), String>)> =
            match (&array, accepted, arrays) {
                (Some((id, values)), true, true) if in_blocks => {
                    Some((id.clone(), write_in_blocks(client, seen, id, values)))
                }
                (Some((id, values)), true, true) => {
                    let written = timed(seen, "PutDataArrays", || {
                        retp_client::arrays::write_arrays_with(
                            client,
                            &[(id.clone(), values.clone())],
                            DEFAULT_MESSAGE,
                            retp_client::arrays::Grouping::OneByOne,
                            |_| {},
                        )
                    });
                    Some((
                        id.clone(),
                        match written {
                            Ok(mut results) => match results.pop() {
                                Some(Ok(())) => Ok(()),
                                Some(Err(why)) => Err(format!("refused: {why:?}")),
                                None => Err("neither written nor refused".into()),
                            },
                            Err(why) => Err(format!("not answered: {why}")),
                        },
                    ))
                }
                _ => None,
            };
        if let Some(opened) = transaction {
            match timed(seen, "CommitTransaction", || commit_transaction(client, &opened)) {
                Ok(Ended::Done) => {}
                Ok(Ended::Refused(why)) => {
                    if accepted {
                        upload.put = format!("accepted, then refused at the commit: {why}");
                    }
                    accepted = false;
                }
                Err(why) => {
                    if accepted {
                        upload.put = format!("accepted, the commit not answered: {why}");
                    }
                    accepted = false;
                }
            }
        }
        if !accepted {
            upload.read_back = "-".into();
            upload.array = if array.is_some() {
                "not written: the object was not stored".into()
            } else {
                "-".into()
            };
            upload.deleted = "nothing to delete".into();
            uploads.push(upload);
            continue;
        }
        created.push(uri.clone());
        let uuid = EtpUri::parse(&uri).ok().and_then(|u| u.uuid).unwrap_or_default();
        upload.read_back = match ask(
            client,
            seen,
            &GetDataObjects {
                uris: [("1".to_string(), uri.clone())].into_iter().collect(),
                format: "xml".into(),
            },
        ) {
            Ok(answer) => match answer.iter().find_map(|d| match &d.body {
                Body::Message(Message::GetDataObjectsResponse(r)) => r.data_objects.get("1").cloned(),
                _ => None,
            }) {
                Some(back) if String::from_utf8_lossy(&back.data).contains(&uuid) => "same object".into(),
                Some(_) => "another object came back".into(),
                None => format!("not returned: {}", exception_of(&answer).unwrap_or_default()),
            },
            Err(why) => format!("not answered: {why}"),
        };
        upload.array = match (array, array_written) {
            (None, _) => "-".into(),
            (Some(_), None) => "not written: DataArray (9) was not agreed".into(),
            (Some(_), Some((id, Err(why)))) => {
                // An array written in blocks did not go by PutDataArrays:
                // its failure is `write.array.blocks`'s alone.
                if in_blocks {
                    blocks_results.push((id.uri.clone(), Verdict::Fail, why.clone()));
                } else {
                    array_results.push((id.uri.clone(), Verdict::Fail, why.clone()));
                }
                format!(
                    "{} {}{}: {why}",
                    id.uri,
                    id.path_in_resource,
                    if in_blocks { " (in blocks)" } else { "" }
                )
            }
            (Some(_), Some((id, Ok(())))) => {
                let (compared, said) = read_back(client, seen, &id, &mut array_results);
                if in_blocks {
                    // The blocks landed where they were sent: what the
                    // values lost on the way is `write.array.read-back`'s.
                    let (verdict, how) = match compared {
                        Some(Comparison::Equal) => (Verdict::Pass, "read back equal".to_string()),
                        Some(rounded @ Comparison::Rounded32(_)) => (
                            Verdict::Pass,
                            format!("each block where it was sent, the values {}", rounded.rounding()),
                        ),
                        _ => (Verdict::Fail, said.clone()),
                    };
                    blocks_results.push((
                        id.uri.clone(),
                        verdict,
                        format!("declared, filled in two blocks; {how}"),
                    ));
                }
                metadata_results.push(written_metadata(client, seen, &id));
                subarray_results.push(written_subarray(client, seen, &id));
                format!(
                    "{} {}{}: {said}",
                    id.uri,
                    id.path_in_resource,
                    if in_blocks { " (in blocks)" } else { "" }
                )
            }
        };
        uploads.push(upload);
    }
    if let Some(why) = &without_transaction {
        report.notes.push(format!("The store announced Transaction (18) and refused to open one for the samples ({why}): they were written without."));
    }

    // Listed in the dataspace.
    if discovery && !created.is_empty() {
        let listed: Vec<String> = match ask(client, seen, &resources_of(space, 1)) {
            Ok(answer) => answer
                .iter()
                .filter_map(|d| match &d.body {
                    Body::Message(Message::GetResourcesResponse(r)) => Some(r.resources.clone()),
                    _ => None,
                })
                .flatten()
                .map(|r| r.uri)
                .collect(),
            Err(_) => Vec::new(),
        };
        let missing: Vec<&String> = created.iter().filter(|uri| !listed.contains(uri)).collect();
        report.findings.push(Finding::new(
            "write.samples.listed",
            "§8.2.1",
            "What was put in a dataspace is listed by GetResources on it",
            Some(3),
            if missing.is_empty() {
                Verdict::Pass
            } else {
                Verdict::Fail
            },
            if missing.is_empty() {
                format!("{} object(s) put, all listed", created.len())
            } else {
                {
                    // Listed under another spelling? Said with the URI it came back as.
                    let said: Vec<String> = missing
                        .iter()
                        .map(|uri| {
                            let uuid = EtpUri::parse(uri).ok().and_then(|u| u.uuid).unwrap_or_default();
                            match listed.iter().find(|one| !uuid.is_empty() && one.contains(&uuid)) {
                                Some(other) => format!("{uri} is listed as {other}"),
                                None => format!("{uri} is not listed"),
                            }
                        })
                        .collect();
                    format!(
                        "{} resource(s) listed in {space}; {} put and not listed as put: {}",
                        listed.len(),
                        missing.len(),
                        said.join("; ")
                    )
                }
            },
        ));
    }

    // Deleted - referencing objects first, so a store that checks references
    // is not asked to break one. Not at all when the store added objects of
    // its own while the suite wrote: they reference what it wrote, and they
    // are not the suite's to delete.
    let made = if discovery && !created.is_empty() {
        store_made(client, seen, space, &before)
    } else {
        Vec::new()
    };
    let kept = (!made.is_empty()).then(|| kept_because(&made));
    if let Some(why) = &kept {
        note_kept(report, space, &made);
        for upload in uploads.iter_mut().filter(|one| created.contains(&one.uri)) {
            upload.deleted = format!("kept: {why}");
        }
    }
    for uri in created.iter().rev().filter(|_| kept.is_none()) {
        let said = match delete_ours(client, seen, space, uri) {
            Ok(answers) => match answers.get(uri) {
                Some(Ok(_)) => match held(client, seen, uri) {
                    Some(false) => "deleted".to_string(),
                    Some(true) => "answered as deleted, still there".to_string(),
                    None => "deleted (not re-read)".to_string(),
                },
                Some(Err(why)) => format!("refused: {} ({})", why.message, why.code),
                None => "neither deleted nor refused".to_string(),
            },
            Err(why) => format!("not answered: {why}"),
        };
        if let Some(upload) = uploads.iter_mut().find(|one| &one.uri == uri) {
            upload.deleted = said;
        }
    }

    // What the run adds up to.
    let announced_refused: Vec<String> = uploads
        .iter()
        .filter(|u| u.announced && u.put != "accepted")
        .map(|u| format!("{}: {}", u.qualified_type, u.put))
        .collect();
    let announced_count = uploads.iter().filter(|u| u.announced).count();
    report.findings.push(Finding::new(
        "write.samples.announced",
        "§9.2.1.2, §5.2.2",
        "PutDataObjects accepts schema-valid objects of the data object types the session agreed",
        Some(4),
        if announced_count == 0 {
            Verdict::NotChecked
        } else if announced_refused.is_empty() {
            Verdict::Pass
        } else {
            Verdict::Fail
        },
        if announced_count == 0 {
            "no sample is of a type the session agreed".to_string()
        } else if announced_refused.is_empty() {
            format!("{announced_count} object(s) of agreed types, all accepted")
        } else {
            format!("refused: {}", announced_refused.join("; "))
        },
    ));
    let unannounced: Vec<&Upload> = uploads.iter().filter(|u| !u.announced).collect();
    let taken: Vec<String> = unannounced
        .iter()
        .filter(|u| u.put == "accepted")
        .map(|u| u.qualified_type.clone())
        .collect();
    report.findings.push(Finding::new(
        "write.samples.unannounced",
        "§5.2.2, §9.2.1.2",
        "A store refuses a data object of a type the session did not agree",
        Some(4),
        if unannounced.is_empty() {
            Verdict::NotChecked
        } else if taken.is_empty() {
            Verdict::Pass
        } else {
            Verdict::Tolerated
        },
        if unannounced.is_empty() {
            "every sample is of an agreed type".to_string()
        } else if taken.is_empty() {
            format!("{} object(s) of types not agreed, all refused", unannounced.len())
        } else {
            format!("accepted types it did not announce: {}", taken.join(", "))
        },
    ));
    let not_deleted: Vec<String> = uploads
        .iter()
        .filter(|u| {
            created.contains(&u.uri) && u.deleted != "deleted" && u.deleted != "deleted (not re-read)"
        })
        .map(|u| format!("{}: {}", u.qualified_type, u.deleted))
        .collect();
    report.findings.push(Finding::new(
        "write.samples.delete",
        "§9.2.1.3",
        "DeleteDataObjects deletes each object the suite put, and it is gone afterwards",
        Some(4),
        if created.is_empty() || kept.is_some() {
            Verdict::NotChecked
        } else if not_deleted.is_empty() {
            Verdict::Pass
        } else {
            Verdict::Fail
        },
        match &kept {
            Some(why) => format!("{} object(s) kept, none deleted: {why}", created.len()),
            None if created.is_empty() => "nothing was put".to_string(),
            None if not_deleted.is_empty() => format!("{} object(s) deleted", created.len()),
            None => format!("left: {}", not_deleted.join("; ")),
        },
    ));
    let (array_put, array_back) = if array_results.is_empty() {
        (
            Finding::new(
                "write.array.put",
                "§13.2.3",
                "PutDataArrays writes an array of an object the suite put",
                Some(9),
                Verdict::NotChecked,
                "no array was written: no object carrying one was accepted",
            ),
            Finding::new(
                "write.array.read-back",
                "§13.2.1.1",
                "An array written reads back as written",
                Some(9),
                Verdict::NotChecked,
                "no array was written",
            ),
        )
    } else {
        let refused: Vec<&(String, Verdict, String)> = array_results
            .iter()
            .filter(|(_, _, said)| {
                said.starts_with("refused") || said.starts_with("not answered") || said.starts_with("neither")
            })
            .collect();
        let back_bad: Vec<&(String, Verdict, String)> = array_results
            .iter()
            .filter(|(_, verdict, _)| *verdict == Verdict::Fail)
            .collect();
        (
            Finding::new(
                "write.array.put",
                "§13.2.3",
                "PutDataArrays writes an array of an object the suite put",
                Some(9),
                if refused.is_empty() {
                    Verdict::Pass
                } else {
                    Verdict::Fail
                },
                array_results
                    .iter()
                    .map(|(uri, _, said)| format!("{uri}: {said}"))
                    .collect::<Vec<_>>()
                    .join("; "),
            ),
            Finding::new(
                "write.array.read-back",
                "§13.2.1.1",
                "An array written reads back as written",
                Some(9),
                if back_bad.is_empty() {
                    Verdict::Pass
                } else if back_bad.len() == refused.len() {
                    Verdict::NotChecked
                } else {
                    Verdict::Fail
                },
                format!(
                    "{} of {} array(s) read back equal",
                    array_results.len() - back_bad.len(),
                    array_results.len()
                ),
            ),
        )
    };
    report.findings.push(array_put);
    report.findings.push(array_back);
    report.findings.push(per_array(
        "write.array.blocks",
        "§13.2.1.2, §13.3.9, §13.3.7",
        "An array declared by PutUninitializedDataArrays and filled by PutDataSubarrays holds each block where it was sent",
        &blocks_results,
        "no array was written block by block: no object carrying one was accepted",
    ));
    report.findings.push(per_array(
        "write.array.metadata",
        "§13.3.11",
        "GetDataArrayMetadata states the dimensions an array was written with",
        &metadata_results,
        "no array was written",
    ));
    report.findings.push(per_array(
        "write.array.subarray",
        "§13.3.3, §13.2.1.1",
        "GetDataSubarrays returns the rows asked for of an array the suite wrote",
        &subarray_results,
        "no array was written",
    ));
    report.uploads.extend(uploads);
}

/// An array written as §13.2.1.2 writes one that does not fit a message:
/// declared by `PutUninitializedDataArrays` (step 1, §13.3.9), then filled
/// by `PutDataSubarrays` (step 3.b, §13.3.7), half its rows each. Sent by
/// `ask`, so that the rules of the wire see these messages too, and the
/// response must name the key: a store that answers without it has said
/// nothing of the block.
fn write_in_blocks<K: Session>(
    client: &mut K,
    seen: &mut Seen,
    id: &DataArrayIdentifier,
    array: &Array,
) -> Result<(), String> {
    let ArrayValues::Doubles(values) = &array.values else {
        return Err("the suite writes blocks of doubles only".into());
    };
    let declare = PutUninitializedDataArrays {
        data_arrays: [(
            "1".to_string(),
            PutUninitializedDataArrayType {
                uid: id.clone(),
                metadata: DataArrayMetadata {
                    dimensions: array.dimensions.iter().map(|d| *d as i64).collect(),
                    transport_array_type: array.transport,
                    logical_array_type: array.logical,
                    ..Default::default()
                },
            },
        )]
        .into_iter()
        .collect(),
    };
    said_of_one(
        ask(client, seen, &declare),
        "PutUninitializedDataArrays",
        |m| matches!(m, Message::PutUninitializedDataArraysResponse(r) if r.success.contains_key("1")),
    )?;
    let rows = array.dimensions[0];
    let width: u64 = array.dimensions[1..].iter().product();
    let half = rows / 2;
    for (first, count) in [(0, half), (half, rows - half)] {
        let mut starts = vec![0_i64; array.dimensions.len()];
        starts[0] = first as i64;
        let mut counts: Vec<i64> = array.dimensions.iter().map(|d| *d as i64).collect();
        counts[0] = count as i64;
        let block = values[(first * width) as usize..((first + count) * width) as usize].to_vec();
        let request = PutDataSubarrays {
            data_subarrays: [(
                "1".to_string(),
                PutDataSubarraysType {
                    uid: id.clone(),
                    data: AnyArray {
                        item: AnyArrayItem::ArrayOfDouble(ArrayOfDouble { values: block }),
                    },
                    starts,
                    counts,
                },
            )]
            .into_iter()
            .collect(),
        };
        said_of_one(
            ask(client, seen, &request),
            &format!("PutDataSubarrays of rows {first}..{}", first + count),
            |m| matches!(m, Message::PutDataSubarraysResponse(r) if r.success.contains_key("1")),
        )?;
    }
    Ok(())
}

/// What a store said of a one-key write: the response naming the key, or
/// why not.
fn said_of_one(
    answer: Result<Vec<Delivery>, ClientError>,
    what: &str,
    names_the_key: impl Fn(&Message) -> bool,
) -> Result<(), String> {
    let answer = answer.map_err(|why| format!("{what} not answered: {why}"))?;
    if let Some(why) = exception_of(&answer) {
        return Err(format!("{what} refused: {why}"));
    }
    if answer
        .iter()
        .any(|d| matches!(&d.body, Body::Message(m) if names_the_key(m)))
    {
        Ok(())
    } else {
        Err(format!("{what}: neither accepted nor refused"))
    }
}

/// How numbers read back stand against those written.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Comparison {
    /// The same, bit for bit.
    Equal,
    /// Each one the written double rounded to a 32-bit float: the right
    /// values at the right place, at a precision the store lost - the
    /// largest difference.
    Rounded32(f64),
    /// Something else.
    Different,
}

impl Comparison {
    fn of(numbers: &[f64], written: &[f64]) -> Comparison {
        if numbers.len() != written.len() {
            return Comparison::Different;
        }
        if numbers
            .iter()
            .zip(written)
            .all(|(a, b)| a.to_bits() == b.to_bits())
        {
            return Comparison::Equal;
        }
        if numbers
            .iter()
            .zip(written)
            .all(|(a, b)| a.to_bits() == f64::from(*b as f32).to_bits())
        {
            let largest = numbers
                .iter()
                .zip(written)
                .map(|(a, b)| (a - b).abs())
                .fold(0.0, f64::max);
            return Comparison::Rounded32(largest);
        }
        Comparison::Different
    }

    fn rounding(self) -> String {
        match self {
            Comparison::Rounded32(largest) => {
                format!("rounded to 32-bit floats - written as 64-bit doubles, largest difference {largest}")
            }
            _ => String::new(),
        }
    }
}

/// An array the suite wrote, read back whole and compared with
/// [`samples::POINTS`]; the outcome goes to `results`, and is returned -
/// `None` when nothing could be compared - with its words.
fn read_back<K: Session>(
    client: &mut K,
    seen: &mut Seen,
    id: &DataArrayIdentifier,
    results: &mut Vec<(String, Verdict, String)>,
) -> (Option<Comparison>, String) {
    let (compared, verdict, said) = match timed(seen, "GetDataArrays", || {
        read_arrays(client, std::slice::from_ref(id), DEFAULT_MESSAGE, |_| {})
    }) {
        Ok(mut back) => match back.pop() {
            Some(Ok(read)) => match read.values.to_f64s(read.logical) {
                Some(numbers) => match Comparison::of(&numbers, &samples::POINTS) {
                    Comparison::Equal => (
                        Some(Comparison::Equal),
                        Verdict::Pass,
                        "written, read back equal".to_string(),
                    ),
                    rounded @ Comparison::Rounded32(_) => (
                        Some(rounded),
                        Verdict::Fail,
                        format!(
                            "written, read back {} (the store says {:?})",
                            rounded.rounding(),
                            read.logical
                        ),
                    ),
                    Comparison::Different => (
                        Some(Comparison::Different),
                        Verdict::Fail,
                        format!("written, read back different: {numbers:?}"),
                    ),
                },
                None => (
                    None,
                    Verdict::Fail,
                    format!(
                        "written, read back in a form this suite cannot decode ({:?})",
                        read.transport
                    ),
                ),
            },
            Some(Err(why)) => (None, Verdict::Fail, format!("written, not read back: {why:?}")),
            None => (
                None,
                Verdict::Fail,
                "written, no answer reading it back".to_string(),
            ),
        },
        Err(why) => (
            None,
            Verdict::Fail,
            format!("written, GetDataArrays not answered: {why}"),
        ),
    };
    results.push((id.uri.clone(), verdict, said.clone()));
    (compared, said)
}

/// `GetDataArrayMetadata` on an array the suite wrote (§13.3.11): the
/// dimensions it was written with, `ROWS × 3`.
fn written_metadata<K: Session>(
    client: &mut K,
    seen: &mut Seen,
    id: &DataArrayIdentifier,
) -> (String, Verdict, String) {
    let expected = vec![samples::ROWS as i64, 3];
    let (verdict, said) = match timed(seen, "GetDataArrayMetadata", || {
        array_metadata(client, std::slice::from_ref(id))
    }) {
        Ok(mut answers) => match answers.pop() {
            Some(Ok(meta)) if meta.dimensions == expected => (
                Verdict::Pass,
                if meta.logical_array_type == AnyLogicalArrayType::ArrayOfDouble64Le {
                    format!("dimensions {:?}, {:?}", meta.dimensions, meta.logical_array_type)
                } else {
                    format!(
                        "dimensions {:?}; logical type {:?}, written as ArrayOfDouble64Le",
                        meta.dimensions, meta.logical_array_type
                    )
                },
            ),
            Some(Ok(meta)) => (
                Verdict::Fail,
                format!("dimensions {:?}, written as {expected:?}", meta.dimensions),
            ),
            Some(Err(why)) => (Verdict::Fail, format!("refused: {} ({})", why.message, why.code)),
            None => (Verdict::Fail, "neither answered nor refused".into()),
        },
        Err(why) => (Verdict::Fail, format!("not answered: {why}")),
    };
    (id.uri.clone(), verdict, said)
}

/// `GetDataSubarrays` on an array the suite wrote (§13.3.3): the rows
/// [`samples::SUBARRAY_ROWS`] say, every column, against the same rows of
/// what was written.
fn written_subarray<K: Session>(
    client: &mut K,
    seen: &mut Seen,
    id: &DataArrayIdentifier,
) -> (String, Verdict, String) {
    let [first, count] = samples::SUBARRAY_ROWS;
    let region = Block {
        starts: vec![first, 0],
        counts: vec![count, 3],
    };
    let expected = &samples::POINTS[(first * 3) as usize..((first + count) * 3) as usize];
    let (verdict, said) = match timed(seen, "GetDataSubarrays", || {
        read_regions(client, &[(id.clone(), Some(region))], DEFAULT_MESSAGE, |_| {})
    }) {
        Ok(mut back) => match back.pop() {
            Some(Ok(read)) => match read.values.to_f64s(read.logical) {
                Some(numbers) => match Comparison::of(&numbers, expected) {
                    Comparison::Equal => (
                        Verdict::Pass,
                        format!("rows {first}..{} read back equal", first + count),
                    ),
                    // The rows asked for; the precision is `write.array.read-back`'s.
                    rounded @ Comparison::Rounded32(_) => (
                        Verdict::Pass,
                        format!(
                            "rows {first}..{} returned, the values {}",
                            first + count,
                            rounded.rounding()
                        ),
                    ),
                    Comparison::Different => (
                        Verdict::Fail,
                        format!("rows {first}..{} asked, {numbers:?} returned", first + count),
                    ),
                },
                None => (
                    Verdict::Fail,
                    format!(
                        "returned in a form this suite cannot decode ({:?})",
                        read.transport
                    ),
                ),
            },
            Some(Err(why)) => (Verdict::Fail, format!("refused: {why:?}")),
            None => (Verdict::Fail, "neither answered nor refused".into()),
        },
        Err(why) => (Verdict::Fail, format!("not answered: {why}")),
    };
    (id.uri.clone(), verdict, said)
}

/// One finding out of per-array outcomes: `Pass` when every one passed,
/// `NotChecked` when there were none.
fn per_array(
    id: &'static str,
    section: &'static str,
    rule: &'static str,
    results: &[(String, Verdict, String)],
    none: &str,
) -> Finding {
    let verdict = if results.is_empty() {
        Verdict::NotChecked
    } else if results.iter().all(|(_, verdict, _)| *verdict == Verdict::Pass) {
        Verdict::Pass
    } else {
        Verdict::Fail
    };
    let said = if results.is_empty() {
        none.to_string()
    } else {
        results
            .iter()
            .map(|(uri, _, said)| format!("{uri}: {said}"))
            .collect::<Vec<_>>()
            .join("; ")
    };
    Finding::new(id, section, rule, Some(9), verdict, said)
}

/// A RESQML 2.0.1 feature to write: the smallest object every store that
/// speaks resqml20 takes.
pub(crate) fn feature(space: &str, uuid: &str) -> (String, DataObject) {
    let path = EtpUri::parse(space).ok().and_then(|parsed| parsed.dataspace);
    let uri = EtpUri::object(path.as_deref(), "resqml20.obj_GeneticBoundaryFeature", uuid);
    let xml = format!(
        "<resqml:obj_GeneticBoundaryFeature xmlns:resqml=\"http://www.energistics.org/energyml/data/resqmlv2\" \
xmlns:eml=\"http://www.energistics.org/energyml/data/commonv2\" uuid=\"{uuid}\" schemaVersion=\"2.0\">\
<eml:Citation><eml:Title>retp-conformance</eml:Title><eml:Originator>retp-conformance</eml:Originator>\
<eml:Creation>2026-09-17T00:00:00Z</eml:Creation><eml:Format>retp-conformance</eml:Format></eml:Citation>\
<resqml:GeneticBoundaryKind>horizon</resqml:GeneticBoundaryKind></resqml:obj_GeneticBoundaryFeature>"
    );
    let object = DataObject {
        resource: Resource {
            uri: uri.clone(),
            name: "retp-conformance".into(),
            ..Default::default()
        },
        format: "xml".into(),
        blob_id: None,
        data: xml.into_bytes(),
    };
    (uri, object)
}

/// Whether the store holds `uri`, asked plainly.
fn held<K: Session>(client: &mut K, seen: &mut Seen, uri: &str) -> Option<bool> {
    let request = GetDataObjects {
        uris: [("1".to_string(), uri.to_string())].into_iter().collect(),
        format: "xml".into(),
    };
    let answer = ask(client, seen, &request).ok()?;
    Some(answer.iter().any(|d| matches!(&d.body, Body::Message(Message::GetDataObjectsResponse(r)) if r.data_objects.contains_key("1"))))
}

/// Transaction (18) on the suite's own dataspace: a put rolled back leaves
/// nothing (§18.1.1.1 §6), a put committed stays (§18.1.1.1 §8.b.i).
fn transaction_checks<K: Session>(client: &mut K, seen: &mut Seen, report: &mut Report, space: &str) {
    // Rolled back.
    let (rolled_uri, rolled) = feature(space, &format!("{}-{}", &NOBODY[..23], stamp_hex()));
    match start_transaction(
        client,
        std::slice::from_ref(&space.to_string()),
        false,
        "retp-conformance: rollback",
    ) {
        Ok(Opened::Open(transaction)) => {
            let put = put_data_objects(client, std::slice::from_ref(&rolled), false);
            let put_ok = matches!(put.as_ref().map(|a| a.get(&rolled_uri)), Ok(Some(Ok(_))));
            let ended = rollback_transaction(client, &transaction);
            report.findings.push(match (put_ok, ended, held(client, seen, &rolled_uri)) {
                (true, Ok(Ended::Done), Some(false)) => Finding::new("transaction.rollback-leaves-nothing", "§18.1.1.1 §6, §18.2.5", "On RollbackTransaction the store MUST disregard what was sent with the transaction: the store is as it was before", Some(18), Verdict::Pass, format!("{rolled_uri}: put in the transaction, not there after the rollback")),
                (true, Ok(Ended::Done), Some(true)) => Finding::new("transaction.rollback-leaves-nothing", "§18.1.1.1 §6, §18.2.5", "A rolled-back put leaves nothing", Some(18), Verdict::Fail, format!("{rolled_uri}: still there after the rollback")),
                (true, Ok(Ended::Done), None) => Finding::new("transaction.rollback-leaves-nothing", "§18.1.1.1 §6", "A rolled-back put leaves nothing", Some(18), Verdict::NotChecked, "the store did not answer whether the object is there"),
                (true, Ok(Ended::Refused(why)), _) => Finding::new("transaction.rollback-leaves-nothing", "§18.1.1.1 §6", "A rollback is answered with successful=true", Some(18), Verdict::Fail, format!("rollback refused: {why}")),
                (true, Err(why), _) => failed("transaction.rollback-leaves-nothing", "§18.1.1.1 §6", "A rollback is answered", Some(18), why),
                (false, _, _) => Finding::new("transaction.rollback-leaves-nothing", "§18.1.1.1 §6", "A put inside a transaction is accepted", Some(18), Verdict::Fail, match put { Ok(a) => format!("{rolled_uri}: {}", a.get(&rolled_uri).map(|r| match r { Ok(_) => "ok".to_string(), Err(e) => format!("refused: {} ({})", e.message, e.code) }).unwrap_or_else(|| "not answered".into())), Err(why) => format!("not answered: {why}") }),
            });
        }
        Ok(Opened::Refused(why)) => report.findings.push(Finding::new(
            "transaction.rollback-leaves-nothing",
            "§18.1.1.1",
            "A writing transaction on one dataspace opens",
            Some(18),
            Verdict::Fail,
            format!("refused: {why}"),
        )),
        Ok(Opened::Unsupported(why)) => report.findings.push(Finding::new(
            "transaction.rollback-leaves-nothing",
            "§18.1.1.1",
            "A writing transaction on one dataspace opens",
            Some(18),
            Verdict::NotApplicable,
            format!("no transactions here: {why}"),
        )),
        Err(why) => report.findings.push(failed(
            "transaction.rollback-leaves-nothing",
            "§18.1.1.1",
            "The store MUST answer StartTransaction",
            Some(18),
            why,
        )),
    }
    // Committed.
    let (kept_uri, kept) = feature(space, &format!("{}-{}", &NOBODY[..23], stamp_hex()));
    match start_transaction(
        client,
        std::slice::from_ref(&space.to_string()),
        false,
        "retp-conformance: commit",
    ) {
        Ok(Opened::Open(transaction)) => {
            let put = put_data_objects(client, std::slice::from_ref(&kept), false);
            let put_ok = matches!(put.as_ref().map(|a| a.get(&kept_uri)), Ok(Some(Ok(_))));
            let ended = retp_client::commit_transaction(client, &transaction);
            report.findings.push(match (put_ok, ended, held(client, seen, &kept_uri)) {
                (true, Ok(Ended::Done), Some(true)) => Finding::new("transaction.commit-keeps", "§18.1.1.1 §8.b.i", "On CommitTransaction the store MUST apply all requests of the transaction and answer successful=true", Some(18), Verdict::Pass, format!("{kept_uri}: put in the transaction, there after the commit")),
                (true, Ok(Ended::Done), Some(false)) => Finding::new("transaction.commit-keeps", "§18.1.1.1 §8.b.i", "A committed put stays", Some(18), Verdict::Fail, format!("{kept_uri}: committed, not there")),
                (true, Ok(Ended::Done), None) => Finding::new("transaction.commit-keeps", "§18.1.1.1 §8.b.i", "A committed put stays", Some(18), Verdict::NotChecked, "the store did not answer whether the object is there"),
                (true, Ok(Ended::Refused(why)), _) => Finding::new("transaction.commit-keeps", "§18.1.1.1 §8.b", "A commit of one small put goes through", Some(18), Verdict::Fail, format!("commit refused: {why}")),
                (true, Err(why), _) => failed("transaction.commit-keeps", "§18.1.1.1 §8", "A commit is answered", Some(18), why),
                (false, _, _) => Finding::new("transaction.commit-keeps", "§18.1.1.1", "A put inside a transaction is accepted", Some(18), Verdict::Fail, match put { Ok(a) => format!("{kept_uri}: {}", a.get(&kept_uri).map(|r| match r { Ok(_) => "ok".to_string(), Err(e) => format!("refused: {} ({})", e.message, e.code) }).unwrap_or_else(|| "not answered".into())), Err(why) => format!("not answered: {why}") }),
            });
            // Cleaned up, whatever the verdict: the dataspace goes next anyway.
            if store_made(client, seen, space, &[]).is_empty() {
                let _ = delete_ours(client, seen, space, &kept_uri);
            }
        }
        Ok(Opened::Refused(why)) => report.findings.push(Finding::new(
            "transaction.commit-keeps",
            "§18.1.1.1",
            "A writing transaction on one dataspace opens",
            Some(18),
            Verdict::Fail,
            format!("refused: {why}"),
        )),
        Ok(Opened::Unsupported(why)) => report.findings.push(Finding::new(
            "transaction.commit-keeps",
            "§18.1.1.1",
            "A writing transaction on one dataspace opens",
            Some(18),
            Verdict::NotApplicable,
            format!("no transactions here: {why}"),
        )),
        Err(why) => report.findings.push(failed(
            "transaction.commit-keeps",
            "§18.1.1.1",
            "The store MUST answer StartTransaction",
            Some(18),
            why,
        )),
    }
}

/// The rules on message ids, correlation and the FIN bit, over every header
/// the run received.
fn wire_findings(seen: &Seen) -> Vec<Finding> {
    let mut out = Vec::new();
    if seen.headers.is_empty() {
        out.push(Finding::new(
            "wire.message-ids",
            "§3.5.4.1",
            "Server message ids are odd, unique and strictly increasing",
            None,
            Verdict::NotChecked,
            "nothing was received",
        ));
        return out;
    }
    let even: Vec<i64> = seen
        .headers
        .iter()
        .map(|(h, _, _)| h.message_id)
        .filter(|id| id % 2 == 0)
        .collect();
    let mut last = 0;
    let mut not_increasing = Vec::new();
    for (n, (header, _, _)) in seen.headers.iter().enumerate() {
        // Ids are the session's: a session opened again starts them over.
        if seen.sessions_from.contains(&n) {
            last = 0;
        }
        if header.message_id <= last {
            not_increasing.push((last, header.message_id));
        }
        last = last.max(header.message_id);
    }
    out.push(Finding::new(
        "wire.message-ids",
        "§3.5.4.1 §3.b, §3.c.ii",
        "The server side MUST use only non-zero odd message ids, unique within the session and strictly increasing",
        None,
        if even.is_empty() && not_increasing.is_empty() { Verdict::Pass } else { Verdict::Fail },
        if even.is_empty() && not_increasing.is_empty() {
            format!("{} message(s) received, ids odd and increasing", seen.headers.len())
        } else {
            format!("even ids: {even:?}; not increasing: {not_increasing:?}")
        },
    ));
    // A response correlated to anything but our request would never have
    // reached it - the request would have timed out and its check said so.
    // What can still be seen here is the other half: a message the wire
    // could place nowhere, an exception correlated to 0 about a request.
    let answered = seen
        .headers
        .iter()
        .filter(|(_, place, _)| matches!(place, Place::Answers { .. }))
        .count();
    let misplaced: Vec<String> = seen
        .headers
        .iter()
        .filter(|(_, place, _)| !matches!(place, Place::Answers { .. } | Place::Acknowledges { .. }))
        .map(|(h, place, _)| format!("#{} {:?}", h.message_id, place))
        .collect();
    let fine = misplaced.is_empty() && seen.stray.is_empty();
    out.push(Finding::new(
        "wire.correlation",
        "§3.5.4.1 §4.c, §3.7.2.1 §3.b, §3.7.3 §7",
        "A response, or an exception about a request, MUST carry the messageId of that request as correlationId",
        None,
        if fine { Verdict::Pass } else { Verdict::Fail },
        if fine {
            format!("{answered} response(s), each correlated to its request")
        } else {
            format!("misplaced: {misplaced:?}; stray: {:?}", seen.stray)
        },
    ));
    let finished = seen.headers.iter().any(|(_, _, last)| *last);
    out.push(Finding::new(
        "wire.fin",
        "§3.5.4.1 §5.a.i, §3.7.3 §6",
        "The FIN bit MUST be set on the final message of every response",
        None,
        if finished { Verdict::Pass } else { Verdict::Fail },
        if finished {
            "every action received ended with a FIN".to_string()
        } else {
            "no message carried the FIN bit".to_string()
        },
    ));
    out
}

// --- helpers -------------------------------------------------------------------------

/// One request, its deliveries kept for the wire's rules.
pub(crate) fn ask<K: Session, M: EtpMessage>(
    client: &mut K,
    seen: &mut Seen,
    request: &M,
) -> Result<Vec<Delivery>, ClientError> {
    // A new session since the last request: its ids start over.
    let current = client.generation();
    if seen.session.is_some_and(|before| before != current) {
        seen.sessions_from.push(seen.headers.len());
    }
    seen.session = Some(current);
    let started = std::time::Instant::now();
    let answer = retp_client::blocking(retp_client::Caller::call(client, request));
    let took = started.elapsed();
    match &answer {
        Ok(deliveries) => {
            let refused = deliveries.iter().any(|d| d.exception().is_some());
            seen.record(
                M::FULL_NAME,
                took,
                if refused {
                    CallOutcome::Refused
                } else {
                    CallOutcome::Answered
                },
            );
            seen.note(deliveries);
        }
        Err(_) => seen.record(M::FULL_NAME, took, CallOutcome::Failed),
    }
    answer
}

/// A request made through one of the client's operations (they send the
/// message themselves): timed and noted like [`ask`]'s. A map answer whose
/// keys were refused still counts as answered - the refusal is the
/// operation's result, read by the check.
pub(crate) fn timed<T, E>(
    seen: &mut Seen,
    request: &str,
    call: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    let started = std::time::Instant::now();
    let answer = call();
    seen.record(
        request,
        started.elapsed(),
        if answer.is_ok() {
            CallOutcome::Answered
        } else {
            CallOutcome::Failed
        },
    );
    answer
}

/// The error a response carried, if any, as a sentence.
pub(crate) fn exception_of(deliveries: &[Delivery]) -> Option<String> {
    deliveries.iter().find_map(|delivery| {
        delivery.exception().map(|exception| match exception {
            Exception::Single(info) => format!("{} ({})", info.message, info.code),
            Exception::Map(map) => map
                .values()
                .map(|info| format!("{} ({})", info.message, info.code))
                .collect::<Vec<_>>()
                .join("; "),
        })
    })
}

/// A request that must be refused with `expected`, and what came instead.
#[allow(clippy::too_many_arguments)]
fn expect_error<K: Session, M: EtpMessage>(
    client: &mut K,
    seen: &mut Seen,
    report: &mut Report,
    id: &'static str,
    section: &'static str,
    rule: &'static str,
    protocol: Option<i32>,
    request: &M,
    expected: EtpError,
) {
    let finding = match ask(client, seen, request) {
        Ok(answer) => {
            let codes: Vec<i32> = answer
                .iter()
                .filter_map(|d| d.exception())
                .flat_map(|e| match e {
                    Exception::Single(info) => vec![info.code],
                    Exception::Map(map) => map.values().map(|info| info.code).collect(),
                })
                .collect();
            let positive = answer
                .iter()
                .any(|d| d.exception().is_none() && matches!(d.body, Body::Message(_)));
            if codes.contains(&expected.code()) {
                Finding::new(
                    id,
                    section,
                    rule,
                    protocol,
                    Verdict::Pass,
                    format!("{} ({})", expected.name(), expected.code()),
                )
            } else if codes.is_empty() && positive {
                Finding::new(
                    id,
                    section,
                    rule,
                    protocol,
                    Verdict::Fail,
                    format!(
                        "answered as if it were fine, instead of {} ({})",
                        expected.name(),
                        expected.code()
                    ),
                )
            } else if codes.is_empty() {
                Finding::new(
                    id,
                    section,
                    rule,
                    protocol,
                    Verdict::Fail,
                    format!(
                        "no error and no answer, instead of {} ({})",
                        expected.name(),
                        expected.code()
                    ),
                )
            } else {
                let named: Vec<String> = codes
                    .iter()
                    .map(|code| {
                        EtpError::from_code(*code)
                            .map(|e| format!("{} ({code})", e.name()))
                            .unwrap_or_else(|| format!("code {code}"))
                    })
                    .collect();
                Finding::new(
                    id,
                    section,
                    rule,
                    protocol,
                    Verdict::Fail,
                    format!(
                        "{} instead of {} ({})",
                        named.join(", "),
                        expected.name(),
                        expected.code()
                    ),
                )
            }
        }
        Err(ClientError::Invalid(incident)) => Finding::new(
            id,
            section,
            rule,
            protocol,
            Verdict::NotChecked,
            format!("the client would not send it: {}", incident.message()),
        ),
        Err(why) => Finding::new(
            id,
            section,
            rule,
            protocol,
            Verdict::Fail,
            format!("not answered: {why}"),
        ),
    };
    report.findings.push(finding);
}

pub(crate) fn resources_of(uri: &str, depth: i32) -> GetResources {
    GetResources {
        context: ContextInfo {
            uri: uri.to_string(),
            depth,
            data_object_types: Vec::new(),
            navigable_edges: RelationshipKind::Primary,
            ..Default::default()
        },
        scope: ContextScopeKind::Targets,
        count_objects: false,
        include_edges: false,
        ..Default::default()
    }
}

/// §3.12.5.2 §f over a list of resources or dataspaces: storeLastWrite is
/// never earlier than storeCreated. A zero on both is *not stated*, which
/// this does not judge.
fn timestamps<'a>(
    id: &'static str,
    protocol: Option<i32>,
    items: impl Iterator<Item = (&'a str, i64, i64)>,
) -> Finding {
    let mut stated = 0;
    let mut wrong = Vec::new();
    for (uri, created, written) in items {
        if created == 0 && written == 0 {
            continue;
        }
        stated += 1;
        if written < created {
            wrong.push(uri.to_string());
        }
    }
    Finding::new(
        id,
        "§3.12.5.2 §f",
        "storeLastWrite MUST always be equal to or more recent than storeCreated",
        protocol,
        if stated == 0 {
            Verdict::NotChecked
        } else if wrong.is_empty() {
            Verdict::Pass
        } else {
            Verdict::Fail
        },
        if stated == 0 {
            "no timestamps stated".to_string()
        } else if wrong.is_empty() {
            format!("{stated} item(s) with storeLastWrite >= storeCreated")
        } else {
            format!(
                "storeLastWrite before storeCreated on {:?}",
                wrong.iter().take(5).collect::<Vec<_>>()
            )
        },
    )
}

pub(crate) fn failed(
    id: &'static str,
    section: &'static str,
    rule: &'static str,
    protocol: Option<i32>,
    why: ClientError,
) -> Finding {
    Finding::new(
        id,
        section,
        rule,
        protocol,
        Verdict::Fail,
        format!("not answered: {why}"),
    )
}

pub(crate) fn not_applicable(id: &'static str, section: &'static str, protocol: Option<i32>) -> Finding {
    Finding::new(
        id,
        section,
        "A protocol is mandatory in full once announced (§3.1.1.2 §3)",
        protocol,
        Verdict::NotApplicable,
        format!(
            "protocol {} was not agreed for this session",
            protocol.unwrap_or(0)
        ),
    )
}

fn hex(uuid: &Uuid) -> String {
    let h: String = uuid.0.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// A stamp for the names this suite makes: seconds since the epoch, or a
/// counter where no clock is to be had.
fn stamp() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_else(|_| "0".into())
}

fn stamp_hex() -> String {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros())
        .unwrap_or(0);
    format!("{:012x}", n & 0xffff_ffff_ffff)
}
