//! How the Rust client's failures are named for Python.
//!
//! The classification is checked against a second source: `ClientError`'s own
//! `is_connection_lost`, which the library defines. The bridge must agree with
//! it on every variant, or a Python caller would reconnect (or not) against
//! the library's own advice.

use retp_client::{ClientError, TransportError};
use retp_proto::negotiate::OfferRefusal;
use retp_proto::wire::EtpError;
use retp_python::session::{Failure, Kind};

/// Every variant of `ClientError` that can be built here, with the kind the
/// bridge must give it.
fn cases() -> Vec<(ClientError, Kind)> {
    vec![
        (ClientError::Offer(OfferRefusal::CoreListed), Kind::SessionRefused),
        (
            ClientError::Transport(TransportError::new("connection refused")),
            Kind::ConnectionLost,
        ),
        (
            ClientError::Refused(EtpError::EUNSUPPORTED_PROTOCOL.with_detail("no")),
            Kind::SessionRefused,
        ),
        (
            ClientError::AuthorizationNeeded {
                challenges: vec!["Bearer realm=\"x\"".into()],
            },
            Kind::AuthorizationNeeded,
        ),
        (ClientError::Closed { reason: "bye".into() }, Kind::ConnectionLost),
        (
            ClientError::Invalid(EtpError::EINVALID_ARGUMENT.with_detail("not canonical")),
            Kind::NotSent,
        ),
        (
            ClientError::TimedOut {
                request: 7,
                incident: EtpError::ETIMED_OUT.with_detail("slow"),
            },
            Kind::TimedOut,
        ),
        (
            ClientError::AnswerRefused {
                request: 7,
                incident: EtpError::EINVALID_MESSAGE.with_detail("garbled"),
            },
            Kind::AnswerRefused,
        ),
        (
            ClientError::Interrupted {
                request: 9,
                sent: true,
                confirmed: false,
                why: TransportError::new("reset"),
            },
            Kind::OutcomeUnknown,
        ),
    ]
}

#[test]
fn every_client_error_gets_the_kind_a_caller_acts_on() {
    for (error, kind) in cases() {
        let shown = format!("{error:?}");
        assert_eq!(Failure::from(error).kind, kind, "{shown}");
    }
}

#[test]
fn the_bridge_agrees_with_the_library_on_what_is_a_lost_connection() {
    // A second source for the same fact: the library's own predicate.
    for (error, _) in cases() {
        let lost = error.is_connection_lost();
        let shown = format!("{error:?}");
        let kind = Failure::from(error).kind;
        assert_eq!(
            lost,
            matches!(kind, Kind::ConnectionLost | Kind::OutcomeUnknown),
            "{shown}: the library says lost = {lost}, the bridge says {kind:?}"
        );
    }
}

#[test]
fn the_library_s_words_are_kept() {
    let error = ClientError::Closed {
        reason: "the server went away".into(),
    };
    let expected = error.to_string();
    assert_eq!(Failure::from(error).message, expected);
}

#[test]
fn an_unknown_outcome_says_it_is_unknown() {
    let (error, _) = cases().pop().expect("the last case is Interrupted");
    let failure = Failure::from(error);
    assert_eq!(failure.kind, Kind::OutcomeUnknown);
    assert!(
        failure.message.contains("unknown"),
        "a retry of a write may write twice; the message must say so: {}",
        failure.message
    );
}
