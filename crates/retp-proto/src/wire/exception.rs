//! The shape of a `ProtocolException`: one error, **or** a map of them.

use std::collections::BTreeMap;
use std::fmt;

use retp_types::datatypes::ErrorInfo;
use retp_types::protocol::core::ProtocolException;

/// What a `ProtocolException` says - in a type where the forbidden cases do
/// not exist.
///
/// The schema has two fields, `error` and `errors`, and the specification
/// says to fill exactly one: both at once is "undefined behavior", neither is
/// no exception at all (§3.7.2.1 §4.c, p.65). As two optional fields that is
/// four states to check at every use; as this, two.
#[derive(Debug, Clone, PartialEq)]
pub enum Exception {
    /// One error for the whole message - or a terminating error for a whole
    /// request, or `EMULTIPART_CANCELLED`.
    Single(ErrorInfo),
    /// One error per key of a map request (§3.7.3 §4.d-5).
    Map(BTreeMap<String, ErrorInfo>),
}

/// A received `ProtocolException` that fills both fields, or neither.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MalformedException {
    /// `error` and `errors` both populated.
    Both,
    /// Neither populated.
    Neither,
}

impl fmt::Display for MalformedException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MalformedException::Both => write!(
                f,
                "a ProtocolException populates both `error` and `errors` (§3.7.2.1 §4.c: undefined)"
            ),
            MalformedException::Neither => {
                write!(f, "a ProtocolException populates neither `error` nor `errors`")
            }
        }
    }
}

impl std::error::Error for MalformedException {}

impl Exception {
    /// The message body.
    pub fn into_message(self) -> ProtocolException {
        match self {
            Exception::Single(error) => ProtocolException {
                error: Some(error),
                errors: BTreeMap::new(),
            },
            Exception::Map(errors) => ProtocolException { error: None, errors },
        }
    }

    /// Read a received body, refusing the two shapes the specification forbids.
    pub fn from_message(message: &ProtocolException) -> Result<Exception, MalformedException> {
        match (&message.error, message.errors.is_empty()) {
            (Some(_), false) => Err(MalformedException::Both),
            (None, true) => Err(MalformedException::Neither),
            (Some(error), true) => Ok(Exception::Single(error.clone())),
            (None, false) => Ok(Exception::Map(message.errors.clone())),
        }
    }
}
