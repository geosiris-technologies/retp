//! The error codes, and the registry an endpoint extends with its own.
//!
//! A `ProtocolException` carries a bare integer and a message; the meaning of
//! the integer is in chapter 24 of the specification, not in any schema. So
//! the 37 Energistics codes are an enumeration here, held against
//! `rc/etp-1.2/error-codes.json` by `error_enum_matches_the_declared_table`,
//! and an application adds the codes the specification leaves to it:
//!
//! - **custom codes are negative** (§24.1 rule 2, p.482);
//! - **a domain code** - published by an ML in its ETP implementation guide
//!   (§24.2) - is positive, and comes with the name of its publisher;
//! - **nothing added replaces a code of the specification.** This is the one
//!   registry of the repository where the caller's entry is *not* tried first:
//!   what code 3 means is fixed by Energistics, and redefining it would make
//!   every message carrying it lie.
//!
//! An **incident** is a code plus what the endpoint knows about this one
//! occurrence. The message that goes on the wire is the name, then that
//! detail - or the description when there is none - as §24 recommends.

use std::collections::BTreeMap;
use std::fmt;

use retp_types::datatypes::ErrorInfo;

macro_rules! codes {
    ($($name:ident = $code:literal, $description:literal;)*) => {
        /// An error code of ETP v1.2 (§24.3, p.482-486), under the name the
        /// specification gives it.
        ///
        /// The names are the specification's, verbatim, so that a search for
        /// `EINVALID_MESSAGETYPE` finds this and the front end at once.
        #[allow(non_camel_case_types)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum EtpError {
            $(
                #[doc = $description]
                $name,
            )*
        }

        impl EtpError {
            /// How many codes the specification defines.
            pub const COUNT: usize = [$(stringify!($name)),*].len();

            /// Every code, in the order of the table.
            pub const ALL: [EtpError; Self::COUNT] = [$(EtpError::$name),*];

            /// The number that travels.
            pub const fn code(self) -> i32 {
                match self {
                    $(EtpError::$name => $code,)*
                }
            }

            /// The specification's name for it.
            pub const fn name(self) -> &'static str {
                match self {
                    $(EtpError::$name => stringify!($name),)*
                }
            }

            /// One line on what it means - ours, not the specification's text.
            pub const fn description(self) -> &'static str {
                match self {
                    $(EtpError::$name => $description,)*
                }
            }
        }
    };
}

codes! {
    ENOROLE = 1, "The requested role is not one this endpoint plays.";
    ENOSUPPORTEDPROTOCOLS = 2, "No protocol in common between the two endpoints.";
    EINVALID_MESSAGETYPE = 3, "The messageType is unknown for that protocol.";
    EUNSUPPORTED_PROTOCOL = 4, "A message arrived for a protocol not negotiated.";
    EINVALID_ARGUMENT = 5, "A field in the message body is unusable.";
    EREQUEST_DENIED = 6, "Refused on purpose; the message should say why.";
    ENOTSUPPORTED = 7, "The operation itself is not implemented here.";
    EINVALID_STATE = 8, "Legal message, wrong moment in the protocol.";
    EINVALID_URI = 9, "Malformed URI, or an alternate form the peer does not accept.";
    EAUTHORIZATION_EXPIRED = 10, "Authorization ran out; the session is about to end.";
    ENOT_FOUND = 11, "No such resource, data object, part or range.";
    ELIMIT_EXCEEDED = 12, "A stated capability limit was passed, with no finer code available.";
    ECOMPRESSION_NOTSUPPORTED = 13, "A Core (protocol 0) message arrived compressed, which is never allowed.";
    EINVALID_OBJECT = 14, "The data object sent does not parse or does not validate.";
    EMAX_TRANSACTIONS_EXCEEDED = 15, "Too many concurrent transactions.";
    EDATAOBJECTTYPE_NOTSUPPORTED = 16, "That data object type was not agreed for this session.";
    EMAXSIZE_EXCEEDED = 17, "A single object, part or array is over the declared maximum.";
    EMULTIPART_CANCELLED = 18, "A multipart request, response or notification was abandoned.";
    EINVALID_MESSAGE = 19, "The message header could not be deserialized; sent with protocol 0 and correlationId 0.";
    EINVALID_INDEXKIND = 20, "The index kind does not match the channel or growing object.";
    ENOSUPPORTEDFORMATS = 21, "No data format in common (xml, json, ...).";
    EREQUESTUUID_REJECTED = 22, "The requestUuid offered for a subscription was refused.";
    EUPDATEGROWINGOBJECT_DENIED = 23, "A growing object cannot be updated that way.";
    EBACKPRESSURE_LIMIT_EXCEEDED = 24, "Send queue exhausted; the sender closes the session right after.";
    EBACKPRESSURE_WARNING = 25, "Send queue filling up; slow down.";
    ETIMED_OUT = 26, "A response or multipart timeout period was passed.";
    EAUTHORIZATION_REQUIRED = 27, "Sent during session negotiation only, to ask for authorization.";
    EAUTHORIZATION_EXPIRING = 28, "Warning: renew the authorization or the connection ends.";
    ENOSUPPORTEDDATAOBJECTTYPES = 29, "No data object type in common.";
    ERESPONSECOUNT_EXCEEDED = 30, "A non-map response was cut short at the declared response limit; there is more data.";
    EINVALID_APPEND = 31, "The append would break index ordering.";
    EINVALID_OPERATION = 32, "The operation is not valid for that object or that state.";
    EINVALID_CHANNELID = 1002, "Unknown channel id.";
    ENOCASCADE_DELETE = 4003, "Deleting a container would orphan contained objects, and cascade was not requested.";
    EPLURAL_OBJECT = 4004, "One URI resolved to several objects where one was required.";
    ERETENTION_PERIOD_EXCEEDED = 5001, "The change history asked for is older than the store keeps; a full reload is needed.";
    ENOTGROWINGOBJECT = 6001, "The URI names an object that is not a growing object.";
}

impl EtpError {
    /// The Energistics code with this number, if there is one.
    pub fn from_code(code: i32) -> Option<EtpError> {
        EtpError::ALL.into_iter().find(|e| e.code() == code)
    }

    /// This code, for an incident the endpoint has nothing more to say about.
    pub fn incident(self) -> Incident {
        Incident {
            code: self.code(),
            name: self.name().to_string(),
            description: self.description().to_string(),
            detail: None,
        }
    }

    /// This code, and what the endpoint knows about this occurrence.
    pub fn with_detail(self, detail: impl Into<String>) -> Incident {
        Incident {
            detail: Some(detail.into()),
            ..self.incident()
        }
    }
}

impl fmt::Display for EtpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.name(), self.code())
    }
}

/// One occurrence of an error: a code, and what is known about this one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Incident {
    code: i32,
    name: String,
    description: String,
    detail: Option<String>,
}

impl Incident {
    /// The code that travels.
    pub fn code(&self) -> i32 {
        self.code
    }

    /// The code's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// What the endpoint added about this occurrence.
    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }

    /// The text that goes on the wire: the name, then the detail - or the
    /// description when there is no detail. Never the detail alone: a message
    /// that does not name its code has to be decoded by the number.
    pub fn message(&self) -> String {
        match &self.detail {
            Some(detail) => format!("{}: {detail}", self.name),
            None => format!("{}: {}", self.name, self.description),
        }
    }

    /// The record a `ProtocolException` carries.
    pub fn error_info(&self) -> ErrorInfo {
        ErrorInfo {
            message: self.message(),
            code: self.code,
        }
    }
}

impl From<EtpError> for Incident {
    fn from(error: EtpError) -> Incident {
        error.incident()
    }
}

impl fmt::Display for Incident {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.message(), self.code)
    }
}

/// Where a code the specification does not define comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Origin {
    /// This endpoint's own: a negative number (§24.1 rule 2).
    Custom,
    /// A domain code an ML publishes in its ETP implementation guide (§24.2):
    /// a positive number, with who published it.
    Domain {
        /// `RESQML`, `WITSML`, `PRODML`, and the document it comes from.
        publisher: String,
    },
}

/// A code added to the registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeEntry {
    /// The number.
    pub code: i32,
    /// Its name, in the style of the specification's.
    pub name: String,
    /// One line on what it means.
    pub description: String,
    /// Where it comes from.
    pub origin: Origin,
}

/// Why a code was not added.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryRefusal {
    /// 0 is no code.
    Zero,
    /// The number is an Energistics code; its meaning is not the caller's to set.
    OverridesSpec {
        /// The code already defined.
        existing: EtpError,
    },
    /// A positive number with no publisher: it would take a number
    /// Energistics may assign tomorrow (§24.1 rule 1).
    PositiveWithoutSource {
        /// The number offered.
        code: i32,
    },
    /// A domain code is positive; a negative one is custom.
    NegativeDomainCode {
        /// The number offered.
        code: i32,
    },
    /// The number is registered already.
    Duplicate {
        /// The number offered.
        code: i32,
    },
}

impl fmt::Display for RegistryRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RegistryRefusal::Zero => write!(f, "0 is not an error code"),
            RegistryRefusal::OverridesSpec { existing } => write!(
                f,
                "{} is Energistics' {existing}; an endpoint adds codes, it does not redefine them",
                existing.code()
            ),
            RegistryRefusal::PositiveWithoutSource { code } => write!(
                f,
                "{code} is positive, and positive codes are reserved to Energistics and the MLs \
                 (§24.1): a custom code is negative, a domain code names its publisher"
            ),
            RegistryRefusal::NegativeDomainCode { code } => {
                write!(f, "{code} is negative, so it is a custom code, not a domain one")
            }
            RegistryRefusal::Duplicate { code } => write!(f, "{code} is registered already"),
        }
    }
}

impl std::error::Error for RegistryRefusal {}

/// What a received code is, read against the registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Known<'a> {
    /// One of the specification's.
    Energistics(EtpError),
    /// One this endpoint registered.
    Registered(&'a CodeEntry),
    /// Neither - named, never `None`: *an absence and a mistake are not the
    /// same news*.
    Unknown {
        /// The number received.
        code: i32,
        /// Positive and unknown: either a code Energistics defined after
        /// v1.2, or a peer using a positive custom code, which §24.1 forbids.
        /// Either way it cannot be read as a custom code.
        suspicious: bool,
    },
}

/// The codes an endpoint understands: the specification's, and its own.
#[derive(Debug, Clone, Default)]
pub struct ErrorRegistry {
    added: BTreeMap<i32, CodeEntry>,
}

impl ErrorRegistry {
    /// The 37 codes of the specification, and nothing else.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a code, or say why not.
    pub fn register(&mut self, entry: CodeEntry) -> Result<(), RegistryRefusal> {
        let code = entry.code;
        if code == 0 {
            return Err(RegistryRefusal::Zero);
        }
        if let Some(existing) = EtpError::from_code(code) {
            return Err(RegistryRefusal::OverridesSpec { existing });
        }
        match (&entry.origin, code > 0) {
            (Origin::Custom, true) => return Err(RegistryRefusal::PositiveWithoutSource { code }),
            (Origin::Domain { .. }, false) => {
                return Err(RegistryRefusal::NegativeDomainCode { code });
            }
            _ => {}
        }
        if self.added.contains_key(&code) {
            return Err(RegistryRefusal::Duplicate { code });
        }
        self.added.insert(code, entry);
        Ok(())
    }

    /// What `code` is.
    pub fn lookup(&self, code: i32) -> Known<'_> {
        if let Some(error) = EtpError::from_code(code) {
            return Known::Energistics(error);
        }
        match self.added.get(&code) {
            Some(entry) => Known::Registered(entry),
            None => Known::Unknown {
                code,
                suspicious: code > 0,
            },
        }
    }

    /// An incident of a registered or Energistics code; `None` for a code
    /// nobody defined - an endpoint does not send what it cannot name.
    pub fn incident(&self, code: i32, detail: Option<String>) -> Option<Incident> {
        let (name, description) = match self.lookup(code) {
            Known::Energistics(error) => (error.name().to_string(), error.description().to_string()),
            Known::Registered(entry) => (entry.name.clone(), entry.description.clone()),
            Known::Unknown { .. } => return None,
        };
        Some(Incident {
            code,
            name,
            description,
            detail,
        })
    }
}
