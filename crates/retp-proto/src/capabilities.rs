//! Capabilities, read through one typed table.
//!
//! A capability travels as an entry of a `map<DataValue>` - in the
//! `ServerCapabilities` of the probe, in `RequestSession` and `OpenSession`,
//! per protocol and per data object type. What each one means, its type, its
//! bounds and its default are in the specification's prose, not in a schema:
//! [`DEFINITIONS`] carries them, mirrored from `rc/etp-1.2/capabilities.json`
//! and held to it by `capability_table_matches_the_kind_enums`.
//!
//! Three rules, each tested:
//!
//! - **a capability this table does not know is ignored**, never fatal - a
//!   client "MUST ignore any capabilities it does not understand" (§4.3.1 §4.a),
//!   and OSDU's server announces some;
//! - **a value outside its bounds is named and brought to the bound** - the
//!   decision the specification left open: refusing a session over a timeout would turn a
//!   peer's misconfiguration into an outage, and taking the value as it is
//!   would put this end outside the specification;
//! - **a value of the wrong type is named and not used**: a `long` sent as a
//!   string is not guessed at. An `int` is taken for a `long`, the one widening
//!   that loses nothing.

use std::collections::BTreeMap;
use std::fmt;

use retp_types::datatypes::{ArrayOfString, DataValue, DataValueItem};

/// Where a capability applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Scope {
    /// The endpoint, in every protocol.
    Endpoint,
    /// One protocol.
    Protocol,
    /// One data object type.
    DataObject,
}

/// The type a capability's value has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// An Avro `long`.
    Long,
    /// An Avro `boolean`.
    Boolean,
    /// An `ArrayOfString`.
    ArrayOfString,
}

/// How two values of a capability combine, where the specification says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Combine {
    /// Nothing is said: each end's value governs what the other must do.
    Own,
    /// The session uses the smaller of the two endpoints' values.
    SmallerOfTwo,
    /// A data object's value overrides the endpoint's; a protocol's may lower
    /// it and never raise it.
    ObjectOverrides,
}

/// A default the specification gives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefaultValue {
    /// A number.
    Long(i64),
    /// A flag.
    Boolean(bool),
}

/// One capability of ETP v1.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Definition {
    /// Its name, as it travels.
    pub name: &'static str,
    /// Where it applies.
    pub scopes: &'static [Scope],
    /// The type of its value.
    pub kind: Kind,
    /// Its unit, or `""`.
    pub unit: &'static str,
    /// The smallest value allowed.
    pub min: Option<i64>,
    /// The largest value allowed.
    pub max: Option<i64>,
    /// Its value when an endpoint says nothing.
    pub default: Option<DefaultValue>,
    /// How two ends' values combine.
    pub combine: Combine,
    /// Where the specification defines it.
    pub section: &'static str,
}

use Scope::{DataObject as D, Endpoint as E, Protocol as P};

/// Every capability ETP v1.2 defines.
pub static DEFINITIONS: &[Definition] = &[
    Definition {
        name: "ActiveTimeoutPeriod",
        scopes: &[E, D],
        kind: Kind::Long,
        unit: "seconds",
        min: Some(60),
        max: None,
        default: Some(DefaultValue::Long(3600)),
        combine: Combine::ObjectOverrides,
        section: "3.3.2.1",
    },
    Definition {
        name: "AuthorizationDetails",
        scopes: &[E],
        kind: Kind::ArrayOfString,
        unit: "",
        min: None,
        max: None,
        default: None,
        combine: Combine::Own,
        section: "5.2.3",
    },
    Definition {
        name: "ChangePropagationPeriod",
        scopes: &[E],
        kind: Kind::Long,
        unit: "seconds",
        min: Some(1),
        max: Some(600),
        default: Some(DefaultValue::Long(5)),
        combine: Combine::Own,
        section: "3.3.2.2",
    },
    Definition {
        name: "ChangeRetentionPeriod",
        scopes: &[E],
        kind: Kind::Long,
        unit: "seconds",
        min: Some(86400),
        max: None,
        default: Some(DefaultValue::Long(86400)),
        combine: Combine::Own,
        section: "23.4",
    },
    Definition {
        name: "MaxConcurrentMultipart",
        scopes: &[E],
        kind: Kind::Long,
        unit: "messages",
        min: Some(1),
        max: None,
        default: None,
        combine: Combine::Own,
        section: "3.7.3.1.1.1",
    },
    Definition {
        name: "MaxDataObjectSize",
        scopes: &[E, P, D],
        kind: Kind::Long,
        unit: "bytes",
        min: Some(100000),
        max: None,
        default: None,
        combine: Combine::ObjectOverrides,
        section: "3.3.2.4",
    },
    Definition {
        name: "MaxPartSize",
        scopes: &[E],
        kind: Kind::Long,
        unit: "bytes",
        min: Some(10000),
        max: None,
        default: None,
        combine: Combine::Own,
        section: "3.3.2.5",
    },
    Definition {
        name: "MaxSessionClientCount",
        scopes: &[E],
        kind: Kind::Long,
        unit: "sessions",
        min: Some(2),
        max: None,
        default: None,
        combine: Combine::Own,
        section: "3.3.2.6",
    },
    Definition {
        name: "MaxSessionGlobalCount",
        scopes: &[E],
        kind: Kind::Long,
        unit: "sessions",
        min: Some(2),
        max: None,
        default: None,
        combine: Combine::Own,
        section: "3.3.2.7",
    },
    Definition {
        name: "MaxWebSocketFramePayloadSize",
        scopes: &[E],
        kind: Kind::Long,
        unit: "bytes",
        min: None,
        max: None,
        default: Some(DefaultValue::Long(4194304)),
        combine: Combine::SmallerOfTwo,
        section: "3.3.2.8, 4.3",
    },
    Definition {
        name: "MaxWebSocketMessagePayloadSize",
        scopes: &[E],
        kind: Kind::Long,
        unit: "bytes",
        min: None,
        max: None,
        default: Some(DefaultValue::Long(16777216)),
        combine: Combine::SmallerOfTwo,
        section: "3.3.2.9, 4.3",
    },
    Definition {
        name: "MultipartMessageTimeoutPeriod",
        scopes: &[E],
        kind: Kind::Long,
        unit: "seconds",
        min: None,
        max: Some(60),
        default: None,
        combine: Combine::Own,
        section: "3.7.3.1.1.2",
    },
    Definition {
        name: "ResponseTimeoutPeriod",
        scopes: &[E],
        kind: Kind::Long,
        unit: "seconds",
        min: Some(60),
        max: None,
        default: Some(DefaultValue::Long(300)),
        combine: Combine::Own,
        section: "3.3.2.3",
    },
    Definition {
        name: "SupportsAlternateRequestUris",
        scopes: &[E],
        kind: Kind::Boolean,
        unit: "",
        min: None,
        max: None,
        default: Some(DefaultValue::Boolean(false)),
        combine: Combine::Own,
        section: "3.7.4.1",
    },
    Definition {
        name: "SupportsMessageHeaderExtensions",
        scopes: &[E],
        kind: Kind::Boolean,
        unit: "",
        min: None,
        max: None,
        default: Some(DefaultValue::Boolean(false)),
        combine: Combine::Own,
        section: "3.6.2",
    },
    Definition {
        name: "RequestSessionTimeoutPeriod",
        scopes: &[E],
        kind: Kind::Long,
        unit: "seconds",
        min: Some(5),
        max: None,
        default: Some(DefaultValue::Long(45)),
        combine: Combine::Own,
        section: "3.3.2.10, 5.2.3",
    },
    Definition {
        name: "SessionEstablishmentTimeoutPeriod",
        scopes: &[E],
        kind: Kind::Long,
        unit: "seconds",
        min: Some(5),
        max: None,
        default: Some(DefaultValue::Long(60)),
        combine: Combine::Own,
        section: "5.2.3, 23.4",
    },
    Definition {
        name: "FrameChangeDetectionPeriod",
        scopes: &[P],
        kind: Kind::Long,
        unit: "seconds",
        min: Some(1),
        max: Some(600),
        default: Some(DefaultValue::Long(60)),
        combine: Combine::Own,
        section: "23.5",
    },
    Definition {
        name: "MaxDataArraySize",
        scopes: &[P],
        kind: Kind::Long,
        unit: "bytes",
        min: Some(100000),
        max: None,
        default: None,
        combine: Combine::Own,
        section: "23.5",
    },
    Definition {
        name: "MaxFrameResponseRowCount",
        scopes: &[P],
        kind: Kind::Long,
        unit: "rows",
        min: Some(100000),
        max: None,
        default: None,
        combine: Combine::Own,
        section: "23.5",
    },
    Definition {
        name: "MaxIndexCount",
        scopes: &[P],
        kind: Kind::Long,
        unit: "indexes",
        min: Some(1),
        max: None,
        default: Some(DefaultValue::Long(100)),
        combine: Combine::Own,
        section: "23.5",
    },
    Definition {
        name: "MaxRangeChannelCount",
        scopes: &[P],
        kind: Kind::Long,
        unit: "channels",
        min: None,
        max: None,
        default: None,
        combine: Combine::Own,
        section: "23.5",
    },
    Definition {
        name: "MaxRangeDataItemCount",
        scopes: &[P],
        kind: Kind::Long,
        unit: "records",
        min: Some(1000000),
        max: None,
        default: None,
        combine: Combine::Own,
        section: "23.5",
    },
    Definition {
        name: "MaxResponseCount",
        scopes: &[P],
        kind: Kind::Long,
        unit: "responses",
        min: Some(10000),
        max: None,
        default: None,
        combine: Combine::SmallerOfTwo,
        section: "3.7.3.1 §4.e, 23.5",
    },
    Definition {
        name: "MaxStreamingChannelsSessionCount",
        scopes: &[P],
        kind: Kind::Long,
        unit: "channels",
        min: Some(10000),
        max: None,
        default: None,
        combine: Combine::Own,
        section: "23.5",
    },
    Definition {
        name: "MaxSubscriptionSessionCount",
        scopes: &[P],
        kind: Kind::Long,
        unit: "subscriptions",
        min: Some(100),
        max: None,
        default: None,
        combine: Combine::Own,
        section: "23.5",
    },
    Definition {
        name: "MaxTransactionCount",
        scopes: &[P],
        kind: Kind::Long,
        unit: "transactions",
        min: Some(1),
        max: Some(1),
        default: Some(DefaultValue::Long(1)),
        combine: Combine::Own,
        section: "23.5",
    },
    Definition {
        name: "TransactionTimeoutPeriod",
        scopes: &[P],
        kind: Kind::Long,
        unit: "seconds",
        min: Some(5),
        max: None,
        default: None,
        combine: Combine::Own,
        section: "23.5",
    },
    Definition {
        name: "SupportsSecondaryIndexFiltering",
        scopes: &[P],
        kind: Kind::Boolean,
        unit: "",
        min: None,
        max: None,
        default: Some(DefaultValue::Boolean(false)),
        combine: Combine::Own,
        section: "23.5",
    },
    Definition {
        name: "MaxContainedDataObjectCount",
        scopes: &[D],
        kind: Kind::Long,
        unit: "objects",
        min: None,
        max: None,
        default: None,
        combine: Combine::Own,
        section: "23.3",
    },
    Definition {
        name: "OrphanedChildrenPrunedOnDelete",
        scopes: &[D],
        kind: Kind::Boolean,
        unit: "",
        min: None,
        max: None,
        default: Some(DefaultValue::Boolean(false)),
        combine: Combine::Own,
        section: "23.3",
    },
    Definition {
        name: "SupportsGet",
        scopes: &[D],
        kind: Kind::Boolean,
        unit: "",
        min: None,
        max: None,
        default: Some(DefaultValue::Boolean(true)),
        combine: Combine::Own,
        section: "23.3",
    },
    Definition {
        name: "SupportsPut",
        scopes: &[D],
        kind: Kind::Boolean,
        unit: "",
        min: None,
        max: None,
        default: Some(DefaultValue::Boolean(true)),
        combine: Combine::Own,
        section: "23.3",
    },
    Definition {
        name: "SupportsDelete",
        scopes: &[D],
        kind: Kind::Boolean,
        unit: "",
        min: None,
        max: None,
        default: Some(DefaultValue::Boolean(true)),
        combine: Combine::Own,
        section: "23.3",
    },
    Definition {
        name: "MaxSecondaryIndexCount",
        scopes: &[D],
        kind: Kind::Long,
        unit: "indexes",
        min: Some(1),
        max: None,
        default: Some(DefaultValue::Long(1)),
        combine: Combine::Own,
        section: "23.3",
    },
];

/// The definition of `name` in `scope`, if ETP defines one.
pub fn definition(name: &str, scope: Scope) -> Option<&'static Definition> {
    DEFINITIONS
        .iter()
        .find(|d| d.name == name && d.scopes.contains(&scope))
}

/// A value read from a map of capabilities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// A number.
    Long(i64),
    /// A flag.
    Boolean(bool),
    /// A list of strings.
    Strings(Vec<String>),
}

impl Value {
    /// The `DataValue` this travels as.
    pub fn to_data_value(&self) -> DataValue {
        DataValue {
            item: match self {
                Value::Long(v) => DataValueItem::Long(*v),
                Value::Boolean(v) => DataValueItem::Boolean(*v),
                Value::Strings(v) => DataValueItem::ArrayOfString(ArrayOfString { values: v.clone() }),
            },
        }
    }
}

/// Something wrong with a capability a peer sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Problem {
    /// Not the type the specification gives it; the value is not used.
    WrongType {
        /// The capability.
        name: String,
        /// The type it should have.
        expected: Kind,
    },
    /// Outside its bounds; the bound is used instead.
    OutOfBounds {
        /// The capability.
        name: String,
        /// What was sent.
        sent: i64,
        /// What is used.
        used: i64,
    },
}

impl fmt::Display for Problem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Problem::WrongType { name, expected } => {
                write!(
                    f,
                    "{name} is not a {expected:?} as the specification defines it; it is ignored"
                )
            }
            Problem::OutOfBounds { name, sent, used } => write!(
                f,
                "{name} = {sent} is outside the bounds the specification gives it; {used} is used"
            ),
        }
    }
}

/// The capabilities of one scope, read and checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Capabilities {
    scope: Scope,
    values: BTreeMap<&'static str, Value>,
    unknown: Vec<String>,
    problems: Vec<Problem>,
}

impl Capabilities {
    /// None sent: every capability at its default.
    pub fn empty(scope: Scope) -> Self {
        Self {
            scope,
            values: BTreeMap::new(),
            unknown: Vec::new(),
            problems: Vec::new(),
        }
    }

    /// Read a map a peer sent.
    pub fn read(map: &BTreeMap<String, DataValue>, scope: Scope) -> Self {
        let mut out = Self::empty(scope);
        for (name, value) in map {
            let Some(definition) = definition(name, scope) else {
                out.unknown.push(name.clone());
                continue;
            };
            let value = match (definition.kind, &value.item) {
                (Kind::Long, DataValueItem::Long(v)) => Value::Long(*v),
                (Kind::Long, DataValueItem::Int(v)) => Value::Long(i64::from(*v)),
                (Kind::Boolean, DataValueItem::Boolean(v)) => Value::Boolean(*v),
                (Kind::ArrayOfString, DataValueItem::ArrayOfString(v)) => Value::Strings(v.values.clone()),
                _ => {
                    out.problems.push(Problem::WrongType {
                        name: name.clone(),
                        expected: definition.kind,
                    });
                    continue;
                }
            };
            let value = match value {
                Value::Long(sent) => {
                    let used = sent
                        .max(definition.min.unwrap_or(i64::MIN))
                        .min(definition.max.unwrap_or(i64::MAX));
                    if used != sent {
                        out.problems.push(Problem::OutOfBounds {
                            name: name.clone(),
                            sent,
                            used,
                        });
                    }
                    Value::Long(used)
                }
                other => other,
            };
            out.values.insert(definition.name, value);
        }
        out
    }

    /// Set one of ours - refused for a name this scope does not define, or a
    /// value of the wrong type, so that we never announce what we would refuse
    /// from a peer.
    pub fn set(mut self, name: &str, value: Value) -> Result<Self, Problem> {
        let definition = definition(name, self.scope).ok_or_else(|| Problem::WrongType {
            name: name.to_string(),
            expected: Kind::Long,
        })?;
        let fits = matches!(
            (definition.kind, &value),
            (Kind::Long, Value::Long(_))
                | (Kind::Boolean, Value::Boolean(_))
                | (Kind::ArrayOfString, Value::Strings(_))
        );
        if !fits {
            return Err(Problem::WrongType {
                name: name.to_string(),
                expected: definition.kind,
            });
        }
        if let Value::Long(v) = value {
            let bounded = v
                .max(definition.min.unwrap_or(i64::MIN))
                .min(definition.max.unwrap_or(i64::MAX));
            if bounded != v {
                return Err(Problem::OutOfBounds {
                    name: name.to_string(),
                    sent: v,
                    used: bounded,
                });
            }
        }
        self.values.insert(definition.name, value);
        Ok(self)
    }

    /// Where these apply.
    pub fn scope(&self) -> Scope {
        self.scope
    }

    /// The map these travel as.
    pub fn to_map(&self) -> BTreeMap<String, DataValue> {
        self.values
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_data_value()))
            .collect()
    }

    /// A `long`: the value sent, else the default, else `None`.
    pub fn long(&self, name: &str) -> Option<i64> {
        match self.values.get(name) {
            Some(Value::Long(v)) => Some(*v),
            _ => match definition(name, self.scope)?.default? {
                DefaultValue::Long(v) => Some(v),
                DefaultValue::Boolean(_) => None,
            },
        }
    }

    /// A `boolean`: the value sent, else the default.
    pub fn boolean(&self, name: &str) -> Option<bool> {
        match self.values.get(name) {
            Some(Value::Boolean(v)) => Some(*v),
            _ => match definition(name, self.scope)?.default? {
                DefaultValue::Boolean(v) => Some(v),
                DefaultValue::Long(_) => None,
            },
        }
    }

    /// An `ArrayOfString`, if sent.
    pub fn strings(&self, name: &str) -> Option<&[String]> {
        match self.values.get(name) {
            Some(Value::Strings(v)) => Some(v),
            _ => None,
        }
    }

    /// Whether the peer sent this capability at all.
    pub fn sent(&self, name: &str) -> bool {
        self.values.contains_key(name)
    }

    /// Names the table does not know, which were ignored.
    pub fn unknown(&self) -> &[String] {
        &self.unknown
    }

    /// What was wrong with what was sent.
    pub fn problems(&self) -> &[Problem] {
        &self.problems
    }
}

/// The smaller of two ends' values, where either may be silent (§3.3.2.8-9).
pub fn smaller_of_two(ours: Option<i64>, theirs: Option<i64>) -> Option<i64> {
    match (ours, theirs) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

/// `MaxDataObjectSize` for one object type in one protocol (§3.3.2.4): the
/// object's value overrides the endpoint's, and the protocol's can only lower
/// the result.
pub fn data_object_size(endpoint: Option<i64>, protocol: Option<i64>, object: Option<i64>) -> Option<i64> {
    let base = object.or(endpoint);
    match (base, protocol) {
        (Some(b), Some(p)) => Some(b.min(p)),
        (b, p) => b.or(p),
    }
}

/// The names of capabilities used across the crate, spelled once.
pub mod names {
    /// `MaxDataArraySize` - a DataArray (9) protocol capability (§13.2.3).
    pub const MAX_DATA_ARRAY_SIZE: &str = "MaxDataArraySize";
    /// `MaxDataObjectSize` - an endpoint, protocol **and** object-type
    /// capability; [`super::data_object_size`] says how the three combine.
    pub const MAX_DATA_OBJECT_SIZE: &str = "MaxDataObjectSize";
    /// `MaxWebSocketMessagePayloadSize`.
    pub const MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE: &str = "MaxWebSocketMessagePayloadSize";
    /// `MaxWebSocketFramePayloadSize`.
    pub const MAX_WEBSOCKET_FRAME_PAYLOAD_SIZE: &str = "MaxWebSocketFramePayloadSize";
    /// `SupportsMessageHeaderExtensions`.
    pub const SUPPORTS_MESSAGE_HEADER_EXTENSIONS: &str = "SupportsMessageHeaderExtensions";
    /// `SupportsAlternateRequestUris`.
    pub const SUPPORTS_ALTERNATE_REQUEST_URIS: &str = "SupportsAlternateRequestUris";
    /// `ResponseTimeoutPeriod`.
    pub const RESPONSE_TIMEOUT_PERIOD: &str = "ResponseTimeoutPeriod";
    /// `MultipartMessageTimeoutPeriod`.
    pub const MULTIPART_MESSAGE_TIMEOUT_PERIOD: &str = "MultipartMessageTimeoutPeriod";
    /// `RequestSessionTimeoutPeriod`.
    pub const REQUEST_SESSION_TIMEOUT_PERIOD: &str = "RequestSessionTimeoutPeriod";
    /// `SessionEstablishmentTimeoutPeriod`.
    pub const SESSION_ESTABLISHMENT_TIMEOUT_PERIOD: &str = "SessionEstablishmentTimeoutPeriod";
    /// `MaxConcurrentMultipart`.
    pub const MAX_CONCURRENT_MULTIPART: &str = "MaxConcurrentMultipart";
    /// `ChangeRetentionPeriod`.
    pub const CHANGE_RETENTION_PERIOD: &str = "ChangeRetentionPeriod";
    /// How long a change may take to become visible to everyone (§3.3.2.2).
    pub const CHANGE_PROPAGATION_PERIOD: &str = "ChangePropagationPeriod";
    /// `AuthorizationDetails`.
    pub const AUTHORIZATION_DETAILS: &str = "AuthorizationDetails";
}
