//! What an application says of itself - its offer, written in a few lines.
//!
//! The content of an `OpenSession` or `RequestSession` depends on what the
//! final server or client wants to support, so the supported types and the
//! rest must be easy for the client or server library to describe, so that it
//! states its constraints according to its use.
//!
//! Two ways, one result: methods on [`Offer`] for a library that builds its
//! offer in code, and an [`OfferDescription`] - JSON - for one that keeps it
//! in a file. Both go through the capability registry, so what an
//! application announces is what it would accept from a peer: a capability
//! the specification does not define, a value of the wrong type or out of
//! its bounds, a type that is not a qualified type, a format ETP does not
//! have - each is refused by name, never announced.

use std::fmt;

use retp_types::datatypes::SupportedDataObject;
use serde_json::Value as Json;

use crate::capabilities::{self, Capabilities, Kind, Problem, Scope, Value};
use crate::negotiate::{
    Deviation, Offer, OfferRefusal, QualifiedType, covers, deviation, read_qualified_type,
};
use crate::wire::Tolerance;

/// The formats of data objects ETP v1.2 has (§5.3.1 `supportedFormats`).
pub const FORMATS: &[&str] = &["xml", "json"];

fn capability_into(
    into: Capabilities,
    scope: Scope,
    name: &str,
    value: Value,
) -> Result<Capabilities, OfferRefusal> {
    if capabilities::definition(name, scope).is_none() {
        return Err(OfferRefusal::Capability {
            name: name.to_string(),
            why: format!("the specification defines no such {scope:?} capability"),
        });
    }
    into.set(name, value).map_err(|problem| OfferRefusal::Capability {
        name: name.to_string(),
        why: match problem {
            Problem::WrongType { expected, .. } => format!("the specification makes it a {expected:?}"),
            Problem::OutOfBounds { sent, used, .. } => {
                format!(
                    "{sent} is outside the bounds the specification gives it (the nearest allowed is {used})"
                )
            }
        },
    })
}

impl Offer {
    /// Data object types, each a qualified type or a family's wildcard
    /// (`resqml20.obj_HorizonInterpretation`, `resqml20.*`), with the default
    /// capabilities - they replace any given before.
    pub fn with_types<I, S>(mut self, types: I) -> Result<Self, OfferRefusal>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.data_objects.clear();
        for qualified_type in types {
            self = self.with_type(qualified_type.as_ref(), Capabilities::empty(Scope::DataObject))?;
        }
        Ok(self)
    }

    /// One more type, with its capabilities - `SupportsPut: false` for a
    /// type a store only serves, `MaxDataObjectSize` for one it caps.
    pub fn with_type(
        mut self,
        qualified_type: &str,
        capabilities: Capabilities,
    ) -> Result<Self, OfferRefusal> {
        match read_qualified_type(qualified_type) {
            QualifiedType::Valid => {}
            QualifiedType::Family(pattern) => {
                return Err(OfferRefusal::NotAQualifiedType(format!(
                    "`{qualified_type}` is not a qualified data object type (§23.20); `{pattern}` names the family"
                )));
            }
            QualifiedType::Unreadable => {
                return Err(OfferRefusal::NotAQualifiedType(format!(
                    "`{qualified_type}` is not a qualified data object type (§23.20)"
                )));
            }
        }
        if capabilities.scope() != Scope::DataObject {
            return Err(OfferRefusal::Capability {
                name: qualified_type.to_string(),
                why: format!(
                    "{:?} capabilities given to a data object type",
                    capabilities.scope()
                ),
            });
        }
        self.data_objects.retain(|d| d.qualified_type != qualified_type);
        self.data_objects.push(SupportedDataObject {
            qualified_type: qualified_type.to_string(),
            data_object_capabilities: capabilities.to_map(),
        });
        Ok(self)
    }

    /// The formats objects are read and written in, by preference.
    pub fn with_formats<I, S>(mut self, formats: I) -> Result<Self, OfferRefusal>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let formats: Vec<String> = formats.into_iter().map(|f| f.as_ref().to_string()).collect();
        if formats.is_empty() {
            return Err(OfferRefusal::Format(
                "none: an endpoint reads at least one".into(),
            ));
        }
        if let Some(unknown) = formats.iter().find(|f| !FORMATS.contains(&f.as_str())) {
            return Err(OfferRefusal::Format(format!(
                "`{unknown}` - ETP v1.2 has {}",
                FORMATS.join(" and ")
            )));
        }
        self.formats = formats;
        Ok(self)
    }

    /// One endpoint capability (`MaxWebSocketMessagePayloadSize`…).
    pub fn with_capability(mut self, name: &str, value: Value) -> Result<Self, OfferRefusal> {
        self.endpoint = capability_into(self.endpoint, Scope::Endpoint, name, value)?;
        Ok(self)
    }

    /// How the peer's faults are taken.
    pub fn with_tolerance(mut self, tolerance: Tolerance) -> Self {
        self.tolerance = tolerance;
        self
    }

    /// Ask for whatever the server supports as well: each data object type
    /// it announces - the `supportedDataObjects` of its capabilities,
    /// probed before any session (§4.1.2) - that the offer does not cover
    /// yet is added, with default capabilities. The session agrees to the
    /// intersection of the two lists (§5.2.2 row 6.1), so a client that reads
    /// any type widens its own to lose nothing of the server's.
    ///
    /// A family without its `.*` (`resqml20`) is read with it; what is not a
    /// qualified type is left out. Both are named, never refused: the
    /// server's list is advice here, not a request.
    pub fn widened_to(mut self, server: &[SupportedDataObject]) -> (Self, Widened) {
        let mut widened = Widened::default();
        for object in server {
            let qualified_type = match read_qualified_type(&object.qualified_type) {
                QualifiedType::Valid => object.qualified_type.clone(),
                QualifiedType::Family(pattern) => {
                    widened.deviations.push(deviation(
                        format!(
                            "the server announces data object type `{}`",
                            object.qualified_type
                        ),
                        "§23.20 SupportedDataObject.qualifiedType",
                        format!("read as `{pattern}`"),
                    ));
                    pattern
                }
                QualifiedType::Unreadable => {
                    widened.deviations.push(deviation(
                        format!(
                            "the server announces data object type `{}`, which is not a qualified type",
                            object.qualified_type
                        ),
                        "§23.20 SupportedDataObject.qualifiedType",
                        "not asked for",
                    ));
                    continue;
                }
            };
            if self
                .data_objects
                .iter()
                .any(|d| covers(&d.qualified_type, &qualified_type))
            {
                continue;
            }
            self.data_objects.push(SupportedDataObject {
                qualified_type: qualified_type.clone(),
                data_object_capabilities: Default::default(),
            });
            widened.added.push(qualified_type);
        }
        (self, widened)
    }
}

/// What [`Offer::widened_to`] did.
#[derive(Debug, Default)]
pub struct Widened {
    /// The types added - each one the server announced that the offer did
    /// not cover.
    pub added: Vec<String>,
    /// What the server announced wrong, and what was made of it.
    pub deviations: Vec<Deviation>,
}

/// Why a description could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescriptionRefusal(pub String);

impl fmt::Display for DescriptionRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the offer description: {}", self.0)
    }
}

impl std::error::Error for DescriptionRefusal {}

impl From<OfferRefusal> for DescriptionRefusal {
    fn from(why: OfferRefusal) -> Self {
        DescriptionRefusal(why.to_string())
    }
}

/// An offer written as JSON - every key optional, none unknown:
///
/// ```json
/// {
///   "types": ["resqml20.*", { "type": "eml20.*", "capabilities": { "SupportsPut": false } }],
///   "formats": ["xml", "json"],
///   "protocols": [3, 4, 24, 25],
///   "endpoint": { "MaxWebSocketMessagePayloadSize": 8000000 },
///   "tolerance": "strict"
/// }
/// ```
///
/// `protocols` is for the application that builds the handlers: the offer's
/// protocols are its handlers' (ADR-0001), and a description names which.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OfferDescription {
    /// Each type, with its capabilities.
    pub types: Vec<(String, Capabilities)>,
    /// The formats, when given.
    pub formats: Option<Vec<String>>,
    /// The protocols, when given.
    pub protocols: Option<Vec<i32>>,
    /// Endpoint capabilities.
    pub endpoint: Vec<(String, Value)>,
    /// The tolerance, when given.
    pub tolerance: Option<Tolerance>,
}

const KEYS: &[&str] = &["types", "formats", "protocols", "endpoint", "tolerance"];

fn value_of(name: &str, scope: Scope, json: &Json) -> Result<Value, DescriptionRefusal> {
    let definition = capabilities::definition(name, scope).ok_or_else(|| {
        DescriptionRefusal(format!(
            "`{name}`: the specification defines no such {scope:?} capability"
        ))
    })?;
    let refuse = |what: &str| DescriptionRefusal(format!("`{name}` is {what}, and {json} is not"));
    match definition.kind {
        Kind::Long => json
            .as_i64()
            .map(Value::Long)
            .ok_or_else(|| refuse("a whole number")),
        Kind::Boolean => json
            .as_bool()
            .map(Value::Boolean)
            .ok_or_else(|| refuse("true or false")),
        Kind::ArrayOfString => json
            .as_array()
            .and_then(|items| {
                items
                    .iter()
                    .map(|i| i.as_str().map(str::to_string))
                    .collect::<Option<Vec<_>>>()
            })
            .map(Value::Strings)
            .ok_or_else(|| refuse("a list of strings")),
    }
}

fn capabilities_of(
    scope: Scope,
    json: &Json,
    place: &str,
) -> Result<Vec<(String, Value)>, DescriptionRefusal> {
    let map = json.as_object().ok_or_else(|| {
        DescriptionRefusal(format!("{place} is an object of capabilities, and {json} is not"))
    })?;
    map.iter()
        .map(|(name, v)| Ok((name.clone(), value_of(name, scope, v)?)))
        .collect()
}

fn strings(json: &Json, place: &str) -> Result<Vec<String>, DescriptionRefusal> {
    json.as_array()
        .and_then(|items| items.iter().map(|i| i.as_str().map(str::to_string)).collect())
        .ok_or_else(|| DescriptionRefusal(format!("`{place}` is a list of strings")))
}

impl OfferDescription {
    /// Read a description; nothing is applied yet - [`OfferDescription::apply`].
    pub fn parse(text: &str) -> Result<Self, DescriptionRefusal> {
        let json: Json =
            serde_json::from_str(text).map_err(|why| DescriptionRefusal(format!("not JSON: {why}")))?;
        let object = json
            .as_object()
            .ok_or_else(|| DescriptionRefusal("a description is a JSON object".into()))?;
        if let Some(unknown) = object.keys().find(|k| !KEYS.contains(&k.as_str())) {
            return Err(DescriptionRefusal(format!(
                "unknown key `{unknown}` - the keys are {}",
                KEYS.join(", ")
            )));
        }
        let mut description = OfferDescription::default();
        if let Some(types) = object.get("types") {
            let items = types
                .as_array()
                .ok_or_else(|| DescriptionRefusal("`types` is a list".into()))?;
            for item in items {
                let (name, capabilities) = match item {
                    Json::String(name) => (name.clone(), Capabilities::empty(Scope::DataObject)),
                    Json::Object(entry) => {
                        if let Some(unknown) = entry.keys().find(|k| *k != "type" && *k != "capabilities") {
                            return Err(DescriptionRefusal(format!(
                                "unknown key `{unknown}` in a type - the keys are type, capabilities"
                            )));
                        }
                        let name = entry.get("type").and_then(Json::as_str).ok_or_else(|| {
                            DescriptionRefusal(format!("a type without its `type`: {item}"))
                        })?;
                        let mut capabilities = Capabilities::empty(Scope::DataObject);
                        if let Some(given) = entry.get("capabilities") {
                            for (capability, value) in capabilities_of(Scope::DataObject, given, name)? {
                                capabilities =
                                    capability_into(capabilities, Scope::DataObject, &capability, value)?;
                            }
                        }
                        (name.to_string(), capabilities)
                    }
                    other => {
                        return Err(DescriptionRefusal(format!(
                            "a type is a string or an object, and {other} is neither"
                        )));
                    }
                };
                description.types.push((name, capabilities));
            }
        }
        if let Some(formats) = object.get("formats") {
            description.formats = Some(strings(formats, "formats")?);
        }
        if let Some(protocols) = object.get("protocols") {
            let numbers = protocols
                .as_array()
                .and_then(|items| {
                    items
                        .iter()
                        .map(|i| i.as_i64().and_then(|n| i32::try_from(n).ok()))
                        .collect::<Option<Vec<_>>>()
                })
                .ok_or_else(|| DescriptionRefusal("`protocols` is a list of protocol numbers".into()))?;
            description.protocols = Some(numbers);
        }
        if let Some(endpoint) = object.get("endpoint") {
            description.endpoint = capabilities_of(Scope::Endpoint, endpoint, "`endpoint`")?;
        }
        if let Some(tolerance) = object.get("tolerance") {
            description.tolerance = Some(match tolerance.as_str() {
                Some("tolerant") => Tolerance::Tolerant,
                Some("strict") => Tolerance::Strict,
                _ => {
                    return Err(DescriptionRefusal(format!(
                        "`tolerance` is \"tolerant\" or \"strict\", and {tolerance} is neither"
                    )));
                }
            });
        }
        Ok(description)
    }

    /// The offer, with this description applied on top: what it gives
    /// replaces what the offer had; what it leaves out stays.
    pub fn apply(&self, mut offer: Offer) -> Result<Offer, OfferRefusal> {
        if !self.types.is_empty() {
            offer.data_objects.clear();
            for (qualified_type, capabilities) in &self.types {
                offer = offer.with_type(qualified_type, capabilities.clone())?;
            }
        }
        if let Some(formats) = &self.formats {
            offer = offer.with_formats(formats)?;
        }
        for (name, value) in &self.endpoint {
            offer = offer.with_capability(name, value.clone())?;
        }
        if let Some(tolerance) = self.tolerance {
            offer = offer.with_tolerance(tolerance);
        }
        Ok(offer)
    }
}

/// A data object type's capabilities, in one call:
/// `data_object(&[("SupportsPut", Value::Boolean(false))])`.
pub fn data_object(values: &[(&str, Value)]) -> Result<Capabilities, OfferRefusal> {
    let mut out = Capabilities::empty(Scope::DataObject);
    for (name, value) in values {
        out = capability_into(out, Scope::DataObject, name, value.clone())?;
    }
    Ok(out)
}
