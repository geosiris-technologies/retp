//! What a general-purpose client asks a server for.

use std::collections::BTreeMap;

use retp_avro::Role;
use retp_proto::connection::{Forward, Handler};
use retp_proto::describe::OfferDescription;
use retp_proto::negotiate::{Offer, OfferRefusal, ProtocolOffer};
use retp_types::datatypes::{SupportedDataObject, Uuid};

/// Every data object type of the releases this repository generates, by
/// wildcard (§5.3.1: `resqml20.*` means every object of RESQML 2.0.x).
///
/// **Written out, and confronted with the registry by a test.** A hand-kept
/// second copy of a list that already exists is the defect this workspace names
/// everywhere, and this one had already drifted: it once offered
/// `prodml21.*`, a release nothing here generates, and omitted `prodml20.*`,
/// which it does. Neither half breaks anything visibly - a client asks a server
/// for objects it could not read, and never asks for ones it could - so nothing
/// said a word. `tests/offer.rs` now derives the same set from
/// `energyml-schemas` and fails naming the difference.
///
/// It stays a constant rather than becoming a call: this crate must not depend
/// on `energyml-schemas`, which pulls in every generated crate. What an ETP
/// client offers is a *choice*, and the registry it reads with is the caller's.
pub const EVERY_RELEASE: &[&str] = &[
    "eml20.*",
    "eml21.*",
    "eml22.*",
    "eml23.*",
    "resqml20.*",
    "resqml22.*",
    "witsml20.*",
    "witsml21.*",
    "prodml20.*",
    "prodml22.*",
    "prodml23.*",
];

/// The protocols a reading client implements, as their customer: Discovery
/// (3), Store (4), DataArray (9), Dataspace (24), SupportedTypes (25). Each
/// has a handler that forwards what the store sends unasked - nothing is
/// announced that nothing handles. A server that lacks one agrees to the
/// others.
pub const READING: &[i32] = &[1, 3, 4, 5, 9, 21, 24, 25];

/// The protocols a writing client implements: [`READING`] and **Transaction
/// (18)**.
///
/// A list of its own rather than one more entry in `READING`, because the rule
/// above is what keeps an offer honest - *nothing is announced that nothing
/// handles*. A client that only reads has no use for a transaction, and a
/// store is entitled to read the announcement as a statement of intent.
pub const WRITING: &[i32] = &[1, 3, 4, 5, 9, 18, 21, 24, 25];

/// The formats a reading client reads objects in, by preference: XML, and
/// the JSON of EnergyML (§5.3.1 `supportedFormats`).
pub const FORMATS: &[&str] = &["xml", "json"];

/// A reading client's offer and handlers.
///
/// `data_object_types` empty means [`EVERY_RELEASE`].
pub fn reading_client(
    application_name: &str,
    application_version: &str,
    instance_id: Uuid,
    data_object_types: &[String],
) -> (Offer, Vec<Box<dyn Handler>>) {
    let mut offer = Offer::new(application_name, application_version, instance_id);
    let types: Vec<String> = if data_object_types.is_empty() {
        EVERY_RELEASE.iter().map(|t| t.to_string()).collect()
    } else {
        data_object_types.to_vec()
    };
    offer.formats = FORMATS.iter().map(|f| f.to_string()).collect();
    offer.data_objects = types
        .into_iter()
        .map(|qualified_type| SupportedDataObject {
            qualified_type,
            data_object_capabilities: BTreeMap::new(),
        })
        .collect();
    (offer, customer_of(READING))
}

/// A writing client's offer and handlers - [`reading_client`] plus the
/// Transaction protocol.
pub fn writing_client(
    application_name: &str,
    application_version: &str,
    instance_id: Uuid,
    data_object_types: &[String],
) -> (Offer, Vec<Box<dyn Handler>>) {
    let (offer, _) = reading_client(
        application_name,
        application_version,
        instance_id,
        data_object_types,
    );
    (offer, customer_of(WRITING))
}

/// A handler per protocol, each played as its customer - its **consumer**
/// for ChannelStreaming (1), whose pair is producer/consumer (§6.1) -,
/// forwarding what the store sends to the application: what a client that
/// asks, and listens, needs.
pub fn customer_of(protocols: &[i32]) -> Vec<Box<dyn Handler>> {
    protocols
        .iter()
        .map(|p| Box::new(Forward(ProtocolOffer::v12(*p, role_of(*p)))) as Box<dyn Handler>)
        .collect()
}

/// The role this client plays in a protocol: consumer of a stream (1),
/// customer of everything else.
pub fn role_of(protocol: i32) -> Role {
    if protocol == 1 {
        Role::Consumer
    } else {
        Role::Customer
    }
}

/// A client's offer and handlers from a description
/// ([`OfferDescription`]): its types, formats, protocols, capabilities and
/// tolerance, and [`reading_client`]'s for what it leaves out.
pub fn described_client(
    application_name: &str,
    application_version: &str,
    instance_id: Uuid,
    description: &OfferDescription,
) -> Result<(Offer, Vec<Box<dyn Handler>>), OfferRefusal> {
    let (offer, _) = reading_client(application_name, application_version, instance_id, &[]);
    let offer = description.apply(offer)?;
    let protocols = description.protocols.clone().unwrap_or_else(|| READING.to_vec());
    Ok((offer, customer_of(&protocols)))
}
