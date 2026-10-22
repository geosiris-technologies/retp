// Generated from the Energistics ETP v1.2 Avro schemas (Apache-2.0).
// Generator: energyml-codegen 0.1.0
// Input fingerprint (SHA-256, see schemas.lock): 84bd808714c5d32f2b7e91d897b5677012f16bafd221694662c8058e98c13fdc
//! The ETP v1.2 types - generated from the Avro schemas.
//!
//! Do not edit: run `cargo run -p energyml-codegen -- generate --etp` instead.
//!
//! 199 records, 14 enumerations, 1 fixed, 3 named unions; 140 of the records
//! are messages.
//!
//! There is **no re-export at the crate root**: five simple names occur in
//! more than one namespace (`ChannelData`, `Chunk`, `TruncateChannels`,
//! `GetChangeAnnotations` and its response), so a flat root would have to
//! invent disambiguated names that no schema uses. The module tree mirrors
//! the Avro namespaces instead.

#![allow(clippy::module_name_repetitions, clippy::too_many_lines)]
#![allow(clippy::large_enum_variant, clippy::doc_markdown)]
#![allow(rustdoc::bare_urls, rustdoc::invalid_html_tags, rustdoc::broken_intra_doc_links)]
// The WitsmlSoap private protocol names its messages `WMLS_GetVersion`
// and so on. The schema's spelling is kept: renaming a generated type
// to please a lint would put a name in the API that no schema uses.
#![allow(non_camel_case_types)]

pub mod dispatch;
pub mod datatypes;
pub mod private_protocols;
pub mod protocol;
