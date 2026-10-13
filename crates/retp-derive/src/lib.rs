//! The derives behind the generated ETP types.
//!
//! `energyml-codegen --etp` emits plain Rust with a line of attributes; these
//! macros give each item its behaviour. It is the split `energyml-derive`
//! makes for the XSD crates, and ADR-0004
//! states it: **a derive for what a type knows about itself**, the generator
//! for what needs all of them at once (the dispatch table).
//!
//! Two derives, re-exported by `retp-avro` under the names of their
//! traits:
//!
//! - [`macro@Avro`] - the codec, and a `Default` faithful to the schema. It
//!   reads a record, a fixed, an enumeration or a union (`#[avro(union)]`).
//! - [`macro@EtpMessage`] - what a message says about itself on the wire.
//!
//! ```ignore
//! #[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
//! #[avro(name = "Energistics.Etp.v12.Protocol.Store.GetDataObjects")]
//! #[etp(protocol = 4, message_type = 1, sender = "customer", roles = "store,customer")]
//! pub struct GetDataObjects {
//!     pub uris: std::collections::BTreeMap<String, String>,
//!     #[avro(default = "xml")]
//!     pub format: String,
//! }
//! ```
//!
//! **The field order is the encoding**, as the variant order is for an
//! enumeration and a union: Avro writes neither names nor tags. The
//! generator emits them in schema order, and a hand-written type must too.
//!
//! What the derive reads off a field's type, and what it refuses, is in
//! `shape.rs`. What it cannot read off a type is in the attributes:
//!
//! | attribute | on | means |
//! |---|---|---|
//! | `name = "…"` | the type | the Avro full name, used in refusals |
//! | `union` | an enum | a union rather than an enumeration |
//! | `rename = "…"` | a field | the Avro field name, when it is not the camelCase of the Rust one |
//! | `default = …` | a field | the schema's default, spelled as the schema spells it |
//! | `symbol = "…"` | a variant | the symbol, when it is not the variant's name |
//! | `default` | a variant | the enumeration's default, when it is not the first symbol |

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

mod items;
mod shape;

/// The Avro codec of a record, fixed, enumeration or union - and, for all but
/// the fixed, a `Default` that says what the schema says.
#[proc_macro_derive(Avro, attributes(avro))]
pub fn derive_avro(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    items::avro(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// What an ETP message knows about itself: protocol, message type, who sends
/// it, and whether it may be split.
#[proc_macro_derive(EtpMessage, attributes(etp))]
pub fn derive_etp_message(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    items::etp_message(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
