//! Canonical and alternate URIs (§3.7.4, p.75-76).
//!
//! Every endpoint understands the canonical Energistics URI; an alternate form
//! may be sent only to a peer that announced `SupportsAlternateRequestUris`,
//! and never in the places §3.7.4.2 reserves to the canonical form.
//!
//! "Canonical" is decided where the repository reads ETP URIs,
//! `energyml_core::identifiers::EtpUri::is_canonical`, on the regular
//! expressions §25.3.10 gives - one reader, not two. The alternate grammar
//! (§25.3.9) is not read here: an alternate URI, where one is allowed, is
//! passed on as it is.

use energyml_core::identifiers::EtpUri;

use crate::wire::{EtpError, Incident};

/// Where a URI is used, which decides whether an alternate form may be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UriUse {
    /// A request where alternate URIs are allowed once negotiated -
    /// `GetDataObjects`, say.
    Request,
    /// Where §3.7.4.2 requires the canonical form whatever was negotiated:
    /// Discovery and query requests, puts and deletes, every response and
    /// every notification.
    CanonicalOnly,
}

/// Whether `uri` is a canonical Energistics URI.
pub fn is_canonical(uri: &str) -> bool {
    EtpUri::is_canonical(uri)
}

/// Check a URI before it is used - ours before sending, a peer's on receipt.
///
/// `alternate_negotiated` is whether the endpoint receiving the URI announced
/// `SupportsAlternateRequestUris`.
pub fn check_uri(uri: &str, usage: UriUse, alternate_negotiated: bool) -> Result<(), Incident> {
    if is_canonical(uri) {
        return Ok(());
    }
    match usage {
        UriUse::Request if alternate_negotiated => Ok(()),
        UriUse::Request => Err(EtpError::EINVALID_URI.with_detail(format!(
            "`{uri}` is not a canonical URI, and alternate URIs were not negotiated (§3.7.4.1 §1.b)"
        ))),
        UriUse::CanonicalOnly => Err(EtpError::EINVALID_URI.with_detail(format!(
            "`{uri}` is not a canonical URI, which this message requires whatever was negotiated (§3.7.4.2)"
        ))),
    }
}
