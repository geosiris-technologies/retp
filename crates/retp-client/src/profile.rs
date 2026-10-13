//! A server has habits, and the client can follow them without the protocol
//! knowing.

use retp_proto::probe::Handshake;

/// Which server the client talks to, as far as its habits go.
///
/// A profile only ever **adds to the handshake**. It never reaches a message:
/// the day a divergence needs to, it goes here and nowhere else -
/// `server_profile_changes_no_protocol_code` holds that.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ServerProfile {
    /// A server that follows the specification.
    #[default]
    Standard,
    /// OSDU's Reservoir DDMS (`open-etp-server`): every request names its
    /// data partition in a `data-partition-id` header (issue #18 and its
    /// README).
    Rddms {
        /// The OSDU data partition.
        data_partition: String,
    },
}

impl ServerProfile {
    /// The handshake, with what this server expects in addition - as a
    /// header, or in the query string for a browser, which sets no header.
    pub fn adapt(&self, handshake: Handshake) -> Handshake {
        match self {
            ServerProfile::Standard => handshake,
            ServerProfile::Rddms { data_partition } => {
                handshake.with_attribute("data-partition-id", data_partition)
            }
        }
    }
}

/// A `ws://` URL to a host that is not this machine: the session would travel
/// in clear, credentials included. The specification allows it - so this is said,
/// not refused (encryption is TLS, and nothing else).
pub fn plain_text_warning(url: &str) -> Option<String> {
    let rest = url.strip_prefix("ws://")?;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    // `[::1]:8080` - an IPv6 literal carries its colons inside brackets.
    let host = match authority.strip_prefix('[') {
        Some(rest) => rest.split(']').next().unwrap_or(rest),
        None => authority.split(':').next().unwrap_or(authority),
    };
    if matches!(host, "localhost" | "127.0.0.1" | "::1") || host.starts_with("127.") {
        return None;
    }
    Some(format!(
        "{url} is not encrypted: the session, and the credentials if any are sent, travel in clear to {host}. Use wss:// for anything but this machine."
    ))
}
