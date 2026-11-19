//! What the client says of a server before it connects.

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
