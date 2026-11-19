//! What a client proves itself with (§4.1, p.93-96).
//!
//! ETP carries credentials in two places, and in the same form in both: the
//! `authorization` header of the WebSocket handshake, which every client MUST
//! use (§4.1.4 §2), and the `authorization` field of `Authorize`, which
//! carries *« the content of the equivalent HTTP Authorization header (NOT
//! just a token) »* (§5.3.4). So a [`Credentials`] **is** that header value -
//! scheme included - and nothing downstream adds a `Bearer ` in front of it.
//!
//! How a token is obtained is not here: that is an HTTP exchange with an
//! authorization server (§4.1.2), and this crate opens no socket.

use std::fmt;

use base64::Engine;
use zeroize::Zeroize;

/// Why credentials could not be written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialsRefusal(pub String);

impl fmt::Display for CredentialsRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CredentialsRefusal {}

/// The value of an HTTP `Authorization` header.
///
/// Its `Debug` shows the scheme and hides the rest: a credential that reaches
/// a log through a `{:?}` is a leaked credential.
///
/// **It is wiped from memory when dropped** (`zeroize`), every clone with it.
/// That covers the copies this type holds; a copy made by whoever reads
/// [`Credentials::header`] - an HTTP header map, a message buffer - is theirs
/// to wipe, and the wiping of a buffer already moved or reallocated is not
/// something Rust can promise.
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials(String);

impl Credentials {
    /// An OAuth 2.0 bearer token (RFC 6750) - the scheme every ETP server
    /// MUST support (§4.1.1 §1.b).
    ///
    /// The token alone: one that already starts with its scheme is refused
    /// rather than sent as `Bearer Bearer …`, and so is anything outside the
    /// `b64token` syntax of RFC 6750 §2.1 - a space, a line break, a quote
    /// pasted with it.
    pub fn bearer(token: &str) -> Result<Self, CredentialsRefusal> {
        if token
            .get(..7)
            .is_some_and(|scheme| scheme.eq_ignore_ascii_case("bearer "))
        {
            return Err(CredentialsRefusal(
                "the token starts with `Bearer `: give the token alone, the scheme is added".into(),
            ));
        }
        let body = token.trim_end_matches('=');
        let allowed = |c: char| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_' | '~' | '+' | '/');
        if body.is_empty() || !body.chars().all(allowed) {
            return Err(CredentialsRefusal(
                "this is not a bearer token: RFC 6750 allows letters, digits and `-._~+/`, then `=` padding"
                    .into(),
            ));
        }
        Ok(Self(format!("Bearer {token}")))
    }

    /// HTTP Basic (RFC 7617): `user:password`, in UTF-8, in base64.
    ///
    /// ETP v1.2 no longer recommends it (§4.1.1) and still allows it; some
    /// servers take nothing else. A user-id with a colon is refused - RFC 7617
    /// §2 calls it invalid, since the colon is what separates it from the
    /// password - and so is a control character in either.
    pub fn basic(user: &str, password: &str) -> Result<Self, CredentialsRefusal> {
        if user.contains(':') {
            return Err(CredentialsRefusal(
                "a Basic user-id cannot contain `:` (RFC 7617 §2): the colon separates it from the password"
                    .into(),
            ));
        }
        if user.chars().chain(password.chars()).any(char::is_control) {
            return Err(CredentialsRefusal(
                "a Basic user-id or password cannot contain a control character (RFC 7617 §2)".into(),
            ));
        }
        let pair = format!("{user}:{password}");
        Ok(Self(format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(pair)
        )))
    }

    /// The header value, scheme included.
    pub fn header(&self) -> &str {
        &self.0
    }

    /// `Bearer`, `Basic`…
    pub fn scheme(&self) -> &str {
        self.0.split(' ').next().unwrap_or("")
    }
}

impl Drop for Credentials {
    fn drop(&mut self) {
        #[cfg(test)]
        let held = !self.0.is_empty();
        self.0.zeroize();
        // The test seam: that the drop wiped something, counted - a safe way to
        // see it, since the freed memory itself cannot be read without `unsafe`.
        #[cfg(test)]
        {
            if held && self.0.is_empty() {
                tests::WIPED.with(|wiped| wiped.set(wiped.get() + 1));
            }
        }
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Credentials({} …)", self.scheme())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    thread_local! {
        /// How many credentials this thread dropped holding something, and
        /// left empty (see the `Drop` impl).
        pub(super) static WIPED: Cell<usize> = const { Cell::new(0) };
    }

    /// A credential that is dropped is wiped, and so is each clone of it: a
    /// copy left behind is a token left in memory.
    #[test]
    fn a_dropped_credential_is_wiped_with_every_clone() {
        let before = WIPED.with(Cell::get);
        let one = Credentials::bearer("fake.token.value").unwrap();
        let two = one.clone();
        drop(one);
        assert_eq!(WIPED.with(Cell::get), before + 1);
        drop(two);
        assert_eq!(WIPED.with(Cell::get), before + 2);
    }

    /// Its `Debug` shows the scheme and nothing of the token.
    #[test]
    fn a_credentials_debug_shows_no_token() {
        let shown = format!("{:?}", Credentials::bearer("fake.token.value").unwrap());
        assert!(
            shown.contains("Bearer") && !shown.contains("fake.token.value"),
            "{shown}"
        );
        let basic = format!("{:?}", Credentials::basic("user", "fake-password").unwrap());
        assert!(
            !basic.contains("fake-password") && !basic.contains("dXNlcjpmYWtl"),
            "{basic}"
        );
    }
}
