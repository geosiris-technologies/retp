//! Where credentials come from when one fixed [`Credentials`] is not enough:
//! a source that can give a new one, and take back the news that one was refused.
//!
//! A token lasts less than an ETP session (ETP v1.2 §3.13.5, p.90: an endpoint
//! MUST re-authorize *before* its authorization expires), and the same server
//! is often reached twice - an ETP session and the REST API beside it - with
//! the **same** token. An application that owns the token (it knows when it
//! changes, how to ask for another, when to give up) hands this crate a
//! [`CredentialSource`]; the crate asks it for credentials when it opens a
//! session, sends a REST request or renews an open session
//! ([`LiveSession::renew`](crate::live::LiveSession::renew),
//! [`Client::reauthorize_with`](crate::Client::reauthorize_with),
//! [`AsyncClient::reauthorize_with`](crate::AsyncClient::reauthorize_with)).
//!
//! # The witness
//!
//! The source keeps **no state per connection**. Every [`Fresh`] it gives
//! carries a [`Witness`]: whatever the application put in it (a generation
//! number, the id of a token, the time it was taken). When the server refuses
//! the credentials of **that** connection, [`CredentialSource::refused`] gets
//! **that** witness back. So two connections that share one source - a
//! session and REST calls to the same server - never step on each other: the
//! refusal of one is never read as the refusal of the credentials the other
//! holds. A source that kept "the last credentials it gave" in a field could
//! not tell them apart.
//!
//! # Where the source is called
//!
//! A source may block (it may make a network request to get a token). It is
//! called **never on the thread or task that runs a session**: the live
//! session's task reads the socket, and a source that blocks there would stop
//! the keepalive and every answer with it. It is called on the thread of the
//! caller that asks - the one that opens the session, renews it, or drives
//! the REST future.
//!
//! # What this crate does not do
//!
//! It does not decide **when** to renew. The timing belongs to the application,
//! which knows when its token changes; it calls the door and this crate carries
//! the credentials and the refusal.
//!
//! A secret is never in an error, a `Debug` or a log line of this module: the
//! types print what they are, not what they hold.

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;

use retp_proto::credentials::Credentials;
use retp_proto::wire::{EtpError, Incident};

use crate::ClientError;

/// What an application puts in a [`Fresh`] and gets back when the server
/// refuses those credentials.
///
/// Opaque to this crate: it is carried, never read. Its `Debug` shows nothing
/// of what it holds.
pub struct Witness(Option<Box<dyn Any + Send + Sync>>);

impl Witness {
    /// A witness holding `value`.
    pub fn new<T: Any + Send + Sync>(value: T) -> Self {
        Self(Some(Box::new(value)))
    }

    /// A witness with nothing in it - for a source that has nothing to tell
    /// its credentials apart.
    pub fn none() -> Self {
        Self(None)
    }

    /// Whether nothing is in it.
    pub fn is_none(&self) -> bool {
        self.0.is_none()
    }

    /// What the application put in it, if it is a `T`.
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.0.as_deref().and_then(|held| held.downcast_ref::<T>())
    }

    /// Take back what the application put in it, if it is a `T`; otherwise
    /// the witness comes back whole.
    pub fn downcast<T: Any>(self) -> Result<T, Self> {
        match self.0 {
            Some(held) => match held.downcast::<T>() {
                Ok(value) => Ok(*value),
                Err(held) => Err(Self(Some(held))),
            },
            None => Err(self),
        }
    }
}

impl Default for Witness {
    fn default() -> Self {
        Self::none()
    }
}

impl fmt::Debug for Witness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.is_none() {
            "Witness(none)"
        } else {
            "Witness(..)"
        })
    }
}

/// Credentials from a [`CredentialSource`], with their witness and, when the
/// source knows it, the moment they end.
///
/// Its `Debug` shows what [`Credentials`] and [`Witness`] show: the scheme, and
/// nothing of the secret.
#[derive(Debug)]
pub struct Fresh {
    /// What to send.
    pub credentials: Credentials,
    /// Given back to [`CredentialSource::refused`] if the server refuses
    /// **these** credentials.
    pub witness: Witness,
    /// When they end, if the source knows (`expires_in` counted from the
    /// moment they were obtained, see
    /// [`Obtained::into_fresh`](crate::login::Obtained::into_fresh)).
    pub valid_until: Option<SystemTime>,
}

/// The source could not give credentials at all. The reason is for a person
/// to read: never put a credential in it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceError(pub String);

impl fmt::Display for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SourceError {}

/// The source has no other credentials to try. The reason is for a person to
/// read: never put a credential in it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GaveUp(pub String);

impl fmt::Display for GaveUp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for GaveUp {}

/// What the server refused, and where: the equivalent of an HTTP 401 on each
/// of the three ways credentials reach it. Never carries a credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// HTTP 401 on the WebSocket upgrade that opens an ETP session
    /// (§4.1.4, p.95).
    Handshake {
        /// The `WWW-Authenticate` challenges.
        challenges: Vec<String>,
    },
    /// HTTP 401 on a REST request.
    Rest {
        /// The `WWW-Authenticate` challenge, if the server sent one.
        challenges: Vec<String>,
    },
    /// An `AuthorizeResponse` without `success` (§5.3.7, p.123): the server
    /// refused the renewal of an open session. The session stays open until
    /// the authorization it holds runs out (§3.13.5 point 3, p.90).
    Authorize {
        /// The challenges the server sent with it.
        challenges: Vec<String>,
    },
}

impl Refused {
    /// The challenges the server sent with the refusal.
    pub fn challenges(&self) -> &[String] {
        match self {
            Refused::Handshake { challenges }
            | Refused::Rest { challenges }
            | Refused::Authorize { challenges } => challenges,
        }
    }
}

/// What this crate asks credentials from. See the [module](self) for what it
/// promises (the witness, the thread) and what it leaves to the application.
pub trait CredentialSource: Send + Sync {
    /// Credentials for ONE opening, ONE REST request or ONE renewal. May
    /// block: never called on the thread or task of a session.
    fn credentials(&self) -> Result<Fresh, SourceError>;

    /// The server refused credentials this source gave - `witness` is that of
    /// **those** credentials. The next credentials to try (once: a second
    /// refusal is the error of the call), or giving up.
    fn refused(&self, witness: Witness, why: &Refused) -> Result<Fresh, GaveUp>;
}

/// A fixed [`Credentials`] is a source that has nothing else to give: the
/// same credentials every time, and no other after a refusal.
impl CredentialSource for Credentials {
    fn credentials(&self) -> Result<Fresh, SourceError> {
        Ok(Fresh {
            credentials: self.clone(),
            witness: Witness::none(),
            valid_until: None,
        })
    }

    fn refused(&self, _: Witness, _: &Refused) -> Result<Fresh, GaveUp> {
        Err(GaveUp(
            "these are fixed credentials: there are no others to try".into(),
        ))
    }
}

impl<S: CredentialSource + ?Sized> CredentialSource for Arc<S> {
    fn credentials(&self) -> Result<Fresh, SourceError> {
        (**self).credentials()
    }

    fn refused(&self, witness: Witness, why: &Refused) -> Result<Fresh, GaveUp> {
        (**self).refused(witness, why)
    }
}

// ---------------------------------------------------------------------------
// What the doors share: one implementation of "ask, and ask again once".
// ---------------------------------------------------------------------------

/// A source that cannot give credentials: said by name. An opening that has
/// none is refused; a call of an open session is not sent.
pub(crate) fn unavailable(why: &SourceError, opening: bool) -> ClientError {
    let incident: Incident = EtpError::EAUTHORIZATION_REQUIRED
        .with_detail(format!("the credential source gave no credentials: {why}"));
    if opening {
        ClientError::Refused(incident)
    } else {
        ClientError::Invalid(incident)
    }
}

/// The first credentials of an attempt.
pub(crate) fn begin(source: &dyn CredentialSource, opening: bool) -> Result<Fresh, ClientError> {
    source.credentials().map_err(|why| unavailable(&why, opening))
}

/// The credentials after a refusal of `used`: the witness of **those**
/// credentials goes back to the source. The source giving up is the refusal
/// as the server said it, [`ClientError::AuthorizationNeeded`]. The caller
/// tries once more with what comes back, and a second refusal is its error:
/// this is not called again.
pub(crate) fn again(source: &dyn CredentialSource, used: Fresh, why: Refused) -> Result<Fresh, ClientError> {
    let Fresh { witness, .. } = used;
    source.refused(witness, &why).map_err(|gave_up| {
        tracing::info!(target: "retp_client", "the credential source gave up: {gave_up}");
        ClientError::AuthorizationNeeded {
            challenges: why.challenges().to_vec(),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_witness_gives_back_what_was_put_in_it_and_shows_nothing() {
        let witness = Witness::new(7_u64);
        assert_eq!(format!("{witness:?}"), "Witness(..)");
        assert_eq!(witness.downcast_ref::<u64>(), Some(&7));
        assert!(witness.downcast_ref::<String>().is_none());
        let witness = witness.downcast::<String>().unwrap_err();
        assert_eq!(witness.downcast::<u64>().unwrap(), 7);
        assert!(Witness::none().is_none());
    }

    #[test]
    fn fixed_credentials_have_no_other_to_give() {
        let fixed = Credentials::bearer("fake.token").unwrap();
        let fresh = fixed.credentials().unwrap();
        assert_eq!(fresh.credentials.header(), "Bearer fake.token");
        assert!(
            fixed
                .refused(
                    fresh.witness,
                    &Refused::Rest {
                        challenges: Vec::new()
                    }
                )
                .is_err()
        );
    }
}
