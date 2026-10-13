//! A session that opens again after a request timed out.
//!
//! A store that does not answer one request may not answer the next ones on
//! the same session either - a server's `Pong`, correlated to 0, never answered
//! the `Ping`, and the run waited past the folder script's limit with
//! nothing written. The rule: when a command times out, cut and open the
//! connection again for the following tests. So the checks run on
//! a [`Renewing`] session: a request that times out is reported as it is -
//! the check that sent it says it - and the next request goes on a session
//! opened again from the start, by the same [`Connector`] that opened the
//! first. Each renewal is said in the report's head.

use std::time::Duration;

use retp_avro::EtpMessage;
use retp_client::resume::Connector;
use retp_client::{Acknowledged, Blocking, Caller, ClientError};
use retp_proto::connection::Event;
use retp_proto::negotiate::{Negotiated, Tolerance};
use retp_proto::wire::Delivery;

use crate::Session;
use crate::exchanges::{self, Exchange};

/// How many times a run opens its session again, at most: a store that
/// times out on everything is not opened forty times.
const MOST: usize = 5;

/// A session the checks run on, opened again after a timeout.
pub struct Renewing<C: Connector>
where
    C::Session: Session,
{
    inner: C::Session,
    connector: C,
    renewed: Vec<String>,
    exchanged: Vec<Exchange>,
    generation: usize,
}

impl<C: Connector> Renewing<C>
where
    C::Session: Session,
{
    /// The session `inner`, which `connector` opened and can open again.
    pub fn new(inner: C::Session, connector: C) -> Self {
        Self {
            inner,
            connector,
            renewed: Vec::new(),
            exchanged: Vec::new(),
            generation: 0,
        }
    }

    /// The session in use, to close it.
    pub fn into_inner(self) -> C::Session {
        self.inner
    }

    /// Keep one exchange for the report's appendix.
    fn keep(&mut self, request: &str, sent: String, answer: &Result<Vec<Delivery>, ClientError>) {
        let (received, outcome) = match answer {
            Ok(deliveries) => {
                let refused = deliveries.iter().any(|d| d.exception().is_some());
                (
                    exchanges::all_received(deliveries),
                    if refused {
                        "refused".to_string()
                    } else {
                        "answered".to_string()
                    },
                )
            }
            Err(why) => (Vec::new(), format!("not answered: {why}")),
        };
        self.exchanged.push(Exchange {
            request: request.to_string(),
            sent,
            received,
            outcome,
        });
    }

    /// After an answer: a timeout opens the session again, once the request
    /// is over - the caller has its error.
    fn after<T>(&mut self, answer: &Result<T, ClientError>, what: &str) {
        if let Err(ClientError::TimedOut { request, incident }) = answer {
            if self.renewed.len() >= MOST {
                return;
            }
            let why = format!("{what} #{request} timed out ({incident})");
            match self.connector.connect() {
                Ok(session) => {
                    let old = std::mem::replace(&mut self.inner, session);
                    old.end("retp-conformance: a request timed out; going on with a new session");
                    self.generation += 1;
                    self.renewed.push(format!("{why}: the session was closed and opened again for the checks that follow"));
                }
                Err(error) => self.renewed.push(format!("{why}: opening the session again failed ({error}); the checks that follow use the old one")),
            }
        }
    }
}

impl<C: Connector> Caller for Renewing<C>
where
    C::Session: Session,
{
    async fn call<M: EtpMessage>(&mut self, request: &M) -> Result<Vec<Delivery>, ClientError> {
        let sent = exchanges::sent(request);
        let answer = self.inner.call(request).await;
        self.keep(M::FULL_NAME, sent, &answer);
        self.after(&answer, M::FULL_NAME);
        answer
    }

    async fn call_parts<M: EtpMessage, P: EtpMessage>(
        &mut self,
        first: &M,
        parts: &[P],
    ) -> Result<Vec<Delivery>, ClientError> {
        let sent = format!("{}\n… and {} part(s)", exchanges::sent(first), parts.len());
        let answer = self.inner.call_parts(first, parts).await;
        self.keep(M::FULL_NAME, sent, &answer);
        self.after(&answer, M::FULL_NAME);
        answer
    }

    async fn call_all<M: EtpMessage>(&mut self, requests: &[M]) -> Vec<Result<Vec<Delivery>, ClientError>> {
        let answers = self.inner.call_all(requests).await;
        for (request, answer) in requests.iter().zip(&answers) {
            self.keep(M::FULL_NAME, exchanges::sent(request), answer);
        }
        if let Some(timed_out) = answers
            .iter()
            .find(|one| matches!(one, Err(ClientError::TimedOut { .. })))
        {
            let timed_out = timed_out.clone().map(|_| ());
            self.after(&timed_out, M::FULL_NAME);
        }
        answers
    }

    fn at_once(&self) -> usize {
        self.inner.at_once()
    }

    fn tell<M: EtpMessage>(&mut self, message: &M) -> impl Future<Output = Result<(), ClientError>> {
        self.inner.tell(message)
    }

    fn subscribe(&mut self, protocol: i32) {
        self.inner.subscribe(protocol)
    }

    fn unsubscribe(&mut self, protocol: i32) {
        self.inner.unsubscribe(protocol)
    }

    fn events(&mut self, protocol: i32) -> impl Future<Output = Result<Vec<Event>, ClientError>> {
        self.inner.events(protocol)
    }

    fn watermark(&self) -> Option<i64> {
        self.inner.watermark()
    }

    fn negotiated(&self) -> Negotiated {
        self.inner.negotiated()
    }

    fn tolerance(&self) -> Tolerance {
        self.inner.tolerance()
    }
}

impl<C: Connector> Blocking for Renewing<C>
where
    C::Session: Session,
{
    fn end(self, reason: &str) {
        self.inner.end(reason)
    }
}

impl<C: Connector> Session for Renewing<C>
where
    C::Session: Session,
{
    fn is_rest(&self) -> bool {
        self.inner.is_rest()
    }

    fn session(&self) -> Negotiated {
        self.inner.session()
    }

    fn ping(&mut self) -> Result<Duration, ClientError> {
        let answer = self.inner.ping();
        self.exchanged.push(Exchange {
            request: "Energistics.Etp.v12.Protocol.Core.Ping".into(),
            sent: "Ping { current_date_time: <now> }".into(),
            received: Vec::new(),
            outcome: match &answer {
                Ok(took) => format!("Pong in {} ms", took.as_millis()),
                Err(why) => format!("no Pong: {why}"),
            },
        });
        self.after(&answer, "Ping");
        answer
    }

    fn ask_acknowledge(&mut self, on: bool) {
        self.inner.ask_acknowledge(on)
    }

    fn acknowledgements(&mut self) -> Vec<Acknowledged> {
        self.inner.acknowledgements()
    }

    fn unsolicited(&mut self) -> Vec<Event> {
        self.inner.unsolicited()
    }

    fn renewals(&mut self) -> Vec<String> {
        std::mem::take(&mut self.renewed)
    }

    fn exchanges(&mut self) -> Vec<Exchange> {
        std::mem::take(&mut self.exchanged)
    }

    fn generation(&self) -> usize {
        self.generation
    }
}
