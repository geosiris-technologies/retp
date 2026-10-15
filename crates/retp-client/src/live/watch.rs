//! Subscribing to a destination.
//!
//! A subscription and a route are two halves of one thing: the store notifies for a
//! subscription uuid, and the route says where those notifications go. Done apart, the second
//! can lose what arrives between the answer to the first and the call that sets it. These two
//! methods do both, in the order that loses nothing.

use std::sync::Arc;

use retp_types::datatypes::{ErrorInfo, Uuid};

use super::{Destination, Filter, LiveSession, RouteId};
use crate::ClientError;
use crate::notifications::Watch;
use crate::notifications::asynchronous::{subscribe_notifications, unsubscribe_notifications};

impl LiveSession {
    /// Subscribe to `watch` and send what comes for it to `to`: the route is set first, so
    /// that nothing arriving between the answer and the route is lost; a refused subscription
    /// removes the route. The outer error is the session's, the inner the server's refusal.
    pub fn subscribe_to(
        &self,
        watch: &Watch,
        to: Arc<dyn Destination>,
    ) -> Result<Result<RouteId, ErrorInfo>, ClientError> {
        let route = self.route(Filter::Subscription(watch.request_uuid), to);
        let mut client = self.client().clone();
        let outcome = self.block_on(subscribe_notifications(&mut client, std::slice::from_ref(watch)));
        match outcome {
            Ok(mut outcomes) => match outcomes.drain(..).next() {
                Some(Ok(())) => Ok(Ok(route)),
                refused => {
                    self.unroute(route);
                    Ok(Err(refused
                        .and_then(Result::err)
                        .unwrap_or_else(crate::operations::asynchronous::nothing_said)))
                }
            },
            Err(why) => {
                self.unroute(route);
                Err(why)
            }
        }
    }

    /// End a subscription (`UnsubscribeNotifications`) and its route. The route is removed
    /// whatever the server answers: after this call nothing more is sent to the destination.
    /// The `Ok` is the store's reason (`SubscriptionEnded`), the inner error its refusal.
    pub fn unsubscribe_from(
        &self,
        subscription: Uuid,
        route: RouteId,
    ) -> Result<Result<String, ErrorInfo>, ClientError> {
        let mut client = self.client().clone();
        let outcome = self.block_on(unsubscribe_notifications(&mut client, subscription));
        self.unroute(route);
        outcome
    }
}
