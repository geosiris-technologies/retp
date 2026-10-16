//! The live session as a blocking caller, and `subscribe_to`, against a local WebSocket server
//! (`live_server`).
//!
//! The functions at the root of the crate (`retp_client::list_dataspaces`, ...) take a
//! `Blocking` caller; a `LiveSession` is one. Every test runs under a deadline, so a hang fails
//! instead of blocking CI.

#![cfg(all(feature = "live", not(target_arch = "wasm32")))]

mod live_server;

use std::sync::Arc;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

use live_server::{
    ARRIVAL, DATASPACE, OTHER_SUBSCRIPTION, REFUSED, SUBSCRIPTION, is_the_change_of, open, start_server,
    start_server_notifying_after, within,
};
use retp_client::Blocking;
use retp_client::live::Queue;
use retp_client::notifications::Watch;

/// A blocking operation of the crate runs on a `&mut LiveSession`, and `Blocking::end` closes it.
#[test]
fn a_blocking_operation_runs_on_the_live_session() {
    within(|| {
        let server = start_server();
        let mut session = open(&server);
        let list = retp_client::list_dataspaces(&mut session).expect("GetDataspaces is answered");
        let uris: Vec<String> = list.items.into_iter().map(|d| d.uri).collect();
        assert_eq!(uris, vec![DATASPACE.to_string()]);
        // Again: the session stays usable between two blocking calls.
        let again = retp_client::list_dataspaces(&mut session).expect("answered a second time");
        assert_eq!(again.items.len(), 1);
        session.end("test over");
        server
            .ended
            .recv_timeout(ARRIVAL)
            .expect("the server saw the connection end");
    });
}

/// What the session agreed is read from the open session, and `events` returns at once.
#[test]
fn the_caller_methods_answer_without_waiting() {
    use retp_client::Caller;
    within(|| {
        let server = start_server();
        let mut session = open(&server);
        let _ = Caller::negotiated(&session);
        let _ = Caller::tolerance(&session);
        let events = retp_client::blocking(Caller::events(&mut session, 5)).expect("events");
        assert!(events.is_empty(), "nothing was subscribed: {events:?}");
        session.end("done");
    });
}

/// The notification the server sends unasked reaches the queue, with nothing polling.
#[test]
fn subscribe_to_a_queue_delivers_the_notification_with_nothing_polling() {
    within(|| {
        let server = start_server();
        let session = open(&server);
        let (queue, arrivals) = Queue::bounded(16);
        let route = session
            .subscribe_to(&Watch::of(DATASPACE, SUBSCRIPTION), Arc::new(queue))
            .expect("the session answers")
            .expect("the server accepts the subscription");
        // From here the test calls nothing on the session.
        let event = arrivals
            .recv_timeout(ARRIVAL)
            .expect("the notification arrives without anything polling");
        assert!(
            is_the_change_of(&event, SUBSCRIPTION),
            "it is the ObjectChanged of the subscription: {event:?}"
        );
        assert_eq!(session.dropped(), 0);
        session.unroute(route);
        session.close("done").expect("the session closes");
    });
}

/// `unsubscribe_from` ends the subscription at the store and removes the route: the queue is
/// disconnected (the session no longer holds it) and nothing arrives.
#[test]
fn unsubscribe_from_ends_the_subscription_and_its_route() {
    within(|| {
        // The notification would come after a second: the subscription ends first.
        let server = start_server_notifying_after(Duration::from_millis(1000));
        let mut session = open(&server);
        let (queue, arrivals) = Queue::bounded(16);
        let route = session
            .subscribe_to(&Watch::of(DATASPACE, SUBSCRIPTION), Arc::new(queue))
            .expect("the session answers")
            .expect("the server accepts the subscription");
        let reason = session
            .unsubscribe_from(SUBSCRIPTION, route)
            .expect("the session answers")
            .expect("the store ends the subscription");
        assert_eq!(reason, "unsubscribed");
        // The route is gone, so is the queue it held: the channel reports it at once instead of
        // timing out, and nothing was delivered.
        match arrivals.recv_timeout(ARRIVAL) {
            Err(RecvTimeoutError::Disconnected) => {}
            other => panic!("the queue should be disconnected and empty: {other:?}"),
        }
        assert_eq!(session.dropped(), 0);
        // The session goes on.
        assert_eq!(
            retp_client::list_dataspaces(&mut session)
                .expect("answered after the unsubscription")
                .items
                .len(),
            1
        );
        session.end("done");
    });
}

/// A refusal leaves no route: the store notifies for the refused uuid all the same (the test
/// server is faulty on purpose), and nothing reaches the destination, nothing is counted dropped.
#[test]
fn a_refused_subscription_leaves_no_route() {
    within(|| {
        let server = start_server();
        let session = open(&server);
        let (queue, arrivals) = Queue::bounded(16);
        let refused = session
            .subscribe_to(&Watch::of(REFUSED, OTHER_SUBSCRIPTION), Arc::new(queue))
            .expect("the session answers");
        let _refusal = refused.expect_err("the store refuses this context");
        // The queue was dropped with the route.
        match arrivals.recv_timeout(ARRIVAL) {
            Err(RecvTimeoutError::Disconnected) => {}
            other => panic!("the queue should be disconnected and empty: {other:?}"),
        }
        // Let the faulty notification come (200 ms) and be seen by nobody.
        std::thread::sleep(Duration::from_millis(600));
        assert_eq!(session.dropped(), 0, "an unrouted arrival is not a dropped one");
        // A good subscription is still accepted afterwards.
        let (queue, _arrivals) = Queue::bounded(16);
        session
            .subscribe_to(&Watch::of(DATASPACE, SUBSCRIPTION), Arc::new(queue))
            .expect("the session answers")
            .expect("the store accepts this one");
        session.close("done").expect("the session closes");
    });
}
