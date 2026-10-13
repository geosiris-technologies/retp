//! An ETP v1.2 client.
//!
//! `retp-proto` knows what ETP **is** and reads no clock and opens no
//! socket; this crate is what a client **does** with it: a [`Transport`]
//! under the connection, a [`Clock`] beside it, and a [`Client`] that drives
//! the two - open a session, send a request and gather its answer, ping,
//! close (ADR-0001).
//!
//! Two clients over one [`engine::Engine`] - the client without a transport or
//! a clock: bytes in, bytes out, answers gathered. The [`Client`] **blocks**
//! on a [`Transport`], which is all the command line needs; the
//! [`AsyncClient`] is **fed** by whoever holds the socket and answers with
//! futures, which is what a browser's worker needs.
//! Every operation is written once, as
//! an `async fn` over [`Caller`] ([`asynchronous`]), and the functions at the
//! root run it on the blocking client. The tests bring a transport that talks
//! to a server connection in memory - every rule of tiers 1 and 2 is
//! exercised without a socket.

#![forbid(unsafe_code)]

pub mod arrays;
mod async_client;
mod caller;
pub mod channels;
mod client;
mod clock;
pub mod engine;
pub mod notifications;
mod offer;
mod operations;
mod profile;
pub mod rest;
pub mod resume;
mod transport;

#[cfg(not(target_arch = "wasm32"))]
pub mod download;
#[cfg(not(target_arch = "wasm32"))]
pub mod login;
pub mod memory;
#[cfg(not(target_arch = "wasm32"))]
pub mod native;
#[cfg(target_arch = "wasm32")]
pub mod web;

pub use async_client::{AsyncClient, Sink};
pub use caller::{Blocking, Caller, run as blocking};
pub use client::{Acknowledged, Client, ClientError, Settings};
pub use clock::{Clock, SystemClock};
pub use offer::{
    EVERY_RELEASE, FORMATS, READING, WRITING, customer_of, described_client, reading_client, writing_client,
};
pub use operations::{
    DataspaceList, Ended, Listed, Opened, ResourceList, ResourceQuery, SupportedTypeList, Transaction,
    commit_transaction, custom_data_value, dataspace_uri, delete_data_objects, delete_dataspaces,
    get_data_objects, list_dataspaces, list_resources, list_supported_types, object_file_name,
    put_data_objects, put_dataspaces, rollback_transaction, start_transaction,
};
pub use profile::{ServerProfile, plain_text_warning};
pub use transport::{Transport, TransportError, browser_fetch_hint};

/// Every operation, for any [`Caller`] - the [`AsyncClient`] of a front end among
/// them. Each is written once, here; the functions of the same name at the
/// root of the crate run it on the blocking [`Client`].
pub mod asynchronous {
    pub use crate::arrays::asynchronous::*;
    pub use crate::channels::asynchronous::*;
    pub use crate::notifications::asynchronous::*;
    pub use crate::operations::asynchronous::*;
}
