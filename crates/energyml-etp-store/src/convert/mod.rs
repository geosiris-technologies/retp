//! The conversions between ETP messages and the values of the storage traits, **both
//! ways, in one place**.
//!
//! A client over ETP reads an answer into storage values (ETP to storage); a server
//! built on the same traits answers a request from them (storage to ETP). Both sides
//! take their conversions from here, so that they cannot drift at the first change:
//! a server writes no conversion of its own. Each pair is named after what it converts,
//! `<thing>_from_etp` and `<thing>_to_etp`, side by side, and the tests play every pair
//! both ways (`tests/convert_objects.rs`, `tests/convert_arrays.rs`).
//!
//! - [`objects`](crate::convert::objects): resources, edges, dataspaces, contexts, supported types, deleted
//!   resources, errors;
//! - [`arrays`](crate::convert::arrays): array metadata, identifiers and locations, array values.

pub mod arrays;
pub mod objects;
