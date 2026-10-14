//! Blocks of arrays and declarations on an ETP dataspace - `put_arrays` with a
//! location that has a `start` and a `count` (`PutDataSubarrays`), and
//! `declare_arrays` (`PutUninitializedDataArrays`) - played against a fake
//! server that keeps what it is given.
//!
//! The server is the **second source**: it keeps the arrays it was declared
//! (a cell nobody wrote stays `NaN`), walks each block into place with its own
//! row arithmetic, and logs what arrived. Each test states what the trait
//! promises (one result per item, in order; a refusal is that item's own; a
//! read-only transaction writes nothing; a write forgets the listing) and
//! reads it in the server's log, not in the store's return value.
//!
//! ETP 1.2: §13.2.1.2 (declare, then blocks), §13.3.7 and §13.3.9, §23.32.7
//! (`starts` and `counts` per dimension).
//!
//! # How to make each test fail
//!
//! - `a_declared_array_takes_a_block_where_its_start_says`: in `src/blocks.rs`
//!   send `starts` as zeros (the block lands at the top), or in `declaration_of`
//!   map every element type to `ArrayValues::F64`.
//! - `a_call_mixes_whole_arrays_and_blocks_and_each_is_answered`: in
//!   `writer.rs` send blocks in the whole-array list (`writes`), or drop the
//!   `ShapeDisagrees` check in `block_of`.
//! - `a_block_the_server_refuses_...`: answer `Ok(())` for every block in
//!   `write_blocks`.
//! - `what_cannot_be_declared_or_placed_is_refused_by_name_before_anything_leaves`:
//!   drop the `count` refusal in `block_of`, the element refusal in
//!   `empty_values`, or the `start`/`count` refusal in `declaration_of`.
//! - `a_read_only_transaction_...`: drop `writable()` from `declare_arrays` or
//!   from the block branch of `put_arrays` in `writer.rs`.
//! - `a_listing_taken_before_...`: remove `forget_listing()` after `write_blocks`
//!   or after `declare` in `writer.rs`.
//! - `a_block_bigger_than_a_message_...`: pass the block whole (do not cut it
//!   in `write_subarrays`).

mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use energyml_etp_store::EtpStore;
use energyml_storage::array::{ArrayData, ArrayLocation, ArrayMetadata, ArrayValues};
use energyml_storage::error::{Error, ExternalDataReason};
use energyml_storage::storage::EnergymlStorage;
use energyml_storage::write::{EnergymlWriter, TransactionRequest};
use retp_avro::Role;
use retp_client::memory::{ManualClock, MemoryTransport};
use retp_client::{Client, Settings, writing_client};
use retp_proto::capabilities::{Value as CapabilityValue, names};
use retp_proto::connection::{Connection, Declined, Handled, Handler, Responder};
use retp_proto::negotiate::{Offer, ProtocolOffer};
use retp_proto::wire::{Body, Delivery, EtpError};
use retp_types::datatypes::{AnyArrayItem, AnyArrayType, AnyLogicalArrayType, SupportedDataObject, Uuid};
use retp_types::dispatch::Message;
use retp_types::protocol::data_array::{
    PutDataArraysResponse, PutDataSubarraysResponse, PutUninitializedDataArraysResponse,
};
use retp_types::protocol::discovery::GetResourcesResponse;
use retp_types::protocol::transaction::StartTransactionResponse;

use common::*;

const POINTS_TYPE: &str = "resqml20.obj_PointSetRepresentation";

/// A declared array: path, dimensions, transport and logical type.
type Declared = (String, Vec<i64>, AnyArrayType, AnyLogicalArrayType);

/// What the server was given, in the order it was given.
#[derive(Default)]
struct Log {
    /// `start`, `resources`, `declare`, `array`, `subarray`, as they came.
    events: Vec<&'static str>,
    /// How many `GetResources` were asked.
    listings: usize,
    declared: Vec<Declared>,
    /// Each array by path: dimensions and doubles, `NaN` where nobody wrote.
    held: BTreeMap<String, (Vec<i64>, Vec<f64>)>,
    /// Each block that arrived: path, starts, counts.
    blocks: Vec<(String, Vec<i64>, Vec<i64>)>,
}

struct Receiving {
    protocol: i32,
    log: Arc<Mutex<Log>>,
}

impl Handler for Receiving {
    fn offer(&self) -> ProtocolOffer {
        ProtocolOffer::v12(self.protocol, Role::Store)
    }

    fn handle(&mut self, delivery: &Delivery, responder: &mut Responder<'_>) -> Result<Handled, Declined> {
        let Body::Message(message) = &delivery.body else {
            return Err(Declined("no private protocol".into()));
        };
        let mut log = self.log.lock().unwrap();
        match message {
            Message::StartTransaction(_) => {
                log.events.push("start");
                let response = StartTransactionResponse {
                    transaction_uuid: Uuid([9; 16]),
                    successful: true,
                    failure_reason: String::new(),
                };
                let out = responder.wire().respond(delivery, &response).unwrap();
                responder.send(out);
            }
            Message::GetResources(_) => {
                log.events.push("resources");
                log.listings += 1;
                let out = responder
                    .wire()
                    .respond(
                        delivery,
                        &GetResourcesResponse {
                            resources: Vec::new(),
                        },
                    )
                    .unwrap();
                responder.send(out);
            }
            Message::PutDataArrays(request) => {
                log.events.push("array");
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_arrays.keys().cloned());
                for (key, put) in &request.data_arrays {
                    if let AnyArrayItem::ArrayOfDouble(values) = &put.array.data.item {
                        log.held.insert(
                            put.uid.path_in_resource.clone(),
                            (put.array.dimensions.clone(), values.values.clone()),
                        );
                    }
                    reply.answered(key.clone()).unwrap();
                }
                let out = responder
                    .wire()
                    .last(
                        &mut reply,
                        &PutDataArraysResponse {
                            success: BTreeMap::new(),
                        },
                    )
                    .unwrap();
                responder.send_all(out);
            }
            Message::PutUninitializedDataArrays(request) => {
                log.events.push("declare");
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_arrays.keys().cloned());
                for (key, put) in &request.data_arrays {
                    let path = put.uid.path_in_resource.clone();
                    let dimensions = put.metadata.dimensions.clone();
                    log.declared.push((
                        path.clone(),
                        dimensions.clone(),
                        put.metadata.transport_array_type,
                        put.metadata.logical_array_type,
                    ));
                    let count: i64 = dimensions.iter().product();
                    log.held
                        .insert(path, (dimensions, vec![f64::NAN; count.max(0) as usize]));
                    reply.answered(key.clone()).unwrap();
                }
                let out = responder
                    .wire()
                    .last(
                        &mut reply,
                        &PutUninitializedDataArraysResponse {
                            success: BTreeMap::new(),
                        },
                    )
                    .unwrap();
                responder.send_all(out);
            }
            Message::PutDataSubarrays(request) => {
                log.events.push("subarray");
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_subarrays.keys().cloned());
                for (key, put) in &request.data_subarrays {
                    log.blocks.push((
                        put.uid.path_in_resource.clone(),
                        put.starts.clone(),
                        put.counts.clone(),
                    ));
                    // A block of an array nobody declared has nowhere to go.
                    let Some((dimensions, into)) = log.held.get_mut(&put.uid.path_in_resource) else {
                        let refusal = EtpError::ENOT_FOUND
                            .with_detail(format!("{} was not declared", put.uid.path_in_resource));
                        reply.failed(key.clone(), &refusal).unwrap();
                        continue;
                    };
                    let rank = dimensions.len();
                    let inside = put.starts.len() == rank
                        && put.counts.len() == rank
                        && (0..rank).all(|k| {
                            put.starts[k] >= 0
                                && put.counts[k] > 0
                                && put.starts[k] + put.counts[k] <= dimensions[k]
                        });
                    let AnyArrayItem::ArrayOfDouble(from) = &put.data.item else {
                        reply
                            .failed(
                                key.clone(),
                                &EtpError::EINVALID_ARGUMENT.with_detail("this server holds doubles"),
                            )
                            .unwrap();
                        continue;
                    };
                    if !inside || from.values.len() as i64 != put.counts.iter().product::<i64>() {
                        reply
                            .failed(
                                key.clone(),
                                &EtpError::EINVALID_ARGUMENT.with_detail("the block does not fit the array"),
                            )
                            .unwrap();
                        continue;
                    }
                    // Row by row, walked here on purpose rather than by the
                    // client's helper.
                    let run = put.counts[rank - 1] as usize;
                    let rows = put.counts.iter().product::<i64>() / put.counts[rank - 1];
                    let mut index = vec![0i64; rank - 1];
                    for row in 0..rows as usize {
                        let offset = (0..rank).fold(0i64, |offset, k| {
                            let at = if k + 1 == rank {
                                put.starts[k]
                            } else {
                                put.starts[k] + index[k]
                            };
                            offset * dimensions[k] + at
                        }) as usize;
                        into[offset..offset + run].copy_from_slice(&from.values[row * run..row * run + run]);
                        for k in (0..rank - 1).rev() {
                            index[k] += 1;
                            if index[k] < put.counts[k] {
                                break;
                            }
                            index[k] = 0;
                        }
                    }
                    reply.answered(key.clone()).unwrap();
                }
                let out = responder
                    .wire()
                    .last(
                        &mut reply,
                        &PutDataSubarraysResponse {
                            success: BTreeMap::new(),
                        },
                    )
                    .unwrap();
                responder.send_all(out);
            }
            _ => return Err(Declined("not in the receiving server".into())),
        }
        Ok(Handled::Answered)
    }
}

type Store = EtpStore<MemoryTransport, ManualClock>;

/// A store of dataspace `demo` over a writing session with a server that keeps
/// what it is given; its messages carry `payload` bytes at most when it says
/// so.
fn writing_store(payload: Option<i64>) -> (Store, Arc<Mutex<Log>>) {
    let log = Arc::new(Mutex::new(Log::default()));
    let mut offer = Offer::new("a receiving store", "0.1", Uuid([3; 16]));
    offer.formats = vec!["xml".to_string(), "json".to_string()];
    offer.data_objects = ["resqml20.*", "eml20.*", "eml23.*"]
        .iter()
        .map(|t| SupportedDataObject {
            qualified_type: t.to_string(),
            data_object_capabilities: BTreeMap::new(),
        })
        .collect();
    if let Some(payload) = payload {
        offer.endpoint = offer
            .endpoint
            .set(
                names::MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE,
                CapabilityValue::Long(payload),
            )
            .unwrap();
    }
    let handlers: Vec<Box<dyn Handler>> = [3, 4, 9, 18, 24]
        .iter()
        .map(|p| {
            Box::new(Receiving {
                protocol: *p,
                log: Arc::clone(&log),
            }) as Box<dyn Handler>
        })
        .collect();
    let server = Connection::server(offer, handlers, None, Uuid([0xBC; 16]), T0).unwrap();
    let clock = ManualClock::new(T0);
    let (offer, handlers) = writing_client(
        "the blocks' tests",
        "0.1",
        Uuid([4; 16]),
        &[
            "resqml20.*".to_string(),
            "eml20.*".to_string(),
            "eml23.*".to_string(),
        ],
    );
    let client = Client::open(
        MemoryTransport::new(server, clock.clone()),
        clock,
        offer,
        handlers,
        None,
        Settings::default(),
    )
    .unwrap();
    (EtpStore::new(client, "demo", registry()), log)
}

/// The whole array `path` of the point set.
fn whole(path: &str) -> ArrayLocation {
    ArrayLocation::etp(uri(POINTS_TYPE, POINTS), path)
}

/// The block of `path` from `start`, `count` long.
fn part(path: &str, start: Vec<i64>, count: Vec<i64>) -> ArrayLocation {
    let mut location = whole(path);
    location.start = start;
    location.count = count;
    location
}

/// A block of doubles `first, first + 1, ...`, shaped `dimensions`.
fn doubles(dimensions: Vec<i64>, first: f64) -> ArrayData {
    let count: i64 = dimensions.iter().product();
    ArrayData {
        metadata: ArrayMetadata::new(dimensions, "double"),
        values: ArrayValues::F64((0..count).map(|i| first + i as f64).collect()),
    }
}

/// `NaN` is « nobody wrote it »: `None`.
fn cells(values: &[f64]) -> Vec<Option<f64>> {
    values.iter().map(|v| (!v.is_nan()).then_some(*v)).collect()
}

/// **A declared array takes a block where its `start` says**, and the server
/// was told the shape and the two types the element type stands for.
#[test]
fn a_declared_array_takes_a_block_where_its_start_says() {
    let (mut store, log) = writing_store(None);
    let declared = block(store.declare_arrays(&[(whole("/grid"), ArrayMetadata::new(vec![4, 3], "int16"))]));
    assert_eq!(declared.len(), 1);
    assert!(declared[0].is_ok(), "{:?}", declared[0]);
    assert_eq!(
        log.lock().unwrap().declared,
        vec![(
            "/grid".to_string(),
            vec![4, 3],
            AnyArrayType::ArrayOfInt,
            AnyLogicalArrayType::ArrayOfInt16Le
        )],
        "int16 is declared as the writer would send it: arrayOfInt, arrayOfInt16LE"
    );

    // A double array, for the values: declared as such, then a block of it.
    assert!(
        block(store.declare_arrays(&[(whole("/d"), ArrayMetadata::new(vec![4, 3], "double"))]))[0].is_ok()
    );
    let results = block(store.put_arrays(&[(part("/d", vec![2, 0], vec![2, 3]), doubles(vec![2, 3], 1.0))]));
    assert_eq!(results.len(), 1);
    assert!(results[0].is_ok(), "{:?}", results[0]);

    let log = log.lock().unwrap();
    assert_eq!(log.blocks, vec![("/d".to_string(), vec![2, 0], vec![2, 3])]);
    let mut expected = vec![None; 12];
    for (i, cell) in expected.iter_mut().enumerate().skip(6) {
        *cell = Some((i - 5) as f64);
    }
    assert_eq!(cells(&log.held["/d"].1), expected);
}

/// **One call mixes whole arrays and blocks, one result each, in order**: the
/// whole array goes by `PutDataArrays`, the block by `PutDataSubarrays`; a
/// block whose values do not make its shape, and one in another dataspace, are
/// refused before anything leaves for them.
#[test]
fn a_call_mixes_whole_arrays_and_blocks_and_each_is_answered() {
    let (mut store, log) = writing_store(None);
    assert!(
        block(store.declare_arrays(&[(whole("/d"), ArrayMetadata::new(vec![4, 3], "double"))]))[0].is_ok()
    );

    let mut lying = doubles(vec![2, 3], 0.0);
    lying.values = ArrayValues::F64(vec![1.0; 5]);
    let mut elsewhere =
        ArrayLocation::etp(format!("eml:///dataspace('other')/{POINTS_TYPE}({POINTS})"), "/d");
    elsewhere.start = vec![0, 0];
    elsewhere.count = vec![2, 3];
    let results = block(store.put_arrays(&[
        (whole("/whole"), doubles(vec![2, 3], 10.0)),
        (part("/d", vec![0, 0], vec![2, 3]), doubles(vec![2, 3], 1.0)),
        (part("/d", vec![2, 0], vec![2, 3]), lying),
        (elsewhere, doubles(vec![2, 3], 0.0)),
    ]));
    assert_eq!(results.len(), 4);
    assert!(results[0].is_ok(), "{:?}", results[0]);
    assert!(results[1].is_ok(), "{:?}", results[1]);
    assert!(
        matches!(
            results[2],
            Err(Error::ExternalData {
                reason: ExternalDataReason::ShapeDisagrees { promised: 6, held: 5 },
                ..
            })
        ),
        "{:?}",
        results[2]
    );
    assert!(
        matches!(results[3], Err(Error::BadUri { .. })),
        "{:?}",
        results[3]
    );

    let log = log.lock().unwrap();
    assert_eq!(log.events, ["declare", "array", "subarray"]);
    assert_eq!(log.blocks.len(), 1, "only the honest block left");
    assert_eq!(
        cells(&log.held["/d"].1)[..6],
        [Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0), Some(6.0)]
    );
    assert_eq!(cells(&log.held["/d"].1)[6..], [None; 6]);
    assert_eq!(log.held["/whole"].1, vec![10.0, 11.0, 12.0, 13.0, 14.0, 15.0]);
}

/// **A block the server refuses is that block's own error**, and the next one
/// goes through - here a block of an array nobody declared.
#[test]
fn a_block_the_server_refuses_fails_alone_and_the_others_go_through() {
    let (mut store, log) = writing_store(None);
    assert!(
        block(store.declare_arrays(&[(whole("/d"), ArrayMetadata::new(vec![4, 3], "double"))]))[0].is_ok()
    );
    let results = block(store.put_arrays(&[
        (
            part("/undeclared", vec![0, 0], vec![1, 3]),
            doubles(vec![1, 3], 0.0),
        ),
        (part("/d", vec![3, 0], vec![1, 3]), doubles(vec![1, 3], 7.0)),
    ]));
    assert_eq!(results.len(), 2);
    match &results[0] {
        Err(Error::ExternalData {
            reason: ExternalDataReason::Backend(why),
            ..
        }) => assert!(why.contains("was not declared"), "{why}"),
        other => panic!("expected the server's refusal, got {other:?}"),
    }
    assert!(results[1].is_ok(), "{:?}", results[1]);
    assert_eq!(
        cells(&log.lock().unwrap().held["/d"].1)[9..],
        [Some(7.0), Some(8.0), Some(9.0)]
    );
}

/// **What cannot be placed or declared is refused by name, before anything
/// leaves**: a `start` without a `count`, a negative start, a count that is not
/// the data's shape; a declaration of an element type this build does not send,
/// of a block, or of a logical type of another element.
#[test]
fn what_cannot_be_declared_or_placed_is_refused_by_name_before_anything_leaves() {
    let (mut store, log) = writing_store(None);
    let mut start_alone = whole("/d");
    start_alone.start = vec![2, 0];
    let results = block(store.put_arrays(&[
        (start_alone, doubles(vec![2, 3], 0.0)),
        (part("/d", vec![-1, 0], vec![2, 3]), doubles(vec![2, 3], 0.0)),
        (part("/d", vec![0, 0], vec![2, 3]), doubles(vec![3, 2], 0.0)),
    ]));
    assert_eq!(results.len(), 3);
    for (position, result) in results.iter().enumerate() {
        assert!(
            matches!(
                result,
                Err(Error::ExternalData {
                    reason: ExternalDataReason::Backend(_),
                    ..
                })
            ),
            "{position}: {result:?}"
        );
    }

    let mut odd = ArrayMetadata::new(vec![4], "double");
    odd.logical_type = Some("arrayOfInt32LE".into());
    let declared = block(store.declare_arrays(&[
        (whole("/s"), ArrayMetadata::new(vec![4], "string")),
        (
            part("/d", vec![0], vec![2]),
            ArrayMetadata::new(vec![4], "double"),
        ),
        (whole("/odd"), odd),
        (whole("/negative"), ArrayMetadata::new(vec![-4], "double")),
    ]));
    assert_eq!(declared.len(), 4);
    for (position, result) in declared.iter().enumerate() {
        assert!(
            matches!(
                result,
                Err(Error::ExternalData {
                    reason: ExternalDataReason::Backend(_),
                    ..
                })
            ),
            "{position}: {result:?}"
        );
    }
    assert!(log.lock().unwrap().events.is_empty(), "nothing left the client");
}

/// **A read-only transaction writes nothing**: neither a block nor a
/// declaration; only the start leaves.
#[test]
fn a_read_only_transaction_refuses_blocks_and_declarations() {
    let (mut store, log) = writing_store(None);
    let transaction = block(store.begin(TransactionRequest::new().read_only())).unwrap();
    assert!(transaction.request.read_only);

    let declared = block(store.declare_arrays(&[(whole("/d"), ArrayMetadata::new(vec![4, 3], "double"))]));
    let written = block(store.put_arrays(&[(part("/d", vec![0, 0], vec![2, 3]), doubles(vec![2, 3], 0.0))]));
    assert!(
        matches!(declared[0], Err(Error::Transaction { .. })),
        "{:?}",
        declared[0]
    );
    assert!(
        matches!(written[0], Err(Error::Transaction { .. })),
        "{:?}",
        written[0]
    );
    assert_eq!(log.lock().unwrap().events, ["start"]);
}

/// **A write is not a read**: a listing taken before a declaration or a block
/// is not served after it. Counted at the server: a cached listing asks it
/// nothing.
#[test]
fn a_listing_taken_before_a_declaration_or_a_block_is_not_served_after_it() {
    let (mut store, log) = writing_store(None);
    let listings = || log.lock().unwrap().listings;
    block(store.resources()).unwrap();
    block(store.resources()).unwrap();
    assert_eq!(listings(), 1, "the second listing came from the cache");

    assert!(
        block(store.declare_arrays(&[(whole("/d"), ArrayMetadata::new(vec![4, 3], "double"))]))[0].is_ok()
    );
    block(store.resources()).unwrap();
    assert_eq!(listings(), 2, "the declaration forgot the listing");

    let written = block(store.put_arrays(&[(part("/d", vec![0, 0], vec![2, 3]), doubles(vec![2, 3], 0.0))]));
    assert!(written[0].is_ok(), "{:?}", written[0]);
    block(store.resources()).unwrap();
    assert_eq!(listings(), 3, "the block forgot the listing");
}

/// **A block bigger than a message is cut by the client, each piece in its
/// place**: the session announces 20 000 bytes a message, the block is 48 000.
#[test]
fn a_block_bigger_than_a_message_arrives_in_pieces_each_in_its_place() {
    let (mut store, log) = writing_store(Some(20_000));
    assert!(
        block(store.declare_arrays(&[(whole("/big"), ArrayMetadata::new(vec![200, 100], "double"))]))[0]
            .is_ok()
    );
    let results = block(store.put_arrays(&[(
        part("/big", vec![10, 0], vec![60, 100]),
        doubles(vec![60, 100], 1.0),
    )]));
    assert!(results[0].is_ok(), "{:?}", results[0]);

    let log = log.lock().unwrap();
    assert!(log.blocks.len() >= 3, "{} piece(s)", log.blocks.len());
    let held = cells(&log.held["/big"].1);
    for row in 0..200usize {
        for col in 0..100usize {
            let want = (10..70)
                .contains(&row)
                .then(|| ((row - 10) * 100 + col + 1) as f64);
            assert_eq!(held[row * 100 + col], want, "row {row}, column {col}");
        }
    }
}
