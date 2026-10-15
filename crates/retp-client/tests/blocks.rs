//! Writing a block the caller chose (`PutDataSubarrays`) and declaring an array
//! (`PutUninitializedDataArrays`), over an in-process session.
//!
//! The server is the **second source**. It keeps the arrays it was declared
//! and the blocks it was sent, walks each block into place with its own row
//! arithmetic (not the client's `carve`), and logs every request. A cell
//! nobody wrote stays `NaN`, so a block put in the wrong place cannot pass for
//! one put in the right place: the tests read what the *server* holds.
//!
//! ETP 1.2 behaviour tested: §13.2.1.2 step 1 (declare), step 3.b (blocks),
//! §23.32.7 (`starts` and `counts` per dimension), §3.7.3 (an answer per
//! key), §13.2.2.1 (the transport a logical type allows).
//!
//! # How to make each test fail
//!
//! - `a_block_lands_where_its_start_says`: in `write_subarrays`
//!   (`arrays/asynchronous/blocks.rs`) drop the `+= *offset as i64` shift: the
//!   block then lands at the top of the array.
//! - `a_block_too_big_for_a_message_...`: the same drop (the cut pieces land
//!   at the top), or plan with `u64::MAX` as the budget (one request carries
//!   the lot and the byte count per message fails).
//! - `small_blocks_ride_together_...`: key every entry `"1"` (the map keeps one),
//!   or send one request per block (`requests.len()` is no longer 1).
//! - `a_block_that_cannot_be_sent_...`: skip `check_subarray` in
//!   `write_subarrays`.
//! - `declarations_answer_one_each`: skip `check_declaration`, or answer
//!   every key with the first one's outcome.
//! - `the_async_twin_...`: make the blocking function do something else than
//!   run the asynchronous one.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use retp_avro::Role;
use retp_client::arrays::{
    Array, ArrayFailure, ArrayProgress, ArrayValues, Declaration, SubarrayWrite, declare_arrays,
    write_subarrays,
};
use retp_client::memory::{ManualClock, MemoryTransport};
use retp_client::{Client, Settings, asynchronous, blocking, writing_client};
use retp_proto::capabilities::{Value as CapabilityValue, names};
use retp_proto::connection::{Connection, Declined, Handled, Handler, Responder};
use retp_proto::negotiate::{Offer, ProtocolOffer};
use retp_proto::wire::{Body, Delivery, EtpError};
use retp_types::datatypes::data_array_types::DataArrayIdentifier;
use retp_types::datatypes::{AnyArrayItem, AnyArrayType, AnyLogicalArrayType, SupportedDataObject, Uuid};
use retp_types::dispatch::Message;
use retp_types::protocol::data_array::{PutDataSubarraysResponse, PutUninitializedDataArraysResponse};

const S: i64 = 1_000_000;
const T0: i64 = 1_760_000_000 * S;
const OBJECT: &str =
    "eml:///dataspace('demo')/resqml20.obj_PointSetRepresentation(bb000000-0000-4000-8000-000000000002)";

/// A declared array: path, dimensions, transport and logical type.
type Declared = (String, Vec<i64>, AnyArrayType, AnyLogicalArrayType);
/// One request's entries: path, starts, counts.
type Entries = Vec<(String, Vec<i64>, Vec<i64>)>;

/// What the server was given.
#[derive(Default)]
struct Log {
    declared: Vec<Declared>,
    /// How many keys each `PutUninitializedDataArrays` carried.
    declaration_messages: Vec<usize>,
    /// Each array by path: dimensions and doubles, `NaN` where nobody wrote.
    held: BTreeMap<String, (Vec<i64>, Vec<f64>)>,
    /// Each `PutDataSubarrays`, its entries.
    requests: Vec<Entries>,
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
            Message::PutUninitializedDataArrays(request) => {
                log.declaration_messages.push(request.data_arrays.len());
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_arrays.keys().cloned());
                for (key, put) in &request.data_arrays {
                    let path = put.uid.path_in_resource.clone();
                    if path.contains("refused") {
                        reply
                            .failed(
                                key.clone(),
                                &EtpError::EREQUEST_DENIED.with_detail("not this one"),
                            )
                            .unwrap();
                        continue;
                    }
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
                log.requests.push(
                    request
                        .data_subarrays
                        .values()
                        .map(|put| {
                            (
                                put.uid.path_in_resource.clone(),
                                put.starts.clone(),
                                put.counts.clone(),
                            )
                        })
                        .collect(),
                );
                let mut reply = responder
                    .wire()
                    .begin_response(delivery)
                    .for_keys(request.data_subarrays.keys().cloned());
                for (key, put) in &request.data_subarrays {
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

type Session = Client<MemoryTransport, ManualClock>;

/// A writing session with the server, whose messages carry `payload` bytes at
/// most when it says so.
fn session(payload: Option<i64>) -> (Session, Arc<Mutex<Log>>) {
    let log = Arc::new(Mutex::new(Log::default()));
    let mut offer = Offer::new("a receiving store", "0.1", Uuid([3; 16]));
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
    (client, log)
}

fn id(path: &str) -> DataArrayIdentifier {
    DataArrayIdentifier {
        uri: OBJECT.to_string(),
        path_in_resource: path.to_string(),
    }
}

fn double_declaration(path: &str, dimensions: Vec<u64>) -> Declaration {
    Declaration {
        id: id(path),
        dimensions,
        logical: AnyLogicalArrayType::ArrayOfDouble64Le,
        transport: AnyArrayType::ArrayOfDouble,
        preferred: Vec::new(),
    }
}

/// A block of doubles `first, first + 1, ...`, row-major, `rows` by `cols`.
fn doubles(rows: u64, cols: u64, first: f64) -> Array {
    Array {
        dimensions: vec![rows, cols],
        logical: AnyLogicalArrayType::ArrayOfDouble64Le,
        transport: AnyArrayType::ArrayOfDouble,
        values: ArrayValues::Doubles((0..rows * cols).map(|i| first + i as f64).collect()),
        blocks: 0,
    }
}

fn write(path: &str, starts: Vec<u64>, block: Array, declared: Option<Vec<u64>>) -> SubarrayWrite {
    SubarrayWrite {
        id: id(path),
        starts,
        block,
        declared,
    }
}

/// `NaN` is « nobody wrote it »: `None`.
fn cells(values: &[f64]) -> Vec<Option<f64>> {
    values.iter().map(|v| (!v.is_nan()).then_some(*v)).collect()
}

/// **A block lands where its `start` says, in an array declared before it.**
/// The server holds rows 2 and 3 of six; the other cells are untouched.
#[test]
fn a_block_lands_where_its_start_says() {
    let (mut client, log) = session(None);
    let declared = declare_arrays(
        &mut client,
        &[double_declaration("/a", vec![6, 3])],
        16 * 1024 * 1024,
    )
    .unwrap();
    assert!(declared[0].is_ok(), "{declared:?}");
    let written = write_subarrays(
        &mut client,
        &[write("/a", vec![2, 0], doubles(2, 3, 1.0), None)],
        16 * 1024 * 1024,
        |_| {},
    )
    .unwrap();
    assert!(written[0].is_ok(), "{written:?}");

    let log = log.lock().unwrap();
    // What was declared: the shape and both types, §13.2.2.1's pair.
    assert_eq!(
        log.declared,
        vec![(
            "/a".to_string(),
            vec![6, 3],
            AnyArrayType::ArrayOfDouble,
            AnyLogicalArrayType::ArrayOfDouble64Le
        )]
    );
    let mut expected = vec![None; 18];
    for (i, cell) in expected.iter_mut().enumerate().skip(6).take(6) {
        *cell = Some((i - 5) as f64);
    }
    assert_eq!(cells(&log.held["/a"].1), expected);
    // The server was told the absolute place.
    assert_eq!(
        log.requests,
        vec![vec![("/a".to_string(), vec![2, 0], vec![2, 3])]]
    );
}

/// **A block too big for one message is cut, and every piece lands where it
/// belongs**: the pieces' starts are in the array, not in the block, no piece
/// exceeds the message, and the array ends up as if the block had gone whole.
#[test]
fn a_block_too_big_for_a_message_is_cut_and_every_piece_lands_where_it_belongs() {
    // 20 000 bytes a message: the budget is 20 000 - (20 000 / 16 + 1024).
    let message_budget: i64 = 20_000 - (20_000 / 16 + 1024);
    let (mut client, log) = session(Some(20_000));
    declare_arrays(
        &mut client,
        &[double_declaration("/big", vec![200, 100])],
        16 * 1024 * 1024,
    )
    .unwrap()
    .remove(0)
    .unwrap();

    let mut written_blocks = 0;
    let outcome = write_subarrays(
        &mut client,
        &[write(
            "/big",
            vec![10, 0],
            doubles(60, 100, 1.0),
            Some(vec![200, 100]),
        )],
        16 * 1024 * 1024,
        |event| {
            if let ArrayProgress::Written { .. } = event {
                written_blocks += 1;
            }
        },
    )
    .unwrap();
    assert!(outcome[0].is_ok(), "{outcome:?}");

    let log = log.lock().unwrap();
    let entries: usize = log.requests.iter().map(Vec::len).sum();
    assert!(
        entries >= 3,
        "60 x 100 doubles is 48 000 bytes, a message takes {message_budget}: {entries} piece(s)"
    );
    assert_eq!(written_blocks, entries, "one `Written` per piece");
    for request in &log.requests {
        let bytes: i64 = request
            .iter()
            .map(|(_, _, counts)| counts.iter().product::<i64>() * 8)
            .sum();
        assert!(
            bytes <= message_budget,
            "{bytes} bytes in one message: {request:?}"
        );
        for (_, starts, counts) in request {
            assert!(
                starts[0] >= 10 && starts[0] + counts[0] <= 70,
                "{starts:?} {counts:?}"
            );
            assert_eq!(starts[1], 0);
        }
    }
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

/// **Small blocks ride in one message, and a refusal marks only its own
/// item** (§3.7.3): the middle block is for an array nobody declared.
#[test]
fn small_blocks_ride_together_and_a_refusal_marks_only_its_item() {
    let (mut client, log) = session(None);
    declare_arrays(
        &mut client,
        &[double_declaration("/a", vec![6, 3])],
        16 * 1024 * 1024,
    )
    .unwrap()
    .remove(0)
    .unwrap();
    let written = write_subarrays(
        &mut client,
        &[
            write("/a", vec![2, 0], doubles(2, 3, 1.0), None),
            write("/undeclared", vec![0, 0], doubles(1, 3, 100.0), None),
            write("/a", vec![0, 0], doubles(1, 3, 50.0), None),
        ],
        16 * 1024 * 1024,
        |_| {},
    )
    .unwrap();
    assert_eq!(written.len(), 3);
    assert!(written[0].is_ok(), "{:?}", written[0]);
    assert!(
        matches!(&written[1], Err(ArrayFailure::Store(e)) if e.code == EtpError::ENOT_FOUND.code()),
        "{:?}",
        written[1]
    );
    assert!(written[2].is_ok(), "{:?}", written[2]);

    let log = log.lock().unwrap();
    assert_eq!(log.requests.len(), 1, "one message for the three");
    assert_eq!(log.requests[0].len(), 3);
    let held = cells(&log.held["/a"].1);
    assert_eq!(&held[0..3], &[Some(50.0), Some(51.0), Some(52.0)]);
    assert_eq!(&held[3..6], &[None, None, None]);
    assert_eq!(
        &held[6..12],
        &[Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0), Some(6.0)]
    );
}

/// **An item that cannot be sent fails alone, before anything leaves for it**:
/// a block outside the declared array, a block whose values do not make its
/// shape; the third goes through, and the server saw one block.
#[test]
fn a_block_that_cannot_be_sent_fails_alone_before_anything_leaves() {
    let (mut client, log) = session(None);
    declare_arrays(
        &mut client,
        &[double_declaration("/a", vec![6, 3])],
        16 * 1024 * 1024,
    )
    .unwrap()
    .remove(0)
    .unwrap();
    let mut short = doubles(2, 3, 0.0);
    short.values = ArrayValues::Doubles(vec![1.0; 5]);
    let written = write_subarrays(
        &mut client,
        &[
            write("/a", vec![5, 0], doubles(2, 3, 0.0), Some(vec![6, 3])),
            write("/a", vec![0, 0], short, None),
            write("/a", vec![0, 0], doubles(1, 3, 7.0), Some(vec![6, 3])),
        ],
        16 * 1024 * 1024,
        |_| {},
    )
    .unwrap();
    assert!(
        matches!(&written[0], Err(ArrayFailure::Plan(why)) if why.contains("not inside")),
        "{:?}",
        written[0]
    );
    assert!(
        matches!(&written[1], Err(ArrayFailure::Inconsistent(_))),
        "{:?}",
        written[1]
    );
    assert!(written[2].is_ok(), "{:?}", written[2]);

    let log = log.lock().unwrap();
    assert_eq!(log.requests.len(), 1);
    assert_eq!(log.requests[0].len(), 1, "only the honest block left");
}

/// **A declaration answers one outcome per item**: the store refuses one by
/// name, the client refuses one whose transport §13.2.2.1 does not allow
/// before it leaves, and the third is declared - in one message of two.
#[test]
fn declarations_answer_one_each() {
    let (mut client, log) = session(None);
    let mut bad = double_declaration("/bad", vec![2]);
    bad.transport = AnyArrayType::ArrayOfInt;
    let declared = declare_arrays(
        &mut client,
        &[
            double_declaration("/ok", vec![4, 3]),
            double_declaration("/refused", vec![2]),
            bad,
        ],
        16 * 1024 * 1024,
    )
    .unwrap();
    assert_eq!(declared.len(), 3);
    assert!(declared[0].is_ok(), "{:?}", declared[0]);
    assert!(
        matches!(&declared[1], Err(ArrayFailure::Store(e)) if e.code == EtpError::EREQUEST_DENIED.code()),
        "{:?}",
        declared[1]
    );
    assert!(
        matches!(&declared[2], Err(ArrayFailure::Inconsistent(why)) if why.contains("13.2.2.1")),
        "{:?}",
        declared[2]
    );
    let log = log.lock().unwrap();
    assert_eq!(log.declaration_messages, vec![2], "the bad one never left");
    assert_eq!(log.declared.len(), 1);
    assert_eq!(log.declared[0].0, "/ok");
}

/// **The asynchronous functions do what the blocking ones do** - they are the
/// same code: the blocking client answers inside `call`, so `blocking` runs
/// them at once.
#[test]
fn the_async_twin_does_what_the_blocking_function_does() {
    let (mut client, log) = session(None);
    let declared = blocking(asynchronous::declare_arrays(
        &mut client,
        &[double_declaration("/a", vec![2, 3])],
        16 * 1024 * 1024,
    ))
    .unwrap();
    assert!(declared[0].is_ok(), "{declared:?}");
    let written = blocking(asynchronous::write_subarrays(
        &mut client,
        &[write("/a", vec![1, 0], doubles(1, 3, 4.0), None)],
        16 * 1024 * 1024,
        |_| {},
    ))
    .unwrap();
    assert!(written[0].is_ok(), "{written:?}");
    let held = cells(&log.lock().unwrap().held["/a"].1);
    assert_eq!(held, vec![None, None, None, Some(4.0), Some(5.0), Some(6.0)]);
}
