//! The shape of a generated ETP type, written by hand - and held to it.
//!
//! This is `energyml-core/tests/generated_shape.rs` for the ETP side: the
//! **specification of the generator's output**. What `energyml-codegen --etp`
//! emits is a struct and a line of attributes, and the derives of
//! `retp-derive` do the rest (ADR-0004). The types below are spelled
//! the way the generator spells them, so a test of tier 1 can build a
//! message the generator has never seen.
//!
//! Where the expected bytes come from matters, and it is never this crate:
//! every `[0x…]` here is worked out from the Avro 1.11 specification - zig-zag
//! varints, length-prefixed strings, blocks ending in a zero count, a union as
//! its branch index. A codec that agreed with itself and not with Avro would
//! pass a round trip and fail here.

use std::collections::BTreeMap;

use retp_avro::{Avro, Error, EtpMessage, Role, Senders};

// ---------------------------------------------------------------------------
// A hand-written slice, in the generator's spelling
// ---------------------------------------------------------------------------

/// `Test.Uuid` - 4 bytes, with no length on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Avro)]
#[avro(name = "Test.Uuid")]
pub struct Uuid(pub [u8; 4]);

/// `Test.Kind`. The variant order **is** the encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Avro)]
#[avro(name = "Test.Kind")]
pub enum Kind {
    /// `first`.
    #[avro(symbol = "first")]
    First,
    /// `Second`.
    Second,
    /// `self`.
    #[avro(symbol = "self")]
    Self_,
}

/// `Test.Level`, whose schema declares a default that is not the first symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Avro)]
#[avro(name = "Test.Level")]
pub enum Level {
    /// `Low`.
    Low,
    /// `High`.
    #[avro(default)]
    High,
}

/// The union at `Test.Holder.value`.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(union, name = "Test.Holder.value")]
pub enum HolderValue {
    /// The `null` branch.
    Null,
    /// The `int` branch.
    Int(i32),
    /// The `string` branch.
    String(String),
    /// The `kind` branch.
    Kind(Kind),
}

/// `Test.Everything` - one field of every shape the derive reads.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Test.Everything")]
pub struct Everything {
    /// `flag`.
    pub flag: bool,
    /// `count`.
    pub count: i32,
    /// `total`.
    pub total: i64,
    /// `ratio`.
    pub ratio: f32,
    /// `value`.
    pub value: f64,
    /// `blob`.
    pub blob: Vec<u8>,
    /// `text`.
    pub text: String,
    /// `samples` - `array<double>`, the block path (ADR-0004).
    pub samples: Vec<f64>,
    /// `indices`.
    pub indices: Vec<i64>,
    /// `named`.
    pub named: BTreeMap<String, i32>,
    /// `maybe`.
    pub maybe: Option<i32>,
    /// `holes` - optionals inside an array.
    pub holes: Vec<Option<bool>>,
    /// `kind`.
    pub kind: Kind,
    /// `holder`.
    pub holder: HolderValue,
    /// `id`.
    pub id: Uuid,
}

/// `Test.Defaults` - every spelling a schema default takes.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Test.Defaults")]
pub struct Defaults {
    /// `format`.
    #[avro(default = "xml")]
    pub format: String,
    /// `empty`.
    #[avro(default = "")]
    pub empty: String,
    /// `formats`.
    #[avro(default = ["xml", "json"])]
    pub formats: Vec<String>,
    /// `none`.
    #[avro(default = [])]
    pub none: Vec<i64>,
    /// `entries`.
    #[avro(default = {})]
    pub entries: BTreeMap<String, i32>,
    /// `absent`.
    #[avro(default = null)]
    pub absent: Option<i32>,
    /// `present`.
    #[avro(default = 7)]
    pub present: Option<i32>,
    /// `enabled`.
    #[avro(default = true)]
    pub enabled: bool,
    /// `limit`.
    #[avro(default = -3)]
    pub limit: i64,
    /// `scale`.
    #[avro(default = 2.5)]
    pub scale: f64,
    /// `whole` - an integer written for a double, as JSON allows.
    #[avro(default = 1)]
    pub whole: f32,
    /// `raw` - a `bytes` default is a string of code points 0 to 255.
    #[avro(default = "\u{1}\u{ff}")]
    pub raw: Vec<u8>,
    /// `kind`.
    #[avro(default = Second)]
    pub kind: Kind,
    /// `untouched` - no default in the schema: the Avro zero.
    pub untouched: i32,
}

/// `Test.Renamed` - an Avro name that is not the camelCase of the Rust one.
#[derive(Debug, Clone, PartialEq, Avro)]
#[avro(name = "Test.Renamed")]
pub struct Renamed {
    /// `XMLin`.
    #[avro(rename = "XMLin")]
    pub xm_lin: Option<i32>,
    /// `dataObjects`.
    pub data_objects: Option<i32>,
    /// `final` - a keyword, kept as a raw identifier.
    pub r#final: bool,
}

/// `Test.Protocol.Get` - a message.
///
/// ETP protocol 4, message 1, sent by the customer.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Test.Protocol.Get")]
#[etp(protocol = 4, message_type = 1, sender = "customer", roles = "store,customer")]
pub struct Get {
    /// `uris`.
    pub uris: BTreeMap<String, String>,
}

/// `Test.Protocol.GetResponse` - may be split across several messages.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Test.Protocol.GetResponse")]
#[etp(
    protocol = 4,
    message_type = 4,
    sender = "store",
    roles = "store,customer",
    multipart
)]
pub struct GetResponse {}

/// `Test.Protocol.Universal` - sent by any role, like `ProtocolException`.
#[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
#[avro(name = "Test.Protocol.Universal")]
#[etp(protocol = 0, message_type = 1000, sender = "*", roles = "client,server")]
pub struct Universal {}

// ---------------------------------------------------------------------------
// The codec, against the Avro specification
// ---------------------------------------------------------------------------

fn populated() -> Everything {
    Everything {
        flag: true,
        count: -1,
        total: 64,
        ratio: 2.5,
        value: 1.0,
        blob: vec![1, 2],
        text: "ab".to_string(),
        samples: vec![1.0],
        indices: vec![1, -1],
        named: BTreeMap::from([("k".to_string(), 1)]),
        maybe: Some(1),
        holes: vec![None, Some(true)],
        kind: Kind::Self_,
        holder: HolderValue::String("z".to_string()),
        id: Uuid([9, 8, 7, 6]),
    }
}

#[test]
fn every_shape_writes_what_avro_says() {
    let expected: Vec<u8> = [
        &[0x01][..],                                 // flag: boolean true
        &[0x01],                                     // count: int -1, zig-zag 1
        &[0x80, 0x01],                               // total: long 64, zig-zag 128
        &[0x00, 0x00, 0x20, 0x40],                   // ratio: float 2.5, little-endian
        &[0, 0, 0, 0, 0, 0, 0xF0, 0x3F],             // value: double 1.0
        &[0x04, 0x01, 0x02],                         // blob: length 2, then the bytes
        &[0x04, b'a', b'b'],                         // text: length 2, then UTF-8
        &[0x02, 0, 0, 0, 0, 0, 0, 0xF0, 0x3F, 0x00], // samples: one block of 1, then 0
        &[0x04, 0x02, 0x01, 0x00],                   // indices: block of 2: 1, -1
        &[0x02, 0x02, b'k', 0x02, 0x00],             // named: block of 1: "k" -> 1
        &[0x02, 0x02],                               // maybe: branch 1, then 1
        &[0x04, 0x00, 0x02, 0x01, 0x00],             // holes: [null, branch 1 true]
        &[0x04],                                     // kind: ordinal 2
        &[0x04, 0x02, b'z'],                         // holder: branch 2, string "z"
        &[9, 8, 7, 6],                               // id: fixed, no length
    ]
    .concat();
    assert_eq!(populated().to_avro(), expected);
    assert_eq!(Everything::from_avro(&expected).unwrap(), populated());
}

#[test]
fn an_empty_message_is_no_bytes_at_all() {
    assert!(GetResponse {}.to_avro().is_empty());
    assert_eq!(GetResponse::from_avro(&[]).unwrap(), GetResponse {});
}

#[test]
fn an_enumeration_speaks_its_symbols() {
    assert_eq!(Kind::SYMBOLS, [Kind::First, Kind::Second, Kind::Self_]);
    assert_eq!(Kind::First.as_str(), "first");
    assert_eq!(Kind::Second.as_str(), "Second");
    assert_eq!(Kind::Self_.as_str(), "self");
    assert_eq!(
        Kind::default(),
        Kind::First,
        "no declared default: the first symbol"
    );
    assert_eq!(Level::default(), Level::High, "the declared default wins");
    assert_eq!(Kind::from_avro(&[0x02]).unwrap(), Kind::Second);
}

#[test]
fn a_union_defaults_to_its_first_branch() {
    assert_eq!(HolderValue::default(), HolderValue::Null);
    assert_eq!(HolderValue::Null.to_avro(), vec![0x00]);
    assert_eq!(HolderValue::Kind(Kind::Second).to_avro(), vec![0x06, 0x02]);
}

// ---------------------------------------------------------------------------
// Refusals name the place
// ---------------------------------------------------------------------------

#[test]
fn a_bad_branch_names_the_union() {
    let error = HolderValue::from_avro(&[0x08]).unwrap_err();
    assert!(
        matches!(
            error,
            Error::NoSuchBranch {
                in_type: "Test.Holder.value",
                index: 4,
                branches: 4
            }
        ),
        "{error:?}"
    );
}

#[test]
fn a_bad_optional_names_the_field_by_its_avro_name() {
    // Branch 2 of a two-branch union, in each of the three fields.
    let error = Renamed::from_avro(&[0x04]).unwrap_err();
    assert!(
        matches!(
            error,
            Error::NoSuchBranch {
                in_type: "Test.Renamed.XMLin",
                ..
            }
        ),
        "{error:?}"
    );
    let error = Renamed::from_avro(&[0x00, 0x04]).unwrap_err();
    assert!(
        matches!(
            error,
            Error::NoSuchBranch {
                in_type: "Test.Renamed.dataObjects",
                ..
            }
        ),
        "the camelCase of `data_objects`: {error:?}"
    );
    let error = Kind::from_avro(&[0x06]).unwrap_err();
    assert!(
        matches!(
            error,
            Error::NoSuchSymbol {
                in_type: "Test.Kind",
                ..
            }
        ),
        "{error:?}"
    );
}

// ---------------------------------------------------------------------------
// Defaults say what the schema says
// ---------------------------------------------------------------------------

#[test]
fn every_default_spelling_lands_on_its_value() {
    let d = Defaults::default();
    assert_eq!(d.format, "xml");
    assert_eq!(d.empty, "");
    assert_eq!(d.formats, ["xml", "json"]);
    assert!(d.none.is_empty());
    assert!(d.entries.is_empty());
    assert_eq!(d.absent, None);
    assert_eq!(d.present, Some(7));
    assert!(d.enabled);
    assert_eq!(d.limit, -3);
    assert_eq!(d.scale, 2.5);
    assert_eq!(d.whole, 1.0);
    assert_eq!(d.raw, [1, 255]);
    assert_eq!(d.kind, Kind::Second);
    assert_eq!(d.untouched, 0);
}

// ---------------------------------------------------------------------------
// What a message knows about itself
// ---------------------------------------------------------------------------

#[test]
fn a_message_carries_its_header_and_roles() {
    assert_eq!((Get::PROTOCOL, Get::MESSAGE_TYPE), (4, 1));
    assert_eq!(Get::SENDER_ROLE, Senders::These(&[Role::Customer]));
    assert!(Get::PROTOCOL_ROLES.allows(Role::Store));
    // Read through a value: the constants are what is under test.
    let multipart = [Get::MULTIPART, GetResponse::MULTIPART];
    assert_eq!(multipart, [false, true], "`multipart` alone means true");
    assert_eq!(Universal::SENDER_ROLE, Senders::Any);
}

/// The generator's `GetDataObjects`, and the same thing by hand: one shape.
#[test]
fn a_hand_written_message_is_the_generated_one() {
    use retp_types::protocol::store::GetDataObjects as Generated;

    #[derive(Debug, Clone, PartialEq, Avro, EtpMessage)]
    #[avro(name = "Energistics.Etp.v12.Protocol.Store.GetDataObjects")]
    #[etp(protocol = 4, message_type = 1, sender = "customer", roles = "store,customer")]
    pub struct GetDataObjects {
        pub uris: BTreeMap<String, String>,
        #[avro(default = "xml")]
        pub format: String,
    }

    let uris = BTreeMap::from([("a".to_string(), "eml:///x".to_string())]);
    let hand = GetDataObjects {
        uris: uris.clone(),
        format: "json".into(),
    };
    let generated = Generated {
        uris,
        format: "json".into(),
    };
    assert_eq!(hand.to_avro(), generated.to_avro());
    assert_eq!(
        GetDataObjects::default().to_avro(),
        Generated::default().to_avro()
    );
    assert_eq!(GetDataObjects::FULL_NAME, Generated::FULL_NAME);
    assert_eq!(
        (
            GetDataObjects::PROTOCOL,
            GetDataObjects::MESSAGE_TYPE,
            GetDataObjects::MULTIPART
        ),
        (Generated::PROTOCOL, Generated::MESSAGE_TYPE, Generated::MULTIPART)
    );
    assert_eq!(GetDataObjects::SENDER_ROLE, Generated::SENDER_ROLE);
}
