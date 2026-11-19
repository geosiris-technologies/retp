//! A downstream application that depends on `retp-client` alone can name the
//! types its signatures use. This file names them only through `retp_client`;
//! it fails to compile if a re-export is removed.

use retp_client::avro::EtpMessage;
use retp_client::proto::negotiate::Negotiated;
use retp_client::proto::wire::{Delivery, Exception};
use retp_client::types::datatypes::object::{ActiveStatusKind, Dataspace, Resource};
use retp_client::types::datatypes::{DataValue, ErrorInfo};

#[test]
fn the_types_of_the_signatures_are_nameable_from_the_client() {
    // The very type of the public signatures, not a lookalike.
    let value: DataValue = retp_client::custom_data_value("x");
    assert!(retp_client::custom_data_json(&value).is_some());

    fn takes<T>() {}
    takes::<(
        ErrorInfo,
        Resource,
        Dataspace,
        ActiveStatusKind,
        Negotiated,
        Delivery,
        Exception,
    )>();
    fn is_message<T: EtpMessage>() {}
    is_message::<retp_client::types::protocol::store::GetDataObjects>();
}
