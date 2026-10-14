//! The refusals of staging objects for a push, with no server.
//!
//! Only refusals are exercised here: they need no valid RESQML document, and a
//! hand-written object would be a fixture this suite could not vouch for (the
//! spec decides what a valid object is, not a string typed in a test).
//! TODO(verify): a positive test with an object built by the library itself
//! (`energyml-build` or a registry `create`), then `put` and `resources`.

use std::sync::Arc;

use retp_python::session::{Kind, stage};

fn registry() -> Arc<energyml_core::registry::Registry> {
    Arc::new(energyml_schemas::all())
}

#[test]
fn no_object_is_refused() {
    let refusal = stage(&registry(), &[]).err().expect("nothing to put");
    assert_eq!(refusal.kind, Kind::BadArgument);
}

#[test]
fn text_that_is_not_json_is_refused_and_names_the_object() {
    let refusal = stage(&registry(), &["{not json".to_string()])
        .err()
        .expect("not JSON");
    assert_eq!(refusal.kind, Kind::BadArgument);
    assert!(refusal.message.contains("object 0"), "{}", refusal.message);
}

#[test]
fn an_object_without_a_type_is_refused() {
    let refusal = stage(&registry(), &["{\"Citation\": {}}".to_string()])
        .err()
        .expect("no $type");
    assert_eq!(refusal.kind, Kind::BadArgument);
}

#[test]
fn an_unknown_type_is_refused() {
    let refusal = stage(
        &registry(),
        &["{\"$type\": \"resqml99.NoSuchThing\"}".to_string()],
    )
    .err()
    .expect("unknown type");
    assert_eq!(refusal.kind, Kind::BadArgument);
}
