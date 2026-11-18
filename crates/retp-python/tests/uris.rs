//! The canonical URI of a data object, and the words of a push, with no
//! server.
//!
//! The expected URIs are written as ETP v1.2 section 25.3.7 (p.489) spells a
//! canonical data-object URI, and a quote inside a quoted value is written
//! twice (section 25.3.10): they are typed here, not built by the function
//! under test.

use retp_python::session::{Kind, PushChoices, object_uri};

const UUID: &str = "421a7a05-033a-450d-bcef-051352023578";

#[test]
fn an_object_uri_is_the_canonical_form() {
    assert_eq!(
        object_uri("demo", "resqml20.obj_HorizonInterpretation", UUID, None).unwrap(),
        "eml:///dataspace('demo')/resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)"
    );
    // The default dataspace has no dataspace segment.
    assert_eq!(
        object_uri("", "eml20.obj_EpcExternalPartReference", UUID, None).unwrap(),
        "eml:///eml20.obj_EpcExternalPartReference(421a7a05-033a-450d-bcef-051352023578)"
    );
}

#[test]
fn a_quote_is_written_twice_and_a_uuid_lowercased() {
    assert_eq!(
        object_uri(
            "carl's/teapot",
            "resqml22.TriangulatedSetRepresentation",
            &UUID.to_ascii_uppercase(),
            Some("O'Neil"),
        )
        .unwrap(),
        "eml:///dataspace('carl''s/teapot')/resqml22.TriangulatedSetRepresentation(uuid=421a7a05-033a-450d-bcef-051352023578,version='O''Neil')"
    );
}

#[test]
fn a_dataspace_given_as_a_uri_is_the_same_dataspace() {
    assert_eq!(
        object_uri(
            "eml:///dataspace('osdu/project')",
            "resqml20.obj_HorizonInterpretation",
            UUID,
            None
        )
        .unwrap(),
        object_uri("osdu/project", "resqml20.obj_HorizonInterpretation", UUID, None).unwrap()
    );
}

#[test]
fn what_does_not_read_is_refused() {
    for (dataspace, kind, uuid) in [
        ("demo", "not a type", UUID),
        ("demo", "resqml20.obj_HorizonInterpretation", "not-a-uuid"),
        (
            "eml:///dataspace('demo')/resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)",
            "resqml20.obj_HorizonInterpretation",
            UUID,
        ),
    ] {
        let refusal = object_uri(dataspace, kind, uuid, None).expect_err(kind);
        assert_eq!(refusal.kind, Kind::BadArgument, "{}", refusal.message);
    }
}

#[test]
fn a_push_told_nothing_is_the_default_and_an_empty_budget_is_refused() {
    assert!(PushChoices::default().check().is_ok());
    for choices in [
        PushChoices {
            objects: Some(0),
            ..Default::default()
        },
        PushChoices {
            bytes: Some(0),
            ..Default::default()
        },
        PushChoices {
            only: Some(Vec::new()),
            ..Default::default()
        },
    ] {
        let refusal = choices.check().expect_err("nothing could go");
        assert_eq!(refusal.kind, Kind::BadArgument, "{}", refusal.message);
    }
}
