//! Tier 4 - the bridge, against a store that is an ETP server.
//!
//! The server is a real server-side [`Connection`] with handlers serving a
//! small RESQML 2.0.1 model - a CRS, a point set, the HDF5 proxy its array
//! goes through, and the array itself - reached through the in-memory
//! transport. What is tested is the bridge, not the wire: every question goes
//! through [`EnergymlStorage`], and the last test asks it the way the 3D view
//! does, through code that was written for files.

mod common;

use energyml_domain::source::{ObjectSource, Prefetched};
use energyml_storage::array::ArrayValues;
use energyml_storage::error::Error;
use energyml_storage::storage::EnergymlStorage;

use common::*;

/// A dataspace answers what a package answers: what it holds, their titles -
/// from the listing, no object read -, and the objects themselves, each
/// with its own answer.
#[test]
fn a_dataspace_is_read_through_the_trait() {
    let store = store();
    assert_eq!(store.store_kind(), "etp");
    let dataspaces = block(store.dataspaces()).unwrap();
    assert_eq!(
        (dataspaces[0].uri.as_str(), dataspaces[0].name.as_str()),
        (DATASPACE, "demo")
    );

    let resources = block(store.resources()).unwrap();
    let types: Vec<&str> = resources.iter().map(|r| r.qualified_type.as_str()).collect();
    assert_eq!(
        types,
        [
            "resqml20.obj_LocalDepth3dCrs",
            "resqml20.obj_PointSetRepresentation",
            "eml20.obj_EpcExternalPartReference"
        ]
    );
    assert_eq!(resources[1].uuid, POINTS);
    assert!(
        resources[1].content_type.contains("obj_PointSetRepresentation"),
        "{}",
        resources[1].content_type
    );

    let headers = block(store.headers(&[uri("resqml20.obj_PointSetRepresentation", POINTS)]));
    assert_eq!(headers[0].as_ref().unwrap().title.as_deref(), Some("a point set"));

    let asked = vec![
        uri("resqml20.obj_LocalDepth3dCrs", CRS),
        uri(
            "resqml20.obj_LocalDepth3dCrs",
            "dd000000-0000-4000-8000-000000000004",
        ),
        "eml:///resqml20.obj_X(uuid=aa000000-0000-4000-8000-000000000001)".to_string(),
    ];
    let objects = block(store.get_objects(&asked));
    assert_eq!(
        objects[0].as_ref().unwrap().as_reflect().type_info().object_type,
        "obj_LocalDepth3dCrs"
    );
    assert!(
        matches!(&objects[1], Err(Error::NotFound(_))),
        "{:?}",
        objects[1].as_ref().err()
    );
    assert!(
        matches!(&objects[2], Err(Error::BadUri { .. })),
        "{:?}",
        objects[2].as_ref().err()
    );
    assert!(store.diagnostics().is_empty(), "{:?}", store.diagnostics());
}

/// The arrays of an object are found **on the object** - `locate_with`, the
/// same reflection an EPC uses - its proxy resolved to the proxy's URI in the
/// dataspace, and read by DataArray: the metadata, then the values.
#[test]
fn arrays_are_found_on_the_object_and_read_by_the_protocol() {
    let store = store();
    let points = block(store.get_object(&uri("resqml20.obj_PointSetRepresentation", POINTS))).unwrap();
    let located = store.arrays_of(points.as_reflect());
    assert_eq!(located.len(), 1);
    assert_eq!(
        located[0].file,
        uri("eml20.obj_EpcExternalPartReference", PROXY),
        "the proxy, as the dataspace names it"
    );
    assert_eq!(located[0].path_in_file, "/points");

    let metadata = block(store.array_metadata(&located[0])).unwrap();
    assert_eq!(
        (metadata.dimensions, metadata.element_type.as_str()),
        (vec![2, 3], "double")
    );
    let data = block(store.read_array(&located[0])).unwrap();
    assert_eq!(
        data.values,
        ArrayValues::F64(vec![0.0, 0.0, 0.0, 10.0, 20.0, 30.0])
    );
    assert_eq!(data.metadata.dimensions, vec![2, 3]);
}

/// eml 2.3 names a **file** (`URI`) where eml 2.0 names a proxy; over ETP an
/// array is addressed by the object that declares it, the file name meaning
/// nothing to a server. The slice - `StartIndex`, `Count` - comes along.
#[test]
fn an_eml_2_3_array_is_addressed_by_its_object() {
    let store = store();
    let sub = energyml_resqml_2_2::SubRepresentation {
        uuid: "66666666-6666-6666-6666-666666666666".to_string(),
        sub_representation_patch: vec![energyml_resqml_2_2::SubRepresentationPatch {
            indices: Some(energyml_eml_2_3::AbstractIntegerArray::IntegerExternalArray(
                energyml_eml_2_3::IntegerExternalArray {
                    values: energyml_eml_2_3::ExternalDataArray {
                        external_data_array_part: vec![energyml_eml_2_3::ExternalDataArrayPart {
                            path_in_external_file: "/sub/0".to_string(),
                            uri: "model.h5".to_string(),
                            start_index: vec![5],
                            count: vec![3],
                            ..Default::default()
                        }],
                    },
                    ..Default::default()
                },
            )),
            ..Default::default()
        }],
        ..Default::default()
    };
    let located = store.arrays_of(&sub);
    assert_eq!(located.len(), 1);
    assert_eq!(
        located[0].file,
        format!("{DATASPACE}/resqml22.SubRepresentation(66666666-6666-6666-6666-666666666666)")
    );
    assert_eq!(
        (
            located[0].path_in_file.as_str(),
            located[0].start.clone(),
            located[0].count.clone()
        ),
        ("/sub/0", vec![5], vec![3])
    );
}

/// **The acceptance criterion**: code written for files
/// reads the server unchanged. The prefetch the 3D view uses follows the
/// point set to its CRS, locates and fetches its array, and the geometry
/// reader - which has never heard of ETP - draws the two points.
#[test]
fn what_reads_a_file_reads_the_server() {
    let store = store();
    let roots = vec![uri("resqml20.obj_PointSetRepresentation", POINTS)];
    let mut cache = block(Prefetched::fetch(&store, &roots, 1)).expect("a prefetch through the bridge");
    let wanted = cache.locations();
    assert_eq!(block(cache.fetch_arrays(&store, &wanted)), 1);

    let representation = cache.object(POINTS).expect("prefetched");
    let geometry = energyml_geometry::geometry::of(representation.as_reflect(), &cache).expect("drawn");
    assert_eq!(geometry.point_count(), 2);
    let [a, b] = [geometry.patches[0].points[0], geometry.patches[0].points[1]];
    let step = [b[0] - a[0], b[1] - a[1], b[2] - a[2]];
    assert!(
        step.iter()
            .zip([10.0, 20.0, 30.0])
            .all(|(s, e)| (s - e).abs() < 1e-9),
        "{a:?} → {b:?}"
    );
}

/// A server that writes class names in its URIs (an RDDMS server) is still read: the
/// object comes back as its root element says, and the store says so.
#[test]
fn a_misspelled_type_is_read_as_its_root_says_and_said() {
    let store = store_with(&[3, 4, 9, 24], Spelling::ClassName);
    let crs = object_uri("resqml20.obj_LocalDepth3dCrs", CRS, Spelling::ClassName);
    assert!(crs.contains("resqml20.ObjLocalDepth3dCrs"), "{crs}");
    let object = block(store.get_object(&crs)).unwrap();
    assert_eq!(object.as_reflect().type_info().object_type, "obj_LocalDepth3dCrs");
    let said = store.diagnostics();
    assert!(
        said.iter()
            .any(|d| d.message.contains("ObjLocalDepth3dCrs") && d.message.contains("root")),
        "{said:?}"
    );
}

/// A server that does not serve a protocol answers with a refusal, never an
/// empty list: no Dataspace here, so no dataspaces - said.
#[test]
fn a_failure_is_said_never_an_empty_answer() {
    let store = store_with(&[3, 4], Spelling::Energistics);
    match block(store.dataspaces()) {
        Err(Error::Remote { store, reason }) => {
            assert_eq!(store, "etp");
            assert!(
                reason.contains("24") || reason.to_lowercase().contains("dataspace"),
                "{reason}"
            );
        }
        other => panic!("{other:?}"),
    }
    let points = block(store.get_object(&uri("resqml20.obj_PointSetRepresentation", POINTS))).unwrap();
    let located = store.arrays_of(points.as_reflect());
    assert!(
        matches!(block(store.read_array(&located[0])), Err(Error::Remote { .. })),
        "no DataArray: said, not zeros"
    );
}

/// A dataspace packed is a package. The EPC that comes out is read back by
/// the **EPC reader**, which has never heard of ETP: the same three objects,
/// the point set drawn from the array that came beside the archive, and the
/// file named after the proxy it belongs to. Two readers, one fact.
#[test]
fn a_dataspace_packed_reads_back_as_an_epc() {
    use energyml_etp_store::pack::{Wanted, pack};

    let mut client = store().into_client();
    let wanted = Wanted {
        arrays: true,
        ..Default::default()
    };
    let packed = block(pack(&mut client, "demo", &registry(), &wanted)).unwrap();
    assert!(packed.left_out.is_empty(), "{:?}", packed.left_out);
    assert_eq!(packed.objects, 3);

    let read = energyml_storage::EpcStore::from_reader(
        Box::new(std::io::Cursor::new(packed.epc.clone())),
        "packed.epc",
        registry(),
    )
    .unwrap();
    let mut held: Vec<String> = read
        .resources()
        .iter()
        .filter_map(|summary| read.header(&summary.uri).ok().and_then(|header| header.title))
        .collect();
    held.sort();
    assert_eq!(held, ["a local depth CRS", "a point set", "the HDF5 proxy"]);

    // The array, named for the file the proxy stands for - the package says
    // that name in the proxy's `.rels`, and the caller writes it there.
    assert_eq!(packed.arrays.len(), 1);
    let array = &packed.arrays[0];
    assert_eq!(
        (array.path.as_str(), array.file.as_str()),
        ("/points", &*format!("{PROXY}.h5"))
    );
    assert_eq!(array.dimensions, [2, 3]);
    assert_eq!(
        array.values,
        ArrayValues::F64(vec![0.0, 0.0, 0.0, 10.0, 20.0, 30.0])
    );
    let proxy = read
        .resources()
        .iter()
        .find(|r| r.uuid.eq_ignore_ascii_case(PROXY))
        .expect("the proxy")
        .clone();
    let rels = String::from_utf8(
        read.raw_part(&format!("_rels/{}.rels", proxy.locator))
            .unwrap_or_default(),
    )
    .unwrap_or_default();
    assert!(
        rels.contains(&format!("{PROXY}.h5")),
        "the package names the file in {}: {rels}",
        proxy.locator
    );
}

/// Only what was asked for, and what was asked for and is not there is said
/// rather than quietly left out.
#[test]
fn a_packed_dataspace_takes_what_was_asked_for() {
    use energyml_etp_store::pack::{Wanted, pack};

    let mut client = store().into_client();
    let lost = uri(
        "resqml20.obj_LocalDepth3dCrs",
        "00000000-0000-0000-0000-000000000000",
    );
    let wanted = Wanted {
        uris: vec![uri("resqml20.obj_LocalDepth3dCrs", CRS), lost.clone()],
        ..Default::default()
    };
    let packed = block(pack(&mut client, "demo", &registry(), &wanted)).unwrap();
    assert_eq!(packed.objects, 1);
    assert!(packed.arrays.is_empty(), "the arrays were not asked for");
    assert!(
        packed.left_out.iter().any(|said| said.contains(&lost)),
        "{:?}",
        packed.left_out
    );

    // A family, `*` and all: the two resqml objects, and not the eml proxy.
    let family = block(pack(
        &mut client,
        "demo",
        &registry(),
        &Wanted {
            types: vec!["resqml20.*".into()],
            ..Default::default()
        },
    ))
    .unwrap();
    assert_eq!(family.objects, 2);
}

/// A package closes on what it names: asked for the point set alone, it comes
/// with the CRS the point set references, and with the proxy its array hangs
/// from - a package whose representation has no CRS draws nothing.
#[test]
fn a_packed_object_brings_what_it_references() {
    use energyml_etp_store::pack::{Wanted, pack};

    let mut client = store().into_client();
    let wanted = Wanted {
        uris: vec![uri("resqml20.obj_PointSetRepresentation", POINTS)],
        references: true,
        arrays: true,
        ..Default::default()
    };
    let packed = block(pack(&mut client, "demo", &registry(), &wanted)).unwrap();
    assert!(packed.left_out.is_empty(), "{:?}", packed.left_out);
    assert_eq!(
        packed.objects, 3,
        "the point set, its CRS, and the proxy of its array"
    );
    assert_eq!(packed.arrays.len(), 1);

    // Asked for the same one alone, the package dangles - and the checker of
    // `energyml-storage`, not this crate, is the one that says so.
    let plain = block(pack(
        &mut client,
        "demo",
        &registry(),
        &Wanted {
            uris: vec![uri("resqml20.obj_PointSetRepresentation", POINTS)],
            arrays: true,
            ..Default::default()
        },
    ))
    .unwrap();
    assert_eq!(
        plain.objects, 2,
        "the point set and the proxy its array hangs from"
    );
    assert_eq!(plain.arrays.len(), 1, "and its array all the same");
    assert_eq!(
        plain.arrays[0].file,
        format!("{PROXY}.h5"),
        "in the file that proxy stands for"
    );
}
