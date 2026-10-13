//! What the writing checks send: a small package of several releases, with
//! its references closed and two arrays.
//!
//! The objects are the shape this library writes (taken from a synthetic
//! model), not hand-made guesses: a store that refuses
//! one refuses a document the schema accepts, which is the finding. Every
//! run takes fresh uuids - `0f0e0d0c-0b0a-4908-8706-` and a stamp - so that
//! nothing the suite writes can stand for an object somebody else holds,
//! and so that what it deletes is recognisably its own.
//!
//! Several releases on purpose, the ones a session may not have agreed
//! included: what a store does with a type it did not announce is part of
//! what a provider is told.

use energyml_core::identifiers::EtpUri;
use retp_client::arrays::{Array, ArrayValues};
use retp_types::datatypes::data_array_types::DataArrayIdentifier;
use retp_types::datatypes::object::{DataObject, Resource};
use retp_types::datatypes::{AnyArrayType, AnyLogicalArrayType};

/// One object to write, and its array when it has one.
#[derive(Debug, Clone)]
pub struct Sample {
    /// `resqml22.PointSetRepresentation`.
    pub qualified_type: String,
    /// Its ETP URI in the suite's dataspace.
    pub uri: String,
    pub object: DataObject,
    /// Where its numbers go, and the numbers.
    pub array: Option<(DataArrayIdentifier, Array)>,
    /// The array is written block by block - declared by
    /// `PutUninitializedDataArrays`, then filled by `PutDataSubarrays`
    /// (§13.2.1.2 steps 1 and 3.b) - instead of whole by `PutDataArrays`.
    /// The other way a client writes an array, asked of the store even when
    /// the array would fit one message: one array of the two goes this way.
    pub in_blocks: bool,
}

/// The uuid prefix of everything the suite writes.
pub const PREFIX: &str = "0f0e0d0c-0b0a-4908-8706";

/// The rows of [`POINTS`]: one per node.
pub const ROWS: u64 = 16;

/// The points both point sets carry: sixteen nodes, x y z, in EPSG:2154 and
/// metres up. Copied from a synthetic model, the point
/// set `d7cb8ad6-3d5a-58f0-9ac8-b57abbdb8c0c` *Quaternary (system) - base* - a
/// real geometry rather than three made-up nodes, and enough rows for a
/// sub-array to be a real part of the array. Copied, never read: nothing the
/// suite runs opens a file.
pub const POINTS: [f64; 48] = [
    651259.6553230823,
    6861322.516456878,
    33.75,
    650925.967364612,
    6861781.247060937,
    28.05,
    650882.0369354663,
    6861792.7391978055,
    28.300000000000004,
    651113.2699898992,
    6861368.229005156,
    32.1,
    650960.6783291759,
    6861547.43599403,
    28.880000000000003,
    651211.2852672959,
    6861678.758626449,
    29.759999999999998,
    651123.5157236492,
    6861712.857964776,
    29.909999999999997,
    651147.2159519666,
    6861912.815842617,
    22.5,
    651403.554564632,
    6861855.060662853,
    24.5,
    651034.3376877476,
    6861580.172595028,
    26.5,
    651555.0359422622,
    6861542.436006437,
    29.22,
    651569.618505563,
    6861531.194253832,
    28.78,
    651483.4467609834,
    6862632.784751466,
    15.34,
    651839.3599345337,
    6861317.6691046795,
    32.2,
    650666.5535649733,
    6862339.445391042,
    20.28,
    651240.7850427264,
    6862568.10246881,
    24.83,
];

/// The rows a sub-array read asks for, `[first, count]` along the nodes -
/// the middle half, so that a store reading from the start, or to the end,
/// answers something else.
pub const SUBARRAY_ROWS: [u64; 2] = [4, 8];

fn citation(title: &str) -> String {
    format!(
        "<eml:Citation><eml:Title>{title}</eml:Title><eml:Originator>retp-conformance</eml:Originator>\
<eml:Creation>2026-10-08T00:00:00Z</eml:Creation><eml:Format>retp-conformance</eml:Format></eml:Citation>"
    )
}

fn dor(uuid: &str, qualified_type: &str, title: &str) -> String {
    format!(
        "<eml:Uuid>{uuid}</eml:Uuid><eml:QualifiedType>{qualified_type}</eml:QualifiedType><eml:Title>{title}</eml:Title>"
    )
}

const EML23: &str = "xmlns:eml=\"http://www.energistics.org/energyml/data/commonv2\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"";
const RESQML: &str = "xmlns:resqml=\"http://www.energistics.org/energyml/data/resqmlv2\"";
const WITSML: &str = "xmlns:witsml=\"http://www.energistics.org/energyml/data/witsmlv2\"";
const PRODML: &str = "xmlns:prodml=\"http://www.energistics.org/energyml/data/prodmlv2\"";

/// The samples, written into `space` (a dataspace URI), their uuids drawn
/// from `stamp` (12 hex digits). In the order they must be put: what is
/// referenced before what references it.
pub fn samples(space: &str, stamp: u64) -> Vec<Sample> {
    let id = |n: u64| format!("{PREFIX}-{:012x}", (stamp + n) & 0xffff_ffff_ffff);
    let (projected, vertical, local2d, compound) = (id(0), id(1), id(2), id(3));
    let (feature, interpretation, points22, well) = (id(4), id(5), id(6), id(7));
    let (feature20, proxy20, points20, crs20) = (id(8), id(9), id(10), id(11));
    let (flow, series) = (id(12), id(13));
    // Through the one constructor of an object URI: in the default dataspace
    // (`eml:///`) there is no dataspace segment to join with a `/`.
    let path = EtpUri::parse(space).ok().and_then(|parsed| parsed.dataspace);
    let uri = |qualified_type: &str, uuid: &str| EtpUri::object(path.as_deref(), qualified_type, uuid);
    let mut out = Vec::new();
    let mut push =
        |qualified_type: &str, uuid: &str, xml: String, array: Option<(DataArrayIdentifier, Array)>| {
            let object_uri = uri(qualified_type, uuid);
            out.push(Sample {
                in_blocks: false,
                qualified_type: qualified_type.to_string(),
                uri: object_uri.clone(),
                object: DataObject {
                    resource: Resource {
                        uri: object_uri,
                        name: "retp-conformance".into(),
                        ..Default::default()
                    },
                    format: "xml".into(),
                    blob_id: None,
                    data: xml.into_bytes(),
                },
                array,
            });
        };
    let points = |uri: String, path: String| {
        (
            DataArrayIdentifier {
                uri,
                path_in_resource: path,
            },
            Array {
                dimensions: vec![ROWS, 3],
                logical: AnyLogicalArrayType::ArrayOfDouble64Le,
                transport: AnyArrayType::ArrayOfDouble,
                values: ArrayValues::Doubles(POINTS.to_vec()),
                blocks: 1,
            },
        )
    };

    // --- eml 2.3: the systems a RESQML 2.2 geometry names ---------------------------
    push(
        "eml23.ProjectedCrs",
        &projected,
        format!(
            "<eml:ProjectedCrs {EML23} uuid=\"{projected}\" schemaVersion=\"2.3\" uom=\"m\">{}\
<eml:AxisOrder>easting northing</eml:AxisOrder>\
<eml:AbstractProjectedCrs xsi:type=\"eml:ProjectedEpsgCrs\"><eml:EpsgCode>2154</eml:EpsgCode></eml:AbstractProjectedCrs></eml:ProjectedCrs>",
            citation("EPSG:2154")
        ),
        None,
    );
    push(
        "eml23.VerticalCrs",
        &vertical,
        format!(
            "<eml:VerticalCrs {EML23} uuid=\"{vertical}\" schemaVersion=\"2.3\" uom=\"m\">{}<eml:Direction>up</eml:Direction>\
<eml:AbstractVerticalCrs xsi:type=\"eml:VerticalEpsgCrs\"><eml:EpsgCode>5720</eml:EpsgCode></eml:AbstractVerticalCrs></eml:VerticalCrs>",
            citation("EPSG:5720")
        ),
        None,
    );
    push(
        "eml23.LocalEngineering2dCrs",
        &local2d,
        format!(
            "<eml:LocalEngineering2dCrs {EML23} uuid=\"{local2d}\" schemaVersion=\"2.3\">{}\
<eml:Azimuth uom=\"dega\">0</eml:Azimuth><eml:AzimuthReference>grid north</eml:AzimuthReference>\
<eml:OriginProjectedCoordinate1>0</eml:OriginProjectedCoordinate1><eml:OriginProjectedCoordinate2>0</eml:OriginProjectedCoordinate2>\
<eml:HorizontalAxes><eml:Direction1>east</eml:Direction1><eml:Direction2>north</eml:Direction2><eml:Uom>m</eml:Uom><eml:IsTime>false</eml:IsTime></eml:HorizontalAxes>\
<eml:OriginProjectedCrs uuid=\"{projected}\" schemaVersion=\"2.3\" uom=\"m\">{}<eml:AxisOrder>easting northing</eml:AxisOrder>\
<eml:AbstractProjectedCrs xsi:type=\"eml:ProjectedEpsgCrs\"><eml:EpsgCode>2154</eml:EpsgCode></eml:AbstractProjectedCrs></eml:OriginProjectedCrs>\
</eml:LocalEngineering2dCrs>",
            citation("EPSG:2154 local"),
            citation("EPSG:2154")
        ),
        None,
    );
    push(
        "eml23.LocalEngineeringCompoundCrs",
        &compound,
        format!(
            "<eml:LocalEngineeringCompoundCrs {EML23} uuid=\"{compound}\" schemaVersion=\"2.3\">{}\
<eml:VerticalCrs>{}</eml:VerticalCrs><eml:OriginVerticalCoordinate>0</eml:OriginVerticalCoordinate>\
<eml:VerticalAxis><eml:Direction>up</eml:Direction><eml:Uom>m</eml:Uom><eml:IsTime>false</eml:IsTime></eml:VerticalAxis>\
<eml:LocalEngineering2dCrs>{}</eml:LocalEngineering2dCrs></eml:LocalEngineeringCompoundCrs>",
            citation("EPSG:2154 + EPSG:5720"),
            dor(&vertical, "eml23.VerticalCrs", "EPSG:5720"),
            dor(&local2d, "eml23.LocalEngineering2dCrs", "EPSG:2154 local")
        ),
        None,
    );

    // --- RESQML 2.2: a feature, its interpretation, a point set with an array -----
    push(
        "resqml22.BoundaryFeature",
        &feature,
        format!(
            "<resqml:BoundaryFeature {RESQML} {EML23} uuid=\"{feature}\" schemaVersion=\"2.2\">{}<resqml:IsWellKnown>false</resqml:IsWellKnown></resqml:BoundaryFeature>",
            citation("retp-conformance horizon")
        ),
        None,
    );
    push(
        "resqml22.HorizonInterpretation",
        &interpretation,
        format!(
            "<resqml:HorizonInterpretation {RESQML} {EML23} uuid=\"{interpretation}\" schemaVersion=\"2.2\">{}\
<resqml:InterpretedFeature>{}</resqml:InterpretedFeature></resqml:HorizonInterpretation>",
            citation("retp-conformance horizon"),
            dor(&feature, "resqml22.BoundaryFeature", "retp-conformance horizon")
        ),
        None,
    );
    let points22_uri = uri("resqml22.PointSetRepresentation", &points22);
    let path22 = format!("/resqml22/{points22}/points");
    push(
        "resqml22.PointSetRepresentation",
        &points22,
        format!(
            "<resqml:PointSetRepresentation {RESQML} {EML23} uuid=\"{points22}\" schemaVersion=\"2.2\">{}\
<resqml:RepresentedObject>{}</resqml:RepresentedObject><resqml:NodePatchGeometry><resqml:LocalCrs>{}</resqml:LocalCrs>\
<resqml:Points xsi:type=\"resqml:Point3dExternalArray\"><resqml:Coordinates><eml:ExternalDataArrayPart>\
<eml:Count>{ROWS}</eml:Count><eml:Count>3</eml:Count><eml:PathInExternalFile>{path22}</eml:PathInExternalFile>\
<eml:StartIndex>0</eml:StartIndex><eml:StartIndex>0</eml:StartIndex><eml:URI>retp-conformance.h5</eml:URI>\
<eml:MimeType>application/x-hdf5</eml:MimeType></eml:ExternalDataArrayPart></resqml:Coordinates></resqml:Points>\
</resqml:NodePatchGeometry></resqml:PointSetRepresentation>",
            citation("retp-conformance points"),
            dor(
                &interpretation,
                "resqml22.HorizonInterpretation",
                "retp-conformance horizon"
            ),
            dor(
                &compound,
                "eml23.LocalEngineeringCompoundCrs",
                "EPSG:2154 + EPSG:5720"
            )
        ),
        Some(points(points22_uri, path22)),
    );

    // --- WITSML 2.1: a well, a family a RESQML store may not have announced -------
    push(
        "witsml21.Well",
        &well,
        format!(
            "<witsml:Well {WITSML} {EML23} uuid=\"{well}\" schemaVersion=\"2.1\">{}\
<eml:Existence>actual</eml:Existence><eml:ActiveStatus>inactive</eml:ActiveStatus><witsml:Country>France</witsml:Country>\
<witsml:InformationalProjectedLocation><eml:Coordinate1>651000</eml:Coordinate1><eml:Coordinate2>6861000</eml:Coordinate2>\
<eml:ProjectedCrs>{}</eml:ProjectedCrs></witsml:InformationalProjectedLocation></witsml:Well>",
            citation("retp-conformance well"),
            dor(&projected, "eml23.ProjectedCrs", "EPSG:2154")
        ),
        None,
    );

    // --- RESQML 2.0.1: a feature, a CRS, a point set and the proxy it names ----
    // The proxy is written because the 2.0.1 geometry must name one
    // (`HdfProxy`); it carries no array. The array is identified by the point
    // set's own URI: §23.32.3 (`DataArrayIdentifier.uri`) allows « an
    // Energistics data object (that references the array) » as well as the
    // proxy, and an `ExternalPartReference` is not supposed to carry arrays.
    let r20 = "xmlns:resqml=\"http://www.energistics.org/energyml/data/resqmlv2\" xmlns:eml=\"http://www.energistics.org/energyml/data/commonv2\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"";
    push(
        "resqml20.obj_GeneticBoundaryFeature",
        &feature20,
        format!(
            "<resqml:obj_GeneticBoundaryFeature {r20} uuid=\"{feature20}\" schemaVersion=\"2.0\">{}\
<resqml:GeneticBoundaryKind>horizon</resqml:GeneticBoundaryKind></resqml:obj_GeneticBoundaryFeature>",
            citation("retp-conformance horizon 2.0.1")
        ),
        None,
    );
    push(
        "eml20.obj_EpcExternalPartReference",
        &proxy20,
        format!(
            "<eml:EpcExternalPartReference xmlns:eml=\"http://www.energistics.org/energyml/data/commonv2\" uuid=\"{proxy20}\" schemaVersion=\"2.0\">{}\
<eml:MimeType>application/x-hdf5</eml:MimeType></eml:EpcExternalPartReference>",
            citation("retp-conformance HDF5 proxy")
        ),
        None,
    );
    push(
        "resqml20.obj_LocalDepth3dCrs",
        &crs20,
        format!(
            "<resqml:obj_LocalDepth3dCrs {r20} uuid=\"{crs20}\" schemaVersion=\"2.0\">{}\
<resqml:YOffset>0</resqml:YOffset><resqml:ZOffset>0</resqml:ZOffset><resqml:ArealRotation uom=\"rad\">0</resqml:ArealRotation>\
<resqml:ProjectedAxisOrder>easting northing</resqml:ProjectedAxisOrder><resqml:ProjectedUom>m</resqml:ProjectedUom>\
<resqml:VerticalUom>m</resqml:VerticalUom><resqml:XOffset>0</resqml:XOffset><resqml:ZIncreasingDownward>false</resqml:ZIncreasingDownward>\
<resqml:VerticalCrs xsi:type=\"eml:VerticalCrsEpsgCode\"><eml:EpsgCode>5720</eml:EpsgCode></resqml:VerticalCrs>\
<resqml:ProjectedCrs xsi:type=\"eml:ProjectedCrsEpsgCode\"><eml:EpsgCode>2154</eml:EpsgCode></resqml:ProjectedCrs></resqml:obj_LocalDepth3dCrs>",
            citation("retp-conformance CRS 2.0.1")
        ),
        None,
    );
    let points20_uri = uri("resqml20.obj_PointSetRepresentation", &points20);
    let path20 = format!("/RESQML/{points20}/points");
    push(
        "resqml20.obj_PointSetRepresentation",
        &points20,
        format!(
            "<resqml:obj_PointSetRepresentation {r20} uuid=\"{points20}\" schemaVersion=\"2.0\">{}\
<resqml:NodePatch><resqml:PatchIndex>0</resqml:PatchIndex><resqml:Count>{ROWS}</resqml:Count><resqml:Geometry>\
<resqml:LocalCrs><eml:ContentType>application/x-resqml+xml;version=2.0;type=obj_LocalDepth3dCrs</eml:ContentType>\
<eml:Title>retp-conformance CRS 2.0.1</eml:Title><eml:UUID>{crs20}</eml:UUID></resqml:LocalCrs>\
<resqml:Points xsi:type=\"resqml:Point3dHdf5Array\"><resqml:Coordinates><eml:PathInHdfFile>{path20}</eml:PathInHdfFile>\
<eml:HdfProxy><eml:ContentType>application/x-eml+xml;version=2.0;type=obj_EpcExternalPartReference</eml:ContentType>\
<eml:Title>retp-conformance HDF5 proxy</eml:Title><eml:UUID>{proxy20}</eml:UUID></eml:HdfProxy></resqml:Coordinates>\
</resqml:Points></resqml:Geometry></resqml:NodePatch></resqml:obj_PointSetRepresentation>",
            citation("retp-conformance points 2.0.1")
        ),
        Some(points(points20_uri.clone(), path20)),
    );

    // --- PRODML 2.3: two objects of the Lutetia model, a family a RESQML store
    // may not have announced, sent all the same. Their bodies are those of the
    // synthetic model's doublet and product flow model, the citation and uuid the suite's.
    push(
        "prodml23.ProductFlowModel",
        &flow,
        format!(
            "<prodml:ProductFlowModel {PRODML} {EML23} uuid=\"{flow}\" schemaVersion=\"2.3\">{}\
<prodml:Comment>synthetic - GTH-1 → Lutetia - heat exchanger → GTH-2</prodml:Comment>\
<prodml:Network uid=\"network-1\"><prodml:Name>Lutetia - doublet loop</prodml:Name>\
<prodml:Unit uid=\"unit-1\"><prodml:Name>GTH-1</prodml:Name><prodml:Facility>GTH-1</prodml:Facility>\
<prodml:Port uid=\"unit-1-outlet\"><prodml:Direction>outlet</prodml:Direction><prodml:Name>outlet</prodml:Name>\
<prodml:ConnectedNode uid=\"unit-1-outlet-node\"><prodml:Node>node 1</prodml:Node></prodml:ConnectedNode></prodml:Port></prodml:Unit>\
<prodml:Unit uid=\"unit-2\"><prodml:Name>Lutetia - heat exchanger</prodml:Name><prodml:Facility>Lutetia - heat exchanger</prodml:Facility>\
<prodml:Port uid=\"unit-2-inlet\"><prodml:Direction>inlet</prodml:Direction><prodml:Name>inlet</prodml:Name>\
<prodml:ConnectedNode uid=\"unit-2-inlet-node\"><prodml:Node>node 1</prodml:Node></prodml:ConnectedNode></prodml:Port>\
<prodml:Port uid=\"unit-2-outlet\"><prodml:Direction>outlet</prodml:Direction><prodml:Name>outlet</prodml:Name>\
<prodml:ConnectedNode uid=\"unit-2-outlet-node\"><prodml:Node>node 2</prodml:Node></prodml:ConnectedNode></prodml:Port></prodml:Unit>\
<prodml:Unit uid=\"unit-3\"><prodml:Name>GTH-2</prodml:Name><prodml:Facility>GTH-2</prodml:Facility>\
<prodml:Port uid=\"unit-3-inlet\"><prodml:Direction>inlet</prodml:Direction><prodml:Name>inlet</prodml:Name>\
<prodml:ConnectedNode uid=\"unit-3-inlet-node\"><prodml:Node>node 2</prodml:Node></prodml:ConnectedNode></prodml:Port></prodml:Unit>\
</prodml:Network></prodml:ProductFlowModel>",
            citation("retp-conformance - Lutetia doublet loop")
        ),
        None,
    );
    let hours: String = (0..4)
        .map(|h| {
            format!(
                "<prodml:DataValue xsi:type=\"prodml:DoubleValue\"><prodml:DoubleValue dTim=\"2025-06-02T{h:02}:00:00Z\">16.5</prodml:DoubleValue></prodml:DataValue>"
            )
        })
        .collect();
    push(
        "prodml23.TimeSeriesData",
        &series,
        format!(
            "<prodml:TimeSeriesData {PRODML} {EML23} uuid=\"{series}\" schemaVersion=\"2.3\">{}\
<prodml:Key keyword=\"asset identifier\">eml://example.com/well(GTH-1)/wellbore(01)/wellhead</prodml:Key>\
<prodml:Key keyword=\"flow\">production</prodml:Key><prodml:Key keyword=\"qualifier\">simulated</prodml:Key>\
<prodml:Uom>degC</prodml:Uom><prodml:MeasureClass>thermodynamic temperature</prodml:MeasureClass>\
<prodml:Comment>synthetic, qualified `simulated`: a constant 16.5 degC</prodml:Comment>{hours}</prodml:TimeSeriesData>",
            citation("retp-conformance - GTH-1 wellhead temperature")
        ),
        None,
    );
    // The 2.0.1 point set's array goes block by block, the 2.2 one whole.
    for sample in out.iter_mut().filter(|sample| sample.uri == points20_uri) {
        sample.in_blocks = true;
    }
    out
}

/// Whether `qualified_type` is among the session's data object types -
/// `resqml22.*` and `resqml22.PointSetRepresentation` both cover it.
pub fn announced(qualified_type: &str, agreed: &[String]) -> bool {
    let family = qualified_type.split('.').next().unwrap_or("");
    agreed.iter().any(|one| {
        one == "*"
            || one.eq_ignore_ascii_case(qualified_type)
            || one
                .strip_suffix(".*")
                .is_some_and(|f| f.eq_ignore_ascii_case(family))
    })
}
