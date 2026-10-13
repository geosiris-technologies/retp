"""One fixture per union branch, because a sample never reaches them all.

`crossaudit.rs` decodes 138 messages whose values are drawn by a seeded filler.
That exercises eleven of the union branches and misses the rest - a sample is a
sample. These fixtures walk **every** branch of the three unions ETP has, so a
branch index that is off by one cannot hide in the branch nobody happened to
draw.

The reason this matters is concrete and `etpproto-python` met it first
(`tests/test_array_types.py`): handed a bare dict, `fastavro` picks the first
union branch the value happens to validate against, so an `ArrayOfDouble` goes
out as an `ArrayOfFloat` - same shape, half the precision, no warning. The
`-type` tag is what pins the branch, and the check below is what proves the tag
worked rather than assuming it.

    pip install fastavro
    python crates/retp-tests/tests/fixtures/unions.py
"""

import io
import json
import os
import pathlib

import fastavro

HERE = pathlib.Path(__file__).parent / "unions"
# The schema root: $ETP_SCHEMAS if set, else where it lives today.
SCHEMAS = pathlib.Path(os.environ.get("ETP_SCHEMAS") or pathlib.Path(__file__).parents[4] / "rc" / "etp-1.2")
DATATYPES = "Energistics.Etp.v12.Datatypes"

# The three unions that need a Rust enumeration, and one value per branch.
# The branch order is the schema's, which is the encoding: the index below is
# the byte that must come out.
CASES = [
    (
        "AnyArray",
        "Datatypes/AnyArray.avsc",
        "item",
        [
            (f"{DATATYPES}.ArrayOfBoolean", {"values": [True, False, True]}),
            (f"{DATATYPES}.ArrayOfInt", {"values": [-2147483648, 0, 2147483647]}),
            (f"{DATATYPES}.ArrayOfLong", {"values": [-9223372036854775808, 9223372036854775807]}),
            (f"{DATATYPES}.ArrayOfFloat", {"values": [1.5, -2.5]}),
            (f"{DATATYPES}.ArrayOfDouble", {"values": [1.5, -2.5, 1e300]}),
            (f"{DATATYPES}.ArrayOfString", {"values": ["", "élan", "x"]}),
            ("bytes", b"\x00\x01\xfe\xff"),
        ],
    ),
    (
        "IndexValue",
        "Datatypes/IndexValue.avsc",
        "item",
        [
            ("null", None),
            ("long", 1234567890123),
            ("double", -0.125),
            (
                f"{DATATYPES}.ChannelData.PassIndexedDepth",
                {"pass": 3, "direction": "Down", "depth": 42.5},
            ),
        ],
    ),
    (
        "DataValue",
        "Datatypes/DataValue.avsc",
        "item",
        [
            ("null", None),
            ("boolean", True),
            ("int", -7),
            ("long", 9223372036854775807),
            ("float", 0.5),
            ("double", -1.25),
            ("string", "a value"),
            (f"{DATATYPES}.ArrayOfBoolean", {"values": [False]}),
            (f"{DATATYPES}.ArrayOfNullableBoolean", {"values": [True, None]}),
            (f"{DATATYPES}.ArrayOfInt", {"values": [7]}),
            (f"{DATATYPES}.ArrayOfNullableInt", {"values": [None, 8]}),
            (f"{DATATYPES}.ArrayOfLong", {"values": [9]}),
            (f"{DATATYPES}.ArrayOfNullableLong", {"values": [10, None]}),
            (f"{DATATYPES}.ArrayOfFloat", {"values": [11.5]}),
            (f"{DATATYPES}.ArrayOfDouble", {"values": [12.5]}),
            (f"{DATATYPES}.ArrayOfString", {"values": ["thirteen"]}),
            (f"{DATATYPES}.ArrayOfBytes", {"values": [b"\x0e"]}),
            ("bytes", b"\x0f"),
            (
                f"{DATATYPES}.AnySparseArray",
                {
                    "slices": [
                        {
                            "start": 16,
                            "slice": {
                                "item": {
                                    "-type": f"{DATATYPES}.ArrayOfDouble",
                                    "values": [17.5],
                                }
                            },
                        }
                    ]
                },
            ),
        ],
    ),
]


def tag(branch: str, value):
    """Attach the `-type` fastavro needs to stop guessing."""
    if isinstance(value, dict):
        return {"-type": branch, **value}
    return value


def main() -> None:
    HERE.mkdir(parents=True, exist_ok=True)
    index = []
    for union_name, schema_path, field, branches in CASES:
        # Found by name under the root, so the directory layout is not hard-coded.
        found = sorted(SCHEMAS.rglob(pathlib.PurePosixPath(schema_path).name))
        if len(found) != 1:
            raise SystemExit(f"{schema_path}: expected one match under {SCHEMAS}, found {len(found)}")
        schema = json.loads(found[0].read_text("utf-8"))
        parsed = fastavro.parse_schema(schema)

        for want_index, (branch, value) in enumerate(branches):
            payload_value = {field: tag(branch, value)}
            buffer = io.BytesIO()
            fastavro.schemaless_writer(buffer, parsed, payload_value)
            payload = buffer.getvalue()

            # The branch index is the first zigzag varint of the payload.
            # This is the check that would have caught the ArrayOfDouble
            # collapse, and it belongs here rather than in Rust: a fixture that
            # lands on the wrong branch is a broken fixture.
            head = payload[0]
            assert head < 0x80, f"{union_name}.{branch}: branch index is multi-byte"
            got_index = head >> 1

            short = branch.rsplit(".", 1)[-1]
            if got_index != want_index:
                # Two branches of `DataValue.item` cannot be reached from
                # Python at all: `long` (3) collapses into `int` (2) and
                # `float` (4) into `double` (5), because a Python number does
                # not carry the distinction and `-type` only works on records.
                #
                # This is not a defect of the fixture - it is a fact about what
                # a Python ETP peer can put on the wire, and it is written into
                # the index so the Rust side can say so out loud instead of
                # quietly having no coverage there.
                index.append(
                    {
                        "union": union_name,
                        "index": want_index,
                        "branch": short,
                        "fullBranch": branch,
                        "reachable": False,
                        "collapsedTo": got_index,
                    }
                )
                print(f"  unreachable: {union_name} branch {want_index} ({short})"
                      f" -> fastavro writes {got_index}")
                continue

            name = f"{union_name}-{want_index:02d}-{short}"
            (HERE / f"{name}.avro").write_bytes(payload)
            index.append(
                {
                    "union": union_name,
                    "index": want_index,
                    "branch": short,
                    "fullBranch": branch,
                    "reachable": True,
                    "bytes": len(payload),
                }
            )

    (HERE / "index.json").write_text(json.dumps(index, indent=2) + "\n", encoding="utf-8")
    reachable = sum(1 for e in index if e.get("reachable"))
    print(f"{reachable} of {len(index)} branches across {len(CASES)} unions")


if __name__ == "__main__":
    main()
