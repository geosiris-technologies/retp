"""Produce the Avro fixtures this crate is cross-audited against.

The rule of the house: never ask a component to check itself. Every assertion in
`crossaudit.rs` compares this crate's reading of a byte string with what an
*independent* Avro implementation put there - here, `fastavro`.

    pip install fastavro
    python crates/retp-avro/tests/fixtures/make.py

Rerun it only when adding a case. The `.avro` files are committed on purpose:
the point of the fixture is that it was written by someone else's code, so
regenerating it with the same tool is fine and regenerating it with ours would
destroy the whole exercise.
"""

import io
import json
import pathlib

import fastavro

HERE = pathlib.Path(__file__).parent

# Each case: a file name, a schema, and the value. The schemas deliberately
# cover the shapes ETP actually uses.
CASES = [
    (
        "primitives",
        {
            "type": "record",
            "name": "Primitives",
            "fields": [
                {"name": "flag", "type": "boolean"},
                {"name": "small", "type": "int"},
                {"name": "big", "type": "long"},
                {"name": "single", "type": "float"},
                {"name": "wide", "type": "double"},
                {"name": "text", "type": "string"},
                {"name": "raw", "type": "bytes"},
            ],
        },
        {
            "flag": True,
            "small": -2_000_000_000,
            "big": -9_223_372_036_854_775_808,
            "single": 1.5,
            "wide": -0.125,
            "text": "un puits déviéwith non-ascii ✔",
            "raw": b"\x00\x01\xfe\xff",
        },
    ),
    (
        "extremes",
        {
            "type": "record",
            "name": "Extremes",
            "fields": [
                {"name": "min_long", "type": "long"},
                {"name": "max_long", "type": "long"},
                {"name": "zero", "type": "long"},
                {"name": "minus_one", "type": "long"},
                {"name": "min_int", "type": "int"},
                {"name": "max_int", "type": "int"},
                {"name": "empty", "type": "string"},
            ],
        },
        {
            "min_long": -9_223_372_036_854_775_808,
            "max_long": 9_223_372_036_854_775_807,
            "zero": 0,
            "minus_one": -1,
            "min_int": -2_147_483_648,
            "max_int": 2_147_483_647,
            "empty": "",
        },
    ),
    (
        "collections",
        {
            "type": "record",
            "name": "Collections",
            "fields": [
                {"name": "doubles", "type": {"type": "array", "items": "double"}},
                {"name": "longs", "type": {"type": "array", "items": "long"}},
                {"name": "names", "type": {"type": "array", "items": "string"}},
                {"name": "empty", "type": {"type": "array", "items": "int"}},
                {"name": "caps", "type": {"type": "map", "values": "long"}},
                {"name": "no_caps", "type": {"type": "map", "values": "string"}},
            ],
        },
        {
            "doubles": [0.0, -0.0, 1.0 / 3.0, 1e308, -1e-308],
            "longs": [0, -1, 1, 127, -128, 2**40],
            "names": ["", "a", "élan"],
            "empty": [],
            # Deliberately not in sorted order: the reader must not depend on it.
            "caps": {"zeta": 1, "alpha": 2, "MaxWebSocketMessagePayloadSize": 16777216},
            "no_caps": {},
        },
    ),
    (
        "unions",
        {
            "type": "record",
            "name": "Unions",
            "fields": [
                {"name": "absent", "type": ["null", "long"], "default": None},
                {"name": "present", "type": ["null", "long"]},
                {"name": "picked", "type": ["null", "boolean", "int", "string"]},
                {"name": "first", "type": ["null", "boolean", "int", "string"]},
                {"name": "last", "type": ["null", "boolean", "int", "string"]},
            ],
        },
        {
            "absent": None,
            "present": 4_294_967_296,
            "picked": 42,
            "first": None,
            "last": "the last branch",
        },
    ),
    (
        "named",
        {
            "type": "record",
            "name": "Named",
            "fields": [
                {
                    "name": "kind",
                    "type": {
                        "type": "enum",
                        "name": "IndexDirection",
                        "symbols": ["Increasing", "Decreasing", "Unordered"],
                    },
                },
                {
                    "name": "first_symbol",
                    "type": "IndexDirection",
                },
                {
                    "name": "id",
                    "type": {"type": "fixed", "name": "Uuid", "size": 16},
                },
                {
                    "name": "nested",
                    "type": {
                        "type": "record",
                        "name": "Version",
                        "fields": [
                            {"name": "major", "type": "int"},
                            {"name": "minor", "type": "int"},
                            {"name": "revision", "type": "int"},
                            {"name": "patch", "type": "int"},
                        ],
                    },
                },
                {
                    "name": "many",
                    "type": {"type": "array", "items": "Version"},
                },
            ],
        },
        {
            "kind": "Unordered",
            "first_symbol": "Increasing",
            "id": bytes(range(16)),
            "nested": {"major": 1, "minor": 2, "revision": 0, "patch": 0},
            "many": [
                {"major": 1, "minor": 2, "revision": 3, "patch": 4},
                {"major": 0, "minor": 0, "revision": 0, "patch": 0},
            ],
        },
    ),
]


def main() -> None:
    index = []
    for name, schema, value in CASES:
        parsed = fastavro.parse_schema(schema)
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, parsed, value)
        payload = buffer.getvalue()
        (HERE / f"{name}.avro").write_bytes(payload)

        # Read it straight back, so a broken fixture is caught here and not in
        # Rust, where the blame would land on the wrong crate.
        back = fastavro.schemaless_reader(io.BytesIO(payload), parsed)
        assert back == value, f"{name}: fastavro does not read back what it wrote"

        index.append({"name": name, "bytes": len(payload), "schema": schema})
        print(f"{name}: {len(payload)} bytes")

    (HERE / "index.json").write_text(
        json.dumps(index, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
