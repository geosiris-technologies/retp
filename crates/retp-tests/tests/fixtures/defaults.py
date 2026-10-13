"""Encode every ETP record at its schema default, with an independent Avro.

One fixture per `.avsc` -- 197 records, messages and datatypes alike -- each
holding the record as the schema defines its default: a field's `default` where
the schema declares one, the Avro zero where it does not. `fastavro` writes the
bytes; `defaults_match_schema` in `retp-types` compares them with
`T::default().to_avro()`.

The two halves come from different places, which is the point. Here, the
defaults are read straight out of the JSON of each `.avsc`. There, they went
through the generator, an attribute and the derive. A default lost on the way
-- `"xml"` become `""`, a `true` become `false` -- is a byte that differs.

What the "Avro zero" is, where the schema says nothing, is **our** convention
and is written out below so that it can be read: the zero of each primitive,
empty collections, `null` for an optional, the first branch of any other union,
the first symbol of an enumeration, zero bytes for a fixed, and a nested record
at its own default. Only the second half -- the schema's declared values -- is
the specification's.

    pip install fastavro==1.13.1
    python crates/retp-tests/tests/fixtures/defaults.py
"""

import io
import json
import os
import pathlib

import fastavro

import avsc
from avsc import full_name

HERE = pathlib.Path(__file__).parent / "defaults"
# The schema root: $ETP_SCHEMAS if set, else where it lives today.
SCHEMAS = pathlib.Path(os.environ.get("ETP_SCHEMAS") or pathlib.Path(__file__).parents[4] / "rc" / "etp-1.2")


class Defaults:
    """The default value of every schema in one `.avsc`."""

    def __init__(self) -> None:
        self.defs: dict[str, dict] = {}

    def scan(self, schema) -> None:
        if isinstance(schema, list):
            for branch in schema:
                self.scan(branch)
            return
        if not isinstance(schema, dict):
            return
        kind = schema.get("type")
        if isinstance(kind, (dict, list)):
            self.scan(kind)
            return
        if kind in ("record", "enum", "fixed"):
            self.defs[full_name(schema)] = schema
        if kind == "record":
            for field in schema["fields"]:
                self.scan(field["type"])
        elif kind == "array":
            self.scan(schema["items"])
        elif kind == "map":
            self.scan(schema["values"])

    def resolve(self, schema):
        if isinstance(schema, str) and schema in self.defs:
            return self.defs[schema]
        if isinstance(schema, dict) and isinstance(schema.get("type"), (dict, list)):
            return schema["type"]
        return schema

    def tag(self, branch, value):
        """Name a record branch: fastavro otherwise guesses which one it is."""
        resolved = self.resolve(branch)
        if isinstance(value, dict) and isinstance(resolved, dict) and resolved.get("type") == "record":
            return {"-type": full_name(resolved), **value}
        return value

    def zero(self, schema):
        """The value of a type where the schema declares no default."""
        schema = self.resolve(schema)
        if isinstance(schema, list):
            if len(schema) == 2 and "null" in schema:
                return None
            first = schema[0]
            return None if first == "null" else self.tag(first, self.zero(first))
        kind = schema if isinstance(schema, str) else schema["type"]
        if kind == "null":
            return None
        if kind == "boolean":
            return False
        if kind in ("int", "long"):
            return 0
        if kind in ("float", "double"):
            return 0.0
        if kind == "string":
            return ""
        if kind == "bytes":
            return b""
        if kind == "array":
            return []
        if kind == "map":
            return {}
        if kind == "enum":
            return schema.get("default", schema["symbols"][0])
        if kind == "fixed":
            return bytes(schema["size"])
        if kind == "record":
            return self.record(schema)
        raise SystemExit(f"no zero for {schema!r}")

    def declared(self, value, schema):
        """A value the schema declares, in its JSON spelling, made Python."""
        schema = self.resolve(schema)
        if isinstance(schema, list):
            # Avro applies a union's default to its first branch.
            first = schema[0]
            if first == "null":
                if value is not None:
                    raise SystemExit(f"a non-null default for a union led by null: {value!r}")
                return None
            return self.tag(first, self.declared(value, first))
        kind = schema if isinstance(schema, str) else schema["type"]
        if kind == "bytes":
            # A JSON string whose code points are the byte values.
            return value.encode("latin-1")
        if kind == "array":
            return [self.declared(item, schema["items"]) for item in value]
        if kind == "map":
            return {key: self.declared(item, schema["values"]) for key, item in sorted(value.items())}
        if kind == "record":
            return {
                field["name"]: self.declared(value[field["name"]], field["type"])
                if field["name"] in value
                else self.field(field)
                for field in schema["fields"]
            }
        return value

    def field(self, field: dict):
        if "default" in field:
            return self.declared(field["default"], field["type"])
        return self.zero(field["type"])

    def record(self, schema: dict) -> dict:
        return {field["name"]: self.field(field) for field in schema["fields"]}


def main() -> None:
    files = avsc.files(SCHEMAS)
    if not files:
        raise SystemExit(f"no .avsc under {SCHEMAS}")
    HERE.mkdir(exist_ok=True)
    for stale in HERE.glob("*.avro"):
        stale.unlink()

    table = avsc.load(SCHEMAS)
    index = []
    for path in files:
        schema = json.loads(path.read_text(encoding="utf-8"))
        if schema.get("type") != "record":
            continue
        schema = avsc.expand(schema, table)
        defaults = Defaults()
        defaults.scan(schema)
        value = defaults.record(schema)
        parsed = fastavro.parse_schema(schema)
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, parsed, value)
        payload = buffer.getvalue()
        name = full_name(schema)
        (HERE / f"{name}.avro").write_bytes(payload)
        declared = sum(1 for field in schema["fields"] if "default" in field)
        index.append({"fullName": name, "declared": declared, "bytes": len(payload)})

    index.sort(key=lambda e: e["fullName"])
    (HERE / "index.json").write_text(json.dumps(index, indent=2) + "\n", encoding="utf-8")
    print(
        f"{len(index)} records, {sum(e['declared'] for e in index)} declared defaults, "
        f"{sum(e['bytes'] for e in index)} bytes"
    )


if __name__ == "__main__":
    main()
