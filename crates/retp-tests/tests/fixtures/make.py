"""Encode every ETP message with an independent Avro implementation.

138 fixtures, one per message, each carrying **populated** values -- not the
default value, which for most messages is empty and would let a round trip pass
without proving anything.

The values are produced by walking the real `.avsc`, so every union branch, enum
ordinal, array and map in the schema is exercised. They are deterministic: a
counter drives every choice, so rerunning this produces the same bytes.

    pip install fastavro
    python crates/retp-tests/tests/fixtures/make.py

The `.avro` files are committed because the point of a fixture is that someone
else's code wrote it. Regenerating them with `fastavro` is fine; regenerating
them with ours would destroy the exercise.
"""

import collections
import io
import json
import os
import pathlib

import fastavro

HERE = pathlib.Path(__file__).parent
# The schema root: $ETP_SCHEMAS if set, else where it lives today.
SCHEMAS = pathlib.Path(os.environ.get("ETP_SCHEMAS") or pathlib.Path(__file__).parents[4] / "rc" / "etp-1.2")


class Filler:
    """Builds one value per schema, varying every choice it is offered."""

    def __init__(self, seed: int) -> None:
        self.n = seed
        # An .avsc inlines a named type the first time and refers to it by full
        # name afterwards, so the table is built while walking.
        self.defs: dict[str, dict] = {}

    def remember(self, schema: dict) -> None:
        name = schema.get("fullName")
        if not name:
            namespace = schema.get("namespace")
            name = f"{namespace}.{schema['name']}" if namespace else schema.get("name")
        if name:
            self.defs[name] = schema

    def scan(self, schema) -> None:
        """Record every named definition before filling anything.

        A union branch may name a type defined earlier in the same file, and a
        walk driven by the choices being made would not have visited it yet.
        """
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
            self.remember(schema)
        if kind == "record":
            for field in schema.get("fields", []):
                self.scan(field["type"])
        elif kind == "array":
            self.scan(schema["items"])
        elif kind == "map":
            self.scan(schema["values"])

    def next(self) -> int:
        self.n = (self.n * 1103515245 + 12345) & 0x7FFFFFFF
        return self.n

    def value(self, schema, depth: int = 0):
        if isinstance(schema, str):
            if schema in self.defs:
                return self.value(self.defs[schema], depth)
            return self.primitive(schema)
        if isinstance(schema, list):
            # Take a branch that is not `null` whenever there is one: a fixture
            # that always picks null would never test a branch index.
            carrying = [b for b in schema if b != "null"]
            if not carrying or depth > 6:
                return None
            branch = carrying[self.next() % len(carrying)]
            return self.tagged(branch, self.value(branch, depth + 1))

        kind = schema["type"]
        if isinstance(kind, (dict, list)):
            return self.value(kind, depth)
        if kind in ("record", "enum", "fixed"):
            self.remember(schema)
        if kind == "record":
            return {f["name"]: self.value(f["type"], depth + 1) for f in schema["fields"]}
        if kind == "enum":
            # Not symbol 0: an ordinal bug that always reads zero would hide.
            symbols = schema["symbols"]
            return symbols[self.next() % len(symbols)]
        if kind == "fixed":
            return bytes((self.next() + i) & 0xFF for i in range(schema["size"]))
        if kind == "array":
            if depth > 5:
                return []
            return [self.value(schema["items"], depth + 1) for _ in range(1 + self.next() % 3)]
        if kind == "map":
            if depth > 5:
                return {}
            entries = {
                f"key{self.next() % 1000}": self.value(schema["values"], depth + 1)
                for _ in range(1 + self.next() % 2)
            }
            # Sorted, and the reason matters: an Avro map is unordered, so the
            # order fastavro happens to write is not a fact about the encoding.
            # The Rust side holds maps in a BTreeMap and therefore writes them
            # sorted; writing the fixture sorted too is what makes a byte-level
            # comparison mean "we disagree" rather than "we chose differently".
            return dict(sorted(entries.items()))
        return self.primitive(kind)

    def tagged(self, branch, value):
        """Name the union branch, because fastavro otherwise guesses -- wrongly.

        Handed a bare dict, fastavro picks the first branch the value happens to
        validate against. For `AnyArray.item` that means an `ArrayOfDouble` goes
        out as an `ArrayOfFloat`: same shape, half the precision, no warning.
        `etpproto-python` hit this and works around it the same way.

        On the wire the branch index is explicit, so no *reader* can be confused
        -- this is a Python API hazard, not an Avro one. But a fixture that
        silently lands on another branch tests the wrong thing, which is worse
        than testing nothing.
        """
        if not isinstance(value, dict):
            return value
        name = self.full_name(branch)
        if name is None:
            return value
        return {"-type": name, **value}

    @staticmethod
    def full_name(branch):
        if isinstance(branch, str):
            return branch if "." in branch else None
        if isinstance(branch, dict) and branch.get("type") == "record":
            if "fullName" in branch:
                return branch["fullName"]
            namespace = branch.get("namespace")
            return f"{namespace}.{branch['name']}" if namespace else branch.get("name")
        return None

    def primitive(self, kind: str):
        if kind == "null":
            return None
        if kind == "boolean":
            return self.next() % 2 == 0
        if kind == "int":
            return (self.next() % 200_000) - 100_000
        if kind == "long":
            return (self.next() % 2_000_000_000) - 1_000_000_000
        if kind == "float":
            return float(self.next() % 1000) + 0.5
        if kind == "double":
            return float(self.next() % 100_000) / 8.0
        if kind == "string":
            return f"value-{self.next() % 100_000}"
        if kind == "bytes":
            return bytes((self.next() + i) & 0xFF for i in range(1 + self.next() % 5))
        raise SystemExit(f"unhandled primitive {kind!r}")


def branches_of(value, out=None):
    """Every named union branch in a decoded value, in encounter order."""
    if out is None:
        out = []
    if isinstance(value, tuple):
        out.append(value[0])
        branches_of(value[1], out)
    elif isinstance(value, dict):
        if "-type" in value:
            out.append(value["-type"])
        for key, item in value.items():
            if key != "-type":
                branches_of(item, out)
    elif isinstance(value, list):
        for item in value:
            branches_of(item, out)
    return out


def main() -> None:
    files = sorted(SCHEMAS.rglob("*.avsc"))
    if not files:
        raise SystemExit(f"no .avsc under {SCHEMAS}")

    index = []
    for seed, path in enumerate(files, start=1):
        schema = json.loads(path.read_text(encoding="utf-8"))
        if "protocol" not in schema:
            continue

        filler = Filler(seed * 7919)
        filler.scan(schema)
        value = filler.value(schema)
        parsed = fastavro.parse_schema(schema)
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, parsed, value)
        payload = buffer.getvalue()

        # A fixture that fastavro cannot read back is a broken fixture, and the
        # blame belongs here rather than in Rust.
        back = fastavro.schemaless_reader(
            io.BytesIO(payload),
            parsed,
            return_record_name=True,
            return_record_name_override=True,
        )
        assert back is not None, f"{schema['fullName']}: fastavro read back nothing"
        # Every branch fastavro wrote must be one we asked for. Not the other
        # way round: fastavro only names a branch when its union holds more
        # than one record type, so `IndexValue.item` -- [null, long, double,
        # PassIndexedDepth] -- comes back unnamed even though we tagged it.
        # Inclusion is what catches the failure that matters: asking for an
        # ArrayOfDouble and getting an ArrayOfFloat.
        wanted = collections.Counter(branches_of(value))
        for got in branches_of(back):
            assert wanted[got] > 0, (
                f"{schema['fullName']}: fastavro wrote branch {got}, "
                f"which we never asked for (asked: {sorted(wanted)})"
            )
            wanted[got] -= 1

        name = schema["fullName"].rsplit(".", 1)[1]
        protocol = int(schema["protocol"])
        message_type = int(schema["messageType"])
        (HERE / f"{protocol:02d}-{message_type:02d}-{name}.avro").write_bytes(payload)
        index.append(
            {
                "protocol": protocol,
                "messageType": message_type,
                "name": name,
                "fullName": schema["fullName"],
                "bytes": len(payload),
            }
        )

    index.sort(key=lambda e: (e["protocol"], e["messageType"]))
    (HERE / "index.json").write_text(
        json.dumps(index, indent=2) + "\n", encoding="utf-8"
    )
    print(f"{len(index)} messages, {sum(e['bytes'] for e in index)} bytes")


if __name__ == "__main__":
    main()
