"""Write the Avro JSON encoding of every ETP message with an independent
implementation.

The same 138 messages as `make.py` -- the values are drawn by its `Filler`, with
the same seeds, so `json/03-01-GetResources.json` and `03-01-GetResources.avro`
are one value in two encodings. The JSON is written by `fastavro.json_writer`,
which follows the Avro specification's "JSON Encoding": a union is `null` or
`{"<branch>": value}` with the branch named by its type name or full name, and
`bytes` / `fixed` are strings of code points 0-255.

    pip install fastavro==1.13.1
    python crates/retp-tests/tests/fixtures/make_json.py

The files are committed because the point of a fixture is that someone else's
code wrote it. Regenerating them with `to_json` would destroy the exercise.
"""

import io
import json
import pathlib

import fastavro

import avsc
import make

HERE = pathlib.Path(__file__).parent / "json"


def main() -> None:
    HERE.mkdir(exist_ok=True)
    files = avsc.files(make.SCHEMAS)
    if not files:
        raise SystemExit(f"no .avsc under {make.SCHEMAS}")

    table = avsc.load(make.SCHEMAS)
    count = 0
    skipped = []
    for seed, path in enumerate(files, start=1):
        schema = json.loads(path.read_text(encoding="utf-8"))
        if "protocol" not in schema:
            continue
        schema = avsc.expand(schema, table)

        if not schema["fields"]:
            # fastavro 1.12's json_writer raises "Internal Parser Exception" on a
            # record with no field. The Avro JSON of one is `{}`, and the Rust
            # test states that itself instead of reading a witness that does
            # not exist.
            skipped.append(avsc.full_name(schema))
            continue

        filler = make.Filler(seed * 7919)
        filler.scan(schema)
        value = filler.value(schema)
        parsed = fastavro.parse_schema(schema)

        out = io.StringIO()
        fastavro.json_writer(out, parsed, [value])
        text = out.getvalue().strip()
        assert "\n" not in text, "one record, one line"

        # `fastavro.json_reader` cannot be the readback check: it raises
        # KeyError in `iter_map` on any map of two or more entries (1.12.2). The
        # text must at least be JSON, and the Rust test is the reader that
        # matters.
        json.loads(text)

        name = avsc.full_name(schema).rsplit(".", 1)[1]
        protocol = int(schema["protocol"])
        message_type = int(schema["messageType"])
        (HERE / f"{protocol:02d}-{message_type:02d}-{name}.json").write_text(
            text + "\n", encoding="utf-8", newline="\n"
        )
        count += 1
    print(f"{count} messages in JSON; no witness for the empty records: {skipped}")


if __name__ == "__main__":
    main()
