"""Read the Energistics `.avsc` files as self-contained schemas.

The files of the official repository (`rc/etp-1.2/PROVENANCE.md`) hold one
named type each and refer to the others by full name, which `fastavro` cannot
parse one file at a time. `expand` inlines each named type the first time a
schema meets it and refers to it by name afterwards, as Avro requires, so that
every file parses on its own. The expansion adds nothing a reader could tell
apart: same types, same field order, same branches.
"""

import copy
import json
import pathlib

PRIMITIVES = {"null", "boolean", "int", "long", "float", "double", "bytes", "string"}
NAMED = ("record", "enum", "fixed")


def full_name(schema: dict, namespace: str | None = None) -> str:
    """The Avro full name of a named type. An older copy of the schemas also
    carried a `fullName`, which wins when present."""
    if "fullName" in schema:
        return schema["fullName"]
    name = schema["name"]
    if "." in name:
        return name
    namespace = schema.get("namespace", namespace)
    return f"{namespace}.{name}" if namespace else name


def files(root: pathlib.Path) -> list[pathlib.Path]:
    """Every `.avsc` under `root`, sorted by relative path, bytewise. A seed is a
    file's rank in this list, so the order must not depend on the platform:
    `sorted` on paths ignores case on Windows and not on Linux, which swaps
    `PartSubscriptionEnded` and `PartsChanged`."""
    return sorted(root.rglob("*.avsc"), key=lambda p: p.relative_to(root).as_posix().encode())


def load(root: pathlib.Path) -> dict[str, dict]:
    """Every named type under `root`, by full name, as its file writes it."""
    table = {}
    for path in files(root):
        schema = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(schema, dict) and schema.get("type") in NAMED:
            table[full_name(schema)] = schema
    return table


def expand(schema: dict, table: dict[str, dict]) -> dict:
    """`schema` with every named type it uses inlined once, in Avro's order."""
    seen: set[str] = set()

    def walk(s, namespace):
        if isinstance(s, str):
            if s in PRIMITIVES:
                return s
            name = s if "." in s or not namespace else f"{namespace}.{s}"
            if name in seen:
                return name
            if name not in table:
                raise SystemExit(f"unknown type {s!r} (looked up as {name!r})")
            return walk(copy.deepcopy(table[name]), namespace)
        if isinstance(s, list):
            return [walk(branch, namespace) for branch in s]
        kind = s["type"]
        if kind in NAMED:
            name = full_name(s, namespace)
            seen.add(name)
            own = name.rsplit(".", 1)[0] if "." in name else None
            if kind == "record":
                s = {**s, "fields": [{**f, "type": walk(f["type"], own)} for f in s["fields"]]}
            return s
        if kind == "array":
            return {**s, "items": walk(s["items"], namespace)}
        if kind == "map":
            return {**s, "values": walk(s["values"], namespace)}
        # `{"type": "string"}`, `{"type": "<full name>"}` or `{"type": {...}}`.
        return {**s, "type": walk(kind, namespace)}

    return walk(schema, None)
