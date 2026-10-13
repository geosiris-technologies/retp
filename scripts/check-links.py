#!/usr/bin/env python3
"""Check the relative links and anchors of the Markdown documents, offline.

    python scripts/check-links.py            # from the repository root
    python scripts/check-links.py --root .   # or from anywhere

Checked: `README.md`, `ARCHITECTURE.md`, `CONTRIBUTING.md` and every
other Markdown file at the root, `docs/**/*.md` (the ADRs included), and the
`README.md` and `ARCHITECTURE.md` of every crate.

For each `[text](target)` and each reference definition `[id]: target`:

- an absolute URL (`https:`, `mailto:`, ...) is not followed: this check is offline;
- a relative path must exist (a folder is fine);
- a `#fragment`, alone or after a path to a Markdown file, must match a heading
  of that file, slugged the way GitHub does it.

Links inside code fences and inline code are ignored. Exit status: 0 when every
link resolves, 1 otherwise (each failure is printed as `file:line: message`,
and as a GitHub annotation when run by Actions). Standard library only.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from urllib.parse import unquote

INLINE = re.compile(r"(?<!\!)\[(?:[^\]\[]|\[[^\]]*\])*\]\(\s*<?([^)\s>]+)>?(?:\s+\"[^\"]*\")?\s*\)")
IMAGE = re.compile(r"\!\[(?:[^\]]*)\]\(\s*<?([^)\s>]+)>?(?:\s+\"[^\"]*\")?\s*\)")
REFDEF = re.compile(r"^\s{0,3}\[[^\]]+\]:\s*<?(\S+?)>?(?:\s+.*)?$")
HEADING = re.compile(r"^\s{0,3}(#{1,6})\s+(.*?)\s*#*\s*$")
SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
SKIP_DIRS = {".git", "target", "node_modules", "book"}


def slug(heading: str) -> str:
    """GitHub's anchor for a heading: lower case, punctuation dropped, spaces to hyphens."""
    text = re.sub(r"`([^`]*)`", r"\1", heading)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = text.strip().lower()
    text = re.sub(r"[^\w\- ]", "", text, flags=re.UNICODE)
    return text.replace(" ", "-")


def strip_code(lines: list[str], blank_inline: bool = True) -> list[tuple[int, str]]:
    """The (line number, text) pairs outside fenced code, inline code blanked out unless
    `blank_inline` is false (a heading's anchor is made from its inline code too)."""
    out: list[tuple[int, str]] = []
    fence: str | None = None
    for no, line in enumerate(lines, 1):
        m = re.match(r"^\s{0,3}(`{3,}|~{3,})", line)
        if m:
            marker = m.group(1)[0] * 3
            if fence is None:
                fence = marker
            elif marker == fence:
                fence = None
            continue
        if fence is not None:
            continue
        out.append((no, re.sub(r"`[^`]*`", lambda mm: " " * len(mm.group(0)), line) if blank_inline else line))
    return out


_anchor_cache: dict[Path, set[str]] = {}


def anchors(path: Path) -> set[str]:
    if path in _anchor_cache:
        return _anchor_cache[path]
    seen: dict[str, int] = {}
    found: set[str] = set()
    for _, line in strip_code(path.read_text(encoding="utf-8").splitlines(), blank_inline=False):
        m = HEADING.match(line)
        if not m:
            continue
        base = slug(m.group(2))
        n = seen.get(base, 0)
        seen[base] = n + 1
        found.add(base if n == 0 else f"{base}-{n}")
    _anchor_cache[path] = found
    return found


def documents(root: Path) -> list[Path]:
    found: list[Path] = sorted(root.glob("*.md"))
    docs = root / "docs"
    if docs.is_dir():
        found += sorted(p for p in docs.rglob("*.md") if not SKIP_DIRS & set(p.relative_to(root).parts))
    crates = root / "crates"
    if crates.is_dir():
        for name in ("README.md", "ARCHITECTURE.md"):
            found += sorted(crates.glob(f"*/{name}"))
            found += sorted(crates.glob(f"generated/*/{name}"))
        found += sorted(crates.glob("generated/README.md"))
    return sorted(set(found))


def targets(path: Path) -> list[tuple[int, str]]:
    found: list[tuple[int, str]] = []
    for no, line in strip_code(path.read_text(encoding="utf-8").splitlines()):
        for rx in (INLINE, IMAGE):
            found += [(no, m.group(1)) for m in rx.finditer(line)]
        m = REFDEF.match(line)
        if m:
            found.append((no, m.group(1)))
    return found


def check(root: Path) -> list[str]:
    problems: list[str] = []
    for doc in documents(root):
        shown = doc.relative_to(root).as_posix()
        for no, target in targets(doc):
            if SCHEME.match(target) or target.startswith("//"):
                continue
            path_part, _, fragment = target.partition("#")
            path_part = unquote(path_part)
            dest = doc if not path_part else (doc.parent / path_part)
            if path_part.startswith("/"):
                dest = root / path_part.lstrip("/")
            dest = Path(os.path.normpath(dest))
            if not dest.exists():
                problems.append(f"{shown}:{no}: broken link `{target}`: {dest.relative_to(root).as_posix() if root in dest.parents else dest} does not exist")
                continue
            if fragment and dest.is_file() and dest.suffix == ".md":
                if unquote(fragment).lower() not in anchors(dest):
                    problems.append(f"{shown}:{no}: broken anchor `{target}`: no heading `#{fragment}` in {dest.relative_to(root).as_posix()}")
    return problems


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=".", help="the repository root (default: the current folder)")
    args = ap.parse_args()
    root = Path(args.root).resolve()
    docs = documents(root)
    if not docs:
        print(f"error: no Markdown document under {root}", file=sys.stderr)
        return 2
    problems = check(root)
    github = os.environ.get("GITHUB_ACTIONS") == "true"
    for p in problems:
        print(p)
        if github:
            file, line, msg = p.split(":", 2)
            print(f"::error file={file},line={line}::{msg.strip()}")
    print(f"{len(docs)} documents checked, {len(problems)} broken link(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
