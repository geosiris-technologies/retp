"""Where the notebooks find the ETP server, and nothing else.

Every setting comes from the environment; no server name, user or token is
written in a notebook.

    RETP_URL              required   wss://... ETP endpoint
    RETP_TOKEN            required   bearer token, without its scheme
    RETP_INSECURE         optional   "1" to skip the certificate check
    RETP_PARTITION        optional   OSDU data partition
    RETP_DATASPACE        optional   the dataspace to read (else the first listed)
    RETP_WRITE_DATASPACE  optional   a dataspace made for the test, to be deleted
                                     by its owner; the writing notebook does
                                     nothing without it
    RETP_EPC              optional   path of an EPC to push

This module does no protocol work: it reads variables and hands them to
`retp.connect`. The token lives in the `Arguments` it returns, and `Arguments`
shows `***` in its `repr` and `str`, so a cell that displays the arguments
does not publish the token.
"""

import os
from pathlib import Path

# examples/notebooks/_server.py -> the root of the repository.
ROOT = Path(__file__).resolve().parents[2]

# Where the EPC fixture lands when it is fetched by the sibling energyml-rust
# repository (`python scripts/fetch-fixtures.py`, default `--dir`).
FIXTURE = Path("target") / "fixtures" / "resqpy" / "tic_tac_toe.epc"


class Arguments(dict):
    """The keyword arguments of `retp.connect`, with the token masked in any display."""

    def __repr__(self) -> str:
        shown = {key: ("***" if key == "token" else value) for key, value in self.items()}
        return f"Arguments({shown!r})"

    __str__ = __repr__


def _env(name: str):
    value = os.environ.get(name, "").strip()
    return value or None


def connect_arguments(writing: bool = False):
    """The keyword arguments for `retp.connect(**arguments)`, or None.

    None (after a printed reason) when RETP_URL or RETP_TOKEN is missing: the
    notebook then stops cleanly, it does not raise.
    """
    url, token = _env("RETP_URL"), _env("RETP_TOKEN")
    missing = [name for name, value in (("RETP_URL", url), ("RETP_TOKEN", token)) if value is None]
    if missing:
        print("Nothing to run: set " + " and ".join(missing) + " (see README.md in this folder).")
        return None
    arguments = Arguments(url=url, token=token, insecure=_env("RETP_INSECURE") == "1", writing=writing)
    partition = _env("RETP_PARTITION")
    if partition:
        arguments["partition"] = partition
    return arguments


def dataspace_to_read(session):
    """The dataspace to read: RETP_DATASPACE, else the first one listed (said aloud)."""
    wanted = _env("RETP_DATASPACE")
    rows = session.dataspaces()
    if wanted:
        known = {row["path"] for row in rows} | {row["uri"] for row in rows}
        if wanted not in known:
            print(f"Note: RETP_DATASPACE={wanted!r} is not in the listing; using it as given.")
        return wanted
    if not rows:
        print("The server lists no dataspace: nothing to read.")
        return None
    first = rows[0]
    name = first["path"] or first["uri"]
    print(f"RETP_DATASPACE is unset: using the first dataspace listed, {name!r}.")
    return name


def write_dataspace():
    """RETP_WRITE_DATASPACE, or None after a printed reason (the read-only default)."""
    name = _env("RETP_WRITE_DATASPACE")
    if name is None:
        print(
            "Nothing written: RETP_WRITE_DATASPACE is unset. Create a dataspace for this "
            "test (with the server's own tools), set the variable to its path, and delete "
            "it afterwards."
        )
    return name


def epc_path():
    """The EPC to push: RETP_EPC, else the fetched fixture; None after a printed reason."""
    given = _env("RETP_EPC")
    candidates = [Path(given)] if given else [
        ROOT / FIXTURE,
        ROOT.parent / "energyml-rust" / FIXTURE,
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    print(
        "No EPC to push. Set RETP_EPC to an EPC file (its .h5 beside it), or fetch the "
        "test fixture: in a checkout of energyml-rust, run `python scripts/fetch-fixtures.py` "
        "(it writes target/fixtures/resqpy/tic_tac_toe.epc and its .h5), then set RETP_EPC to that path."
    )
    return None
