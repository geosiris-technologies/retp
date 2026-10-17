"""retp: an ETP v1.2 client for Python, over the Rust `retp` stack.

    import retp

    with retp.connect("wss://etp.example.org/", token=token) as session:
        for space in session.dataspaces():
            print(space["path"], space["uri"])

Everything comes from the compiled extension (`retp._core`); this package only
arranges it into modules and says where each thing lives. The bridge carries no
operation that the Rust library does not have: see `retp.session`.

Objects cross as dictionaries (the EnergyML JSON encoding, every node carrying
its `$type`); arrays cross as numpy arrays.
"""

from ._core import connect
from .errors import (
    AnswerRefused,
    AuthorizationNeeded,
    ConnectionLost,
    LocalError,
    NotSent,
    OutcomeUnknown,
    PushIncomplete,
    RetpError,
    ServerError,
    SessionRefused,
    TimedOut,
    Unsupported,
)
from .session import Session, Transaction

__all__ = [
    "AnswerRefused",
    "AuthorizationNeeded",
    "ConnectionLost",
    "LocalError",
    "NotSent",
    "OutcomeUnknown",
    "PushIncomplete",
    "RetpError",
    "ServerError",
    "Session",
    "SessionRefused",
    "TimedOut",
    "Transaction",
    "Unsupported",
    "connect",
]
