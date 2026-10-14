"""The exceptions of retp, from the most general down.

    RetpError
    +-- Unsupported          the case is not supported; the message names it
    +-- SessionRefused       the session could not be opened (offer, server)
    +-- AuthorizationNeeded  the server wants credentials
    +-- ConnectionLost       the connection is gone: open a new session
    |   +-- OutcomeUnknown   it fell while a request was out: whether it was
    |                        applied is unknown (a retried write may write twice)
    +-- NotSent              the request was not sent; the session is untouched
    +-- ServerError          the server answered with an error (`.code`)
    +-- TimedOut             no answer in time
    +-- AnswerRefused        the answer arrived and could not be taken
    +-- LocalError           an object or a package could not be read or written
    +-- PushIncomplete       a push did not go through in full (`.report`)

A wrong argument is a plain `ValueError`.

The classes live in the compiled extension, because that is where the failure
is classified, from the Rust client's own `ClientError`.
"""

from ._core import (
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
    "SessionRefused",
    "TimedOut",
    "Unsupported",
]
