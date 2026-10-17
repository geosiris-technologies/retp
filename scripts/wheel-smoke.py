#!/usr/bin/env python3
"""Smoke-test an installed retp wheel: import it and exercise the compiled code.

    python scripts/wheel-smoke.py

Run it from outside the source tree, in a clean virtual environment that holds
only the wheel and its dependencies, so that the installed module is the one
imported (python.yml does exactly that).

Importing is not a test: a module that imports can still be unable to do its
work. So this script calls into the compiled extension (`retp._core`) and checks
what comes back. It needs no server and no external file.

What the bridge exposes without a server is its connection layer, so that is what
is exercised:

  * the exception tree, as the compiled classes declare it;
  * the refusals `retp.connect` makes before any socket is opened (an HTTP URL
    without `rest=True`, zero requests in flight, a token that is not a bearer
    token, a login without its password), each raised by the Rust code as the
    exception of its kind, and a refused token never echoed in the message;
  * a REST session, which opens nothing: what REST does not do (notifications)
    and what a read-only session does not do (writing) are refused by name;
  * one real connection attempt, to a loopback port nothing listens on, which
    must fail with an exception of the retp tree (the native WebSocket
    transport and the error mapping run).

What it does not exercise, because the bridge offers it only over a session:
reading and writing objects and arrays (and so the HDF5 backend the wheel carries).
"""

import sys

import retp

failures = []


def check(ok: bool, what: str) -> None:
    print(("ok    " if ok else "FAIL  ") + what)
    if not ok:
        failures.append(what)


def raised(call, *exceptions):
    """The exception `call` raises if it is one of `exceptions`, else None."""
    try:
        call()
    except exceptions as err:
        return err
    except BaseException as err:  # another class: reported by the caller as a miss
        print(f"      unexpected {type(err).__name__}: {err}")
        return None
    print("      no exception raised")
    return None


# The names the package promises.
for name in retp.__all__:
    check(hasattr(retp, name), f"retp.{name} exists")

# The tree of the exceptions, from the base down (retp/errors.py).
for name in (
    "Unsupported",
    "SessionRefused",
    "AuthorizationNeeded",
    "ConnectionLost",
    "NotSent",
    "ServerError",
    "TimedOut",
    "AnswerRefused",
    "LocalError",
    "PushIncomplete",
):
    check(issubclass(getattr(retp, name), retp.RetpError), f"{name} is a RetpError")
check(issubclass(retp.OutcomeUnknown, retp.ConnectionLost), "OutcomeUnknown is a ConnectionLost")
check(issubclass(retp.RetpError, Exception), "RetpError is an Exception")

# Refusals made by the Rust code before anything is sent.
REST_URL = "https://etp.example.org/api/reservoir-ddms/v2"
err = raised(lambda: retp.connect(REST_URL), ValueError)
check(err is not None and "rest=True" in str(err), "an HTTP URL without rest=True is refused (ValueError)")

err = raised(lambda: retp.connect("wss://etp.example.org/", user="someone"), ValueError)
check(err is not None and "password" in str(err), "a login without its password is refused (ValueError)")

err = raised(lambda: retp.connect("wss://etp.example.org/", at_once=0), ValueError)
check(err is not None and "at_once" in str(err), "at_once=0 is refused (ValueError)")

err = raised(lambda: retp.connect("wss://etp.example.org/", token="Bearer s3cret value"), ValueError)
check(err is not None and "s3cret" not in str(err), "a bad token is refused without being echoed (ValueError)")

# A REST session opens nothing; what it cannot do is refused by name.
try:
    session = retp.connect(REST_URL, rest=True)
except BaseException as err:
    check(False, f"a REST session did not open: {type(err).__name__}: {err}")
else:
    check(isinstance(session, retp.Session), "a REST session opens without a network")
    err = raised(lambda: session.subscribe("eml:///"), retp.Unsupported)
    check(err is not None, "a REST session refuses notifications (Unsupported)")
    err = raised(lambda: session.put_dataspaces(["demo"]), retp.Unsupported)
    check(err is not None and "writing=True" in str(err), "a read-only session refuses to write (Unsupported)")
    err = raised(lambda: session.start_transaction(["demo"]), retp.Unsupported)
    check(err is not None, "a read-only session opens no transaction (Unsupported)")
    session.close()

# One real attempt: nothing listens on port 1 of the loopback interface.
# TODO(verify): which subclass of RetpError this gives on each platform; only the
# family is asserted here, and the class is printed.
try:
    retp.connect("ws://127.0.0.1:1/")
except retp.RetpError as err:
    check(
        not isinstance(err, retp.Unsupported),
        f"a refused connection raises a RetpError ({type(err).__name__})",
    )
except BaseException as err:
    check(False, f"a refused connection raised {type(err).__name__}, not a RetpError: {err}")
else:
    check(False, "a connection to a closed port did not raise")

print(f"retp imported from {retp.__file__}")
if failures:
    print(f"wheel-smoke: {len(failures)} check(s) failed", file=sys.stderr)
    sys.exit(1)
print("wheel-smoke: all checks passed")
