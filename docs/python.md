# Python bridge (planned)

A Python package bridging the Rust client is planned, built with pyo3 and
maturin, in this repository. It will carry no functionality the Rust library
lacks: an operation is written in Rust first and bridged second.

Nothing is published yet. Until it is, the earlier pure-Python projects
(`etpclient`, `etpproto`, `etptypes` on PyPI) remain available.

TODO(verify): the PyPI name of the wheel (the plan reserves `retp`); check
availability again before the first release.
