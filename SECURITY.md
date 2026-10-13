# Security policy

## Supported versions

Only the latest released minor version receives security fixes while the
project is below 1.0.

## Reporting a vulnerability

Please do **not** open a public issue for a security problem.

Use GitHub's private vulnerability reporting: the **Security** tab of this
repository, then **Report a vulnerability**. If that is unavailable, write to
the contact address on https://www.geosiris.com.

Include what you saw, the version, and the smallest input or server behaviour
that reproduces it. You will get an acknowledgement within a few working days.

## Scope worth knowing

- The client decodes bytes sent by a server it does not control. A server
  input that makes the decoder panic, allocate without bound or loop is a
  vulnerability, not just a bug.
- Credentials are read from the environment or from flags. The client never
  prints them; a log line or error message that does is a vulnerability.
- `--insecure` disables certificate checking by design and warns every time.
