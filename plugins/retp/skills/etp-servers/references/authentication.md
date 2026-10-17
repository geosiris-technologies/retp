# Logging in without the catalogue

Use this only for a server that is not in the catalogue (or with a retp older than 0.3.0).
The catalogue ([catalogue.md](catalogue.md)) is better: nothing secret is ever near a command.

## Which variable for which login

`retp` (and `retp-conformance`) read the secret from the environment when no catalogue server
is used. The non-secret options are ordinary arguments.

| The server wants | Options | Secret from |
|---|---|---|
| a bearer token | none | `ETP_TOKEN` |
| HTTP Basic | `--user U` | `ETP_PASSWORD` |
| Azure AD, client credentials | `--azure-tenant T --client-id ID --oauth-scope S` | `ETP_CLIENT_SECRET` |
| any OAuth 2.0 endpoint, client credentials | `--token-url URL --client-id ID [--oauth-scope S]` | `ETP_CLIENT_SECRET` |
| OAuth 2.0 password grant | `--token-url URL --user U` | `ETP_PASSWORD` |
| AWS Cognito | `--cognito-region R --client-id ID --user U` | `ETP_PASSWORD` |

`ETP_USER` can carry the user instead of `--user`. Never use `--token`, `--password` or
`--client-secret`: their value would be on the command line. The command prints the scheme it
sends (`credentials: Bearer, for 3599 s`), never the value.

## Read the secret from its file, in the same command

The user keeps the secret in a file only they can read (mode `0600`). The command reads it
into the variable for `retp`'s process only: the value is not on any command line, is not
printed, and does not outlive the command. Never `cat` the file on its own, never `echo` or
`export` the variable in a separate step.

POSIX shells (sh, bash, zsh):

```sh
ETP_TOKEN="$(cat ~/.config/retp/demo.token)" retp dataspaces wss://etp.example.com/etp
ETP_PASSWORD="$(cat ~/.config/retp/demo.pass)" retp connect wss://etp.example.com/etp --user demo-user
```

PowerShell, scoped to one block and removed afterwards:

```powershell
& { $env:ETP_TOKEN = (Get-Content -Raw -LiteralPath "$HOME\.config\retp\demo.token").Trim()
    try { retp dataspaces wss://etp.example.com/etp } finally { Remove-Item Env:ETP_TOKEN } }
```

If the variable is already set in the environment Claude Code was started from, nothing needs
reading: run the command as is. To know whether it is set without showing it:
`[ -n "${ETP_TOKEN:-}" ] && echo set || echo unset` (PowerShell:
`if ($env:ETP_TOKEN) { 'set' } else { 'unset' }`).

If the user's token comes from a command (`az account get-access-token`, a vault CLI...), the
same pattern applies — `ETP_TOKEN="$(their-token-command)" retp ...` — as long as that command
prints only the token and nothing of it reaches the screen. Ask them for the command; do not
run one that prints a token to check it.

## Docker

`-e NAME` **without a value** passes the variable from the shell to the container, so the
value is not written in the command:

```sh
ETP_TOKEN="$(cat ~/.config/retp/demo.token)" docker run --rm -e ETP_TOKEN \
    ghcr.io/geosiris-technologies/retp:latest dataspaces wss://etp.example.com/etp
```

Never write `-e ETP_TOKEN=...` with the value, nor an `--env-file` that holds it unless the
user keeps that file themselves with restricted permissions.

## Certificates

`--insecure` skips the server's certificate check (it warns every time). Use it only when the
user says the certificate is known to be self-signed, expired or misnamed. Token endpoints are
always reached with their certificates checked.
