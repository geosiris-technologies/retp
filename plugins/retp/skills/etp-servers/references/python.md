# The `retp` Python package

When the user wants a script (a notebook, numpy arrays, a loop over objects) rather than
command lines, the `retp` Python package wraps the same Rust client. Its page upstream is
`docs/python.md`; it is alpha, and `pip install retp` may not be published for every version
yet (check with the user).

```python
import os
import retp

# The token comes from where the user keeps it: never a literal in the script.
token = open(os.path.expanduser("~/.config/retp/demo.token")).read().strip()

with retp.connect("wss://etp.example.com/etp", token=token) as session:
    for space in session.dataspaces():
        print(space["path"], space["uri"])
    listed = session.resources("demo", depth=1)
    for resource in listed["resources"]:
        print(resource["uri"], resource["name"])
```

- `retp.connect(url, token=..., partition=..., insecure=..., writing=...)`. Only a bearer
  token is bridged; the other logins of the command line (and the catalogue) are not.
- A session writes only when opened with `writing=True`; `put_objects` and `push_epc` follow
  the same consent rule as `push` (section 3 of `SKILL.md`).
- Objects are dictionaries in the EnergyML JSON encoding (`obj["$type"]`); arrays come back as
  flat numpy arrays with their dimensions (`session.read_array(uri, path)`).
- Errors derive from `retp.RetpError`; plural reads return `(found, errors)` so nothing is
  silently missing.

Do not print the token, store it in the notebook, or pass it in a URL. The bridge keeps it out
of `repr` and error messages; the script must too.
