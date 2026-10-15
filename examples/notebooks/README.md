# Notebooks

Worked examples of the Python bridge of `retp`, an ETP v1.2 client (imported as `retp`). Each one answers a question a user of an ETP server asks, and asserts what the ETP 1.2 specification promises: the **shape** of the answers, never the values a given server holds. They are meant to be run against a live server, to re-test the bridge quickly.

| Notebook | Question |
|---|---|
| [00 - Connect, and list](00-connect-and-list.ipynb) | open a session, ping, list the dataspaces and the resources of one (depth, scope, types and wildcard, edges, counts); is a wrong request refused by name? |
| [01 - Objects and arrays](01-objects-and-arrays.ipynb) | read one object and several as dictionaries, find the array an object names, read its metadata, the whole array and a slab, and check the sizes agree |
| [02 - Push an EPC](02-push-an-epc.ipynb) | push an EPC into a dataspace, check against the EPC itself (read without retp) that every object is listed, read an array back; does a read-only session refuse to write? **Writes, only when `RETP_WRITE_DATASPACE` is set** |
| [03 - retp and energyml-rust](03-retp-and-energyml.ipynb) | is an object read with `retp` the same dictionary as one read with `energyml.rust`: does it go into an `Epc` and come out with the same identity, and does it agree with the same object in the pushed EPC? |

## The environment

Every setting comes from an environment variable. No server name, user or token is written in a notebook, and the token is never printed (`_server.py` masks it in any display).

| Variable | Needed | Meaning |
|---|---|---|
| `RETP_URL` | yes | `wss://...` ETP endpoint, for example `wss://etp.example.org/` |
| `RETP_TOKEN` | yes | bearer token, without its scheme |
| `RETP_INSECURE` | no | `1` skips the certificate check (default: checked) |
| `RETP_PARTITION` | no | OSDU data partition |
| `RETP_DATASPACE` | no | the dataspace to read; when unset, a notebook takes the first one listed and says so |
| `RETP_WRITE_DATASPACE` | no | a dataspace you created for the test and will delete. Notebook 02 does nothing when it is unset (it prints why), so that running all the notebooks is read-only by default. Notebook 03 part B reads what 02 pushed there |
| `RETP_EPC` | no | path of an EPC to push, its `.h5` beside it. Default: the test fixture `tic_tac_toe.epc`, looked for in `target/fixtures/resqpy/` of this repository, then of a sibling `energyml-rust` checkout. To fetch it, run `python scripts/fetch-fixtures.py` in an `energyml-rust` checkout, then point `RETP_EPC` at the file |

A notebook whose precondition is missing prints the reason and skips the rest, without a traceback. A server answer that the specification forbids fails an `assert` with a message.

The shared reading of these variables is in [`_server.py`](_server.py). It does no protocol work: it only returns the keyword arguments of `retp.connect`.

## Running them

Build the bridge from a checkout (it needs the HDF5 C library, or `hdf5-static` in the features; see [`crates/retp-python/README.md`](../../crates/retp-python/README.md)):

```
pip install maturin numpy jupyter
cd crates/retp-python
maturin develop
```

Notebook 03 also needs the other bridge: `pip install energyml-rust`.

Then, from this folder:

```
export RETP_URL=wss://etp.example.org/
export RETP_TOKEN=...            # from a file or your secret store, not typed into a shared shell history
jupyter lab
```

Headless, to re-test the bridge in one go (the executed copies go outside the repository):

```
jupyter nbconvert --to notebook --execute --output-dir <somewhere outside the repository> *.ipynb
```

**The notebooks are stored without outputs, and outputs must not be committed**: the output of a live server publishes what that server holds. Run them with `--output-dir` as above, or clear the outputs before committing (`jupyter nbconvert --clear-output --inplace`).
