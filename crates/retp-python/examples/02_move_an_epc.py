"""Move an EPC (with its HDF5 arrays) into a dataspace.

    RETP_TOKEN=... python 02_move_an_epc.py wss://etp.example.org/ model.epc demo

The file is read by Rust (`push_epc`); nothing is converted in Python. To move
an `energyml.rust.Epc` that was edited in memory, save it first:

    epc.save_as("model.epc")
"""

import os
import sys

import retp


def main(url: str, epc: str, dataspace: str) -> None:
    with retp.connect(url, token=os.environ.get("RETP_TOKEN"), writing=True) as session:
        try:
            report = session.push_epc(epc, dataspace)
        except retp.PushIncomplete as stopped:
            # The report says what was sent, what failed and what was left.
            print(stopped, file=sys.stderr)
            print(stopped.report, file=sys.stderr)
            raise SystemExit(1)
        print(report["sent"], "island(s) committed")
        for left in report["dangling"]:
            print("leaves the package:", left)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
