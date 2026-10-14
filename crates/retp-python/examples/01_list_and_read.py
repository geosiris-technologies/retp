"""List the dataspaces of an ETP server, then read one object and one array.

    RETP_TOKEN=... python 01_list_and_read.py wss://etp.example.org/ <dataspace> [array-path]

The token comes from the environment, not from the command line (a command line
lands in the shell's history). Nothing here knows a RESQML class: an object is a
dictionary, and its `$type` says which class it is.
"""

import os
import sys

import retp


def main(url: str, dataspace: str, array_path: str = "") -> None:
    with retp.connect(url, token=os.environ.get("RETP_TOKEN")) as session:
        for warning in session.warnings:
            print("warning:", warning, file=sys.stderr)

        for space in session.dataspaces():
            print(space["path"] or "(default)", space["uri"])

        listed = session.resources(dataspace)
        print(len(listed["resources"]), "resources in", dataspace)
        if not listed["resources"]:
            return

        uri = listed["resources"][0]["uri"]
        obj = session.get_object(uri)
        print(obj["$type"], obj.get("Citation", {}).get("Title"))

        # An object does not contain its numbers: it names arrays. The path to
        # ask is in the object (its PathInHdfFile / PathInExternalFile), so the
        # path is an argument, not a guess.
        if array_path:
            values, dimensions = session.read_array(uri, array_path)
            print(values.dtype, dimensions, values[:5])


if __name__ == "__main__":
    main(*sys.argv[1:4])
