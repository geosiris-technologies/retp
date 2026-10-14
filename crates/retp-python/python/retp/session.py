"""A session with an ETP server: its dataspaces, objects and arrays.

    import retp

    with retp.connect("wss://etp.example.org/", token=token) as session:
        space = session.dataspaces()[0]
        listed = session.resources(space["path"])
        objects, errors = session.get_objects([r["uri"] for r in listed["resources"]])
        values, dimensions = session.read_array(uri, "/RESQML/.../points")

`retp.connect` opens it. The token you pass is handed to the Rust client and
kept nowhere else: it is not in `repr(session)`, not in `session.url`, not in
an exception message. A session is read-only by default and refuses to write
by name; open it with `writing=True` to put objects.

Methods, and the Rust function each one calls:

    dataspaces        retp_client::list_dataspaces
    resources         retp_client::list_resources
    get_object(s)     retp_client::get_data_objects, then the registry
    put_objects       energyml_etp_store::push::push (see DESIGN.md, section 9)
    push_epc          energyml_etp_store::push::push, from an EPC file
    array_metadata    retp_client::arrays::array_metadata
    read_array        retp_client::arrays::read_arrays / read_regions
    ping, close       Remote::ping, Remote::close

A failure is an exception of `retp.errors`; an unsupported case raises
`retp.Unsupported` and names it. A call never returns an empty result to mean
"not supported" or "not found".
"""

from ._core import Session

__all__ = ["Session"]
