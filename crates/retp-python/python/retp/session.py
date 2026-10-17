"""A session with an ETP server: its dataspaces, objects and arrays.

    import retp

    with retp.connect("wss://etp.example.org/", token=token) as session:
        space = session.dataspaces()[0]
        listed = session.resources(space["path"])
        objects, errors = session.get_objects([r["uri"] for r in listed["resources"]])
        values, dimensions = session.read_array(uri, "/RESQML/.../points")

`retp.connect` opens it, over an ETP WebSocket or, with `rest=True`, over the
Reservoir DDMS REST API. The token you pass (or the login: a user and a
password, an OAuth 2.0 token endpoint, Azure AD, AWS Cognito) is handed to the
Rust client and kept nowhere else: it is not in `repr(session)`, not in
`session.url`, not in an exception message. A session is read-only by default
and refuses to write by name; open it with `writing=True` to put or delete
objects, create or delete dataspaces, or open a transaction.

Methods, and the Rust function each one calls:

    dataspaces            retp_client::list_dataspaces
    resources             retp_client::list_resources
    supported_types       retp_client::list_supported_types
    get_object(s)         retp_client::get_data_objects, then the registry
    put_objects           energyml_etp_store::push::push (see DESIGN.md, section 9)
    push_epc              energyml_etp_store::push::push, from an EPC file
    delete_objects        retp_client::delete_data_objects
    put_dataspaces        retp_client::put_dataspaces
    delete_dataspaces     retp_client::delete_dataspaces
    start_transaction     retp_client::start_transaction (a `Transaction`)
    commit_transaction    retp_client::commit_transaction
    rollback_transaction  retp_client::rollback_transaction
    array_metadata        retp_client::arrays::array_metadata
    read_array            retp_client::arrays::read_arrays / read_regions
    channel_metadata      retp_client::channels::channel_metadata
    get_ranges            retp_client::channels::get_ranges
    download_epc          energyml_etp_store::pack::pack
    subscribe             retp_client::live::LiveSession::subscribe_to
    ping, close           Remote::ping, Remote::close

A failure is an exception of `retp.errors`; an unsupported case raises
`retp.Unsupported` and names it. A call never returns an empty result to mean
"not supported" or "not found".
"""

from ._core import Session, Transaction

__all__ = ["Session", "Transaction"]
