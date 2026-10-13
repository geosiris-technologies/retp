//! What this client offers, against what this workspace can actually read.
//!
//! `EVERY_RELEASE` is a list of release prefixes written by hand in
//! `offer.rs`, and the registry of `energyml-schemas` is the list of releases
//! that exist. Two lists of the same fact, and until this file was written
//! nothing compared them - so one had drifted without a word: it offered
//! `prodml21.*`, which no crate here generates, and left out `prodml20.*`,
//! which one does.
//!
//! Neither half fails loudly. A client that offers a release it cannot read
//! receives objects it must then refuse; one that omits a release it can read
//! is simply never sent them. Both look exactly like a server with nothing to
//! give.

use std::collections::BTreeSet;

/// The prefix of a qualified type - `prodml23` of `prodml23.FluidSystem`.
///
/// Taken from a real qualified type rather than rebuilt from `package` and
/// `version`, because rebuilding it would be a second implementation of
/// `Identity::qualified_type` and could agree with itself while disagreeing
/// with what goes on the wire. RESQML 2.0.1 is the case that punishes a
/// rebuild: its prefix is `resqml20`, not `resqml201`.
fn prefixes_of_the_registry() -> BTreeSet<String> {
    energyml_schemas::all()
        .top_level_types()
        .filter_map(|info| info.qualified_type)
        .filter_map(|qualified| qualified.split_once('.').map(|(prefix, _)| prefix.to_string()))
        .collect()
}

#[test]
fn every_release_offers_exactly_what_this_workspace_reads() {
    let offered: BTreeSet<String> = retp_client::EVERY_RELEASE
        .iter()
        .map(|entry| {
            entry
                .strip_suffix(".*")
                .unwrap_or_else(|| panic!("`{entry}` should be a `<release>.*` wildcard (§5.3.1)"))
                .to_string()
        })
        .collect();
    let known = prefixes_of_the_registry();

    let offered_but_unreadable: Vec<_> = offered.difference(&known).collect();
    let readable_but_unoffered: Vec<_> = known.difference(&offered).collect();

    assert!(
        offered_but_unreadable.is_empty(),
        "offered to a server and unreadable here: {offered_but_unreadable:?} - \
         a release left in the list after it was renamed, or never generated"
    );
    assert!(
        readable_but_unoffered.is_empty(),
        "generated here and never asked for: {readable_but_unoffered:?} - \
         add it to `EVERY_RELEASE`, which is the list a client sends"
    );
}
