//! The journal of a push ([`Options::journal`](super::Options::journal)):
//! a file where each island is recorded as it commits, so that a push
//! started again with the same file leaves out what it records.
//!
//! # Its format
//!
//! [JSON Lines](https://jsonlines.org/): one JSON object per line, in UTF-8.
//! The first line says what the journal is for:
//!
//! ```json
//! {"journal":"retp push","version":1,"dataspace":"eml:///dataspace('demo')","package":"5f0c3e8a91b2d4c7","objects":3}
//! ```
//!
//! - `dataspace`: the destination, as a dataspace URI;
//! - `package`: a fingerprint of what the push sends - each object's URI in
//!   the destination, its bytes as written, its arrays' addresses and
//!   dimensions, hashed with 64-bit FNV-1a and written in hexadecimal.
//!   Another package gives another one, and so do another selection of its
//!   objects (`only`), another format (`json`) or another writing of the same
//!   objects by another build;
//! - `objects`: how many objects it covers, for whoever reads the file.
//!
//! Then one line per island, written when it commits, and flushed to the
//! disk before the next island leaves:
//!
//! ```json
//! {"island":0,"committed":["eml:///dataspace('demo')/resqml20.obj_LocalDepth3dCrs(aa000000-0000-4000-8000-000000000001)"]}
//! ```
//!
//! `committed` names the island's objects by their URI in the destination,
//! and is what a push started again reads; `island` is the island's rank in
//! that run's plan, for reading only (another budget makes another plan,
//! and the URIs still hold).
//!
//! # What it refuses
//!
//! A first line for another dataspace or another package refuses the
//! journal by name, before anything is asked of the destination: what it
//! records would be taken for what is there. So does a line that does not
//! read, or a URI that does not parse or names another dataspace. A last
//! line without its end of line was cut while it was written: it is dropped,
//! and its island taken as not committed - it is sent again, which replaces
//! what is there.

use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use energyml_core::identifiers::EtpUri;
use serde_json::{Value, json};

/// What the first line names itself.
const KIND: &str = "retp push";
/// The version of this format.
const VERSION: u64 = 1;

/// A journal open for a push: its file, at its end.
pub(crate) struct Journal {
    file: File,
    path: PathBuf,
}

/// What a journal file holds, read: what it records as committed, and what
/// opening it for a push has to write.
struct Read {
    committed: BTreeSet<String>,
    /// No first line yet: the push writes one.
    fresh: bool,
    /// The text up to its last whole line, when a last line was cut while it
    /// was written: the push writes the file back without it.
    cut: Option<String>,
}

/// Read the journal at `path` for a push into `dataspace` whose fingerprint
/// is `package`, **writing nothing**: what it records as committed, or why it
/// is refused. A file that is not there records nothing.
fn read(path: &Path, dataspace: &str, package: &str) -> Result<Read, String> {
    let shown = path.display();
    let text = match std::fs::read(path) {
        Ok(bytes) => {
            String::from_utf8(bytes).map_err(|_| format!("{shown}: the journal is not UTF-8 text"))?
        }
        Err(why) if why.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(why) => return Err(format!("{shown}: the journal cannot be read: {why}")),
    };
    // What ends with an end of line; a last line without one was cut.
    let whole = match text.rfind('\n') {
        Some(end) => &text[..=end],
        None => "",
    };
    let space = EtpUri::parse(dataspace)
        .map_err(|why| format!("{dataspace} is not a dataspace URI: {why}"))?
        .dataspace;
    let mut committed = BTreeSet::new();
    let mut lines = whole
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty());
    let fresh = match lines.next() {
        None => true,
        Some((n, first)) => {
            let header: Value = serde_json::from_str(first)
                .map_err(|why| format!("{shown}, line {}: not a push journal: {why}", n + 1))?;
            if header["journal"] != KIND || header["version"] != VERSION {
                return Err(format!(
                    "{shown}: not a push journal of version {VERSION} (its first line is {first})"
                ));
            }
            let there = header["dataspace"].as_str().unwrap_or_default();
            if there != dataspace {
                return Err(format!(
                    "{shown}: the journal was written for a push into {there}, and this push goes into {dataspace} - use another journal"
                ));
            }
            let print = header["package"].as_str().unwrap_or_default();
            if print != package {
                return Err(format!(
                    "{shown}: the journal was written for another package (fingerprint {print}, this push's is {package}): \
                 another package, another selection of its objects, another format, or another writing of them - use another journal"
                ));
            }
            false
        }
    };
    for (n, line) in lines {
        let entry: Value = serde_json::from_str(line)
            .map_err(|why| format!("{shown}, line {}: it does not read: {why}", n + 1))?;
        let Some(uris) = entry["committed"].as_array() else {
            return Err(format!("{shown}, line {}: it names no committed objects", n + 1));
        };
        for uri in uris {
            let Some(uri) = uri.as_str() else {
                return Err(format!("{shown}, line {}: {uri} is not a URI in a string", n + 1));
            };
            let parsed =
                EtpUri::parse(uri).map_err(|why| format!("{shown}, line {}: {uri}: {why}", n + 1))?;
            if parsed.dataspace != space || parsed.uuid.is_none() {
                return Err(format!(
                    "{shown}, line {}: {uri} is not an object of {dataspace}",
                    n + 1
                ));
            }
            committed.insert(uri.to_string());
        }
    }
    let cut = (whole.len() != text.len()).then(|| whole.to_string());
    Ok(Read {
        committed,
        fresh,
        cut,
    })
}

/// What the journal at `path` records as committed, for a push into
/// `dataspace` whose fingerprint is `package` - read the way a push reads
/// it, refused for the same reasons, and **nothing written**: no file
/// created, no cut line dropped. What `push::dry_run` leaves out.
pub(crate) fn recorded(path: &Path, dataspace: &str, package: &str) -> Result<BTreeSet<String>, String> {
    read(path, dataspace, package).map(|read| read.committed)
}

impl Journal {
    /// Open the journal at `path` for a push of `objects` objects into
    /// `dataspace` whose fingerprint is `package` ([`fingerprint`]),
    /// creating it when it is not there, and give what it records as
    /// committed ([`recorded`], then the writing: a cut last line dropped,
    /// the first line written to a new journal).
    pub(crate) fn open(
        path: &Path,
        dataspace: &str,
        package: &str,
        objects: usize,
    ) -> Result<(Journal, BTreeSet<String>), String> {
        let Read {
            committed,
            fresh,
            cut,
        } = read(path, dataspace, package)?;
        let shown = path.display();
        let failed = |why: std::io::Error| format!("{shown}: the journal cannot be written: {why}");
        if let Some(whole) = cut {
            std::fs::write(path, whole).map_err(failed)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(failed)?;
        if fresh {
            let header = json!({
                "journal": KIND,
                "version": VERSION,
                "dataspace": dataspace,
                "package": package,
                "objects": objects,
            });
            writeln!(file, "{header}")
                .and_then(|()| file.sync_data())
                .map_err(failed)?;
        }
        Ok((
            Journal {
                file,
                path: path.to_path_buf(),
            },
            committed,
        ))
    }

    /// Record that the island of rank `island` committed, with its objects'
    /// URIs in the destination - on the disk before this returns.
    pub(crate) fn record(&mut self, island: usize, uris: &[String]) -> Result<(), String> {
        let line = json!({ "island": island, "committed": uris });
        writeln!(self.file, "{line}")
            .and_then(|()| self.file.flush())
            .and_then(|()| self.file.sync_data())
            .map_err(|why| format!("{}: {why}", self.path.display()))
    }
}

/// The fingerprint of what a push sends: each object's URI in the
/// destination, its bytes as written, and each of its arrays' owner, path
/// and dimensions - in the order of the URIs, so that the source's own order
/// does not count. 64-bit FNV-1a, which is defined by its constants and not
/// by a version of a library, so that a journal reads the same tomorrow.
pub(crate) fn fingerprint<'x>(
    objects: impl IntoIterator<Item = (&'x str, &'x [u8], Vec<(&'x str, &'x str, &'x [i64])>)>,
) -> String {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    fn eat(hash: &mut u64, bytes: &[u8]) {
        for byte in bytes {
            *hash ^= u64::from(*byte);
            *hash = hash.wrapping_mul(PRIME);
        }
    }
    let mut all: Vec<_> = objects.into_iter().collect();
    all.sort_by(|a, b| a.0.cmp(b.0));
    let mut hash = OFFSET;
    for (uri, bytes, arrays) in all {
        eat(&mut hash, uri.as_bytes());
        eat(&mut hash, &[0]);
        eat(&mut hash, bytes);
        eat(&mut hash, &[0]);
        for (owner, path, dimensions) in arrays {
            eat(&mut hash, owner.as_bytes());
            eat(&mut hash, &[0]);
            eat(&mut hash, path.as_bytes());
            eat(&mut hash, &[0]);
            for dimension in dimensions {
                eat(&mut hash, &dimension.to_le_bytes());
            }
            eat(&mut hash, &[0]);
        }
        eat(&mut hash, &[1]);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::fingerprint;

    /// FNV-1a 64 of the empty input is its offset basis, as its authors
    /// publish it with the 64-bit parameters: the constant is the one named,
    /// not one that resembles it.
    #[test]
    fn the_hash_starts_from_the_fnv_1a_64_offset_basis() {
        // No object: nothing eaten.
        assert_eq!(fingerprint(Vec::new()), "cbf29ce484222325");
    }

    /// The source's order does not count; a byte of an object does.
    #[test]
    fn the_fingerprint_follows_the_objects_not_their_order() {
        let a = ("eml:///dataspace('d')/x.A(1)", b"<a/>".as_slice(), Vec::new());
        let b = ("eml:///dataspace('d')/x.B(2)", b"<b/>".as_slice(), Vec::new());
        let changed = ("eml:///dataspace('d')/x.B(2)", b"<b />".as_slice(), Vec::new());
        assert_eq!(
            fingerprint(vec![a.clone(), b.clone()]),
            fingerprint(vec![b.clone(), a.clone()])
        );
        assert_ne!(fingerprint(vec![a.clone(), b]), fingerprint(vec![a, changed]));
    }
}
