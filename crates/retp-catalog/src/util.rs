//! Small helpers shared by the modules: what a name may be, how a text is
//! quoted in TOML, which words are close to a mistyped one.

/// Device names Windows reserves: a folder called `con` cannot be made there.
const WINDOWS_DEVICES: [&str; 22] = [
    "con", "prn", "aux", "nul", "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8", "com9",
    "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9",
];

/// Whether `name` can be a server name or an alias: `[a-z0-9][a-z0-9._-]{0,62}`,
/// no `..`, no trailing dot, and not a device name Windows reserves.
///
/// A name becomes a folder name, so it never holds a path separator.
pub(crate) fn check_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("a name cannot be empty");
    }
    if name.len() > 63 {
        return Err("a name has at most 63 characters");
    }
    let starts_well = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
    if !starts_well {
        return Err("a name starts with a lowercase letter or a digit");
    }
    let alphabet = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-');
    if !name.chars().all(alphabet) {
        return Err("a name holds lowercase letters, digits, `.`, `_` and `-` only");
    }
    if name.contains("..") {
        return Err("a name cannot contain `..`");
    }
    if name.ends_with('.') {
        return Err("a name cannot end with a dot");
    }
    let stem = name.split('.').next().unwrap_or(name);
    if WINDOWS_DEVICES.contains(&stem) {
        return Err("this is a device name Windows reserves");
    }
    Ok(())
}

/// A TOML basic string: quoted, with what must be escaped escaped.
pub(crate) fn quote(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04X}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// The Levenshtein distance between two words, by characters.
fn distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut current = vec![i + 1];
        for (j, cb) in b.iter().enumerate() {
            let substitution = previous[j] + usize::from(ca != *cb);
            current.push(substitution.min(previous[j + 1] + 1).min(current[j] + 1));
        }
        previous = current;
    }
    previous[b.len()]
}

/// Up to five of `candidates` that look like `word`: a distance of two at
/// most, or one starting with the other (three characters at least).
pub(crate) fn near<I: IntoIterator<Item = String>>(word: &str, candidates: I) -> Vec<String> {
    let mut found: Vec<(usize, String)> = candidates
        .into_iter()
        .filter_map(|candidate| {
            let d = distance(word, &candidate);
            let prefix = word.chars().count() >= 3
                && (candidate.starts_with(word)
                    || (candidate.chars().count() >= 3 && word.starts_with(&candidate)));
            (d <= 2 || prefix).then_some((d, candidate))
        })
        .collect();
    found.sort();
    found.dedup();
    found.into_iter().take(5).map(|(_, name)| name).collect()
}

/// `azure-test` as `AZURE_TEST`: a name in the shape of an environment variable.
pub(crate) fn shout(text: &str) -> String {
    text.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

/// Whether a URL carries credentials (`https://user:pw@host/`): the part
/// between `://` and the next `/`, `?` or `#` holds an `@`.
pub(crate) fn has_credentials(url: &str) -> bool {
    let Some((_, rest)) = url.split_once("://") else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    authority.contains('@')
}

/// Whether a word is a WebSocket or HTTP URL.
pub(crate) fn is_url(text: &str) -> bool {
    ["ws://", "wss://", "http://", "https://"]
        .iter()
        .any(|scheme| text.starts_with(scheme))
}
