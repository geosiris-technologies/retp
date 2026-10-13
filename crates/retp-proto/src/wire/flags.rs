//! The `messageFlags` bit-field of the header (§3.5.4.1 §7, p.57).

use std::ops::BitOr;

/// The flags a message header carries.
///
/// `0x01` and `0x04` were ETP 1.1's and are unused in 1.2; nothing here
/// gives them a name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct Flags(pub i32);

impl Flags {
    /// No flag at all.
    pub const NONE: Flags = Flags(0);
    /// `0x02` - the last message of an action, the "FIN bit". Set on every
    /// single message too: a one-message action is still an action.
    pub const FIN: Flags = Flags(0x02);
    /// `0x08` - the body (and the header extension, if any) is compressed.
    pub const COMPRESSED: Flags = Flags(0x08);
    /// `0x10` - the sender asks for an `Acknowledge`.
    pub const ACK_REQUESTED: Flags = Flags(0x10);
    /// `0x20` - a `MessageHeaderExtension` sits between the header and the body.
    pub const EXTENSION: Flags = Flags(0x20);

    /// Whether every bit of `other` is set.
    pub const fn contains(self, other: Flags) -> bool {
        self.0 & other.0 == other.0
    }

    /// These flags and `other`'s.
    pub const fn with(self, other: Flags) -> Flags {
        Flags(self.0 | other.0)
    }
}

impl BitOr for Flags {
    type Output = Flags;
    fn bitor(self, other: Flags) -> Flags {
        self.with(other)
    }
}

impl std::fmt::Display for Flags {
    /// `FIN|ACK` - the names of the bits set, `-` for none, and any bit this
    /// version does not name in hexadecimal.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names = [
            (Flags::FIN, "FIN"),
            (Flags::COMPRESSED, "COMPRESSED"),
            (Flags::ACK_REQUESTED, "ACK"),
            (Flags::EXTENSION, "EXT"),
        ];
        let mut parts: Vec<String> = names
            .iter()
            .filter(|(flag, _)| self.contains(*flag))
            .map(|(_, n)| n.to_string())
            .collect();
        let unnamed = self.0 & !names.iter().fold(0, |all, (flag, _)| all | flag.0);
        if unnamed != 0 {
            parts.push(format!("{unnamed:#x}"));
        }
        if parts.is_empty() {
            f.write_str("-")
        } else {
            f.write_str(&parts.join("|"))
        }
    }
}
