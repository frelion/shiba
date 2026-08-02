use core::fmt;

/// A failed attempt to construct a protocol value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtocolError {
    ZeroValue(&'static str),
    ZeroCommitLsn,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroValue(kind) => write!(formatter, "{kind} must be non-zero"),
            Self::ZeroCommitLsn => formatter.write_str("commit LSN must be non-zero"),
        }
    }
}

impl std::error::Error for ProtocolError {}

/// Explains why two source-scoped coordinates cannot be compared.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScopeMismatch {
    Source,
    SlotGeneration,
}

impl fmt::Display for ScopeMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source => formatter.write_str("source identities differ"),
            Self::SlotGeneration => formatter.write_str("slot generations differ"),
        }
    }
}

impl std::error::Error for ScopeMismatch {}
