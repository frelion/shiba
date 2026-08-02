use core::fmt;

#[derive(Debug)]
pub enum M2Error {
    CoordinateOutOfRange(&'static str),
    CountOutOfRange,
    DuplicateInputSequence(u64),
    DuplicateSourceRow(i64),
    EmptyTransaction,
    IdentityConflict,
    MissingSourceRow,
    OutOfOrder,
    Postgres(postgres::Error),
    SourceBindingMissing,
    SourceInvalidated,
    SlotGenerationMismatch,
}

impl fmt::Display for M2Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CoordinateOutOfRange(field) => {
                write!(formatter, "{field} exceeds PostgreSQL bigint range")
            }
            Self::CountOutOfRange => {
                formatter.write_str("count is outside the non-negative PostgreSQL bigint range")
            }
            Self::DuplicateInputSequence(value) => {
                write!(formatter, "duplicate input sequence {value}")
            }
            Self::DuplicateSourceRow(value) => write!(formatter, "duplicate source row {value}"),
            Self::EmptyTransaction => formatter.write_str("M2 requires at least one INSERT"),
            Self::IdentityConflict => {
                formatter.write_str("source coordinate has a different transaction identity")
            }
            Self::MissingSourceRow => formatter.write_str("source change targets no applied row"),
            Self::OutOfOrder => formatter.write_str("commit LSN is not strictly increasing"),
            Self::Postgres(error) => write!(formatter, "PostgreSQL transaction failed: {error}"),
            Self::SourceBindingMissing => formatter.write_str("source binding is missing"),
            Self::SourceInvalidated => formatter.write_str("source binding is invalidated"),
            Self::SlotGenerationMismatch => {
                formatter.write_str("slot generation does not match source continuation")
            }
        }
    }
}

impl std::error::Error for M2Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Postgres(error) => Some(error),
            _ => None,
        }
    }
}

impl From<postgres::Error> for M2Error {
    fn from(error: postgres::Error) -> Self {
        Self::Postgres(error)
    }
}
