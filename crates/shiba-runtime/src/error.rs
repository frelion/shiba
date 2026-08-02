use core::fmt;

#[derive(Debug)]
pub enum M2Error {
    BootstrapBatchLimitExceeded,
    BootstrapBatchOutOfOrder,
    BootstrapIdentityConflict,
    BootstrapMissing,
    BootstrapRowsOutOfOrder,
    CoordinateOutOfRange(&'static str),
    DuplicateInputSequence(u64),
    DuplicateSourceRow(i64),
    EmptyTransaction,
    EmptyBootstrapBatch,
    IdentityConflict,
    InvalidOperatorDefinition,
    InvalidBootstrapPhase,
    InvalidBootstrapFence,
    InvalidSourceRowState,
    MissingSourceRow,
    MissingSourceOperator,
    Operator(shiba_operator::OperatorError),
    OutOfOrder,
    Postgres(postgres::Error),
    SourceBindingMissing,
    SourceInvalidated,
    SlotGenerationMismatch,
    TransactionLimitExceeded,
}

impl fmt::Display for M2Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BootstrapBatchLimitExceeded => {
                formatter.write_str("bootstrap batch exceeds the 10,000-row limit")
            }
            Self::BootstrapBatchOutOfOrder => {
                formatter.write_str("bootstrap batch ordinal is not the next expected ordinal")
            }
            Self::BootstrapIdentityConflict => {
                formatter.write_str("bootstrap identity or replay digest conflicts")
            }
            Self::BootstrapMissing => formatter.write_str("source bootstrap authority is missing"),
            Self::BootstrapRowsOutOfOrder => {
                formatter.write_str("bootstrap row keys are not strictly increasing")
            }
            Self::CoordinateOutOfRange(field) => {
                write!(formatter, "{field} exceeds PostgreSQL bigint range")
            }
            Self::DuplicateInputSequence(value) => {
                write!(formatter, "duplicate input sequence {value}")
            }
            Self::DuplicateSourceRow(value) => write!(formatter, "duplicate source row {value}"),
            Self::EmptyTransaction => formatter.write_str("M2 requires at least one INSERT"),
            Self::EmptyBootstrapBatch => formatter.write_str("bootstrap batch cannot be empty"),
            Self::IdentityConflict => {
                formatter.write_str("source coordinate has a different transaction identity")
            }
            Self::InvalidOperatorDefinition => {
                formatter.write_str("durable operator definition is invalid")
            }
            Self::InvalidBootstrapPhase => {
                formatter.write_str("bootstrap lifecycle phase rejects this operation")
            }
            Self::InvalidBootstrapFence => {
                formatter.write_str("terminal end LSN does not cover the bootstrap catch-up fence")
            }
            Self::InvalidSourceRowState => {
                formatter.write_str("durable source row state violates its value shape")
            }
            Self::MissingSourceRow => formatter.write_str("source change targets no applied row"),
            Self::MissingSourceOperator => formatter.write_str("source has no registered operator"),
            Self::Operator(error) => write!(formatter, "operator evaluation failed: {error}"),
            Self::OutOfOrder => formatter.write_str("commit LSN is not strictly increasing"),
            Self::Postgres(error) => write!(formatter, "PostgreSQL transaction failed: {error}"),
            Self::SourceBindingMissing => formatter.write_str("source binding is missing"),
            Self::SourceInvalidated => formatter.write_str("source binding is invalidated"),
            Self::SlotGenerationMismatch => {
                formatter.write_str("slot generation does not match source continuation")
            }
            Self::TransactionLimitExceeded => {
                formatter.write_str("source transaction exceeds the 10,000-change limit")
            }
        }
    }
}

impl std::error::Error for M2Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Operator(error) => Some(error),
            Self::Postgres(error) => Some(error),
            _ => None,
        }
    }
}

impl From<shiba_operator::OperatorError> for M2Error {
    fn from(error: shiba_operator::OperatorError) -> Self {
        Self::Operator(error)
    }
}

impl From<postgres::Error> for M2Error {
    fn from(error: postgres::Error) -> Self {
        Self::Postgres(error)
    }
}
