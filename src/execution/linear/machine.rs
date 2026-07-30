use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LinearKind {
    Scan,
    Filter,
    Project,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ScanPhase {
    Bootstrap = 1,
    SnapshotFrontier = 2,
    Data = 3,
    SourceFrontier = 4,
}

impl ScanPhase {
    pub(super) fn decode(raw: i16) -> Result<Self, String> {
        match PhaseCode::active(raw)?.value() {
            1 => Ok(Self::Bootstrap),
            2 => Ok(Self::SnapshotFrontier),
            3 => Ok(Self::Data),
            4 => Ok(Self::SourceFrontier),
            phase => Err(format!("Scan continuation has unknown phase {phase}")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ScanContinuation {
    pub(super) phase: ScanPhase,
    pub(super) input_stream_id: i64,
    pub(super) input_chunk_seq: Option<i64>,
    pub(super) next_row_ordinal: Option<i64>,
    pub(super) next_bootstrap_seq: Option<i64>,
    pub(super) pending_frontier_lsn: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct TransformContinuation {
    pub(super) input_stream_id: i64,
    pub(super) input_chunk_seq: i64,
    pub(super) next_row_ordinal: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct TransformFacts {
    pub(super) usage: WorkUsage,
    pub(super) first_ordinal: i64,
    pub(super) last_ordinal: i64,
    pub(super) output: OutputFacts,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BootstrapFacts {
    pub(super) usage: WorkUsage,
    pub(super) first_sequence: Option<i64>,
    pub(super) last_sequence: Option<i64>,
    pub(super) output: OutputFacts,
}
