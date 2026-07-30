use crate::planner::{WorkBudget, WorkUsage};

/// The semantic phase of a durable operator primitive.
///
/// Concrete operators may have more detailed internal phases, but every
/// bounded database action belongs to one of these four protocol phases. The
/// phase is intentionally metadata supplied to validation rather than a field
/// on `PrimitiveFacts`: existing SQL result shapes and Rust struct literals
/// remain compatible while callers still have to validate the phase boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KernelPhase {
    Admit,
    Process,
    Drain,
    Frontier,
}

/// Whether a committed kernel transition leaves durable work to resume.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KernelCompletion {
    Continue,
    Finished,
}

impl KernelCompletion {
    pub(crate) const fn has_continuation(self) -> bool {
        matches!(self, Self::Continue)
    }
}

/// Input durably admitted since the last forwarded input frontier.
///
/// This is common checkpoint state, not operator-typed data. Aggregate,
/// Window, and TopN use it to schedule geometrically spaced Drain epochs
/// without tying a Drain continuation to consumed stream chunks.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AdmissionProgress {
    rows: u64,
    bytes: u64,
}

impl AdmissionProgress {
    pub(crate) const fn new(rows: u64, bytes: u64) -> Self {
        Self { rows, bytes }
    }

    pub(crate) const fn rows(self) -> u64 {
        self.rows
    }

    pub(crate) const fn bytes(self) -> u64 {
        self.bytes
    }

    pub(crate) const fn is_empty(self) -> bool {
        self.rows == 0 && self.bytes == 0
    }

    pub(crate) fn record(
        self,
        usage: WorkUsage,
        row_quantum: usize,
        row_interval_cap: usize,
        byte_quantum: usize,
        byte_interval_cap: usize,
    ) -> Result<(Self, bool), String> {
        if usage.input_rows == 0 {
            return Err("admission made no input-row progress".into());
        }
        if usage.input_bytes == 0 {
            return Err("admission made no input-byte progress".into());
        }
        let maximum = i64::MAX as u64;
        let next = Self {
            rows: self.rows.saturating_add(usage.input_rows).min(maximum),
            bytes: self.bytes.saturating_add(usage.input_bytes).min(maximum),
        };
        let row_drain = crossed_admission_threshold(
            self.rows,
            next.rows,
            row_quantum,
            row_interval_cap,
            "row",
        )?;
        let byte_drain = crossed_admission_threshold(
            self.bytes,
            next.bytes,
            byte_quantum,
            byte_interval_cap,
            "byte",
        )?;
        Ok((next, row_drain || byte_drain))
    }
}

/// Thresholds are `quantum, 2*quantum, 4*quantum, ...`, capped at
/// `interval_cap`; after the cap they continue at fixed `interval_cap`
/// intervals. This avoids repeatedly rebuilding hot state for every small
/// input page while retaining a hard bound on unreconciled work.
fn crossed_admission_threshold(
    before: u64,
    after: u64,
    quantum: usize,
    interval_cap: usize,
    dimension: &str,
) -> Result<bool, String> {
    let quantum =
        u64::try_from(quantum).map_err(|_| format!("admission {dimension} quantum exceeds u64"))?;
    let interval_cap = u64::try_from(interval_cap)
        .map_err(|_| format!("admission {dimension} interval cap exceeds u64"))?;
    if quantum == 0 || interval_cap == 0 || quantum > interval_cap {
        return Err(format!("invalid admission {dimension} policy"));
    }
    if after < before || after > i64::MAX as u64 {
        return Err(format!("invalid admission {dimension} progress"));
    }
    if before == i64::MAX as u64 {
        return Ok(true);
    }

    let next = if before < interval_cap {
        let mut threshold = quantum;
        while threshold <= before && threshold < interval_cap {
            threshold = threshold.saturating_mul(2).min(interval_cap);
        }
        Some(threshold)
    } else {
        before
            .checked_div(interval_cap)
            .and_then(|epoch| epoch.checked_add(1))
            .and_then(|epoch| epoch.checked_mul(interval_cap))
            .filter(|threshold| *threshold <= i64::MAX as u64)
    };
    Ok(next.is_some_and(|threshold| after >= threshold) || after == i64::MAX as u64)
}

/// A durable position inside one immutable input chunk.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InputPosition {
    pub(crate) stream_id: i64,
    pub(crate) chunk_seq: i64,
    pub(crate) row_ordinal: i64,
}

impl InputPosition {
    pub(crate) fn new(stream_id: i64, chunk_seq: i64, row_ordinal: i64) -> Result<Self, String> {
        if stream_id <= 0 || chunk_seq <= 0 || row_ordinal < 0 {
            return Err("input position contains an invalid identifier".into());
        }
        Ok(Self {
            stream_id,
            chunk_seq,
            row_ordinal,
        })
    }
}

impl WorkUsage {
    pub(crate) fn validate(self, budget: WorkBudget) -> Result<(), String> {
        validate_dimension(
            self.input_rows,
            self.input_bytes,
            budget.max_input_rows,
            budget.max_input_bytes,
            "input",
        )?;
        validate_dimension(
            self.output_rows,
            self.output_bytes,
            budget.max_output_rows,
            budget.max_output_bytes,
            "output",
        )
    }
}

/// Summary returned by a bounded keyset page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PageFacts {
    pub(crate) usage: WorkUsage,
    pub(crate) last_row_id: Option<i64>,
    pub(crate) complete: bool,
}

impl PageFacts {
    pub(crate) fn validate(self, budget: WorkBudget) -> Result<(), String> {
        self.usage.validate(budget)?;
        if self.usage.input_rows == 0 && self.last_row_id.is_some() {
            return Err("empty primitive page returned a row cursor".into());
        }
        if self.usage.input_rows > 0 && self.last_row_id.is_none() && !self.complete {
            return Err("partial primitive page omitted its row cursor".into());
        }
        Ok(())
    }
}

/// Mutation counts that every set primitive must report back to Rust.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PrimitiveFacts {
    pub(crate) usage: WorkUsage,
    pub(crate) state_rows: u64,
    pub(crate) continuation_rows: u64,
    pub(crate) output: OutputFacts,
}

/// Output appended by one primitive.
///
/// Frontiers are real chunks even though they contain no effect rows.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum OutputFacts {
    #[default]
    None,
    Data {
        chunk_seq: i64,
    },
    Frontier {
        chunk_seq: i64,
    },
}

impl PrimitiveFacts {
    pub(crate) fn validate(self, budget: WorkBudget) -> Result<(), String> {
        self.usage.validate(budget)?;
        if self.continuation_rows > 1 {
            return Err("operator continuation relation contains more than one row".into());
        }
        match self.output {
            OutputFacts::None if self.usage.output_rows > 0 => {
                return Err("primitive emitted rows without an output chunk".into());
            }
            OutputFacts::Data { chunk_seq } => {
                if chunk_seq <= 0 {
                    return Err("primitive returned a non-positive output chunk".into());
                }
                if self.usage.output_rows == 0 {
                    return Err("data chunk contains no output rows".into());
                }
            }
            OutputFacts::Frontier { chunk_seq } => {
                if chunk_seq <= 0 {
                    return Err("primitive returned a non-positive frontier chunk".into());
                }
                if self.usage.output_rows != 0 || self.usage.output_bytes != 0 {
                    return Err("frontier chunk contains output rows".into());
                }
            }
            OutputFacts::None => {}
        }
        Ok(())
    }

    /// Validates the shared protocol facts in addition to the operator's
    /// SQL-specific checks.
    ///
    /// `PrimitiveFacts` deliberately remains the stable four-field summary
    /// returned by existing primitives. The phase and completion are supplied
    /// by the action planner, which prevents a SQL result column from silently
    /// becoming a second source of lifecycle truth.
    pub(crate) fn validate_protocol(
        self,
        budget: WorkBudget,
        phase: KernelPhase,
        completion: KernelCompletion,
    ) -> Result<(), String> {
        self.validate(budget)?;
        self.validate_continuation(completion.has_continuation())?;

        if !matches!(phase, KernelPhase::Frontier)
            && matches!(self.output, OutputFacts::Frontier { .. })
        {
            return Err("non-frontier primitive emitted a frontier chunk".into());
        }
        if matches!(phase, KernelPhase::Frontier) && matches!(self.output, OutputFacts::Data { .. })
        {
            return Err("frontier primitive emitted a data chunk".into());
        }
        if matches!(self.output, OutputFacts::Frontier { .. })
            && !matches!(completion, KernelCompletion::Finished)
        {
            return Err("frontier output left a continuation behind".into());
        }
        Ok(())
    }

    pub(crate) fn validate_continuation(self, has_continuation: bool) -> Result<(), String> {
        if self.continuation_rows != u64::from(has_continuation) {
            return Err("primitive facts disagree with their continuation row".into());
        }
        Ok(())
    }
}

/// Strict persisted phase representation. Zero always means no continuation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PhaseCode(i16);

impl PhaseCode {
    pub(crate) fn active(value: i16) -> Result<Self, String> {
        if value <= 0 {
            return Err("active operator phase code must be positive".into());
        }
        Ok(Self(value))
    }

    pub(crate) const fn value(self) -> i16 {
        self.0
    }
}

fn validate_dimension(
    rows: u64,
    bytes: u64,
    max_rows: usize,
    max_bytes: usize,
    name: &str,
) -> Result<(), String> {
    let max_rows = u64::try_from(max_rows).map_err(|_| format!("{name} row budget exceeds u64"))?;
    let max_bytes =
        u64::try_from(max_bytes).map_err(|_| format!("{name} byte budget exceeds u64"))?;
    if rows > max_rows {
        return Err(format!("{name} primitive exceeded its row budget"));
    }
    // A single indivisible typed work item is the only byte-budget exception.
    if bytes > max_bytes && rows != 1 {
        return Err(format!("{name} primitive exceeded its byte budget"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget() -> WorkBudget {
        WorkBudget::new(2, 10, 3, 20)
    }

    #[test]
    fn only_one_row_may_exceed_a_byte_budget() {
        WorkUsage {
            input_rows: 1,
            input_bytes: 11,
            output_rows: 1,
            output_bytes: 21,
        }
        .validate(budget())
        .unwrap();

        assert!(WorkUsage {
            input_rows: 2,
            input_bytes: 11,
            ..WorkUsage::default()
        }
        .validate(budget())
        .is_err());
        assert!(WorkUsage {
            output_rows: 4,
            output_bytes: 4,
            ..WorkUsage::default()
        }
        .validate(budget())
        .is_err());
    }

    #[test]
    fn phase_codes_have_no_compatibility_decoder() {
        assert_eq!(PhaseCode::active(3).unwrap().value(), 3);
        assert!(PhaseCode::active(0).is_err());
        assert!(PhaseCode::active(-1).is_err());
    }

    #[test]
    fn input_row_ordinals_are_zero_based() {
        assert_eq!(
            InputPosition::new(1, 2, 0).unwrap(),
            InputPosition {
                stream_id: 1,
                chunk_seq: 2,
                row_ordinal: 0,
            }
        );
        assert!(InputPosition::new(1, 2, -1).is_err());
    }

    #[test]
    fn admission_progress_uses_geometric_thresholds_and_either_dimension() {
        let (progress, drain) = AdmissionProgress::default()
            .record(
                WorkUsage {
                    input_rows: 2,
                    input_bytes: 9,
                    ..WorkUsage::default()
                },
                8,
                32,
                100,
                400,
            )
            .unwrap();
        assert_eq!(progress, AdmissionProgress::new(2, 9));
        assert!(!drain);
        let (progress, drain) = AdmissionProgress::new(7, 99)
            .record(
                WorkUsage {
                    input_rows: 1,
                    input_bytes: 1,
                    ..WorkUsage::default()
                },
                8,
                32,
                100,
                400,
            )
            .unwrap();
        assert_eq!(progress, AdmissionProgress::new(8, 100));
        assert!(drain);
        assert!(progress
            .record(
                WorkUsage {
                    input_rows: 0,
                    input_bytes: 1,
                    ..WorkUsage::default()
                },
                8,
                32,
                100,
                400
            )
            .is_err());
    }

    #[test]
    fn admission_thresholds_become_fixed_intervals_at_the_cap() {
        for (before, after, expected) in [
            (0, 5, false),
            (5, 6, true),
            (6, 11, false),
            (11, 12, true),
            (12, 19, false),
            (19, 20, true),
            (20, 39, false),
            (39, 40, true),
        ] {
            assert_eq!(
                crossed_admission_threshold(before, after, 6, 20, "test").unwrap(),
                expected,
                "{before} -> {after}"
            );
        }
    }

    #[test]
    fn admission_large_pages_trigger_once_and_progress_saturates_safely() {
        assert!(crossed_admission_threshold(7, 31, 8, 32, "test").unwrap());
        assert!(!crossed_admission_threshold(31, 31, 8, 32, "test").unwrap());
        assert!(AdmissionProgress::new(7, 7)
            .record(
                WorkUsage {
                    input_rows: 1,
                    input_bytes: 1,
                    ..WorkUsage::default()
                },
                8,
                32,
                9,
                8,
            )
            .is_err());
        let maximum = i64::MAX as u64;
        let (progress, drain) = AdmissionProgress::new(maximum - 1, maximum - 1)
            .record(
                WorkUsage {
                    input_rows: 10,
                    input_bytes: 10,
                    ..WorkUsage::default()
                },
                8,
                32,
                8,
                32,
            )
            .unwrap();
        assert_eq!(progress, AdmissionProgress::new(maximum, maximum));
        assert!(drain);
        let (_, drain_again) = progress
            .record(
                WorkUsage {
                    input_rows: 1,
                    input_bytes: 1,
                    ..WorkUsage::default()
                },
                8,
                32,
                8,
                32,
            )
            .unwrap();
        assert!(drain_again);
    }

    #[test]
    fn primitive_facts_require_exact_output_metadata() {
        let mut facts = PrimitiveFacts {
            usage: WorkUsage {
                output_rows: 1,
                output_bytes: 8,
                ..WorkUsage::default()
            },
            output: OutputFacts::Data { chunk_seq: 4 },
            ..PrimitiveFacts::default()
        };
        facts.validate(budget()).unwrap();
        facts.output = OutputFacts::None;
        assert!(facts.validate(budget()).is_err());

        PrimitiveFacts {
            output: OutputFacts::Frontier { chunk_seq: 5 },
            ..PrimitiveFacts::default()
        }
        .validate(budget())
        .unwrap();
    }

    #[test]
    fn primitive_protocol_requires_completion_to_match_continuation() {
        PrimitiveFacts {
            continuation_rows: 1,
            ..PrimitiveFacts::default()
        }
        .validate_protocol(budget(), KernelPhase::Admit, KernelCompletion::Continue)
        .unwrap();

        assert!(PrimitiveFacts::default()
            .validate_protocol(budget(), KernelPhase::Process, KernelCompletion::Continue)
            .is_err());
        assert!(PrimitiveFacts {
            continuation_rows: 1,
            ..PrimitiveFacts::default()
        }
        .validate_protocol(budget(), KernelPhase::Drain, KernelCompletion::Finished)
        .is_err());
    }

    #[test]
    fn primitive_protocol_reserves_frontier_output_for_the_frontier_phase() {
        let facts = PrimitiveFacts {
            output: OutputFacts::Frontier { chunk_seq: 5 },
            ..PrimitiveFacts::default()
        };
        facts
            .validate_protocol(budget(), KernelPhase::Frontier, KernelCompletion::Finished)
            .unwrap();
        assert!(facts
            .validate_protocol(budget(), KernelPhase::Drain, KernelCompletion::Finished)
            .is_err());
        assert!(facts
            .validate_protocol(budget(), KernelPhase::Frontier, KernelCompletion::Continue)
            .is_err());
    }

    #[test]
    fn zero_output_continue_is_valid_metadata_progress() {
        PrimitiveFacts {
            continuation_rows: 1,
            state_rows: 1,
            ..PrimitiveFacts::default()
        }
        .validate_protocol(budget(), KernelPhase::Drain, KernelCompletion::Continue)
        .unwrap();
    }
}
