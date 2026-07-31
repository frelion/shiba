use super::*;

/// What follows one pure candidate/visible Drain epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AfterDrain {
    /// Resume a later row in the same immutable input chunk.
    Admit(InputPosition),
    /// Return to the scheduler after draining an already-consumed input prefix.
    FinishInput,
    /// Forward this pinned frontier after candidate and visible agree.
    Frontier(InputPosition),
}

/// One stable row reference into an operator-owned relation.
///
/// SQL joins this ID back to its typed row to recover the exact ordering keys
/// and collation semantics for the next keyset predicate.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct TopNCursor {
    pub(crate) row_id: Option<i64>,
}

impl TopNCursor {
    pub(super) fn validate(self) -> Result<(), String> {
        if self.row_id.is_some_and(|row_id| row_id <= 0) {
            return Err("TopN cursor is not positive".into());
        }
        Ok(())
    }
}

/// Durable progress through one candidate/visible comparison leg.
///
/// Ordinary pages resume strictly after `row_id`. A multiplicity larger than
/// one effect weight leaves `repeat=true`, so the next bounded page revisits
/// exactly that row and emits another slice.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct TopNDiffCursor {
    pub(crate) row_id: Option<i64>,
    pub(crate) repeat: bool,
}

impl TopNDiffCursor {
    pub(super) fn validate(self) -> Result<(), String> {
        if self.row_id.is_some_and(|row_id| row_id <= 0) {
            return Err("TopN Diff cursor is not positive".into());
        }
        if self.repeat && self.row_id.is_none() {
            return Err("TopN Diff repeat cursor omitted its row".into());
        }
        Ok(())
    }
}

/// Scalar progress through OFFSET, LIMIT, and the optional WITH TIES suffix.
///
/// The boundary ID points at a typed bag row. Its sort values stay in
/// PostgreSQL and are never serialized into a Rust continuation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SelectionProgress {
    pub(crate) cursor: TopNCursor,
    pub(crate) offset_remaining: u64,
    pub(crate) limit_remaining: u64,
    pub(crate) tie_boundary_row_id: Option<i64>,
}

impl SelectionProgress {
    pub(super) fn initial(offset: u64, limit: u64) -> Self {
        Self {
            cursor: TopNCursor::default(),
            offset_remaining: offset,
            limit_remaining: limit,
            tie_boundary_row_id: None,
        }
    }

    fn validate(self, machine: TopNMachine) -> Result<(), String> {
        self.cursor.validate()?;
        if self.tie_boundary_row_id.is_some_and(|row_id| row_id <= 0) {
            return Err("TopN tie boundary is not positive".into());
        }
        if self.offset_remaining > machine.offset || self.limit_remaining > machine.limit {
            return Err("TopN remaining count exceeds its plan".into());
        }
        if !machine.with_ties && self.tie_boundary_row_id.is_some() {
            return Err("TopN without WITH TIES stored a tie boundary".into());
        }
        if self.tie_boundary_row_id.is_some()
            && (self.offset_remaining != 0 || self.limit_remaining != 0)
        {
            return Err("TopN tie boundary was stored before OFFSET and LIMIT completed".into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DiffLeg {
    /// Retract visible rows absent from, or different from, the candidate set.
    Remove,
    /// Publish candidate rows that are not currently visible.
    Add,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopNPhaseKind {
    Admit,
    Select,
    Diff,
    Cleanup,
    Frontier,
}

impl TopNPhaseKind {
    pub(crate) fn code(self) -> PhaseCode {
        let code = match self {
            Self::Admit => ADMIT_PHASE,
            Self::Select => SELECT_PHASE,
            Self::Diff => DIFF_PHASE,
            Self::Cleanup => CLEANUP_PHASE,
            Self::Frontier => FRONTIER_PHASE,
        };
        PhaseCode::active(code).expect("TopN phase codes are positive")
    }

    pub(crate) fn from_code(code: PhaseCode) -> Result<Self, String> {
        match code.value() {
            ADMIT_PHASE => Ok(Self::Admit),
            SELECT_PHASE => Ok(Self::Select),
            DIFF_PHASE => Ok(Self::Diff),
            CLEANUP_PHASE => Ok(Self::Cleanup),
            FRONTIER_PHASE => Ok(Self::Frontier),
            _ => Err("unknown TopN phase code".into()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopNPhase {
    Admit,
    Select {
        generation_id: i64,
        progress: SelectionProgress,
        after_drain: AfterDrain,
    },
    Diff {
        generation_id: i64,
        leg: DiffLeg,
        cursor: TopNDiffCursor,
        after_drain: AfterDrain,
    },
    Cleanup {
        generation_id: i64,
        cursor: TopNCursor,
        after_drain: AfterDrain,
    },
    Frontier,
}

impl TopNPhase {
    pub(crate) fn kind(self) -> TopNPhaseKind {
        match self {
            Self::Admit => TopNPhaseKind::Admit,
            Self::Select { .. } => TopNPhaseKind::Select,
            Self::Diff { .. } => TopNPhaseKind::Diff,
            Self::Cleanup { .. } => TopNPhaseKind::Cleanup,
            Self::Frontier => TopNPhaseKind::Frontier,
        }
    }

    pub(crate) fn code(self) -> PhaseCode {
        self.kind().code()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TopNContinuation {
    pub(crate) input_stream_id: i64,
    pub(crate) input: Option<InputPosition>,
    pub(crate) phase: TopNPhase,
}

/// Immutable plan-local TopN counts. Typed ordering metadata is resolved by
/// the SQL builder and is deliberately absent here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TopNMachine {
    pub(super) limit: u64,
    pub(super) offset: u64,
    pub(super) with_ties: bool,
}

impl TopNMachine {
    pub(crate) const fn new(limit: u64, offset: u64, with_ties: bool) -> Self {
        Self {
            limit,
            offset,
            with_ties,
        }
    }

    pub(crate) fn action(self, continuation: TopNContinuation) -> Result<TopNAction, String> {
        self.validate_continuation(continuation)?;
        Ok(match continuation.phase {
            TopNPhase::Admit => TopNAction::Admit {
                input: continuation
                    .input
                    .ok_or_else(|| "TopN Admit continuation omitted its input".to_string())?,
            },
            TopNPhase::Select {
                generation_id,
                progress,
                ..
            } => TopNAction::SelectCandidates {
                generation_id,
                progress,
            },
            TopNPhase::Diff {
                generation_id,
                leg,
                cursor,
                ..
            } => TopNAction::Diff {
                generation_id,
                leg,
                cursor,
            },
            TopNPhase::Cleanup {
                generation_id,
                cursor,
                ..
            } => TopNAction::Cleanup {
                generation_id,
                cursor,
            },
            TopNPhase::Frontier => TopNAction::ForwardFrontier {
                input: continuation
                    .input
                    .ok_or_else(|| "TopN frontier continuation omitted its input".to_string())?,
            },
        })
    }

    /// Applies the measured result of one committed database primitive.
    ///
    /// A blocked or aborted attempt leaves the old continuation authoritative,
    /// so fanout backpressure and crashes both replay the exact same action.
    pub(crate) fn apply(
        self,
        continuation: TopNContinuation,
        result: TopNActionResult,
        budget: WorkBudget,
    ) -> Result<TopNTransition, String> {
        let expected = self.action(continuation)?;
        match (expected, result) {
            (TopNAction::Admit { input }, TopNActionResult::Admitted(admitted)) => {
                self.apply_admission(input, admitted, budget)
            }
            (
                TopNAction::SelectCandidates {
                    generation_id,
                    progress,
                },
                TopNActionResult::Selected(selected),
            ) => self.apply_selection(continuation, generation_id, progress, selected, budget),
            (
                TopNAction::Diff {
                    generation_id,
                    leg,
                    cursor,
                },
                TopNActionResult::Diffed(page),
            ) => self.apply_diff(continuation, generation_id, leg, cursor, page, budget),
            (
                TopNAction::Cleanup {
                    generation_id,
                    cursor,
                },
                TopNActionResult::Cleaned(page),
            ) => self.apply_cleanup(continuation, generation_id, cursor, page, budget),
            (TopNAction::ForwardFrontier { .. }, TopNActionResult::FrontierForwarded(facts)) => {
                self.apply_frontier(facts, budget)
            }
            _ => Err("database returned facts for another TopN phase".into()),
        }
    }

    fn validate_continuation(self, continuation: TopNContinuation) -> Result<(), String> {
        if continuation.input_stream_id <= 0 {
            return Err("TopN continuation has an invalid input stream".into());
        }
        match continuation.phase {
            TopNPhase::Admit | TopNPhase::Frontier => {
                let input = continuation
                    .input
                    .ok_or_else(|| "TopN input phase omitted its cursor".to_string())?;
                validate_input(input)?;
                if input.stream_id != continuation.input_stream_id {
                    return Err("TopN input phase changed its stream".into());
                }
                if matches!(continuation.phase, TopNPhase::Frontier) && input.row_ordinal != 0 {
                    return Err("TopN frontier continuation has a row cursor".into());
                }
            }
            TopNPhase::Select {
                generation_id,
                progress,
                after_drain,
            } => {
                validate_generation_id(generation_id)?;
                progress.validate(self)?;
                if self.limit == 0 {
                    return Err("zero-limit TopN cannot persist a selection phase".into());
                }
                if continuation.input.is_some() {
                    return Err("TopN Drain continuation retained an input cursor".into());
                }
                validate_after_drain(continuation.input_stream_id, after_drain)?;
            }
            TopNPhase::Diff {
                generation_id,
                cursor,
                after_drain,
                ..
            } => {
                validate_generation_id(generation_id)?;
                cursor.validate()?;
                if continuation.input.is_some() {
                    return Err("TopN Drain continuation retained an input cursor".into());
                }
                validate_after_drain(continuation.input_stream_id, after_drain)?;
            }
            TopNPhase::Cleanup {
                generation_id,
                cursor,
                after_drain,
            } => {
                validate_generation_id(generation_id)?;
                cursor.validate()?;
                if continuation.input.is_some() {
                    return Err("TopN Drain continuation retained an input cursor".into());
                }
                validate_after_drain(continuation.input_stream_id, after_drain)?;
            }
        }
        Ok(())
    }

    fn apply_admission(
        self,
        input: InputPosition,
        admitted: TopNAdmission,
        budget: WorkBudget,
    ) -> Result<TopNTransition, String> {
        admitted.facts.validate(budget)?;
        validate_no_external_output(admitted.facts)?;
        if admitted.facts.usage.input_rows == 0 {
            return Err("TopN admission made no bounded input progress".into());
        }
        let next = match admitted.target {
            TopNAdmissionTarget::Continue(next_input) => {
                validate_input(next_input)?;
                if next_input.stream_id != input.stream_id
                    || next_input.chunk_seq != input.chunk_seq
                    || next_input.row_ordinal <= input.row_ordinal
                {
                    return Err("TopN admission continuation did not advance its page".into());
                }
                Some(TopNContinuation {
                    input_stream_id: input.stream_id,
                    input: Some(next_input),
                    phase: TopNPhase::Admit,
                })
            }
            TopNAdmissionTarget::Drain {
                generation_id,
                after_drain,
            } => {
                validate_generation_id(generation_id)?;
                validate_after_drain(input.stream_id, after_drain)?;
                let phase = if self.limit == 0 {
                    TopNPhase::Diff {
                        generation_id,
                        leg: DiffLeg::Remove,
                        cursor: TopNDiffCursor::default(),
                        after_drain,
                    }
                } else {
                    TopNPhase::Select {
                        generation_id,
                        progress: SelectionProgress::initial(self.offset, self.limit),
                        after_drain,
                    }
                };
                Some(TopNContinuation {
                    input_stream_id: input.stream_id,
                    input: None,
                    phase,
                })
            }
            TopNAdmissionTarget::Idle => None,
        };
        if let Some(next) = next {
            self.validate_continuation(next)?;
        }
        Ok(TopNTransition::Committed {
            continuation: next,
            facts: admitted.facts,
        })
    }

    fn apply_selection(
        self,
        continuation: TopNContinuation,
        generation_id: i64,
        previous: SelectionProgress,
        selected: TopNSelection,
        budget: WorkBudget,
    ) -> Result<TopNTransition, String> {
        selected.page.validate(previous.cursor, budget, false)?;
        selected.progress.validate(self)?;
        if !selected.page.complete && selected.progress.cursor.row_id != selected.page.last_row_id {
            return Err("TopN selection cursor disagrees with its bounded page".into());
        }
        validate_selection_progress(self, previous, selected.progress, selected.page.complete)?;
        let after_drain = phase_after_drain(continuation.phase)?;

        let next_phase = if selected.page.complete {
            TopNPhase::Diff {
                generation_id,
                leg: DiffLeg::Remove,
                cursor: TopNDiffCursor::default(),
                after_drain,
            }
        } else {
            TopNPhase::Select {
                generation_id,
                progress: selected.progress,
                after_drain,
            }
        };
        let next = TopNContinuation {
            input_stream_id: continuation.input_stream_id,
            input: None,
            phase: next_phase,
        };
        Ok(TopNTransition::Committed {
            continuation: Some(next),
            facts: selected.page.facts,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_diff(
        self,
        continuation: TopNContinuation,
        generation_id: i64,
        leg: DiffLeg,
        cursor: TopNDiffCursor,
        page: TopNDiffPage,
        budget: WorkBudget,
    ) -> Result<TopNTransition, String> {
        page.validate(cursor, budget)?;
        let after_drain = phase_after_drain(continuation.phase)?;
        let next_phase = if !page.complete {
            TopNPhase::Diff {
                generation_id,
                leg,
                cursor: TopNDiffCursor {
                    row_id: page.last_row_id,
                    repeat: page.repeat_cursor,
                },
                after_drain,
            }
        } else {
            match leg {
                DiffLeg::Remove => TopNPhase::Diff {
                    generation_id,
                    leg: DiffLeg::Add,
                    cursor: TopNDiffCursor::default(),
                    after_drain,
                },
                DiffLeg::Add => TopNPhase::Cleanup {
                    generation_id,
                    cursor: TopNCursor::default(),
                    after_drain,
                },
            }
        };
        let next = TopNContinuation {
            input_stream_id: continuation.input_stream_id,
            input: None,
            phase: next_phase,
        };
        Ok(TopNTransition::Committed {
            continuation: Some(next),
            facts: page.facts,
        })
    }

    fn apply_cleanup(
        self,
        continuation: TopNContinuation,
        generation_id: i64,
        cursor: TopNCursor,
        page: TopNPage,
        budget: WorkBudget,
    ) -> Result<TopNTransition, String> {
        page.validate(cursor, budget, false)?;
        let after_drain = phase_after_drain(continuation.phase)?;
        let next = if !page.complete {
            Some(TopNContinuation {
                input_stream_id: continuation.input_stream_id,
                input: None,
                phase: TopNPhase::Cleanup {
                    generation_id,
                    cursor: TopNCursor {
                        row_id: page.last_row_id,
                    },
                    after_drain,
                },
            })
        } else {
            match after_drain {
                AfterDrain::Admit(next_input) => Some(TopNContinuation {
                    input_stream_id: continuation.input_stream_id,
                    input: Some(next_input),
                    phase: TopNPhase::Admit,
                }),
                AfterDrain::FinishInput => None,
                AfterDrain::Frontier(frontier) => Some(TopNContinuation {
                    input_stream_id: continuation.input_stream_id,
                    input: Some(frontier),
                    phase: TopNPhase::Frontier,
                }),
            }
        };
        Ok(TopNTransition::Committed {
            continuation: next,
            facts: page.facts,
        })
    }

    fn apply_frontier(
        self,
        facts: PrimitiveFacts,
        budget: WorkBudget,
    ) -> Result<TopNTransition, String> {
        facts.validate_protocol(budget, KernelPhase::Frontier, KernelCompletion::Finished)?;
        if !matches!(facts.output, OutputFacts::Frontier { .. })
            || facts.usage.input_rows != 0
            || facts.usage.input_bytes != 0
        {
            return Err("TopN frontier commit is inconsistent".into());
        }
        Ok(TopNTransition::Committed {
            continuation: None,
            facts,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopNAction {
    Admit {
        input: InputPosition,
    },
    /// Selects a bounded keyset page directly into the typed candidate
    /// relation. The cursor row supplies all typed sort values inside SQL.
    SelectCandidates {
        generation_id: i64,
        progress: SelectionProgress,
    },
    /// Reconciles typed candidate and visible relations. Emission and the
    /// matching visible-state mutation are one database transaction.
    Diff {
        generation_id: i64,
        leg: DiffLeg,
        cursor: TopNDiffCursor,
    },
    Cleanup {
        generation_id: i64,
        cursor: TopNCursor,
    },
    ForwardFrontier {
        input: InputPosition,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TopNAdmission {
    pub(crate) facts: PrimitiveFacts,
    pub(crate) target: TopNAdmissionTarget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopNAdmissionTarget {
    Continue(InputPosition),
    Drain {
        generation_id: i64,
        after_drain: AfterDrain,
    },
    Idle,
}

/// Facts from a bounded candidate, diff, or cleanup page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TopNPage {
    pub(crate) facts: PrimitiveFacts,
    pub(crate) last_row_id: Option<i64>,
    pub(crate) complete: bool,
}

/// Facts from one bounded primary-side comparison page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TopNDiffPage {
    pub(crate) facts: PrimitiveFacts,
    pub(crate) last_row_id: Option<i64>,
    pub(crate) complete: bool,
    pub(crate) repeat_cursor: bool,
}

impl TopNDiffPage {
    fn validate(self, previous: TopNDiffCursor, budget: WorkBudget) -> Result<(), String> {
        self.facts.validate(budget)?;
        PageFacts {
            usage: self.facts.usage,
            last_row_id: self.last_row_id,
            complete: self.complete,
        }
        .validate(budget)?;
        if self.last_row_id.is_some_and(|row_id| row_id <= 0) {
            return Err("TopN Diff page returned a non-positive cursor".into());
        }
        if self.repeat_cursor && (self.complete || self.last_row_id.is_none()) {
            return Err("TopN Diff residual has no resumable row".into());
        }
        if !self.complete {
            if self.facts.usage.input_rows == 0 || self.last_row_id.is_none() {
                return Err("partial TopN Diff page made no resumable progress".into());
            }
            if self.last_row_id < previous.row_id
                || (self.last_row_id == previous.row_id && !previous.repeat)
            {
                return Err("TopN Diff page moved its cursor backwards".into());
            }
        }
        if matches!(self.facts.output, OutputFacts::Frontier { .. }) {
            return Err("TopN Diff emitted a frontier".into());
        }
        if self.facts.usage.output_rows > self.facts.usage.input_rows {
            return Err("TopN Diff emitted more effects than rows compared".into());
        }
        Ok(())
    }
}

impl TopNPage {
    fn validate(
        self,
        previous: TopNCursor,
        budget: WorkBudget,
        permits_output: bool,
    ) -> Result<(), String> {
        self.facts.validate(budget)?;
        PageFacts {
            usage: self.facts.usage,
            last_row_id: self.last_row_id,
            complete: self.complete,
        }
        .validate(budget)?;
        if self.last_row_id.is_some_and(|row_id| row_id <= 0) {
            return Err("TopN page returned a non-positive cursor".into());
        }
        if !self.complete {
            if self.facts.usage.input_rows == 0 || self.last_row_id.is_none() {
                return Err("partial TopN page made no resumable progress".into());
            }
            // A multiplicity larger than bigint is emitted in bigint-sized
            // slices. The durable row ID deliberately stays unchanged until
            // the final slice commits.
            if self.last_row_id < previous.row_id {
                return Err("TopN page moved its cursor backwards".into());
            }
        }
        if permits_output {
            if matches!(self.facts.output, OutputFacts::Frontier { .. }) {
                return Err("TopN diff emitted a frontier".into());
            }
            if self.facts.usage.output_rows > self.facts.usage.input_rows {
                return Err("TopN diff emitted more effects than rows compared".into());
            }
        } else {
            validate_no_external_output(self.facts)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TopNSelection {
    pub(crate) page: TopNPage,
    pub(crate) progress: SelectionProgress,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopNActionResult {
    Admitted(TopNAdmission),
    Selected(TopNSelection),
    Diffed(TopNDiffPage),
    Cleaned(TopNPage),
    FrontierForwarded(PrimitiveFacts),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopNTransition {
    Committed {
        continuation: Option<TopNContinuation>,
        facts: PrimitiveFacts,
    },
}

pub(super) fn phase_after_drain(phase: TopNPhase) -> Result<AfterDrain, String> {
    match phase {
        TopNPhase::Select { after_drain, .. }
        | TopNPhase::Diff { after_drain, .. }
        | TopNPhase::Cleanup { after_drain, .. } => Ok(after_drain),
        TopNPhase::Admit | TopNPhase::Frontier => {
            Err("TopN phase has no Drain completion target".into())
        }
    }
}

pub(super) fn validate_selection_progress(
    machine: TopNMachine,
    previous: SelectionProgress,
    next: SelectionProgress,
    complete: bool,
) -> Result<(), String> {
    if next.offset_remaining > previous.offset_remaining
        || next.limit_remaining > previous.limit_remaining
    {
        return Err("TopN selection counters moved backwards".into());
    }
    if next.offset_remaining > 0
        && (next.limit_remaining != previous.limit_remaining
            || next.tie_boundary_row_id != previous.tie_boundary_row_id)
    {
        return Err("TopN selected rows before OFFSET completed".into());
    }
    if let Some(boundary) = previous.tie_boundary_row_id {
        if next.tie_boundary_row_id != Some(boundary) {
            return Err("TopN changed its persisted tie boundary".into());
        }
    }
    if !complete && next.cursor.row_id.is_none() {
        return Err("resumable TopN selection omitted its keyset cursor".into());
    }
    if !complete
        && machine.with_ties
        && next.limit_remaining == 0
        && next.tie_boundary_row_id.is_none()
    {
        return Err("resumable WITH TIES selection omitted its boundary row".into());
    }
    if !complete && !machine.with_ties && next.limit_remaining == 0 {
        return Err("TopN without ties continued after LIMIT completed".into());
    }
    Ok(())
}

pub(super) fn validate_input(input: InputPosition) -> Result<(), String> {
    if input.stream_id <= 0 || input.chunk_seq <= 0 || input.row_ordinal < 0 {
        return Err("TopN input position is invalid".into());
    }
    Ok(())
}

pub(super) fn validate_generation_id(generation_id: i64) -> Result<(), String> {
    if generation_id <= 0 {
        return Err("TopN candidate generation id is not positive".into());
    }
    Ok(())
}

pub(super) fn validate_after_drain(input_stream_id: i64, after: AfterDrain) -> Result<(), String> {
    if input_stream_id <= 0 {
        return Err("TopN Drain target has an invalid input stream".into());
    }
    match after {
        AfterDrain::Admit(next) => {
            validate_input(next)?;
            if next.stream_id != input_stream_id {
                return Err("TopN Drain target changed its input stream".into());
            }
        }
        AfterDrain::Frontier(frontier) => {
            validate_input(frontier)?;
            if frontier.stream_id != input_stream_id || frontier.row_ordinal != 0 {
                return Err("TopN frontier target is invalid".into());
            }
        }
        AfterDrain::FinishInput => {}
    }
    Ok(())
}

pub(super) fn validate_no_external_output(facts: PrimitiveFacts) -> Result<(), String> {
    if facts.output != OutputFacts::None
        || facts.usage.output_rows != 0
        || facts.usage.output_bytes != 0
    {
        return Err("TopN internal phase reported external output".into());
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub(super) struct TopNStorage {
    pub(super) input: RelationRef,
    pub(super) candidate: RelationRef,
    pub(super) visible: RelationRef,
    pub(super) control: RelationRef,
    pub(super) continuation: RelationRef,
    pub(super) input_payload: RelationRef,
    pub(super) output_payload: RelationRef,
    pub(super) input_type: TypeRef,
    pub(super) output_type: TypeRef,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DurableTopN {
    pub(super) continuation: TopNContinuation,
    pub(super) persisted: bool,
}

#[derive(Clone, Debug)]
pub(super) struct TopNExpressions {
    pub(super) key_expressions: Vec<String>,
    pub(super) key_columns: Vec<String>,
    pub(super) output_expressions: String,
    pub(super) order_by: String,
    pub(super) keyset_after: String,
    pub(super) keys_equal: String,
}
