//! Bounded scalar control flow for a durable TopN stage.
//!
//! PostgreSQL owns every typed input, sort key, collation comparison,
//! candidate row, and visible row. Rust owns only fixed plan counts, stable
//! relation IDs, and counters needed to resume a bounded keyset page.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiTupleTable};

use crate::kernel::{
    advance_input, append_frontier, attribute_matches_slot, canonical_row_key_sql, chunk,
    compile_named_outputs, compile_stage_bindings, next_chunk, payload_facts,
    validate_output_attributes, AttributeRef, BindingInput, ChunkKind, InputPosition, OutputFacts,
    PageFacts, PhaseCode, PrimitiveFacts, ProducerKind, RelationRef, StepTxn, TypeRef, WorkUsage,
};
use crate::logical::model::{DataflowPlan, DataflowStage, OperatorSpec, TopNSpec};
use crate::logical::{StepOutcome, WorkBudget};
use crate::postgres::{format_lsn, quote_identifier};
use crate::scalar_sql::compile_scalar_expression;

use super::btree::{
    resolve_client as resolve_btree_client, resolve_step as resolve_btree_step, BtreeOrder,
};
use super::register::{
    catalog_continuation, catalog_state, column_sql, qualified_internal, resolve_relation_oid,
};

const ADMIT_PHASE: i16 = 1;
const SELECT_PHASE: i16 = 2;
const DIFF_PHASE: i16 = 3;
const CLEANUP_PHASE: i16 = 4;
const FRONTIER_PHASE: i16 = 5;

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
    fn validate(self) -> Result<(), String> {
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
    fn validate(self) -> Result<(), String> {
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
    fn initial(offset: u64, limit: u64) -> Self {
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
    limit: u64,
    offset: u64,
    with_ties: bool,
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
        validate_continuation_count(admitted.facts, next.is_some())?;
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
        validate_continuation_count(selected.page.facts, true)?;
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
        validate_continuation_count(page.facts, true)?;
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
        validate_continuation_count(page.facts, next.is_some())?;
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
        facts.validate(budget)?;
        if !matches!(facts.output, OutputFacts::Frontier { .. })
            || facts.usage.input_rows != 0
            || facts.usage.input_bytes != 0
            || facts.continuation_rows != 0
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

fn phase_after_drain(phase: TopNPhase) -> Result<AfterDrain, String> {
    match phase {
        TopNPhase::Select { after_drain, .. }
        | TopNPhase::Diff { after_drain, .. }
        | TopNPhase::Cleanup { after_drain, .. } => Ok(after_drain),
        TopNPhase::Admit | TopNPhase::Frontier => {
            Err("TopN phase has no Drain completion target".into())
        }
    }
}

fn validate_selection_progress(
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

fn validate_input(input: InputPosition) -> Result<(), String> {
    if input.stream_id <= 0 || input.chunk_seq <= 0 || input.row_ordinal < 0 {
        return Err("TopN input position is invalid".into());
    }
    Ok(())
}

fn validate_generation_id(generation_id: i64) -> Result<(), String> {
    if generation_id <= 0 {
        return Err("TopN candidate generation id is not positive".into());
    }
    Ok(())
}

fn validate_after_drain(input_stream_id: i64, after: AfterDrain) -> Result<(), String> {
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

fn validate_no_external_output(facts: PrimitiveFacts) -> Result<(), String> {
    if facts.output != OutputFacts::None
        || facts.usage.output_rows != 0
        || facts.usage.output_bytes != 0
    {
        return Err("TopN internal phase reported external output".into());
    }
    Ok(())
}

fn validate_continuation_count(
    facts: PrimitiveFacts,
    has_continuation: bool,
) -> Result<(), String> {
    if facts.continuation_rows != u64::from(has_continuation) {
        return Err("TopN checkpoint disagrees with its continuation row".into());
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct TopNStorage {
    input: RelationRef,
    candidate: RelationRef,
    visible: RelationRef,
    control: RelationRef,
    continuation: RelationRef,
    input_payload: RelationRef,
    output_payload: RelationRef,
    input_type: TypeRef,
    output_type: TypeRef,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DurableTopN {
    continuation: TopNContinuation,
    persisted: bool,
}

#[derive(Clone, Debug)]
struct TopNExpressions {
    key_expressions: Vec<String>,
    key_columns: Vec<String>,
    output_expressions: String,
    order_by: String,
    keyset_after: String,
    keys_equal: String,
}

/// Provision the only storage layout understood by this kernel.
///
/// There is intentionally no schema-version branch: a relation that does not
/// have this exact typed ABI is rejected by `execute`.
pub(crate) fn provision(
    client: &mut SpiClient<'_>,
    result_oid: pg_sys::Oid,
    stage_id: i32,
    stage: &DataflowStage,
    input_streams: &[i64],
    output_stream: i64,
) -> Result<(), String> {
    let OperatorSpec::TopN(spec) = &stage.spec else {
        return Err("TopN provisioner received another operator".into());
    };
    if result_oid == pg_sys::InvalidOid
        || stage_id < 0
        || input_streams.len() != 1
        || input_streams[0] <= 0
        || output_stream <= 0
        || spec.outputs.is_empty()
    {
        return Err(format!(
            "TopN stage {stage_id} has an invalid storage contract"
        ));
    }
    let input_payload = super::storage::payload(client, input_streams[0])?;
    let output_payload = super::storage::payload(client, output_stream)?;
    let output_attributes = super::storage::composite_attributes(client, &output_payload.row_type)?;
    validate_output_attributes(&output_attributes, &stage.schema.outputs)?;
    let prefix = format!("r{}_s{stage_id}", result_oid.to_u32());

    let input_name = format!("topn_input_{prefix}");
    let input = qualified_internal(&input_name);
    let mut key_definitions = Vec::with_capacity(spec.order_by.len());
    let mut index_columns = Vec::with_capacity(spec.order_by.len() + 1);
    for (index, order) in spec.order_by.iter().enumerate() {
        let name = format!("key_{}", index + 1);
        let mut definition = format!(
            "{} {}",
            quote_identifier(&name),
            column_sql(client, &order.type_)?
        );
        if !order.type_.nullable {
            definition.push_str(" NOT NULL");
        }
        key_definitions.push(definition);
        index_columns.push(resolve_btree_client(client, order, "TopN")?.index_column(&name));
    }
    index_columns.push("entry_id ASC".into());
    let key_suffix = if key_definitions.is_empty() {
        String::new()
    } else {
        format!(",{}", key_definitions.join(","))
    };
    create_topn_relation(
        client,
        &input,
        &format!(
            r#"
            entry_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            row_key bytea NOT NULL UNIQUE,
            row_value {} NOT NULL,
            multiplicity numeric NOT NULL CHECK(
              multiplicity > 0 AND multiplicity=pg_catalog.trunc(multiplicity)
            )
            {key_suffix}
            "#,
            input_payload.row_type.sql()
        ),
        "input",
    )?;
    let order_index = quote_identifier(&format!("topn_order_{prefix}"));
    client
        .update(
            &format!(
                "CREATE INDEX {order_index} ON {input}({})",
                index_columns.join(",")
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not create TopN ordering index: {error}"))?;
    let input_oid = resolve_relation_oid(client, &input)?;
    catalog_state(client, result_oid, stage_id, 0, input_oid)?;

    let candidate_name = format!("topn_candidate_{prefix}");
    let candidate = qualified_internal(&candidate_name);
    create_topn_relation(
        client,
        &candidate,
        &format!(
            r#"
            candidate_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            generation_id bigint NOT NULL CHECK(generation_id > 0),
            output_key bytea NOT NULL,
            output_row {} NOT NULL,
            multiplicity numeric NOT NULL CHECK(
              multiplicity > 0 AND multiplicity=pg_catalog.trunc(multiplicity)
            ),
            UNIQUE(generation_id,output_key)
            "#,
            output_payload.row_type.sql()
        ),
        "candidate",
    )?;
    let candidate_index = quote_identifier(&format!("topn_candidate_page_{prefix}"));
    client
        .update(
            &format!("CREATE INDEX {candidate_index} ON {candidate}(generation_id,candidate_id)"),
            None,
            &[],
        )
        .map_err(|error| format!("could not create TopN candidate page index: {error}"))?;
    let candidate_oid = resolve_relation_oid(client, &candidate)?;
    catalog_state(client, result_oid, stage_id, 1, candidate_oid)?;

    let visible_name = format!("topn_visible_{prefix}");
    let visible = qualified_internal(&visible_name);
    create_topn_relation(
        client,
        &visible,
        &format!(
            r#"
            visible_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            output_key bytea NOT NULL UNIQUE,
            output_row {} NOT NULL,
            multiplicity numeric NOT NULL CHECK(
              multiplicity > 0 AND multiplicity=pg_catalog.trunc(multiplicity)
            )
            "#,
            output_payload.row_type.sql()
        ),
        "visible",
    )?;
    let visible_oid = resolve_relation_oid(client, &visible)?;
    catalog_state(client, result_oid, stage_id, 2, visible_oid)?;

    let control_name = format!("topn_control_{prefix}");
    let control = qualified_internal(&control_name);
    create_topn_relation(
        client,
        &control,
        r#"
        singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
        dirty boolean NOT NULL DEFAULT false,
        causal_lsn pg_lsn,
        CHECK(dirty = (causal_lsn IS NOT NULL))
        "#,
        "control",
    )?;
    client
        .update(
            &format!("INSERT INTO {control}(singleton) VALUES(true)"),
            Some(1),
            &[],
        )
        .map_err(|error| format!("could not seed TopN control state: {error}"))?;
    let control_oid = resolve_relation_oid(client, &control)?;
    catalog_state(client, result_oid, stage_id, 3, control_oid)?;

    let continuation_name = format!("topn_continuation_{prefix}");
    let continuation = qualified_internal(&continuation_name);
    create_topn_relation(
        client,
        &continuation,
        r#"
        singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
        phase smallint NOT NULL CHECK(phase BETWEEN 1 AND 5),
        input_stream_id bigint NOT NULL CHECK(input_stream_id > 0),
        input_chunk_seq bigint CHECK(input_chunk_seq > 0),
        input_row_ordinal bigint CHECK(input_row_ordinal >= 0),
        generation_id bigint CHECK(generation_id > 0),
        cursor_row_id bigint CHECK(cursor_row_id > 0),
        cursor_repeat boolean NOT NULL DEFAULT false,
        offset_remaining numeric CHECK(
          offset_remaining >= 0
          AND offset_remaining=pg_catalog.trunc(offset_remaining)
        ),
        limit_remaining numeric CHECK(
          limit_remaining >= 0
          AND limit_remaining=pg_catalog.trunc(limit_remaining)
        ),
        tie_boundary_row_id bigint CHECK(tie_boundary_row_id > 0),
        diff_leg smallint CHECK(diff_leg IN (1,2)),
        after_kind smallint CHECK(after_kind IN (1,2,3)),
        after_chunk_seq bigint CHECK(after_chunk_seq > 0),
        after_row_ordinal bigint CHECK(after_row_ordinal >= 0),
        FOREIGN KEY(input_stream_id,input_chunk_seq)
          REFERENCES shiba_internal.effect_stream_chunks(stream_id,chunk_seq)
          ON DELETE RESTRICT,
        CHECK(
          (phase IN (1,5) AND input_chunk_seq IS NOT NULL
           AND input_row_ordinal IS NOT NULL
           AND generation_id IS NULL AND cursor_row_id IS NULL
           AND NOT cursor_repeat
           AND offset_remaining IS NULL AND limit_remaining IS NULL
           AND tie_boundary_row_id IS NULL AND diff_leg IS NULL
           AND after_kind IS NULL AND after_chunk_seq IS NULL
           AND after_row_ordinal IS NULL)
          OR
          (phase=2 AND input_chunk_seq IS NULL AND input_row_ordinal IS NULL
           AND generation_id IS NOT NULL
           AND NOT cursor_repeat
           AND offset_remaining IS NOT NULL AND limit_remaining IS NOT NULL
           AND diff_leg IS NULL AND after_kind IS NOT NULL)
          OR
          (phase=3 AND input_chunk_seq IS NULL AND input_row_ordinal IS NULL
           AND generation_id IS NOT NULL
           AND offset_remaining IS NULL AND limit_remaining IS NULL
           AND tie_boundary_row_id IS NULL AND diff_leg IS NOT NULL
           AND after_kind IS NOT NULL)
          OR
          (phase=4 AND input_chunk_seq IS NULL AND input_row_ordinal IS NULL
           AND generation_id IS NOT NULL
           AND NOT cursor_repeat
           AND offset_remaining IS NULL AND limit_remaining IS NULL
           AND tie_boundary_row_id IS NULL AND diff_leg IS NULL
           AND after_kind IS NOT NULL)
        ),
        CHECK(NOT cursor_repeat OR (phase=3 AND cursor_row_id IS NOT NULL)),
        CHECK(
          after_kind IS NULL
          OR (after_kind=1 AND after_chunk_seq IS NOT NULL
              AND after_row_ordinal IS NOT NULL)
          OR (after_kind=2 AND after_chunk_seq IS NULL
              AND after_row_ordinal IS NULL)
          OR (after_kind=3 AND after_chunk_seq IS NOT NULL
              AND after_row_ordinal=0)
        )
        "#,
        "continuation",
    )?;
    let continuation_oid = resolve_relation_oid(client, &continuation)?;
    catalog_continuation(client, result_oid, stage_id, continuation_oid)?;
    Ok(())
}

fn create_topn_relation(
    client: &mut SpiClient<'_>,
    relation: &str,
    body: &str,
    label: &str,
) -> Result<(), String> {
    client
        .update(&format!("CREATE TABLE {relation}({body})"), None, &[])
        .map_err(|error| format!("could not create TopN {label} relation: {error}"))?;
    client
        .update(
            &format!("REVOKE ALL ON TABLE {relation} FROM PUBLIC"),
            None,
            &[],
        )
        .map_err(|error| format!("could not protect TopN {label} relation: {error}"))?;
    Ok(())
}

/// Execute one TopN checkpoint. Every action performs one bounded set
/// primitive; typed rows and ordering values remain in PostgreSQL.
pub(crate) fn execute(
    mut transaction: StepTxn<'_, '_>,
    plan: &DataflowPlan,
    stage_id: u32,
) -> Result<StepOutcome, String> {
    let stage = plan
        .stages
        .get(usize::try_from(stage_id).map_err(|_| "TopN stage ID exceeds usize")?)
        .ok_or_else(|| format!("dataflow has no TopN stage {stage_id}"))?;
    let OperatorSpec::TopN(spec) = &stage.spec else {
        return Err("TopN kernel received another operator".into());
    };
    if stage.inputs.len() != 1
        || transaction.inputs().len() != 1
        || transaction.input(0)?.port != 0
        || transaction.input(0)?.producer != ProducerKind::Operator
    {
        return Err("TopN must have one operator input".into());
    }

    let machine = TopNMachine::new(spec.limit, spec.offset, spec.with_ties);
    let storage = load_topn_storage(&mut transaction, stage, spec)?;
    validate_topn_control_state(&mut transaction, &storage)?;
    let expressions = compile_topn_expressions(
        &mut transaction,
        plan,
        stage,
        spec,
        &storage.input_type,
        &storage.output_type,
    )?;
    let durable = load_topn_continuation(&mut transaction, &storage.continuation)?;
    if transaction.checkpoint_had_continuation() != durable.is_some() {
        return Err("TopN checkpoint disagrees with its typed continuation".into());
    }
    let current = match durable {
        Some(durable) => durable,
        None => start_topn_continuation(&mut transaction, &storage, machine)?,
    };
    if current.continuation.input_stream_id != transaction.input(0)?.stream_id {
        return Err("TopN continuation changed its input stream".into());
    }
    if let Some(input) = current.continuation.input {
        if input.stream_id != transaction.input(0)?.stream_id
            || input.chunk_seq != transaction.input(0)?.next_chunk_seq
        {
            return Err("TopN continuation is not at its input cursor".into());
        }
    }

    let action = machine.action(current.continuation)?;
    let result = match action {
        TopNAction::Admit { input } => TopNActionResult::Admitted(run_topn_admission(
            &mut transaction,
            &storage,
            &expressions,
            input,
        )?),
        TopNAction::SelectCandidates {
            generation_id,
            progress,
        } => TopNActionResult::Selected(run_topn_selection(
            &mut transaction,
            &storage,
            &expressions,
            spec,
            generation_id,
            progress,
        )?),
        TopNAction::Diff {
            generation_id,
            leg,
            cursor,
        } => TopNActionResult::Diffed(run_topn_diff(
            &mut transaction,
            &storage,
            generation_id,
            leg,
            cursor,
        )?),
        TopNAction::Cleanup {
            generation_id,
            cursor,
        } => {
            let after_drain = phase_after_drain(current.continuation.phase)?;
            let mut page = run_topn_cleanup(
                &mut transaction,
                &storage,
                generation_id,
                cursor,
                after_drain,
            )?;
            if page.complete {
                let finalized = finish_topn_drain(&mut transaction, &storage)?;
                page.facts.state_rows = page
                    .facts
                    .state_rows
                    .checked_add(finalized)
                    .ok_or_else(|| "TopN cleanup state count overflow".to_string())?;
            }
            TopNActionResult::Cleaned(page)
        }
        TopNAction::ForwardFrontier { input } => {
            TopNActionResult::FrontierForwarded(run_topn_frontier(&mut transaction, input)?)
        }
    };
    let transition = machine.apply(current.continuation, result, transaction.budget())?;
    let TopNTransition::Committed {
        continuation: next,
        facts,
    } = transition;
    let has_continuation = next.is_some();
    if facts.continuation_rows != u64::from(has_continuation) {
        return Err("TopN continuation mutation disagrees with primitive facts".into());
    }
    replace_topn_continuation(
        &mut transaction,
        &storage.continuation,
        current.persisted.then_some(current.continuation),
        next,
    )?;
    transaction.finish(has_continuation)
}

fn start_topn_continuation(
    transaction: &mut StepTxn<'_, '_>,
    storage: &TopNStorage,
    machine: TopNMachine,
) -> Result<DurableTopN, String> {
    let chunk = next_chunk(transaction, 0)?
        .ok_or_else(|| "runnable TopN has no input chunk".to_string())?;
    let input = InputPosition::new(chunk.stream_id, chunk.sequence, 0)?;
    let (input, phase) = match chunk.kind {
        ChunkKind::Data => (Some(input), TopNPhase::Admit),
        ChunkKind::Frontier if topn_is_dirty(transaction, storage)? => (
            None,
            initial_topn_drain_phase(transaction, machine, AfterDrain::Frontier(input))?,
        ),
        ChunkKind::Frontier => (Some(input), TopNPhase::Frontier),
    };
    Ok(DurableTopN {
        continuation: TopNContinuation {
            input_stream_id: chunk.stream_id,
            input,
            phase,
        },
        persisted: false,
    })
}

fn initial_topn_drain_phase(
    transaction: &mut StepTxn<'_, '_>,
    machine: TopNMachine,
    after_drain: AfterDrain,
) -> Result<TopNPhase, String> {
    let generation_id = next_generation_id(transaction)?;
    Ok(if machine.limit == 0 {
        TopNPhase::Diff {
            generation_id,
            leg: DiffLeg::Remove,
            cursor: TopNDiffCursor::default(),
            after_drain,
        }
    } else {
        TopNPhase::Select {
            generation_id,
            progress: SelectionProgress::initial(machine.offset, machine.limit),
            after_drain,
        }
    })
}

fn load_topn_storage(
    transaction: &mut StepTxn<'_, '_>,
    stage: &DataflowStage,
    spec: &TopNSpec,
) -> Result<TopNStorage, String> {
    let input_stream = transaction.input(0)?.stream_id;
    let output_stream = transaction.output()?.stream_id;
    let input_payload = transaction.payload_storage(input_stream)?;
    let output_payload = transaction.payload_storage(output_stream)?;
    let storage = TopNStorage {
        input: transaction.state_storage(0)?,
        candidate: transaction.state_storage(1)?,
        visible: transaction.state_storage(2)?,
        control: transaction.state_storage(3)?,
        continuation: transaction.continuation_storage()?,
        input_payload: input_payload.relation,
        output_payload: output_payload.relation,
        input_type: input_payload.row_type,
        output_type: output_payload.row_type,
    };
    validate_topn_storage(transaction, &storage, stage, spec)?;
    Ok(storage)
}

fn validate_topn_storage(
    transaction: &mut StepTxn<'_, '_>,
    storage: &TopNStorage,
    stage: &DataflowStage,
    spec: &TopNSpec,
) -> Result<(), String> {
    let input = transaction.relation_attributes(storage.input.oid())?;
    let expected_input_len = 4usize
        .checked_add(spec.order_by.len())
        .ok_or_else(|| "TopN input ABI is too wide".to_string())?;
    if input.len() != expected_input_len
        || !attribute_is(&input[0], "entry_id", pg_sys::INT8OID, true)
        || !attribute_is(&input[1], "row_key", pg_sys::BYTEAOID, true)
        || input[2].name != "row_value"
        || input[2].type_oid != storage.input_type.oid()
        || !input[2].not_null
        || !attribute_is(&input[3], "multiplicity", pg_sys::NUMERICOID, true)
    {
        return Err("TopN input relation has an invalid ABI".into());
    }
    for (ordinal, (attribute, order)) in input[4..].iter().zip(&spec.order_by).enumerate() {
        if attribute.name != format!("key_{}", ordinal + 1)
            || !attribute_matches_slot(attribute, &order.type_)
        {
            return Err("TopN ordering column changed its typed ABI".into());
        }
    }

    let candidate = transaction.relation_attributes(storage.candidate.oid())?;
    if candidate.len() != 5
        || !attribute_is(&candidate[0], "candidate_id", pg_sys::INT8OID, true)
        || !attribute_is(&candidate[1], "generation_id", pg_sys::INT8OID, true)
        || !attribute_is(&candidate[2], "output_key", pg_sys::BYTEAOID, true)
        || candidate[3].name != "output_row"
        || candidate[3].type_oid != storage.output_type.oid()
        || !candidate[3].not_null
        || !attribute_is(&candidate[4], "multiplicity", pg_sys::NUMERICOID, true)
    {
        return Err("TopN candidate relation has an invalid ABI".into());
    }
    let visible = transaction.relation_attributes(storage.visible.oid())?;
    if visible.len() != 4
        || !attribute_is(&visible[0], "visible_id", pg_sys::INT8OID, true)
        || !attribute_is(&visible[1], "output_key", pg_sys::BYTEAOID, true)
        || visible[2].name != "output_row"
        || visible[2].type_oid != storage.output_type.oid()
        || !visible[2].not_null
        || !attribute_is(&visible[3], "multiplicity", pg_sys::NUMERICOID, true)
    {
        return Err("TopN visible relation has an invalid ABI".into());
    }
    let control = transaction.relation_attributes(storage.control.oid())?;
    if control.len() != 3
        || !attribute_is(&control[0], "singleton", pg_sys::BOOLOID, true)
        || !attribute_is(&control[1], "dirty", pg_sys::BOOLOID, true)
        || !attribute_is(&control[2], "causal_lsn", pg_sys::PG_LSNOID, false)
    {
        return Err("TopN control relation has an invalid ABI".into());
    }
    validate_topn_continuation_abi(transaction, &storage.continuation)?;

    let output = transaction.composite_attributes(&storage.output_type)?;
    validate_output_attributes(&output, &stage.schema.outputs)?;
    Ok(())
}

fn validate_topn_control_state(
    transaction: &mut StepTxn<'_, '_>,
    storage: &TopNStorage,
) -> Result<(), String> {
    let rows = transaction.read(
        &format!(
            "SELECT dirty,causal_lsn IS NOT NULL FROM {} WHERE singleton",
            storage.control.sql()
        ),
        &[],
    )?;
    if rows.len() != 1 {
        return Err("TopN control state has no singleton row".into());
    }
    let row = rows.first();
    let dirty: bool = required(&row, 1, "TopN dirty state")?;
    let has_lsn: bool = required(&row, 2, "TopN causal LSN presence")?;
    if dirty != has_lsn || (dirty && transaction.admission_progress().is_empty()) {
        return Err("TopN dirty state disagrees with its admission checkpoint".into());
    }
    Ok(())
}

fn topn_is_dirty(transaction: &mut StepTxn<'_, '_>, storage: &TopNStorage) -> Result<bool, String> {
    let rows = transaction.read(
        &format!(
            "SELECT dirty FROM {} WHERE singleton",
            storage.control.sql()
        ),
        &[],
    )?;
    if rows.len() != 1 {
        return Err("TopN control state has no singleton row".into());
    }
    required(&rows.first(), 1, "TopN dirty state")
}

fn validate_topn_continuation_abi(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
) -> Result<(), String> {
    let attributes = transaction.relation_attributes(relation.oid())?;
    let expected = [
        ("singleton", pg_sys::BOOLOID, true),
        ("phase", pg_sys::INT2OID, true),
        ("input_stream_id", pg_sys::INT8OID, true),
        ("input_chunk_seq", pg_sys::INT8OID, false),
        ("input_row_ordinal", pg_sys::INT8OID, false),
        ("generation_id", pg_sys::INT8OID, false),
        ("cursor_row_id", pg_sys::INT8OID, false),
        ("cursor_repeat", pg_sys::BOOLOID, true),
        ("offset_remaining", pg_sys::NUMERICOID, false),
        ("limit_remaining", pg_sys::NUMERICOID, false),
        ("tie_boundary_row_id", pg_sys::INT8OID, false),
        ("diff_leg", pg_sys::INT2OID, false),
        ("after_kind", pg_sys::INT2OID, false),
        ("after_chunk_seq", pg_sys::INT8OID, false),
        ("after_row_ordinal", pg_sys::INT8OID, false),
    ];
    if attributes.len() != expected.len()
        || attributes
            .iter()
            .zip(expected)
            .any(|(actual, (name, type_oid, not_null))| {
                !attribute_is(actual, name, type_oid, not_null)
            })
    {
        return Err("TopN continuation relation has an invalid ABI".into());
    }
    Ok(())
}

fn compile_topn_expressions(
    transaction: &mut StepTxn<'_, '_>,
    plan: &DataflowPlan,
    stage: &DataflowStage,
    spec: &TopNSpec,
    input_type: &TypeRef,
    output_type: &TypeRef,
) -> Result<TopNExpressions, String> {
    let bindings = compile_stage_bindings(
        transaction,
        plan,
        stage,
        &[BindingInput {
            row_type: input_type,
            alias: "input_row",
        }],
    )?;
    let key_expressions = spec
        .order_by
        .iter()
        .map(|key| compile_scalar_expression(&key.expr, &bindings))
        .collect::<Result<Vec<_>, _>>()?;
    let key_columns = (1..=spec.order_by.len())
        .map(|ordinal| format!("key_{ordinal}"))
        .collect::<Vec<_>>();
    let output_expressions =
        compile_named_outputs(&stage.schema.outputs, &spec.outputs, &bindings, "TopN")?.join(", ");
    let output_attributes = transaction.composite_attributes(output_type)?;
    validate_output_attributes(&output_attributes, &stage.schema.outputs)?;

    let mut resolved = Vec::with_capacity(spec.order_by.len());
    for key in &spec.order_by {
        resolved.push(resolve_btree_step(transaction, key, "TopN")?);
    }
    let order_by = resolved
        .iter()
        .enumerate()
        .map(|(index, order)| {
            format!(
                "input_row.key_{} USING {} NULLS {}",
                index + 1,
                order.sort_operator,
                if order.nulls_first { "FIRST" } else { "LAST" }
            )
        })
        .chain(std::iter::once("input_row.entry_id ASC".into()))
        .collect::<Vec<_>>()
        .join(", ");
    let keyset_after = keyset_after_sql(&resolved);
    let keys_equal = keys_equal_sql(&resolved, "input_row", "tie_boundary");
    Ok(TopNExpressions {
        key_expressions,
        key_columns,
        output_expressions,
        order_by,
        keyset_after,
        keys_equal,
    })
}

fn keyset_after_sql(orders: &[BtreeOrder]) -> String {
    let mut alternatives = Vec::with_capacity(orders.len() + 1);
    let mut equal_prefix = Vec::new();
    for (index, order) in orders.iter().enumerate() {
        let column = format!("key_{}", index + 1);
        let before = format!("boundary.{column}");
        let current = format!("input_row.{column}");
        let after = if order.nulls_first {
            format!(
                "(CASE WHEN {before} IS NULL THEN {current} IS NOT NULL \
                 WHEN {current} IS NULL THEN FALSE \
                 ELSE {before} {} {current} END)",
                order.sort_operator
            )
        } else {
            format!(
                "(CASE WHEN {before} IS NULL THEN FALSE \
                 WHEN {current} IS NULL THEN TRUE \
                 ELSE {before} {} {current} END)",
                order.sort_operator
            )
        };
        alternatives.push(if equal_prefix.is_empty() {
            after
        } else {
            format!("({} AND {after})", equal_prefix.join(" AND "))
        });
        equal_prefix.push(format!(
            "(({before} IS NULL AND {current} IS NULL) OR \
             ({before} IS NOT NULL AND {current} IS NOT NULL \
              AND {before} {} {current}))",
            order.equality_operator
        ));
    }
    let id_after = "input_row.entry_id > boundary.entry_id";
    alternatives.push(if equal_prefix.is_empty() {
        id_after.into()
    } else {
        format!("({} AND {id_after})", equal_prefix.join(" AND "))
    });
    alternatives.join(" OR ")
}

fn keys_equal_sql(orders: &[BtreeOrder], left: &str, right: &str) -> String {
    if orders.is_empty() {
        return "TRUE".into();
    }
    orders
        .iter()
        .enumerate()
        .map(|(index, order)| {
            let column = format!("key_{}", index + 1);
            format!(
                "(({left}.{column} IS NULL AND {right}.{column} IS NULL) OR \
                 ({left}.{column} IS NOT NULL AND {right}.{column} IS NOT NULL \
                  AND {left}.{column} {} {right}.{column}))",
                order.equality_operator
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

#[derive(Clone, Debug)]
struct TopNFields {
    phase: i16,
    input_stream_id: i64,
    input_chunk_seq: Option<i64>,
    input_row_ordinal: Option<i64>,
    generation_id: Option<i64>,
    cursor_row_id: Option<i64>,
    cursor_repeat: bool,
    offset_remaining: Option<String>,
    limit_remaining: Option<String>,
    tie_boundary_row_id: Option<i64>,
    diff_leg: Option<i16>,
    after_kind: Option<i16>,
    after_chunk_seq: Option<i64>,
    after_row_ordinal: Option<i64>,
}

fn load_topn_continuation(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
) -> Result<Option<DurableTopN>, String> {
    let query = format!(
        r#"
        SELECT phase,input_stream_id,input_chunk_seq,input_row_ordinal,
               generation_id,cursor_row_id,cursor_repeat,offset_remaining::text,
               limit_remaining::text,tie_boundary_row_id,diff_leg,
               after_kind,after_chunk_seq,after_row_ordinal
        FROM {}
        WHERE singleton
        FOR UPDATE
        "#,
        relation.sql()
    );
    let rows = transaction.lock(&query, &[])?;
    match rows.len() {
        0 => Ok(None),
        1 => {
            let row = rows.first();
            let fields = TopNFields {
                phase: required(&row, 1, "TopN phase")?,
                input_stream_id: required(&row, 2, "TopN input stream")?,
                input_chunk_seq: row.get(3).map_err(|error| error.to_string())?,
                input_row_ordinal: row.get(4).map_err(|error| error.to_string())?,
                generation_id: row.get(5).map_err(|error| error.to_string())?,
                cursor_row_id: row.get(6).map_err(|error| error.to_string())?,
                cursor_repeat: required(&row, 7, "TopN cursor repeat")?,
                offset_remaining: row.get(8).map_err(|error| error.to_string())?,
                limit_remaining: row.get(9).map_err(|error| error.to_string())?,
                tie_boundary_row_id: row.get(10).map_err(|error| error.to_string())?,
                diff_leg: row.get(11).map_err(|error| error.to_string())?,
                after_kind: row.get(12).map_err(|error| error.to_string())?,
                after_chunk_seq: row.get(13).map_err(|error| error.to_string())?,
                after_row_ordinal: row.get(14).map_err(|error| error.to_string())?,
            };
            Ok(Some(DurableTopN {
                continuation: decode_topn_fields(fields)?,
                persisted: true,
            }))
        }
        count => Err(format!("TopN continuation relation contains {count} rows")),
    }
}

fn decode_topn_fields(fields: TopNFields) -> Result<TopNContinuation, String> {
    let kind = TopNPhaseKind::from_code(PhaseCode::active(fields.phase)?)?;
    let after = || {
        decode_after_drain(
            fields.input_stream_id,
            fields.after_kind,
            fields.after_chunk_seq,
            fields.after_row_ordinal,
        )
    };
    let input = match (kind, fields.input_chunk_seq, fields.input_row_ordinal) {
        (TopNPhaseKind::Admit | TopNPhaseKind::Frontier, Some(chunk), Some(row)) => {
            Some(InputPosition::new(fields.input_stream_id, chunk, row)?)
        }
        (TopNPhaseKind::Select | TopNPhaseKind::Diff | TopNPhaseKind::Cleanup, None, None) => None,
        _ => return Err("TopN continuation has an invalid input cursor shape".into()),
    };
    let cursor = TopNCursor {
        row_id: fields.cursor_row_id,
    };
    let phase = match kind {
        TopNPhaseKind::Admit => {
            require_topn_nulls(&fields, &[4, 5, 6, 7, 8, 9, 10, 11, 12, 13])?;
            TopNPhase::Admit
        }
        TopNPhaseKind::Select => {
            if fields.diff_leg.is_some() || fields.cursor_repeat {
                return Err("TopN Select continuation contains Diff state".into());
            }
            TopNPhase::Select {
                generation_id: fields
                    .generation_id
                    .ok_or_else(|| "TopN Select omitted its generation".to_string())?,
                progress: SelectionProgress {
                    cursor,
                    offset_remaining: parse_u64_numeric(
                        fields.offset_remaining.as_deref(),
                        "TopN OFFSET",
                    )?,
                    limit_remaining: parse_u64_numeric(
                        fields.limit_remaining.as_deref(),
                        "TopN LIMIT",
                    )?,
                    tie_boundary_row_id: fields.tie_boundary_row_id,
                },
                after_drain: after()?,
            }
        }
        TopNPhaseKind::Diff => {
            if fields.offset_remaining.is_some()
                || fields.limit_remaining.is_some()
                || fields.tie_boundary_row_id.is_some()
            {
                return Err("TopN Diff continuation contains selection state".into());
            }
            TopNPhase::Diff {
                generation_id: fields
                    .generation_id
                    .ok_or_else(|| "TopN Diff omitted its generation".to_string())?,
                leg: match fields.diff_leg {
                    Some(1) => DiffLeg::Remove,
                    Some(2) => DiffLeg::Add,
                    _ => return Err("TopN Diff continuation has an invalid leg".into()),
                },
                cursor: TopNDiffCursor {
                    row_id: fields.cursor_row_id,
                    repeat: fields.cursor_repeat,
                },
                after_drain: after()?,
            }
        }
        TopNPhaseKind::Cleanup => {
            if fields.offset_remaining.is_some()
                || fields.limit_remaining.is_some()
                || fields.tie_boundary_row_id.is_some()
                || fields.diff_leg.is_some()
                || fields.cursor_repeat
            {
                return Err("TopN Cleanup continuation contains another phase's state".into());
            }
            TopNPhase::Cleanup {
                generation_id: fields
                    .generation_id
                    .ok_or_else(|| "TopN Cleanup omitted its generation".to_string())?,
                cursor,
                after_drain: after()?,
            }
        }
        TopNPhaseKind::Frontier => {
            require_topn_nulls(&fields, &[4, 5, 6, 7, 8, 9, 10, 11, 12, 13])?;
            TopNPhase::Frontier
        }
    };
    Ok(TopNContinuation {
        input_stream_id: fields.input_stream_id,
        input,
        phase,
    })
}

fn require_topn_nulls(fields: &TopNFields, ordinals: &[usize]) -> Result<(), String> {
    let present = |ordinal| match ordinal {
        4 => fields.generation_id.is_some(),
        5 => fields.cursor_row_id.is_some(),
        6 => fields.cursor_repeat,
        7 => fields.offset_remaining.is_some(),
        8 => fields.limit_remaining.is_some(),
        9 => fields.tie_boundary_row_id.is_some(),
        10 => fields.diff_leg.is_some(),
        11 => fields.after_kind.is_some(),
        12 => fields.after_chunk_seq.is_some(),
        13 => fields.after_row_ordinal.is_some(),
        _ => true,
    };
    if ordinals.iter().copied().any(present) {
        return Err("TopN continuation contains fields from another phase".into());
    }
    Ok(())
}

fn decode_after_drain(
    input_stream_id: i64,
    kind: Option<i16>,
    chunk_seq: Option<i64>,
    row_ordinal: Option<i64>,
) -> Result<AfterDrain, String> {
    match (kind, chunk_seq, row_ordinal) {
        (Some(1), Some(chunk), Some(row)) => Ok(AfterDrain::Admit(InputPosition::new(
            input_stream_id,
            chunk,
            row,
        )?)),
        (Some(2), None, None) => Ok(AfterDrain::FinishInput),
        (Some(3), Some(chunk), Some(row)) => Ok(AfterDrain::Frontier(InputPosition::new(
            input_stream_id,
            chunk,
            row,
        )?)),
        _ => Err("TopN continuation has an invalid Drain target".into()),
    }
}

fn encode_topn_fields(continuation: TopNContinuation) -> TopNFields {
    let mut fields = TopNFields {
        phase: continuation.phase.code().value(),
        input_stream_id: continuation.input_stream_id,
        input_chunk_seq: continuation.input.map(|input| input.chunk_seq),
        input_row_ordinal: continuation.input.map(|input| input.row_ordinal),
        generation_id: None,
        cursor_row_id: None,
        cursor_repeat: false,
        offset_remaining: None,
        limit_remaining: None,
        tie_boundary_row_id: None,
        diff_leg: None,
        after_kind: None,
        after_chunk_seq: None,
        after_row_ordinal: None,
    };
    match continuation.phase {
        TopNPhase::Admit | TopNPhase::Frontier => {}
        TopNPhase::Select {
            generation_id,
            progress,
            after_drain,
        } => {
            fields.generation_id = Some(generation_id);
            fields.cursor_row_id = progress.cursor.row_id;
            fields.offset_remaining = Some(progress.offset_remaining.to_string());
            fields.limit_remaining = Some(progress.limit_remaining.to_string());
            fields.tie_boundary_row_id = progress.tie_boundary_row_id;
            encode_after(&mut fields, after_drain);
        }
        TopNPhase::Diff {
            generation_id,
            leg,
            cursor,
            after_drain,
        } => {
            fields.generation_id = Some(generation_id);
            fields.cursor_row_id = cursor.row_id;
            fields.cursor_repeat = cursor.repeat;
            fields.diff_leg = Some(match leg {
                DiffLeg::Remove => 1,
                DiffLeg::Add => 2,
            });
            encode_after(&mut fields, after_drain);
        }
        TopNPhase::Cleanup {
            generation_id,
            cursor,
            after_drain,
        } => {
            fields.generation_id = Some(generation_id);
            fields.cursor_row_id = cursor.row_id;
            encode_after(&mut fields, after_drain);
        }
    }
    fields
}

fn encode_after(fields: &mut TopNFields, after: AfterDrain) {
    match after {
        AfterDrain::Admit(input) => {
            fields.after_kind = Some(1);
            fields.after_chunk_seq = Some(input.chunk_seq);
            fields.after_row_ordinal = Some(input.row_ordinal);
        }
        AfterDrain::FinishInput => fields.after_kind = Some(2),
        AfterDrain::Frontier(input) => {
            fields.after_kind = Some(3);
            fields.after_chunk_seq = Some(input.chunk_seq);
            fields.after_row_ordinal = Some(input.row_ordinal);
        }
    }
}

fn replace_topn_continuation(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
    old: Option<TopNContinuation>,
    next: Option<TopNContinuation>,
) -> Result<(), String> {
    if let Some(old) = old {
        delete_topn_continuation(transaction, relation, &encode_topn_fields(old))?;
    }
    if let Some(next) = next {
        insert_topn_continuation(transaction, relation, &encode_topn_fields(next))?;
    }
    Ok(())
}

fn delete_topn_continuation(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
    fields: &TopNFields,
) -> Result<(), String> {
    let query = format!(
        r#"
        DELETE FROM {}
        WHERE singleton AND phase=$1 AND input_stream_id=$2
          AND input_chunk_seq IS NOT DISTINCT FROM $3
          AND input_row_ordinal IS NOT DISTINCT FROM $4
          AND generation_id IS NOT DISTINCT FROM $5
          AND cursor_row_id IS NOT DISTINCT FROM $6
          AND cursor_repeat=$7
          AND offset_remaining IS NOT DISTINCT FROM $8::numeric
          AND limit_remaining IS NOT DISTINCT FROM $9::numeric
          AND tie_boundary_row_id IS NOT DISTINCT FROM $10
          AND diff_leg IS NOT DISTINCT FROM $11
          AND after_kind IS NOT DISTINCT FROM $12
          AND after_chunk_seq IS NOT DISTINCT FROM $13
          AND after_row_ordinal IS NOT DISTINCT FROM $14
        RETURNING singleton
        "#,
        relation.sql()
    );
    let arguments = topn_field_arguments(fields);
    if transaction.write(&query, &arguments)?.len() != 1 {
        return Err("TopN continuation compare-and-set failed".into());
    }
    Ok(())
}

fn insert_topn_continuation(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
    fields: &TopNFields,
) -> Result<(), String> {
    let query = format!(
        r#"
        INSERT INTO {}(
          singleton,phase,input_stream_id,input_chunk_seq,input_row_ordinal,
          generation_id,cursor_row_id,cursor_repeat,offset_remaining,limit_remaining,
          tie_boundary_row_id,diff_leg,after_kind,after_chunk_seq,
          after_row_ordinal
        )
        VALUES(true,$1,$2,$3,$4,$5,$6,$7,$8::numeric,$9::numeric,$10,$11,$12,$13,$14)
        RETURNING singleton
        "#,
        relation.sql()
    );
    let arguments = topn_field_arguments(fields);
    if transaction.write(&query, &arguments)?.len() != 1 {
        return Err("TopN continuation insert failed".into());
    }
    Ok(())
}

fn topn_field_arguments<'a>(fields: &'a TopNFields) -> [DatumWithOid<'a>; 14] {
    unsafe {
        [
            DatumWithOid::new(fields.phase, pg_sys::INT2OID),
            DatumWithOid::new(fields.input_stream_id, pg_sys::INT8OID),
            DatumWithOid::new(fields.input_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(fields.input_row_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(fields.generation_id, pg_sys::INT8OID),
            DatumWithOid::new(fields.cursor_row_id, pg_sys::INT8OID),
            DatumWithOid::new(fields.cursor_repeat, pg_sys::BOOLOID),
            DatumWithOid::new(fields.offset_remaining.as_deref(), pg_sys::TEXTOID),
            DatumWithOid::new(fields.limit_remaining.as_deref(), pg_sys::TEXTOID),
            DatumWithOid::new(fields.tie_boundary_row_id, pg_sys::INT8OID),
            DatumWithOid::new(fields.diff_leg, pg_sys::INT2OID),
            DatumWithOid::new(fields.after_kind, pg_sys::INT2OID),
            DatumWithOid::new(fields.after_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(fields.after_row_ordinal, pg_sys::INT8OID),
        ]
    }
}

fn parse_u64_numeric(value: Option<&str>, label: &str) -> Result<u64, String> {
    value
        .ok_or_else(|| format!("{label} continuation value is NULL"))?
        .parse()
        .map_err(|_| format!("{label} continuation value is not an unsigned integer"))
}

fn required<T: FromDatum + IntoDatum>(
    table: &SpiTupleTable<'_>,
    ordinal: usize,
    name: &str,
) -> Result<T, String> {
    table
        .get::<T>(ordinal)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("database returned NULL {name}"))
}

fn nonnegative(value: i64, name: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("database returned negative {name}"))
}

fn i64_from_usize(value: usize, name: &str) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("{name} exceeds bigint"))
}

fn attribute_is(
    attribute: &AttributeRef,
    name: &str,
    type_oid: pg_sys::Oid,
    not_null: bool,
) -> bool {
    attribute.name == name && attribute.type_oid == type_oid && attribute.not_null == not_null
}

fn run_topn_admission(
    transaction: &mut StepTxn<'_, '_>,
    storage: &TopNStorage,
    expressions: &TopNExpressions,
    input: InputPosition,
) -> Result<TopNAdmission, String> {
    let input_state = transaction.input(0)?.clone();
    let input_chunk = chunk(transaction, &input_state, input.chunk_seq)?
        .ok_or_else(|| "TopN admission references a missing input chunk".to_string())?;
    if input_chunk.kind != ChunkKind::Data || input_chunk.stream_id != input.stream_id {
        return Err("TopN admission does not reference a data chunk".into());
    }
    if input.row_ordinal == 0 {
        payload_facts(transaction, &storage.input_payload, &input_chunk)?;
    }
    let chunk_rows =
        i64::try_from(input_chunk.rows).map_err(|_| "TopN chunk row count exceeds bigint")?;
    if input.row_ordinal >= chunk_rows {
        return Err("TopN admission cursor is outside its data chunk".into());
    }
    let budget = transaction.budget();
    let max_rows = i64_from_usize(budget.max_input_rows, "TopN input row budget")?;
    let max_bytes = i64_from_usize(budget.max_input_bytes, "TopN input byte budget")?;
    let key_select = expressions
        .key_expressions
        .iter()
        .zip(&expressions.key_columns)
        .map(|(expression, column)| format!("{expression} AS {}", quote_identifier(column)))
        .collect::<Vec<_>>()
        .join(",");
    let key_select = if key_select.is_empty() {
        String::new()
    } else {
        format!(",{key_select}")
    };
    let key_columns = expressions
        .key_columns
        .iter()
        .map(|column| quote_identifier(column))
        .collect::<Vec<_>>();
    let insert_keys = if key_columns.is_empty() {
        String::new()
    } else {
        format!(",{}", key_columns.join(","))
    };
    let representative_keys = if key_columns.is_empty() {
        String::new()
    } else {
        format!(
            ",{}",
            key_columns
                .iter()
                .map(|column| format!("representative.{column}"))
                .collect::<Vec<_>>()
                .join(",")
        )
    };
    let decision_keys = if key_columns.is_empty() {
        String::new()
    } else {
        format!(
            ",{}",
            key_columns
                .iter()
                .map(|column| format!("decision.{column}"))
                .collect::<Vec<_>>()
                .join(",")
        )
    };
    let update_keys = key_columns
        .iter()
        .map(|column| format!("{column}=EXCLUDED.{column}"))
        .collect::<Vec<_>>();
    let update_keys = if update_keys.is_empty() {
        String::new()
    } else {
        format!(",{}", update_keys.join(","))
    };
    let row_key = canonical_row_key_sql("input_row.row_value", &storage.input_type);
    let query = format!(
        r#"
        WITH source AS MATERIALIZED (
          SELECT input_row.row_ordinal,input_row.weight,input_row.row_value,
                 shiba_internal.effect_row_bytes(input_row.row_value) AS row_bytes
          FROM {payload} AS input_row
          WHERE input_row.stream_id=$1 AND input_row.chunk_seq=$2
            AND input_row.row_ordinal >= $3
          ORDER BY input_row.row_ordinal
          LIMIT $4
        ),
        measured AS MATERIALIZED (
          SELECT source.*,
                 row_number() OVER (ORDER BY row_ordinal) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY row_ordinal) AS running_bytes
          FROM source
        ),
        bounded AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_ordinal=1 OR running_bytes <= $5
        ),
        evaluated AS MATERIALIZED (
          SELECT input_row.*,
                 {row_key} AS row_key
                 {key_select}
          FROM bounded AS input_row
        ),
        prefixes AS MATERIALIZED (
          SELECT evaluated.*,
                 sum(weight::numeric) OVER (
                   PARTITION BY row_key ORDER BY row_ordinal
                   ROWS UNBOUNDED PRECEDING
                 ) AS key_prefix
          FROM evaluated
        ),
        collapsed AS MATERIALIZED (
          SELECT row_key,min(row_ordinal) AS representative_ordinal,
                 sum(weight::numeric) AS net_weight,
                 min(key_prefix) AS min_prefix
          FROM prefixes
          GROUP BY row_key
        ),
        representative AS MATERIALIZED (
          SELECT evaluated.*
          FROM evaluated
          JOIN collapsed
            ON collapsed.row_key=evaluated.row_key
           AND collapsed.representative_ordinal=evaluated.row_ordinal
        ),
        existing AS MATERIALIZED (
          SELECT state.entry_id,state.row_key,state.multiplicity
          FROM {state} AS state
          JOIN collapsed USING(row_key)
          FOR UPDATE OF state
        ),
        decision AS MATERIALIZED (
          SELECT collapsed.*,representative.row_value
                 {representative_keys},
                 existing.entry_id,
                 coalesce(existing.multiplicity,0::numeric) AS old_multiplicity,
                 coalesce(existing.multiplicity,0::numeric)+collapsed.net_weight
                   AS new_multiplicity,
                 coalesce(existing.multiplicity,0::numeric)+collapsed.min_prefix
                   AS minimum_multiplicity
          FROM collapsed
          JOIN representative USING(row_key)
          LEFT JOIN existing USING(row_key)
        ),
        status AS MATERIALIZED (
          SELECT CASE
                   WHEN EXISTS(
                     SELECT 1 FROM decision WHERE minimum_multiplicity < 0
                   ) THEN 'negative'
                   WHEN EXISTS(SELECT 1 FROM {candidate}) THEN 'dirty_candidate'
                   ELSE 'ok'
                 END AS value
        ),
        removed AS (
          DELETE FROM {state} AS state
          USING decision,status
          WHERE status.value='ok'
            AND decision.new_multiplicity=0
            AND state.entry_id=decision.entry_id
          RETURNING 1
        ),
        changed AS (
          INSERT INTO {state}(row_key,row_value,multiplicity{insert_keys})
          SELECT decision.row_key,decision.row_value,decision.new_multiplicity
                 {decision_keys}
          FROM decision,status
          WHERE status.value='ok' AND decision.new_multiplicity > 0
          ON CONFLICT(row_key) DO UPDATE
          SET row_value=EXCLUDED.row_value,
              multiplicity=EXCLUDED.multiplicity
              {update_keys}
          RETURNING 1
        ),
        control_changed AS (
          UPDATE {control} AS control
          SET dirty=true,
              causal_lsn=CASE
                WHEN control.causal_lsn IS NULL THEN $6::pg_lsn
                ELSE greatest(control.causal_lsn,$6::pg_lsn)
              END
          FROM status
          WHERE control.singleton AND status.value='ok'
          RETURNING 1
        )
        SELECT (SELECT value FROM status),
               count(*)::bigint,
               min(row_ordinal)::bigint,
               max(row_ordinal)::bigint,
               coalesce(sum(row_bytes),0)::bigint,
               (SELECT count(*)::bigint FROM removed)
                 +(SELECT count(*)::bigint FROM changed)
                 +(SELECT count(*)::bigint FROM control_changed)
        FROM bounded
        "#,
        payload = storage.input_payload.sql(),
        state = storage.input.sql(),
        candidate = storage.candidate.sql(),
        control = storage.control.sql(),
        row_key = row_key,
    );
    let causal_lsn = format_lsn(input_chunk.lsn);
    let arguments = unsafe {
        [
            DatumWithOid::new(input.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(input.chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(input.row_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(max_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_bytes, pg_sys::INT8OID),
            DatumWithOid::new(causal_lsn.as_str(), pg_sys::TEXTOID),
        ]
    };
    let rows = transaction.write(&query, &arguments)?;
    if rows.len() != 1 {
        return Err("TopN admission returned no summary".into());
    }
    let row = rows.first();
    let status = required::<String>(&row, 1, "TopN admission status")?;
    if status != "ok" {
        return Err(format!("TopN admission returned {status}"));
    }
    let processed = nonnegative(
        required(&row, 2, "TopN admitted rows")?,
        "TopN admitted rows",
    )?;
    let first = required::<i64>(&row, 3, "TopN first admitted row")?;
    let last = required::<i64>(&row, 4, "TopN last admitted row")?;
    let input_bytes = nonnegative(
        required(&row, 5, "TopN admitted bytes")?,
        "TopN admitted bytes",
    )?;
    let touched = nonnegative(
        required(&row, 6, "TopN touched state rows")?,
        "TopN touched state rows",
    )?;
    if processed == 0
        || first != input.row_ordinal
        || last
            != input
                .row_ordinal
                .checked_add(i64::try_from(processed).map_err(|_| "TopN page exceeds bigint")?)
                .and_then(|value| value.checked_sub(1))
                .ok_or_else(|| "TopN input ordinal overflow".to_string())?
    {
        return Err("TopN admission returned inconsistent row facts".into());
    }
    let next_row = last
        .checked_add(1)
        .ok_or_else(|| "TopN input ordinal exhausted".to_string())?;
    let usage = WorkUsage {
        input_rows: processed,
        input_bytes,
        ..WorkUsage::default()
    };
    let drain_reached = transaction.record_admission(usage)?;
    let target = if next_row < chunk_rows {
        let next = InputPosition::new(input.stream_id, input.chunk_seq, next_row)?;
        if drain_reached {
            TopNAdmissionTarget::Drain {
                generation_id: next_generation_id(transaction)?,
                after_drain: AfterDrain::Admit(next),
            }
        } else {
            TopNAdmissionTarget::Continue(next)
        }
    } else if next_row == chunk_rows {
        advance_input(
            transaction,
            0,
            input_chunk.sequence + 1,
            input_state.consumed_frontier_lsn,
            WorkUsage {
                input_rows: input_chunk.rows,
                input_bytes: input_chunk.bytes,
                ..WorkUsage::default()
            },
        )?;
        match chunk(transaction, &input_state, input_chunk.sequence + 1)? {
            Some(next) if next.kind == ChunkKind::Frontier => TopNAdmissionTarget::Drain {
                generation_id: next_generation_id(transaction)?,
                after_drain: AfterDrain::Frontier(InputPosition::new(
                    next.stream_id,
                    next.sequence,
                    0,
                )?),
            },
            _ if drain_reached => TopNAdmissionTarget::Drain {
                generation_id: next_generation_id(transaction)?,
                after_drain: AfterDrain::FinishInput,
            },
            _ => TopNAdmissionTarget::Idle,
        }
    } else {
        return Err("TopN admission advanced beyond its input chunk".into());
    };
    Ok(TopNAdmission {
        facts: PrimitiveFacts {
            usage,
            state_rows: touched,
            continuation_rows: u64::from(!matches!(target, TopNAdmissionTarget::Idle)),
            output: OutputFacts::None,
        },
        target,
    })
}

fn next_generation_id(transaction: &mut StepTxn<'_, '_>) -> Result<i64, String> {
    let arguments = unsafe {
        [
            DatumWithOid::new(transaction.result_oid(), pg_sys::OIDOID),
            DatumWithOid::new(transaction.stage_id(), pg_sys::INT4OID),
        ]
    };
    let rows = transaction.read(
        r#"
        SELECT checkpoint.revision + 1
        FROM shiba_internal.operator_checkpoints AS checkpoint
        WHERE checkpoint.result_oid=$1 AND checkpoint.stage_id=$2
        "#,
        &arguments,
    )?;
    if rows.len() != 1 {
        return Err("TopN checkpoint generation is missing".into());
    }
    let generation = required(&rows.first(), 1, "TopN generation")?;
    validate_generation_id(generation)?;
    Ok(generation)
}

fn run_topn_selection(
    transaction: &mut StepTxn<'_, '_>,
    storage: &TopNStorage,
    expressions: &TopNExpressions,
    spec: &TopNSpec,
    generation_id: i64,
    progress: SelectionProgress,
) -> Result<TopNSelection, String> {
    let budget = transaction.budget();
    let max_rows = i64_from_usize(budget.max_input_rows, "TopN selection row budget")?;
    let max_bytes = i64_from_usize(budget.max_input_bytes, "TopN selection byte budget")?;
    let offset = progress.offset_remaining.to_string();
    let limit = progress.limit_remaining.to_string();
    let output_key = canonical_row_key_sql("selected_rows.output_row", &storage.output_type);
    let query = format!(
        r#"
        WITH cursor_boundary AS MATERIALIZED (
          SELECT * FROM {state} WHERE entry_id=$2
        ),
        source AS MATERIALIZED (
          SELECT input_row.*,
                 shiba_internal.effect_row_bytes(input_row.row_value) AS row_bytes
          FROM {state} AS input_row
          WHERE input_row.multiplicity > 0
            AND (
              $2 IS NULL OR EXISTS(
                SELECT 1
                FROM cursor_boundary AS boundary
                WHERE {keyset_after}
              )
            )
          ORDER BY {order_by}
          LIMIT $7
        ),
        measured AS MATERIALIZED (
          SELECT source.*,
                 row_number() OVER (ORDER BY {source_order}) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY {source_order}) AS running_bytes
          FROM source
        ),
        bounded AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_ordinal=1 OR running_bytes <= $8
        ),
        offset_prefix AS MATERIALIZED (
          SELECT bounded.*,
                 coalesce(
                   sum(multiplicity) OVER (
                     ORDER BY page_ordinal ROWS BETWEEN UNBOUNDED PRECEDING
                       AND 1 PRECEDING
                   ),
                   0::numeric
                 ) AS multiplicity_before
          FROM bounded
        ),
        offsetted AS MATERIALIZED (
          SELECT offset_prefix.*,
                 greatest(
                   multiplicity
                     - least(
                         multiplicity,
                         greatest($3::numeric-multiplicity_before,0::numeric)
                       ),
                   0::numeric
                 ) AS available
          FROM offset_prefix
        ),
        limit_prefix AS MATERIALIZED (
          SELECT offsetted.*,
                 coalesce(
                   sum(available) OVER (
                     ORDER BY page_ordinal ROWS BETWEEN UNBOUNDED PRECEDING
                       AND 1 PRECEDING
                   ),
                   0::numeric
                 ) AS available_before
          FROM offsetted
        ),
        limited AS MATERIALIZED (
          SELECT limit_prefix.*,
                 greatest(
                   least(
                     available,
                     greatest($4::numeric-available_before,0::numeric)
                   ),
                   0::numeric
                 ) AS base_take
          FROM limit_prefix
        ),
        new_boundary AS MATERIALIZED (
          SELECT entry_id,page_ordinal
          FROM limited
          WHERE $6::boolean
            AND $4::numeric > 0
            AND available > 0
            AND available_before < $4::numeric
            AND available_before + available >= $4::numeric
          ORDER BY page_ordinal
          LIMIT 1
        ),
        boundary_choice AS MATERIALIZED (
          SELECT $5::bigint AS entry_id,0::bigint AS page_ordinal,true AS persisted
          WHERE $5 IS NOT NULL
          UNION ALL
          SELECT new_boundary.entry_id,new_boundary.page_ordinal,false
          FROM new_boundary
          WHERE $5 IS NULL
        ),
        tie_boundary AS MATERIALIZED (
          SELECT state.*,boundary_choice.page_ordinal,boundary_choice.persisted
          FROM boundary_choice
          JOIN {state} AS state USING(entry_id)
        ),
        classified AS MATERIALIZED (
          SELECT input_row.*,
                 tie_boundary.entry_id AS boundary_entry_id,
                 tie_boundary.page_ordinal AS boundary_page_ordinal,
                 tie_boundary.persisted AS boundary_persisted,
                 CASE
                   WHEN tie_boundary.entry_id IS NULL THEN input_row.base_take
                   WHEN NOT $6::boolean THEN input_row.base_take
                   WHEN input_row.page_ordinal < tie_boundary.page_ordinal
                     THEN input_row.available
                   WHEN ({keys_equal}) THEN input_row.available
                   ELSE 0::numeric
                 END AS take_weight,
                 CASE
                   WHEN tie_boundary.entry_id IS NULL THEN false
                   ELSE ({keys_equal})
                 END AS tied
          FROM limited AS input_row
          LEFT JOIN tie_boundary ON true
        ),
        selected_rows AS MATERIALIZED (
          SELECT input_row.*,
                 ROW({outputs})::{output_type} AS output_row
          FROM classified AS input_row
          WHERE input_row.take_weight > 0
        ),
        keyed AS MATERIALIZED (
          SELECT selected_rows.*,
                 {output_key} AS output_key
          FROM selected_rows
        ),
        collapsed AS MATERIALIZED (
          SELECT output_key,min(page_ordinal) AS representative_ordinal,
                 sum(take_weight) AS multiplicity
          FROM keyed
          GROUP BY output_key
        ),
        candidate_rows AS MATERIALIZED (
          SELECT collapsed.output_key,keyed.output_row,collapsed.multiplicity
          FROM collapsed
          JOIN keyed
            ON keyed.output_key=collapsed.output_key
           AND keyed.page_ordinal=collapsed.representative_ordinal
        ),
        inserted AS (
          INSERT INTO {candidate} AS target(
            generation_id,output_key,output_row,multiplicity
          )
          SELECT $1,output_key,output_row,multiplicity
          FROM candidate_rows
          ON CONFLICT(generation_id,output_key) DO UPDATE
          SET output_row=EXCLUDED.output_row,
              multiplicity=target.multiplicity+EXCLUDED.multiplicity
          RETURNING 1
        ),
        last_processed AS MATERIALIZED (
          SELECT input_row.*
          FROM bounded AS page
          JOIN {state} AS input_row USING(entry_id)
          ORDER BY page.page_ordinal DESC
          LIMIT 1
        ),
        has_more AS MATERIALIZED (
          SELECT EXISTS(
            SELECT 1
            FROM {state} AS input_row
            JOIN last_processed AS boundary ON true
            WHERE input_row.multiplicity > 0 AND ({keyset_after})
          ) AS value
        ),
        summary AS MATERIALIZED (
          SELECT count(*)::bigint AS processed,
                 coalesce(sum(row_bytes),0)::bigint AS input_bytes,
                 (array_agg(entry_id ORDER BY page_ordinal DESC))[1] AS last_id,
                 greatest(
                   $3::numeric-coalesce(sum(multiplicity),0::numeric),
                   0::numeric
                 ) AS offset_remaining,
                 greatest(
                   $4::numeric-coalesce(sum(available),0::numeric),
                   0::numeric
                 ) AS limit_remaining,
                 (SELECT entry_id FROM boundary_choice) AS tie_boundary_row_id,
                 coalesce(
                   bool_or(
                     boundary_entry_id IS NOT NULL
                     AND page_ordinal > boundary_page_ordinal
                     AND NOT tied
                   ),
                   false
                 ) AS crossed_tie_boundary
          FROM classified
        )
        SELECT summary.processed,summary.input_bytes,summary.last_id,
               summary.offset_remaining::text,
               summary.limit_remaining::text,
               summary.tie_boundary_row_id,
               CASE
                 WHEN summary.processed=0 THEN true
                 WHEN NOT $6::boolean AND summary.limit_remaining=0 THEN true
                 WHEN $6::boolean
                      AND summary.tie_boundary_row_id IS NOT NULL
                      AND summary.crossed_tie_boundary THEN true
                 WHEN NOT coalesce((SELECT value FROM has_more),false) THEN true
                 ELSE false
               END,
               (SELECT count(*)::bigint FROM inserted)
        FROM summary
        "#,
        state = storage.input.sql(),
        candidate = storage.candidate.sql(),
        keyset_after = expressions.keyset_after,
        order_by = expressions.order_by,
        source_order = expressions.order_by.replace("input_row.", "source."),
        keys_equal = expressions.keys_equal,
        outputs = expressions.output_expressions,
        output_type = storage.output_type.sql(),
        output_key = output_key,
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(generation_id, pg_sys::INT8OID),
            DatumWithOid::new(progress.cursor.row_id, pg_sys::INT8OID),
            DatumWithOid::new(offset.as_str(), pg_sys::TEXTOID),
            DatumWithOid::new(limit.as_str(), pg_sys::TEXTOID),
            DatumWithOid::new(progress.tie_boundary_row_id, pg_sys::INT8OID),
            DatumWithOid::new(spec.with_ties, pg_sys::BOOLOID),
            DatumWithOid::new(max_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_bytes, pg_sys::INT8OID),
        ]
    };
    let rows = transaction.write(&query, &arguments)?;
    if rows.len() != 1 {
        return Err("TopN selection returned no summary".into());
    }
    let row = rows.first();
    let processed = nonnegative(
        required(&row, 1, "TopN selected input rows")?,
        "TopN selected input rows",
    )?;
    let input_bytes = nonnegative(
        required(&row, 2, "TopN selected input bytes")?,
        "TopN selected input bytes",
    )?;
    let last_row_id = row.get(3).map_err(|error| error.to_string())?;
    let offset_remaining = parse_u64_numeric(
        Some(&required::<String>(&row, 4, "TopN remaining OFFSET")?),
        "TopN OFFSET",
    )?;
    let limit_remaining = parse_u64_numeric(
        Some(&required::<String>(&row, 5, "TopN remaining LIMIT")?),
        "TopN LIMIT",
    )?;
    let tie_boundary_row_id = row.get(6).map_err(|error| error.to_string())?;
    let complete: bool = required(&row, 7, "TopN selection completion")?;
    let changed = nonnegative(
        required(&row, 8, "TopN candidate rows")?,
        "TopN candidate rows",
    )?;
    if processed == 0 && (!complete || last_row_id.is_some()) {
        return Err("TopN empty selection page is not complete".into());
    }
    Ok(TopNSelection {
        page: TopNPage {
            facts: PrimitiveFacts {
                usage: WorkUsage {
                    input_rows: processed,
                    input_bytes,
                    ..WorkUsage::default()
                },
                state_rows: changed,
                continuation_rows: 1,
                output: OutputFacts::None,
            },
            last_row_id,
            complete,
        },
        progress: SelectionProgress {
            cursor: TopNCursor {
                row_id: last_row_id,
            },
            offset_remaining,
            limit_remaining,
            tie_boundary_row_id,
        },
    })
}

fn run_topn_diff(
    transaction: &mut StepTxn<'_, '_>,
    storage: &TopNStorage,
    generation_id: i64,
    leg: DiffLeg,
    cursor: TopNDiffCursor,
) -> Result<TopNDiffPage, String> {
    cursor.validate()?;
    let causal_rows = transaction.read(
        &format!(
            "SELECT causal_lsn::text FROM {} \
             WHERE singleton AND dirty AND causal_lsn IS NOT NULL",
            storage.control.sql()
        ),
        &[],
    )?;
    if causal_rows.len() != 1 {
        return Err("TopN dirty state has no unique causal LSN".into());
    }
    let lsn: String = required(&causal_rows.first(), 1, "TopN causal LSN")?;
    let output = transaction.output()?.clone();
    let budget = transaction.budget();
    let max_rows = i64::min(
        i64::min(
            i64_from_usize(budget.max_input_rows, "TopN diff input row budget")?,
            i64_from_usize(budget.max_output_rows, "TopN diff output row budget")?,
        ),
        output.target_rows,
    );
    let raw_rows = max_rows
        .checked_add(1)
        .ok_or_else(|| "TopN diff row budget overflow".to_string())?;
    let max_bytes = i64::min(
        i64::min(
            i64_from_usize(budget.max_input_bytes, "TopN diff input byte budget")?,
            i64_from_usize(budget.max_output_bytes, "TopN diff output byte budget")?,
        ),
        output.target_bytes,
    );
    let cursor_predicate = |identity: &str| {
        if cursor.repeat {
            format!("{identity}>=$2")
        } else if cursor.row_id.is_some() {
            format!("{identity}>$2")
        } else {
            "$2 IS NULL".into()
        }
    };
    let (source, compared, mutation, weight) = match leg {
        DiffLeg::Remove => (
            format!(
                r#"
                SELECT visible.visible_id AS row_id,visible.output_key,
                       visible.output_row,visible.multiplicity,
                       shiba_internal.effect_row_bytes(visible.output_row) AS row_bytes
                FROM {visible} AS visible
                WHERE {cursor_predicate}
                ORDER BY visible.visible_id
                LIMIT $5
                "#,
                visible = storage.visible.sql(),
                cursor_predicate = cursor_predicate("visible.visible_id"),
            ),
            format!(
                r#"
                SELECT bounded_prefix.*,
                       bounded_prefix.multiplicity
                         -coalesce(candidate.multiplicity,0::numeric) AS delta
                FROM bounded_prefix
                LEFT JOIN {candidate} AS candidate
                  ON candidate.generation_id=$1
                 AND candidate.output_key=bounded_prefix.output_key
                "#,
                candidate = storage.candidate.sql(),
            ),
            format!(
                r#"
                deleted AS (
                  DELETE FROM {visible} AS visible
                  USING differences
                  WHERE visible.visible_id=differences.row_id
                    AND visible.multiplicity=differences.slice
                  RETURNING 1
                ),
                changed AS (
                  UPDATE {visible} AS visible
                  SET multiplicity=visible.multiplicity-differences.slice
                  FROM differences
                  WHERE visible.visible_id=differences.row_id
                    AND visible.multiplicity>differences.slice
                  RETURNING 1
                )
                "#,
                visible = storage.visible.sql(),
            ),
            "-differences.slice",
        ),
        DiffLeg::Add => (
            format!(
                r#"
                SELECT candidate.candidate_id AS row_id,candidate.output_key,
                       candidate.output_row,candidate.multiplicity,
                       shiba_internal.effect_row_bytes(candidate.output_row) AS row_bytes
                FROM {candidate} AS candidate
                WHERE candidate.generation_id=$1
                  AND {cursor_predicate}
                ORDER BY candidate.candidate_id
                LIMIT $5
                "#,
                candidate = storage.candidate.sql(),
                cursor_predicate = cursor_predicate("candidate.candidate_id"),
            ),
            format!(
                r#"
                SELECT bounded_prefix.*,
                       bounded_prefix.multiplicity
                         -coalesce(visible.multiplicity,0::numeric) AS delta
                FROM bounded_prefix
                LEFT JOIN {visible} AS visible
                  ON visible.output_key=bounded_prefix.output_key
                "#,
                visible = storage.visible.sql(),
            ),
            format!(
                r#"
                changed AS (
                  INSERT INTO {visible} AS target(
                    output_key,output_row,multiplicity
                  )
                  SELECT output_key,output_row,slice::numeric
                  FROM differences
                  ON CONFLICT(output_key) DO UPDATE
                  SET output_row=EXCLUDED.output_row,
                      multiplicity=target.multiplicity+EXCLUDED.multiplicity
                  RETURNING 1
                ),
                deleted AS (
                  SELECT 1 WHERE false
                )
                "#,
                visible = storage.visible.sql(),
            ),
            "differences.slice",
        ),
    };
    let query = format!(
        r#"
        WITH source AS MATERIALIZED (
          {source}
        ),
        numbered AS MATERIALIZED (
          SELECT source.*,
                 row_number() OVER (ORDER BY row_id) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY row_id) AS running_bytes
          FROM source
        ),
        bounded_prefix AS MATERIALIZED (
          SELECT numbered.*
          FROM numbered
          WHERE page_ordinal<=$3
            AND (page_ordinal=1 OR running_bytes<=$4)
        ),
        joined AS MATERIALIZED (
          {compared}
        ),
        marked AS MATERIALIZED (
          SELECT joined.*,
                 min(
                   CASE WHEN delta > 9223372036854775807::numeric
                        THEN page_ordinal
                   END
                 ) OVER () AS first_huge_ordinal
          FROM joined
        ),
        compared AS MATERIALIZED (
          SELECT marked.*
          FROM marked
          WHERE first_huge_ordinal IS NULL
             OR page_ordinal<=first_huge_ordinal
        ),
        differences AS MATERIALIZED (
          SELECT compared.*,
                 least(delta,9223372036854775807::numeric)::bigint AS slice
          FROM compared
          WHERE delta>0
        ),
        stats AS MATERIALIZED (
          SELECT count(*)::bigint AS compared_rows,
                 coalesce(sum(row_bytes),0)::bigint AS compared_bytes,
                 (array_agg(row_id ORDER BY page_ordinal DESC))[1] AS last_id,
                 coalesce(bool_or(delta>9223372036854775807::numeric),false)
                   AS repeat_cursor,
                 (SELECT count(*)::bigint FROM differences) AS emitted_rows,
                 (SELECT coalesce(sum(row_bytes),0)::bigint FROM differences)
                   AS emitted_bytes
          FROM compared
        ),
        appended AS MATERIALIZED (
          SELECT append.outcome,append.appended_chunk_seq
          FROM stats
          CROSS JOIN LATERAL shiba_internal.append_effect_stream_chunk(
            $7,$8,'data',stats.emitted_rows,stats.emitted_bytes,$6::pg_lsn
          ) AS append
          WHERE stats.emitted_rows>0
        ),
        payload_insert AS (
          INSERT INTO {output_payload}(
            stream_id,chunk_seq,row_ordinal,weight,row_value
          )
          SELECT $7,appended.appended_chunk_seq,
                 row_number() OVER (ORDER BY differences.page_ordinal)-1,
                 {weight},differences.output_row
          FROM differences
          CROSS JOIN appended
          WHERE appended.outcome='appended'
          RETURNING 1
        ),
        {mutation}
        SELECT stats.compared_rows,stats.compared_bytes,stats.last_id,
               (SELECT count(*) FROM source)
                 =(SELECT count(*) FROM bounded_prefix)
                 AND (SELECT count(*) FROM bounded_prefix)=stats.compared_rows
                 AND NOT stats.repeat_cursor AS complete,
               stats.repeat_cursor,stats.emitted_rows,stats.emitted_bytes,
               appended.outcome,appended.appended_chunk_seq,
               (SELECT count(*)::bigint FROM payload_insert),
               (SELECT count(*)::bigint FROM changed)
                 +(SELECT count(*)::bigint FROM deleted)
        FROM stats
        LEFT JOIN appended ON true
        "#,
        output_payload = storage.output_payload.sql(),
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(generation_id, pg_sys::INT8OID),
            DatumWithOid::new(cursor.row_id, pg_sys::INT8OID),
            DatumWithOid::new(max_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_bytes, pg_sys::INT8OID),
            DatumWithOid::new(raw_rows, pg_sys::INT8OID),
            DatumWithOid::new(lsn.as_str(), pg_sys::TEXTOID),
            DatumWithOid::new(output.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(output.next_chunk_seq, pg_sys::INT8OID),
        ]
    };
    let rows = transaction.write(&query, &arguments)?;
    if rows.len() != 1 {
        return Err("TopN diff returned no summary".into());
    }
    let row = rows.first();
    let compared_rows = nonnegative(
        required(&row, 1, "TopN compared rows")?,
        "TopN compared rows",
    )?;
    let compared_bytes = nonnegative(
        required(&row, 2, "TopN compared bytes")?,
        "TopN compared bytes",
    )?;
    let last_row_id = row.get(3).map_err(|error| error.to_string())?;
    let complete = required(&row, 4, "TopN diff completion")?;
    let repeat_cursor = required(&row, 5, "TopN residual cursor")?;
    let emitted = nonnegative(required(&row, 6, "TopN diff rows")?, "TopN diff rows")?;
    let emitted_bytes = nonnegative(required(&row, 7, "TopN diff bytes")?, "TopN diff bytes")?;
    let append_outcome = row.get::<String>(8).map_err(|error| error.to_string())?;
    let appended_sequence = row.get::<i64>(9).map_err(|error| error.to_string())?;
    let inserted = nonnegative(
        required(&row, 10, "TopN payload rows")?,
        "TopN payload rows",
    )?;
    let mutated = nonnegative(
        required(&row, 11, "TopN visible mutations")?,
        "TopN visible mutations",
    )?;
    let output_facts = if emitted == 0 {
        if append_outcome.is_some() || appended_sequence.is_some() || inserted != 0 || mutated != 0
        {
            return Err("TopN appended or mutated an empty diff".into());
        }
        OutputFacts::None
    } else {
        if append_outcome.as_deref() != Some("appended")
            || appended_sequence != Some(output.next_chunk_seq)
            || inserted != emitted
            || mutated != emitted
        {
            return Err("TopN diff append is inconsistent".into());
        }
        OutputFacts::Data {
            chunk_seq: output.next_chunk_seq,
        }
    };
    Ok(TopNDiffPage {
        facts: PrimitiveFacts {
            usage: WorkUsage {
                input_rows: compared_rows,
                input_bytes: compared_bytes,
                output_rows: emitted,
                output_bytes: emitted_bytes,
            },
            state_rows: mutated,
            continuation_rows: 1,
            output: output_facts,
        },
        last_row_id,
        complete,
        repeat_cursor,
    })
}

fn run_topn_cleanup(
    transaction: &mut StepTxn<'_, '_>,
    storage: &TopNStorage,
    generation_id: i64,
    cursor: TopNCursor,
    after_drain: AfterDrain,
) -> Result<TopNPage, String> {
    let budget = transaction.budget();
    let max_rows = i64_from_usize(budget.max_input_rows, "TopN cleanup row budget")?;
    let raw_rows = max_rows
        .checked_add(1)
        .ok_or_else(|| "TopN cleanup row budget overflow".to_string())?;
    let max_bytes = i64_from_usize(budget.max_input_bytes, "TopN cleanup byte budget")?;
    let query = format!(
        r#"
        WITH source AS MATERIALIZED (
          SELECT candidate_id,
                 shiba_internal.effect_row_bytes(output_row) AS row_bytes
          FROM {candidate}
          WHERE generation_id=$1
            AND ($2 IS NULL OR candidate_id >= $2)
          ORDER BY candidate_id
          LIMIT $5
        ),
        measured AS MATERIALIZED (
          SELECT source.*,
                 row_number() OVER (ORDER BY candidate_id) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY candidate_id) AS running_bytes
          FROM source
        ),
        selected AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_ordinal <= $3
            AND (page_ordinal=1 OR running_bytes <= $4)
        ),
        deleted AS (
          DELETE FROM {candidate} AS candidate
          USING selected
          WHERE candidate.candidate_id=selected.candidate_id
          RETURNING candidate.candidate_id
        )
        SELECT count(*)::bigint,
               coalesce(sum(row_bytes),0)::bigint,
               (array_agg(candidate_id ORDER BY page_ordinal DESC))[1],
               (SELECT count(*) FROM source)=(SELECT count(*) FROM selected),
               (SELECT count(*)::bigint FROM deleted)
        FROM selected
        "#,
        candidate = storage.candidate.sql(),
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(generation_id, pg_sys::INT8OID),
            DatumWithOid::new(cursor.row_id, pg_sys::INT8OID),
            DatumWithOid::new(max_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_bytes, pg_sys::INT8OID),
            DatumWithOid::new(raw_rows, pg_sys::INT8OID),
        ]
    };
    let rows = transaction.write(&query, &arguments)?;
    if rows.len() != 1 {
        return Err("TopN cleanup returned no summary".into());
    }
    let row = rows.first();
    let deleted = nonnegative(required(&row, 1, "TopN cleanup rows")?, "TopN cleanup rows")?;
    let bytes = nonnegative(
        required(&row, 2, "TopN cleanup bytes")?,
        "TopN cleanup bytes",
    )?;
    let last_row_id = row.get(3).map_err(|error| error.to_string())?;
    let complete: bool = required(&row, 4, "TopN cleanup completion")?;
    let mutation_count = nonnegative(
        required(&row, 5, "TopN candidate deletes")?,
        "TopN candidate deletes",
    )?;
    if mutation_count != deleted {
        return Err("TopN cleanup delete count is inconsistent".into());
    }
    let continuation_rows = u64::from(!complete || !matches!(after_drain, AfterDrain::FinishInput));
    Ok(TopNPage {
        facts: PrimitiveFacts {
            usage: WorkUsage {
                input_rows: deleted,
                input_bytes: bytes,
                ..WorkUsage::default()
            },
            state_rows: mutation_count,
            continuation_rows,
            output: OutputFacts::None,
        },
        last_row_id,
        complete,
    })
}

fn finish_topn_drain(
    transaction: &mut StepTxn<'_, '_>,
    storage: &TopNStorage,
) -> Result<u64, String> {
    let candidate_rows = transaction.read(
        &format!("SELECT count(*)::bigint FROM {}", storage.candidate.sql()),
        &[],
    )?;
    if required::<i64>(
        &candidate_rows.first(),
        1,
        "TopN candidate rows after cleanup",
    )? != 0
    {
        return Err("TopN cleanup left candidate rows behind".into());
    }
    let reset = transaction.write(
        &format!(
            "UPDATE {} SET dirty=false,causal_lsn=NULL \
             WHERE singleton AND dirty AND causal_lsn IS NOT NULL \
             RETURNING singleton",
            storage.control.sql()
        ),
        &[],
    )?;
    if reset.len() != 1 {
        return Err("TopN Drain did not reset its dirty control state".into());
    }
    Ok(1)
}

fn run_topn_frontier(
    transaction: &mut StepTxn<'_, '_>,
    input: InputPosition,
) -> Result<PrimitiveFacts, String> {
    if input.row_ordinal != 0 {
        return Err("TopN frontier has a row cursor".into());
    }
    let input_state = transaction.input(0)?.clone();
    let frontier = chunk(transaction, &input_state, input.chunk_seq)?
        .ok_or_else(|| "TopN frontier chunk is missing".to_string())?;
    if frontier.kind != ChunkKind::Frontier || frontier.stream_id != input.stream_id {
        return Err("TopN frontier continuation references data".into());
    }
    let output = append_frontier(transaction, frontier.lsn)?;
    advance_input(
        transaction,
        0,
        frontier.sequence + 1,
        frontier.lsn,
        WorkUsage::default(),
    )?;
    transaction.reset_admission();
    Ok(PrimitiveFacts {
        continuation_rows: 0,
        output,
        ..PrimitiveFacts::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::WorkUsage;

    fn budget() -> WorkBudget {
        WorkBudget::new(2, 20, 1, 12)
    }

    fn position(row: i64) -> InputPosition {
        InputPosition::new(3, 9, row).unwrap()
    }

    fn frontier_position() -> InputPosition {
        InputPosition::new(3, 10, 0).unwrap()
    }

    fn internal_page(last_row_id: Option<i64>, complete: bool) -> TopNPage {
        TopNPage {
            facts: PrimitiveFacts {
                usage: WorkUsage {
                    input_rows: u64::from(last_row_id.is_some()),
                    input_bytes: u64::from(last_row_id.is_some()) * 8,
                    ..WorkUsage::default()
                },
                state_rows: u64::from(last_row_id.is_some()),
                continuation_rows: 1,
                output: OutputFacts::None,
            },
            last_row_id,
            complete,
        }
    }

    fn diff_page(last_row_id: Option<i64>, complete: bool, chunk_seq: Option<i64>) -> TopNDiffPage {
        let output_rows = u64::from(chunk_seq.is_some());
        TopNDiffPage {
            facts: PrimitiveFacts {
                usage: WorkUsage {
                    input_rows: u64::from(last_row_id.is_some()),
                    input_bytes: u64::from(last_row_id.is_some()) * 8,
                    output_rows,
                    output_bytes: output_rows * 9,
                },
                state_rows: output_rows,
                continuation_rows: 1,
                output: chunk_seq.map_or(OutputFacts::None, |chunk_seq| OutputFacts::Data {
                    chunk_seq,
                }),
            },
            last_row_id,
            complete,
            repeat_cursor: false,
        }
    }

    fn committed_continuation(transition: TopNTransition) -> TopNContinuation {
        let TopNTransition::Committed {
            continuation: Some(continuation),
            ..
        } = transition
        else {
            panic!("step should have a continuation");
        };
        continuation
    }

    fn select_continuation(machine: TopNMachine) -> TopNContinuation {
        let admitted = machine
            .apply(
                TopNContinuation {
                    input_stream_id: 3,
                    input: Some(position(1)),
                    phase: TopNPhase::Admit,
                },
                TopNActionResult::Admitted(TopNAdmission {
                    facts: PrimitiveFacts {
                        usage: WorkUsage {
                            input_rows: 1,
                            input_bytes: 8,
                            ..WorkUsage::default()
                        },
                        state_rows: 1,
                        continuation_rows: 1,
                        output: OutputFacts::None,
                    },
                    target: TopNAdmissionTarget::Drain {
                        generation_id: 17,
                        after_drain: AfterDrain::Frontier(frontier_position()),
                    },
                }),
                budget(),
            )
            .unwrap();
        committed_continuation(admitted)
    }

    #[test]
    fn strict_phase_codes_have_no_idle_or_unknown_decoder() {
        assert_eq!(
            TopNPhaseKind::from_code(TopNPhaseKind::Select.code()).unwrap(),
            TopNPhaseKind::Select
        );
        assert!(PhaseCode::active(0).is_err());
        assert!(TopNPhaseKind::from_code(PhaseCode::active(99).unwrap()).is_err());
    }

    #[test]
    fn large_offset_limit_and_ties_resume_with_scalar_ids() {
        let machine = TopNMachine::new(2, 3, true);
        let mut continuation = select_continuation(machine);
        assert!(matches!(
            continuation.phase,
            TopNPhase::Select {
                progress: SelectionProgress {
                    offset_remaining: 3,
                    limit_remaining: 2,
                    tie_boundary_row_id: None,
                    ..
                },
                ..
            }
        ));

        continuation = committed_continuation(
            machine
                .apply(
                    continuation,
                    TopNActionResult::Selected(TopNSelection {
                        page: internal_page(Some(21), false),
                        progress: SelectionProgress {
                            cursor: TopNCursor { row_id: Some(21) },
                            offset_remaining: 1,
                            limit_remaining: 2,
                            tie_boundary_row_id: None,
                        },
                    }),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(
            machine.action(continuation).unwrap(),
            TopNAction::SelectCandidates {
                progress: SelectionProgress {
                    cursor: TopNCursor { row_id: Some(21) },
                    ..
                },
                ..
            }
        ));

        continuation = committed_continuation(
            machine
                .apply(
                    continuation,
                    TopNActionResult::Selected(TopNSelection {
                        page: internal_page(Some(25), false),
                        progress: SelectionProgress {
                            cursor: TopNCursor { row_id: Some(25) },
                            offset_remaining: 0,
                            limit_remaining: 0,
                            tie_boundary_row_id: Some(25),
                        },
                    }),
                    budget(),
                )
                .unwrap(),
        );

        continuation = committed_continuation(
            machine
                .apply(
                    continuation,
                    TopNActionResult::Selected(TopNSelection {
                        page: internal_page(Some(28), true),
                        progress: SelectionProgress {
                            cursor: TopNCursor { row_id: Some(28) },
                            offset_remaining: 0,
                            limit_remaining: 0,
                            tie_boundary_row_id: Some(25),
                        },
                    }),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(
            continuation.phase,
            TopNPhase::Diff {
                leg: DiffLeg::Remove,
                ..
            }
        ));
    }

    #[test]
    fn final_cleanup_can_resume_the_same_input_chunk() {
        let machine = TopNMachine::new(5, 0, false);
        let transition = machine
            .apply(
                TopNContinuation {
                    input_stream_id: 3,
                    input: None,
                    phase: TopNPhase::Cleanup {
                        generation_id: 17,
                        cursor: TopNCursor::default(),
                        after_drain: AfterDrain::Admit(position(2)),
                    },
                },
                TopNActionResult::Cleaned(internal_page(None, true)),
                budget(),
            )
            .unwrap();
        assert_eq!(
            committed_continuation(transition),
            TopNContinuation {
                input_stream_id: 3,
                input: Some(position(2)),
                phase: TopNPhase::Admit,
            }
        );
    }

    #[test]
    fn frontier_waits_for_remove_add_and_cleanup() {
        let machine = TopNMachine::new(5, 0, false);
        let mut continuation = TopNContinuation {
            input_stream_id: 3,
            input: None,
            phase: TopNPhase::Diff {
                generation_id: 17,
                leg: DiffLeg::Remove,
                cursor: TopNDiffCursor::default(),
                after_drain: AfterDrain::Frontier(frontier_position()),
            },
        };

        assert!(machine
            .apply(
                continuation,
                TopNActionResult::FrontierForwarded(PrimitiveFacts {
                    output: OutputFacts::Frontier { chunk_seq: 50 },
                    ..PrimitiveFacts::default()
                }),
                budget(),
            )
            .is_err());

        continuation = committed_continuation(
            machine
                .apply(
                    continuation,
                    TopNActionResult::Diffed(diff_page(Some(31), true, Some(51))),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(
            continuation.phase,
            TopNPhase::Diff {
                leg: DiffLeg::Add,
                ..
            }
        ));

        continuation = committed_continuation(
            machine
                .apply(
                    continuation,
                    TopNActionResult::Diffed(diff_page(None, true, None)),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(continuation.phase, TopNPhase::Cleanup { .. }));

        continuation = committed_continuation(
            machine
                .apply(
                    continuation,
                    TopNActionResult::Cleaned(internal_page(None, true)),
                    budget(),
                )
                .unwrap(),
        );
        assert_eq!(continuation.phase, TopNPhase::Frontier);
        assert!(matches!(
            machine.action(continuation).unwrap(),
            TopNAction::ForwardFrontier { .. }
        ));
    }

    #[test]
    fn diff_cursor_advances_on_zero_difference_and_repeats_only_residuals() {
        let machine = TopNMachine::new(5, 0, false);
        let start = TopNContinuation {
            input_stream_id: 3,
            input: None,
            phase: TopNPhase::Diff {
                generation_id: 17,
                leg: DiffLeg::Remove,
                cursor: TopNDiffCursor::default(),
                after_drain: AfterDrain::FinishInput,
            },
        };
        let after_equal_prefix = committed_continuation(
            machine
                .apply(
                    start,
                    TopNActionResult::Diffed(diff_page(Some(31), false, None)),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(
            after_equal_prefix.phase,
            TopNPhase::Diff {
                cursor: TopNDiffCursor {
                    row_id: Some(31),
                    repeat: false,
                },
                ..
            }
        ));
        assert!(machine
            .apply(
                after_equal_prefix,
                TopNActionResult::Diffed(diff_page(Some(31), false, None)),
                budget(),
            )
            .is_err());

        let mut residual = diff_page(Some(41), false, Some(52));
        residual.repeat_cursor = true;
        let after_residual = committed_continuation(
            machine
                .apply(start, TopNActionResult::Diffed(residual), budget())
                .unwrap(),
        );
        assert!(matches!(
            after_residual.phase,
            TopNPhase::Diff {
                cursor: TopNDiffCursor {
                    row_id: Some(41),
                    repeat: true,
                },
                ..
            }
        ));
        let after_final_slice = committed_continuation(
            machine
                .apply(
                    after_residual,
                    TopNActionResult::Diffed(diff_page(Some(41), false, Some(53))),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(
            after_final_slice.phase,
            TopNPhase::Diff {
                cursor: TopNDiffCursor {
                    row_id: Some(41),
                    repeat: false,
                },
                ..
            }
        ));
    }

    #[test]
    fn diff_repeat_cursor_roundtrips_through_the_typed_continuation() {
        let continuation = TopNContinuation {
            input_stream_id: 3,
            input: None,
            phase: TopNPhase::Diff {
                generation_id: 17,
                leg: DiffLeg::Add,
                cursor: TopNDiffCursor {
                    row_id: Some(41),
                    repeat: true,
                },
                after_drain: AfterDrain::Frontier(frontier_position()),
            },
        };
        let fields = encode_topn_fields(continuation);
        assert!(fields.cursor_repeat);
        assert_eq!(decode_topn_fields(fields).unwrap(), continuation);
    }

    #[test]
    fn zero_limit_skips_candidate_selection() {
        let machine = TopNMachine::new(0, u64::MAX, true);
        let continuation = select_continuation(machine);
        assert!(matches!(
            continuation.phase,
            TopNPhase::Diff {
                leg: DiffLeg::Remove,
                ..
            }
        ));
    }

    #[test]
    fn selection_rejects_counter_and_tie_boundary_regressions() {
        let machine = TopNMachine::new(2, 3, true);
        let continuation = select_continuation(machine);
        let invalid = TopNSelection {
            page: internal_page(Some(21), false),
            progress: SelectionProgress {
                cursor: TopNCursor { row_id: Some(21) },
                offset_remaining: 4,
                limit_remaining: 2,
                tie_boundary_row_id: None,
            },
        };
        assert!(machine
            .apply(continuation, TopNActionResult::Selected(invalid), budget(),)
            .is_err());
    }

    #[test]
    fn one_oversized_effect_row_is_allowed_but_two_are_not() {
        let machine = TopNMachine::new(5, 0, false);
        let durable = TopNContinuation {
            input_stream_id: 3,
            input: None,
            phase: TopNPhase::Diff {
                generation_id: 17,
                leg: DiffLeg::Add,
                cursor: TopNDiffCursor::default(),
                after_drain: AfterDrain::FinishInput,
            },
        };
        let oversized = TopNDiffPage {
            facts: PrimitiveFacts {
                usage: WorkUsage {
                    input_rows: 1,
                    input_bytes: 21,
                    output_rows: 1,
                    output_bytes: 13,
                },
                state_rows: 1,
                continuation_rows: 1,
                output: OutputFacts::Data { chunk_seq: 61 },
            },
            last_row_id: Some(1),
            complete: false,
            repeat_cursor: false,
        };
        machine
            .apply(durable, TopNActionResult::Diffed(oversized), budget())
            .unwrap();

        let two_rows = TopNDiffPage {
            facts: PrimitiveFacts {
                usage: WorkUsage {
                    input_rows: 2,
                    input_bytes: 21,
                    ..WorkUsage::default()
                },
                continuation_rows: 1,
                ..PrimitiveFacts::default()
            },
            last_row_id: Some(2),
            complete: false,
            repeat_cursor: false,
        };
        assert!(machine
            .apply(durable, TopNActionResult::Diffed(two_rows), budget())
            .is_err());
    }

    #[test]
    fn admission_can_commit_without_starting_a_drain() {
        let machine = TopNMachine::new(5, 0, false);
        let transition = machine
            .apply(
                TopNContinuation {
                    input_stream_id: 3,
                    input: Some(position(0)),
                    phase: TopNPhase::Admit,
                },
                TopNActionResult::Admitted(TopNAdmission {
                    facts: PrimitiveFacts {
                        usage: WorkUsage {
                            input_rows: 1,
                            input_bytes: 8,
                            ..WorkUsage::default()
                        },
                        state_rows: 2,
                        continuation_rows: 0,
                        output: OutputFacts::None,
                    },
                    target: TopNAdmissionTarget::Idle,
                }),
                budget(),
            )
            .unwrap();
        assert!(matches!(
            transition,
            TopNTransition::Committed {
                continuation: None,
                ..
            }
        ));
    }

    #[test]
    fn drain_continuation_does_not_retain_consumed_chunk() {
        let machine = TopNMachine::new(5, 0, false);
        let continuation = committed_continuation(
            machine
                .apply(
                    TopNContinuation {
                        input_stream_id: 3,
                        input: Some(position(1)),
                        phase: TopNPhase::Admit,
                    },
                    TopNActionResult::Admitted(TopNAdmission {
                        facts: PrimitiveFacts {
                            usage: WorkUsage {
                                input_rows: 1,
                                input_bytes: 8,
                                ..WorkUsage::default()
                            },
                            state_rows: 2,
                            continuation_rows: 1,
                            output: OutputFacts::None,
                        },
                        target: TopNAdmissionTarget::Drain {
                            generation_id: 17,
                            after_drain: AfterDrain::FinishInput,
                        },
                    }),
                    budget(),
                )
                .unwrap(),
        );
        assert_eq!(continuation.input_stream_id, 3);
        assert_eq!(continuation.input, None);
        assert!(matches!(continuation.phase, TopNPhase::Select { .. }));
    }

    #[test]
    fn partial_chunk_drain_keeps_only_its_resume_target() {
        let machine = TopNMachine::new(5, 0, false);
        let continuation = committed_continuation(
            machine
                .apply(
                    TopNContinuation {
                        input_stream_id: 3,
                        input: Some(position(0)),
                        phase: TopNPhase::Admit,
                    },
                    TopNActionResult::Admitted(TopNAdmission {
                        facts: PrimitiveFacts {
                            usage: WorkUsage {
                                input_rows: 2,
                                input_bytes: 16,
                                ..WorkUsage::default()
                            },
                            state_rows: 3,
                            continuation_rows: 1,
                            output: OutputFacts::None,
                        },
                        target: TopNAdmissionTarget::Drain {
                            generation_id: 17,
                            after_drain: AfterDrain::Admit(position(2)),
                        },
                    }),
                    budget(),
                )
                .unwrap(),
        );
        assert_eq!(continuation.input, None);
        assert!(matches!(
            continuation.phase,
            TopNPhase::Select {
                after_drain: AfterDrain::Admit(input),
                ..
            } if input == position(2)
        ));
    }
}
