//! Bounded scalar control flow for a durable Window stage.
//!
//! PostgreSQL owns partition keys, ordering values, peer comparisons, frame
//! bounds, function arguments, candidate rows, and visible rows. Rust keeps
//! only the phase and stable relation IDs needed to resume one database
//! primitive after a commit or restart.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiTupleTable};

use crate::kernel::{
    advance_input, append_frontier, attribute_matches_slot, canonical_row_key_sql, chunk,
    compile_named_outputs, compile_stage_bindings, next_chunk, payload_facts,
    scalar_work_bytes_sql, validate_output_attributes, AttributeRef, BindingInput, ChunkKind,
    InputPosition, OutputFacts, PageFacts, PhaseCode, PrimitiveFacts, ProducerKind, RelationRef,
    StepTxn, TypeRef, WorkUsage,
};
use crate::logical::model::{
    DataflowPlan, DataflowStage, OperatorSpec, OutputSlot, SlotType, WindowExpr, WindowSpec,
};
use crate::logical::{StepExecution, WorkBudget};
use crate::postgres::{format_lsn, quote_identifier};
use crate::scalar_sql::{compile_scalar_expression, SqlBinding};

use super::aggregate_capability::{
    decode_aggregate_capability, initial_state_sql, AggregateCapability, AGGREGATE_CAPABILITY_SQL,
};
use super::btree::{
    resolve_client as resolve_btree_client, resolve_step as resolve_btree_step, BtreeOrder,
};
use super::register::{
    catalog_continuation, catalog_state, column_sql, qualified_internal, resolve_relation_oid,
};

const ADMIT_PHASE: i16 = 1;
const ENUMERATE_PHASE: i16 = 2;
const PEERS_PHASE: i16 = 3;
const FRAMES_PHASE: i16 = 4;
const FOLD_AGGREGATE_PHASE: i16 = 5;
const EVALUATE_PHASE: i16 = 6;
const DIFF_PHASE: i16 = 7;
const CLEANUP_PHASE: i16 = 8;
const FRONTIER_PHASE: i16 = 9;

/// Caps aggregate-frame control work even when every frame is empty.
///
/// Input rows and bytes remain the primary budget. An output ordinal whose
/// frame contains no rows consumes neither, so it also consumes one explicit
/// work item before the step may visit the next ordinal.
const WINDOW_FOLD_WORK_ITEM_CAP: usize = 64;

/// What follows the last dirty partition created by one admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AfterPartitions {
    /// Resume the same immutable data chunk at a later row.
    Admit(InputPosition),
    /// The final cleanup transaction also consumes the completed data chunk.
    FinishInput,
    /// Forward this pinned frontier only after every partition diff commits.
    Frontier(InputPosition),
}

/// One stable row reference into an operator-owned work relation.
///
/// The referenced row contains any typed ordering or frame value. Those
/// values never become continuation columns and never enter Rust.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct WindowCursor {
    pub(crate) row_id: Option<i64>,
}

impl WindowCursor {
    fn validate(self) -> Result<(), String> {
        if self.row_id.is_some_and(|row_id| row_id <= 0) {
            return Err("Window cursor is not positive".into());
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
pub(crate) struct WindowDiffCursor {
    pub(crate) row_id: Option<i64>,
    pub(crate) repeat: bool,
}

impl WindowDiffCursor {
    fn validate(self) -> Result<(), String> {
        if self.row_id.is_some_and(|row_id| row_id <= 0) {
            return Err("Window Diff cursor is not positive".into());
        }
        if self.repeat && self.row_id.is_none() {
            return Err("Window Diff repeat cursor omitted its row".into());
        }
        Ok(())
    }
}

/// Resume point for one strict, input-ordered aggregate frame fold.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WindowFoldCursor {
    pub(crate) output_ordinal: i64,
    pub(crate) last_frame_ordinal: Option<i64>,
    /// The accumulator contains the complete frame and is waiting for its
    /// exact materialized value to fit a fresh or remaining step budget.
    pub(crate) ready_to_finalize: bool,
}

impl WindowFoldCursor {
    fn validate(self) -> Result<(), String> {
        if self.output_ordinal <= 0 || self.last_frame_ordinal.is_some_and(|ordinal| ordinal <= 0) {
            return Err("Window aggregate fold cursor is not positive".into());
        }
        Ok(())
    }
}

/// Cleanup visits exactly one typed work relation per step.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct WindowCleanupCursor {
    pub(crate) relation_ordinal: u32,
    pub(crate) row: WindowCursor,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DiffLeg {
    /// Retract visible rows that are absent from, or differ from, candidates.
    Remove,
    /// Publish candidate rows that are not currently visible.
    Add,
}

/// The persisted phase tag. Codes are intentionally exact: there is no old
/// phase decoder and no catch-all phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WindowPhaseKind {
    Admit,
    Enumerate,
    Peers,
    Frames,
    FoldAggregate,
    Evaluate,
    Diff,
    Cleanup,
    Frontier,
}

impl WindowPhaseKind {
    pub(crate) fn code(self) -> PhaseCode {
        let code = match self {
            Self::Admit => ADMIT_PHASE,
            Self::Enumerate => ENUMERATE_PHASE,
            Self::Peers => PEERS_PHASE,
            Self::Frames => FRAMES_PHASE,
            Self::FoldAggregate => FOLD_AGGREGATE_PHASE,
            Self::Evaluate => EVALUATE_PHASE,
            Self::Diff => DIFF_PHASE,
            Self::Cleanup => CLEANUP_PHASE,
            Self::Frontier => FRONTIER_PHASE,
        };
        PhaseCode::active(code).expect("Window phase codes are positive")
    }

    pub(crate) fn from_code(code: PhaseCode) -> Result<Self, String> {
        match code.value() {
            ADMIT_PHASE => Ok(Self::Admit),
            ENUMERATE_PHASE => Ok(Self::Enumerate),
            PEERS_PHASE => Ok(Self::Peers),
            FRAMES_PHASE => Ok(Self::Frames),
            FOLD_AGGREGATE_PHASE => Ok(Self::FoldAggregate),
            EVALUATE_PHASE => Ok(Self::Evaluate),
            DIFF_PHASE => Ok(Self::Diff),
            CLEANUP_PHASE => Ok(Self::Cleanup),
            FRONTIER_PHASE => Ok(Self::Frontier),
            _ => Err("unknown Window phase code".into()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WindowPhase {
    Admit,
    Enumerate {
        partition_queue_id: i64,
        cursor: WindowCursor,
        after_partitions: AfterPartitions,
    },
    Peers {
        partition_queue_id: i64,
        cursor: WindowCursor,
        after_partitions: AfterPartitions,
    },
    Frames {
        partition_queue_id: i64,
        cursor: WindowCursor,
        after_partitions: AfterPartitions,
    },
    FoldAggregate {
        partition_queue_id: i64,
        function_ordinal: u32,
        cursor: WindowFoldCursor,
        after_partitions: AfterPartitions,
    },
    Evaluate {
        partition_queue_id: i64,
        function_ordinal: u32,
        cursor: WindowCursor,
        after_partitions: AfterPartitions,
    },
    Diff {
        partition_queue_id: i64,
        leg: DiffLeg,
        cursor: WindowDiffCursor,
        after_partitions: AfterPartitions,
    },
    Cleanup {
        partition_queue_id: i64,
        cursor: WindowCleanupCursor,
        after_partitions: AfterPartitions,
    },
    Frontier,
}

impl WindowPhase {
    pub(crate) fn kind(self) -> WindowPhaseKind {
        match self {
            Self::Admit => WindowPhaseKind::Admit,
            Self::Enumerate { .. } => WindowPhaseKind::Enumerate,
            Self::Peers { .. } => WindowPhaseKind::Peers,
            Self::Frames { .. } => WindowPhaseKind::Frames,
            Self::FoldAggregate { .. } => WindowPhaseKind::FoldAggregate,
            Self::Evaluate { .. } => WindowPhaseKind::Evaluate,
            Self::Diff { .. } => WindowPhaseKind::Diff,
            Self::Cleanup { .. } => WindowPhaseKind::Cleanup,
            Self::Frontier => WindowPhaseKind::Frontier,
        }
    }

    pub(crate) fn code(self) -> PhaseCode {
        self.kind().code()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WindowContinuation {
    pub(crate) input_stream_id: i64,
    pub(crate) input: Option<InputPosition>,
    pub(crate) phase: WindowPhase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WindowFunctionKind {
    Native,
    Aggregate,
}

/// Plan-local function kinds choose the only legal phase for each function.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WindowMachine {
    function_kinds: Vec<WindowFunctionKind>,
}

impl WindowMachine {
    fn new(function_kinds: Vec<WindowFunctionKind>) -> Result<Self, String> {
        if function_kinds.is_empty() {
            return Err("Window must contain at least one function".into());
        }
        if u32::try_from(function_kinds.len())
            .ok()
            .and_then(|count| count.checked_add(4))
            .is_none()
        {
            return Err("Window contains too many functions".into());
        }
        Ok(Self { function_kinds })
    }

    fn function_count(&self) -> u32 {
        u32::try_from(self.function_kinds.len()).expect("Window function count was validated")
    }

    fn function_kind(&self, ordinal: u32) -> Result<WindowFunctionKind, String> {
        self.function_kinds
            .get(usize::try_from(ordinal - 1).map_err(|_| "Window function exceeds usize")?)
            .copied()
            .ok_or_else(|| "Window function ordinal is outside its plan".into())
    }

    fn first_function_phase(
        &self,
        partition_queue_id: i64,
        function_ordinal: u32,
        after_partitions: AfterPartitions,
    ) -> Result<WindowPhase, String> {
        Ok(match self.function_kind(function_ordinal)? {
            WindowFunctionKind::Native => WindowPhase::Evaluate {
                partition_queue_id,
                function_ordinal,
                cursor: WindowCursor::default(),
                after_partitions,
            },
            WindowFunctionKind::Aggregate => WindowPhase::FoldAggregate {
                partition_queue_id,
                function_ordinal,
                cursor: WindowFoldCursor {
                    output_ordinal: 1,
                    last_frame_ordinal: None,
                    ready_to_finalize: false,
                },
                after_partitions,
            },
        })
    }

    pub(crate) fn action(&self, continuation: WindowContinuation) -> Result<WindowAction, String> {
        self.validate_continuation(continuation)?;
        Ok(match continuation.phase {
            WindowPhase::Admit => WindowAction::Admit {
                input: continuation
                    .input
                    .ok_or_else(|| "Window Admit continuation omitted its input".to_string())?,
            },
            WindowPhase::Enumerate {
                partition_queue_id,
                cursor,
                ..
            } => WindowAction::Enumerate {
                partition_queue_id,
                cursor,
            },
            WindowPhase::Peers {
                partition_queue_id,
                cursor,
                ..
            } => WindowAction::BuildPeers {
                partition_queue_id,
                cursor,
            },
            WindowPhase::Frames {
                partition_queue_id,
                cursor,
                ..
            } => WindowAction::BuildFrames {
                partition_queue_id,
                cursor,
            },
            WindowPhase::FoldAggregate {
                partition_queue_id,
                function_ordinal,
                cursor,
                ..
            } => WindowAction::FoldAggregate {
                partition_queue_id,
                function_ordinal,
                cursor,
            },
            WindowPhase::Evaluate {
                partition_queue_id,
                function_ordinal,
                cursor,
                ..
            } => WindowAction::Evaluate {
                partition_queue_id,
                function_ordinal,
                cursor,
            },
            WindowPhase::Diff {
                partition_queue_id,
                leg,
                cursor,
                ..
            } => WindowAction::Diff {
                partition_queue_id,
                leg,
                cursor,
            },
            WindowPhase::Cleanup {
                partition_queue_id,
                cursor,
                ..
            } => WindowAction::Cleanup {
                partition_queue_id,
                cursor,
            },
            WindowPhase::Frontier => WindowAction::ForwardFrontier {
                input: continuation
                    .input
                    .ok_or_else(|| "Window frontier continuation omitted its input".to_string())?,
            },
        })
    }

    /// Applies facts from one committed database primitive.
    ///
    /// Before commit the caller must retain the old continuation. Replanning
    /// from it yields the same immutable action, so crashes cannot skip work.
    pub(crate) fn apply(
        &self,
        continuation: WindowContinuation,
        result: WindowActionResult,
        budget: WorkBudget,
    ) -> Result<WindowTransition, String> {
        let expected = self.action(continuation)?;
        match (expected, result) {
            (WindowAction::Admit { input }, WindowActionResult::Admitted(admitted)) => {
                self.apply_admission(input, admitted, budget)
            }
            (
                WindowAction::Enumerate {
                    partition_queue_id,
                    cursor,
                },
                WindowActionResult::Enumerated(page),
            ) => self.apply_internal_page(
                continuation,
                partition_queue_id,
                cursor,
                page,
                WindowInternalStage::Enumerate,
                budget,
            ),
            (
                WindowAction::BuildPeers {
                    partition_queue_id,
                    cursor,
                },
                WindowActionResult::PeersBuilt(page),
            ) => self.apply_internal_page(
                continuation,
                partition_queue_id,
                cursor,
                page,
                WindowInternalStage::Peers,
                budget,
            ),
            (
                WindowAction::BuildFrames {
                    partition_queue_id,
                    cursor,
                },
                WindowActionResult::FramesBuilt(page),
            ) => self.apply_internal_page(
                continuation,
                partition_queue_id,
                cursor,
                page,
                WindowInternalStage::Frames,
                budget,
            ),
            (
                WindowAction::FoldAggregate {
                    partition_queue_id,
                    function_ordinal,
                    cursor,
                },
                WindowActionResult::AggregateFolded(page),
            ) => self.apply_aggregate_fold(
                continuation,
                partition_queue_id,
                function_ordinal,
                cursor,
                page,
                budget,
            ),
            (
                WindowAction::Evaluate {
                    partition_queue_id,
                    function_ordinal,
                    cursor,
                },
                WindowActionResult::Evaluated(page),
            ) => self.apply_evaluation(
                continuation,
                partition_queue_id,
                function_ordinal,
                cursor,
                page,
                budget,
            ),
            (
                WindowAction::Diff {
                    partition_queue_id,
                    leg,
                    cursor,
                },
                WindowActionResult::Diffed(page),
            ) => self.apply_diff(continuation, partition_queue_id, leg, cursor, page, budget),
            (
                WindowAction::Cleanup {
                    partition_queue_id,
                    cursor,
                },
                WindowActionResult::Cleaned(page),
            ) => self.apply_cleanup(continuation, partition_queue_id, cursor, page, budget),
            (
                WindowAction::ForwardFrontier { .. },
                WindowActionResult::FrontierForwarded(facts),
            ) => self.apply_frontier(facts, budget),
            _ => Err("database returned facts for another Window phase".into()),
        }
    }

    fn validate_continuation(&self, continuation: WindowContinuation) -> Result<(), String> {
        if continuation.input_stream_id <= 0
            || continuation
                .input
                .is_some_and(|input| input.stream_id != continuation.input_stream_id)
        {
            return Err("Window continuation has an invalid input stream".into());
        }
        match continuation.phase {
            WindowPhase::Admit | WindowPhase::Frontier => {
                validate_input(
                    continuation
                        .input
                        .ok_or_else(|| "Window input phase omitted its cursor".to_string())?,
                )?;
            }
            WindowPhase::Enumerate {
                partition_queue_id,
                cursor,
                after_partitions,
            }
            | WindowPhase::Peers {
                partition_queue_id,
                cursor,
                after_partitions,
            }
            | WindowPhase::Frames {
                partition_queue_id,
                cursor,
                after_partitions,
            } => {
                validate_queue_id(partition_queue_id)?;
                cursor.validate()?;
                if continuation.input.is_some() {
                    return Err("Window drain continuation retained an input cursor".into());
                }
                validate_after_partitions(continuation.input_stream_id, after_partitions)?;
            }
            WindowPhase::Diff {
                partition_queue_id,
                cursor,
                after_partitions,
                ..
            } => {
                validate_queue_id(partition_queue_id)?;
                cursor.validate()?;
                if continuation.input.is_some() {
                    return Err("Window drain continuation retained an input cursor".into());
                }
                validate_after_partitions(continuation.input_stream_id, after_partitions)?;
            }
            WindowPhase::FoldAggregate {
                partition_queue_id,
                function_ordinal,
                cursor,
                after_partitions,
            } => {
                validate_queue_id(partition_queue_id)?;
                if self.function_kind(function_ordinal)? != WindowFunctionKind::Aggregate {
                    return Err("native Window function entered aggregate fold".into());
                }
                cursor.validate()?;
                if continuation.input.is_some() {
                    return Err("Window drain continuation retained an input cursor".into());
                }
                validate_after_partitions(continuation.input_stream_id, after_partitions)?;
            }
            WindowPhase::Evaluate {
                partition_queue_id,
                function_ordinal,
                cursor,
                after_partitions,
            } => {
                validate_queue_id(partition_queue_id)?;
                if self.function_kind(function_ordinal)? != WindowFunctionKind::Native {
                    return Err("aggregate Window function entered native evaluation".into());
                }
                cursor.validate()?;
                if continuation.input.is_some() {
                    return Err("Window drain continuation retained an input cursor".into());
                }
                validate_after_partitions(continuation.input_stream_id, after_partitions)?;
            }
            WindowPhase::Cleanup {
                partition_queue_id,
                cursor,
                after_partitions,
            } => {
                validate_queue_id(partition_queue_id)?;
                if cursor.relation_ordinal >= self.cleanup_relation_count() {
                    return Err("Window cleanup relation ordinal is outside its plan".into());
                }
                cursor.row.validate()?;
                if continuation.input.is_some() {
                    return Err("Window drain continuation retained an input cursor".into());
                }
                validate_after_partitions(continuation.input_stream_id, after_partitions)?;
            }
        }
        Ok(())
    }

    fn cleanup_relation_count(&self) -> u32 {
        4
    }

    fn apply_admission(
        &self,
        input: InputPosition,
        admitted: WindowAdmission,
        budget: WorkBudget,
    ) -> Result<WindowTransition, String> {
        admitted.facts.validate(budget)?;
        validate_no_external_output(admitted.facts)?;
        if admitted.facts.usage.input_rows == 0 {
            return Err("Window admission made no bounded input progress".into());
        }
        let next = match admitted.target {
            WindowAdmissionTarget::Continue(next_input) => {
                validate_input(next_input)?;
                if next_input.stream_id != input.stream_id
                    || next_input.chunk_seq != input.chunk_seq
                    || next_input.row_ordinal <= input.row_ordinal
                {
                    return Err("Window admission continuation did not advance its page".into());
                }
                Some(WindowContinuation {
                    input_stream_id: input.stream_id,
                    input: Some(next_input),
                    phase: WindowPhase::Admit,
                })
            }
            WindowAdmissionTarget::Drain {
                first_partition_queue_id,
                after_partitions,
            } => {
                validate_queue_id(first_partition_queue_id)?;
                validate_after_partitions(input.stream_id, after_partitions)?;
                Some(WindowContinuation {
                    input_stream_id: input.stream_id,
                    input: None,
                    phase: WindowPhase::Enumerate {
                        partition_queue_id: first_partition_queue_id,
                        cursor: WindowCursor::default(),
                        after_partitions,
                    },
                })
            }
            WindowAdmissionTarget::Idle => None,
        };
        validate_continuation_count(admitted.facts, next.is_some())?;
        if let Some(next) = next {
            self.validate_continuation(next)?;
        }
        Ok(WindowTransition::Committed {
            continuation: next,
            facts: admitted.facts,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_internal_page(
        &self,
        continuation: WindowContinuation,
        partition_queue_id: i64,
        cursor: WindowCursor,
        page: WindowPage,
        stage: WindowInternalStage,
        budget: WorkBudget,
    ) -> Result<WindowTransition, String> {
        page.validate(cursor, budget, false)?;
        let after_partitions = phase_after_partitions(continuation.phase)?;
        let next_phase = if !page.complete {
            stage.phase(
                partition_queue_id,
                WindowCursor {
                    row_id: page.last_row_id,
                },
                after_partitions,
            )
        } else {
            match stage {
                WindowInternalStage::Enumerate => WindowPhase::Peers {
                    partition_queue_id,
                    cursor: WindowCursor::default(),
                    after_partitions,
                },
                WindowInternalStage::Peers => WindowPhase::Frames {
                    partition_queue_id,
                    cursor: WindowCursor::default(),
                    after_partitions,
                },
                WindowInternalStage::Frames => {
                    self.first_function_phase(partition_queue_id, 1, after_partitions)?
                }
            }
        };
        let next = WindowContinuation {
            input_stream_id: continuation.input_stream_id,
            input: None,
            phase: next_phase,
        };
        validate_continuation_count(page.facts, true)?;
        Ok(WindowTransition::Committed {
            continuation: Some(next),
            facts: page.facts,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_aggregate_fold(
        &self,
        continuation: WindowContinuation,
        partition_queue_id: i64,
        function_ordinal: u32,
        cursor: WindowFoldCursor,
        page: WindowFoldPage,
        budget: WorkBudget,
    ) -> Result<WindowTransition, String> {
        page.validate(cursor, budget, false)?;
        let after_partitions = phase_after_partitions(continuation.phase)?;
        let next_phase = if let Some(next_cursor) = page.next_cursor {
            WindowPhase::FoldAggregate {
                partition_queue_id,
                function_ordinal,
                cursor: next_cursor,
                after_partitions,
            }
        } else if function_ordinal < self.function_count() {
            self.first_function_phase(partition_queue_id, function_ordinal + 1, after_partitions)?
        } else {
            WindowPhase::Diff {
                partition_queue_id,
                leg: DiffLeg::Remove,
                cursor: WindowDiffCursor::default(),
                after_partitions,
            }
        };
        let next = WindowContinuation {
            input_stream_id: continuation.input_stream_id,
            input: None,
            phase: next_phase,
        };
        validate_continuation_count(page.facts, true)?;
        Ok(WindowTransition::Committed {
            continuation: Some(next),
            facts: page.facts,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_evaluation(
        &self,
        continuation: WindowContinuation,
        partition_queue_id: i64,
        function_ordinal: u32,
        cursor: WindowCursor,
        page: WindowPage,
        budget: WorkBudget,
    ) -> Result<WindowTransition, String> {
        page.validate(cursor, budget, false)?;
        let after_partitions = phase_after_partitions(continuation.phase)?;
        let next_phase = if !page.complete {
            WindowPhase::Evaluate {
                partition_queue_id,
                function_ordinal,
                cursor: WindowCursor {
                    row_id: page.last_row_id,
                },
                after_partitions,
            }
        } else if function_ordinal < self.function_count() {
            self.first_function_phase(partition_queue_id, function_ordinal + 1, after_partitions)?
        } else {
            WindowPhase::Diff {
                partition_queue_id,
                leg: DiffLeg::Remove,
                cursor: WindowDiffCursor::default(),
                after_partitions,
            }
        };
        let next = WindowContinuation {
            input_stream_id: continuation.input_stream_id,
            input: None,
            phase: next_phase,
        };
        validate_continuation_count(page.facts, true)?;
        Ok(WindowTransition::Committed {
            continuation: Some(next),
            facts: page.facts,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_diff(
        &self,
        continuation: WindowContinuation,
        partition_queue_id: i64,
        leg: DiffLeg,
        cursor: WindowDiffCursor,
        page: WindowDiffPage,
        budget: WorkBudget,
    ) -> Result<WindowTransition, String> {
        page.validate(cursor, budget)?;
        let after_partitions = phase_after_partitions(continuation.phase)?;
        let next_phase = if !page.complete {
            WindowPhase::Diff {
                partition_queue_id,
                leg,
                cursor: WindowDiffCursor {
                    row_id: page.last_row_id,
                    repeat: page.repeat_cursor,
                },
                after_partitions,
            }
        } else {
            match leg {
                DiffLeg::Remove => WindowPhase::Diff {
                    partition_queue_id,
                    leg: DiffLeg::Add,
                    cursor: WindowDiffCursor::default(),
                    after_partitions,
                },
                DiffLeg::Add => WindowPhase::Cleanup {
                    partition_queue_id,
                    cursor: WindowCleanupCursor::default(),
                    after_partitions,
                },
            }
        };
        let next = WindowContinuation {
            input_stream_id: continuation.input_stream_id,
            input: None,
            phase: next_phase,
        };
        validate_continuation_count(page.facts, true)?;
        Ok(WindowTransition::Committed {
            continuation: Some(next),
            facts: page.facts,
        })
    }

    fn apply_cleanup(
        &self,
        continuation: WindowContinuation,
        partition_queue_id: i64,
        cursor: WindowCleanupCursor,
        page: WindowCleanup,
        budget: WorkBudget,
    ) -> Result<WindowTransition, String> {
        page.page.validate(cursor.row, budget, false)?;
        let after_partitions = phase_after_partitions(continuation.phase)?;
        if !page.page.complete && page.next_partition_queue_id.is_some() {
            return Err("partial Window cleanup returned another partition".into());
        }

        let next = if !page.page.complete {
            Some(WindowContinuation {
                input_stream_id: continuation.input_stream_id,
                input: None,
                phase: WindowPhase::Cleanup {
                    partition_queue_id,
                    cursor: WindowCleanupCursor {
                        relation_ordinal: cursor.relation_ordinal,
                        row: WindowCursor {
                            row_id: page.page.last_row_id,
                        },
                    },
                    after_partitions,
                },
            })
        } else if cursor.relation_ordinal + 1 < self.cleanup_relation_count() {
            if page.next_partition_queue_id.is_some() {
                return Err("Window cleanup returned a partition before all work relations".into());
            }
            Some(WindowContinuation {
                input_stream_id: continuation.input_stream_id,
                input: None,
                phase: WindowPhase::Cleanup {
                    partition_queue_id,
                    cursor: WindowCleanupCursor {
                        relation_ordinal: cursor.relation_ordinal + 1,
                        row: WindowCursor::default(),
                    },
                    after_partitions,
                },
            })
        } else if let Some(next_queue_id) = page.next_partition_queue_id {
            validate_queue_id(next_queue_id)?;
            if next_queue_id <= partition_queue_id {
                return Err("Window partition queue did not advance".into());
            }
            Some(WindowContinuation {
                input_stream_id: continuation.input_stream_id,
                input: None,
                phase: WindowPhase::Enumerate {
                    partition_queue_id: next_queue_id,
                    cursor: WindowCursor::default(),
                    after_partitions,
                },
            })
        } else {
            match after_partitions {
                AfterPartitions::Admit(next_input) => Some(WindowContinuation {
                    input_stream_id: continuation.input_stream_id,
                    input: Some(next_input),
                    phase: WindowPhase::Admit,
                }),
                AfterPartitions::FinishInput => None,
                AfterPartitions::Frontier(frontier) => Some(WindowContinuation {
                    input_stream_id: continuation.input_stream_id,
                    input: Some(frontier),
                    phase: WindowPhase::Frontier,
                }),
            }
        };
        validate_continuation_count(page.page.facts, next.is_some())?;
        Ok(WindowTransition::Committed {
            continuation: next,
            facts: page.page.facts,
        })
    }

    fn apply_frontier(
        &self,
        facts: PrimitiveFacts,
        budget: WorkBudget,
    ) -> Result<WindowTransition, String> {
        facts.validate(budget)?;
        if !matches!(facts.output, OutputFacts::Frontier { .. })
            || facts.usage.input_rows != 0
            || facts.usage.input_bytes != 0
            || facts.continuation_rows != 0
        {
            return Err("Window frontier commit is inconsistent".into());
        }
        Ok(WindowTransition::Committed {
            continuation: None,
            facts,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WindowInternalStage {
    Enumerate,
    Peers,
    Frames,
}

impl WindowInternalStage {
    fn phase(
        self,
        partition_queue_id: i64,
        cursor: WindowCursor,
        after_partitions: AfterPartitions,
    ) -> WindowPhase {
        match self {
            Self::Enumerate => WindowPhase::Enumerate {
                partition_queue_id,
                cursor,
                after_partitions,
            },
            Self::Peers => WindowPhase::Peers {
                partition_queue_id,
                cursor,
                after_partitions,
            },
            Self::Frames => WindowPhase::Frames {
                partition_queue_id,
                cursor,
                after_partitions,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WindowAction {
    Admit {
        input: InputPosition,
    },
    Enumerate {
        partition_queue_id: i64,
        cursor: WindowCursor,
    },
    BuildPeers {
        partition_queue_id: i64,
        cursor: WindowCursor,
    },
    BuildFrames {
        partition_queue_id: i64,
        cursor: WindowCursor,
    },
    /// Advances one aggregate frame strictly in expanded input order.
    FoldAggregate {
        partition_queue_id: i64,
        function_ordinal: u32,
        cursor: WindowFoldCursor,
    },
    Evaluate {
        partition_queue_id: i64,
        function_ordinal: u32,
        cursor: WindowCursor,
    },
    /// The SQL primitive compares typed candidate and visible relations. It
    /// appends an effect chunk and mutates visible state in one transaction.
    Diff {
        partition_queue_id: i64,
        leg: DiffLeg,
        cursor: WindowDiffCursor,
    },
    Cleanup {
        partition_queue_id: i64,
        cursor: WindowCleanupCursor,
    },
    ForwardFrontier {
        input: InputPosition,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WindowAdmission {
    pub(crate) facts: PrimitiveFacts,
    pub(crate) target: WindowAdmissionTarget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WindowAdmissionTarget {
    Continue(InputPosition),
    Drain {
        first_partition_queue_id: i64,
        after_partitions: AfterPartitions,
    },
    Idle,
}

/// Facts from one bounded relation page. `last_row_id` references the typed
/// work row that supplies the next SQL keyset boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WindowPage {
    pub(crate) facts: PrimitiveFacts,
    pub(crate) last_row_id: Option<i64>,
    pub(crate) complete: bool,
}

/// Facts from one bounded primary-side comparison page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WindowDiffPage {
    pub(crate) facts: PrimitiveFacts,
    pub(crate) last_row_id: Option<i64>,
    pub(crate) complete: bool,
    pub(crate) repeat_cursor: bool,
}

impl WindowDiffPage {
    fn validate(self, previous: WindowDiffCursor, budget: WorkBudget) -> Result<(), String> {
        self.facts.validate(budget)?;
        PageFacts {
            usage: self.facts.usage,
            last_row_id: self.last_row_id,
            complete: self.complete,
        }
        .validate(budget)?;
        if self.last_row_id.is_some_and(|row_id| row_id <= 0) {
            return Err("Window Diff page returned a non-positive cursor".into());
        }
        if self.repeat_cursor && (self.complete || self.last_row_id.is_none()) {
            return Err("Window Diff residual has no resumable row".into());
        }
        if !self.complete {
            if self.facts.usage.input_rows == 0 || self.last_row_id.is_none() {
                return Err("partial Window Diff page made no resumable progress".into());
            }
            if self.last_row_id < previous.row_id
                || (self.last_row_id == previous.row_id && !previous.repeat)
            {
                return Err("Window Diff page moved its cursor backwards".into());
            }
        }
        if matches!(self.facts.output, OutputFacts::Frontier { .. }) {
            return Err("Window Diff emitted a frontier".into());
        }
        if self.facts.usage.output_rows > self.facts.usage.input_rows {
            return Err("Window Diff emitted more effects than rows compared".into());
        }
        Ok(())
    }
}

impl WindowPage {
    fn validate(
        self,
        previous: WindowCursor,
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
            return Err("Window page returned a non-positive cursor".into());
        }
        if !self.complete {
            if self.facts.usage.input_rows == 0 || self.last_row_id.is_none() {
                return Err("partial Window page made no resumable progress".into());
            }
            if self.last_row_id < previous.row_id {
                return Err("Window page moved its cursor backwards".into());
            }
        }
        if permits_output {
            if matches!(self.facts.output, OutputFacts::Frontier { .. }) {
                return Err("Window diff emitted a frontier".into());
            }
            if self.facts.usage.output_rows > self.facts.usage.input_rows {
                return Err("Window diff emitted more effects than rows compared".into());
            }
        } else {
            validate_no_external_output(self.facts)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WindowFoldPage {
    pub(crate) facts: PrimitiveFacts,
    /// Exact restart point after every accumulator mutation in this step.
    /// `None` means that all output ordinals for this function are complete.
    pub(crate) next_cursor: Option<WindowFoldCursor>,
    /// Output ordinals visited by this step, including empty frames.
    pub(crate) work_items: usize,
}

impl WindowFoldPage {
    fn validate(
        self,
        previous: WindowFoldCursor,
        budget: WorkBudget,
        permits_output: bool,
    ) -> Result<(), String> {
        self.facts.validate(budget)?;
        if permits_output {
            return Err("Window aggregate fold cannot emit an external chunk".into());
        }
        validate_no_external_output(self.facts)?;
        if self.work_items == 0 || self.work_items > WINDOW_FOLD_WORK_ITEM_CAP {
            return Err("Window aggregate fold returned an invalid work-item count".into());
        }
        let Some(next) = self.next_cursor else {
            return Ok(());
        };
        next.validate()?;
        if next.output_ordinal < previous.output_ordinal {
            return Err("Window aggregate fold moved its output cursor backwards".into());
        }
        let completed = usize::try_from(next.output_ordinal - previous.output_ordinal)
            .map_err(|_| "Window aggregate output cursor exceeds usize")?;
        let current_visit = if completed == 0 {
            match (previous.ready_to_finalize, next.ready_to_finalize) {
                (false, true) => 1,
                (false, false)
                    if self.facts.usage.input_rows > 0
                        && next.last_frame_ordinal.is_some()
                        && next.last_frame_ordinal > previous.last_frame_ordinal =>
                {
                    1
                }
                _ => return Err("partial Window aggregate fold made no resumable progress".into()),
            }
        } else {
            usize::from(next.ready_to_finalize || next.last_frame_ordinal.is_some())
        };
        let visits = completed
            .checked_add(current_visit)
            .ok_or_else(|| "Window aggregate fold work-item count overflow".to_string())?;
        if visits > self.work_items {
            return Err("Window aggregate fold skipped an unvisited output ordinal".into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WindowCleanup {
    pub(crate) page: WindowPage,
    pub(crate) next_partition_queue_id: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WindowActionResult {
    Admitted(WindowAdmission),
    Enumerated(WindowPage),
    PeersBuilt(WindowPage),
    FramesBuilt(WindowPage),
    AggregateFolded(WindowFoldPage),
    Evaluated(WindowPage),
    Diffed(WindowDiffPage),
    Cleaned(WindowCleanup),
    FrontierForwarded(PrimitiveFacts),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WindowTransition {
    Committed {
        continuation: Option<WindowContinuation>,
        facts: PrimitiveFacts,
    },
}

fn phase_after_partitions(phase: WindowPhase) -> Result<AfterPartitions, String> {
    match phase {
        WindowPhase::Enumerate {
            after_partitions, ..
        }
        | WindowPhase::Peers {
            after_partitions, ..
        }
        | WindowPhase::Frames {
            after_partitions, ..
        }
        | WindowPhase::FoldAggregate {
            after_partitions, ..
        }
        | WindowPhase::Evaluate {
            after_partitions, ..
        }
        | WindowPhase::Diff {
            after_partitions, ..
        }
        | WindowPhase::Cleanup {
            after_partitions, ..
        } => Ok(after_partitions),
        WindowPhase::Admit | WindowPhase::Frontier => {
            Err("Window phase has no partition completion target".into())
        }
    }
}

fn validate_input(input: InputPosition) -> Result<(), String> {
    if input.stream_id <= 0 || input.chunk_seq <= 0 || input.row_ordinal < 0 {
        return Err("Window input position is invalid".into());
    }
    Ok(())
}

fn validate_queue_id(queue_id: i64) -> Result<(), String> {
    if queue_id <= 0 {
        return Err("Window partition queue id is not positive".into());
    }
    Ok(())
}

fn validate_after_partitions(input_stream_id: i64, after: AfterPartitions) -> Result<(), String> {
    if input_stream_id <= 0 {
        return Err("Window partition target has an invalid input stream".into());
    }
    match after {
        AfterPartitions::Admit(next) => {
            validate_input(next)?;
            if next.stream_id != input_stream_id {
                return Err("Window admission target changed its input stream".into());
            }
        }
        AfterPartitions::Frontier(frontier) => {
            validate_input(frontier)?;
            if frontier.stream_id != input_stream_id || frontier.row_ordinal != 0 {
                return Err("Window frontier target is invalid".into());
            }
        }
        AfterPartitions::FinishInput => {}
    }
    Ok(())
}

fn validate_no_external_output(facts: PrimitiveFacts) -> Result<(), String> {
    if facts.output != OutputFacts::None
        || facts.usage.output_rows != 0
        || facts.usage.output_bytes != 0
    {
        return Err("Window internal phase reported external output".into());
    }
    Ok(())
}

fn validate_continuation_count(
    facts: PrimitiveFacts,
    has_continuation: bool,
) -> Result<(), String> {
    if facts.continuation_rows != u64::from(has_continuation) {
        return Err("Window checkpoint disagrees with its continuation row".into());
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct WindowStorage {
    partitions: RelationRef,
    input: RelationRef,
    ordered: RelationRef,
    peers: RelationRef,
    frames: RelationRef,
    candidate: RelationRef,
    visible: RelationRef,
    continuation: RelationRef,
    accumulators: Vec<Option<RelationRef>>,
    ntile_states: Vec<Option<RelationRef>>,
    input_payload: RelationRef,
    output_payload: RelationRef,
    input_type: TypeRef,
    output_type: TypeRef,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DurableWindow {
    continuation: WindowContinuation,
    persisted: bool,
}

#[derive(Clone, Debug)]
struct WindowExpressions {
    partition_expressions: Vec<String>,
    partition_columns: Vec<String>,
    order_expressions: Vec<String>,
    order_columns: Vec<String>,
    order_by: String,
    keyset_after: String,
    peer_equal: String,
    outputs: String,
    functions: Vec<WindowFunctionPlan>,
    frame_start_offset: Option<String>,
    frame_end_offset: Option<String>,
}

#[derive(Clone, Debug)]
struct WindowFunctionPlan {
    current_arguments: Vec<String>,
    target_arguments: Vec<String>,
    filter: String,
    result_type: String,
    capability: WindowFunctionCapability,
}

#[derive(Clone, Debug)]
enum WindowFunctionCapability {
    Native(NativeWindow),
    Aggregate(AggregateCapability),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativeWindow {
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Ntile,
    Lag,
    Lead,
    FirstValue,
    LastValue,
    NthValue,
}

/// Create the sole Window storage ABI understood by `execute`.
pub(crate) fn provision(
    client: &mut SpiClient<'_>,
    result_oid: pg_sys::Oid,
    stage_id: i32,
    stage: &DataflowStage,
    input_streams: &[i64],
    output_stream: i64,
) -> Result<(), String> {
    let OperatorSpec::Window(spec) = &stage.spec else {
        return Err("Window provisioner received another operator".into());
    };
    if result_oid == pg_sys::InvalidOid
        || stage_id < 0
        || input_streams.len() != 1
        || input_streams[0] <= 0
        || output_stream <= 0
        || spec.functions.is_empty()
    {
        return Err(format!(
            "Window stage {stage_id} has an invalid storage contract"
        ));
    }
    validate_window_frame(spec)?;
    let input_payload = super::storage::payload(client, input_streams[0])?;
    let output_payload = super::storage::payload(client, output_stream)?;
    let output_attributes = super::storage::composite_attributes(client, &output_payload.row_type)?;
    validate_output_attributes(&output_attributes, &stage.schema.outputs)?;
    let prefix = format!("r{}_s{stage_id}", result_oid.to_u32());

    let mut partition_definitions = Vec::with_capacity(spec.partition_by.len());
    let mut partition_columns = Vec::with_capacity(spec.partition_by.len());
    for (index, key) in spec.partition_by.iter().enumerate() {
        let name = format!("partition_{}", index + 1);
        let mut definition = format!(
            "{} {}",
            quote_identifier(&name),
            column_sql(client, &key.type_)?
        );
        if !key.type_.nullable {
            definition.push_str(" NOT NULL");
        }
        partition_definitions.push(definition);
        partition_columns.push(quote_identifier(&name));
    }
    let partition_suffix = if partition_definitions.is_empty() {
        String::new()
    } else {
        format!(",{}", partition_definitions.join(","))
    };
    let partitions = create_window_state(
        client,
        result_oid,
        stage_id,
        0,
        &format!("window_partitions_{prefix}"),
        &format!(
            r#"
            partition_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            dirty boolean NOT NULL DEFAULT false,
            causal_lsn pg_lsn,
            row_count numeric NOT NULL DEFAULT 0 CHECK(
              row_count>=0 AND row_count<=9223372036854775807::numeric
              AND row_count=pg_catalog.trunc(row_count)
            )
            {partition_suffix}
            "#
        ),
    )?;
    if partition_columns.is_empty() {
        client
            .update(
                &format!("INSERT INTO {partitions}(dirty,row_count) VALUES(false,0)"),
                Some(1),
                &[],
            )
            .map_err(|error| format!("could not seed Window global partition: {error}"))?;
    } else {
        let index = quote_identifier(&format!("window_partition_keys_{prefix}"));
        client
            .update(
                &format!(
                    "CREATE UNIQUE INDEX {index} ON {partitions}({}) NULLS NOT DISTINCT",
                    partition_columns.join(",")
                ),
                None,
                &[],
            )
            .map_err(|error| format!("could not create Window partition index: {error}"))?;
    }
    let dirty_partition_index = quote_identifier(&format!("window_dirty_partitions_{prefix}"));
    client
        .update(
            &format!(
                "CREATE INDEX {dirty_partition_index} \
                 ON {partitions}(partition_id) WHERE dirty"
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not create Window dirty-partition index: {error}"))?;

    let mut order_definitions = Vec::with_capacity(spec.order_by.len());
    let mut order_index = Vec::with_capacity(spec.order_by.len() + 2);
    order_index.push("partition_id ASC".into());
    for (index, order) in spec.order_by.iter().enumerate() {
        let name = format!("order_{}", index + 1);
        let mut definition = format!(
            "{} {}",
            quote_identifier(&name),
            column_sql(client, &order.type_)?
        );
        if !order.type_.nullable {
            definition.push_str(" NOT NULL");
        }
        order_definitions.push(definition);
        order_index.push(resolve_btree_client(client, order, "Window")?.index_column(&name));
    }
    order_index.push("entry_id ASC".into());
    let order_suffix = if order_definitions.is_empty() {
        String::new()
    } else {
        format!(",{}", order_definitions.join(","))
    };
    let input = create_window_state(
        client,
        result_oid,
        stage_id,
        1,
        &format!("window_input_{prefix}"),
        &format!(
            r#"
            entry_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            row_key bytea NOT NULL UNIQUE,
            row_value {} NOT NULL,
            multiplicity numeric NOT NULL CHECK(
              multiplicity>0 AND multiplicity<=9223372036854775807::numeric
              AND multiplicity=pg_catalog.trunc(multiplicity)
            ),
            partition_id bigint NOT NULL REFERENCES {partitions}(partition_id)
              ON DELETE RESTRICT
            {order_suffix}
            "#,
            input_payload.row_type.sql()
        ),
    )?;
    let input_index = quote_identifier(&format!("window_input_order_{prefix}"));
    client
        .update(
            &format!(
                "CREATE INDEX {input_index} ON {input}({})",
                order_index.join(",")
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not create Window input order index: {error}"))?;

    let mut function_columns = Vec::with_capacity(spec.functions.len());
    let mut capabilities = Vec::with_capacity(spec.functions.len());
    for (index, function) in spec.functions.iter().enumerate() {
        function_columns.push(format!(
            "{} {}",
            quote_identifier(&format!("function_{}", index + 1)),
            column_sql(client, &function.type_)?
        ));
        capabilities.push(resolve_window_function_client(client, function)?);
    }
    let ordered = create_window_state(
        client,
        result_oid,
        stage_id,
        2,
        &format!("window_ordered_{prefix}"),
        &format!(
            r#"
            ordinal bigint PRIMARY KEY CHECK(ordinal>0),
            entry_id bigint NOT NULL REFERENCES {input}(entry_id) ON DELETE RESTRICT,
            copy_ordinal bigint NOT NULL CHECK(copy_ordinal>0),
            peer_id bigint,
            {},
            UNIQUE(entry_id,copy_ordinal)
            "#,
            function_columns.join(",")
        ),
    )?;
    let peer_index = quote_identifier(&format!("window_ordered_peer_{prefix}"));
    client
        .update(
            &format!("CREATE INDEX {peer_index} ON {ordered}(peer_id,ordinal)"),
            None,
            &[],
        )
        .map_err(|error| format!("could not create Window peer index: {error}"))?;

    let _peers = create_window_state(
        client,
        result_oid,
        stage_id,
        3,
        &format!("window_peers_{prefix}"),
        r#"
        peer_id bigint PRIMARY KEY CHECK(peer_id>0),
        first_ordinal bigint NOT NULL CHECK(first_ordinal>0),
        last_ordinal bigint NOT NULL CHECK(last_ordinal>=first_ordinal)
        "#,
    )?;
    let _frames = create_window_state(
        client,
        result_oid,
        stage_id,
        4,
        &format!("window_frames_{prefix}"),
        r#"
        ordinal bigint PRIMARY KEY CHECK(ordinal>0),
        start_1 bigint,end_1 bigint,start_2 bigint,end_2 bigint,
        start_3 bigint,end_3 bigint,
        frame_count bigint NOT NULL CHECK(frame_count>=0)
        "#,
    )?;
    let candidate = create_window_state(
        client,
        result_oid,
        stage_id,
        5,
        &format!("window_candidate_{prefix}"),
        &format!(
            r#"
            candidate_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            partition_id bigint NOT NULL REFERENCES {partitions}(partition_id)
              ON DELETE RESTRICT,
            output_key bytea NOT NULL,
            output_row {} NOT NULL,
            multiplicity numeric NOT NULL CHECK(
              multiplicity>0 AND multiplicity=pg_catalog.trunc(multiplicity)
            ),
            UNIQUE(partition_id,output_key)
            "#,
            output_payload.row_type.sql()
        ),
    )?;
    let candidate_page_index = quote_identifier(&format!("window_candidate_page_{prefix}"));
    client
        .update(
            &format!(
                "CREATE INDEX {candidate_page_index} \
                 ON {candidate}(partition_id,candidate_id)"
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not create Window candidate page index: {error}"))?;
    let visible = create_window_state(
        client,
        result_oid,
        stage_id,
        6,
        &format!("window_visible_{prefix}"),
        &format!(
            r#"
            visible_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            partition_id bigint NOT NULL REFERENCES {partitions}(partition_id)
              ON DELETE RESTRICT,
            output_key bytea NOT NULL,
            output_row {} NOT NULL,
            multiplicity numeric NOT NULL CHECK(
              multiplicity>0 AND multiplicity=pg_catalog.trunc(multiplicity)
            ),
            UNIQUE(partition_id,output_key)
            "#,
            output_payload.row_type.sql()
        ),
    )?;
    let visible_page_index = quote_identifier(&format!("window_visible_page_{prefix}"));
    client
        .update(
            &format!(
                "CREATE INDEX {visible_page_index} \
                 ON {visible}(partition_id,visible_id)"
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not create Window visible page index: {error}"))?;
    for (index, capability) in capabilities.iter().enumerate() {
        match capability {
            WindowFunctionCapability::Aggregate(capability) => {
                let transition_column_sql = column_sql(
                    client,
                    &SlotType {
                        type_oid: capability.transition_type_oid.to_u32(),
                        typmod: -1,
                        collation_oid: capability.transition_collation_oid.to_u32(),
                        nullable: true,
                    },
                )?;
                create_window_state(
                    client,
                    result_oid,
                    stage_id,
                    i32::try_from(1001 + index)
                        .map_err(|_| "Window accumulator slot exceeds integer")?,
                    &format!("window_accumulator_{prefix}_f{}", index + 1),
                    &format!(
                        r#"
                        singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
                        partition_id bigint NOT NULL REFERENCES {partitions}(partition_id)
                          ON DELETE RESTRICT,
                        output_ordinal bigint NOT NULL CHECK(output_ordinal>0),
                        state_value {},
                        no_trans_value boolean NOT NULL,
                        UNIQUE(partition_id,output_ordinal)
                        "#,
                        transition_column_sql
                    ),
                )?;
            }
            WindowFunctionCapability::Native(NativeWindow::Ntile) => {
                create_window_state(
                    client,
                    result_oid,
                    stage_id,
                    i32::try_from(2001 + index)
                        .map_err(|_| "Window ntile state slot exceeds integer")?,
                    &format!("window_ntile_{prefix}_f{}", index + 1),
                    &format!(
                        r#"
                        singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
                        partition_id bigint NOT NULL REFERENCES {partitions}(partition_id)
                          ON DELETE RESTRICT,
                        bucket_count bigint,
                        first_ordinal bigint CHECK(first_ordinal>0),
                        CHECK((bucket_count IS NULL)=(first_ordinal IS NULL))
                        "#
                    ),
                )?;
            }
            WindowFunctionCapability::Native(_) => {}
        }
    }

    let continuation = qualified_internal(&format!("window_continuation_{prefix}"));
    client
        .update(
            &format!(
                r#"
                CREATE TABLE {continuation}(
                  singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
                  phase smallint NOT NULL CHECK(phase BETWEEN 1 AND 9),
                  input_stream_id bigint NOT NULL CHECK(input_stream_id>0),
                  input_chunk_seq bigint CHECK(input_chunk_seq>0),
                  input_row_ordinal bigint CHECK(input_row_ordinal>=0),
                  partition_queue_id bigint CHECK(partition_queue_id>0),
                  function_ordinal integer CHECK(function_ordinal>0),
                  output_ordinal bigint CHECK(output_ordinal>0),
                  cursor_row_id bigint CHECK(cursor_row_id>0),
                  fold_ready boolean NOT NULL DEFAULT false,
                  cursor_repeat boolean NOT NULL DEFAULT false,
                  diff_leg smallint CHECK(diff_leg IN (1,2)),
                  cleanup_ordinal integer CHECK(cleanup_ordinal>=0),
                  after_kind smallint CHECK(after_kind IN (1,2,3)),
                  after_chunk_seq bigint CHECK(after_chunk_seq>0),
                  after_row_ordinal bigint CHECK(after_row_ordinal>=0),
                  FOREIGN KEY(input_stream_id,input_chunk_seq)
                    REFERENCES shiba_internal.effect_stream_chunks(stream_id,chunk_seq)
                    ON DELETE RESTRICT,
                  CHECK(
                    (phase IN (1,9) AND input_chunk_seq IS NOT NULL
                     AND input_row_ordinal IS NOT NULL
                     AND partition_queue_id IS NULL AND function_ordinal IS NULL
                     AND output_ordinal IS NULL
                     AND cursor_row_id IS NULL AND diff_leg IS NULL
                     AND cleanup_ordinal IS NULL AND after_kind IS NULL
                     AND after_chunk_seq IS NULL AND after_row_ordinal IS NULL)
                    OR
                    (phase IN (2,3,4) AND input_chunk_seq IS NULL
                     AND input_row_ordinal IS NULL
                     AND partition_queue_id IS NOT NULL AND function_ordinal IS NULL
                     AND output_ordinal IS NULL
                     AND diff_leg IS NULL AND cleanup_ordinal IS NULL
                     AND after_kind IS NOT NULL)
                    OR
                    (phase=5 AND input_chunk_seq IS NULL
                     AND input_row_ordinal IS NULL
                     AND partition_queue_id IS NOT NULL
                     AND function_ordinal IS NOT NULL AND output_ordinal IS NOT NULL
                     AND diff_leg IS NULL
                     AND cleanup_ordinal IS NULL AND after_kind IS NOT NULL)
                    OR
                    (phase=6 AND input_chunk_seq IS NULL
                     AND input_row_ordinal IS NULL
                     AND partition_queue_id IS NOT NULL
                     AND function_ordinal IS NOT NULL AND output_ordinal IS NULL
                     AND diff_leg IS NULL
                     AND cleanup_ordinal IS NULL AND after_kind IS NOT NULL)
                    OR
                    (phase=7 AND input_chunk_seq IS NULL
                     AND input_row_ordinal IS NULL
                     AND partition_queue_id IS NOT NULL
                     AND function_ordinal IS NULL AND output_ordinal IS NULL
                     AND diff_leg IS NOT NULL
                     AND cleanup_ordinal IS NULL AND after_kind IS NOT NULL)
                    OR
                    (phase=8 AND input_chunk_seq IS NULL
                     AND input_row_ordinal IS NULL
                     AND partition_queue_id IS NOT NULL
                     AND function_ordinal IS NULL AND output_ordinal IS NULL
                     AND diff_leg IS NULL
                     AND cleanup_ordinal IS NOT NULL AND after_kind IS NOT NULL)
                  ),
                  CHECK(phase=5 OR NOT fold_ready),
                  CHECK(
                    NOT cursor_repeat
                    OR (phase=7 AND cursor_row_id IS NOT NULL)
                  ),
                  CHECK(
                    after_kind IS NULL
                    OR (after_kind=1 AND after_chunk_seq IS NOT NULL
                        AND after_row_ordinal IS NOT NULL)
                    OR (after_kind=2 AND after_chunk_seq IS NULL
                        AND after_row_ordinal IS NULL)
                    OR (after_kind=3 AND after_chunk_seq IS NOT NULL
                        AND after_row_ordinal=0)
                  )
                )
                "#
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not create Window continuation: {error}"))?;
    revoke_window_relation(client, &continuation, "continuation")?;
    let continuation_oid = resolve_relation_oid(client, &continuation)?;
    catalog_continuation(client, result_oid, stage_id, continuation_oid)?;
    Ok(())
}

fn create_window_state(
    client: &mut SpiClient<'_>,
    result_oid: pg_sys::Oid,
    stage_id: i32,
    slot: i32,
    name: &str,
    body: &str,
) -> Result<String, String> {
    let relation = qualified_internal(name);
    client
        .update(&format!("CREATE TABLE {relation}({body})"), None, &[])
        .map_err(|error| {
            format!("could not create Window stage {stage_id} state slot {slot}: {error}")
        })?;
    revoke_window_relation(client, &relation, "state")?;
    let oid = resolve_relation_oid(client, &relation)?;
    catalog_state(client, result_oid, stage_id, slot, oid)?;
    Ok(relation)
}

fn revoke_window_relation(
    client: &mut SpiClient<'_>,
    relation: &str,
    label: &str,
) -> Result<(), String> {
    client
        .update(
            &format!("REVOKE ALL ON TABLE {relation} FROM PUBLIC"),
            None,
            &[],
        )
        .map_err(|error| format!("could not protect Window {label}: {error}"))?;
    Ok(())
}

fn resolve_window_function_client(
    client: &mut SpiClient<'_>,
    function: &WindowExpr,
) -> Result<WindowFunctionCapability, String> {
    let arguments = unsafe {
        [
            DatumWithOid::new(pg_sys::Oid::from(function.function_oid), pg_sys::OIDOID),
            DatumWithOid::new(pg_sys::Oid::from(function.type_.type_oid), pg_sys::OIDOID),
        ]
    };
    if function.aggregate {
        let rows = client
            .select(AGGREGATE_CAPABILITY_SQL, None, &arguments)
            .map_err(|error| format!("could not resolve Window aggregate: {error}"))?;
        return decode_aggregate_capability(
            rows,
            function.function_oid,
            function.args.len(),
            function.input_collation_oid,
        )
        .map(WindowFunctionCapability::Aggregate);
    }
    if function.filter.is_some() || function.star {
        return Err("native Window function cannot use FILTER or star".into());
    }
    let rows = client
        .select(
            r#"
            SELECT procedure.proname::text,procedure.pronargs::integer
            FROM pg_catalog.pg_proc AS procedure
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid=procedure.pronamespace
            WHERE procedure.oid=$1 AND procedure.prokind='w'
              AND procedure.provolatile='i' AND namespace.nspname='pg_catalog'
            "#,
            None,
            &arguments[..1],
        )
        .map_err(|error| format!("could not resolve native Window function: {error}"))?;
    decode_native_window(rows, function).map(WindowFunctionCapability::Native)
}

fn decode_native_window(
    rows: SpiTupleTable<'_>,
    function: &WindowExpr,
) -> Result<NativeWindow, String> {
    if rows.len() != 1 {
        return Err("Window function has no trusted native capability".into());
    }
    let row = rows.first();
    let name: String = window_required(&row, 1, "Window function name")?;
    let arity: i32 = window_required(&row, 2, "Window function arity")?;
    if usize::try_from(arity).ok() != Some(function.args.len()) {
        return Err("Window function arity changed".into());
    }
    match (name.as_str(), function.args.len()) {
        ("row_number", 0) => Ok(NativeWindow::RowNumber),
        ("rank", 0) => Ok(NativeWindow::Rank),
        ("dense_rank", 0) => Ok(NativeWindow::DenseRank),
        ("percent_rank", 0) => Ok(NativeWindow::PercentRank),
        ("cume_dist", 0) => Ok(NativeWindow::CumeDist),
        ("ntile", 1) => Ok(NativeWindow::Ntile),
        ("lag", 1..=3) => Ok(NativeWindow::Lag),
        ("lead", 1..=3) => Ok(NativeWindow::Lead),
        ("first_value", 1) => Ok(NativeWindow::FirstValue),
        ("last_value", 1) => Ok(NativeWindow::LastValue),
        ("nth_value", 2) => Ok(NativeWindow::NthValue),
        _ => Err(format!("Window function {name} has no bounded capability")),
    }
}

/// Execute exactly one durable Window action.
pub(crate) fn execute(
    mut transaction: StepTxn<'_, '_>,
    plan: &DataflowPlan,
    stage_id: u32,
) -> Result<StepExecution, String> {
    let stage = plan
        .stages
        .get(usize::try_from(stage_id).map_err(|_| "Window stage ID exceeds usize")?)
        .ok_or_else(|| format!("dataflow has no Window stage {stage_id}"))?;
    let OperatorSpec::Window(spec) = &stage.spec else {
        return Err("Window kernel received another operator".into());
    };
    if stage.inputs.len() != 1
        || transaction.inputs().len() != 1
        || transaction.input(0)?.port != 0
        || transaction.input(0)?.producer != ProducerKind::Operator
    {
        return Err("Window must have one operator input".into());
    }
    let capabilities = spec
        .functions
        .iter()
        .map(|function| resolve_window_function(&mut transaction, function))
        .collect::<Result<Vec<_>, _>>()?;
    let storage = load_window_storage(&mut transaction, stage, spec, &capabilities)?;
    let expressions = compile_window_expressions(
        &mut transaction,
        plan,
        stage,
        spec,
        &storage.input_type,
        &storage.output_type,
        capabilities,
    )?;
    let machine = WindowMachine::new(
        expressions
            .functions
            .iter()
            .map(|function| match function.capability {
                WindowFunctionCapability::Native(_) => WindowFunctionKind::Native,
                WindowFunctionCapability::Aggregate(_) => WindowFunctionKind::Aggregate,
            })
            .collect(),
    )?;
    let durable = load_window_continuation(&mut transaction, &storage.continuation)?;
    if transaction.checkpoint_had_continuation() != durable.is_some() {
        return Err("Window checkpoint disagrees with its typed continuation".into());
    }
    let current = match durable {
        Some(durable) => durable,
        None => start_window_continuation(&mut transaction, &storage)?,
    };
    if current.continuation.input_stream_id != transaction.input(0)?.stream_id {
        return Err("Window continuation changed its input stream".into());
    }
    if let Some(input) = current.continuation.input {
        if input.stream_id != transaction.input(0)?.stream_id
            || input.chunk_seq != transaction.input(0)?.next_chunk_seq
        {
            return Err("Window continuation is not at its input cursor".into());
        }
    }
    let action = machine.action(current.continuation)?;
    let result = match action {
        WindowAction::Admit { input } => WindowActionResult::Admitted(run_window_admission(
            &mut transaction,
            &storage,
            &expressions,
            input,
        )?),
        WindowAction::Enumerate {
            partition_queue_id,
            cursor,
        } => WindowActionResult::Enumerated(run_window_enumeration(
            &mut transaction,
            &storage,
            &expressions,
            partition_queue_id,
            cursor,
        )?),
        WindowAction::BuildPeers {
            partition_queue_id,
            cursor,
        } => WindowActionResult::PeersBuilt(run_window_peers(
            &mut transaction,
            &storage,
            &expressions,
            partition_queue_id,
            cursor,
        )?),
        WindowAction::BuildFrames {
            partition_queue_id,
            cursor,
        } => WindowActionResult::FramesBuilt(run_window_frames(
            &mut transaction,
            &storage,
            &expressions,
            spec,
            partition_queue_id,
            cursor,
        )?),
        WindowAction::FoldAggregate {
            partition_queue_id,
            function_ordinal,
            cursor,
        } => WindowActionResult::AggregateFolded(run_window_aggregate_fold(
            &mut transaction,
            &storage,
            &expressions,
            partition_queue_id,
            function_ordinal,
            cursor,
        )?),
        WindowAction::Evaluate {
            partition_queue_id,
            function_ordinal,
            cursor,
        } => WindowActionResult::Evaluated(run_window_evaluate(
            &mut transaction,
            &storage,
            &expressions,
            partition_queue_id,
            function_ordinal,
            cursor,
        )?),
        WindowAction::Diff {
            partition_queue_id,
            leg,
            cursor,
        } => WindowActionResult::Diffed(run_window_diff(
            &mut transaction,
            &storage,
            partition_queue_id,
            leg,
            cursor,
        )?),
        WindowAction::Cleanup {
            partition_queue_id,
            cursor,
        } => {
            let after = phase_after_partitions(current.continuation.phase)?;
            let cleanup = run_window_cleanup(
                &mut transaction,
                &storage,
                &expressions,
                partition_queue_id,
                cursor,
                after,
            )?;
            WindowActionResult::Cleaned(cleanup)
        }
        WindowAction::ForwardFrontier { input } => {
            WindowActionResult::FrontierForwarded(run_window_frontier(&mut transaction, input)?)
        }
    };
    let transition = machine.apply(current.continuation, result, transaction.budget())?;
    let WindowTransition::Committed {
        continuation: next,
        facts,
    } = transition;
    let has_continuation = next.is_some();
    if facts.continuation_rows != u64::from(has_continuation) {
        return Err("Window continuation mutation disagrees with primitive facts".into());
    }
    replace_window_continuation(
        &mut transaction,
        &storage.continuation,
        current.persisted.then_some(current.continuation),
        next,
    )?;
    transaction.finish(has_continuation, facts.usage)
}

fn start_window_continuation(
    transaction: &mut StepTxn<'_, '_>,
    storage: &WindowStorage,
) -> Result<DurableWindow, String> {
    let chunk = next_chunk(transaction, 0)?
        .ok_or_else(|| "runnable Window has no input chunk".to_string())?;
    let input = InputPosition::new(chunk.stream_id, chunk.sequence, 0)?;
    let (input, phase) = match chunk.kind {
        ChunkKind::Data => (Some(input), WindowPhase::Admit),
        ChunkKind::Frontier => {
            let arguments: [DatumWithOid<'_>; 0] = [];
            let rows = transaction.read(
                &format!(
                    "SELECT min(partition_id)::bigint FROM {} WHERE dirty",
                    storage.partitions.sql()
                ),
                &arguments,
            )?;
            if rows.len() != 1 {
                return Err("Window dirty queue returned no summary".into());
            }
            match rows
                .first()
                .get::<i64>(1)
                .map_err(|error| error.to_string())?
            {
                Some(first_partition_queue_id) => (
                    None,
                    WindowPhase::Enumerate {
                        partition_queue_id: first_partition_queue_id,
                        cursor: WindowCursor::default(),
                        after_partitions: AfterPartitions::Frontier(input),
                    },
                ),
                None => (Some(input), WindowPhase::Frontier),
            }
        }
    };
    Ok(DurableWindow {
        continuation: WindowContinuation {
            input_stream_id: chunk.stream_id,
            input,
            phase,
        },
        persisted: false,
    })
}

fn load_window_storage(
    transaction: &mut StepTxn<'_, '_>,
    stage: &DataflowStage,
    spec: &WindowSpec,
    capabilities: &[WindowFunctionCapability],
) -> Result<WindowStorage, String> {
    if capabilities.len() != spec.functions.len() {
        return Err("Window capability count changed".into());
    }
    let input_stream = transaction.input(0)?.stream_id;
    let output_stream = transaction.output()?.stream_id;
    let input_payload = transaction.payload_storage(input_stream)?;
    let output_payload = transaction.payload_storage(output_stream)?;
    let mut accumulators = Vec::with_capacity(spec.functions.len());
    let mut ntile_states = Vec::with_capacity(spec.functions.len());
    for (index, capability) in capabilities.iter().enumerate() {
        accumulators.push(
            if matches!(capability, WindowFunctionCapability::Aggregate(_)) {
                Some(
                    transaction.state_storage(
                        i32::try_from(1001 + index)
                            .map_err(|_| "Window accumulator slot exceeds integer")?,
                    )?,
                )
            } else {
                None
            },
        );
        ntile_states.push(
            if matches!(
                capability,
                WindowFunctionCapability::Native(NativeWindow::Ntile)
            ) {
                Some(
                    transaction.state_storage(
                        i32::try_from(2001 + index)
                            .map_err(|_| "Window ntile state slot exceeds integer")?,
                    )?,
                )
            } else {
                None
            },
        );
    }
    let storage = WindowStorage {
        partitions: transaction.state_storage(0)?,
        input: transaction.state_storage(1)?,
        ordered: transaction.state_storage(2)?,
        peers: transaction.state_storage(3)?,
        frames: transaction.state_storage(4)?,
        candidate: transaction.state_storage(5)?,
        visible: transaction.state_storage(6)?,
        continuation: transaction.continuation_storage()?,
        accumulators,
        ntile_states,
        input_payload: input_payload.relation,
        output_payload: output_payload.relation,
        input_type: input_payload.row_type,
        output_type: output_payload.row_type,
    };
    validate_window_storage(transaction, &storage, stage, spec, capabilities)?;
    Ok(storage)
}

fn validate_window_storage(
    transaction: &mut StepTxn<'_, '_>,
    storage: &WindowStorage,
    stage: &DataflowStage,
    spec: &WindowSpec,
    capabilities: &[WindowFunctionCapability],
) -> Result<(), String> {
    let partitions = transaction.relation_attributes(storage.partitions.oid())?;
    if partitions.len() != 4 + spec.partition_by.len()
        || !window_attribute_is(&partitions[0], "partition_id", pg_sys::INT8OID, true)
        || !window_attribute_is(&partitions[1], "dirty", pg_sys::BOOLOID, true)
        || !window_attribute_is(&partitions[2], "causal_lsn", pg_sys::PG_LSNOID, false)
        || !window_attribute_is(&partitions[3], "row_count", pg_sys::NUMERICOID, true)
    {
        return Err("Window partition relation has an invalid ABI".into());
    }
    for (index, (attribute, key)) in partitions[4..].iter().zip(&spec.partition_by).enumerate() {
        if attribute.name != format!("partition_{}", index + 1)
            || !attribute_matches_slot(attribute, &key.type_)
        {
            return Err("Window partition key changed its typed ABI".into());
        }
    }

    let input = transaction.relation_attributes(storage.input.oid())?;
    if input.len() != 5 + spec.order_by.len()
        || !window_attribute_is(&input[0], "entry_id", pg_sys::INT8OID, true)
        || !window_attribute_is(&input[1], "row_key", pg_sys::BYTEAOID, true)
        || input[2].name != "row_value"
        || input[2].type_oid != storage.input_type.oid()
        || !input[2].not_null
        || !window_attribute_is(&input[3], "multiplicity", pg_sys::NUMERICOID, true)
        || !window_attribute_is(&input[4], "partition_id", pg_sys::INT8OID, true)
    {
        return Err("Window input relation has an invalid ABI".into());
    }
    for (index, (attribute, key)) in input[5..].iter().zip(&spec.order_by).enumerate() {
        if attribute.name != format!("order_{}", index + 1)
            || !attribute_matches_slot(attribute, &key.type_)
        {
            return Err("Window order key changed its typed ABI".into());
        }
    }

    let ordered = transaction.relation_attributes(storage.ordered.oid())?;
    if ordered.len() != 4 + spec.functions.len()
        || !window_attribute_is(&ordered[0], "ordinal", pg_sys::INT8OID, true)
        || !window_attribute_is(&ordered[1], "entry_id", pg_sys::INT8OID, true)
        || !window_attribute_is(&ordered[2], "copy_ordinal", pg_sys::INT8OID, true)
        || !window_attribute_is(&ordered[3], "peer_id", pg_sys::INT8OID, false)
    {
        return Err("Window ordered relation has an invalid ABI".into());
    }
    for (index, (attribute, function)) in ordered[4..].iter().zip(&spec.functions).enumerate() {
        if attribute.name != format!("function_{}", index + 1)
            || !attribute_matches_slot(attribute, &function.type_)
            || attribute.not_null
        {
            return Err("Window function result changed its typed ABI".into());
        }
    }
    validate_exact_window_attributes(
        transaction,
        &storage.peers,
        &[
            ("peer_id", pg_sys::INT8OID, true),
            ("first_ordinal", pg_sys::INT8OID, true),
            ("last_ordinal", pg_sys::INT8OID, true),
        ],
        "peer",
    )?;
    validate_exact_window_attributes(
        transaction,
        &storage.frames,
        &[
            ("ordinal", pg_sys::INT8OID, true),
            ("start_1", pg_sys::INT8OID, false),
            ("end_1", pg_sys::INT8OID, false),
            ("start_2", pg_sys::INT8OID, false),
            ("end_2", pg_sys::INT8OID, false),
            ("start_3", pg_sys::INT8OID, false),
            ("end_3", pg_sys::INT8OID, false),
            ("frame_count", pg_sys::INT8OID, true),
        ],
        "frame",
    )?;
    validate_window_output_state(
        transaction,
        &storage.candidate,
        "candidate_id",
        &storage.output_type,
        "candidate",
    )?;
    validate_window_output_state(
        transaction,
        &storage.visible,
        "visible_id",
        &storage.output_type,
        "visible",
    )?;
    validate_window_continuation_abi(transaction, &storage.continuation)?;
    if storage.accumulators.len() != capabilities.len()
        || storage.ntile_states.len() != capabilities.len()
    {
        return Err("Window function state count changed".into());
    }
    for (index, capability) in capabilities.iter().enumerate() {
        match capability {
            WindowFunctionCapability::Aggregate(capability) => {
                let accumulator = storage.accumulators[index]
                    .as_ref()
                    .ok_or_else(|| "Window aggregate omitted its accumulator".to_string())?;
                if storage.ntile_states[index].is_some() {
                    return Err("Window aggregate has native state".into());
                }
                let attributes = transaction.relation_attributes(accumulator.oid())?;
                if attributes.len() != 5
                    || !window_attribute_is(&attributes[0], "singleton", pg_sys::BOOLOID, true)
                    || !window_attribute_is(&attributes[1], "partition_id", pg_sys::INT8OID, true)
                    || !window_attribute_is(&attributes[2], "output_ordinal", pg_sys::INT8OID, true)
                    || attributes[3].name != "state_value"
                    || attributes[3].type_oid != capability.transition_type_oid
                    || attributes[3].collation_oid != capability.transition_collation_oid
                    || !window_attribute_is(&attributes[4], "no_trans_value", pg_sys::BOOLOID, true)
                {
                    return Err("Window aggregate accumulator has an invalid ABI".into());
                }
            }
            WindowFunctionCapability::Native(NativeWindow::Ntile) => {
                if storage.accumulators[index].is_some() {
                    return Err("Window ntile has aggregate state".into());
                }
                let state = storage.ntile_states[index]
                    .as_ref()
                    .ok_or_else(|| "Window ntile omitted its state".to_string())?;
                validate_exact_window_attributes(
                    transaction,
                    state,
                    &[
                        ("singleton", pg_sys::BOOLOID, true),
                        ("partition_id", pg_sys::INT8OID, true),
                        ("bucket_count", pg_sys::INT8OID, false),
                        ("first_ordinal", pg_sys::INT8OID, false),
                    ],
                    "ntile state",
                )?;
            }
            WindowFunctionCapability::Native(_) => {
                if storage.accumulators[index].is_some() || storage.ntile_states[index].is_some() {
                    return Err("stateless Window function has durable state".into());
                }
            }
        }
    }
    let output = transaction.composite_attributes(&storage.output_type)?;
    validate_output_attributes(&output, &stage.schema.outputs)?;
    Ok(())
}

fn validate_window_output_state(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
    identity: &str,
    output_type: &TypeRef,
    label: &str,
) -> Result<(), String> {
    let attributes = transaction.relation_attributes(relation.oid())?;
    if attributes.len() != 5
        || !window_attribute_is(&attributes[0], identity, pg_sys::INT8OID, true)
        || !window_attribute_is(&attributes[1], "partition_id", pg_sys::INT8OID, true)
        || !window_attribute_is(&attributes[2], "output_key", pg_sys::BYTEAOID, true)
        || attributes[3].name != "output_row"
        || attributes[3].type_oid != output_type.oid()
        || !attributes[3].not_null
        || !window_attribute_is(&attributes[4], "multiplicity", pg_sys::NUMERICOID, true)
    {
        return Err(format!("Window {label} relation has an invalid ABI"));
    }
    Ok(())
}

fn validate_window_continuation_abi(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
) -> Result<(), String> {
    validate_exact_window_attributes(
        transaction,
        relation,
        &[
            ("singleton", pg_sys::BOOLOID, true),
            ("phase", pg_sys::INT2OID, true),
            ("input_stream_id", pg_sys::INT8OID, true),
            ("input_chunk_seq", pg_sys::INT8OID, false),
            ("input_row_ordinal", pg_sys::INT8OID, false),
            ("partition_queue_id", pg_sys::INT8OID, false),
            ("function_ordinal", pg_sys::INT4OID, false),
            ("output_ordinal", pg_sys::INT8OID, false),
            ("cursor_row_id", pg_sys::INT8OID, false),
            ("fold_ready", pg_sys::BOOLOID, true),
            ("cursor_repeat", pg_sys::BOOLOID, true),
            ("diff_leg", pg_sys::INT2OID, false),
            ("cleanup_ordinal", pg_sys::INT4OID, false),
            ("after_kind", pg_sys::INT2OID, false),
            ("after_chunk_seq", pg_sys::INT8OID, false),
            ("after_row_ordinal", pg_sys::INT8OID, false),
        ],
        "continuation",
    )
}

fn validate_exact_window_attributes(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
    expected: &[(&str, pg_sys::Oid, bool)],
    label: &str,
) -> Result<(), String> {
    let attributes = transaction.relation_attributes(relation.oid())?;
    if attributes.len() != expected.len()
        || attributes
            .iter()
            .zip(expected)
            .any(|(actual, (name, type_oid, not_null))| {
                !window_attribute_is(actual, name, *type_oid, *not_null)
            })
    {
        return Err(format!("Window {label} relation has an invalid ABI"));
    }
    Ok(())
}

fn compile_window_expressions(
    transaction: &mut StepTxn<'_, '_>,
    plan: &DataflowPlan,
    stage: &DataflowStage,
    spec: &WindowSpec,
    input_type: &TypeRef,
    output_type: &TypeRef,
    capabilities: Vec<WindowFunctionCapability>,
) -> Result<WindowExpressions, String> {
    validate_window_frame(spec)?;
    if capabilities.len() != spec.functions.len() {
        return Err("Window capability count changed".into());
    }
    let input_bindings = compile_stage_bindings(
        transaction,
        plan,
        stage,
        &[BindingInput {
            row_type: input_type,
            alias: "input_row",
        }],
    )?;
    let current_bindings = compile_stage_bindings(
        transaction,
        plan,
        stage,
        &[BindingInput {
            row_type: input_type,
            alias: "current_input",
        }],
    )?;
    let target_bindings = compile_stage_bindings(
        transaction,
        plan,
        stage,
        &[BindingInput {
            row_type: input_type,
            alias: "target_input",
        }],
    )?;
    let partition_expressions = spec
        .partition_by
        .iter()
        .map(|key| compile_scalar_expression(&key.expr, &input_bindings))
        .collect::<Result<Vec<_>, _>>()?;
    let partition_columns = (1..=partition_expressions.len())
        .map(|index| format!("partition_{index}"))
        .collect();
    let order_expressions = spec
        .order_by
        .iter()
        .map(|key| compile_scalar_expression(&key.expr, &input_bindings))
        .collect::<Result<Vec<_>, _>>()?;
    let order_columns = (1..=order_expressions.len())
        .map(|index| format!("order_{index}"))
        .collect::<Vec<_>>();
    let resolved = spec
        .order_by
        .iter()
        .map(|key| resolve_btree_step(transaction, key, "Window"))
        .collect::<Result<Vec<_>, _>>()?;
    let order_by = resolved
        .iter()
        .enumerate()
        .map(|(index, order)| {
            format!(
                "input_row.order_{} USING {} NULLS {}",
                index + 1,
                order.sort_operator,
                if order.nulls_first { "FIRST" } else { "LAST" }
            )
        })
        .chain(std::iter::once("input_row.entry_id ASC".into()))
        .collect::<Vec<_>>()
        .join(",");
    let keyset_after = window_keyset_sql(&resolved, "input_row", "boundary");
    let peer_equal = window_keys_equal_sql(&resolved, "next_row", "boundary_row");
    let outputs = compile_window_outputs(&stage.schema.outputs, spec, &input_bindings)?;
    let mut functions = Vec::with_capacity(spec.functions.len());
    for (function, capability) in spec.functions.iter().zip(capabilities) {
        functions.push(WindowFunctionPlan {
            current_arguments: function
                .args
                .iter()
                .map(|argument| compile_scalar_expression(argument, &current_bindings))
                .collect::<Result<_, _>>()?,
            target_arguments: function
                .args
                .iter()
                .map(|argument| compile_scalar_expression(argument, &target_bindings))
                .collect::<Result<_, _>>()?,
            filter: function
                .filter
                .as_ref()
                .map(|filter| compile_scalar_expression(filter, &current_bindings))
                .transpose()?
                .unwrap_or_else(|| "true".into()),
            result_type: resolve_window_type_sql(transaction, &function.type_)?,
            capability,
        });
    }
    let frame_start_offset = spec
        .frame
        .start_offset
        .as_ref()
        .map(|offset| compile_scalar_expression(offset, &current_bindings))
        .transpose()?;
    let frame_end_offset = spec
        .frame
        .end_offset
        .as_ref()
        .map(|offset| compile_scalar_expression(offset, &current_bindings))
        .transpose()?;
    let output_attributes = transaction.composite_attributes(output_type)?;
    validate_output_attributes(&output_attributes, &stage.schema.outputs)?;
    Ok(WindowExpressions {
        partition_expressions,
        partition_columns,
        order_expressions,
        order_columns,
        order_by,
        keyset_after,
        peer_equal,
        outputs,
        functions,
        frame_start_offset,
        frame_end_offset,
    })
}

fn validate_window_frame(spec: &WindowSpec) -> Result<(), String> {
    let options = spec.frame.options;
    let modes = [
        pg_sys::FRAMEOPTION_ROWS,
        pg_sys::FRAMEOPTION_RANGE,
        pg_sys::FRAMEOPTION_GROUPS,
    ]
    .into_iter()
    .filter(|flag| options & flag != 0)
    .count();
    if modes != 1 {
        return Err("Window frame has no unique ROWS, RANGE, or GROUPS mode".into());
    }
    let starts = [
        pg_sys::FRAMEOPTION_START_UNBOUNDED_PRECEDING,
        pg_sys::FRAMEOPTION_START_CURRENT_ROW,
        pg_sys::FRAMEOPTION_START_OFFSET_PRECEDING,
        pg_sys::FRAMEOPTION_START_OFFSET_FOLLOWING,
    ]
    .into_iter()
    .filter(|flag| options & flag != 0)
    .count();
    let ends = [
        pg_sys::FRAMEOPTION_END_UNBOUNDED_FOLLOWING,
        pg_sys::FRAMEOPTION_END_CURRENT_ROW,
        pg_sys::FRAMEOPTION_END_OFFSET_PRECEDING,
        pg_sys::FRAMEOPTION_END_OFFSET_FOLLOWING,
    ]
    .into_iter()
    .filter(|flag| options & flag != 0)
    .count();
    if starts != 1 || ends != 1 {
        return Err("Window frame has invalid start or end bounds".into());
    }
    let start_offset = options & pg_sys::FRAMEOPTION_START_OFFSET != 0;
    let end_offset = options & pg_sys::FRAMEOPTION_END_OFFSET != 0;
    if start_offset != spec.frame.start_offset.is_some()
        || end_offset != spec.frame.end_offset.is_some()
    {
        return Err("Window frame offset expression does not match its options".into());
    }
    if options & pg_sys::FRAMEOPTION_RANGE != 0 && (start_offset || end_offset) {
        return Err(
            "resumable Window RANGE offsets are not supported by the bounded frame ABI".into(),
        );
    }
    Ok(())
}

fn compile_window_outputs(
    outputs: &[OutputSlot],
    spec: &WindowSpec,
    bindings: &[SqlBinding],
) -> Result<String, String> {
    if outputs.len() != spec.outputs.len() + spec.functions.len() {
        return Err("Window outputs do not match its stage schema".into());
    }
    let mut sql = compile_named_outputs(
        &outputs[..spec.outputs.len()],
        &spec.outputs,
        bindings,
        "Window passthrough",
    )?;
    for (index, (output, function)) in outputs[spec.outputs.len()..]
        .iter()
        .zip(&spec.functions)
        .enumerate()
    {
        if output.slot != function.output {
            return Err("Window function output order changed".into());
        }
        sql.push(format!("updated.function_{}", index + 1));
    }
    Ok(sql.join(","))
}

fn resolve_window_type_sql(
    transaction: &mut StepTxn<'_, '_>,
    type_: &SlotType,
) -> Result<String, String> {
    let arguments = unsafe {
        [
            DatumWithOid::new(pg_sys::Oid::from(type_.type_oid), pg_sys::OIDOID),
            DatumWithOid::new(type_.typmod, pg_sys::INT4OID),
        ]
    };
    let rows = transaction.read(
        r#"
        SELECT pg_catalog.format_type(type_catalog.oid,$2)
        FROM pg_catalog.pg_type AS type_catalog
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid=type_catalog.typnamespace
        WHERE type_catalog.oid=$1 AND type_catalog.typtype<>'p'
          AND namespace.nspname='pg_catalog'
        "#,
        &arguments,
    )?;
    if rows.len() != 1 {
        return Err("Window function result type is not a trusted pg_catalog type".into());
    }
    window_required(&rows.first(), 1, "Window function result type")
}

fn window_keyset_sql(orders: &[BtreeOrder], current_alias: &str, boundary_alias: &str) -> String {
    let mut alternatives = Vec::with_capacity(orders.len() + 1);
    let mut prefix = Vec::new();
    for (index, order) in orders.iter().enumerate() {
        let column = format!("order_{}", index + 1);
        let before = format!("{boundary_alias}.{column}");
        let current = format!("{current_alias}.{column}");
        let after = if order.nulls_first {
            format!(
                "(CASE WHEN {before} IS NULL THEN {current} IS NOT NULL \
                 WHEN {current} IS NULL THEN false \
                 ELSE {before} {} {current} END)",
                order.sort_operator
            )
        } else {
            format!(
                "(CASE WHEN {before} IS NULL THEN false \
                 WHEN {current} IS NULL THEN true \
                 ELSE {before} {} {current} END)",
                order.sort_operator
            )
        };
        alternatives.push(if prefix.is_empty() {
            after
        } else {
            format!("({} AND {after})", prefix.join(" AND "))
        });
        prefix.push(format!(
            "(({before} IS NULL AND {current} IS NULL) OR \
             ({before} IS NOT NULL AND {current} IS NOT NULL \
              AND {before} {} {current}))",
            order.equality_operator
        ));
    }
    let id = format!("{current_alias}.entry_id>{boundary_alias}.entry_id");
    alternatives.push(if prefix.is_empty() {
        id
    } else {
        format!("({} AND {id})", prefix.join(" AND "))
    });
    alternatives.join(" OR ")
}

fn window_keys_equal_sql(orders: &[BtreeOrder], left: &str, right: &str) -> String {
    if orders.is_empty() {
        return "true".into();
    }
    orders
        .iter()
        .enumerate()
        .map(|(index, order)| {
            let column = format!("order_{}", index + 1);
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

fn resolve_window_function(
    transaction: &mut StepTxn<'_, '_>,
    function: &WindowExpr,
) -> Result<WindowFunctionCapability, String> {
    if function.aggregate {
        if function.star && !function.args.is_empty() {
            return Err("Window aggregate star has explicit arguments".into());
        }
        return load_window_aggregate(transaction, function)
            .map(WindowFunctionCapability::Aggregate);
    }
    if function.filter.is_some() || function.star {
        return Err("native Window function cannot use FILTER or star".into());
    }
    let arguments = unsafe {
        [DatumWithOid::new(
            pg_sys::Oid::from(function.function_oid),
            pg_sys::OIDOID,
        )]
    };
    let rows = transaction.read(
        r#"
        SELECT procedure.proname::text,procedure.pronargs::integer
        FROM pg_catalog.pg_proc AS procedure
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid=procedure.pronamespace
        WHERE procedure.oid=$1 AND procedure.prokind='w'
          AND procedure.provolatile='i' AND namespace.nspname='pg_catalog'
        "#,
        &arguments,
    )?;
    if rows.len() != 1 {
        return Err(format!(
            "Window function OID {} is not a trusted native capability",
            function.function_oid
        ));
    }
    let row = rows.first();
    let name: String = window_required(&row, 1, "Window function name")?;
    let arity: i32 = window_required(&row, 2, "Window function arity")?;
    if usize::try_from(arity).ok() != Some(function.args.len()) {
        return Err("Window function arity changed".into());
    }
    let capability = match (name.as_str(), function.args.len()) {
        ("row_number", 0) => NativeWindow::RowNumber,
        ("rank", 0) => NativeWindow::Rank,
        ("dense_rank", 0) => NativeWindow::DenseRank,
        ("percent_rank", 0) => NativeWindow::PercentRank,
        ("cume_dist", 0) => NativeWindow::CumeDist,
        ("ntile", 1) => NativeWindow::Ntile,
        ("lag", 1..=3) => NativeWindow::Lag,
        ("lead", 1..=3) => NativeWindow::Lead,
        ("first_value", 1) => NativeWindow::FirstValue,
        ("last_value", 1) => NativeWindow::LastValue,
        ("nth_value", 2) => NativeWindow::NthValue,
        _ => return Err(format!("Window function {name} has no bounded capability")),
    };
    Ok(WindowFunctionCapability::Native(capability))
}

fn load_window_aggregate(
    transaction: &mut StepTxn<'_, '_>,
    function: &WindowExpr,
) -> Result<AggregateCapability, String> {
    let arguments = unsafe {
        [
            DatumWithOid::new(pg_sys::Oid::from(function.function_oid), pg_sys::OIDOID),
            DatumWithOid::new(pg_sys::Oid::from(function.type_.type_oid), pg_sys::OIDOID),
        ]
    };
    let rows = transaction.read(AGGREGATE_CAPABILITY_SQL, &arguments)?;
    decode_aggregate_capability(
        rows,
        function.function_oid,
        function.args.len(),
        function.input_collation_oid,
    )
}

fn window_attribute_is(
    attribute: &AttributeRef,
    name: &str,
    type_oid: pg_sys::Oid,
    not_null: bool,
) -> bool {
    attribute.name == name && attribute.type_oid == type_oid && attribute.not_null == not_null
}

fn window_required<T: FromDatum + IntoDatum>(
    table: &SpiTupleTable<'_>,
    ordinal: usize,
    name: &str,
) -> Result<T, String> {
    table
        .get::<T>(ordinal)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("database returned NULL {name}"))
}

fn window_nonnegative(value: i64, name: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("database returned negative {name}"))
}

fn window_i64_budget(value: usize, name: &str) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("{name} exceeds bigint"))
}

#[derive(Clone, Copy, Debug)]
struct WindowFields {
    phase: i16,
    input_stream_id: i64,
    input_chunk_seq: Option<i64>,
    input_row_ordinal: Option<i64>,
    partition_queue_id: Option<i64>,
    function_ordinal: Option<i32>,
    output_ordinal: Option<i64>,
    cursor_row_id: Option<i64>,
    fold_ready: bool,
    cursor_repeat: bool,
    diff_leg: Option<i16>,
    cleanup_ordinal: Option<i32>,
    after_kind: Option<i16>,
    after_chunk_seq: Option<i64>,
    after_row_ordinal: Option<i64>,
}

fn load_window_continuation(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
) -> Result<Option<DurableWindow>, String> {
    let query = format!(
        r#"
        SELECT phase,input_stream_id,input_chunk_seq,input_row_ordinal,
               partition_queue_id,function_ordinal,output_ordinal,
               cursor_row_id,fold_ready,cursor_repeat,diff_leg,
               cleanup_ordinal,after_kind,after_chunk_seq,after_row_ordinal
        FROM {} WHERE singleton FOR UPDATE
        "#,
        relation.sql()
    );
    let rows = transaction.lock(&query, &[])?;
    match rows.len() {
        0 => Ok(None),
        1 => {
            let row = rows.first();
            let fields = WindowFields {
                phase: window_required(&row, 1, "Window phase")?,
                input_stream_id: window_required(&row, 2, "Window input stream")?,
                input_chunk_seq: row.get(3).map_err(|error| error.to_string())?,
                input_row_ordinal: row.get(4).map_err(|error| error.to_string())?,
                partition_queue_id: row.get(5).map_err(|error| error.to_string())?,
                function_ordinal: row.get(6).map_err(|error| error.to_string())?,
                output_ordinal: row.get(7).map_err(|error| error.to_string())?,
                cursor_row_id: row.get(8).map_err(|error| error.to_string())?,
                fold_ready: window_required(&row, 9, "Window Fold ready state")?,
                cursor_repeat: window_required(&row, 10, "Window cursor repeat")?,
                diff_leg: row.get(11).map_err(|error| error.to_string())?,
                cleanup_ordinal: row.get(12).map_err(|error| error.to_string())?,
                after_kind: row.get(13).map_err(|error| error.to_string())?,
                after_chunk_seq: row.get(14).map_err(|error| error.to_string())?,
                after_row_ordinal: row.get(15).map_err(|error| error.to_string())?,
            };
            Ok(Some(DurableWindow {
                continuation: decode_window_fields(fields)?,
                persisted: true,
            }))
        }
        count => Err(format!(
            "Window continuation relation contains {count} rows"
        )),
    }
}

fn decode_window_fields(fields: WindowFields) -> Result<WindowContinuation, String> {
    let input = match (fields.input_chunk_seq, fields.input_row_ordinal) {
        (Some(chunk_seq), Some(row_ordinal)) => Some(InputPosition::new(
            fields.input_stream_id,
            chunk_seq,
            row_ordinal,
        )?),
        (None, None) => None,
        _ => return Err("Window continuation has a partial input cursor".into()),
    };
    let kind = WindowPhaseKind::from_code(PhaseCode::active(fields.phase)?)?;
    if kind != WindowPhaseKind::FoldAggregate && fields.fold_ready {
        return Err("non-Fold Window continuation contains ready state".into());
    }
    let queue = || {
        fields
            .partition_queue_id
            .ok_or_else(|| "Window continuation omitted its partition".to_string())
    };
    let cursor = WindowCursor {
        row_id: fields.cursor_row_id,
    };
    let after = || {
        decode_after_partitions(
            fields.input_stream_id,
            fields.after_kind,
            fields.after_chunk_seq,
            fields.after_row_ordinal,
        )
    };
    let plain = || -> Result<(), String> {
        if fields.function_ordinal.is_some()
            || fields.output_ordinal.is_some()
            || fields.cursor_repeat
            || fields.diff_leg.is_some()
            || fields.cleanup_ordinal.is_some()
        {
            Err("Window continuation contains another phase's fields".into())
        } else {
            Ok(())
        }
    };
    let phase = match kind {
        WindowPhaseKind::Admit | WindowPhaseKind::Frontier => {
            if fields.partition_queue_id.is_some()
                || fields.function_ordinal.is_some()
                || fields.output_ordinal.is_some()
                || fields.cursor_row_id.is_some()
                || fields.cursor_repeat
                || fields.diff_leg.is_some()
                || fields.cleanup_ordinal.is_some()
                || fields.after_kind.is_some()
                || fields.after_chunk_seq.is_some()
                || fields.after_row_ordinal.is_some()
            {
                return Err("Window idle phase contains work fields".into());
            }
            if kind == WindowPhaseKind::Admit {
                WindowPhase::Admit
            } else {
                WindowPhase::Frontier
            }
        }
        WindowPhaseKind::Enumerate => {
            plain()?;
            WindowPhase::Enumerate {
                partition_queue_id: queue()?,
                cursor,
                after_partitions: after()?,
            }
        }
        WindowPhaseKind::Peers => {
            plain()?;
            WindowPhase::Peers {
                partition_queue_id: queue()?,
                cursor,
                after_partitions: after()?,
            }
        }
        WindowPhaseKind::Frames => {
            plain()?;
            WindowPhase::Frames {
                partition_queue_id: queue()?,
                cursor,
                after_partitions: after()?,
            }
        }
        WindowPhaseKind::FoldAggregate | WindowPhaseKind::Evaluate => {
            if fields.cursor_repeat || fields.diff_leg.is_some() || fields.cleanup_ordinal.is_some()
            {
                return Err("Window function continuation contains another phase's fields".into());
            }
            let ordinal =
                u32::try_from(fields.function_ordinal.ok_or_else(|| {
                    "Window function continuation omitted its ordinal".to_string()
                })?)
                .map_err(|_| "Window function ordinal is negative")?;
            if kind == WindowPhaseKind::FoldAggregate {
                WindowPhase::FoldAggregate {
                    partition_queue_id: queue()?,
                    function_ordinal: ordinal,
                    cursor: WindowFoldCursor {
                        output_ordinal: fields.output_ordinal.ok_or_else(|| {
                            "Window aggregate fold omitted its output ordinal".to_string()
                        })?,
                        last_frame_ordinal: fields.cursor_row_id,
                        ready_to_finalize: fields.fold_ready,
                    },
                    after_partitions: after()?,
                }
            } else {
                if fields.output_ordinal.is_some() {
                    return Err("Window native evaluation contains an output ordinal".into());
                }
                WindowPhase::Evaluate {
                    partition_queue_id: queue()?,
                    function_ordinal: ordinal,
                    cursor,
                    after_partitions: after()?,
                }
            }
        }
        WindowPhaseKind::Diff => {
            if fields.function_ordinal.is_some()
                || fields.output_ordinal.is_some()
                || fields.cleanup_ordinal.is_some()
            {
                return Err("Window Diff continuation contains another phase's fields".into());
            }
            WindowPhase::Diff {
                partition_queue_id: queue()?,
                leg: match fields.diff_leg {
                    Some(1) => DiffLeg::Remove,
                    Some(2) => DiffLeg::Add,
                    _ => return Err("Window Diff continuation has an invalid leg".into()),
                },
                cursor: WindowDiffCursor {
                    row_id: fields.cursor_row_id,
                    repeat: fields.cursor_repeat,
                },
                after_partitions: after()?,
            }
        }
        WindowPhaseKind::Cleanup => {
            if fields.function_ordinal.is_some()
                || fields.output_ordinal.is_some()
                || fields.cursor_repeat
                || fields.diff_leg.is_some()
            {
                return Err("Window Cleanup continuation contains another phase's fields".into());
            }
            WindowPhase::Cleanup {
                partition_queue_id: queue()?,
                cursor: WindowCleanupCursor {
                    relation_ordinal: u32::try_from(fields.cleanup_ordinal.ok_or_else(|| {
                        "Window Cleanup continuation omitted its relation".to_string()
                    })?)
                    .map_err(|_| "Window cleanup ordinal is negative")?,
                    row: cursor,
                },
                after_partitions: after()?,
            }
        }
    };
    Ok(WindowContinuation {
        input_stream_id: fields.input_stream_id,
        input,
        phase,
    })
}

fn decode_after_partitions(
    input_stream_id: i64,
    kind: Option<i16>,
    chunk_seq: Option<i64>,
    row_ordinal: Option<i64>,
) -> Result<AfterPartitions, String> {
    match (kind, chunk_seq, row_ordinal) {
        (Some(1), Some(chunk), Some(row)) => Ok(AfterPartitions::Admit(InputPosition::new(
            input_stream_id,
            chunk,
            row,
        )?)),
        (Some(2), None, None) => Ok(AfterPartitions::FinishInput),
        (Some(3), Some(chunk), Some(row)) => Ok(AfterPartitions::Frontier(InputPosition::new(
            input_stream_id,
            chunk,
            row,
        )?)),
        _ => Err("Window continuation has an invalid partition target".into()),
    }
}

fn encode_window_fields(continuation: WindowContinuation) -> Result<WindowFields, String> {
    let mut fields = WindowFields {
        phase: continuation.phase.code().value(),
        input_stream_id: continuation.input_stream_id,
        input_chunk_seq: continuation.input.map(|input| input.chunk_seq),
        input_row_ordinal: continuation.input.map(|input| input.row_ordinal),
        partition_queue_id: None,
        function_ordinal: None,
        output_ordinal: None,
        cursor_row_id: None,
        fold_ready: false,
        cursor_repeat: false,
        diff_leg: None,
        cleanup_ordinal: None,
        after_kind: None,
        after_chunk_seq: None,
        after_row_ordinal: None,
    };
    match continuation.phase {
        WindowPhase::Admit | WindowPhase::Frontier => {}
        WindowPhase::Enumerate {
            partition_queue_id,
            cursor,
            after_partitions,
        }
        | WindowPhase::Peers {
            partition_queue_id,
            cursor,
            after_partitions,
        }
        | WindowPhase::Frames {
            partition_queue_id,
            cursor,
            after_partitions,
        } => {
            fields.partition_queue_id = Some(partition_queue_id);
            fields.cursor_row_id = cursor.row_id;
            encode_window_after(&mut fields, after_partitions);
        }
        WindowPhase::FoldAggregate {
            partition_queue_id,
            function_ordinal,
            cursor,
            after_partitions,
        } => {
            fields.partition_queue_id = Some(partition_queue_id);
            fields.function_ordinal =
                Some(i32::try_from(function_ordinal).map_err(|_| "Window function exceeds i32")?);
            fields.output_ordinal = Some(cursor.output_ordinal);
            fields.cursor_row_id = cursor.last_frame_ordinal;
            fields.fold_ready = cursor.ready_to_finalize;
            encode_window_after(&mut fields, after_partitions);
        }
        WindowPhase::Evaluate {
            partition_queue_id,
            function_ordinal,
            cursor,
            after_partitions,
        } => {
            fields.partition_queue_id = Some(partition_queue_id);
            fields.function_ordinal =
                Some(i32::try_from(function_ordinal).map_err(|_| "Window function exceeds i32")?);
            fields.cursor_row_id = cursor.row_id;
            encode_window_after(&mut fields, after_partitions);
        }
        WindowPhase::Diff {
            partition_queue_id,
            leg,
            cursor,
            after_partitions,
        } => {
            fields.partition_queue_id = Some(partition_queue_id);
            fields.cursor_row_id = cursor.row_id;
            fields.cursor_repeat = cursor.repeat;
            fields.diff_leg = Some(match leg {
                DiffLeg::Remove => 1,
                DiffLeg::Add => 2,
            });
            encode_window_after(&mut fields, after_partitions);
        }
        WindowPhase::Cleanup {
            partition_queue_id,
            cursor,
            after_partitions,
        } => {
            fields.partition_queue_id = Some(partition_queue_id);
            fields.cleanup_ordinal = Some(
                i32::try_from(cursor.relation_ordinal)
                    .map_err(|_| "Window cleanup ordinal exceeds i32")?,
            );
            fields.cursor_row_id = cursor.row.row_id;
            encode_window_after(&mut fields, after_partitions);
        }
    }
    Ok(fields)
}

fn encode_window_after(fields: &mut WindowFields, after: AfterPartitions) {
    match after {
        AfterPartitions::Admit(input) => {
            fields.after_kind = Some(1);
            fields.after_chunk_seq = Some(input.chunk_seq);
            fields.after_row_ordinal = Some(input.row_ordinal);
        }
        AfterPartitions::FinishInput => fields.after_kind = Some(2),
        AfterPartitions::Frontier(input) => {
            fields.after_kind = Some(3);
            fields.after_chunk_seq = Some(input.chunk_seq);
            fields.after_row_ordinal = Some(input.row_ordinal);
        }
    }
}

fn replace_window_continuation(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
    old: Option<WindowContinuation>,
    next: Option<WindowContinuation>,
) -> Result<(), String> {
    if let Some(old) = old {
        let fields = encode_window_fields(old)?;
        let query = format!(
            r#"
            DELETE FROM {}
            WHERE singleton AND phase=$1 AND input_stream_id=$2
              AND input_chunk_seq IS NOT DISTINCT FROM $3
              AND input_row_ordinal IS NOT DISTINCT FROM $4
              AND partition_queue_id IS NOT DISTINCT FROM $5
              AND function_ordinal IS NOT DISTINCT FROM $6
              AND output_ordinal IS NOT DISTINCT FROM $7
              AND cursor_row_id IS NOT DISTINCT FROM $8
              AND fold_ready=$9
              AND cursor_repeat=$10
              AND diff_leg IS NOT DISTINCT FROM $11
              AND cleanup_ordinal IS NOT DISTINCT FROM $12
              AND after_kind IS NOT DISTINCT FROM $13
              AND after_chunk_seq IS NOT DISTINCT FROM $14
              AND after_row_ordinal IS NOT DISTINCT FROM $15
            RETURNING singleton
            "#,
            relation.sql()
        );
        if transaction
            .write(&query, &window_field_arguments(&fields))?
            .len()
            != 1
        {
            return Err("Window continuation compare-and-set failed".into());
        }
    }
    if let Some(next) = next {
        let fields = encode_window_fields(next)?;
        let query = format!(
            r#"
            INSERT INTO {}(
              singleton,phase,input_stream_id,input_chunk_seq,input_row_ordinal,
              partition_queue_id,function_ordinal,output_ordinal,cursor_row_id,
              fold_ready,cursor_repeat,diff_leg,
              cleanup_ordinal,after_kind,after_chunk_seq,after_row_ordinal
            )
            VALUES(true,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
            RETURNING singleton
            "#,
            relation.sql()
        );
        if transaction
            .write(&query, &window_field_arguments(&fields))?
            .len()
            != 1
        {
            return Err("Window continuation insert failed".into());
        }
    }
    Ok(())
}

fn window_field_arguments(fields: &WindowFields) -> [DatumWithOid<'_>; 15] {
    unsafe {
        [
            DatumWithOid::new(fields.phase, pg_sys::INT2OID),
            DatumWithOid::new(fields.input_stream_id, pg_sys::INT8OID),
            DatumWithOid::new(fields.input_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(fields.input_row_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(fields.partition_queue_id, pg_sys::INT8OID),
            DatumWithOid::new(fields.function_ordinal, pg_sys::INT4OID),
            DatumWithOid::new(fields.output_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(fields.cursor_row_id, pg_sys::INT8OID),
            DatumWithOid::new(fields.fold_ready, pg_sys::BOOLOID),
            DatumWithOid::new(fields.cursor_repeat, pg_sys::BOOLOID),
            DatumWithOid::new(fields.diff_leg, pg_sys::INT2OID),
            DatumWithOid::new(fields.cleanup_ordinal, pg_sys::INT4OID),
            DatumWithOid::new(fields.after_kind, pg_sys::INT2OID),
            DatumWithOid::new(fields.after_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(fields.after_row_ordinal, pg_sys::INT8OID),
        ]
    }
}

fn run_window_admission(
    transaction: &mut StepTxn<'_, '_>,
    storage: &WindowStorage,
    expressions: &WindowExpressions,
    input: InputPosition,
) -> Result<WindowAdmission, String> {
    let input_state = transaction.input(0)?.clone();
    let input_chunk = chunk(transaction, &input_state, input.chunk_seq)?
        .ok_or_else(|| "Window admission references a missing input chunk".to_string())?;
    if input_chunk.kind != ChunkKind::Data || input_chunk.stream_id != input.stream_id {
        return Err("Window admission does not reference a data chunk".into());
    }
    if input.row_ordinal == 0 {
        payload_facts(transaction, &storage.input_payload, &input_chunk)?;
    }
    let chunk_rows =
        i64::try_from(input_chunk.rows).map_err(|_| "Window chunk rows exceed bigint")?;
    if input.row_ordinal >= chunk_rows {
        return Err("Window admission cursor is outside its chunk".into());
    }
    let budget = transaction.budget();
    let max_rows = window_i64_budget(budget.max_input_rows, "Window admission row budget")?;
    let max_bytes = window_i64_budget(budget.max_input_bytes, "Window admission byte budget")?;
    let causal_lsn = format_lsn(input_chunk.lsn);
    let evaluated = window_admission_evaluated_sql(storage, expressions);
    let partition_columns = expressions
        .partition_columns
        .iter()
        .map(|column| quote_identifier(column))
        .collect::<Vec<_>>();
    let partition_values = partition_columns
        .iter()
        .map(|column| format!("evaluated.{column}"))
        .collect::<Vec<_>>()
        .join(",");
    let partition_touch = if partition_columns.is_empty() {
        format!(
            "UPDATE {} SET dirty=true RETURNING partition_id",
            storage.partitions.sql()
        )
    } else {
        format!(
            r#"
            INSERT INTO {partitions} AS target({columns},dirty,row_count)
            SELECT DISTINCT {values},true,0::numeric
            FROM evaluated
            ON CONFLICT({columns}) DO UPDATE SET dirty=true
            RETURNING partition_id
            "#,
            partitions = storage.partitions.sql(),
            columns = partition_columns.join(","),
            values = partition_values,
        )
    };
    let touch_query = format!(
        r#"
        WITH {evaluated},
        touched AS ({partition_touch})
        SELECT count(*)::bigint FROM touched
        "#
    );
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
    let touched_rows = transaction.write(&touch_query, &arguments)?;
    if touched_rows.len() != 1
        || window_required::<i64>(&touched_rows.first(), 1, "Window touched partitions")? <= 0
    {
        return Err("Window admission did not resolve a partition".into());
    }

    let partition_predicate = if partition_columns.is_empty() {
        "true".into()
    } else {
        partition_columns
            .iter()
            .map(|column| format!("partition.{column} IS NOT DISTINCT FROM evaluated.{column}"))
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    let order_columns = expressions
        .order_columns
        .iter()
        .map(|column| quote_identifier(column))
        .collect::<Vec<_>>();
    let insert_orders = if order_columns.is_empty() {
        String::new()
    } else {
        format!(",{}", order_columns.join(","))
    };
    let decision_orders = if order_columns.is_empty() {
        String::new()
    } else {
        format!(
            ",{}",
            order_columns
                .iter()
                .map(|column| format!("decision.{column}"))
                .collect::<Vec<_>>()
                .join(",")
        )
    };
    let representative_orders = if order_columns.is_empty() {
        String::new()
    } else {
        format!(
            ",{}",
            order_columns
                .iter()
                .map(|column| format!("representative.{column}"))
                .collect::<Vec<_>>()
                .join(",")
        )
    };
    let update_orders = order_columns
        .iter()
        .map(|column| format!("{column}=EXCLUDED.{column}"))
        .collect::<Vec<_>>();
    let update_orders = if update_orders.is_empty() {
        String::new()
    } else {
        format!(",{}", update_orders.join(","))
    };
    let query = format!(
        r#"
        WITH {evaluated},
        assigned AS MATERIALIZED (
          SELECT evaluated.*,partition.partition_id
          FROM evaluated
          JOIN {partitions} AS partition ON {partition_predicate}
        ),
        prefixes AS MATERIALIZED (
          SELECT assigned.*,
                 sum(weight::numeric) OVER (
                   PARTITION BY row_key ORDER BY row_ordinal
                   ROWS UNBOUNDED PRECEDING
                 ) AS key_prefix
          FROM assigned
        ),
        collapsed AS MATERIALIZED (
          SELECT row_key,min(row_ordinal) AS representative_ordinal,
                 sum(weight::numeric) AS net_weight,min(key_prefix) AS min_prefix
          FROM prefixes GROUP BY row_key
        ),
        representative AS MATERIALIZED (
          SELECT assigned.*
          FROM assigned JOIN collapsed
            ON collapsed.row_key=assigned.row_key
           AND collapsed.representative_ordinal=assigned.row_ordinal
        ),
        existing AS MATERIALIZED (
          SELECT state.entry_id,state.row_key,state.partition_id,state.multiplicity
          FROM {state} AS state JOIN collapsed USING(row_key)
          FOR UPDATE OF state
        ),
        decision AS MATERIALIZED (
          SELECT collapsed.*,representative.row_value,
                 representative.partition_id {representative_orders},
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
        partition_delta AS MATERIALIZED (
          SELECT partition_id,sum(new_multiplicity-old_multiplicity) AS delta
          FROM decision GROUP BY partition_id
        ),
        partition_decision AS MATERIALIZED (
          SELECT partition.partition_id,
                 partition.row_count+partition_delta.delta AS new_count
          FROM {partitions} AS partition
          JOIN partition_delta USING(partition_id)
          FOR UPDATE OF partition
        ),
        status AS MATERIALIZED (
          SELECT CASE
                   WHEN EXISTS(
                     SELECT 1 FROM decision WHERE minimum_multiplicity<0
                   ) THEN 'negative'
                   WHEN EXISTS(
                     SELECT 1 FROM partition_decision
                     WHERE new_count<0 OR new_count>9223372036854775807::numeric
                   ) THEN 'partition_overflow'
                   ELSE 'ok'
                 END AS value
        ),
        removed AS (
          DELETE FROM {state} AS state
          USING decision,status
          WHERE status.value='ok' AND decision.new_multiplicity=0
            AND state.entry_id=decision.entry_id
          RETURNING 1
        ),
        changed AS (
          INSERT INTO {state} AS target(
            row_key,row_value,multiplicity,partition_id{insert_orders}
          )
          SELECT decision.row_key,decision.row_value,decision.new_multiplicity,
                 decision.partition_id{decision_orders}
          FROM decision,status
          WHERE status.value='ok' AND decision.new_multiplicity>0
          ON CONFLICT(row_key) DO UPDATE
          SET row_value=EXCLUDED.row_value,
              multiplicity=EXCLUDED.multiplicity,
              partition_id=EXCLUDED.partition_id{update_orders}
          RETURNING 1
        ),
        partition_changed AS (
          UPDATE {partitions} AS partition
          SET row_count=partition_decision.new_count,
              dirty=true,
              causal_lsn=CASE
                WHEN partition.causal_lsn IS NULL THEN $6::pg_lsn
                ELSE greatest(partition.causal_lsn,$6::pg_lsn)
              END
          FROM partition_decision,status
          WHERE status.value='ok'
            AND partition.partition_id=partition_decision.partition_id
          RETURNING partition.partition_id
        )
        SELECT (SELECT value FROM status),
               count(*)::bigint,min(row_ordinal)::bigint,max(row_ordinal)::bigint,
               coalesce(sum(row_bytes),0)::bigint,
               (SELECT count(*)::bigint FROM removed)
                 +(SELECT count(*)::bigint FROM changed)
                 +(SELECT count(*)::bigint FROM partition_changed),
               (SELECT min(partition_id)::bigint
                FROM {partitions} WHERE dirty)
        FROM bounded
        "#,
        partitions = storage.partitions.sql(),
        state = storage.input.sql(),
    );
    let rows = transaction.write(&query, &arguments)?;
    if rows.len() != 1 {
        return Err("Window admission returned no summary".into());
    }
    let row = rows.first();
    let status: String = window_required(&row, 1, "Window admission status")?;
    if status != "ok" {
        return Err(format!("Window admission returned {status}"));
    }
    let processed = window_nonnegative(
        window_required(&row, 2, "Window admitted rows")?,
        "Window admitted rows",
    )?;
    let first = window_required::<i64>(&row, 3, "Window first admitted row")?;
    let last = window_required::<i64>(&row, 4, "Window last admitted row")?;
    let input_bytes = window_nonnegative(
        window_required(&row, 5, "Window admitted bytes")?,
        "Window admitted bytes",
    )?;
    let state_rows = window_nonnegative(
        window_required(&row, 6, "Window state mutations")?,
        "Window state mutations",
    )?;
    let first_partition_queue_id = window_required(&row, 7, "Window first dirty partition")?;
    if processed == 0
        || first != input.row_ordinal
        || last
            != input
                .row_ordinal
                .checked_add(i64::try_from(processed).map_err(|_| "Window page too large")?)
                .and_then(|value| value.checked_sub(1))
                .ok_or_else(|| "Window input ordinal overflow".to_string())?
    {
        return Err("Window admission returned inconsistent row facts".into());
    }
    let next_row = last
        .checked_add(1)
        .ok_or_else(|| "Window input ordinal exhausted".to_string())?;
    let usage = WorkUsage {
        input_rows: processed,
        input_bytes,
        ..WorkUsage::default()
    };
    let drain_reached = transaction.record_admission(usage)?;
    let target = if next_row < chunk_rows {
        let next = InputPosition::new(input.stream_id, input.chunk_seq, next_row)?;
        if drain_reached {
            WindowAdmissionTarget::Drain {
                first_partition_queue_id,
                after_partitions: AfterPartitions::Admit(next),
            }
        } else {
            WindowAdmissionTarget::Continue(next)
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
        let next = chunk(transaction, &input_state, input_chunk.sequence + 1)?;
        match next {
            Some(next) if next.kind == ChunkKind::Frontier => WindowAdmissionTarget::Drain {
                first_partition_queue_id,
                after_partitions: AfterPartitions::Frontier(InputPosition::new(
                    next.stream_id,
                    next.sequence,
                    0,
                )?),
            },
            _ if drain_reached => WindowAdmissionTarget::Drain {
                first_partition_queue_id,
                after_partitions: AfterPartitions::FinishInput,
            },
            _ => WindowAdmissionTarget::Idle,
        }
    } else {
        return Err("Window admission advanced beyond its input chunk".into());
    };
    let continuation_rows = u64::from(!matches!(target, WindowAdmissionTarget::Idle));
    Ok(WindowAdmission {
        facts: PrimitiveFacts {
            usage,
            state_rows,
            continuation_rows,
            output: OutputFacts::None,
        },
        target,
    })
}

fn window_admission_evaluated_sql(
    storage: &WindowStorage,
    expressions: &WindowExpressions,
) -> String {
    let partition_select = expressions
        .partition_expressions
        .iter()
        .zip(&expressions.partition_columns)
        .map(|(expression, column)| format!("{expression} AS {}", quote_identifier(column)));
    let order_select = expressions
        .order_expressions
        .iter()
        .zip(&expressions.order_columns)
        .map(|(expression, column)| format!("{expression} AS {}", quote_identifier(column)));
    let keys = partition_select
        .chain(order_select)
        .collect::<Vec<_>>()
        .join(",");
    let keys = if keys.is_empty() {
        String::new()
    } else {
        format!(",{keys}")
    };
    let row_key = canonical_row_key_sql("input_row.row_value", &storage.input_type);
    format!(
        r#"
        source AS MATERIALIZED (
          SELECT input_row.row_ordinal,input_row.weight,input_row.row_value,
                 shiba_internal.effect_row_bytes(input_row.row_value) AS row_bytes
          FROM {payload} AS input_row
          WHERE input_row.stream_id=$1 AND input_row.chunk_seq=$2
            AND input_row.row_ordinal >= $3
          ORDER BY input_row.row_ordinal
          LIMIT $4
        ),
        measured AS MATERIALIZED (
          SELECT source.*,row_number() OVER (ORDER BY row_ordinal) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY row_ordinal) AS running_bytes
          FROM source
        ),
        bounded AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_ordinal=1 OR running_bytes <= $5
        ),
        evaluated AS MATERIALIZED (
          SELECT input_row.*,
                 {row_key} AS row_key{keys}
          FROM bounded AS input_row
        )
        "#,
        payload = storage.input_payload.sql(),
        row_key = row_key,
    )
}

fn run_window_enumeration(
    transaction: &mut StepTxn<'_, '_>,
    storage: &WindowStorage,
    expressions: &WindowExpressions,
    partition_queue_id: i64,
    cursor: WindowCursor,
) -> Result<WindowPage, String> {
    let budget = transaction.budget();
    let max_rows = window_i64_budget(budget.max_input_rows, "Window enumeration row budget")?;
    let raw_rows = max_rows
        .checked_add(1)
        .ok_or_else(|| "Window enumeration row budget overflow".to_string())?;
    let max_bytes = window_i64_budget(budget.max_input_bytes, "Window enumeration byte budget")?;
    let entry_order = expressions.order_by.replace("input_row.", "entry_prefix.");
    let logical_order = format!("{entry_order},copy.copy_ordinal");
    let source_order_columns = expressions
        .order_columns
        .iter()
        .map(|column| format!(",entry_prefix.{}", quote_identifier(column)))
        .collect::<String>();
    let query = format!(
        r#"
        WITH partition AS MATERIALIZED (
          SELECT partition_id,row_count
          FROM {partitions}
          WHERE partition_id=$1 AND dirty
        ),
        boundary AS MATERIALIZED (
          SELECT input_row.*,ordered.copy_ordinal,ordered.ordinal
          FROM {ordered} AS ordered
          JOIN {input} AS input_row USING(entry_id)
          WHERE ordered.ordinal=$2
        ),
        entries AS MATERIALIZED (
          SELECT input_row.*,
                 shiba_internal.effect_row_bytes(input_row.row_value) AS row_bytes,
                 CASE
                   WHEN input_row.entry_id=(SELECT entry_id FROM boundary)
                     THEN (SELECT copy_ordinal+1 FROM boundary)
                   ELSE 1
                 END::bigint AS start_copy
          FROM {input} AS input_row
          JOIN partition USING(partition_id)
          WHERE $2 IS NULL
             OR (
               input_row.entry_id=(SELECT entry_id FROM boundary)
               AND (SELECT copy_ordinal FROM boundary)<input_row.multiplicity
             )
             OR EXISTS(
               SELECT 1 FROM boundary WHERE {keyset_after}
             )
          ORDER BY {entry_order}
          LIMIT $5
        ),
        entry_prefix AS MATERIALIZED (
          SELECT entries.*,
                 coalesce(
                   sum((multiplicity::bigint-start_copy+1)::numeric) OVER (
                     ORDER BY {prefix_order}
                     ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                   ),
                   0::numeric
                 ) AS available_before
          FROM entries
        ),
        source AS MATERIALIZED (
          SELECT entry_prefix.entry_id,copy.copy_ordinal,
                 entry_prefix.row_value,entry_prefix.row_bytes
                 {source_order_columns}
          FROM entry_prefix
          CROSS JOIN LATERAL pg_catalog.generate_series(
            entry_prefix.start_copy,
            least(
              entry_prefix.multiplicity,
              entry_prefix.start_copy::numeric
                +greatest($5::numeric-entry_prefix.available_before,0::numeric)-1
            )::bigint
          ) AS copy(copy_ordinal)
          WHERE entry_prefix.available_before<$5::numeric
          ORDER BY {logical_order}
          LIMIT $5
        ),
        measured AS MATERIALIZED (
          SELECT source.*,
                 row_number() OVER (ORDER BY {source_order}) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY {source_order}) AS running_bytes
          FROM source
        ),
        selected AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_ordinal <= $3
            AND (page_ordinal=1 OR running_bytes <= $4)
        ),
        base AS MATERIALIZED (
          SELECT coalesce(max(ordinal),0)::bigint AS last_ordinal
          FROM {ordered}
        ),
        inserted AS (
          INSERT INTO {ordered}(ordinal,entry_id,copy_ordinal,peer_id)
          SELECT base.last_ordinal+selected.page_ordinal,
                 selected.entry_id,selected.copy_ordinal,NULL
          FROM selected CROSS JOIN base
          RETURNING ordinal
        ),
        summary AS MATERIALIZED (
          SELECT count(*)::bigint AS processed,
                 coalesce(sum(row_bytes),0)::bigint AS input_bytes,
                 (SELECT max(ordinal) FROM inserted) AS last_id,
                 (SELECT count(*) FROM source)=(SELECT count(*) FROM selected)
                   AS source_complete
          FROM selected
        )
        SELECT CASE
                 WHEN $2 IS DISTINCT FROM NULL
                      AND $2 IS DISTINCT FROM (SELECT last_ordinal FROM base)
                   THEN 'cursor_mismatch'
                 WHEN NOT EXISTS(SELECT 1 FROM partition) THEN 'missing_partition'
                 ELSE 'ok'
               END,
               summary.processed,summary.input_bytes,summary.last_id,
               summary.source_complete,
               (SELECT count(*)::bigint FROM inserted)
        FROM summary
        "#,
        partitions = storage.partitions.sql(),
        ordered = storage.ordered.sql(),
        input = storage.input.sql(),
        keyset_after = expressions.keyset_after,
        entry_order = expressions.order_by,
        prefix_order = expressions.order_by.replace("input_row.", "entries."),
        logical_order = logical_order,
        source_order = logical_order
            .replace("entry_prefix.", "source.")
            .replace("copy.", "source."),
        source_order_columns = source_order_columns,
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(partition_queue_id, pg_sys::INT8OID),
            DatumWithOid::new(cursor.row_id, pg_sys::INT8OID),
            DatumWithOid::new(max_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_bytes, pg_sys::INT8OID),
            DatumWithOid::new(raw_rows, pg_sys::INT8OID),
        ]
    };
    let rows = transaction.write(&query, &arguments)?;
    if rows.len() != 1 {
        return Err("Window enumeration returned no summary".into());
    }
    let row = rows.first();
    let status: String = window_required(&row, 1, "Window enumeration status")?;
    if status != "ok" {
        return Err(format!("Window enumeration returned {status}"));
    }
    let processed = window_nonnegative(
        window_required(&row, 2, "Window enumerated rows")?,
        "Window enumerated rows",
    )?;
    let bytes = window_nonnegative(
        window_required(&row, 3, "Window enumerated bytes")?,
        "Window enumerated bytes",
    )?;
    let last_row_id = row.get(4).map_err(|error| error.to_string())?;
    let complete = window_required(&row, 5, "Window enumeration completion")?;
    let inserted = window_nonnegative(
        window_required(&row, 6, "Window ordered inserts")?,
        "Window ordered inserts",
    )?;
    if inserted != processed {
        return Err("Window enumeration insert count is inconsistent".into());
    }
    Ok(window_internal_page(
        processed,
        bytes,
        inserted,
        last_row_id,
        complete,
    ))
}

fn window_internal_page(
    input_rows: u64,
    input_bytes: u64,
    state_rows: u64,
    last_row_id: Option<i64>,
    complete: bool,
) -> WindowPage {
    WindowPage {
        facts: PrimitiveFacts {
            usage: WorkUsage {
                input_rows,
                input_bytes,
                ..WorkUsage::default()
            },
            state_rows,
            continuation_rows: 1,
            output: OutputFacts::None,
        },
        last_row_id,
        complete,
    }
}

fn run_window_peers(
    transaction: &mut StepTxn<'_, '_>,
    storage: &WindowStorage,
    expressions: &WindowExpressions,
    partition_queue_id: i64,
    cursor: WindowCursor,
) -> Result<WindowPage, String> {
    let budget = transaction.budget();
    let max_rows = window_i64_budget(budget.max_input_rows, "Window peer row budget")?;
    let raw_rows = max_rows
        .checked_add(1)
        .ok_or_else(|| "Window peer row budget overflow".to_string())?;
    let max_bytes = window_i64_budget(budget.max_input_bytes, "Window peer byte budget")?;
    let peer_keys = expressions
        .order_columns
        .iter()
        .map(|column| {
            let column = quote_identifier(column);
            format!(",input_row.{column}")
        })
        .collect::<String>();
    let query = format!(
        r#"
        WITH source AS MATERIALIZED (
          SELECT ordered.ordinal,ordered.entry_id,input_row.row_value{peer_keys},
                 shiba_internal.effect_row_bytes(input_row.row_value) AS row_bytes
          FROM {ordered} AS ordered
          JOIN {input} AS input_row USING(entry_id)
          WHERE ($2 IS NULL OR ordered.ordinal>$2)
          ORDER BY ordered.ordinal
          LIMIT $5
        ),
        measured AS MATERIALIZED (
          SELECT source.*,row_number() OVER (ORDER BY ordinal) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY ordinal) AS running_bytes
          FROM source
        ),
        selected AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_ordinal <= $3
            AND (page_ordinal=1 OR running_bytes <= $4)
        ),
        marked AS MATERIALIZED (
          SELECT next_row.*,
                 CASE
                   WHEN next_row.ordinal=1 THEN 1
                   WHEN ({peer_equal}) THEN 0
                   ELSE 1
                 END AS starts_peer
          FROM selected AS next_row
          LEFT JOIN {ordered} AS previous_ordered
            ON previous_ordered.ordinal=next_row.ordinal-1
          LEFT JOIN {input} AS boundary_row
            ON boundary_row.entry_id=previous_ordered.entry_id
        ),
        base AS MATERIALIZED (
          SELECT coalesce(
                   (SELECT peer_id FROM {ordered} WHERE ordinal=$2),
                   0
                 )::bigint AS peer_id
        ),
        assigned AS MATERIALIZED (
          SELECT marked.*,
                 base.peer_id+sum(starts_peer) OVER (ORDER BY ordinal) AS peer_id
          FROM marked CROSS JOIN base
        ),
        updated AS (
          UPDATE {ordered} AS ordered
          SET peer_id=assigned.peer_id
          FROM assigned
          WHERE ordered.ordinal=assigned.ordinal
          RETURNING 1
        ),
        peer_ranges AS MATERIALIZED (
          SELECT peer_id,min(ordinal) AS first_ordinal,max(ordinal) AS last_ordinal
          FROM assigned GROUP BY peer_id
        ),
        peer_changed AS (
          INSERT INTO {peers} AS target(peer_id,first_ordinal,last_ordinal)
          SELECT peer_id,first_ordinal,last_ordinal FROM peer_ranges
          ON CONFLICT(peer_id) DO UPDATE
          SET first_ordinal=least(
                target.first_ordinal,EXCLUDED.first_ordinal
              ),
              last_ordinal=greatest(
                target.last_ordinal,EXCLUDED.last_ordinal
              )
          RETURNING 1
        )
        SELECT CASE
                 WHEN NOT EXISTS(
                   SELECT 1 FROM {partitions}
                   WHERE partition_id=$1 AND dirty
                 ) THEN 'missing_partition'
                 WHEN $2 IS NOT NULL AND NOT EXISTS(
                   SELECT 1 FROM {ordered}
                   WHERE ordinal=$2 AND peer_id IS NOT NULL
                 ) THEN 'cursor_mismatch'
                 ELSE 'ok'
               END,
               count(*)::bigint,coalesce(sum(row_bytes),0)::bigint,
               (array_agg(ordinal ORDER BY page_ordinal DESC))[1],
               (SELECT count(*) FROM source)=(SELECT count(*) FROM selected),
               (SELECT count(*)::bigint FROM updated)
                 +(SELECT count(*)::bigint FROM peer_changed)
        FROM selected
        "#,
        ordered = storage.ordered.sql(),
        input = storage.input.sql(),
        peers = storage.peers.sql(),
        partitions = storage.partitions.sql(),
        peer_equal = expressions.peer_equal,
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(partition_queue_id, pg_sys::INT8OID),
            DatumWithOid::new(cursor.row_id, pg_sys::INT8OID),
            DatumWithOid::new(max_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_bytes, pg_sys::INT8OID),
            DatumWithOid::new(raw_rows, pg_sys::INT8OID),
        ]
    };
    let rows = transaction.write(&query, &arguments)?;
    if rows.len() != 1 {
        return Err("Window peer build returned no summary".into());
    }
    let row = rows.first();
    let status: String = window_required(&row, 1, "Window peer status")?;
    if status != "ok" {
        return Err(format!("Window peer build returned {status}"));
    }
    let processed = window_nonnegative(
        window_required(&row, 2, "Window peer rows")?,
        "Window peer rows",
    )?;
    let bytes = window_nonnegative(
        window_required(&row, 3, "Window peer bytes")?,
        "Window peer bytes",
    )?;
    let last_row_id = row.get(4).map_err(|error| error.to_string())?;
    let complete = window_required(&row, 5, "Window peer completion")?;
    let state_rows = window_nonnegative(
        window_required(&row, 6, "Window peer mutations")?,
        "Window peer mutations",
    )?;
    Ok(window_internal_page(
        processed,
        bytes,
        state_rows,
        last_row_id,
        complete,
    ))
}

fn run_window_frames(
    transaction: &mut StepTxn<'_, '_>,
    storage: &WindowStorage,
    expressions: &WindowExpressions,
    spec: &WindowSpec,
    partition_queue_id: i64,
    cursor: WindowCursor,
) -> Result<WindowPage, String> {
    let budget = transaction.budget();
    let max_rows = window_i64_budget(budget.max_input_rows, "Window frame row budget")?;
    let raw_rows = max_rows
        .checked_add(1)
        .ok_or_else(|| "Window frame row budget overflow".to_string())?;
    let max_bytes = window_i64_budget(budget.max_input_bytes, "Window frame byte budget")?;
    let (base_start, base_end, offset_valid) =
        window_frame_base_expressions(storage, expressions, spec)?;
    let intervals = window_frame_intervals(spec);
    let query = format!(
        r#"
        WITH partition AS MATERIALIZED (
          SELECT partition_id,row_count::bigint AS partition_rows
          FROM {partitions} WHERE partition_id=$1 AND dirty
        ),
        source AS MATERIALIZED (
          SELECT ordered.ordinal,ordered.peer_id,input_row.row_value,
                 shiba_internal.effect_row_bytes(input_row.row_value) AS row_bytes
          FROM {ordered} AS ordered
          JOIN {input} AS input_row USING(entry_id)
          WHERE $2 IS NULL OR ordered.ordinal>$2
          ORDER BY ordered.ordinal
          LIMIT $5
        ),
        measured AS MATERIALIZED (
          SELECT source.*,row_number() OVER (ORDER BY ordinal) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY ordinal) AS running_bytes
          FROM source
        ),
        selected AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_ordinal <= $3
            AND (page_ordinal=1 OR running_bytes <= $4)
        ),
        based AS MATERIALIZED (
          SELECT current_input.*,peer.first_ordinal,peer.last_ordinal,
                 partition.partition_rows,
                 ({base_start})::bigint AS base_start,
                 ({base_end})::bigint AS base_end,
                 ({offset_valid}) AS offset_valid
          FROM selected AS current_input
          CROSS JOIN partition
          JOIN {peers} AS peer USING(peer_id)
        ),
        split AS MATERIALIZED (
          SELECT based.*,{intervals}
          FROM based
        ),
        normalized AS MATERIALIZED (
          SELECT split.*,
                 CASE WHEN raw_start_1<=raw_end_1 THEN raw_start_1 END AS start_1,
                 CASE WHEN raw_start_1<=raw_end_1 THEN raw_end_1 END AS end_1,
                 CASE WHEN raw_start_2<=raw_end_2 THEN raw_start_2 END AS start_2,
                 CASE WHEN raw_start_2<=raw_end_2 THEN raw_end_2 END AS end_2,
                 CASE WHEN raw_start_3<=raw_end_3 THEN raw_start_3 END AS start_3,
                 CASE WHEN raw_start_3<=raw_end_3 THEN raw_end_3 END AS end_3
          FROM split
        ),
        status AS MATERIALIZED (
          SELECT CASE
                   WHEN NOT EXISTS(SELECT 1 FROM partition) THEN 'missing_partition'
                   WHEN EXISTS(SELECT 1 FROM based WHERE NOT offset_valid)
                     THEN 'invalid_offset'
                   ELSE 'ok'
                 END AS value
        ),
        inserted AS (
          INSERT INTO {frames}(
            ordinal,start_1,end_1,start_2,end_2,start_3,end_3,frame_count
          )
          SELECT ordinal,start_1,end_1,start_2,end_2,start_3,end_3,
                 coalesce(end_1-start_1+1,0)
                   +coalesce(end_2-start_2+1,0)
                   +coalesce(end_3-start_3+1,0)
          FROM normalized,status WHERE status.value='ok'
          RETURNING ordinal
        )
        SELECT (SELECT value FROM status),
               count(*)::bigint,coalesce(sum(row_bytes),0)::bigint,
               (array_agg(ordinal ORDER BY page_ordinal DESC))[1],
               (SELECT count(*) FROM source)=(SELECT count(*) FROM selected),
               (SELECT count(*)::bigint FROM inserted)
        FROM selected
        "#,
        partitions = storage.partitions.sql(),
        ordered = storage.ordered.sql(),
        input = storage.input.sql(),
        peers = storage.peers.sql(),
        frames = storage.frames.sql(),
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(partition_queue_id, pg_sys::INT8OID),
            DatumWithOid::new(cursor.row_id, pg_sys::INT8OID),
            DatumWithOid::new(max_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_bytes, pg_sys::INT8OID),
            DatumWithOid::new(raw_rows, pg_sys::INT8OID),
        ]
    };
    let rows = transaction.write(&query, &arguments)?;
    if rows.len() != 1 {
        return Err("Window frame build returned no summary".into());
    }
    let row = rows.first();
    let status: String = window_required(&row, 1, "Window frame status")?;
    if status != "ok" {
        return Err(format!("Window frame build returned {status}"));
    }
    let processed = window_nonnegative(
        window_required(&row, 2, "Window frame rows")?,
        "Window frame rows",
    )?;
    let bytes = window_nonnegative(
        window_required(&row, 3, "Window frame bytes")?,
        "Window frame bytes",
    )?;
    let last_row_id = row.get(4).map_err(|error| error.to_string())?;
    let complete = window_required(&row, 5, "Window frame completion")?;
    let inserted = window_nonnegative(
        window_required(&row, 6, "Window frame inserts")?,
        "Window frame inserts",
    )?;
    if inserted != processed {
        return Err("Window frame insert count is inconsistent".into());
    }
    Ok(window_internal_page(
        processed,
        bytes,
        inserted,
        last_row_id,
        complete,
    ))
}

fn window_frame_base_expressions(
    storage: &WindowStorage,
    expressions: &WindowExpressions,
    spec: &WindowSpec,
) -> Result<(String, String, String), String> {
    let options = spec.frame.options;
    let start_offset = expressions.frame_start_offset.as_deref().unwrap_or("NULL");
    let end_offset = expressions.frame_end_offset.as_deref().unwrap_or("NULL");
    let mode_rows = options & pg_sys::FRAMEOPTION_ROWS != 0;
    let mode_groups = options & pg_sys::FRAMEOPTION_GROUPS != 0;
    let start = if options & pg_sys::FRAMEOPTION_START_UNBOUNDED_PRECEDING != 0 {
        "1".into()
    } else if options & pg_sys::FRAMEOPTION_START_CURRENT_ROW != 0 {
        if mode_rows {
            "current_input.ordinal".into()
        } else {
            "peer.first_ordinal".into()
        }
    } else if options & pg_sys::FRAMEOPTION_START_OFFSET_PRECEDING != 0 {
        if mode_rows {
            format!("greatest(1::numeric,current_input.ordinal::numeric-({start_offset})::numeric)")
        } else if mode_groups {
            format!(
                "coalesce((SELECT first_ordinal FROM {peers} \
                 WHERE peer_id=greatest(1::numeric,current_input.peer_id::numeric-({start_offset})::numeric)::bigint), \
                 partition.partition_rows+1)",
                peers = storage.peers.sql()
            )
        } else {
            return Err("Window RANGE offset escaped capability validation".into());
        }
    } else if mode_rows {
        format!(
            "least(partition.partition_rows::numeric+1,current_input.ordinal::numeric+({start_offset})::numeric)"
        )
    } else if mode_groups {
        format!(
            "coalesce((SELECT first_ordinal FROM {peers} \
             WHERE peer_id::numeric=current_input.peer_id::numeric+({start_offset})::numeric), \
             partition.partition_rows+1)",
            peers = storage.peers.sql()
        )
    } else {
        return Err("Window RANGE offset escaped capability validation".into());
    };
    let end = if options & pg_sys::FRAMEOPTION_END_UNBOUNDED_FOLLOWING != 0 {
        "partition.partition_rows".into()
    } else if options & pg_sys::FRAMEOPTION_END_CURRENT_ROW != 0 {
        if mode_rows {
            "current_input.ordinal".into()
        } else {
            "peer.last_ordinal".into()
        }
    } else if options & pg_sys::FRAMEOPTION_END_OFFSET_PRECEDING != 0 {
        if mode_rows {
            format!("greatest(0::numeric,current_input.ordinal::numeric-({end_offset})::numeric)")
        } else if mode_groups {
            format!(
                "coalesce((SELECT last_ordinal FROM {peers} \
                 WHERE peer_id=current_input.peer_id-({end_offset})::bigint),0)",
                peers = storage.peers.sql()
            )
        } else {
            return Err("Window RANGE offset escaped capability validation".into());
        }
    } else if mode_rows {
        format!(
            "least(partition.partition_rows::numeric,current_input.ordinal::numeric+({end_offset})::numeric)"
        )
    } else if mode_groups {
        format!(
            "coalesce((SELECT last_ordinal FROM {peers} \
             WHERE peer_id::numeric=current_input.peer_id::numeric+({end_offset})::numeric), \
             partition.partition_rows)",
            peers = storage.peers.sql()
        )
    } else {
        return Err("Window RANGE offset escaped capability validation".into());
    };
    let mut valid = Vec::new();
    if spec.frame.start_offset.is_some() {
        valid.push(format!(
            "({start_offset}) IS NOT NULL AND ({start_offset})::numeric>=0 \
             AND ({start_offset})::numeric=pg_catalog.trunc(({start_offset})::numeric)"
        ));
    }
    if spec.frame.end_offset.is_some() {
        valid.push(format!(
            "({end_offset}) IS NOT NULL AND ({end_offset})::numeric>=0 \
             AND ({end_offset})::numeric=pg_catalog.trunc(({end_offset})::numeric)"
        ));
    }
    Ok((
        start,
        end,
        if valid.is_empty() {
            "true".into()
        } else {
            valid.join(" AND ")
        },
    ))
}

fn window_frame_intervals(spec: &WindowSpec) -> String {
    let options = spec.frame.options;
    let pairs = if options & pg_sys::FRAMEOPTION_EXCLUDE_CURRENT_ROW != 0 {
        vec![
            ("based.base_start", "least(based.base_end,based.ordinal-1)"),
            (
                "greatest(based.base_start,based.ordinal+1)",
                "based.base_end",
            ),
        ]
    } else if options & pg_sys::FRAMEOPTION_EXCLUDE_GROUP != 0 {
        vec![
            (
                "based.base_start",
                "least(based.base_end,based.first_ordinal-1)",
            ),
            (
                "greatest(based.base_start,based.last_ordinal+1)",
                "based.base_end",
            ),
        ]
    } else if options & pg_sys::FRAMEOPTION_EXCLUDE_TIES != 0 {
        vec![
            (
                "based.base_start",
                "least(based.base_end,based.first_ordinal-1)",
            ),
            (
                "greatest(based.base_start,based.ordinal)",
                "least(based.base_end,based.ordinal)",
            ),
            (
                "greatest(based.base_start,based.last_ordinal+1)",
                "based.base_end",
            ),
        ]
    } else {
        vec![("based.base_start", "based.base_end")]
    };
    (0..3)
        .flat_map(|index| {
            let (start, end) = pairs.get(index).copied().unwrap_or(("NULL", "NULL"));
            [
                format!("{start}::bigint AS raw_start_{}", index + 1),
                format!("{end}::bigint AS raw_end_{}", index + 1),
            ]
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WindowFoldPrimitive {
    processed_rows: u64,
    processed_bytes: u64,
    last_frame_ordinal: Option<i64>,
    complete: bool,
    state_rows: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WindowFinalizePrimitive {
    applied: bool,
    work_bytes: u64,
    state_rows: u64,
}

fn validate_window_fold_status(status: &str) -> Result<(), String> {
    if status == "ok" {
        Ok(())
    } else {
        Err(format!("Window aggregate fold returned {status}"))
    }
}

fn validate_window_finalize_decision(
    applied: bool,
    work_bytes: u64,
    remaining_rows: usize,
    remaining_bytes: usize,
    allow_oversized_item: bool,
) -> Result<(), String> {
    let remaining_bytes = u64::try_from(remaining_bytes)
        .map_err(|_| "Window finalize remaining byte budget exceeds u64")?;
    if applied {
        if remaining_rows == 0 {
            return Err("Window aggregate finalization exceeded its remaining rows".into());
        }
        if work_bytes > remaining_bytes && !allow_oversized_item {
            return Err("Window aggregate finalization exceeded its remaining bytes".into());
        }
    } else if remaining_rows > 0 && (work_bytes <= remaining_bytes || allow_oversized_item) {
        return Err("Window aggregate finalization blocked despite available budget".into());
    }
    Ok(())
}

fn run_window_aggregate_fold(
    transaction: &mut StepTxn<'_, '_>,
    storage: &WindowStorage,
    expressions: &WindowExpressions,
    partition_queue_id: i64,
    function_ordinal: u32,
    cursor: WindowFoldCursor,
) -> Result<WindowFoldPage, String> {
    let index = usize::try_from(function_ordinal - 1)
        .map_err(|_| "Window function ordinal exceeds usize")?;
    let function = expressions
        .functions
        .get(index)
        .ok_or_else(|| "Window aggregate fold is outside its plan".to_string())?;
    let WindowFunctionCapability::Aggregate(capability) = &function.capability else {
        return Err("native Window function entered aggregate fold".into());
    };
    let accumulator = storage
        .accumulators
        .get(index)
        .and_then(Option::as_ref)
        .ok_or_else(|| "Window aggregate has no accumulator relation".to_string())?;
    let partition_arguments = unsafe { [DatumWithOid::new(partition_queue_id, pg_sys::INT8OID)] };
    let partition_rows = transaction.read(
        &format!(
            "SELECT row_count::bigint FROM {} \
             WHERE partition_id=$1 AND dirty",
            storage.partitions.sql()
        ),
        &partition_arguments,
    )?;
    if partition_rows.len() != 1 {
        return Err("Window aggregate fold has no unique dirty partition".into());
    }
    let partition_rows: i64 = window_required(&partition_rows.first(), 1, "Window partition rows")?;
    if partition_rows < 0 || cursor.output_ordinal > i64::max(partition_rows, 1) {
        return Err("Window aggregate output ordinal is outside its partition".into());
    }
    if partition_rows == 0 {
        if cursor.output_ordinal != 1
            || cursor.last_frame_ordinal.is_some()
            || cursor.ready_to_finalize
        {
            return Err("empty Window partition has an aggregate fold cursor".into());
        }
        let rows = transaction.read(
            &format!("SELECT count(*)::bigint FROM {}", accumulator.sql()),
            &[],
        )?;
        if window_required::<i64>(&rows.first(), 1, "Window accumulator rows")? != 0 {
            return Err("empty Window partition retained aggregate state".into());
        }
        return Ok(WindowFoldPage {
            facts: PrimitiveFacts {
                continuation_rows: 1,
                ..PrimitiveFacts::default()
            },
            next_cursor: None,
            work_items: 1,
        });
    }

    let budget = transaction.budget();
    let max_rows =
        u64::try_from(budget.max_input_rows).map_err(|_| "Window fold row budget exceeds u64")?;
    let max_bytes =
        u64::try_from(budget.max_input_bytes).map_err(|_| "Window fold byte budget exceeds u64")?;
    let mut state_rows = 0_u64;
    let mut processed_rows = 0_u64;
    let mut processed_bytes = 0_u64;
    let mut work_items = 0_usize;
    let mut next_cursor = Some(cursor);

    while work_items < WINDOW_FOLD_WORK_ITEM_CAP
        && processed_rows < max_rows
        && processed_bytes < max_bytes
    {
        let current = next_cursor
            .ok_or_else(|| "completed Window aggregate retained a fold cursor".to_string())?;
        work_items += 1;
        let state_arguments = unsafe {
            [
                DatumWithOid::new(partition_queue_id, pg_sys::INT8OID),
                DatumWithOid::new(current.output_ordinal, pg_sys::INT8OID),
            ]
        };
        let initialized = if current.last_frame_ordinal.is_none() && !current.ready_to_finalize {
            let initial = initial_state_sql(capability);
            let no_trans_value =
                capability.transition_is_strict && capability.initial_literal.is_none();
            let inserted = transaction.write(
                &format!(
                    "INSERT INTO {}(
                       singleton,partition_id,output_ordinal,state_value,no_trans_value
                     ) VALUES(true,$1,$2,{initial},$3)
                     RETURNING singleton",
                    accumulator.sql()
                ),
                &unsafe {
                    [
                        DatumWithOid::new(partition_queue_id, pg_sys::INT8OID),
                        DatumWithOid::new(current.output_ordinal, pg_sys::INT8OID),
                        DatumWithOid::new(no_trans_value, pg_sys::BOOLOID),
                    ]
                },
            )?;
            if inserted.len() != 1 {
                return Err("Window aggregate fold did not initialize one accumulator".into());
            }
            state_rows = state_rows
                .checked_add(1)
                .ok_or_else(|| "Window aggregate fold state count overflow".to_string())?;
            true
        } else {
            let rows = transaction.read(
                &format!(
                    "SELECT count(*)::bigint FROM {} \
                     WHERE singleton AND partition_id=$1 AND output_ordinal=$2",
                    accumulator.sql()
                ),
                &state_arguments,
            )?;
            if window_required::<i64>(&rows.first(), 1, "Window accumulator rows")? != 1 {
                return Err("Window aggregate fold lost its accumulator".into());
            }
            false
        };

        let ready = if current.ready_to_finalize {
            current
        } else {
            let remaining_rows = usize::try_from(max_rows - processed_rows)
                .map_err(|_| "Window fold remaining row budget exceeds usize")?;
            let remaining_bytes = usize::try_from(max_bytes - processed_bytes)
                .map_err(|_| "Window fold remaining byte budget exceeds usize")?;
            let allow_oversized_row = processed_rows == 0;
            let folded = window_fold_page(
                transaction,
                storage,
                accumulator,
                function,
                capability,
                partition_queue_id,
                current.output_ordinal,
                current.last_frame_ordinal,
                remaining_rows,
                remaining_bytes,
                allow_oversized_row,
            )?;
            let remaining_rows = u64::try_from(remaining_rows)
                .map_err(|_| "Window fold remaining row budget exceeds u64")?;
            let remaining_bytes = u64::try_from(remaining_bytes)
                .map_err(|_| "Window fold remaining byte budget exceeds u64")?;
            if folded.processed_rows > remaining_rows
                || (folded.processed_bytes > remaining_bytes
                    && !(allow_oversized_row && folded.processed_rows == 1))
            {
                return Err("Window aggregate fold exceeded its remaining step budget".into());
            }
            processed_rows = processed_rows
                .checked_add(folded.processed_rows)
                .ok_or_else(|| "Window aggregate fold row count overflow".to_string())?;
            processed_bytes = processed_bytes
                .checked_add(folded.processed_bytes)
                .ok_or_else(|| "Window aggregate fold byte count overflow".to_string())?;
            state_rows = state_rows
                .checked_add(folded.state_rows)
                .ok_or_else(|| "Window aggregate fold state count overflow".to_string())?;

            if !folded.complete {
                if folded.processed_rows == 0 {
                    if !initialized || current.last_frame_ordinal.is_some() {
                        return Err("resumed Window aggregate fold made no progress".into());
                    }
                    let deleted = transaction.write(
                        &format!(
                            "DELETE FROM {} \
                             WHERE singleton AND partition_id=$1 AND output_ordinal=$2 \
                             RETURNING singleton",
                            accumulator.sql()
                        ),
                        &state_arguments,
                    )?;
                    if deleted.len() != 1 {
                        return Err(
                            "Window aggregate fold could not release an unstarted accumulator"
                                .into(),
                        );
                    }
                    state_rows = state_rows
                        .checked_add(1)
                        .ok_or_else(|| "Window aggregate fold state count overflow".to_string())?;
                    next_cursor = Some(current);
                } else {
                    next_cursor = Some(WindowFoldCursor {
                        output_ordinal: current.output_ordinal,
                        last_frame_ordinal: folded.last_frame_ordinal,
                        ready_to_finalize: false,
                    });
                }
                break;
            }
            WindowFoldCursor {
                output_ordinal: current.output_ordinal,
                last_frame_ordinal: folded.last_frame_ordinal.or(current.last_frame_ordinal),
                ready_to_finalize: true,
            }
        };
        next_cursor = Some(ready);

        if processed_rows == max_rows || processed_bytes >= max_bytes {
            break;
        }
        let remaining_rows = usize::try_from(max_rows - processed_rows)
            .map_err(|_| "Window finalize remaining row budget exceeds usize")?;
        let remaining_bytes = usize::try_from(max_bytes - processed_bytes)
            .map_err(|_| "Window finalize remaining byte budget exceeds usize")?;
        let finalized = window_finalize_fold(
            transaction,
            storage,
            expressions,
            accumulator,
            function,
            capability,
            partition_queue_id,
            current.output_ordinal,
            function_ordinal,
            remaining_rows,
            remaining_bytes,
            processed_rows == 0,
        )?;
        if !finalized.applied {
            break;
        }
        processed_rows = processed_rows
            .checked_add(1)
            .ok_or_else(|| "Window aggregate finalization row count overflow".to_string())?;
        processed_bytes = processed_bytes
            .checked_add(finalized.work_bytes)
            .ok_or_else(|| "Window aggregate finalization byte count overflow".to_string())?;
        state_rows = state_rows
            .checked_add(finalized.state_rows)
            .ok_or_else(|| "Window aggregate finalization state count overflow".to_string())?;
        next_cursor = if current.output_ordinal == partition_rows {
            None
        } else {
            Some(WindowFoldCursor {
                output_ordinal: current
                    .output_ordinal
                    .checked_add(1)
                    .ok_or_else(|| "Window output ordinal overflow".to_string())?,
                last_frame_ordinal: None,
                ready_to_finalize: false,
            })
        };
        if next_cursor.is_none() {
            break;
        }
    }

    Ok(WindowFoldPage {
        facts: PrimitiveFacts {
            usage: WorkUsage {
                input_rows: processed_rows,
                input_bytes: processed_bytes,
                ..WorkUsage::default()
            },
            state_rows,
            continuation_rows: 1,
            output: OutputFacts::None,
        },
        next_cursor,
        work_items,
    })
}

#[allow(clippy::too_many_arguments)]
fn window_fold_page(
    transaction: &mut StepTxn<'_, '_>,
    storage: &WindowStorage,
    accumulator: &RelationRef,
    function: &WindowFunctionPlan,
    capability: &AggregateCapability,
    partition_queue_id: i64,
    output_ordinal: i64,
    last_frame_ordinal: Option<i64>,
    max_rows: usize,
    max_bytes: usize,
    allow_oversized_row: bool,
) -> Result<WindowFoldPrimitive, String> {
    let arguments_nonnull = if function.current_arguments.is_empty() {
        "true".into()
    } else {
        function
            .current_arguments
            .iter()
            .map(|argument| format!("({argument}) IS NOT NULL"))
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    let transition_call = format!(
        "{}(fold.state_value{})",
        capability.transition_function,
        if function.current_arguments.is_empty() {
            String::new()
        } else {
            format!(",{}", function.current_arguments.join(","))
        }
    );
    let (next_state, next_no_trans) = if capability.transition_is_strict {
        let advance = if capability.initial_literal.is_none() {
            let first = function.current_arguments.first().ok_or_else(|| {
                "strict aggregate with NULL initial state has no argument".to_string()
            })?;
            format!(
                "CASE WHEN fold.no_trans_value \
                      THEN ({first})::{} \
                      WHEN fold.state_value IS NULL \
                      THEN fold.state_value \
                      ELSE {transition_call} END",
                capability.transition_type
            )
        } else {
            format!(
                "CASE WHEN fold.state_value IS NULL \
                      THEN fold.state_value ELSE {transition_call} END"
            )
        };
        (
            format!(
                "CASE WHEN ({filter}) IS TRUE \
                      THEN CASE WHEN {arguments_nonnull} \
                                THEN {advance} ELSE fold.state_value END \
                      ELSE fold.state_value END",
                filter = function.filter,
            ),
            format!(
                "CASE WHEN ({filter}) IS TRUE AND ({arguments_nonnull}) \
                      THEN false ELSE fold.no_trans_value END",
                filter = function.filter,
            ),
        )
    } else {
        (
            format!(
                "CASE WHEN ({}) IS TRUE THEN {transition_call} \
                      ELSE fold.state_value END",
                function.filter
            ),
            "false".into(),
        )
    };
    let max_rows = window_i64_budget(max_rows, "Window fold row budget")?;
    let raw_rows = max_rows
        .checked_add(1)
        .ok_or_else(|| "Window fold row budget overflow".to_string())?;
    let max_bytes = window_i64_budget(max_bytes, "Window fold byte budget")?;
    let query = format!(
        r#"
        WITH RECURSIVE frame AS MATERIALIZED (
          SELECT start_1,end_1,start_2,end_2,start_3,end_3
          FROM {frames}
          WHERE ordinal=$2
        ),
        interval_1 AS MATERIALIZED (
          SELECT interval_row.ordinal,interval_row.entry_id,
                 shiba_internal.effect_row_bytes(current_input.row_value)
                   AS row_bytes
          FROM frame
          CROSS JOIN LATERAL (
            SELECT ordered.ordinal,ordered.entry_id
            FROM {ordered} AS ordered
            WHERE frame.start_1 IS NOT NULL
              AND ordered.ordinal BETWEEN frame.start_1 AND frame.end_1
              AND ($3 IS NULL OR ordered.ordinal>$3)
            ORDER BY ordered.ordinal
            LIMIT $6
          ) AS interval_row
          JOIN {input} AS current_input
            ON current_input.entry_id=interval_row.entry_id
        ),
        interval_2 AS MATERIALIZED (
          SELECT interval_row.ordinal,interval_row.entry_id,
                 shiba_internal.effect_row_bytes(current_input.row_value)
                   AS row_bytes
          FROM frame
          CROSS JOIN LATERAL (
            SELECT ordered.ordinal,ordered.entry_id
            FROM {ordered} AS ordered
            WHERE frame.start_2 IS NOT NULL
              AND ordered.ordinal BETWEEN frame.start_2 AND frame.end_2
              AND ($3 IS NULL OR ordered.ordinal>$3)
            ORDER BY ordered.ordinal
            LIMIT $6
          ) AS interval_row
          JOIN {input} AS current_input
            ON current_input.entry_id=interval_row.entry_id
        ),
        interval_3 AS MATERIALIZED (
          SELECT interval_row.ordinal,interval_row.entry_id,
                 shiba_internal.effect_row_bytes(current_input.row_value)
                   AS row_bytes
          FROM frame
          CROSS JOIN LATERAL (
            SELECT ordered.ordinal,ordered.entry_id
            FROM {ordered} AS ordered
            WHERE frame.start_3 IS NOT NULL
              AND ordered.ordinal BETWEEN frame.start_3 AND frame.end_3
              AND ($3 IS NULL OR ordered.ordinal>$3)
            ORDER BY ordered.ordinal
            LIMIT $6
          ) AS interval_row
          JOIN {input} AS current_input
            ON current_input.entry_id=interval_row.entry_id
        ),
        source AS MATERIALIZED (
          SELECT intervals.*
          FROM (
            SELECT * FROM interval_1
            UNION ALL
            SELECT * FROM interval_2
            UNION ALL
            SELECT * FROM interval_3
          ) AS intervals
          ORDER BY ordinal
          LIMIT $6
        ),
        measured AS MATERIALIZED (
          SELECT source.*,
                 row_number() OVER (ORDER BY ordinal) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY ordinal) AS running_bytes
          FROM source
        ),
        selected AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_ordinal<=$4
            AND (($7::boolean AND page_ordinal=1) OR running_bytes<=$5)
        ),
        fold(step,state_value,no_trans_value,last_frame_ordinal) AS (
          SELECT 0::bigint,accumulator.state_value,
                 accumulator.no_trans_value,NULL::bigint
          FROM {accumulator} AS accumulator
          WHERE accumulator.singleton
            AND accumulator.partition_id=$1
            AND accumulator.output_ordinal=$2
          UNION ALL
          SELECT selected.page_ordinal,{next_state},{next_no_trans},
                 selected.ordinal
          FROM fold
          JOIN selected ON selected.page_ordinal=fold.step+1
          JOIN {input} AS current_input
            ON current_input.entry_id=selected.entry_id
        ),
        final_fold AS MATERIALIZED (
          SELECT * FROM fold ORDER BY step DESC LIMIT 1
        ),
        updated AS (
          UPDATE {accumulator} AS accumulator
          SET state_value=final_fold.state_value,
              no_trans_value=final_fold.no_trans_value
          FROM final_fold
          WHERE accumulator.singleton
            AND accumulator.partition_id=$1
            AND accumulator.output_ordinal=$2
            AND final_fold.step>0
          RETURNING 1
        )
        SELECT CASE
                 WHEN (SELECT count(*) FROM frame)<>1
                   THEN 'missing_frame'
                 WHEN (
                   SELECT count(*) FROM {accumulator}
                   WHERE singleton AND partition_id=$1 AND output_ordinal=$2
                 )<>1 THEN 'missing_accumulator'
                 ELSE 'ok'
               END,
               count(*)::bigint,coalesce(sum(row_bytes),0)::bigint,
               (array_agg(ordinal ORDER BY page_ordinal DESC))[1],
               (SELECT count(*) FROM source)=(SELECT count(*) FROM selected),
               (SELECT count(*)::bigint FROM updated)
        FROM selected
        "#,
        accumulator = accumulator.sql(),
        frames = storage.frames.sql(),
        ordered = storage.ordered.sql(),
        input = storage.input.sql(),
    );
    let rows = transaction.write(&query, &unsafe {
        [
            DatumWithOid::new(partition_queue_id, pg_sys::INT8OID),
            DatumWithOid::new(output_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(last_frame_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(max_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_bytes, pg_sys::INT8OID),
            DatumWithOid::new(raw_rows, pg_sys::INT8OID),
            DatumWithOid::new(allow_oversized_row, pg_sys::BOOLOID),
        ]
    })?;
    if rows.len() != 1 {
        return Err("Window aggregate fold returned no page summary".into());
    }
    let row = rows.first();
    let status: String = window_required(&row, 1, "Window aggregate fold status")?;
    validate_window_fold_status(&status)?;
    let processed_rows = window_nonnegative(
        window_required(&row, 2, "Window aggregate fold rows")?,
        "Window aggregate fold rows",
    )?;
    let processed_bytes = window_nonnegative(
        window_required(&row, 3, "Window aggregate fold bytes")?,
        "Window aggregate fold bytes",
    )?;
    let last_frame_ordinal = row.get(4).map_err(|error| error.to_string())?;
    let complete = window_required(&row, 5, "Window aggregate fold completion")?;
    let state_rows = window_nonnegative(
        window_required(&row, 6, "Window aggregate fold mutations")?,
        "Window aggregate fold mutations",
    )?;
    if state_rows != u64::from(processed_rows > 0) {
        return Err("Window aggregate fold mutation count is inconsistent".into());
    }
    Ok(WindowFoldPrimitive {
        processed_rows,
        processed_bytes,
        last_frame_ordinal,
        complete,
        state_rows,
    })
}

#[allow(clippy::too_many_arguments)]
fn window_finalize_fold(
    transaction: &mut StepTxn<'_, '_>,
    storage: &WindowStorage,
    expressions: &WindowExpressions,
    accumulator: &RelationRef,
    function: &WindowFunctionPlan,
    capability: &AggregateCapability,
    partition_queue_id: i64,
    output_ordinal: i64,
    function_ordinal: u32,
    remaining_rows: usize,
    remaining_bytes: usize,
    allow_oversized_item: bool,
) -> Result<WindowFinalizePrimitive, String> {
    let state = "accumulator.state_value";
    let value = capability.final_function.as_ref().map_or_else(
        || state.into(),
        |final_function| format!("{final_function}({state})"),
    );
    let function_column = quote_identifier(&format!("function_{function_ordinal}"));
    let function_bytes = scalar_work_bytes_sql("finalized.function_value");
    let is_last_function = usize::try_from(function_ordinal)
        .ok()
        .is_some_and(|ordinal| ordinal == expressions.functions.len());
    let (candidate_prepare, candidate_bytes, candidate_write) = if is_last_function {
        let output_key = canonical_row_key_sql("output_rows.output_row", &storage.output_type);
        let projected_functions = expressions
            .functions
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let column = quote_identifier(&format!("function_{}", index + 1));
                if index + 1 == usize::try_from(function_ordinal).unwrap_or(usize::MAX) {
                    format!(",finalized.function_value AS {column}")
                } else {
                    format!(",ordered.{column}")
                }
            })
            .collect::<String>();
        (
            format!(
                r#"
                projected AS MATERIALIZED (
                  SELECT ordered.ordinal,ordered.entry_id,ordered.copy_ordinal,
                         ordered.peer_id{projected_functions}
                  FROM {ordered} AS ordered CROSS JOIN finalized
                  WHERE ordered.ordinal=$2
                ),
                output_rows AS MATERIALIZED (
                  SELECT updated.ordinal,
                         ROW({outputs})::{output_type} AS output_row
                  FROM projected AS updated
                  JOIN {input} AS input_row
                    ON input_row.entry_id=updated.entry_id
                ),
                keyed AS MATERIALIZED (
                  SELECT output_rows.*,
                         {output_key} AS output_key
                  FROM output_rows
                ),
                "#,
                ordered = storage.ordered.sql(),
                outputs = expressions.outputs,
                output_type = storage.output_type.sql(),
                input = storage.input.sql(),
            ),
            "shiba_internal.effect_row_bytes(keyed.output_row)".to_string(),
            format!(
                r#"
                candidate_changed AS (
                  INSERT INTO {candidate} AS target(
                    partition_id,output_key,output_row,multiplicity
                  )
                  SELECT $1,keyed.output_key,keyed.output_row,1::numeric
                  FROM keyed,decision
                  WHERE decision.permitted
                  ON CONFLICT(partition_id,output_key) DO UPDATE
                  SET output_row=EXCLUDED.output_row,
                      multiplicity=target.multiplicity+1::numeric
                  RETURNING 1
                ),
                "#,
                candidate = storage.candidate.sql(),
            ),
        )
    } else {
        (
            String::new(),
            "0::bigint".to_string(),
            r#"
            candidate_changed AS (SELECT 1 WHERE false),
            "#
            .to_string(),
        )
    };
    let query = format!(
        r#"
        WITH finalized AS MATERIALIZED (
          SELECT ({value})::{result_type} AS function_value
          FROM {accumulator} AS accumulator
          WHERE accumulator.singleton
            AND accumulator.partition_id=$1
            AND accumulator.output_ordinal=$2
        ),
        {candidate_prepare}
        materialized AS MATERIALIZED (
          SELECT (
                   {function_bytes}+{candidate_bytes}
                 )::bigint AS work_bytes
          FROM finalized{candidate_from}
        ),
        decision AS MATERIALIZED (
          SELECT materialized.work_bytes,
                 $3::bigint>=1
                   AND (materialized.work_bytes<=$4::bigint OR $5::boolean)
                   AS permitted
          FROM materialized
        ),
        state_updated AS (
          UPDATE {ordered} AS ordered
          SET {function_column}=finalized.function_value
          FROM finalized,decision
          WHERE ordered.ordinal=$2 AND decision.permitted
          RETURNING 1
        ),
        {candidate_write}
        deleted AS (
          DELETE FROM {accumulator} AS accumulator
          USING decision
          WHERE accumulator.singleton
            AND accumulator.partition_id=$1
            AND accumulator.output_ordinal=$2
            AND decision.permitted
          RETURNING 1
        )
        SELECT CASE
                 WHEN (SELECT count(*) FROM finalized)<>1
                   THEN 'missing_accumulator'
                 WHEN (SELECT count(*) FROM materialized)<>1
                   THEN 'missing_output'
                 WHEN (SELECT permitted FROM decision)
                   THEN 'applied'
                 ELSE 'blocked'
               END,
               coalesce((SELECT work_bytes FROM decision),0)::bigint,
               (SELECT count(*)::bigint FROM state_updated)
                 +(SELECT count(*)::bigint FROM candidate_changed)
                 +(SELECT count(*)::bigint FROM deleted)
        "#,
        result_type = function.result_type,
        accumulator = accumulator.sql(),
        ordered = storage.ordered.sql(),
        candidate_from = if is_last_function { ",keyed" } else { "" },
    );
    let rows = transaction.write(&query, &unsafe {
        [
            DatumWithOid::new(partition_queue_id, pg_sys::INT8OID),
            DatumWithOid::new(output_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(
                window_i64_budget(remaining_rows, "Window finalize row budget")?,
                pg_sys::INT8OID,
            ),
            DatumWithOid::new(
                window_i64_budget(remaining_bytes, "Window finalize byte budget")?,
                pg_sys::INT8OID,
            ),
            DatumWithOid::new(allow_oversized_item, pg_sys::BOOLOID),
        ]
    })?;
    if rows.len() != 1 {
        return Err("Window aggregate finalization returned no summary".into());
    }
    let row = rows.first();
    let status: String = window_required(&row, 1, "Window aggregate finalization status")?;
    if status != "applied" && status != "blocked" {
        return Err(format!("Window aggregate finalization returned {status}"));
    }
    let work_bytes = window_nonnegative(
        window_required(&row, 2, "Window aggregate finalization bytes")?,
        "Window aggregate finalization bytes",
    )?;
    let state_rows = window_nonnegative(
        window_required(&row, 3, "Window aggregate finalization mutations")?,
        "Window aggregate finalization mutations",
    )?;
    let expected = if is_last_function { 3 } else { 2 };
    let applied = status == "applied";
    if state_rows != if applied { expected } else { 0 } {
        return Err("Window aggregate finalization mutation count is inconsistent".into());
    }
    if work_bytes == 0 {
        return Err("Window aggregate finalization returned no materialized bytes".into());
    }
    validate_window_finalize_decision(
        applied,
        work_bytes,
        remaining_rows,
        remaining_bytes,
        allow_oversized_item,
    )?;
    Ok(WindowFinalizePrimitive {
        applied,
        work_bytes,
        state_rows,
    })
}

fn run_window_evaluate(
    transaction: &mut StepTxn<'_, '_>,
    storage: &WindowStorage,
    expressions: &WindowExpressions,
    partition_queue_id: i64,
    function_ordinal: u32,
    cursor: WindowCursor,
) -> Result<WindowPage, String> {
    let index = usize::try_from(function_ordinal - 1)
        .map_err(|_| "Window function ordinal exceeds usize")?;
    let function = expressions
        .functions
        .get(index)
        .ok_or_else(|| "Window evaluation function is outside its plan".to_string())?;
    let WindowFunctionCapability::Native(native) = &function.capability else {
        return Err("aggregate Window function entered native evaluation".into());
    };
    let mut extra_state_rows = 0_u64;
    let ntile_state = if *native == NativeWindow::Ntile {
        Some(
            storage
                .ntile_states
                .get(index)
                .and_then(Option::as_ref)
                .ok_or_else(|| "Window ntile omitted its durable state".to_string())?,
        )
    } else {
        None
    };
    if cursor.row_id.is_none() {
        if let Some(state) = ntile_state {
            let rows = transaction.write(
                &format!(
                    "INSERT INTO {}(partition_id,bucket_count,first_ordinal) \
                     VALUES($1,NULL,NULL) RETURNING 1",
                    state.sql()
                ),
                &unsafe { [DatumWithOid::new(partition_queue_id, pg_sys::INT8OID)] },
            )?;
            if rows.len() != 1 {
                return Err("Window ntile did not initialize one durable state row".into());
            }
            extra_state_rows = 1;
        }
    }
    let budget = transaction.budget();
    let max_rows = window_i64_budget(budget.max_input_rows, "Window evaluation row budget")?;
    let raw_rows = max_rows
        .checked_add(1)
        .ok_or_else(|| "Window evaluation row budget overflow".to_string())?;
    let max_bytes = window_i64_budget(budget.max_input_bytes, "Window evaluation byte budget")?;
    let function_column = quote_identifier(&format!("function_{function_ordinal}"));
    let final_function = index + 1 == expressions.functions.len();
    let output_key = canonical_row_key_sql("output_rows.output_row", &storage.output_type);
    let (computation, native_state_status) = if let Some(state) = ntile_state {
        let bucket_argument = function
            .current_arguments
            .first()
            .ok_or_else(|| "Window ntile omitted its bucket argument".to_string())?;
        let value = window_ntile_value("fold.bucket_count", "fold.first_ordinal");
        (
            format!(
                r#"
                fold(step,ordinal,bucket_count,first_ordinal) AS (
                  SELECT 0::bigint,NULL::bigint,
                         state.bucket_count,state.first_ordinal
                  FROM {state} AS state
                  WHERE state.singleton AND state.partition_id=$1
                  UNION ALL
                  SELECT selected.page_ordinal,selected.ordinal,next.bucket_count,
                         CASE WHEN fold.first_ordinal IS NOT NULL
                              THEN fold.first_ordinal
                              WHEN next.bucket_count IS NOT NULL
                              THEN selected.ordinal
                         END
                  FROM fold
                  JOIN selected ON selected.page_ordinal=fold.step+1
                  JOIN {input} AS current_input
                    ON current_input.entry_id=selected.entry_id
                  CROSS JOIN LATERAL (
                    SELECT CASE WHEN fold.bucket_count IS NOT NULL
                                THEN fold.bucket_count
                                ELSE ({bucket_argument})::bigint
                           END AS bucket_count
                    OFFSET 0
                  ) AS next
                ),
                computed AS MATERIALIZED (
                  SELECT ordered.ordinal,({value})::{result_type} AS function_value
                  FROM fold
                  JOIN {ordered} AS ordered ON ordered.ordinal=fold.ordinal
                  JOIN {partitions} AS partition ON partition.partition_id=$1
                  WHERE fold.step>0
                ),
                final_fold AS MATERIALIZED (
                  SELECT step,bucket_count,first_ordinal
                  FROM fold ORDER BY step DESC LIMIT 1
                ),
                native_state_changed AS (
                  UPDATE {state} AS state
                  SET bucket_count=final_fold.bucket_count,
                      first_ordinal=final_fold.first_ordinal
                  FROM final_fold
                  WHERE state.singleton AND state.partition_id=$1
                    AND final_fold.step>0
                  RETURNING 1
                )
                "#,
                state = state.sql(),
                input = storage.input.sql(),
                ordered = storage.ordered.sql(),
                partitions = storage.partitions.sql(),
                result_type = function.result_type,
            ),
            format!(
                "WHEN (SELECT count(*) FROM {} \
                 WHERE singleton AND partition_id=$1)<>1 \
                 THEN 'missing_ntile_state'",
                state.sql()
            ),
        )
    } else {
        let value = window_native_value(storage, function, *native)?;
        (
            format!(
                r#"
                computed AS MATERIALIZED (
                  SELECT ordered.ordinal,({value}) AS function_value
                  FROM selected
                  JOIN {ordered} AS ordered
                    ON ordered.ordinal=selected.ordinal
                  JOIN {input} AS current_input
                    ON current_input.entry_id=ordered.entry_id
                  JOIN {peers} AS peer
                    ON peer.peer_id=ordered.peer_id
                  JOIN {frames} AS frame
                    ON frame.ordinal=ordered.ordinal
                  JOIN {partitions} AS partition ON partition.partition_id=$1
                ),
                native_state_changed AS (SELECT 1 WHERE false)
                "#,
                ordered = storage.ordered.sql(),
                input = storage.input.sql(),
                peers = storage.peers.sql(),
                frames = storage.frames.sql(),
                partitions = storage.partitions.sql(),
            ),
            String::new(),
        )
    };
    let candidate_write = if final_function {
        format!(
            r#"
            output_rows AS MATERIALIZED (
              SELECT updated.ordinal,
                     ROW({outputs})::{output_type} AS output_row
              FROM updated
              JOIN {input} AS input_row
                ON input_row.entry_id=updated.entry_id
            ),
            keyed AS MATERIALIZED (
              SELECT output_rows.*,
                     {output_key} AS output_key
              FROM output_rows
            ),
            collapsed AS MATERIALIZED (
              SELECT output_key,min(ordinal) AS representative_ordinal,
                     count(*)::numeric AS multiplicity
              FROM keyed GROUP BY output_key
            ),
            candidate_rows AS MATERIALIZED (
              SELECT collapsed.output_key,keyed.output_row,collapsed.multiplicity
              FROM collapsed JOIN keyed
                ON keyed.output_key=collapsed.output_key
               AND keyed.ordinal=collapsed.representative_ordinal
            ),
            candidate_changed AS (
              INSERT INTO {candidate} AS target(
                partition_id,output_key,output_row,multiplicity
              )
              SELECT $1,output_key,output_row,multiplicity FROM candidate_rows
              ON CONFLICT(partition_id,output_key) DO UPDATE
              SET output_row=EXCLUDED.output_row,
                  multiplicity=target.multiplicity+EXCLUDED.multiplicity
              RETURNING 1
            )
            "#,
            outputs = expressions.outputs,
            output_type = storage.output_type.sql(),
            input = storage.input.sql(),
            candidate = storage.candidate.sql(),
            output_key = output_key,
        )
    } else {
        "candidate_changed AS (SELECT 1 WHERE false)".into()
    };
    let query = format!(
        r#"
        WITH RECURSIVE source AS MATERIALIZED (
          SELECT ordered.ordinal,ordered.entry_id,ordered.peer_id,
                 current_input.row_value,
                 shiba_internal.effect_row_bytes(current_input.row_value) AS row_bytes
          FROM {ordered} AS ordered
          JOIN {input} AS current_input USING(entry_id)
          WHERE $2 IS NULL OR ordered.ordinal>$2
          ORDER BY ordered.ordinal
          LIMIT $5
        ),
        measured AS MATERIALIZED (
          SELECT source.*,row_number() OVER (ORDER BY ordinal) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY ordinal) AS running_bytes
          FROM source
        ),
        selected AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_ordinal <= $3
            AND (page_ordinal=1 OR running_bytes <= $4)
        ),
        {computation},
        updated AS (
          UPDATE {ordered} AS ordered
          SET {function_column}=computed.function_value
          FROM computed
          WHERE ordered.ordinal=computed.ordinal
          RETURNING ordered.*
        ),
        {candidate_write}
        SELECT CASE
                 WHEN NOT EXISTS(
                   SELECT 1 FROM {partitions}
                   WHERE partition_id=$1 AND dirty
                 ) THEN 'missing_partition'
                 WHEN $2 IS NOT NULL AND NOT EXISTS(
                   SELECT 1 FROM {ordered} WHERE ordinal=$2
                 ) THEN 'cursor_mismatch'
                 {native_state_status}
                 ELSE 'ok'
               END,
               count(*)::bigint,coalesce(sum(row_bytes),0)::bigint,
               (array_agg(ordinal ORDER BY page_ordinal DESC))[1],
               (SELECT count(*) FROM source)=(SELECT count(*) FROM selected),
               (SELECT count(*)::bigint FROM updated)
                 +(SELECT count(*)::bigint FROM candidate_changed)
                 +(SELECT count(*)::bigint FROM native_state_changed)
        FROM selected
        "#,
        ordered = storage.ordered.sql(),
        input = storage.input.sql(),
        partitions = storage.partitions.sql(),
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(partition_queue_id, pg_sys::INT8OID),
            DatumWithOid::new(cursor.row_id, pg_sys::INT8OID),
            DatumWithOid::new(max_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_bytes, pg_sys::INT8OID),
            DatumWithOid::new(raw_rows, pg_sys::INT8OID),
        ]
    };
    let rows = transaction.write(&query, &arguments)?;
    if rows.len() != 1 {
        return Err("Window evaluation returned no summary".into());
    }
    let row = rows.first();
    let status: String = window_required(&row, 1, "Window evaluation status")?;
    if status != "ok" {
        return Err(format!("Window evaluation returned {status}"));
    }
    let processed = window_nonnegative(
        window_required(&row, 2, "Window evaluation rows")?,
        "Window evaluation rows",
    )?;
    let bytes = window_nonnegative(
        window_required(&row, 3, "Window evaluation bytes")?,
        "Window evaluation bytes",
    )?;
    let last_row_id = row.get(4).map_err(|error| error.to_string())?;
    let complete = window_required(&row, 5, "Window evaluation completion")?;
    let mut state_rows = window_nonnegative(
        window_required(&row, 6, "Window evaluation mutations")?,
        "Window evaluation mutations",
    )?;
    if state_rows < processed + u64::from(ntile_state.is_some() && processed > 0) {
        return Err("Window evaluation did not update every selected row".into());
    }
    state_rows = state_rows
        .checked_add(extra_state_rows)
        .ok_or_else(|| "Window evaluation state count overflow".to_string())?;
    if complete {
        if let Some(state) = ntile_state {
            let rows = transaction.write(
                &format!(
                    "DELETE FROM {} WHERE singleton AND partition_id=$1 RETURNING 1",
                    state.sql()
                ),
                &unsafe { [DatumWithOid::new(partition_queue_id, pg_sys::INT8OID)] },
            )?;
            if rows.len() != 1 {
                return Err("Window ntile did not release one durable state row".into());
            }
            state_rows = state_rows
                .checked_add(1)
                .ok_or_else(|| "Window evaluation state count overflow".to_string())?;
        }
    }
    Ok(window_internal_page(
        processed,
        bytes,
        state_rows,
        last_row_id,
        complete,
    ))
}

fn window_native_value(
    storage: &WindowStorage,
    function: &WindowFunctionPlan,
    native: NativeWindow,
) -> Result<String, String> {
    let output_type = &function.result_type;
    let value = match native {
        NativeWindow::RowNumber => "ordered.ordinal".into(),
        NativeWindow::Rank => "peer.first_ordinal".into(),
        NativeWindow::DenseRank => "ordered.peer_id".into(),
        NativeWindow::PercentRank => "CASE WHEN partition.row_count<=1 THEN 0::double precision \
             ELSE (peer.first_ordinal-1)::double precision \
                  /(partition.row_count-1)::double precision END"
            .into(),
        NativeWindow::CumeDist => {
            "peer.last_ordinal::double precision/partition.row_count::double precision".into()
        }
        NativeWindow::Ntile => {
            return Err("Window ntile entered stateless evaluation".into());
        }
        NativeWindow::Lag | NativeWindow::Lead => {
            let offset = function
                .current_arguments
                .get(1)
                .cloned()
                .unwrap_or_else(|| "1".into());
            let target = if native == NativeWindow::Lag {
                format!("ordered.ordinal::numeric-({offset})::numeric")
            } else {
                format!("ordered.ordinal::numeric+({offset})::numeric")
            };
            let default = function
                .current_arguments
                .get(2)
                .cloned()
                .unwrap_or_else(|| format!("NULL::{output_type}"));
            window_target_value(
                storage,
                &target,
                &function.target_arguments[0],
                &default,
                output_type,
                Some(&offset),
            )
        }
        NativeWindow::FirstValue => window_target_value(
            storage,
            "coalesce(frame.start_1,frame.start_2,frame.start_3)",
            &function.target_arguments[0],
            &format!("NULL::{output_type}"),
            output_type,
            None,
        ),
        NativeWindow::LastValue => window_target_value(
            storage,
            "coalesce(frame.end_3,frame.end_2,frame.end_1)",
            &function.target_arguments[0],
            &format!("NULL::{output_type}"),
            output_type,
            None,
        ),
        NativeWindow::NthValue => {
            let nth = &function.current_arguments[1];
            let target = format!(
                r#"
                CASE WHEN ({nth}) IS NULL THEN NULL
                     WHEN ({nth})::bigint<=0
                       THEN 1::bigint/(ordered.ordinal-ordered.ordinal)
                     WHEN ({nth})::bigint<=coalesce(frame.end_1-frame.start_1+1,0)
                       THEN frame.start_1+({nth})::bigint-1
                     WHEN ({nth})::bigint<=coalesce(frame.end_1-frame.start_1+1,0)
                          +coalesce(frame.end_2-frame.start_2+1,0)
                       THEN frame.start_2+({nth})::bigint
                          -coalesce(frame.end_1-frame.start_1+1,0)-1
                     WHEN ({nth})::bigint<=frame.frame_count
                       THEN frame.start_3+({nth})::bigint
                          -coalesce(frame.end_1-frame.start_1+1,0)
                          -coalesce(frame.end_2-frame.start_2+1,0)-1
                     ELSE NULL
                END
                "#
            );
            window_target_value(
                storage,
                &target,
                &function.target_arguments[0],
                &format!("NULL::{output_type}"),
                output_type,
                None,
            )
        }
    };
    Ok(format!("({value})::{output_type}"))
}

fn window_ntile_value(buckets: &str, first_ordinal: &str) -> String {
    let active_ordinal = format!("(ordered.ordinal-({first_ordinal})+1)");
    let total_rows = "partition.row_count::bigint";
    format!(
        r#"
        CASE WHEN ({buckets}) IS NULL THEN NULL::bigint
          WHEN ({buckets})<=0
            THEN 1::bigint/(ordered.ordinal-ordered.ordinal)
          WHEN {active_ordinal}
               <= (({total_rows}/({buckets}))+1)
                  *({total_rows}%({buckets}))
          THEN ({active_ordinal}-1)
               /(({total_rows}/({buckets}))+1)+1
          ELSE ({total_rows}%({buckets}))
               +({active_ordinal}
                 -(({total_rows}/({buckets}))+1)
                  *({total_rows}%({buckets}))-1)
                 /({total_rows}/({buckets}))+1
        END
        "#,
    )
}

fn window_target_value(
    storage: &WindowStorage,
    target_ordinal: &str,
    target_value: &str,
    default_value: &str,
    output_type: &str,
    nullable_offset: Option<&str>,
) -> String {
    let lookup = format!(
        r#"
        (
          SELECT CASE WHEN target_ordered.ordinal IS NULL
                      THEN ({default_value})::{output_type}
                      ELSE ({target_value})::{output_type}
                 END
          FROM (SELECT 1) AS seed
          LEFT JOIN {ordered} AS target_ordered
            ON target_ordered.ordinal=({target_ordinal})
          LEFT JOIN {input} AS target_input
            ON target_input.entry_id=target_ordered.entry_id
        )
        "#,
        ordered = storage.ordered.sql(),
        input = storage.input.sql(),
    );
    nullable_offset.map_or(lookup.clone(), |offset| {
        format!("CASE WHEN ({offset}) IS NULL THEN NULL::{output_type} ELSE {lookup} END")
    })
}

fn run_window_diff(
    transaction: &mut StepTxn<'_, '_>,
    storage: &WindowStorage,
    partition_queue_id: i64,
    leg: DiffLeg,
    cursor: WindowDiffCursor,
) -> Result<WindowDiffPage, String> {
    cursor.validate()?;
    let causal_arguments = unsafe { [DatumWithOid::new(partition_queue_id, pg_sys::INT8OID)] };
    let causal_rows = transaction.read(
        &format!(
            "SELECT causal_lsn::text FROM {} \
             WHERE partition_id=$1 AND dirty AND causal_lsn IS NOT NULL",
            storage.partitions.sql()
        ),
        &causal_arguments,
    )?;
    if causal_rows.len() != 1 {
        return Err("Window dirty partition has no unique causal LSN".into());
    }
    let lsn: String = window_required(&causal_rows.first(), 1, "Window partition causal LSN")?;
    let output = transaction.output()?.clone();
    let budget = transaction.budget();
    let max_rows = i64::min(
        i64::min(
            window_i64_budget(budget.max_input_rows, "Window diff input rows")?,
            window_i64_budget(budget.max_output_rows, "Window diff output rows")?,
        ),
        output.target_rows,
    );
    let raw_rows = max_rows
        .checked_add(1)
        .ok_or_else(|| "Window diff row budget overflow".to_string())?;
    let max_bytes = i64::min(
        i64::min(
            window_i64_budget(budget.max_input_bytes, "Window diff input bytes")?,
            window_i64_budget(budget.max_output_bytes, "Window diff output bytes")?,
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
                WHERE visible.partition_id=$1
                  AND {cursor_predicate}
                ORDER BY visible.visible_id LIMIT $5
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
                  ON candidate.partition_id=$1
                 AND candidate.output_key=bounded_prefix.output_key
                "#,
                candidate = storage.candidate.sql(),
            ),
            format!(
                r#"
                deleted AS (
                  DELETE FROM {visible} AS visible USING differences
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
                WHERE candidate.partition_id=$1
                  AND {cursor_predicate}
                ORDER BY candidate.candidate_id LIMIT $5
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
                  ON visible.partition_id=$1
                 AND visible.output_key=bounded_prefix.output_key
                "#,
                visible = storage.visible.sql(),
            ),
            format!(
                r#"
                changed AS (
                  INSERT INTO {visible} AS target(
                    partition_id,output_key,output_row,multiplicity
                  )
                  SELECT $1,output_key,output_row,slice::numeric FROM differences
                  ON CONFLICT(partition_id,output_key) DO UPDATE
                  SET output_row=EXCLUDED.output_row,
                      multiplicity=target.multiplicity+EXCLUDED.multiplicity
                  RETURNING 1
                ),
                deleted AS (SELECT 1 WHERE false)
                "#,
                visible = storage.visible.sql(),
            ),
            "differences.slice",
        ),
    };
    let query = format!(
        r#"
        WITH source AS MATERIALIZED ({source}),
        numbered AS MATERIALIZED (
          SELECT source.*,row_number() OVER (ORDER BY row_id) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY row_id) AS running_bytes
          FROM source
        ),
        bounded_prefix AS MATERIALIZED (
          SELECT numbered.*
          FROM numbered
          WHERE page_ordinal<=$3
            AND (page_ordinal=1 OR running_bytes<=$4)
        ),
        joined AS MATERIALIZED ({compared}),
        marked AS MATERIALIZED (
          SELECT joined.*,
                 min(CASE WHEN delta>9223372036854775807::numeric
                          THEN page_ordinal END) OVER () AS first_huge_ordinal
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
          FROM differences CROSS JOIN appended
          WHERE appended.outcome='appended'
          RETURNING 1
        ),
        {mutation}
        SELECT stats.compared_rows,stats.compared_bytes,stats.last_id,
               (SELECT count(*) FROM source)
                 =(SELECT count(*) FROM bounded_prefix)
                 AND (SELECT count(*) FROM bounded_prefix)=stats.compared_rows
                 AND NOT stats.repeat_cursor,
               stats.repeat_cursor,stats.emitted_rows,stats.emitted_bytes,
               appended.outcome,appended.appended_chunk_seq,
               (SELECT count(*)::bigint FROM payload_insert),
               (SELECT count(*)::bigint FROM changed)
                 +(SELECT count(*)::bigint FROM deleted)
        FROM stats LEFT JOIN appended ON true
        "#,
        output_payload = storage.output_payload.sql(),
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(partition_queue_id, pg_sys::INT8OID),
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
        return Err("Window diff returned no summary".into());
    }
    let row = rows.first();
    let compared_rows = window_nonnegative(
        window_required(&row, 1, "Window compared rows")?,
        "Window compared rows",
    )?;
    let compared_bytes = window_nonnegative(
        window_required(&row, 2, "Window compared bytes")?,
        "Window compared bytes",
    )?;
    let last_row_id = row.get(3).map_err(|error| error.to_string())?;
    let complete = window_required(&row, 4, "Window diff completion")?;
    let repeat_cursor = window_required(&row, 5, "Window residual cursor")?;
    let emitted = window_nonnegative(
        window_required(&row, 6, "Window diff rows")?,
        "Window diff rows",
    )?;
    let emitted_bytes = window_nonnegative(
        window_required(&row, 7, "Window diff bytes")?,
        "Window diff bytes",
    )?;
    let append_outcome = row.get::<String>(8).map_err(|error| error.to_string())?;
    let appended_sequence = row.get::<i64>(9).map_err(|error| error.to_string())?;
    let inserted = window_nonnegative(
        window_required(&row, 10, "Window payload inserts")?,
        "Window payload inserts",
    )?;
    let mutated = window_nonnegative(
        window_required(&row, 11, "Window visible mutations")?,
        "Window visible mutations",
    )?;
    let output_facts = if emitted == 0 {
        if append_outcome.is_some() || appended_sequence.is_some() || inserted != 0 || mutated != 0
        {
            return Err("Window appended or mutated an empty diff".into());
        }
        OutputFacts::None
    } else {
        if append_outcome.as_deref() != Some("appended")
            || appended_sequence != Some(output.next_chunk_seq)
            || inserted != emitted
            || mutated != emitted
        {
            return Err("Window diff append is inconsistent".into());
        }
        OutputFacts::Data {
            chunk_seq: output.next_chunk_seq,
        }
    };
    Ok(WindowDiffPage {
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

fn run_window_cleanup(
    transaction: &mut StepTxn<'_, '_>,
    storage: &WindowStorage,
    expressions: &WindowExpressions,
    partition_queue_id: i64,
    cursor: WindowCleanupCursor,
    after_partitions: AfterPartitions,
) -> Result<WindowCleanup, String> {
    let final_ordinal = 3;
    let relation = match cursor.relation_ordinal {
        0 => Some((
            &storage.candidate,
            "candidate_id",
            format!("partition_id={partition_queue_id}"),
            "shiba_internal.effect_row_bytes(output_row)".to_string(),
        )),
        1 => Some((
            &storage.ordered,
            "ordinal",
            "true".into(),
            format!(
                "coalesce((SELECT shiba_internal.effect_row_bytes(input.row_value) \
                 FROM {} AS input WHERE input.entry_id=target.entry_id),24)",
                storage.input.sql()
            ),
        )),
        2 => Some((&storage.peers, "peer_id", "true".into(), "24".into())),
        3 => Some((&storage.frames, "ordinal", "true".into(), "64".into())),
        _ => None,
    };
    let mut page = if let Some((relation, identity, predicate, bytes)) = relation {
        run_window_cleanup_relation(
            transaction,
            relation,
            identity,
            &predicate,
            &bytes,
            cursor.row,
        )?
    } else {
        window_internal_page(0, 0, 0, None, true)
    };
    let mut next_partition_queue_id = None;
    if page.complete && cursor.relation_ordinal == final_ordinal {
        for accumulator in storage.accumulators.iter().flatten() {
            let rows = transaction.read(
                &format!("SELECT count(*)::bigint FROM {}", accumulator.sql()),
                &[],
            )?;
            if window_required::<i64>(&rows.first(), 1, "Window accumulator rows")? != 0 {
                return Err("Window cleanup found an unfinished aggregate fold".into());
            }
        }
        for state in storage.ntile_states.iter().flatten() {
            let rows = transaction.read(
                &format!("SELECT count(*)::bigint FROM {}", state.sql()),
                &[],
            )?;
            if window_required::<i64>(&rows.first(), 1, "Window ntile state rows")? != 0 {
                return Err("Window cleanup found unfinished ntile evaluation".into());
            }
        }
        let keep_empty = expressions.partition_columns.is_empty();
        let query = format!(
            r#"
            WITH removed AS (
              DELETE FROM {partitions}
              WHERE partition_id=$1 AND dirty AND row_count=0
                AND NOT $2::boolean
              RETURNING 1
            ),
            cleaned AS (
              UPDATE {partitions}
              SET dirty=false,causal_lsn=NULL
              WHERE partition_id=$1 AND dirty
                AND (row_count<>0 OR $2::boolean)
              RETURNING 1
            )
            SELECT (SELECT count(*)::bigint FROM removed)
                     +(SELECT count(*)::bigint FROM cleaned),
                   (SELECT min(partition_id)::bigint
                    FROM {partitions}
                    WHERE dirty AND partition_id>$1)
            "#,
            partitions = storage.partitions.sql(),
        );
        let arguments = unsafe {
            [
                DatumWithOid::new(partition_queue_id, pg_sys::INT8OID),
                DatumWithOid::new(keep_empty, pg_sys::BOOLOID),
            ]
        };
        let rows = transaction.write(&query, &arguments)?;
        if rows.len() != 1 {
            return Err("Window partition finalization returned no summary".into());
        }
        let row = rows.first();
        let finalized = window_nonnegative(
            window_required(&row, 1, "Window finalized partitions")?,
            "Window finalized partitions",
        )?;
        next_partition_queue_id = row.get(2).map_err(|error| error.to_string())?;
        if finalized != 1 {
            return Err("Window finalization did not consume one dirty partition".into());
        }
        page.facts.state_rows = page
            .facts
            .state_rows
            .checked_add(finalized)
            .ok_or_else(|| "Window cleanup state count overflow".to_string())?;
    }
    page.facts.continuation_rows = u64::from(
        !page.complete
            || cursor.relation_ordinal != final_ordinal
            || next_partition_queue_id.is_some()
            || !matches!(after_partitions, AfterPartitions::FinishInput),
    );
    Ok(WindowCleanup {
        page,
        next_partition_queue_id,
    })
}

fn run_window_cleanup_relation(
    transaction: &mut StepTxn<'_, '_>,
    relation: &RelationRef,
    identity: &str,
    predicate: &str,
    bytes: &str,
    cursor: WindowCursor,
) -> Result<WindowPage, String> {
    let budget = transaction.budget();
    let max_rows = window_i64_budget(budget.max_input_rows, "Window cleanup row budget")?;
    let raw_rows = max_rows
        .checked_add(1)
        .ok_or_else(|| "Window cleanup row budget overflow".to_string())?;
    let max_bytes = window_i64_budget(budget.max_input_bytes, "Window cleanup byte budget")?;
    let identity = quote_identifier(identity);
    let query = format!(
        r#"
        WITH source AS MATERIALIZED (
          SELECT target.{identity} AS row_id,({bytes})::bigint AS row_bytes
          FROM {relation} AS target
          WHERE ({predicate}) AND ($1 IS NULL OR target.{identity}>=$1)
          ORDER BY target.{identity}
          LIMIT $4
        ),
        measured AS MATERIALIZED (
          SELECT source.*,row_number() OVER (ORDER BY row_id) AS page_ordinal,
                 sum(row_bytes) OVER (ORDER BY row_id) AS running_bytes
          FROM source
        ),
        selected AS MATERIALIZED (
          SELECT * FROM measured
          WHERE page_ordinal<=$2 AND (page_ordinal=1 OR running_bytes<=$3)
        ),
        deleted AS (
          DELETE FROM {relation} AS target USING selected
          WHERE target.{identity}=selected.row_id
          RETURNING 1
        )
        SELECT count(*)::bigint,coalesce(sum(row_bytes),0)::bigint,
               (array_agg(row_id ORDER BY page_ordinal DESC))[1],
               (SELECT count(*) FROM source)=(SELECT count(*) FROM selected),
               (SELECT count(*)::bigint FROM deleted)
        FROM selected
        "#,
        relation = relation.sql(),
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(cursor.row_id, pg_sys::INT8OID),
            DatumWithOid::new(max_rows, pg_sys::INT8OID),
            DatumWithOid::new(max_bytes, pg_sys::INT8OID),
            DatumWithOid::new(raw_rows, pg_sys::INT8OID),
        ]
    };
    let rows = transaction.write(&query, &arguments)?;
    if rows.len() != 1 {
        return Err("Window relation cleanup returned no summary".into());
    }
    let row = rows.first();
    let deleted = window_nonnegative(
        window_required(&row, 1, "Window cleanup rows")?,
        "Window cleanup rows",
    )?;
    let row_bytes = window_nonnegative(
        window_required(&row, 2, "Window cleanup bytes")?,
        "Window cleanup bytes",
    )?;
    let last_row_id = row.get(3).map_err(|error| error.to_string())?;
    let complete = window_required(&row, 4, "Window cleanup completion")?;
    let mutations = window_nonnegative(
        window_required(&row, 5, "Window cleanup deletes")?,
        "Window cleanup deletes",
    )?;
    if mutations != deleted {
        return Err("Window cleanup delete count is inconsistent".into());
    }
    Ok(window_internal_page(
        deleted,
        row_bytes,
        mutations,
        last_row_id,
        complete,
    ))
}

fn run_window_frontier(
    transaction: &mut StepTxn<'_, '_>,
    input: InputPosition,
) -> Result<PrimitiveFacts, String> {
    if input.row_ordinal != 0 {
        return Err("Window frontier has a row cursor".into());
    }
    let input_state = transaction.input(0)?.clone();
    let frontier = chunk(transaction, &input_state, input.chunk_seq)?
        .ok_or_else(|| "Window frontier chunk is missing".to_string())?;
    if frontier.kind != ChunkKind::Frontier || frontier.stream_id != input.stream_id {
        return Err("Window frontier continuation references data".into());
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
        InputPosition::new(5, 7, row).unwrap()
    }

    fn frontier_position() -> InputPosition {
        InputPosition::new(5, 8, 0).unwrap()
    }

    fn internal_page(last_row_id: Option<i64>, complete: bool) -> WindowPage {
        WindowPage {
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

    fn fold_page(
        next_cursor: Option<WindowFoldCursor>,
        input_rows: u64,
        work_items: usize,
    ) -> WindowFoldPage {
        WindowFoldPage {
            facts: PrimitiveFacts {
                usage: WorkUsage {
                    input_rows,
                    input_bytes: input_rows * 8,
                    ..WorkUsage::default()
                },
                state_rows: 1,
                continuation_rows: 1,
                output: OutputFacts::None,
            },
            next_cursor,
            work_items,
        }
    }

    fn native_machine() -> WindowMachine {
        WindowMachine::new(vec![WindowFunctionKind::Native]).unwrap()
    }

    fn diff_page(
        last_row_id: Option<i64>,
        complete: bool,
        chunk_seq: Option<i64>,
    ) -> WindowDiffPage {
        let output_rows = u64::from(chunk_seq.is_some());
        WindowDiffPage {
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

    fn committed_continuation(transition: WindowTransition) -> WindowContinuation {
        let WindowTransition::Committed {
            continuation: Some(continuation),
            ..
        } = transition
        else {
            panic!("step should have a continuation");
        };
        continuation
    }

    #[test]
    fn strict_phase_codes_have_no_idle_or_unknown_decoder() {
        assert_eq!(
            WindowPhaseKind::from_code(WindowPhaseKind::Frames.code()).unwrap(),
            WindowPhaseKind::Frames
        );
        assert!(PhaseCode::active(0).is_err());
        assert!(WindowPhaseKind::from_code(PhaseCode::active(99).unwrap()).is_err());
    }

    #[test]
    fn admission_can_idle_with_a_durable_dirty_queue() {
        let machine = native_machine();
        let continuation = WindowContinuation {
            input_stream_id: 5,
            input: Some(position(0)),
            phase: WindowPhase::Admit,
        };
        let transition = machine
            .apply(
                continuation,
                WindowActionResult::Admitted(WindowAdmission {
                    facts: PrimitiveFacts {
                        usage: WorkUsage {
                            input_rows: 2,
                            input_bytes: 16,
                            ..WorkUsage::default()
                        },
                        state_rows: 3,
                        continuation_rows: 0,
                        output: OutputFacts::None,
                    },
                    target: WindowAdmissionTarget::Idle,
                }),
                budget(),
            )
            .unwrap();
        assert!(matches!(
            transition,
            WindowTransition::Committed {
                continuation: None,
                ..
            }
        ));
    }

    #[test]
    fn window_resumes_every_bounded_build_phase() {
        let machine = WindowMachine::new(vec![
            WindowFunctionKind::Aggregate,
            WindowFunctionKind::Native,
        ])
        .unwrap();
        let mut continuation = WindowContinuation {
            input_stream_id: 5,
            input: Some(position(1)),
            phase: WindowPhase::Admit,
        };
        continuation = committed_continuation(
            machine
                .apply(
                    continuation,
                    WindowActionResult::Admitted(WindowAdmission {
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
                        target: WindowAdmissionTarget::Drain {
                            first_partition_queue_id: 11,
                            after_partitions: AfterPartitions::Frontier(frontier_position()),
                        },
                    }),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(
            machine.action(continuation).unwrap(),
            WindowAction::Enumerate { cursor, .. } if cursor.row_id.is_none()
        ));

        continuation = committed_continuation(
            machine
                .apply(
                    continuation,
                    WindowActionResult::Enumerated(internal_page(Some(21), false)),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(
            continuation.phase,
            WindowPhase::Enumerate {
                cursor: WindowCursor { row_id: Some(21) },
                ..
            }
        ));

        continuation = committed_continuation(
            machine
                .apply(
                    continuation,
                    WindowActionResult::Enumerated(internal_page(None, true)),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(continuation.phase, WindowPhase::Peers { .. }));

        continuation = committed_continuation(
            machine
                .apply(
                    continuation,
                    WindowActionResult::PeersBuilt(internal_page(None, true)),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(continuation.phase, WindowPhase::Frames { .. }));

        continuation = committed_continuation(
            machine
                .apply(
                    continuation,
                    WindowActionResult::FramesBuilt(internal_page(None, true)),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(
            continuation.phase,
            WindowPhase::FoldAggregate {
                function_ordinal: 1,
                cursor: WindowFoldCursor {
                    output_ordinal: 1,
                    last_frame_ordinal: None,
                    ready_to_finalize: false,
                },
                ..
            }
        ));

        continuation = committed_continuation(
            machine
                .apply(
                    continuation,
                    WindowActionResult::AggregateFolded(fold_page(
                        Some(WindowFoldCursor {
                            output_ordinal: 1,
                            last_frame_ordinal: Some(29),
                            ready_to_finalize: false,
                        }),
                        1,
                        1,
                    )),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(
            continuation.phase,
            WindowPhase::FoldAggregate {
                function_ordinal: 1,
                cursor: WindowFoldCursor {
                    output_ordinal: 1,
                    last_frame_ordinal: Some(29),
                    ready_to_finalize: false,
                },
                ..
            }
        ));

        continuation = committed_continuation(
            machine
                .apply(
                    continuation,
                    WindowActionResult::AggregateFolded(fold_page(
                        Some(WindowFoldCursor {
                            output_ordinal: 2,
                            last_frame_ordinal: None,
                            ready_to_finalize: false,
                        }),
                        1,
                        1,
                    )),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(
            continuation.phase,
            WindowPhase::FoldAggregate {
                function_ordinal: 1,
                cursor: WindowFoldCursor {
                    output_ordinal: 2,
                    last_frame_ordinal: None,
                    ready_to_finalize: false,
                },
                ..
            }
        ));

        continuation = committed_continuation(
            machine
                .apply(
                    continuation,
                    WindowActionResult::AggregateFolded(fold_page(None, 0, 1)),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(
            continuation.phase,
            WindowPhase::Evaluate {
                function_ordinal: 2,
                ..
            }
        ));

        continuation = committed_continuation(
            machine
                .apply(
                    continuation,
                    WindowActionResult::Evaluated(internal_page(None, true)),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(
            continuation.phase,
            WindowPhase::Diff {
                leg: DiffLeg::Remove,
                ..
            }
        ));
    }

    #[test]
    fn aggregate_fold_commits_multiple_output_ordinals_per_step() {
        let machine = WindowMachine::new(vec![WindowFunctionKind::Aggregate]).unwrap();
        let start = WindowContinuation {
            input_stream_id: 5,
            input: None,
            phase: WindowPhase::FoldAggregate {
                partition_queue_id: 11,
                function_ordinal: 1,
                cursor: WindowFoldCursor {
                    output_ordinal: 1,
                    last_frame_ordinal: None,
                    ready_to_finalize: false,
                },
                after_partitions: AfterPartitions::FinishInput,
            },
        };
        let next = committed_continuation(
            machine
                .apply(
                    start,
                    WindowActionResult::AggregateFolded(fold_page(
                        Some(WindowFoldCursor {
                            output_ordinal: 3,
                            last_frame_ordinal: None,
                            ready_to_finalize: false,
                        }),
                        2,
                        2,
                    )),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(
            next.phase,
            WindowPhase::FoldAggregate {
                cursor: WindowFoldCursor {
                    output_ordinal: 3,
                    last_frame_ordinal: None,
                    ready_to_finalize: false,
                },
                ..
            }
        ));
    }

    #[test]
    fn aggregate_fold_work_items_bound_empty_frames() {
        let machine = WindowMachine::new(vec![WindowFunctionKind::Aggregate]).unwrap();
        let start = WindowContinuation {
            input_stream_id: 5,
            input: None,
            phase: WindowPhase::FoldAggregate {
                partition_queue_id: 11,
                function_ordinal: 1,
                cursor: WindowFoldCursor {
                    output_ordinal: 1,
                    last_frame_ordinal: None,
                    ready_to_finalize: false,
                },
                after_partitions: AfterPartitions::FinishInput,
            },
        };
        let page = fold_page(
            Some(WindowFoldCursor {
                output_ordinal: i64::try_from(WINDOW_FOLD_WORK_ITEM_CAP).unwrap() + 1,
                last_frame_ordinal: None,
                ready_to_finalize: false,
            }),
            0,
            WINDOW_FOLD_WORK_ITEM_CAP,
        );
        let next = committed_continuation(
            machine
                .apply(start, WindowActionResult::AggregateFolded(page), budget())
                .unwrap(),
        );
        assert!(matches!(
            next.phase,
            WindowPhase::FoldAggregate {
                cursor: WindowFoldCursor {
                    output_ordinal,
                    last_frame_ordinal: None,
                    ready_to_finalize: false,
                },
                ..
            } if output_ordinal == i64::try_from(WINDOW_FOLD_WORK_ITEM_CAP).unwrap() + 1
        ));

        let mut beyond_cap = page;
        beyond_cap.work_items += 1;
        assert!(machine
            .apply(
                start,
                WindowActionResult::AggregateFolded(beyond_cap),
                budget(),
            )
            .is_err());

        let mut skipped = page;
        skipped.work_items -= 1;
        assert!(machine
            .apply(
                start,
                WindowActionResult::AggregateFolded(skipped),
                budget()
            )
            .is_err());
    }

    #[test]
    fn aggregate_fold_ready_state_roundtrips_and_resumes_finalization() {
        let machine = WindowMachine::new(vec![WindowFunctionKind::Aggregate]).unwrap();
        let start = WindowContinuation {
            input_stream_id: 5,
            input: None,
            phase: WindowPhase::FoldAggregate {
                partition_queue_id: 11,
                function_ordinal: 1,
                cursor: WindowFoldCursor {
                    output_ordinal: 1,
                    last_frame_ordinal: None,
                    ready_to_finalize: false,
                },
                after_partitions: AfterPartitions::FinishInput,
            },
        };
        let ready = committed_continuation(
            machine
                .apply(
                    start,
                    WindowActionResult::AggregateFolded(fold_page(
                        Some(WindowFoldCursor {
                            output_ordinal: 1,
                            last_frame_ordinal: None,
                            ready_to_finalize: true,
                        }),
                        0,
                        1,
                    )),
                    budget(),
                )
                .unwrap(),
        );
        assert_eq!(
            decode_window_fields(encode_window_fields(ready).unwrap()).unwrap(),
            ready
        );

        let next = committed_continuation(
            machine
                .apply(
                    ready,
                    WindowActionResult::AggregateFolded(fold_page(
                        Some(WindowFoldCursor {
                            output_ordinal: 2,
                            last_frame_ordinal: None,
                            ready_to_finalize: false,
                        }),
                        1,
                        1,
                    )),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(
            next.phase,
            WindowPhase::FoldAggregate {
                cursor: WindowFoldCursor {
                    output_ordinal: 2,
                    ready_to_finalize: false,
                    ..
                },
                ..
            }
        ));
    }

    #[test]
    fn finalize_budget_blocks_then_allows_one_oversized_item() {
        validate_window_finalize_decision(false, 5_000, 1, 100, false).unwrap();
        assert!(validate_window_finalize_decision(true, 5_000, 1, 100, false).is_err());
        assert!(validate_window_finalize_decision(false, 80, 1, 100, false).is_err());
        validate_window_finalize_decision(true, 5_000, 1, 100, true).unwrap();
        assert!(validate_window_finalize_decision(true, 5_000, 0, 100, true).is_err());
    }

    #[test]
    fn missing_window_frame_is_not_an_empty_frame() {
        assert!(validate_window_fold_status("missing_frame").is_err());
        validate_window_fold_status("ok").unwrap();
    }

    #[test]
    fn frontier_waits_for_both_diff_legs_and_cleanup() {
        let machine = native_machine();
        let mut continuation = WindowContinuation {
            input_stream_id: 5,
            input: None,
            phase: WindowPhase::Diff {
                partition_queue_id: 11,
                leg: DiffLeg::Remove,
                cursor: WindowDiffCursor::default(),
                after_partitions: AfterPartitions::Frontier(frontier_position()),
            },
        };

        assert!(machine
            .apply(
                continuation,
                WindowActionResult::FrontierForwarded(PrimitiveFacts {
                    output: OutputFacts::Frontier { chunk_seq: 30 },
                    ..PrimitiveFacts::default()
                }),
                budget(),
            )
            .is_err());

        continuation = committed_continuation(
            machine
                .apply(
                    continuation,
                    WindowActionResult::Diffed(diff_page(Some(31), true, Some(40))),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(
            continuation.phase,
            WindowPhase::Diff {
                leg: DiffLeg::Add,
                ..
            }
        ));

        continuation = committed_continuation(
            machine
                .apply(
                    continuation,
                    WindowActionResult::Diffed(diff_page(None, true, None)),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(continuation.phase, WindowPhase::Cleanup { .. }));

        for relation_ordinal in 0..machine.cleanup_relation_count() {
            assert!(matches!(
                machine.action(continuation).unwrap(),
                WindowAction::Cleanup {
                    cursor: WindowCleanupCursor {
                        relation_ordinal: actual,
                        ..
                    },
                    ..
                } if actual == relation_ordinal
            ));
            continuation = committed_continuation(
                machine
                    .apply(
                        continuation,
                        WindowActionResult::Cleaned(WindowCleanup {
                            page: internal_page(None, true),
                            next_partition_queue_id: None,
                        }),
                        budget(),
                    )
                    .unwrap(),
            );
        }
        assert_eq!(continuation.phase, WindowPhase::Frontier);
        assert!(matches!(
            machine.action(continuation).unwrap(),
            WindowAction::ForwardFrontier { .. }
        ));
    }

    #[test]
    fn diff_cursor_advances_on_zero_difference_and_repeats_only_residuals() {
        let machine = native_machine();
        let start = WindowContinuation {
            input_stream_id: 5,
            input: None,
            phase: WindowPhase::Diff {
                partition_queue_id: 11,
                leg: DiffLeg::Remove,
                cursor: WindowDiffCursor::default(),
                after_partitions: AfterPartitions::FinishInput,
            },
        };
        let after_equal_prefix = committed_continuation(
            machine
                .apply(
                    start,
                    WindowActionResult::Diffed(diff_page(Some(31), false, None)),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(
            after_equal_prefix.phase,
            WindowPhase::Diff {
                cursor: WindowDiffCursor {
                    row_id: Some(31),
                    repeat: false,
                },
                ..
            }
        ));
        assert!(machine
            .apply(
                after_equal_prefix,
                WindowActionResult::Diffed(diff_page(Some(31), false, None)),
                budget(),
            )
            .is_err());

        let mut residual = diff_page(Some(41), false, Some(42));
        residual.repeat_cursor = true;
        let after_residual = committed_continuation(
            machine
                .apply(start, WindowActionResult::Diffed(residual), budget())
                .unwrap(),
        );
        assert!(matches!(
            after_residual.phase,
            WindowPhase::Diff {
                cursor: WindowDiffCursor {
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
                    WindowActionResult::Diffed(diff_page(Some(41), false, Some(43))),
                    budget(),
                )
                .unwrap(),
        );
        assert!(matches!(
            after_final_slice.phase,
            WindowPhase::Diff {
                cursor: WindowDiffCursor {
                    row_id: Some(41),
                    repeat: false,
                },
                ..
            }
        ));
    }

    #[test]
    fn diff_repeat_cursor_roundtrips_through_the_typed_continuation() {
        let continuation = WindowContinuation {
            input_stream_id: 5,
            input: None,
            phase: WindowPhase::Diff {
                partition_queue_id: 11,
                leg: DiffLeg::Add,
                cursor: WindowDiffCursor {
                    row_id: Some(41),
                    repeat: true,
                },
                after_partitions: AfterPartitions::Frontier(frontier_position()),
            },
        };
        let fields = encode_window_fields(continuation).unwrap();
        assert!(fields.cursor_repeat);
        assert_eq!(decode_window_fields(fields).unwrap(), continuation);
    }

    #[test]
    fn final_partition_cleanup_can_resume_the_same_input_chunk() {
        let machine = native_machine();
        let transition = machine
            .apply(
                WindowContinuation {
                    input_stream_id: 5,
                    input: None,
                    phase: WindowPhase::Cleanup {
                        partition_queue_id: 11,
                        cursor: WindowCleanupCursor {
                            relation_ordinal: machine.cleanup_relation_count() - 1,
                            row: WindowCursor::default(),
                        },
                        after_partitions: AfterPartitions::Admit(position(2)),
                    },
                },
                WindowActionResult::Cleaned(WindowCleanup {
                    page: internal_page(None, true),
                    next_partition_queue_id: None,
                }),
                budget(),
            )
            .unwrap();
        assert_eq!(
            committed_continuation(transition),
            WindowContinuation {
                input_stream_id: 5,
                input: Some(position(2)),
                phase: WindowPhase::Admit,
            }
        );
    }

    #[test]
    fn one_oversized_row_is_the_only_byte_budget_exception() {
        let machine = native_machine();
        let durable = WindowContinuation {
            input_stream_id: 5,
            input: None,
            phase: WindowPhase::Diff {
                partition_queue_id: 11,
                leg: DiffLeg::Add,
                cursor: WindowDiffCursor::default(),
                after_partitions: AfterPartitions::FinishInput,
            },
        };
        let oversized = WindowDiffPage {
            facts: PrimitiveFacts {
                usage: WorkUsage {
                    input_rows: 1,
                    input_bytes: 21,
                    output_rows: 1,
                    output_bytes: 13,
                },
                state_rows: 1,
                continuation_rows: 1,
                output: OutputFacts::Data { chunk_seq: 50 },
            },
            last_row_id: Some(1),
            complete: false,
            repeat_cursor: false,
        };
        machine
            .apply(durable, WindowActionResult::Diffed(oversized), budget())
            .unwrap();

        let two_rows = WindowDiffPage {
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
            .apply(durable, WindowActionResult::Diffed(two_rows), budget(),)
            .is_err());
    }
}
