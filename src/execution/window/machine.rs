use super::*;

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
    pub(super) fn validate(self) -> Result<(), String> {
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
    pub(super) fn validate(self) -> Result<(), String> {
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
    pub(super) fn validate(self) -> Result<(), String> {
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
pub(super) enum WindowFunctionKind {
    Native,
    Aggregate,
}

/// Plan-local function kinds choose the only legal phase for each function.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WindowMachine {
    function_kinds: Vec<WindowFunctionKind>,
}

impl WindowMachine {
    pub(super) fn new(function_kinds: Vec<WindowFunctionKind>) -> Result<Self, String> {
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

    pub(super) fn function_count(&self) -> u32 {
        u32::try_from(self.function_kinds.len()).expect("Window function count was validated")
    }

    pub(super) fn function_kind(&self, ordinal: u32) -> Result<WindowFunctionKind, String> {
        self.function_kinds
            .get(usize::try_from(ordinal - 1).map_err(|_| "Window function exceeds usize")?)
            .copied()
            .ok_or_else(|| "Window function ordinal is outside its plan".into())
    }

    pub(super) fn first_function_phase(
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

    pub(super) fn validate_continuation(
        &self,
        continuation: WindowContinuation,
    ) -> Result<(), String> {
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

    pub(super) fn cleanup_relation_count(&self) -> u32 {
        4
    }

    pub(super) fn apply_admission(
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
    pub(super) fn apply_internal_page(
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
    pub(super) fn apply_aggregate_fold(
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
    pub(super) fn apply_evaluation(
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
    pub(super) fn apply_diff(
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

    pub(super) fn apply_cleanup(
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

    pub(super) fn apply_frontier(
        &self,
        facts: PrimitiveFacts,
        budget: WorkBudget,
    ) -> Result<WindowTransition, String> {
        facts.validate_protocol(budget, KernelPhase::Frontier, KernelCompletion::Finished)?;
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
pub(super) enum WindowInternalStage {
    Enumerate,
    Peers,
    Frames,
}

impl WindowInternalStage {
    pub(super) fn phase(
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
    pub(super) fn validate(
        self,
        previous: WindowDiffCursor,
        budget: WorkBudget,
    ) -> Result<(), String> {
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
    pub(super) fn validate(
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
    pub(super) fn validate(
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

pub(super) fn phase_after_partitions(phase: WindowPhase) -> Result<AfterPartitions, String> {
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

pub(super) fn validate_input(input: InputPosition) -> Result<(), String> {
    if input.stream_id <= 0 || input.chunk_seq <= 0 || input.row_ordinal < 0 {
        return Err("Window input position is invalid".into());
    }
    Ok(())
}

pub(super) fn validate_queue_id(queue_id: i64) -> Result<(), String> {
    if queue_id <= 0 {
        return Err("Window partition queue id is not positive".into());
    }
    Ok(())
}

pub(super) fn validate_after_partitions(
    input_stream_id: i64,
    after: AfterPartitions,
) -> Result<(), String> {
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

pub(super) fn validate_no_external_output(facts: PrimitiveFacts) -> Result<(), String> {
    if facts.output != OutputFacts::None
        || facts.usage.output_rows != 0
        || facts.usage.output_bytes != 0
    {
        return Err("Window internal phase reported external output".into());
    }
    Ok(())
}

pub(super) fn validate_continuation_count(
    facts: PrimitiveFacts,
    has_continuation: bool,
) -> Result<(), String> {
    facts.validate_continuation(has_continuation)
}

#[derive(Clone, Debug)]
pub(super) struct WindowStorage {
    pub(super) partitions: RelationRef,
    pub(super) input: RelationRef,
    pub(super) ordered: RelationRef,
    pub(super) peers: RelationRef,
    pub(super) frames: RelationRef,
    pub(super) candidate: RelationRef,
    pub(super) visible: RelationRef,
    pub(super) continuation: RelationRef,
    pub(super) accumulators: Vec<Option<RelationRef>>,
    pub(super) ntile_states: Vec<Option<RelationRef>>,
    pub(super) input_payload: RelationRef,
    pub(super) output_payload: RelationRef,
    pub(super) input_type: TypeRef,
    pub(super) output_type: TypeRef,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DurableWindow {
    pub(super) continuation: WindowContinuation,
    pub(super) persisted: bool,
}

#[derive(Clone, Debug)]
pub(super) struct WindowExpressions {
    pub(super) partition_expressions: Vec<String>,
    pub(super) partition_columns: Vec<String>,
    pub(super) order_expressions: Vec<String>,
    pub(super) order_columns: Vec<String>,
    pub(super) order_by: String,
    pub(super) keyset_after: String,
    pub(super) peer_equal: String,
    pub(super) outputs: String,
    pub(super) functions: Vec<WindowFunctionPlan>,
    pub(super) frame_start_offset: Option<String>,
    pub(super) frame_end_offset: Option<String>,
}

#[derive(Clone, Debug)]
pub(super) struct WindowFunctionPlan {
    pub(super) current_arguments: Vec<String>,
    pub(super) target_arguments: Vec<String>,
    pub(super) filter: String,
    pub(super) result_type: String,
    pub(super) capability: WindowFunctionCapability,
}

#[derive(Clone, Debug)]
pub(super) enum WindowFunctionCapability {
    Native(NativeWindow),
    Aggregate(AggregateCapability),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NativeWindow {
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
