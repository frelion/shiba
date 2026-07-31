use super::*;

/// What follows one complete drain of the durable dirty-group queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AfterDrain {
    /// Resume a partially consumed immutable input chunk.
    Apply(InputPosition),
    /// The completed input chunk was already released by Apply.
    Idle,
    /// Forward this frontier only after every dirty group is published.
    Frontier(InputPosition),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EmitLeg {
    /// Compare the published typed output with the newly materialized result.
    /// A replacement retracts the old row and persists the pending new row in
    /// this transaction.
    Decide,
    /// Emit the already persisted typed pending row. This is deliberately a
    /// separate transaction so a one-row output budget is sufficient.
    InsertPending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AggregatePhase {
    Apply,
    DrainRebuild {
        group_queue_id: i64,
        aggregate_ordinal: u32,
        after: AfterDrain,
    },
    DrainEmit {
        group_queue_id: i64,
        leg: EmitLeg,
        after: AfterDrain,
    },
    Frontier,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AggregateContinuation {
    pub(crate) input_stream_id: i64,
    pub(crate) input: Option<InputPosition>,
    pub(crate) phase: AggregatePhase,
}

/// A plan-local state machine. `aggregate_count` is immutable plan metadata,
/// not continuation state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AggregateMachine {
    pub(super) aggregate_count: u32,
}

impl AggregateMachine {
    pub(crate) fn new(aggregate_count: u32) -> Result<Self, String> {
        if aggregate_count == 0 {
            return Err("Aggregate must contain at least one aggregate expression".into());
        }
        Ok(Self { aggregate_count })
    }

    pub(crate) fn action(
        self,
        continuation: AggregateContinuation,
    ) -> Result<AggregateAction, String> {
        self.validate_continuation(continuation)?;
        Ok(match continuation.phase {
            AggregatePhase::Apply => AggregateAction::Apply {
                input: continuation.input.expect("validated Aggregate Apply input"),
            },
            AggregatePhase::DrainRebuild {
                group_queue_id,
                aggregate_ordinal,
                ..
            } => AggregateAction::DrainRebuild {
                group_queue_id,
                aggregate_ordinal,
            },
            AggregatePhase::DrainEmit {
                group_queue_id,
                leg: EmitLeg::Decide,
                ..
            } => AggregateAction::PrepareOutput { group_queue_id },
            AggregatePhase::DrainEmit {
                group_queue_id,
                leg: EmitLeg::InsertPending,
                ..
            } => AggregateAction::EmitPending { group_queue_id },
            AggregatePhase::Frontier => AggregateAction::ForwardFrontier {
                input: continuation
                    .input
                    .expect("validated Aggregate frontier input"),
            },
        })
    }

    /// Apply facts from one transaction. Until that transaction commits,
    /// callers must retain `continuation`; replay therefore proposes the same
    /// action. Only the returned continuation is valid after commit.
    pub(crate) fn apply(
        self,
        continuation: AggregateContinuation,
        result: AggregateActionResult,
        budget: WorkBudget,
    ) -> Result<AggregateTransition, String> {
        let expected = self.action(continuation)?;
        match (expected, result) {
            (AggregateAction::Apply { input }, AggregateActionResult::Applied(applied)) => {
                self.apply_page(input, applied, budget)
            }
            (
                AggregateAction::DrainRebuild {
                    group_queue_id,
                    aggregate_ordinal,
                },
                AggregateActionResult::Rebuilt(rebuilt),
            ) => self.apply_rebuild(
                continuation,
                group_queue_id,
                aggregate_ordinal,
                rebuilt,
                budget,
            ),
            (
                AggregateAction::PrepareOutput { group_queue_id },
                AggregateActionResult::OutputPrepared(prepared),
            ) => self.apply_prepared_output(continuation, group_queue_id, prepared, budget),
            (
                AggregateAction::EmitPending { group_queue_id },
                AggregateActionResult::PendingEmitted(emitted),
            ) => self.apply_pending_output(continuation, group_queue_id, emitted, budget),
            (
                AggregateAction::ForwardFrontier { .. },
                AggregateActionResult::Frontier(frontier),
            ) => self.apply_frontier(continuation, frontier, budget),
            _ => Err("database returned facts for another Aggregate phase".into()),
        }
    }

    fn validate_continuation(self, continuation: AggregateContinuation) -> Result<(), String> {
        if continuation.input_stream_id <= 0
            || continuation
                .input
                .is_some_and(|input| input.stream_id != continuation.input_stream_id)
        {
            return Err("Aggregate continuation has an invalid input stream".into());
        }
        match continuation.phase {
            AggregatePhase::Apply => {
                validate_input(
                    continuation
                        .input
                        .ok_or_else(|| "Aggregate Apply omitted its input cursor".to_string())?,
                )?;
            }
            AggregatePhase::Frontier => {
                let input = continuation
                    .input
                    .ok_or_else(|| "Aggregate frontier omitted its input cursor".to_string())?;
                validate_input(input)?;
                if input.row_ordinal != 0 {
                    return Err("Aggregate frontier has a data-row position".into());
                }
            }
            AggregatePhase::DrainRebuild {
                group_queue_id,
                aggregate_ordinal,
                after,
            } => {
                validate_queue_id(group_queue_id)?;
                if aggregate_ordinal == 0 || aggregate_ordinal > self.aggregate_count {
                    return Err("Aggregate ordinal is outside its plan".into());
                }
                if continuation.input.is_some() {
                    return Err("Aggregate Drain retained an input cursor".into());
                }
                validate_after_drain(continuation.input_stream_id, after)?;
            }
            AggregatePhase::DrainEmit {
                group_queue_id,
                after,
                ..
            } => {
                validate_queue_id(group_queue_id)?;
                if continuation.input.is_some() {
                    return Err("Aggregate Drain retained an input cursor".into());
                }
                validate_after_drain(continuation.input_stream_id, after)?;
            }
        }
        Ok(())
    }

    fn apply_page(
        self,
        input: InputPosition,
        applied: AppliedPage,
        budget: WorkBudget,
    ) -> Result<AggregateTransition, String> {
        applied.facts.validate(budget)?;
        validate_internal_page(applied.facts, true)?;
        if applied.facts.usage.input_rows == 0 {
            return Err("Aggregate Apply made no bounded input progress".into());
        }
        let next = match applied.target {
            ApplyTarget::Continue(next_input) => {
                validate_input(next_input)?;
                if next_input.stream_id != input.stream_id
                    || next_input.chunk_seq != input.chunk_seq
                    || next_input.row_ordinal <= input.row_ordinal
                {
                    return Err("Aggregate Apply continuation did not advance its page".into());
                }
                Some(AggregateContinuation {
                    input_stream_id: input.stream_id,
                    input: Some(next_input),
                    phase: AggregatePhase::Apply,
                })
            }
            ApplyTarget::Drain {
                first_group_queue_id,
                after,
            } => {
                validate_queue_id(first_group_queue_id)?;
                validate_after_drain(input.stream_id, after)?;
                Some(AggregateContinuation {
                    input_stream_id: input.stream_id,
                    input: None,
                    phase: AggregatePhase::DrainRebuild {
                        group_queue_id: first_group_queue_id,
                        aggregate_ordinal: 1,
                        after,
                    },
                })
            }
            ApplyTarget::Idle => None,
        };
        if let Some(next) = next {
            self.validate_continuation(next)?;
        }
        Ok(AggregateTransition::Committed {
            continuation: next,
            facts: applied.facts,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_rebuild(
        self,
        continuation: AggregateContinuation,
        group_queue_id: i64,
        aggregate_ordinal: u32,
        rebuilt: RebuiltPage,
        budget: WorkBudget,
    ) -> Result<AggregateTransition, String> {
        let page = rebuilt.page;
        page.validate(budget)?;
        rebuilt.facts.validate(budget)?;
        if rebuilt.facts.usage != page.usage
            || rebuilt.facts.output != OutputFacts::None
            || rebuilt.facts.state_rows == 0
        {
            return Err("Aggregate rebuild primitive facts are inconsistent".into());
        }
        if page.usage.output_rows != 0 || page.usage.output_bytes != 0 {
            return Err("Aggregate rebuild emitted output".into());
        }
        if !page.complete && page.usage.input_rows == 0 {
            return Err("partial Aggregate rebuild made no bounded progress".into());
        }
        if page.last_row_id.is_some_and(|row_id| row_id <= 0) {
            return Err("Aggregate rebuild returned a non-positive row cursor".into());
        }
        let AggregatePhase::DrainRebuild { after, .. } = continuation.phase else {
            unreachable!("the expected action already established the phase");
        };
        let next_phase = if !page.complete {
            AggregatePhase::DrainRebuild {
                group_queue_id,
                aggregate_ordinal,
                after,
            }
        } else if aggregate_ordinal < self.aggregate_count {
            AggregatePhase::DrainRebuild {
                group_queue_id,
                aggregate_ordinal: aggregate_ordinal + 1,
                after,
            }
        } else {
            AggregatePhase::DrainEmit {
                group_queue_id,
                leg: EmitLeg::Decide,
                after,
            }
        };
        let next = AggregateContinuation {
            input_stream_id: continuation.input_stream_id,
            input: None,
            phase: next_phase,
        };
        Ok(AggregateTransition::Committed {
            continuation: Some(next),
            facts: rebuilt.facts,
        })
    }

    fn apply_prepared_output(
        self,
        continuation: AggregateContinuation,
        group_queue_id: i64,
        prepared: PreparedOutput,
        budget: WorkBudget,
    ) -> Result<AggregateTransition, String> {
        let (facts, completed_group, replacement) = match prepared {
            PreparedOutput::Unchanged {
                facts,
                next_group_queue_id,
            } => {
                validate_no_output(facts)?;
                (facts, Some(next_group_queue_id), false)
            }
            PreparedOutput::Inserted {
                facts,
                next_group_queue_id,
            }
            | PreparedOutput::Deleted {
                facts,
                next_group_queue_id,
            } => {
                validate_one_effect(facts)?;
                (facts, Some(next_group_queue_id), false)
            }
            PreparedOutput::ReplacementRetracted { facts } => {
                validate_one_effect(facts)?;
                (facts, None, true)
            }
        };
        facts.validate(budget)?;
        let AggregatePhase::DrainEmit { after, .. } = continuation.phase else {
            unreachable!("the expected action already established the phase");
        };

        let next = if replacement {
            Some(AggregateContinuation {
                input_stream_id: continuation.input_stream_id,
                input: None,
                phase: AggregatePhase::DrainEmit {
                    group_queue_id,
                    leg: EmitLeg::InsertPending,
                    after,
                },
            })
        } else {
            self.after_completed_group(
                continuation.input_stream_id,
                group_queue_id,
                completed_group.expect("completed output carries its successor"),
                after,
            )?
        };
        Ok(AggregateTransition::Committed {
            continuation: next,
            facts,
        })
    }

    fn apply_pending_output(
        self,
        continuation: AggregateContinuation,
        group_queue_id: i64,
        emitted: PendingOutput,
        budget: WorkBudget,
    ) -> Result<AggregateTransition, String> {
        emitted.facts.validate(budget)?;
        validate_one_effect(emitted.facts)?;
        let AggregatePhase::DrainEmit { after, .. } = continuation.phase else {
            unreachable!("the expected action already established the phase");
        };
        let next = self.after_completed_group(
            continuation.input_stream_id,
            group_queue_id,
            emitted.next_group_queue_id,
            after,
        )?;
        Ok(AggregateTransition::Committed {
            continuation: next,
            facts: emitted.facts,
        })
    }

    fn apply_frontier(
        self,
        continuation: AggregateContinuation,
        frontier: FrontierResult,
        budget: WorkBudget,
    ) -> Result<AggregateTransition, String> {
        match frontier {
            FrontierResult::Forwarded { facts } => {
                facts.validate_protocol(
                    budget,
                    KernelPhase::Frontier,
                    KernelCompletion::Finished,
                )?;
                if !matches!(facts.output, OutputFacts::Frontier { .. })
                    || facts.state_rows != 0
                    || facts.usage.input_rows != 0
                    || facts.usage.input_bytes != 0
                {
                    return Err("Aggregate frontier commit is inconsistent".into());
                }
                Ok(AggregateTransition::Committed {
                    continuation: None,
                    facts,
                })
            }
            FrontierResult::GlobalGroupQueued {
                facts,
                group_queue_id,
            } => {
                facts.validate_protocol(
                    budget,
                    KernelPhase::Frontier,
                    KernelCompletion::Continue,
                )?;
                validate_internal_page(facts, false)?;
                validate_queue_id(group_queue_id)?;
                if facts.state_rows == 0 {
                    return Err("global Aggregate bootstrap omitted its continuation".into());
                }
                let frontier = continuation
                    .input
                    .ok_or_else(|| "global Aggregate bootstrap lost its frontier".to_string())?;
                let next = AggregateContinuation {
                    input_stream_id: continuation.input_stream_id,
                    input: None,
                    phase: AggregatePhase::DrainRebuild {
                        group_queue_id,
                        aggregate_ordinal: 1,
                        after: AfterDrain::Frontier(frontier),
                    },
                };
                Ok(AggregateTransition::Committed {
                    continuation: Some(next),
                    facts,
                })
            }
        }
    }

    fn after_completed_group(
        self,
        input_stream_id: i64,
        completed_queue_id: i64,
        next_group_queue_id: Option<i64>,
        after: AfterDrain,
    ) -> Result<Option<AggregateContinuation>, String> {
        if let Some(next_queue_id) = next_group_queue_id {
            validate_queue_id(next_queue_id)?;
            if next_queue_id <= completed_queue_id {
                return Err("Aggregate dirty-group queue did not advance".into());
            }
            return Ok(Some(AggregateContinuation {
                input_stream_id,
                input: None,
                phase: AggregatePhase::DrainRebuild {
                    group_queue_id: next_queue_id,
                    aggregate_ordinal: 1,
                    after,
                },
            }));
        }
        Ok(match after {
            AfterDrain::Apply(next_input) => Some(AggregateContinuation {
                input_stream_id,
                input: Some(next_input),
                phase: AggregatePhase::Apply,
            }),
            AfterDrain::Idle => None,
            AfterDrain::Frontier(frontier) => Some(AggregateContinuation {
                input_stream_id,
                input: Some(frontier),
                phase: AggregatePhase::Frontier,
            }),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AggregateAction {
    Apply {
        input: InputPosition,
    },
    DrainRebuild {
        group_queue_id: i64,
        aggregate_ordinal: u32,
    },
    PrepareOutput {
        group_queue_id: i64,
    },
    EmitPending {
        group_queue_id: i64,
    },
    ForwardFrontier {
        input: InputPosition,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ApplyTarget {
    Continue(InputPosition),
    Drain {
        first_group_queue_id: i64,
        after: AfterDrain,
    },
    Idle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AppliedPage {
    pub(crate) facts: PrimitiveFacts,
    pub(crate) target: ApplyTarget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RebuiltPage {
    pub(crate) page: PageFacts,
    pub(crate) facts: PrimitiveFacts,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedOutput {
    Unchanged {
        facts: PrimitiveFacts,
        next_group_queue_id: Option<i64>,
    },
    Inserted {
        facts: PrimitiveFacts,
        next_group_queue_id: Option<i64>,
    },
    Deleted {
        facts: PrimitiveFacts,
        next_group_queue_id: Option<i64>,
    },
    /// The transaction emitted old `-1` and persisted the typed pending row.
    /// It must not also emit new `+1`.
    ReplacementRetracted { facts: PrimitiveFacts },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PendingOutput {
    pub(crate) facts: PrimitiveFacts,
    pub(crate) next_group_queue_id: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FrontierResult {
    Forwarded {
        facts: PrimitiveFacts,
    },
    GlobalGroupQueued {
        facts: PrimitiveFacts,
        group_queue_id: i64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AggregateActionResult {
    Applied(AppliedPage),
    Rebuilt(RebuiltPage),
    OutputPrepared(PreparedOutput),
    PendingEmitted(PendingOutput),
    Frontier(FrontierResult),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AggregateTransition {
    Committed {
        continuation: Option<AggregateContinuation>,
        facts: PrimitiveFacts,
    },
}

pub(super) fn validate_queue_id(queue_id: i64) -> Result<(), String> {
    if queue_id <= 0 {
        return Err("Aggregate dirty-group queue id is not positive".into());
    }
    Ok(())
}

pub(super) fn validate_input(input: InputPosition) -> Result<(), String> {
    if input.stream_id <= 0 || input.chunk_seq <= 0 || input.row_ordinal < 0 {
        return Err("Aggregate input position is invalid".into());
    }
    Ok(())
}

pub(super) fn validate_after_drain(input_stream_id: i64, after: AfterDrain) -> Result<(), String> {
    if input_stream_id <= 0 {
        return Err("Aggregate Drain target has an invalid input stream".into());
    }
    match after {
        AfterDrain::Apply(input) => {
            validate_input(input)?;
            if input.stream_id != input_stream_id {
                return Err("Aggregate Drain target changed its input stream".into());
            }
        }
        AfterDrain::Idle => {}
        AfterDrain::Frontier(frontier) => {
            validate_input(frontier)?;
            if frontier.stream_id != input_stream_id || frontier.row_ordinal != 0 {
                return Err("Aggregate Drain has an invalid frontier target".into());
            }
        }
    }
    Ok(())
}

pub(super) fn validate_internal_page(
    facts: PrimitiveFacts,
    permits_input: bool,
) -> Result<(), String> {
    if facts.output != OutputFacts::None
        || facts.usage.output_rows != 0
        || facts.usage.output_bytes != 0
        || (!permits_input && (facts.usage.input_rows != 0 || facts.usage.input_bytes != 0))
    {
        return Err("Aggregate internal phase reported external effects".into());
    }
    Ok(())
}

pub(super) fn validate_no_output(facts: PrimitiveFacts) -> Result<(), String> {
    if facts.output != OutputFacts::None
        || facts.usage.input_rows != 0
        || facts.usage.input_bytes != 0
        || facts.usage.output_rows != 0
        || facts.usage.output_bytes != 0
    {
        return Err("unchanged Aggregate group emitted an effect".into());
    }
    Ok(())
}

pub(super) fn validate_one_effect(facts: PrimitiveFacts) -> Result<(), String> {
    if !matches!(facts.output, OutputFacts::Data { .. })
        || facts.usage.input_rows != 0
        || facts.usage.input_bytes != 0
        || facts.usage.output_rows != 1
        || facts.usage.output_bytes == 0
    {
        return Err("Aggregate emission must contain exactly one typed effect row".into());
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct StoredAggregate {
    pub(super) value: AggregateContinuation,
    pub(super) persisted: bool,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct AggregateFields {
    pub(super) phase: i16,
    pub(super) input_stream_id: i64,
    pub(super) input_chunk_seq: Option<i64>,
    pub(super) input_row_ordinal: Option<i64>,
    pub(super) group_queue_id: Option<i64>,
    pub(super) aggregate_ordinal: Option<i32>,
    pub(super) emit_leg: Option<i16>,
    pub(super) after_kind: Option<i16>,
    pub(super) after_chunk_seq: Option<i64>,
    pub(super) after_row_ordinal: Option<i64>,
}
