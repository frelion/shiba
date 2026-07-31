use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DistinctPhase {
    Apply,
    Drain,
    Frontier,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DistinctContinuation {
    pub(crate) input: InputPosition,
    pub(crate) phase: DistinctPhase,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct OccupancyDiff {
    /// Typed keys whose signed occupancy changed in the admitted prefix.
    pub(crate) touched_keys: u64,
    /// Durable external effects produced or queued by the primitive.
    pub(crate) external_effects: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DistinctAction {
    ApplyPrefix { input: InputPosition },
    DrainEffects { input: InputPosition },
    ForwardFrontier { input: InputPosition },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct AppliedPrefix {
    pub(crate) facts: PrimitiveFacts,
    pub(crate) occupancy: OccupancyDiff,
    /// `Some` resumes the same immutable chunk. `None` means the primitive
    /// atomically advanced the input consumer past the completed chunk.
    pub(crate) next: Option<DistinctContinuation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DistinctActionResult {
    Applied(AppliedPrefix),
    Drained(AppliedPrefix),
    FrontierForwarded(PrimitiveFacts),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DistinctTransition {
    Committed {
        continuation: Option<DistinctContinuation>,
        facts: PrimitiveFacts,
    },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct DistinctMachine;

impl DistinctMachine {
    pub(crate) fn action(
        self,
        continuation: DistinctContinuation,
    ) -> Result<DistinctAction, String> {
        validate_input(continuation.input)?;
        if continuation.phase == DistinctPhase::Frontier && continuation.input.row_ordinal != 0 {
            return Err("Distinct frontier has a data-row position".into());
        }
        Ok(match continuation.phase {
            DistinctPhase::Apply => DistinctAction::ApplyPrefix {
                input: continuation.input,
            },
            DistinctPhase::Drain => DistinctAction::DrainEffects {
                input: continuation.input,
            },
            DistinctPhase::Frontier => DistinctAction::ForwardFrontier {
                input: continuation.input,
            },
        })
    }

    /// Facts are applied only after their database transaction commits. A
    /// crash before commit leaves `continuation` authoritative and therefore
    /// proposes precisely the same action on restart.
    pub(crate) fn apply(
        self,
        continuation: DistinctContinuation,
        result: DistinctActionResult,
        budget: WorkBudget,
    ) -> Result<DistinctTransition, String> {
        let expected = self.action(continuation)?;
        match (expected, result) {
            (DistinctAction::ApplyPrefix { input }, DistinctActionResult::Applied(page)) => {
                self.apply_prefix(input, page, budget, true)
            }
            (DistinctAction::DrainEffects { input }, DistinctActionResult::Drained(page)) => {
                self.apply_prefix(input, page, budget, false)
            }
            (
                DistinctAction::ForwardFrontier { .. },
                DistinctActionResult::FrontierForwarded(facts),
            ) => self.apply_frontier(facts, budget),
            _ => Err("database returned facts for another Distinct phase".into()),
        }
    }

    fn apply_prefix(
        self,
        input: InputPosition,
        page: AppliedPrefix,
        budget: WorkBudget,
        consumes_input: bool,
    ) -> Result<DistinctTransition, String> {
        if consumes_input && page.facts.usage.input_rows == 0 {
            return Err("Distinct prefix made no bounded input progress".into());
        }
        if !consumes_input
            && (page.facts.usage.input_rows != 0 || page.facts.usage.input_bytes != 0)
        {
            return Err("Distinct drain consumed input".into());
        }
        if consumes_input && page.occupancy.touched_keys > page.facts.usage.input_rows {
            return Err("Distinct typed occupancy summary is inconsistent".into());
        }
        if consumes_input
            && (page.facts.usage.output_rows != 0
                || !matches!(page.facts.output, OutputFacts::None)
                || (page.occupancy.external_effects > 0
                    && !matches!(
                        page.next,
                        Some(DistinctContinuation {
                            phase: DistinctPhase::Drain,
                            ..
                        })
                    ))
                || (page.occupancy.external_effects == 0
                    && matches!(
                        page.next,
                        Some(DistinctContinuation {
                            phase: DistinctPhase::Drain,
                            ..
                        })
                    )))
        {
            return Err("Distinct Apply/Drain handoff is inconsistent".into());
        }
        if !consumes_input && page.occupancy.external_effects != page.facts.usage.output_rows {
            return Err("Distinct drain effect summary is inconsistent".into());
        }
        match page.facts.output {
            OutputFacts::None if page.facts.usage.output_rows == 0 => {}
            OutputFacts::Data { .. } if page.facts.usage.output_rows > 0 => {}
            _ => return Err("Distinct output chunk disagrees with its occupancy diff".into()),
        }
        if matches!(page.facts.output, OutputFacts::Frontier { .. }) {
            return Err("Distinct input prefix emitted a frontier".into());
        }

        let continuation = match page.next {
            Some(next) => {
                validate_input(next.input)?;
                if next.input.stream_id != input.stream_id
                    || next.input.chunk_seq != input.chunk_seq
                    || (consumes_input && next.input.row_ordinal <= input.row_ordinal)
                    || (!consumes_input && next.input.row_ordinal != input.row_ordinal)
                    || next.phase == DistinctPhase::Frontier
                {
                    return Err("Distinct continuation did not advance its input".into());
                }
                Some(next)
            }
            None => None,
        };
        page.facts.validate_protocol(
            budget,
            if consumes_input {
                KernelPhase::Admit
            } else {
                KernelPhase::Drain
            },
            if continuation.is_some() {
                KernelCompletion::Continue
            } else {
                KernelCompletion::Finished
            },
        )?;
        Ok(DistinctTransition::Committed {
            continuation,
            facts: page.facts,
        })
    }

    fn apply_frontier(
        self,
        facts: PrimitiveFacts,
        budget: WorkBudget,
    ) -> Result<DistinctTransition, String> {
        facts.validate_protocol(budget, KernelPhase::Frontier, KernelCompletion::Finished)?;
        if !matches!(facts.output, OutputFacts::Frontier { .. })
            || facts.usage.input_rows != 0
            || facts.usage.input_bytes != 0
            || facts.state_rows != 0
        {
            return Err("Distinct frontier commit is inconsistent".into());
        }
        Ok(DistinctTransition::Committed {
            continuation: None,
            facts,
        })
    }
}

pub(super) fn validate_input(input: InputPosition) -> Result<(), String> {
    if input.stream_id <= 0 || input.chunk_seq <= 0 || input.row_ordinal < 0 {
        return Err("Distinct input position is invalid".into());
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct StoredContinuation {
    pub(super) value: DistinctContinuation,
    pub(super) persisted: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PrefixFacts {
    pub(super) usage: WorkUsage,
    pub(super) next_row: i64,
    pub(super) touched_keys: u64,
    pub(super) queued_effects: u64,
    pub(super) state_rows: u64,
}

pub(super) struct PrefixSql<'a> {
    pub(super) input: &'a RelationRef,
    pub(super) output_type: &'a TypeRef,
    pub(super) state: &'a RelationRef,
    pub(super) bag: &'a RelationRef,
    pub(super) queue: &'a RelationRef,
    pub(super) touched: &'a RelationRef,
    pub(super) keys: &'a [String],
    pub(super) key_orders: &'a [BtreeOrder],
    pub(super) expressions: &'a [String],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DrainFacts {
    pub(super) facts: PrimitiveFacts,
    pub(super) remaining_effects: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ReconcileFacts {
    pub(super) queued_effects: u64,
    pub(super) state_rows: u64,
}
