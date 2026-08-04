use core::fmt;

use crate::{
    DeltaBatch, GraphEffectOrigin, GraphTransition, MultiInputBatch, OperatorGraph, ResultDelta,
    StateReadSet, StateSnapshot,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KernelError {
    InvalidPlan,
    InvalidState,
    OutputContractMismatch,
    NegativeCount,
    Underflow,
    Overflow,
    AbsentInput,
    WrongType,
    InvalidGraph,
    InvalidTransition,
}

impl fmt::Display for KernelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "operator kernel rejected transition: {self:?}")
    }
}

impl std::error::Error for KernelError {}

/// Returns the exact generic state keys and partitions needed by one graph batch.
///
/// # Errors
///
/// Rejects graph, origin, layout, type, state-key, or bound violations.
pub fn graph_state_read_set(
    graph: &OperatorGraph,
    batch: &MultiInputBatch,
) -> Result<StateReadSet, KernelError> {
    graph.validate().map_err(|_| KernelError::InvalidGraph)?;
    let mut budget = crate::graph_budget::GraphBudget::new();
    if graph
        .nodes
        .iter()
        .any(|node| matches!(node.kind, crate::OperatorNodeKind::InnerJoin { .. }))
    {
        let read_set =
            crate::join::state_read_set(graph, batch)?.ok_or(KernelError::InvalidGraph)?;
        budget
            .charge_read_set(&read_set)
            .map_err(|_| KernelError::InvalidTransition)?;
        return Ok(read_set);
    }
    let input = singleton_batch(graph, batch)?;
    crate::aggregate::state_read_set(graph, input, &mut budget)
}

/// Applies one canonical graph against its exact generic state snapshot.
///
/// # Errors
///
/// Rejects corrupt graph/state, invalid input, arithmetic, output amplification,
/// duplicate result/state identities, or any nondeterministic transition shape.
pub fn apply_graph_plan(
    graph: &OperatorGraph,
    snapshot: &StateSnapshot,
    batch: &MultiInputBatch,
) -> Result<GraphTransition, KernelError> {
    graph.validate().map_err(|_| KernelError::InvalidGraph)?;
    let read_set = graph_state_read_set(graph, batch)?;
    snapshot
        .validate_exact(&read_set)
        .map_err(|_| KernelError::InvalidState)?;
    if let Some(transition) = crate::join::apply(graph, snapshot, batch)? {
        validate_transition_budget(&transition)?;
        return Ok(transition);
    }
    let input = singleton_batch(graph, batch)?;
    let mut budget = crate::graph_budget::GraphBudget::new();
    let mut transition = crate::graph_eval::apply_graph_with_budget(graph, input, &mut budget)
        .map_err(|_| KernelError::InvalidGraph)?;
    let aggregate = crate::aggregate::apply(graph, snapshot, input, &mut budget)?;
    transition.state_deltas.extend(aggregate.state_deltas);
    transition.results.extend(aggregate.results);
    transition
        .state_deltas
        .sort_by(|left, right| left.key.cmp(&right.key));
    transition.results.sort_by_key(result_node_id);
    if transition
        .state_deltas
        .windows(2)
        .any(|pair| pair[0].key == pair[1].key)
        || transition
            .results
            .windows(2)
            .any(|pair| result_node_id(&pair[0]) == result_node_id(&pair[1]))
    {
        return Err(KernelError::InvalidTransition);
    }
    budget
        .charge_transition(&transition)
        .map_err(|_| KernelError::InvalidTransition)?;
    Ok(transition)
}

/// Validates graph-wide state/result/work limits before Runtime persistence.
pub fn validate_transition_budget(transition: &GraphTransition) -> Result<(), KernelError> {
    let mut budget = crate::graph_budget::GraphBudget::new();
    budget
        .charge_transition(transition)
        .map_err(|_| KernelError::InvalidTransition)
}

fn singleton_batch<'a>(
    graph: &OperatorGraph,
    batch: &'a MultiInputBatch,
) -> Result<&'a DeltaBatch, KernelError> {
    if graph.sources.len() != 1
        || batch.sources.len() != 1
        || batch.sources[0].source_id != graph.sources[0].source_id
    {
        return Err(KernelError::InvalidGraph);
    }
    let source = &batch.sources[0];
    let valid_origin = match (batch.origin, source.delta.origin) {
        (GraphEffectOrigin::Wal(graph_tx), crate::EffectOrigin::Wal(source_tx)) => {
            graph_tx.graph_id == graph.graph_id
                && source_tx.source_id == source.source_id
                && graph_tx.slot_generation == source_tx.slot_generation
                && graph_tx.commit_lsn == source_tx.commit_lsn
                && graph_tx.ingress_transaction_id == source_tx.ingress_transaction_id
        }
        (
            GraphEffectOrigin::Bootstrap(graph_batch),
            crate::EffectOrigin::Bootstrap(source_batch),
        ) => graph_batch == source_batch,
        _ => false,
    };
    let layout = crate::source_typed_layout(source.source_id, &graph.sources[0].layout)
        .map_err(|_| KernelError::InvalidGraph)?;
    if !valid_origin || source.delta.layout_identity != layout.identity {
        return Err(KernelError::InvalidGraph);
    }
    Ok(&source.delta)
}

fn result_node_id(result: &ResultDelta) -> crate::NodeId {
    result.node_id
}
