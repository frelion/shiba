use std::collections::BTreeMap;

use crate::graph_budget::EvaluationBudget;
use crate::{
    DeltaBatch, GraphError, GraphTransition, NodeId, NodeInput, OperatorGraph, OperatorNodeKind,
    RowDelta, TypedLayout, TypedRow, TypedValue, ValueType, graph::MAX_INPUT_DELTA_ROWS,
};

/// Evaluates one bounded typed delta through an immutable stateless graph.
///
/// # Errors
///
/// Rejects graph/layout drift, invalid typed expressions, duplicate result
/// keys, arithmetic failure, or row/result amplification beyond fixed bounds.
pub fn apply_graph(
    graph: &OperatorGraph,
    input: &DeltaBatch,
) -> Result<GraphTransition, GraphError> {
    graph.validate()?;
    let (source_layout, layouts) = graph.layouts()?;
    if input.layout_identity != source_layout.identity {
        return Err(GraphError::Layout);
    }
    if input.rows.len() > MAX_INPUT_DELTA_ROWS {
        return Err(GraphError::OutputLimit);
    }
    validate_batch(input, &source_layout)?;
    let mut budget = EvaluationBudget::new(input)?;
    let mut batches = BTreeMap::<NodeId, DeltaBatch>::new();
    let mut results = Vec::new();
    let mut emitted_rows = 0_usize;
    for node in &graph.nodes {
        let (batch, layout) = match node.input {
            NodeInput::Source => (input, &source_layout),
            NodeInput::Node(node_id) => (
                batches.get(&node_id).ok_or(GraphError::InvalidTopology)?,
                layouts.get(&node_id).ok_or(GraphError::InvalidTopology)?,
            ),
        };
        match &node.kind {
            OperatorNodeKind::Filter { predicate } => {
                let output = filter_batch(batch, layout, predicate)?;
                budget.charge(&output, &mut emitted_rows)?;
                batches.insert(node.node_id, output);
            }
            OperatorNodeKind::Project { expressions } => {
                let output_layout = layouts.get(&node.node_id).ok_or(GraphError::Layout)?;
                let output = map_batch(batch, layout, output_layout, expressions, false)?;
                budget.charge(&output, &mut emitted_rows)?;
                batches.insert(node.node_id, output);
            }
            OperatorNodeKind::Compute { expressions } => {
                let output_layout = layouts.get(&node.node_id).ok_or(GraphError::Layout)?;
                let output = map_batch(batch, layout, output_layout, expressions, true)?;
                budget.charge(&output, &mut emitted_rows)?;
                batches.insert(node.node_id, output);
            }
            OperatorNodeKind::KeyBy { key } => {
                let output_layout = layouts.get(&node.node_id).ok_or(GraphError::Layout)?;
                let output = map_batch(
                    batch,
                    layout,
                    output_layout,
                    core::slice::from_ref(key),
                    true,
                )?;
                budget.charge(&output, &mut emitted_rows)?;
                batches.insert(node.node_id, output);
            }
            OperatorNodeKind::GroupedCount { .. } | OperatorNodeKind::GroupedSumInt8 { .. } => {
                return Err(GraphError::InvalidNode);
            }
            OperatorNodeKind::Materialize {
                key_slot,
                value_slot,
                output,
            } => {
                let crate::OutputContract::KeyedRows {
                    key_nullable,
                    nullable,
                    ..
                } = output
                else {
                    return Err(GraphError::WrongType);
                };
                results.push(crate::materialize::materialize(
                    node.node_id,
                    batch,
                    layout,
                    *key_slot,
                    *value_slot,
                    *key_nullable,
                    *nullable,
                )?);
            }
        }
    }
    Ok(GraphTransition { results })
}

pub(crate) fn apply_prefix(
    graph: &OperatorGraph,
    input: &DeltaBatch,
    stop_before: NodeId,
) -> Result<(DeltaBatch, TypedLayout, EvaluationBudget, usize), GraphError> {
    graph.validate()?;
    let (source_layout, layouts) = graph.layouts()?;
    if input.layout_identity != source_layout.identity {
        return Err(GraphError::Layout);
    }
    if input.rows.len() > MAX_INPUT_DELTA_ROWS {
        return Err(GraphError::OutputLimit);
    }
    validate_batch(input, &source_layout)?;
    let mut current = input.clone();
    let mut current_layout = source_layout;
    let mut budget = EvaluationBudget::new(input)?;
    let mut emitted_rows = 0;
    for node in &graph.nodes {
        if node.node_id == stop_before {
            return Ok((current, current_layout, budget, emitted_rows));
        }
        let output_layout = layouts.get(&node.node_id).ok_or(GraphError::Layout)?;
        current = transform_node(&node.kind, &current, &current_layout, output_layout)?;
        budget.charge(&current, &mut emitted_rows)?;
        current_layout = output_layout.clone();
    }
    Err(GraphError::InvalidTopology)
}

fn validate_batch(batch: &DeltaBatch, layout: &TypedLayout) -> Result<(), GraphError> {
    for delta in &batch.rows {
        for row in [delta.before.as_ref(), delta.after.as_ref()]
            .into_iter()
            .flatten()
        {
            if row.layout_identity != layout.identity
                || row.values.len() != layout.value_types.len()
            {
                return Err(GraphError::Layout);
            }
        }
    }
    Ok(())
}

pub(crate) fn transform_node(
    kind: &OperatorNodeKind,
    batch: &DeltaBatch,
    input_layout: &TypedLayout,
    output_layout: &TypedLayout,
) -> Result<DeltaBatch, GraphError> {
    match kind {
        OperatorNodeKind::Filter { predicate } => filter_batch(batch, input_layout, predicate),
        OperatorNodeKind::Project { expressions } => {
            map_batch(batch, input_layout, output_layout, expressions, false)
        }
        OperatorNodeKind::Compute { expressions } => {
            map_batch(batch, input_layout, output_layout, expressions, true)
        }
        OperatorNodeKind::KeyBy { key } => map_batch(
            batch,
            input_layout,
            output_layout,
            core::slice::from_ref(key),
            true,
        ),
        _ => Err(GraphError::InvalidNode),
    }
}

fn filter_batch(
    batch: &DeltaBatch,
    layout: &TypedLayout,
    predicate: &crate::Expression,
) -> Result<DeltaBatch, GraphError> {
    let mut rows = Vec::with_capacity(batch.rows.len());
    for delta in &batch.rows {
        let before = filter_row(delta.before.as_ref(), layout, predicate)?;
        let after = filter_row(delta.after.as_ref(), layout, predicate)?;
        if before.is_some() || after.is_some() {
            rows.push(RowDelta { before, after });
        }
    }
    Ok(DeltaBatch {
        origin: batch.origin,
        layout_identity: layout.identity,
        rows,
    })
}

fn filter_row(
    row: Option<&TypedRow>,
    layout: &TypedLayout,
    predicate: &crate::Expression,
) -> Result<Option<TypedRow>, GraphError> {
    let Some(row) = row else {
        return Ok(None);
    };
    Ok(match predicate.evaluate(layout, row)? {
        TypedValue::Bool(true) => Some(row.clone()),
        TypedValue::Bool(false) | TypedValue::Null(ValueType::Bool) => None,
        _ => return Err(GraphError::WrongType),
    })
}

fn map_batch(
    batch: &DeltaBatch,
    input_layout: &TypedLayout,
    output_layout: &TypedLayout,
    expressions: &[crate::Expression],
    append: bool,
) -> Result<DeltaBatch, GraphError> {
    let mut rows = Vec::with_capacity(batch.rows.len());
    for delta in &batch.rows {
        rows.push(RowDelta {
            before: map_row(
                delta.before.as_ref(),
                input_layout,
                output_layout,
                expressions,
                append,
            )?,
            after: map_row(
                delta.after.as_ref(),
                input_layout,
                output_layout,
                expressions,
                append,
            )?,
        });
    }
    Ok(DeltaBatch {
        origin: batch.origin,
        layout_identity: output_layout.identity,
        rows,
    })
}

fn map_row(
    row: Option<&TypedRow>,
    input_layout: &TypedLayout,
    output_layout: &TypedLayout,
    expressions: &[crate::Expression],
    append: bool,
) -> Result<Option<TypedRow>, GraphError> {
    let Some(row) = row else {
        return Ok(None);
    };
    let mut values = if append {
        row.values.clone()
    } else {
        Vec::new()
    };
    for expression in expressions {
        values.push(expression.evaluate(input_layout, row)?);
    }
    Ok(Some(TypedRow::new(output_layout, values)?))
}
