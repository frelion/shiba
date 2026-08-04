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
    let (source_layout, layouts) = validated_layouts(graph, input)?;
    let mut budget = EvaluationBudget::new(input)?;
    let mut batches = BTreeMap::<NodeId, DeltaBatch>::new();
    let mut results = Vec::new();
    let mut emitted_rows = 0_usize;
    for node in &graph.nodes {
        if matches!(
            node.kind,
            OperatorNodeKind::CountRows
                | OperatorNodeKind::SumInt8 { .. }
                | OperatorNodeKind::GroupedCount { .. }
                | OperatorNodeKind::GroupedSumInt8 { .. }
                | OperatorNodeKind::InnerJoin { .. }
        ) {
            continue;
        }
        if matches!(node.kind, OperatorNodeKind::Materialize { .. })
            && matches!(node.input, NodeInput::Node(id) if !batches.contains_key(&id))
        {
            continue;
        }
        let (batch, layout) = match node.input {
            NodeInput::SourcePort(source_id)
                if graph.sources.len() == 1 && graph.sources[0].source_id == source_id =>
            {
                (input, &source_layout)
            }
            NodeInput::SourcePort(_) => return Err(GraphError::InvalidTopology),
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
            OperatorNodeKind::CountRows
            | OperatorNodeKind::SumInt8 { .. }
            | OperatorNodeKind::GroupedCount { .. }
            | OperatorNodeKind::GroupedSumInt8 { .. }
            | OperatorNodeKind::InnerJoin { .. } => unreachable!(),
            OperatorNodeKind::Materialize {
                field_slots,
                output,
            } => {
                if output.schema.is_scalar() {
                    return Err(GraphError::WrongType);
                }
                results.push(crate::materialize::materialize(
                    node.node_id,
                    batch,
                    layout,
                    field_slots,
                    output,
                )?);
            }
        }
    }
    Ok(GraphTransition {
        state_deltas: Vec::new(),
        results,
    })
}

fn validated_layouts(
    graph: &OperatorGraph,
    input: &DeltaBatch,
) -> Result<(TypedLayout, BTreeMap<NodeId, TypedLayout>), GraphError> {
    graph.validate()?;
    let layouts = graph.layouts()?;
    if input.layout_identity != layouts.0.identity {
        return Err(GraphError::Layout);
    }
    if input.rows.len() > MAX_INPUT_DELTA_ROWS {
        return Err(GraphError::OutputLimit);
    }
    validate_batch(input, &layouts.0)?;
    Ok(layouts)
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
