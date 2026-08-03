use std::collections::BTreeMap;

use crate::{
    DeltaBatch, GraphError, GraphTransition, NodeId, NodeInput, OperatorGraph, OperatorNodeKind,
    RowDelta, TypedLayout, TypedRow, TypedValue, ValueType,
    graph::{
        MAX_GRAPH_DELTA_ROWS, MAX_GRAPH_WORK_BYTES, MAX_INPUT_DELTA_ROWS, MAX_NODE_DELTA_ROWS,
    },
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
    let mut work_bytes = batch_bytes(input)?;
    let mut batches = BTreeMap::<NodeId, DeltaBatch>::new();
    let states = Vec::new();
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
                charge_rows(&mut emitted_rows, output.rows.len())?;
                charge_bytes(&mut work_bytes, batch_bytes(&output)?)?;
                batches.insert(node.node_id, output);
            }
            OperatorNodeKind::Project { expressions } => {
                let output_layout = layouts.get(&node.node_id).ok_or(GraphError::Layout)?;
                let output = map_batch(batch, layout, output_layout, expressions, false)?;
                charge_rows(&mut emitted_rows, output.rows.len())?;
                charge_bytes(&mut work_bytes, batch_bytes(&output)?)?;
                batches.insert(node.node_id, output);
            }
            OperatorNodeKind::Compute { expressions } => {
                let output_layout = layouts.get(&node.node_id).ok_or(GraphError::Layout)?;
                let output = map_batch(batch, layout, output_layout, expressions, true)?;
                charge_rows(&mut emitted_rows, output.rows.len())?;
                charge_bytes(&mut work_bytes, batch_bytes(&output)?)?;
                batches.insert(node.node_id, output);
            }
            OperatorNodeKind::Materialize {
                key_slot,
                value_slot,
                ..
            } => results.push(crate::materialize::materialize(
                node.node_id,
                batch,
                layout,
                *key_slot,
                *value_slot,
            )?),
        }
    }
    Ok(GraphTransition { states, results })
}

fn charge_bytes(total: &mut usize, bytes: usize) -> Result<(), GraphError> {
    *total = total.checked_add(bytes).ok_or(GraphError::OutputLimit)?;
    if *total > MAX_GRAPH_WORK_BYTES {
        return Err(GraphError::OutputLimit);
    }
    Ok(())
}

fn batch_bytes(batch: &DeltaBatch) -> Result<usize, GraphError> {
    let mut bytes = 64_usize;
    for delta in &batch.rows {
        for row in [delta.before.as_ref(), delta.after.as_ref()]
            .into_iter()
            .flatten()
        {
            bytes = bytes.checked_add(32).ok_or(GraphError::OutputLimit)?;
            for value in &row.values {
                let value_bytes = match value {
                    TypedValue::Text(text) => 16_usize
                        .checked_add(text.len())
                        .ok_or(GraphError::OutputLimit)?,
                    _ => 16,
                };
                bytes = bytes
                    .checked_add(value_bytes)
                    .ok_or(GraphError::OutputLimit)?;
            }
        }
    }
    if bytes > MAX_GRAPH_WORK_BYTES {
        return Err(GraphError::OutputLimit);
    }
    Ok(bytes)
}

fn charge_rows(total: &mut usize, rows: usize) -> Result<(), GraphError> {
    if rows > MAX_NODE_DELTA_ROWS {
        return Err(GraphError::OutputLimit);
    }
    *total = total.checked_add(rows).ok_or(GraphError::OutputLimit)?;
    if *total > MAX_GRAPH_DELTA_ROWS {
        return Err(GraphError::OutputLimit);
    }
    Ok(())
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
