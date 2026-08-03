use crate::{
    DeltaBatch, GraphError, TypedValue,
    graph::{MAX_GRAPH_DELTA_ROWS, MAX_GRAPH_WORK_BYTES, MAX_NODE_DELTA_ROWS},
};

pub(crate) struct EvaluationBudget {
    work_bytes: usize,
}

impl EvaluationBudget {
    pub(crate) fn new(input: &DeltaBatch) -> Result<Self, GraphError> {
        Ok(Self {
            work_bytes: batch_bytes(input)?,
        })
    }

    pub(crate) fn charge(
        &mut self,
        batch: &DeltaBatch,
        emitted_rows: &mut usize,
    ) -> Result<(), GraphError> {
        charge_rows(emitted_rows, batch.rows.len())?;
        charge_bytes(&mut self.work_bytes, batch_bytes(batch)?)
    }
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
