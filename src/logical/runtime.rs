//! PostgreSQL runtime bridge for validated logical plans.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use serde_json::{json, Value};

use super::model::{DeltaBatch, DeltaRow, LogicalPlan};
use super::validate::ExecutionPlan;

/// Loads and validates the persisted plan once per worker, then applies WAL
/// batches using the execution descriptor derived from that plan.
pub struct DagRuntime {
    result_oid: pg_sys::Oid,
    execution: ExecutionPlan,
    encoded_descriptor: String,
}

impl DagRuntime {
    pub fn load(result_oid: pg_sys::Oid) -> Result<Self, String> {
        let argument = unsafe { [DatumWithOid::new(result_oid, pg_sys::OIDOID)] };
        let serialized = Spi::get_one_with_args::<String>(
            "SELECT logical_plan::text FROM shiba_internal.stream_graphs WHERE result_oid = $1",
            &argument,
        )
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("result OID {result_oid} has no logical plan"))?;
        let plan: LogicalPlan = serde_json::from_str(&serialized)
            .map_err(|error| format!("invalid logical plan: {error}"))?;
        let execution = plan.validate_for(result_oid.to_u32())?;
        let encoded_descriptor = serde_json::to_string(&execution.descriptor)
            .map_err(|error| format!("execution descriptor is not serializable: {error}"))?;
        Ok(Self {
            result_oid,
            execution,
            encoded_descriptor,
        })
    }

    pub fn apply_batch(&self, batch: DeltaBatch) -> Result<(), String> {
        let encoded =
            encode_batch_events(&self.execution, self.result_oid, batch.rows)?.to_string();
        let arguments = unsafe {
            [
                DatumWithOid::new(self.result_oid, pg_sys::OIDOID),
                DatumWithOid::new(self.encoded_descriptor.as_str(), pg_sys::TEXTOID),
                DatumWithOid::new(encoded.as_str(), pg_sys::TEXTOID),
                DatumWithOid::new(batch.epoch.as_str(), pg_sys::TEXTOID),
            ]
        };
        Spi::run_with_args(
            "SELECT shiba._apply_dag_delta_batch($1, $2::jsonb, $3::jsonb, $4)",
            &arguments,
        )
        .map_err(|error| error.to_string())
    }
}

pub(super) fn encode_batch_events(
    plan: &ExecutionPlan,
    result_oid: pg_sys::Oid,
    rows: Vec<DeltaRow>,
) -> Result<Value, String> {
    if rows.is_empty() {
        return Err("DAG delta batch must not be empty".into());
    }
    let mut encoded = Vec::with_capacity(rows.len());
    for delta in rows {
        let source_oid_u32 = delta
            .input
            .parse::<u32>()
            .map_err(|_| format!("invalid DAG input OID {}", delta.input))?;
        if !plan.source_oids.contains(&source_oid_u32) {
            return Err(format!(
                "source OID {source_oid_u32} is not an input of result {result_oid}"
            ));
        }
        if !delta.row.is_object() {
            return Err("source delta row must be a JSON object".into());
        }
        let diff = i32::try_from(delta.diff)
            .map_err(|_| format!("differential weight {} exceeds int32", delta.diff))?;
        if !matches!(diff, -1 | 1) {
            return Err(format!("invalid source differential weight {diff}"));
        }
        encoded.push(json!({
            "source_oid": source_oid_u32,
            "row_data": delta.row,
            "delta": diff,
        }));
    }
    Ok(Value::Array(encoded))
}
