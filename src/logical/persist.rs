//! Registration-time persistence bridge for versioned physical plans.

use pgrx::datum::{DatumWithOid, JsonB};
use pgrx::prelude::*;
use serde_json::{json, Value};

use super::model::LogicalPlan;
use super::physical::{PhysicalKernel, StageStorage, PHYSICAL_PLAN_VERSION};

#[pg_extern]
pub fn compile_physical_plan(result_oid: pg_sys::Oid) -> JsonB {
    let argument = unsafe { [DatumWithOid::new(result_oid, pg_sys::OIDOID)] };
    let serialized = Spi::get_one_with_args::<String>(
        "SELECT logical_plan::text
         FROM shiba_internal.stream_graphs
         WHERE result_oid=$1::oid",
        &argument,
    )
    .expect("Shiba could not read its persisted logical plan")
    .unwrap_or_else(|| error!("Shiba result OID {result_oid} has no logical plan"));
    let logical: LogicalPlan = serde_json::from_str(&serialized)
        .unwrap_or_else(|error| error!("Shiba logical plan is invalid: {error}"));
    let physical = logical
        .validate_for(result_oid.to_u32())
        .unwrap_or_else(|error| error!("Shiba could not compile its physical plan: {error}"));

    let left_row_type = relation_row_type(physical.descriptor.left_source_oid);
    let right_row_type = physical.descriptor.right_source_oid.map(relation_row_type);
    let mut stage_specs = Vec::new();
    for stage in &physical.stages {
        if stage.storage != StageStorage::Unlogged {
            continue;
        }
        let (stage_name, schema) = match stage.kernel {
            PhysicalKernel::Source => {
                let node_id = stage
                    .node_ids
                    .first()
                    .map(String::as_str)
                    .unwrap_or_else(|| error!("Shiba source Stage has no logical node"));
                let (stage_name, row_type) = match node_id {
                    "scan_left" => ("left_input_delta", left_row_type),
                    "scan_right" => (
                        "right_input_delta",
                        right_row_type.unwrap_or_else(|| {
                            error!("Shiba right input Stage has no right source type")
                        }),
                    ),
                    unexpected => error!("unsupported materialized source Stage {unexpected}"),
                };
                (
                    stage_name,
                    vec![
                        column("commit_lsn", pg_sys::PG_LSNOID, false),
                        column("sequence", pg_sys::INT8OID, false),
                        column("weight", pg_sys::INT8OID, false),
                        column("row_data", row_type, false),
                    ],
                )
            }
            PhysicalKernel::Join => (
                "join_delta",
                vec![
                    column("commit_lsn", pg_sys::PG_LSNOID, false),
                    column("sequence", pg_sys::INT8OID, false),
                    column("weight", pg_sys::INT8OID, false),
                    column("left_row", left_row_type, true),
                    column(
                        "right_row",
                        right_row_type
                            .unwrap_or_else(|| error!("Shiba JOIN Stage has no right source type")),
                        true,
                    ),
                ],
            ),
            unexpected => error!(
                "unsupported UNLOGGED Stage kernel {unexpected:?}; add a typed schema lowering"
            ),
        };
        stage_specs.push(json!({
            "stage_id": stage.stage_id,
            "stage_name": stage_name,
            "storage": "unlogged",
            "schema": schema,
            "indexes": []
        }));
    }

    JsonB(json!({
        "version": PHYSICAL_PLAN_VERSION,
        "plan": serde_json::to_value(&physical)
            .expect("Shiba physical plan is not serializable"),
        "stages": stage_specs
    }))
}

fn relation_row_type(relation_oid: u32) -> pg_sys::Oid {
    let relation_oid = pg_sys::Oid::from(relation_oid);
    let argument = unsafe { [DatumWithOid::new(relation_oid, pg_sys::OIDOID)] };
    Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT reltype FROM pg_class WHERE oid=$1::oid AND relkind IN ('r','p')",
        &argument,
    )
    .expect("Shiba could not inspect a source composite type")
    .unwrap_or_else(|| error!("Shiba source OID {relation_oid} has no table row type"))
}

fn column(name: &str, type_oid: pg_sys::Oid, nullable: bool) -> Value {
    json!({
        "name": name,
        "type_oid": type_oid.to_u32(),
        "typmod": -1,
        "collation_oid": 0,
        "nullable": nullable
    })
}
