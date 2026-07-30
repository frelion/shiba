//! The single live PostgreSQL ABI compiler for dataflow input bindings.

use std::collections::{HashMap, HashSet};

use crate::logical::model::{DataflowPlan, DataflowStage, NamedExpr, OutputSlot, SlotType};
use crate::scalar_sql::{compile_scalar_expression, SqlBinding};

use super::{AttributeRef, StepContext, TypeRef};

pub(crate) struct BindingInput<'a> {
    pub(crate) row_type: &'a TypeRef,
    pub(crate) alias: &'a str,
}

pub(crate) fn compile_stage_bindings(
    transaction: &mut StepContext<'_, '_>,
    plan: &DataflowPlan,
    stage: &DataflowStage,
    inputs: &[BindingInput<'_>],
) -> Result<Vec<SqlBinding>, String> {
    if stage.inputs.len() != inputs.len() {
        return Err(format!(
            "stage has {} input edges but {} live payloads",
            stage.inputs.len(),
            inputs.len()
        ));
    }

    let input_slots = stage
        .schema
        .inputs
        .iter()
        .map(|slot| (slot.binding, slot))
        .collect::<HashMap<_, _>>();
    if input_slots.len() != stage.schema.inputs.len() {
        return Err("stage has duplicate input BindingIds".into());
    }

    let mut mapped = HashSet::with_capacity(stage.schema.inputs.len());
    let mut bindings = Vec::with_capacity(stage.schema.inputs.len());
    for (port, (edge, input)) in stage.inputs.iter().zip(inputs).enumerate() {
        let upstream = plan
            .stages
            .get(
                usize::try_from(edge.upstream_stage_id)
                    .map_err(|_| "upstream stage ID exceeds usize")?,
            )
            .ok_or_else(|| "stage references a missing upstream stage".to_string())?;
        let attributes = transaction.composite_attributes(input.row_type)?;
        validate_output_attributes(&attributes, &upstream.schema.outputs)?;

        for mapping in &edge.bindings {
            if !mapped.insert(mapping.target_binding) {
                return Err(format!(
                    "stage maps BindingId {} more than once",
                    mapping.target_binding.0
                ));
            }
            let input_slot = input_slots.get(&mapping.target_binding).ok_or_else(|| {
                format!("edge maps unknown BindingId {}", mapping.target_binding.0)
            })?;
            if usize::from(input_slot.input) != port {
                return Err(format!(
                    "BindingId {} belongs to another input port",
                    mapping.target_binding.0
                ));
            }
            let ordinal = upstream
                .schema
                .outputs
                .iter()
                .position(|output| output.slot == mapping.source_slot)
                .ok_or_else(|| {
                    format!(
                        "edge references missing upstream SlotId {}",
                        mapping.source_slot.0
                    )
                })?;
            let attribute = &attributes[ordinal];
            if !attribute_matches_slot(attribute, &input_slot.type_) {
                return Err(format!(
                    "BindingId {} changed PostgreSQL type",
                    mapping.target_binding.0
                ));
            }
            bindings.push(SqlBinding {
                binding_id: mapping.target_binding.0,
                input_alias: input.alias.into(),
                attribute_name: attribute.name.clone(),
            });
        }
    }
    if mapped.len() != stage.schema.inputs.len() {
        return Err("stage edges do not map every input BindingId".into());
    }
    Ok(bindings)
}

pub(crate) fn compile_named_outputs(
    outputs: &[OutputSlot],
    expressions: &[NamedExpr],
    bindings: &[SqlBinding],
    operator: &str,
) -> Result<Vec<String>, String> {
    let by_slot = expressions
        .iter()
        .map(|expression| (expression.output, expression))
        .collect::<HashMap<_, _>>();
    if by_slot.len() != expressions.len() {
        return Err(format!(
            "{operator} output expressions contain duplicate SlotIds"
        ));
    }
    if expressions.len() != outputs.len() {
        return Err(format!(
            "{operator} output expression count does not match its schema"
        ));
    }
    outputs
        .iter()
        .map(|output| {
            by_slot
                .get(&output.slot)
                .ok_or_else(|| {
                    format!(
                        "{operator} output SlotId {} has no expression",
                        output.slot.0
                    )
                })
                .and_then(|expression| compile_scalar_expression(&expression.expr, bindings))
        })
        .collect()
}

pub(crate) fn validate_output_attributes(
    attributes: &[AttributeRef],
    outputs: &[OutputSlot],
) -> Result<(), String> {
    if attributes.len() != outputs.len() {
        return Err("typed payload attribute count does not match its stage schema".into());
    }
    for (attribute, output) in attributes.iter().zip(outputs) {
        if !attribute_matches_slot(attribute, &output.type_) {
            return Err(format!(
                "typed payload for SlotId {} changed PostgreSQL type",
                output.slot.0
            ));
        }
    }
    Ok(())
}

pub(crate) fn attribute_matches_slot(attribute: &AttributeRef, slot: &SlotType) -> bool {
    attribute.type_oid.to_u32() == slot.type_oid
        && attribute.typmod == slot.typmod
        && attribute.collation_oid.to_u32() == slot.collation_oid
}
