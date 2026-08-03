use core::num::NonZeroU64;
use std::collections::HashSet;

use postgres::{Row, Transaction};
use shiba_operator::{
    CompiledPlan, EffectBatch, EffectOrigin, EncodedOperatorState, ObjectAddress, OperatorId,
    OutputContract, OutputDelta, apply_plan,
};
use shiba_protocol::SourceId;

use crate::{M2Error, result_sink};

pub(crate) fn apply_all(
    transaction: &mut Transaction<'_>,
    source_id: i64,
    batch: &EffectBatch,
) -> Result<(), M2Error> {
    let publish = result_visibility(transaction, source_id, batch.origin)?;
    let inputs = load_input_bindings(transaction, source_id)?;
    let rows = load_locked_operators(transaction, source_id)?;
    for row in rows {
        validate_result_status(&row, if publish { "active" } else { "building" })?;
        let (operator_id, plan, state) = decode_operator(&row, source_id)?;
        validate_plan_inputs(&plan, &inputs)?;
        let transition = apply_plan(&plan, &state, &batch.effects)?;
        result_sink::persist_state(transaction, operator_id, &transition.next_state)?;
        result_sink::persist_output(
            transaction,
            operator_id,
            &plan.output_contract,
            transition.output_delta,
            batch.effects.len(),
            publish,
        )?;
    }
    Ok(())
}

pub(crate) fn activate_results(
    transaction: &mut Transaction<'_>,
    source_id: i64,
) -> Result<(), M2Error> {
    let rows = load_locked_operators(transaction, source_id)?;
    let inputs = load_input_bindings(transaction, source_id)?;
    for row in rows {
        validate_result_status(&row, "building")?;
        let (operator_id, plan, state) = decode_operator(&row, source_id)?;
        validate_plan_inputs(&plan, &inputs)?;
        let output = apply_plan(&plan, &state, &[])?.output_delta;
        match output {
            OutputDelta::ScalarReplacement { value } => {
                let value = result_sink::scalar_int8(value)?;
                result_sink::activate_header(transaction, operator_id, "scalar", Some(value))?;
            }
            OutputDelta::KeyedMutations { mutations } if mutations.is_empty() => {
                result_sink::activate_header(transaction, operator_id, "keyed", None)?;
            }
            OutputDelta::KeyedMutations { .. } => {
                return Err(M2Error::InvalidOperatorDefinition);
            }
        }
    }
    Ok(())
}

fn load_input_bindings(
    transaction: &mut Transaction<'_>,
    source_id: i64,
) -> Result<HashSet<ObjectAddress>, M2Error> {
    transaction
        .query(
            "SELECT address_classid::bigint, address_objid::bigint, address_objsubid
             FROM shiba_internal.source_binding
             WHERE source_id = $1 AND binding_kind = 'column'",
            &[&source_id],
        )?
        .into_iter()
        .map(|row| {
            Ok(ObjectAddress {
                class_id: u32::try_from(row.get::<_, i64>(0))
                    .map_err(|_| M2Error::InvalidOperatorDefinition)?,
                object_id: u32::try_from(row.get::<_, i64>(1))
                    .map_err(|_| M2Error::InvalidOperatorDefinition)?,
                sub_id: row.get(2),
            })
        })
        .collect()
}

fn validate_plan_inputs(
    plan: &CompiledPlan,
    bindings: &HashSet<ObjectAddress>,
) -> Result<(), M2Error> {
    if plan
        .inputs
        .iter()
        .all(|input| bindings.contains(&input.address))
    {
        Ok(())
    } else {
        Err(M2Error::InvalidOperatorDefinition)
    }
}

fn load_locked_operators(
    transaction: &mut Transaction<'_>,
    source_id: i64,
) -> Result<Vec<Row>, M2Error> {
    let rows = transaction.query(
        "SELECT definition.operator_id, definition.compiler_version,
                definition.plan_format_version, definition.plan_payload,
                definition.plan_digest, definition.state_codec_version,
                definition.output_shape, definition.output_value_type,
                definition.output_key_type, definition.output_value_nullable,
                state.codec_version, state.state_payload,
                result.output_shape, result.result_status
         FROM shiba_internal.operator_definition AS definition
         JOIN shiba_internal.operator_state AS state USING (operator_id)
         JOIN shiba.operator_result AS result USING (operator_id)
         WHERE definition.source_id = $1
         ORDER BY definition.operator_id
         FOR UPDATE OF state, result",
        &[&source_id],
    )?;
    if rows.is_empty() {
        return Err(M2Error::MissingSourceOperator);
    }
    let count: i64 = transaction
        .query_one(
            "SELECT count(*) FROM shiba_internal.operator_definition WHERE source_id = $1",
            &[&source_id],
        )?
        .get(0);
    if usize::try_from(count).ok() != Some(rows.len()) {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(rows)
}

fn decode_operator(
    row: &Row,
    expected_source_id: i64,
) -> Result<(i64, CompiledPlan, EncodedOperatorState), M2Error> {
    let operator_id: i64 = row.get(0);
    let payload: Vec<u8> = row.get(3);
    let digest = digest(row.get(4))?;
    let plan = CompiledPlan::from_canonical_payload(&payload, digest)
        .map_err(|_| M2Error::InvalidOperatorDefinition)?;
    let expected_operator = u64::try_from(operator_id)
        .ok()
        .and_then(NonZeroU64::new)
        .map(OperatorId::new)
        .ok_or(M2Error::InvalidOperatorDefinition)?;
    let expected_source = u64::try_from(expected_source_id)
        .ok()
        .and_then(|value| SourceId::new(value).ok())
        .ok_or(M2Error::InvalidOperatorDefinition)?;
    let definition_codec: i32 = row.get(5);
    let state_codec: i32 = row.get(10);
    if row.get::<_, i32>(1) != 1
        || u32::try_from(row.get::<_, i32>(2)).ok() != Some(plan.format_version)
        || plan.operator_id != expected_operator
        || plan.source_id != expected_source
        || u32::try_from(definition_codec).ok() != Some(plan.state_contract.codec_version)
        || state_codec != definition_codec
        || !metadata_matches(&plan.output_contract, row)
    {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok((
        operator_id,
        plan,
        EncodedOperatorState {
            codec_version: u32::try_from(state_codec)
                .map_err(|_| M2Error::InvalidOperatorDefinition)?,
            payload: row.get(11),
        },
    ))
}

fn metadata_matches(contract: &OutputContract, row: &Row) -> bool {
    let shape: &str = row.get(6);
    let value_type: &str = row.get(7);
    let key_type: Option<&str> = row.get(8);
    let nullable: bool = row.get(9);
    let result_shape: &str = row.get(12);
    shape == result_shape
        && value_type == "int8"
        && match contract {
            OutputContract::Scalar { .. } => shape == "scalar" && key_type.is_none() && !nullable,
            OutputContract::KeyedRows { nullable: plan, .. } => {
                shape == "keyed" && key_type == Some("int8") && nullable == *plan
            }
        }
}

fn validate_result_status(row: &Row, expected: &str) -> Result<(), M2Error> {
    if row.get::<_, &str>(13) == expected {
        Ok(())
    } else {
        Err(M2Error::InvalidOperatorDefinition)
    }
}

fn digest(value: Vec<u8>) -> Result<[u8; 32], M2Error> {
    value
        .try_into()
        .map_err(|_| M2Error::InvalidOperatorDefinition)
}

fn result_visibility(
    transaction: &mut Transaction<'_>,
    source_id: i64,
    origin: EffectOrigin,
) -> Result<bool, M2Error> {
    let expected_source = u64::try_from(source_id).map_err(|_| M2Error::InvalidBootstrapPhase)?;
    let phase = transaction
        .query_opt(
            "SELECT bootstrap_id, phase FROM shiba_internal.source_bootstrap
             WHERE source_id = $1 FOR UPDATE",
            &[&source_id],
        )?
        .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)));
    match (origin, phase) {
        (EffectOrigin::Wal(id), None) if id.source_id.get() == expected_source => Ok(true),
        (EffectOrigin::Wal(id), Some((_, phase)))
            if id.source_id.get() == expected_source && phase == "active" =>
        {
            Ok(true)
        }
        (EffectOrigin::Wal(id), Some((_, phase)))
            if id.source_id.get() == expected_source && phase == "catching_up" =>
        {
            Ok(false)
        }
        (EffectOrigin::Bootstrap(id), Some((bootstrap_id, phase)))
            if u64::try_from(bootstrap_id).ok() == Some(id.bootstrap_id.get())
                && phase == "scanning" =>
        {
            Ok(false)
        }
        _ => Err(M2Error::InvalidBootstrapPhase),
    }
}
