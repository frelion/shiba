use core::num::NonZeroU64;
use std::collections::HashSet;

use postgres::GenericClient;
use shiba_operator::{CompiledPlan, ObjectAddress, OperatorId, OutputContract, initial_state};
use shiba_protocol::SourceId;

use crate::IngressError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PlanFingerprint {
    operator_id: OperatorId,
    digest: [u8; 32],
}

pub(crate) fn load_plan_fingerprints(
    client: &mut impl GenericClient,
    source_id: SourceId,
) -> Result<Vec<PlanFingerprint>, IngressError> {
    load_plans(client, source_id, None, false)
}

pub(crate) fn validate_prepared_plan_set(
    client: &mut impl GenericClient,
    source_id: SourceId,
    expected: &[PlanFingerprint],
) -> Result<(), IngressError> {
    let actual = load_plans(client, source_id, Some("building"), true)?;
    if actual == expected {
        Ok(())
    } else {
        Err(IngressError::Governance(
            "prepared operator plan set drifted",
        ))
    }
}

fn load_plans(
    client: &mut impl GenericClient,
    source_id: SourceId,
    expected_status: Option<&str>,
    require_initial: bool,
) -> Result<Vec<PlanFingerprint>, IngressError> {
    let source_key = i64::try_from(source_id.get())
        .map_err(|_| IngressError::Governance("source ID exceeds bigint"))?;
    let inputs = load_bound_inputs(client, source_key)?;
    let rows = client.query(
        "SELECT definition.operator_id, definition.compiler_version,
                definition.plan_format_version, definition.plan_payload,
                definition.plan_digest, definition.state_codec_version,
                definition.output_shape, definition.output_value_type,
                definition.output_key_type, definition.output_value_nullable,
                state.codec_version, state.state_payload,
                result.output_shape, result.result_status, result.value_bigint
         FROM shiba_internal.operator_definition AS definition
         JOIN shiba_internal.operator_state AS state USING (operator_id)
         JOIN shiba.operator_result AS result USING (operator_id)
         WHERE definition.source_id = $1 ORDER BY definition.operator_id",
        &[&source_key],
    )?;
    let definition_count: i64 = client
        .query_one(
            "SELECT count(*) FROM shiba_internal.operator_definition WHERE source_id = $1",
            &[&source_key],
        )?
        .get(0);
    if rows.is_empty() || usize::try_from(definition_count).ok() != Some(rows.len()) {
        return Err(IngressError::Governance("operator plan set is incomplete"));
    }
    let mut fingerprints = Vec::with_capacity(rows.len());
    for row in rows {
        let raw_operator: i64 = row.get(0);
        let operator_id = u64::try_from(raw_operator)
            .ok()
            .and_then(NonZeroU64::new)
            .map(OperatorId::new)
            .ok_or(IngressError::Governance("operator identity is invalid"))?;
        let digest = exact_digest(row.get(4))?;
        let plan = CompiledPlan::from_canonical_payload(row.get(3), digest)
            .map_err(|_| IngressError::Governance("compiled operator plan is invalid"))?;
        if row.get::<_, i32>(1) != 1
            || u32::try_from(row.get::<_, i32>(2)).ok() != Some(plan.format_version)
            || plan.operator_id != operator_id
            || plan.source_id != source_id
            || plan
                .inputs
                .iter()
                .any(|input| !inputs.contains(&input.address))
            || u32::try_from(row.get::<_, i32>(5)).ok() != Some(plan.state_contract.codec_version)
            || row.get::<_, i32>(10) != row.get::<_, i32>(5)
            || !output_metadata_matches(&plan.output_contract, &row)
        {
            return Err(IngressError::Governance("operator plan authority drifted"));
        }
        if let Some(status) = expected_status
            && row.get::<_, &str>(13) != status
        {
            return Err(IngressError::Governance(
                "operator result visibility drifted",
            ));
        }
        if require_initial {
            let initial = initial_state(&plan)
                .map_err(|_| IngressError::Governance("operator initial state is invalid"))?;
            if row.get::<_, Vec<u8>>(11) != initial.payload
                || row.get::<_, Option<i64>>(14).is_some()
            {
                return Err(IngressError::Governance(
                    "operator rebuild state is not pristine",
                ));
            }
        }
        fingerprints.push(PlanFingerprint {
            operator_id,
            digest,
        });
    }
    if require_initial {
        let keyed_rows: i64 = client
            .query_one(
                "SELECT count(*) FROM shiba_internal.operator_result_row AS result
                 JOIN shiba_internal.operator_definition AS definition USING (operator_id)
                 WHERE definition.source_id = $1",
                &[&source_key],
            )?
            .get(0);
        if keyed_rows != 0 {
            return Err(IngressError::Governance(
                "keyed rebuild result is not pristine",
            ));
        }
    }
    Ok(fingerprints)
}

fn load_bound_inputs(
    client: &mut impl GenericClient,
    source_id: i64,
) -> Result<HashSet<ObjectAddress>, IngressError> {
    client
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
                    .map_err(|_| IngressError::Governance("input class ID is invalid"))?,
                object_id: u32::try_from(row.get::<_, i64>(1))
                    .map_err(|_| IngressError::Governance("input object ID is invalid"))?,
                sub_id: row.get(2),
            })
        })
        .collect()
}

fn output_metadata_matches(contract: &OutputContract, row: &postgres::Row) -> bool {
    let shape: &str = row.get(6);
    let value_type: &str = row.get(7);
    let key_type: Option<&str> = row.get(8);
    let nullable: bool = row.get(9);
    shape == row.get::<_, &str>(12)
        && value_type == "int8"
        && match contract {
            OutputContract::Scalar { .. } => shape == "scalar" && key_type.is_none() && !nullable,
            OutputContract::KeyedRows { nullable: plan, .. } => {
                shape == "keyed" && key_type == Some("int8") && nullable == *plan
            }
        }
}

fn exact_digest(value: Vec<u8>) -> Result<[u8; 32], IngressError> {
    value
        .try_into()
        .map_err(|_| IngressError::Governance("operator plan digest is invalid"))
}
