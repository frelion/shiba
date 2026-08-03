use core::fmt;

use postgres::{Client, Transaction};
use shiba_compiler::{
    CompilerError, OperatorSpecV1, SourceColumnDescriptor, SourceDescriptor, compile_plan,
};
use shiba_operator::{
    CompiledPlan, ObjectAddress, OutputContract, OutputDelta, ScalarValue, apply_plan,
    initial_state,
};

use crate::M2Error;
use crate::source_preflight;
use crate::transaction::as_bigint;

#[derive(Debug)]
pub enum RegistrationError {
    Compiler(CompilerError),
    Runtime(M2Error),
}

impl fmt::Display for RegistrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compiler(error) => write!(formatter, "operator compilation failed: {error}"),
            Self::Runtime(error) => write!(formatter, "operator registration failed: {error}"),
        }
    }
}

impl std::error::Error for RegistrationError {}

impl From<CompilerError> for RegistrationError {
    fn from(error: CompilerError) -> Self {
        Self::Compiler(error)
    }
}

impl From<M2Error> for RegistrationError {
    fn from(error: M2Error) -> Self {
        Self::Runtime(error)
    }
}

impl From<postgres::Error> for RegistrationError {
    fn from(error: postgres::Error) -> Self {
        Self::Runtime(M2Error::Postgres(error))
    }
}

/// Compiles and atomically installs one generic plan, state, and result sink.
///
/// # Errors
///
/// Fails closed if source validation, compilation, or any authority write fails.
pub fn compile_and_register(
    client: &mut Client,
    spec: &OperatorSpecV1,
) -> Result<CompiledPlan, RegistrationError> {
    let mut transaction = client.transaction()?;
    let plan = register_in_transaction(&mut transaction, spec)?;
    transaction.commit()?;
    Ok(plan)
}

fn register_in_transaction(
    transaction: &mut Transaction<'_>,
    spec: &OperatorSpecV1,
) -> Result<CompiledPlan, RegistrationError> {
    let source_id = as_bigint("source_id", spec.source_id.get())?;
    source_preflight::lock_binding(transaction, source_id)?;
    source_preflight::validate(transaction, source_id)?;
    let descriptor = source_descriptor(transaction, spec)?;
    let plan = compile_plan(spec, &descriptor)?;
    let state = initial_state(&plan).map_err(M2Error::from)?;
    let state_codec =
        i32::try_from(state.codec_version).map_err(|_| M2Error::InvalidOperatorDefinition)?;
    let initial = apply_plan(&plan, &state, &[]).map_err(M2Error::from)?;
    let operator_id = as_bigint("operator_id", plan.operator_id.get())?;
    let spec_payload = spec
        .to_canonical_json()
        .map_err(|_| CompilerError::PlanEncoding)?;
    let (shape, value_type, key_type, nullable) = output_metadata(&plan.output_contract);
    transaction.execute(
        "INSERT INTO shiba_internal.operator_definition (
             operator_id, source_id, compiler_version, spec_payload,
             plan_format_version, plan_payload, plan_digest, state_codec_version,
             output_shape, output_value_type, output_key_type, output_value_nullable
         ) VALUES ($1, $2, 1, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
        &[
            &operator_id,
            &source_id,
            &spec_payload,
            &i32::try_from(plan.format_version).map_err(|_| M2Error::InvalidOperatorDefinition)?,
            &plan.canonical_payload,
            &plan.digest.as_slice(),
            &state_codec,
            &shape,
            &value_type,
            &key_type,
            &nullable,
        ],
    )?;
    transaction.execute(
        "INSERT INTO shiba_internal.operator_state
             (operator_id, codec_version, state_payload) VALUES ($1, $2, $3)",
        &[&operator_id, &state_codec, &state.payload],
    )?;
    let scalar = match initial.output_delta {
        OutputDelta::ScalarReplacement {
            value: ScalarValue::Int8(value),
        } => Some(value),
        OutputDelta::KeyedMutations { ref mutations } if mutations.is_empty() => None,
        _ => return Err(M2Error::InvalidOperatorDefinition.into()),
    };
    transaction.execute(
        "INSERT INTO shiba.operator_result
             (operator_id, result_status, output_shape, value_bigint)
         VALUES ($1, 'active', $2, $3)",
        &[&operator_id, &shape, &scalar],
    )?;
    Ok(plan)
}

fn output_metadata(
    contract: &OutputContract,
) -> (&'static str, &'static str, Option<&'static str>, bool) {
    match contract {
        OutputContract::Scalar { .. } => ("scalar", "int8", None, false),
        OutputContract::KeyedRows { nullable, .. } => ("keyed", "int8", Some("int8"), *nullable),
    }
}

fn source_descriptor(
    transaction: &mut Transaction<'_>,
    spec: &OperatorSpecV1,
) -> Result<SourceDescriptor, RegistrationError> {
    let source_id = as_bigint("source_id", spec.source_id.get())?;
    let relation = transaction
        .query_opt(
            "SELECT address_classid::bigint, address_objid::bigint, address_objsubid
             FROM shiba_internal.source_binding
             WHERE source_id = $1 AND binding_kind = 'relation'",
            &[&source_id],
        )?
        .ok_or(M2Error::SourceBindingMissing)?;
    let rows = transaction.query(
        "SELECT attribute.attname, binding.address_classid::bigint,
                binding.address_objid::bigint, binding.address_objsubid,
                attribute.atttypid::bigint, NOT attribute.attnotnull
         FROM shiba_internal.source_binding AS binding
         JOIN pg_catalog.pg_attribute AS attribute
           ON attribute.attrelid = binding.address_objid
          AND attribute.attnum = binding.address_objsubid
         WHERE binding.source_id = $1 AND binding.binding_kind = 'column'
         ORDER BY binding.address_objsubid",
        &[&source_id],
    )?;
    let columns = rows
        .into_iter()
        .map(|row| {
            Ok(SourceColumnDescriptor {
                name: row.get(0),
                address: object_address(&row, 1, 2, 3)?,
                type_oid: u32::try_from(row.get::<_, i64>(4))
                    .map_err(|_| M2Error::InvalidOperatorDefinition)?,
                nullable: row.get(5),
            })
        })
        .collect::<Result<Vec<_>, RegistrationError>>()?;
    Ok(SourceDescriptor {
        source_id: spec.source_id,
        relation: object_address(&relation, 0, 1, 2)?,
        columns,
    })
}

fn object_address(
    row: &postgres::Row,
    class: usize,
    object: usize,
    sub: usize,
) -> Result<ObjectAddress, RegistrationError> {
    Ok(ObjectAddress {
        class_id: u32::try_from(row.get::<_, i64>(class))
            .map_err(|_| M2Error::InvalidOperatorDefinition)?,
        object_id: u32::try_from(row.get::<_, i64>(object))
            .map_err(|_| M2Error::InvalidOperatorDefinition)?,
        sub_id: row.get(sub),
    })
}
