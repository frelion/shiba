use core::fmt;

use postgres::{Client, Transaction};
use shiba_compiler::{
    CompilerError, OperatorSpecV1, SourceColumnDescriptor, SourceDescriptor, compile_operator,
};
use shiba_operator::{CompiledOperator, CompiledOperatorKind, ObjectAddress};

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

impl std::error::Error for RegistrationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Compiler(error) => Some(error),
            Self::Runtime(error) => Some(error),
        }
    }
}

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

/// Compiles and atomically installs one operator definition and its zero state.
///
/// # Errors
/// Fails closed if the source is missing/invalidated, compilation fails, or any
/// definition, state, or result write fails.
pub fn compile_and_register(
    client: &mut Client,
    spec: &OperatorSpecV1,
) -> Result<CompiledOperator, RegistrationError> {
    let mut transaction = client.transaction()?;
    let operator = register_in_transaction(&mut transaction, spec)?;
    transaction.commit()?;
    Ok(operator)
}

fn register_in_transaction(
    transaction: &mut Transaction<'_>,
    spec: &OperatorSpecV1,
) -> Result<CompiledOperator, RegistrationError> {
    let source_id = as_bigint("source_id", spec.source_id.get())?;
    source_preflight::lock_binding(transaction, source_id)?;
    source_preflight::validate(transaction, source_id)?;
    let descriptor = source_descriptor(transaction, spec)?;
    let operator = compile_operator(spec, &descriptor)?;
    let operator_id = as_bigint("operator_id", operator.operator_id.get())?;
    let (kind, input) = match &operator.kind {
        CompiledOperatorKind::CountRows => ("count_rows", None),
        CompiledOperatorKind::SumInt8 { input } => ("sum_int8", Some(*input)),
    };
    let input_classid = input.map(|address| i64::from(address.class_id));
    let input_objid = input.map(|address| i64::from(address.object_id));
    let input_objsubid = input.map(|address| address.sub_id);
    transaction.execute(
        "INSERT INTO shiba_internal.operator_definition (
             operator_id, source_id, compiler_version, operator_kind,
             input_classid, input_objid, input_objsubid
         ) VALUES ($1, $2, 1, $3, $4::bigint::oid, $5::bigint::oid, $6)",
        &[
            &operator_id,
            &source_id,
            &kind,
            &input_classid,
            &input_objid,
            &input_objsubid,
        ],
    )?;
    transaction.execute(
        "INSERT INTO shiba_internal.operator_state (operator_id, value_bigint)
         VALUES ($1, 0)",
        &[&operator_id],
    )?;
    transaction.execute(
        "INSERT INTO shiba.operator_result (operator_id, operator_kind, value_bigint)
         VALUES ($1, $2, 0)",
        &[&operator_id, &kind],
    )?;
    Ok(operator)
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
    let relation = object_address(&relation, 0, 1, 2)?;
    let rows = transaction.query(
        "SELECT attribute.attname,
                binding.address_classid::bigint,
                binding.address_objid::bigint,
                binding.address_objsubid,
                attribute.atttypid::bigint,
                NOT attribute.attnotnull
         FROM shiba_internal.source_binding AS binding
         JOIN pg_catalog.pg_attribute AS attribute
           ON attribute.attrelid = binding.address_objid
          AND attribute.attnum = binding.address_objsubid
         WHERE binding.source_id = $1 AND binding.binding_kind = 'column'
         ORDER BY binding.address_objsubid",
        &[&source_id],
    )?;
    let mut columns = Vec::with_capacity(rows.len());
    for row in rows {
        columns.push(SourceColumnDescriptor {
            name: row.get(0),
            address: object_address(&row, 1, 2, 3)?,
            type_oid: u32::try_from(row.get::<_, i64>(4))
                .map_err(|_| M2Error::InvalidOperatorDefinition)?,
            nullable: row.get(5),
        });
    }
    Ok(SourceDescriptor {
        source_id: spec.source_id,
        relation,
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
