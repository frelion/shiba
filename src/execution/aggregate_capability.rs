//! Catalog-resolved contract shared by Aggregate and aggregate Window folds.
//!
//! The catalog is authoritative for transition, initial, and final semantics.
//! Operator kernels only decide which typed rows are fed to that contract and
//! in which order.

use pgrx::prelude::*;
use pgrx::spi::SpiTupleTable;

pub(crate) const AGGREGATE_CAPABILITY_SQL: &str = r#"
WITH aggregate_function AS MATERIALIZED (
  SELECT aggregate_catalog.*,
         function_catalog.prorettype,
         ARRAY(
           SELECT unnest(function_catalog.proargtypes::oid[])
         )::oid[] AS argument_types
  FROM pg_catalog.pg_aggregate AS aggregate_catalog
  JOIN pg_catalog.pg_proc AS function_catalog
    ON function_catalog.oid=aggregate_catalog.aggfnoid
   AND function_catalog.prokind='a'
  JOIN pg_catalog.pg_namespace AS function_namespace
    ON function_namespace.oid=function_catalog.pronamespace
   AND function_namespace.nspname='pg_catalog'
  WHERE aggregate_catalog.aggfnoid=$1
    AND aggregate_catalog.aggkind='n'
    AND aggregate_catalog.aggnumdirectargs=0
    AND function_catalog.provariadic=0
    AND function_catalog.prorettype=$2
),
transition_function AS MATERIALIZED (
  SELECT transition_catalog.*,
         pg_catalog.format(
           '%I.%I',transition_namespace.nspname,transition_catalog.proname
         ) AS function_sql,
         ARRAY(
           SELECT unnest(transition_catalog.proargtypes::oid[])
         )::oid[] AS argument_types
  FROM aggregate_function
  JOIN pg_catalog.pg_proc AS transition_catalog
    ON transition_catalog.oid=aggregate_function.aggtransfn
  JOIN pg_catalog.pg_namespace AS transition_namespace
    ON transition_namespace.oid=transition_catalog.pronamespace
   AND transition_namespace.nspname='pg_catalog'
  WHERE transition_catalog.prokind='f'
    AND transition_catalog.provolatile='i'
    AND transition_catalog.prorettype=aggregate_function.aggtranstype
),
transition_type AS MATERIALIZED (
  SELECT type_catalog.oid,
         type_catalog.typcollation<>0 AS collatable,
         pg_catalog.format_type(type_catalog.oid,-1) AS type_sql
  FROM aggregate_function
  JOIN pg_catalog.pg_type AS type_catalog
    ON type_catalog.oid=aggregate_function.aggtranstype
  JOIN pg_catalog.pg_namespace AS type_namespace
    ON type_namespace.oid=type_catalog.typnamespace
   AND type_namespace.nspname='pg_catalog'
  WHERE type_catalog.typtype<>'p'
    AND type_catalog.oid<>'internal'::pg_catalog.regtype::oid
),
final_function AS MATERIALIZED (
  SELECT final_catalog.*,
         pg_catalog.format(
           '%I.%I',final_namespace.nspname,final_catalog.proname
         ) AS function_sql,
         ARRAY(
           SELECT unnest(final_catalog.proargtypes::oid[])
         )::oid[] AS argument_types
  FROM aggregate_function
  JOIN pg_catalog.pg_proc AS final_catalog
    ON final_catalog.oid=aggregate_function.aggfinalfn
  JOIN pg_catalog.pg_namespace AS final_namespace
    ON final_namespace.oid=final_catalog.pronamespace
   AND final_namespace.nspname='pg_catalog'
  WHERE aggregate_function.aggfinalfn<>0
    AND final_catalog.prokind='f'
    AND final_catalog.provolatile='i'
    AND final_catalog.prorettype=$2
    AND NOT aggregate_function.aggfinalextra
    AND aggregate_function.aggfinalmodify='r'
    AND cardinality(
          ARRAY(SELECT unnest(final_catalog.proargtypes::oid[]))
        )=1
    AND (ARRAY(SELECT unnest(final_catalog.proargtypes::oid[]))::oid[])[1]
          =aggregate_function.aggtranstype
)
SELECT transition_function.function_sql,
       transition_type.type_sql,
       transition_type.oid,
       transition_type.collatable,
       transition_function.proisstrict,
       pg_catalog.quote_literal(aggregate_function.agginitval),
       final_function.function_sql,
       cardinality(aggregate_function.argument_types)::integer
FROM aggregate_function
JOIN transition_function ON true
JOIN transition_type ON true
LEFT JOIN final_function ON true
WHERE cardinality(transition_function.argument_types)
        =cardinality(aggregate_function.argument_types)+1
  AND transition_function.argument_types[1]
        =aggregate_function.aggtranstype
  AND transition_function.argument_types[2:]
        =aggregate_function.argument_types
  AND (
    NOT transition_function.proisstrict
    OR aggregate_function.agginitval IS NOT NULL
    OR (
      cardinality(aggregate_function.argument_types)=1
      AND aggregate_function.argument_types[1]
            =aggregate_function.aggtranstype
    )
  )
  AND (
    (aggregate_function.aggfinalfn=0
      AND aggregate_function.aggtranstype=$2)
    OR
    (aggregate_function.aggfinalfn<>0
      AND final_function.oid IS NOT NULL)
  )
"#;

#[derive(Clone, Debug)]
pub(crate) struct AggregateCapability {
    pub(crate) transition_function: String,
    pub(crate) transition_type: String,
    pub(crate) transition_type_oid: pg_sys::Oid,
    pub(crate) transition_collation_oid: pg_sys::Oid,
    pub(crate) transition_is_strict: bool,
    /// A server-quoted SQL literal, or `None` for a NULL initial state.
    pub(crate) initial_literal: Option<String>,
    pub(crate) final_function: Option<String>,
}

pub(crate) fn decode_aggregate_capability(
    rows: SpiTupleTable<'_>,
    function_oid: u32,
    expected_argument_count: usize,
    input_collation_oid: u32,
) -> Result<AggregateCapability, String> {
    if rows.len() != 1 {
        return Err(format!(
            "aggregate function OID {function_oid} has no unique durable capability"
        ));
    }
    let row = rows.first();
    let argument_count = usize::try_from(required::<i32>(&row, 8, "aggregate argument count")?)
        .map_err(|_| "aggregate argument count is negative")?;
    if argument_count != expected_argument_count {
        return Err("aggregate argument arity changed".into());
    }
    let transition_collatable =
        required::<bool>(&row, 4, "aggregate transition collation capability")?;
    let transition_collation_oid = match (transition_collatable, input_collation_oid) {
        (false, _) => pg_sys::InvalidOid,
        (true, 0) => {
            return Err("collatable aggregate transition state omitted input collation".into());
        }
        (true, oid) => pg_sys::Oid::from(oid),
    };
    Ok(AggregateCapability {
        transition_function: required(&row, 1, "aggregate transition function")?,
        transition_type: required(&row, 2, "aggregate transition type")?,
        transition_type_oid: required(&row, 3, "aggregate transition type OID")?,
        transition_collation_oid,
        transition_is_strict: required(&row, 5, "aggregate transition strictness")?,
        initial_literal: row.get(6).map_err(|error| error.to_string())?,
        final_function: row.get(7).map_err(|error| error.to_string())?,
    })
}

pub(crate) fn initial_state_sql(capability: &AggregateCapability) -> String {
    capability.initial_literal.as_ref().map_or_else(
        || format!("NULL::{}", capability.transition_type),
        |literal| format!("{literal}::{}", capability.transition_type),
    )
}

fn required<T: FromDatum + IntoDatum>(
    table: &SpiTupleTable<'_>,
    ordinal: usize,
    name: &str,
) -> Result<T, String> {
    table
        .get::<T>(ordinal)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("database returned NULL {name}"))
}
