use postgres::Transaction;

use crate::M2Error;

pub(crate) fn run(transaction: &mut Transaction<'_>, source_id: i64) -> Result<(), M2Error> {
    let binding = transaction
        .query_opt(
            "SELECT pg_catalog.quote_ident(namespace.nspname),
                    pg_catalog.quote_ident(class.relname)
             FROM shiba_internal.source_binding AS binding
             LEFT JOIN pg_catalog.pg_class AS class
               ON class.oid = binding.address_objid
              AND binding.address_classid = 'pg_catalog.pg_class'::regclass
              AND binding.address_objsubid = 0
              AND class.relkind = 'r'
             LEFT JOIN pg_catalog.pg_namespace AS namespace
               ON namespace.oid = class.relnamespace
             WHERE binding.source_id = $1
               AND binding.address_objsubid = 0",
            &[&source_id],
        )?
        .ok_or(M2Error::SourceBindingMissing)?;
    let schema: Option<&str> = binding.get(0);
    let relation: Option<&str> = binding.get(1);
    let (Some(schema), Some(relation)) = (schema, relation) else {
        return Err(M2Error::SourceInvalidated);
    };

    let lock = format!("LOCK TABLE {schema}.{relation} IN ACCESS SHARE MODE");
    transaction.batch_execute(&lock)?;

    let invalidated: bool = transaction
        .query_one(
            "SELECT EXISTS (
                 SELECT 1
                 FROM shiba_internal.source_binding AS binding
                 JOIN shiba_internal.source_invalidation AS invalidation
                   ON invalidation.source_id = binding.source_id
                  AND invalidation.address_classid = binding.address_classid
                  AND invalidation.address_objid = binding.address_objid
                  AND invalidation.address_objsubid = binding.address_objsubid
                 WHERE binding.source_id = $1
             )",
            &[&source_id],
        )?
        .get(0);
    if invalidated {
        return Err(M2Error::SourceInvalidated);
    }
    Ok(())
}
