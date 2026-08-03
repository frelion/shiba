use postgres::Transaction;

use crate::M2Error;

pub(crate) fn lock_binding(
    transaction: &mut Transaction<'_>,
    source_id: i64,
) -> Result<(), M2Error> {
    transaction
        .query_opt(
            "SELECT 1
             FROM shiba_internal.source_binding AS binding
             WHERE binding.source_id = $1
               AND binding.binding_kind = 'relation'
               AND binding.address_objsubid = 0
             FOR UPDATE OF binding",
            &[&source_id],
        )?
        .ok_or(M2Error::SourceBindingMissing)?;
    Ok(())
}

pub(crate) fn validate_execution_authority(
    transaction: &mut Transaction<'_>,
    source_id: i64,
    expected_slot_generation: i64,
) -> Result<(), M2Error> {
    let authority = transaction.query_one(
        "SELECT (
             SELECT config.slot_generation
             FROM shiba_internal.source_ingress_config AS config
             WHERE config.source_id = $1
         ), (
             SELECT bootstrap.slot_generation
             FROM shiba_internal.source_bootstrap AS bootstrap
             WHERE bootstrap.source_id = $1
         ), (
             SELECT bootstrap.phase
             FROM shiba_internal.source_bootstrap AS bootstrap
             WHERE bootstrap.source_id = $1
         )",
        &[&source_id],
    )?;
    let configured_generation: Option<i64> = authority.get(0);
    let bootstrap_generation: Option<i64> = authority.get(1);
    let bootstrap_phase: Option<&str> = authority.get(2);
    let Some(configured_generation) = configured_generation else {
        return if bootstrap_generation.is_none() && bootstrap_phase.is_none() {
            Ok(())
        } else {
            Err(M2Error::SourceInvalidated)
        };
    };
    if configured_generation != expected_slot_generation {
        return Err(M2Error::SlotGenerationMismatch);
    }
    match (bootstrap_generation, bootstrap_phase) {
        (None, None) => Ok(()),
        (Some(bootstrap_generation), Some(phase)) => {
            if bootstrap_generation != expected_slot_generation {
                return Err(M2Error::SlotGenerationMismatch);
            }
            if !matches!(phase, "catching_up" | "active") {
                return Err(M2Error::InvalidBootstrapPhase);
            }
            Ok(())
        }
        _ => Err(M2Error::SourceInvalidated),
    }
}

pub(crate) fn validate(transaction: &mut Transaction<'_>, source_id: i64) -> Result<(), M2Error> {
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
               AND binding.binding_kind = 'relation'
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
    validate_rebuild_identity(transaction, source_id)?;
    Ok(())
}

pub(crate) fn validate_rebuild_identity(
    transaction: &mut Transaction<'_>,
    source_id: i64,
) -> Result<(), M2Error> {
    let Some(marker) = transaction.query_opt(
        "SELECT retired_bootstrap_id IS NOT NULL,
                retired_slot_name IS NOT NULL,
                retired_slot_generation IS NOT NULL
         FROM shiba_internal.source_bootstrap WHERE source_id = $1",
        &[&source_id],
    )?
    else {
        return Ok(());
    };
    let marker = [
        marker.get::<_, bool>(0),
        marker.get::<_, bool>(1),
        marker.get::<_, bool>(2),
    ];
    if marker == [false; 3] {
        return Ok(());
    }
    if marker != [true; 3] {
        return Err(M2Error::SourceInvalidated);
    }

    let exact: bool = transaction
        .query_one(
            "SELECT count(*) = 4
                    AND count(*) FILTER (
                        WHERE binding.binding_kind = 'relation'
                          AND binding.address_classid = 'pg_class'::regclass
                          AND binding.address_objsubid = 0
                    ) = 1
                    AND count(*) FILTER (
                        WHERE binding.binding_kind = 'column'
                          AND binding.address_classid = 'pg_class'::regclass
                          AND binding.address_objid = relation.address_objid
                          AND binding.address_objsubid IN (1, 2)
                    ) = 2
                    AND count(*) FILTER (
                        WHERE binding.binding_kind = 'identity_index'
                          AND binding.address_classid = 'pg_class'::regclass
                          AND binding.address_objsubid = 0
                          AND identity.indrelid = relation.address_objid
                          AND identity.indisprimary AND identity.indisunique
                          AND identity.indisvalid AND identity.indisready
                          AND identity.indnkeyatts = 1 AND identity.indnatts = 1
                          AND (identity.indkey::smallint[])[0] = 1
                          AND identity.indexprs IS NULL
                          AND identity.indpred IS NULL
                    ) = 1
             FROM shiba_internal.source_binding AS binding
             CROSS JOIN LATERAL (
                 SELECT relation_binding.address_objid
                 FROM shiba_internal.source_binding AS relation_binding
                 JOIN pg_catalog.pg_class AS relation
                   ON relation.oid = relation_binding.address_objid
                  AND relation.relkind = 'r' AND relation.relreplident = 'd'
                 WHERE relation_binding.source_id = $1
                   AND relation_binding.binding_kind = 'relation'
                   AND relation_binding.address_classid = 'pg_class'::regclass
                   AND relation_binding.address_objsubid = 0
             ) AS relation
             LEFT JOIN pg_catalog.pg_index AS identity
               ON identity.indexrelid = binding.address_objid
             WHERE binding.source_id = $1",
            &[&source_id],
        )?
        .get(0);
    if !exact {
        return Err(M2Error::SourceInvalidated);
    }
    Ok(())
}
