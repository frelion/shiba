use postgres::Transaction;
use shiba_operator::{GraphEffectOrigin, OperatorGraph, ValueType};

use crate::M2Error;

pub(crate) fn validate_execution_authority(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    expected_generation: i64,
) -> Result<(), M2Error> {
    let row = transaction
        .query_opt(
            "SELECT config.slot_generation, config.database_oid = pg_catalog.pg_database.oid,
                    NOT EXISTS (SELECT 1 FROM shiba_internal.graph_ingress_invalidation
                                WHERE graph_id = config.graph_id)
             FROM shiba_internal.graph_ingress_config AS config
             JOIN pg_catalog.pg_database ON datname = current_database()
             WHERE config.graph_id = $1",
            &[&graph_id],
        )?
        .ok_or(M2Error::SourceBindingMissing)?;
    if row.get::<_, i64>(0) != expected_generation {
        return Err(M2Error::SlotGenerationMismatch);
    }
    if !row.get::<_, bool>(1) || !row.get::<_, bool>(2) {
        return Err(M2Error::SourceInvalidated);
    }
    let bootstrap = transaction
        .query_opt(
            "SELECT slot_generation, phase FROM shiba_internal.graph_bootstrap
             WHERE graph_id = $1 FOR UPDATE",
            &[&graph_id],
        )?
        .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)));
    match bootstrap
        .as_ref()
        .map(|(generation, phase)| (*generation, phase.as_str()))
    {
        None => Ok(()),
        Some((generation, "catching_up" | "active")) if generation == expected_generation => Ok(()),
        Some((generation, _)) if generation != expected_generation => {
            Err(M2Error::SlotGenerationMismatch)
        }
        Some(_) => Err(M2Error::InvalidBootstrapPhase),
    }
}

pub(crate) fn validate_sources(
    transaction: &mut Transaction<'_>,
    graph: &OperatorGraph,
) -> Result<(), M2Error> {
    for port in &graph.sources {
        let source_id =
            i64::try_from(port.source_id.get()).map_err(|_| M2Error::InvalidSourceRowState)?;
        let relation = transaction
            .query_opt(
                "SELECT pg_catalog.quote_ident(namespace.nspname),
                        pg_catalog.quote_ident(class.relname), class.oid::bigint
                 FROM shiba_internal.source_binding AS binding
                 JOIN pg_catalog.pg_class AS class
                   ON class.oid = binding.address_objid AND class.relkind = 'r'
                 JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
                 WHERE binding.source_id = $1 AND binding.binding_kind = 'relation'
                   AND binding.address_classid = 'pg_class'::regclass
                   AND binding.address_objsubid = 0
                 FOR UPDATE OF binding",
                &[&source_id],
            )?
            .ok_or(M2Error::SourceBindingMissing)?;
        let relation_oid =
            u32::try_from(relation.get::<_, i64>(2)).map_err(|_| M2Error::SourceInvalidated)?;
        let lock = format!(
            "LOCK TABLE {}.{} IN ACCESS SHARE MODE",
            relation.get::<_, &str>(0),
            relation.get::<_, &str>(1)
        );
        transaction.batch_execute(&lock)?;
        validate_columns(transaction, source_id, relation_oid, port)?;
        validate_identity(transaction, source_id, relation_oid, port)?;
        let invalidated: bool = transaction
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM shiba_internal.source_invalidation
                                 WHERE source_id = $1)",
                &[&source_id],
            )?
            .get(0);
        if invalidated {
            return Err(M2Error::SourceInvalidated);
        }
    }
    Ok(())
}

pub(crate) fn result_visibility(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    origin: GraphEffectOrigin,
) -> Result<bool, M2Error> {
    let expected_graph = u64::try_from(graph_id).map_err(|_| M2Error::InvalidBootstrapPhase)?;
    let phase = transaction
        .query_opt(
            "SELECT bootstrap_id, phase FROM shiba_internal.graph_bootstrap
             WHERE graph_id = $1 FOR UPDATE",
            &[&graph_id],
        )?
        .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)));
    match (origin, phase) {
        (GraphEffectOrigin::Wal(id), None) if id.graph_id.get() == expected_graph => Ok(true),
        (GraphEffectOrigin::Wal(id), Some((_, phase)))
            if id.graph_id.get() == expected_graph && phase == "active" =>
        {
            Ok(true)
        }
        (GraphEffectOrigin::Wal(id), Some((_, phase)))
            if id.graph_id.get() == expected_graph && phase == "catching_up" =>
        {
            Ok(false)
        }
        (GraphEffectOrigin::Bootstrap(id), Some((bootstrap_id, phase)))
            if u64::try_from(bootstrap_id).ok() == Some(id.bootstrap_id.get())
                && phase == "scanning" =>
        {
            Ok(false)
        }
        _ => Err(M2Error::InvalidBootstrapPhase),
    }
}

fn validate_columns(
    transaction: &mut Transaction<'_>,
    source_id: i64,
    relation_oid: u32,
    port: &shiba_operator::SourcePort,
) -> Result<(), M2Error> {
    let rows = transaction.query(
        "SELECT binding.address_classid::bigint, binding.address_objid::bigint,
                binding.address_objsubid, attribute.atttypid::bigint
         FROM shiba_internal.source_binding AS binding
         JOIN pg_catalog.pg_attribute AS attribute
           ON attribute.attrelid = binding.address_objid
          AND attribute.attnum = binding.address_objsubid AND NOT attribute.attisdropped
         WHERE binding.source_id = $1 AND binding.binding_kind = 'column'
         ORDER BY binding.address_objsubid FOR UPDATE OF binding",
        &[&source_id],
    )?;
    if rows.len() != port.layout.len() {
        return Err(M2Error::SourceInvalidated);
    }
    for (row, binding) in rows.iter().zip(&port.layout) {
        let value_type = match row.get::<_, i64>(3) {
            20 => ValueType::Int8,
            25 => ValueType::Text,
            _ => return Err(M2Error::SourceInvalidated),
        };
        if u32::try_from(row.get::<_, i64>(0)).ok() != Some(binding.address.class_id)
            || u32::try_from(row.get::<_, i64>(1)).ok() != Some(relation_oid)
            || row.get::<_, i32>(2) != binding.address.sub_id
            || binding.address.object_id != relation_oid
            || binding.value_type != value_type
        {
            return Err(M2Error::SourceInvalidated);
        }
    }
    Ok(())
}

fn validate_identity(
    transaction: &mut Transaction<'_>,
    source_id: i64,
    relation_oid: u32,
    port: &shiba_operator::SourcePort,
) -> Result<(), M2Error> {
    let row = transaction.query_opt(
        "SELECT binding.address_classid::bigint, binding.address_objid::bigint,
                binding.address_objsubid, index.indrelid::bigint,
                index.indisunique AND index.indisvalid AND index.indisready
                AND (index.indisreplident OR
                     (relation.relreplident = 'd' AND index.indisprimary))
                AND index.indnkeyatts > 0
                AND index.indexprs IS NULL AND index.indpred IS NULL,
                (index.indkey::smallint[])[0]::integer
         FROM shiba_internal.source_binding AS binding
         JOIN pg_catalog.pg_index AS index ON index.indexrelid = binding.address_objid
         JOIN pg_catalog.pg_class AS relation ON relation.oid = index.indrelid
         WHERE binding.source_id = $1 AND binding.binding_kind = 'identity_index'
         FOR UPDATE OF binding",
        &[&source_id],
    )?;
    match (port.identity_index, row) {
        (None, None) if port.layout.is_empty() => Ok(()),
        (Some(expected), Some(row))
            if u32::try_from(row.get::<_, i64>(0)).ok() == Some(expected.class_id)
                && u32::try_from(row.get::<_, i64>(1)).ok() == Some(expected.object_id)
                && row.get::<_, i32>(2) == expected.sub_id
                && u32::try_from(row.get::<_, i64>(3)).ok() == Some(relation_oid)
                && row.get::<_, bool>(4)
                && port.layout.first().map(|binding| binding.address.sub_id)
                    == Some(row.get::<_, i32>(5)) =>
        {
            Ok(())
        }
        _ => Err(M2Error::SourceInvalidated),
    }
}
