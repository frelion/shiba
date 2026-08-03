use postgres::Transaction;
use shiba_compiler::{IdentityIndexDescriptor, SourceDescriptor};
use shiba_protocol::{GraphId, SourceId};
use shiba_sql_frontend::{ErrorCode, QualifiedRelation, Span, UnboundQuery};

use crate::SqlRegistrationError;
use crate::catalog_descriptor::describe;

#[derive(Debug)]
pub(crate) struct CatalogSource {
    pub(crate) descriptor: SourceDescriptor,
    pub(crate) identity: IdentityIndexDescriptor,
}

struct Candidate {
    input: usize,
    source_id: SourceId,
    relation_oid: u32,
    schema_name: String,
    relation_name: String,
    quoted_name: String,
    span: Span,
}

pub(crate) fn resolve_sources(
    transaction: &mut Transaction<'_>,
    graph_id: GraphId,
    query: &UnboundQuery,
) -> Result<Vec<CatalogSource>, SqlRegistrationError> {
    reject_existing_graph(transaction, graph_id, query.span)?;
    let class_id = relation_class_id(transaction, query.span)?;
    let mut candidates = query
        .sources
        .iter()
        .enumerate()
        .map(|(input, relation)| candidate(transaction, input, relation, class_id))
        .collect::<Result<Vec<_>, _>>()?;
    candidates.sort_by_key(|candidate| candidate.source_id);
    if candidates
        .windows(2)
        .any(|pair| pair[0].source_id == pair[1].source_id)
    {
        return Err(SqlRegistrationError::catalog(
            ErrorCode::IdentityMismatch,
            query.span,
        ));
    }

    let mut resolved = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        resolved.push((candidate.input, lock_and_describe(transaction, &candidate)?));
    }
    resolved.sort_by_key(|(input, _)| *input);
    Ok(resolved.into_iter().map(|(_, source)| source).collect())
}

fn reject_existing_graph(
    transaction: &mut Transaction<'_>,
    graph_id: GraphId,
    span: Span,
) -> Result<(), SqlRegistrationError> {
    let graph_id = i64::try_from(graph_id.get())
        .map_err(|_| SqlRegistrationError::catalog(ErrorCode::GraphConflict, span))?;
    let exists = transaction
        .query_opt(
            "SELECT 1 FROM shiba_internal.graph_definition WHERE graph_id=$1 FOR UPDATE",
            &[&graph_id],
        )
        .map_err(|error| {
            SqlRegistrationError::postgres(ErrorCode::RegistrationFailed, span, error)
        })?
        .is_some();
    if exists {
        return Err(SqlRegistrationError::catalog(
            ErrorCode::GraphConflict,
            span,
        ));
    }
    Ok(())
}

fn candidate(
    transaction: &mut Transaction<'_>,
    input: usize,
    relation: &QualifiedRelation,
    class_id: u32,
) -> Result<Candidate, SqlRegistrationError> {
    let row = transaction
        .query_opt(
            "SELECT class.oid::bigint,
                    pg_catalog.quote_ident(namespace.nspname) || '.' ||
                    pg_catalog.quote_ident(class.relname)
             FROM pg_catalog.pg_class AS class
             JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=class.relnamespace
             WHERE namespace.nspname=$1 AND class.relname=$2 AND class.relkind='r'",
            &[&relation.schema.value, &relation.relation.value],
        )
        .map_err(|error| {
            SqlRegistrationError::postgres(ErrorCode::RegistrationFailed, relation.span, error)
        })?
        .ok_or_else(|| SqlRegistrationError::catalog(ErrorCode::UnknownRelation, relation.span))?;
    let relation_oid = u32::try_from(row.get::<_, i64>(0))
        .map_err(|_| SqlRegistrationError::catalog(ErrorCode::IdentityMismatch, relation.span))?;
    let source_rows = transaction
        .query(
            "SELECT source_id FROM shiba_internal.source_binding
             WHERE binding_kind='relation' AND address_classid=$1::oid
               AND address_objid=$2::oid AND address_objsubid=0 ORDER BY source_id",
            &[&class_id, &relation_oid],
        )
        .map_err(|error| {
            SqlRegistrationError::postgres(ErrorCode::RegistrationFailed, relation.span, error)
        })?;
    if source_rows.len() != 1 {
        return Err(SqlRegistrationError::catalog(
            ErrorCode::SourceNotRegistered,
            relation.span,
        ));
    }
    let source_key: i64 = source_rows[0].get(0);
    let source_id = u64::try_from(source_key)
        .ok()
        .and_then(|value| SourceId::new(value).ok())
        .ok_or_else(|| SqlRegistrationError::catalog(ErrorCode::IdentityMismatch, relation.span))?;
    Ok(Candidate {
        input,
        source_id,
        relation_oid,
        schema_name: relation.schema.value.clone(),
        relation_name: relation.relation.value.clone(),
        quoted_name: row.get(1),
        span: relation.span,
    })
}

fn lock_and_describe(
    transaction: &mut Transaction<'_>,
    candidate: &Candidate,
) -> Result<CatalogSource, SqlRegistrationError> {
    let source_key = i64::try_from(candidate.source_id.get())
        .map_err(|_| SqlRegistrationError::catalog(ErrorCode::IdentityMismatch, candidate.span))?;
    let bindings = transaction
        .query(
            "SELECT binding_kind,address_classid::bigint,address_objid::bigint,address_objsubid
             FROM shiba_internal.source_binding WHERE source_id=$1
             ORDER BY binding_kind,address_objsubid FOR UPDATE",
            &[&source_key],
        )
        .map_err(|error| {
            SqlRegistrationError::postgres(ErrorCode::RegistrationFailed, candidate.span, error)
        })?;
    if bindings.is_empty() {
        return Err(SqlRegistrationError::catalog(
            ErrorCode::SourceNotRegistered,
            candidate.span,
        ));
    }
    transaction
        .batch_execute(&format!(
            "LOCK TABLE {} IN ACCESS SHARE MODE",
            candidate.quoted_name
        ))
        .map_err(|error| {
            SqlRegistrationError::postgres(ErrorCode::DdlDrift, candidate.span, error)
        })?;
    validate_live_relation(transaction, candidate)?;
    if transaction
        .query_opt(
            "SELECT 1 FROM shiba_internal.source_invalidation WHERE source_id=$1",
            &[&source_key],
        )
        .map_err(|error| {
            SqlRegistrationError::postgres(ErrorCode::RegistrationFailed, candidate.span, error)
        })?
        .is_some()
    {
        return Err(SqlRegistrationError::catalog(
            ErrorCode::DdlDrift,
            candidate.span,
        ));
    }
    describe(
        transaction,
        candidate.source_id,
        candidate.relation_oid,
        candidate.span,
    )
}

fn validate_live_relation(
    transaction: &mut Transaction<'_>,
    candidate: &Candidate,
) -> Result<(), SqlRegistrationError> {
    let observed = transaction
        .query_opt(
            "SELECT class.oid::bigint FROM pg_catalog.pg_class AS class
             JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=class.relnamespace
             WHERE namespace.nspname=$1 AND class.relname=$2 AND class.relkind='r'",
            &[&candidate.schema_name, &candidate.relation_name],
        )
        .map_err(|error| {
            SqlRegistrationError::postgres(ErrorCode::RegistrationFailed, candidate.span, error)
        })?;
    let observed = observed
        .and_then(|row| u32::try_from(row.get::<_, i64>(0)).ok())
        .filter(|oid| *oid == candidate.relation_oid);
    let binding_matches = transaction
        .query_one(
            "SELECT count(*)=1 FROM shiba_internal.source_binding
             WHERE source_id=$1 AND binding_kind='relation'
               AND address_classid='pg_class'::regclass
               AND address_objid=$2::oid AND address_objsubid=0",
            &[
                &i64::try_from(candidate.source_id.get()).map_err(|_| {
                    SqlRegistrationError::catalog(ErrorCode::IdentityMismatch, candidate.span)
                })?,
                &candidate.relation_oid,
            ],
        )
        .map_err(|error| {
            SqlRegistrationError::postgres(ErrorCode::RegistrationFailed, candidate.span, error)
        })?
        .get::<_, bool>(0);
    if observed.is_none() || !binding_matches {
        return Err(SqlRegistrationError::catalog(
            ErrorCode::DdlDrift,
            candidate.span,
        ));
    }
    Ok(())
}

fn relation_class_id(
    transaction: &mut Transaction<'_>,
    span: Span,
) -> Result<u32, SqlRegistrationError> {
    u32::try_from(
        transaction
            .query_one("SELECT 'pg_class'::regclass::oid::bigint", &[])
            .map_err(|error| {
                SqlRegistrationError::postgres(ErrorCode::RegistrationFailed, span, error)
            })?
            .get::<_, i64>(0),
    )
    .map_err(|_| SqlRegistrationError::catalog(ErrorCode::IdentityMismatch, span))
}
