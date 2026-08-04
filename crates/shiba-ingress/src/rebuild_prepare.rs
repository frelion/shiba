use postgres::Transaction;
use shiba_runtime::RebuildGraphArtifact;

use crate::{
    IngressError, bootstrap::as_bigint, rebuild_model::RebuildSpec, transport::validate_slot,
};

pub(crate) fn validate_spec(spec: &RebuildSpec) -> Result<(), IngressError> {
    let old_sources = spec.expected.members.iter().map(|member| member.source_id);
    let new_sources = spec.target.members.iter().map(|member| member.source_id);
    if spec.expected.members.is_empty()
        || spec.expected.members.len() > 2
        || spec.expected.members.len() != spec.target.members.len()
        || !old_sources.eq(new_sources)
    {
        return Err(IngressError::Governance(
            "rebuild graph membership must remain exact",
        ));
    }
    for identity in [&spec.expected, &spec.target] {
        if identity.publication_oid == 0
            || identity.graph_digest == [0; 32]
            || identity
                .members
                .iter()
                .any(|member| member.relation_oid == 0 || member.identity_index_oid == 0)
        {
            return Err(IngressError::InvalidIdentifier(
                "graph rebuild ObjectAddress",
            ));
        }
        validate_slot(&identity.slot_name)?;
    }
    if spec.expected.slot_name == spec.target.slot_name
        || spec.expected.bootstrap_id == spec.target.bootstrap_id
        || spec.expected.slot_generation.get().checked_add(1)
            != Some(spec.target.slot_generation.get())
    {
        return Err(IngressError::Governance(
            "target rebuild identity is not an exact successor",
        ));
    }
    Ok(())
}

pub(crate) fn lock_target_relations(
    transaction: &mut Transaction<'_>,
    spec: &RebuildSpec,
) -> Result<(), IngressError> {
    let mut members = spec.target.members.clone();
    members.sort_by_key(|member| member.source_id);
    for member in members {
        let row = transaction
            .query_opt(
                "SELECT namespace.nspname::text, class.relname::text
             FROM pg_catalog.pg_class AS class JOIN pg_catalog.pg_namespace AS namespace
               ON namespace.oid = class.relnamespace WHERE class.oid = $1::bigint::oid",
                &[&i64::from(member.relation_oid)],
            )?
            .ok_or(IngressError::Governance("target relation is missing"))?;
        let qualified = format!("{}.{}", quote(row.get(0)), quote(row.get(1)));
        transaction.batch_execute(&format!("LOCK TABLE {qualified} IN ACCESS SHARE MODE"))?;
        let oid: i64 = transaction
            .query_one(
                "SELECT pg_catalog.to_regclass($1)::oid::bigint",
                &[&qualified],
            )?
            .get(0);
        if oid != i64::from(member.relation_oid) {
            return Err(IngressError::Governance("target relation identity drifted"));
        }
    }
    Ok(())
}

pub(crate) fn invoke_prepare_writer(
    transaction: &mut Transaction<'_>,
    spec: &RebuildSpec,
    artifact: &RebuildGraphArtifact,
) -> Result<(), IngressError> {
    let old_relations = oids(&spec.expected.members, |member| member.relation_oid);
    let old_indexes = oids(&spec.expected.members, |member| member.identity_index_oid);
    let source_ids = spec
        .target
        .members
        .iter()
        .map(|member| as_bigint(member.source_id.get()))
        .collect::<Result<Vec<_>, _>>()?;
    let target_relations = oids(&spec.target.members, |member| member.relation_oid);
    let target_indexes = oids(&spec.target.members, |member| member.identity_index_oid);
    let result_ids = artifact
        .results
        .iter()
        .map(|result| result.result_id)
        .collect::<Vec<_>>();
    let schema_payloads = artifact
        .results
        .iter()
        .map(|result| result.schema_payload.clone())
        .collect::<Vec<_>>();
    let schema_digests = artifact
        .results
        .iter()
        .map(|result| result.schema_digest.to_vec())
        .collect::<Vec<_>>();
    transaction.query_one(
        "SELECT shiba_internal.prepare_graph_rebuild(
          $1,$2,$3,ARRAY(SELECT value::oid FROM unnest($4::bigint[]) AS value),
          ARRAY(SELECT value::oid FROM unnest($5::bigint[]) AS value),
          $6::bigint::oid,$7::text::name,$8,$9,$10,
          ARRAY(SELECT value::oid FROM unnest($11::bigint[]) AS value),
          ARRAY(SELECT value::oid FROM unnest($12::bigint[]) AS value),
          $13::bigint::oid,$14::text::name,$15,$16,$17,$18,$19,$20,$21)",
        &[
            &as_bigint(spec.graph_id.get())?,
            &spec.expected.graph_digest.as_slice(),
            &as_bigint(spec.expected.bootstrap_id.get())?,
            &old_relations,
            &old_indexes,
            &i64::from(spec.expected.publication_oid),
            &spec.expected.slot_name,
            &as_bigint(spec.expected.slot_generation.get())?,
            &as_bigint(spec.target.bootstrap_id.get())?,
            &source_ids,
            &target_relations,
            &target_indexes,
            &i64::from(spec.target.publication_oid),
            &spec.target.slot_name,
            &as_bigint(spec.target.slot_generation.get())?,
            &artifact.spec_payload,
            &artifact.graph_payload,
            &artifact.graph_digest.as_slice(),
            &result_ids,
            &schema_payloads,
            &schema_digests,
        ],
    )?;
    Ok(())
}

fn oids<T>(members: &[T], get: impl Fn(&T) -> u32) -> Vec<i64> {
    members
        .iter()
        .map(|member| i64::from(get(member)))
        .collect()
}

fn quote(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
