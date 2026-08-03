use postgres::GenericClient;

use crate::{IngressError, bootstrap::as_bigint, rebuild_model::PreparedAuthority};

/// Revalidates every exact durable member and the one graph publication.
pub(crate) fn verify_rebuild_target(
    client: &mut impl GenericClient,
    authority: &PreparedAuthority,
) -> Result<(), IngressError> {
    let graph = as_bigint(authority.graph_id.get())?;
    if authority.target.members.is_empty() || authority.target.members.len() > 2 {
        return Err(IngressError::Governance("rebuild member set is invalid"));
    }
    for member in &authority.target.members {
        let source = as_bigint(member.source_id.get())?;
        let relation = i64::from(member.relation_oid);
        let identity = i64::from(member.identity_index_oid);
        reconcile_identity_rename(
            client,
            graph,
            source,
            relation,
            identity,
            authority.target.bootstrap_id.get(),
            authority.target.slot_generation.get(),
        )?;
        let exact: bool = client
            .query_one(
                "SELECT NOT EXISTS (SELECT 1 FROM shiba_internal.source_invalidation
                                WHERE source_id = $1)
               AND pg_catalog.has_table_privilege(session_user, $2::bigint::oid, 'SELECT')
               AND EXISTS (
                 SELECT 1 FROM pg_catalog.pg_index AS identity
                 JOIN pg_catalog.pg_class AS relation ON relation.oid = identity.indrelid
                 JOIN shiba_internal.source_binding AS index_binding
                   ON index_binding.source_id = $1
                  AND index_binding.binding_kind = 'identity_index'
                  AND index_binding.address_objid = identity.indexrelid
                 JOIN shiba_internal.source_binding AS key_binding
                   ON key_binding.source_id = $1 AND key_binding.binding_kind = 'column'
                  AND key_binding.address_objid = identity.indrelid
                  AND key_binding.address_objsubid = (identity.indkey::smallint[])[0]
                 JOIN pg_catalog.pg_attribute AS key
                   ON key.attrelid = identity.indrelid
                  AND key.attnum = key_binding.address_objsubid
                 WHERE identity.indexrelid = $3::bigint::oid
                   AND identity.indrelid = $2::bigint::oid
                   AND identity.indisunique AND identity.indisvalid AND identity.indisready
                   AND ((relation.relreplident = 'd' AND identity.indisprimary)
                        OR (relation.relreplident = 'i' AND identity.indisreplident))
                   AND identity.indnkeyatts = 1 AND identity.indnatts = 1
                   AND identity.indexprs IS NULL AND identity.indpred IS NULL
                   AND relation.relkind = 'r' AND relation.relreplident IN ('d','i')
                   AND key.atttypid = 20 AND key.attnotnull)
               AND 4 = (SELECT count(*) FROM shiba_internal.source_binding
                         WHERE source_id = $1)",
                &[&source, &relation, &identity],
            )?
            .get(0);
        if !exact {
            return Err(IngressError::Governance("rebuild member target drifted"));
        }
    }
    let relations = authority
        .target
        .members
        .iter()
        .map(|member| i64::from(member.relation_oid))
        .collect::<Vec<_>>();
    let publication = i64::from(authority.target.publication_oid);
    let exact: bool = client
        .query_one(
            "SELECT NOT EXISTS (SELECT 1 FROM shiba_internal.graph_ingress_invalidation
                            WHERE graph_id = $1)
           AND config.graph_digest = $2
           AND config.publication_objid = $3::bigint::oid
           AND config.slot_name = $4::text::name
           AND config.slot_generation = $5
           AND NOT publication.puballtables AND publication.pubinsert
           AND publication.pubupdate AND publication.pubdelete
           AND NOT publication.pubviaroot
           AND (SELECT array_agg(prrelid::bigint ORDER BY prrelid)
                FROM pg_catalog.pg_publication_rel WHERE prpubid = publication.oid
                  AND prqual IS NULL) =
               (SELECT array_agg(value ORDER BY value) FROM unnest($6::bigint[]) AS value)
         FROM shiba_internal.graph_ingress_config AS config
         JOIN pg_catalog.pg_publication AS publication
           ON publication.oid = config.publication_objid
          AND publication.pubname = config.publication_name
         WHERE config.graph_id = $1",
            &[
                &graph,
                &authority.target.graph_digest.as_slice(),
                &publication,
                &authority.target.slot_name,
                &as_bigint(authority.target.slot_generation.get())?,
                &relations,
            ],
        )?
        .get(0);
    if !exact {
        return Err(IngressError::Governance("rebuild graph target drifted"));
    }
    Ok(())
}

fn reconcile_identity_rename(
    client: &mut impl GenericClient,
    graph: i64,
    source: i64,
    relation: i64,
    identity: i64,
    bootstrap: u64,
    generation: u64,
) -> Result<(), IngressError> {
    client.execute(
        "DELETE FROM shiba_internal.source_invalidation AS invalid
         WHERE invalid.source_id = $1 AND invalid.address_objid = $2::bigint::oid
           AND invalid.address_objsubid = 0
           AND 1 = (SELECT count(*) FROM shiba_internal.source_invalidation WHERE source_id = $1)
           AND NOT EXISTS (SELECT 1 FROM shiba_internal.graph_ingress_invalidation WHERE graph_id = $3)
           AND EXISTS (SELECT 1 FROM shiba_internal.graph_bootstrap
                       WHERE graph_id = $3 AND bootstrap_id = $4
                         AND slot_generation = $5 AND phase = 'rebuild_prepared')
           AND EXISTS (SELECT 1 FROM pg_catalog.pg_index AS identity
                       JOIN pg_catalog.pg_class AS relation ON relation.oid = identity.indrelid
                       WHERE identity.indexrelid = $2::bigint::oid
                         AND identity.indrelid = $6::bigint::oid
                         AND indisunique AND indisvalid AND indisready
                         AND ((relation.relreplident = 'd' AND identity.indisprimary)
                              OR (relation.relreplident = 'i' AND identity.indisreplident))
                         AND indnkeyatts = 1 AND indnatts = 1
                         AND indexprs IS NULL AND indpred IS NULL)",
        &[&source, &identity, &graph, &as_bigint(bootstrap)?, &as_bigint(generation)?, &relation])?;
    Ok(())
}
