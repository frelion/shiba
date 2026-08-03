use postgres::GenericClient;

use crate::{IngressError, bootstrap::as_bigint, rebuild_model::PreparedAuthority};

/// Reconciles the one harmless index-rename event and revalidates the target.
///
/// The catalog event trigger remains the durable invalidation writer. These
/// live checks close the interval between destructive prepare and exported
/// snapshot creation without introducing another identity authority. Only the
/// rebuild control plane may clear an invalidation, and only when the durable
/// index OID still proves the exact same primary-key semantics.
pub(crate) fn verify_rebuild_target(
    client: &mut impl GenericClient,
    authority: &PreparedAuthority,
) -> Result<(), IngressError> {
    let source_id = as_bigint(authority.source_id.get())?;
    let relation_oid = i64::from(authority.target.relation_oid);
    let identity_index_oid = i64::from(authority.target.identity_index_oid);
    let publication_oid = i64::from(authority.target.publication_oid);
    reconcile_identity_rename(
        client,
        authority,
        source_id,
        relation_oid,
        identity_index_oid,
    )?;
    let exact: bool = client
        .query_one(
            "SELECT
               NOT EXISTS (SELECT 1 FROM shiba_internal.source_invalidation
                           WHERE source_id = $1)
               AND NOT EXISTS (SELECT 1 FROM shiba_internal.source_ingress_invalidation
                               WHERE source_id = $1)
               AND pg_catalog.has_table_privilege(
                       session_user, $2::bigint::oid, 'SELECT')
               AND EXISTS (
                   SELECT 1 FROM pg_catalog.pg_class AS relation
                   WHERE relation.oid = $2::bigint::oid
                     AND relation.relkind = 'r' AND relation.relreplident = 'd')
               AND 2 = (SELECT count(*) FROM pg_catalog.pg_attribute
                        WHERE attrelid = $2::bigint::oid
                          AND attnum > 0 AND NOT attisdropped)
               AND EXISTS (
                   SELECT 1 FROM pg_catalog.pg_index AS identity
                   JOIN shiba_internal.source_binding AS key_binding
                     ON key_binding.source_id = $1
                    AND key_binding.binding_kind = 'column'
                    AND key_binding.address_objid = identity.indrelid
                    AND key_binding.address_objsubid =
                        (identity.indkey::smallint[])[0]
                   JOIN pg_catalog.pg_attribute AS key
                     ON key.attrelid = identity.indrelid
                    AND key.attnum = key_binding.address_objsubid
                   JOIN shiba_internal.source_binding AS payload_binding
                     ON payload_binding.source_id = $1
                    AND payload_binding.binding_kind = 'column'
                    AND payload_binding.address_objid = identity.indrelid
                    AND payload_binding.address_objsubid <> key_binding.address_objsubid
                   JOIN pg_catalog.pg_attribute AS payload
                     ON payload.attrelid = identity.indrelid
                    AND payload.attnum = payload_binding.address_objsubid
                   WHERE identity.indexrelid = $3::bigint::oid
                     AND identity.indrelid = $2::bigint::oid
                     AND identity.indisprimary AND identity.indisunique
                     AND identity.indisvalid AND identity.indisready
                     AND identity.indnkeyatts = 1 AND identity.indnatts = 1
                     AND identity.indexprs IS NULL AND identity.indpred IS NULL
                     AND key.atttypid = 20 AND key.attnotnull
                     AND payload.atttypid = 20 AND NOT payload.attnotnull
                     AND key.attnum < payload.attnum
                     AND key.attgenerated = '' AND payload.attgenerated = '')
               AND EXISTS (
                   SELECT 1
                   FROM shiba_internal.source_ingress_config AS config
                   JOIN pg_catalog.pg_publication AS publication
                     ON publication.oid = config.publication_objid
                    AND publication.pubname = config.publication_name
                    AND publication.pubinsert = config.publication_insert
                    AND publication.pubupdate = config.publication_update
                    AND publication.pubdelete = config.publication_delete
                    AND publication.pubtruncate = config.publication_truncate
                    AND publication.pubviaroot = config.publication_via_root
                   JOIN pg_catalog.pg_publication_rel AS member
                     ON member.prpubid = publication.oid
                    AND member.prrelid = $2::bigint::oid
                   WHERE config.source_id = $1
                     AND config.publication_objid = $4::bigint::oid
                     AND NOT publication.puballtables AND member.prqual IS NULL
                     AND config.publication_attnums = ARRAY(
                         SELECT binding.address_objsubid::smallint
                         FROM shiba_internal.source_binding AS binding
                         WHERE binding.source_id = $1 AND binding.binding_kind = 'column'
                         ORDER BY binding.address_objsubid)
                     AND (member.prattrs IS NULL OR
                          member.prattrs::smallint[] = ARRAY(
                              SELECT binding.address_objsubid::smallint
                              FROM shiba_internal.source_binding AS binding
                              WHERE binding.source_id = $1
                                AND binding.binding_kind = 'column'
                              ORDER BY binding.address_objsubid))
                     AND 1 = (SELECT count(*) FROM pg_catalog.pg_publication_rel
                              WHERE prpubid = publication.oid))",
            &[
                &source_id,
                &relation_oid,
                &identity_index_oid,
                &publication_oid,
            ],
        )?
        .get(0);
    if !exact {
        return Err(IngressError::Governance("rebuild target drifted"));
    }
    Ok(())
}

fn reconcile_identity_rename(
    client: &mut impl GenericClient,
    authority: &PreparedAuthority,
    source_id: i64,
    relation_oid: i64,
    identity_index_oid: i64,
) -> Result<(), IngressError> {
    client.execute(
        "DELETE FROM shiba_internal.source_invalidation AS invalidation
         WHERE invalidation.source_id = $1
           AND invalidation.address_classid = 'pg_class'::regclass
           AND invalidation.address_objid = $2::bigint::oid
           AND invalidation.address_objsubid = 0
           AND 1 = (SELECT count(*) FROM shiba_internal.source_invalidation
                    WHERE source_id = $1)
           AND NOT EXISTS (
               SELECT 1 FROM shiba_internal.source_ingress_invalidation
               WHERE source_id = $1)
           AND EXISTS (
               SELECT 1 FROM shiba_internal.source_bootstrap
               WHERE source_id = $1 AND bootstrap_id = $4
                 AND slot_generation = $5 AND phase = 'rebuild_prepared')
           AND EXISTS (
               SELECT 1 FROM pg_catalog.pg_index AS identity
               JOIN shiba_internal.source_binding AS key_binding
                 ON key_binding.source_id = $1
                AND key_binding.binding_kind = 'column'
                AND key_binding.address_objid = identity.indrelid
                AND key_binding.address_objsubid =
                    (identity.indkey::smallint[])[0]
               JOIN pg_catalog.pg_attribute AS key
                 ON key.attrelid = identity.indrelid
                AND key.attnum = key_binding.address_objsubid
               JOIN pg_catalog.pg_class AS relation
                 ON relation.oid = identity.indrelid
               WHERE identity.indexrelid = $2::bigint::oid
                 AND identity.indrelid = $3::bigint::oid
                 AND identity.indisprimary AND identity.indisunique
                 AND identity.indisvalid AND identity.indisready
                 AND identity.indnkeyatts = 1 AND identity.indnatts = 1
                 AND identity.indexprs IS NULL AND identity.indpred IS NULL
                 AND key.atttypid = 20 AND key.attnotnull
                 AND relation.relkind = 'r' AND relation.relreplident = 'd')",
        &[
            &source_id,
            &identity_index_oid,
            &relation_oid,
            &as_bigint(authority.target.bootstrap_id.get())?,
            &as_bigint(authority.target.slot_generation.get())?,
        ],
    )?;
    Ok(())
}
