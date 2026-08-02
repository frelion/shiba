use core::str::FromStr;

use postgres::{Client, IsolationLevel, Transaction};
use shiba_protocol::{PostgresLsn, SlotGeneration, SourceId};
use shiba_runtime::PgoutputSource;

use crate::{
    IngressError,
    publication::{PublicationSnapshot, load_live},
    source_shape::derive_source,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GovernedConfig {
    pub(crate) source_id: SourceId,
    pub(crate) generation: SlotGeneration,
    pub(crate) database_name: String,
    pub(crate) publication_name: String,
    pub(crate) slot_name: String,
    pub(crate) source: PgoutputSource,
    pub(crate) streamed_admitted: bool,
    database_oid: i64,
    publication_oid: i64,
    relation_oid: i64,
    publication_snapshot: PublicationSnapshot,
}

struct ConfigAuthority {
    publication_classid: i64,
    publication_oid: i64,
    publication: PublicationSnapshot,
    slot_name: String,
    relation_oid: i64,
}

impl GovernedConfig {
    pub(crate) fn load(
        client: &mut Client,
        source_id: SourceId,
        generation: SlotGeneration,
        slot_active: bool,
    ) -> Result<(Self, u64), IngressError> {
        let mut transaction = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()?;
        let loaded = Self::load_snapshot(&mut transaction, source_id, generation, slot_active)?;
        transaction.commit()?;
        Ok(loaded)
    }

    pub(crate) fn revalidate(
        &self,
        client: &mut Client,
        slot_active: bool,
    ) -> Result<(), IngressError> {
        let (current, _) = Self::load(client, self.source_id, self.generation, slot_active)?;
        if current != *self {
            return Err(IngressError::Governance("source configuration drifted"));
        }
        Ok(())
    }

    fn load_snapshot(
        transaction: &mut Transaction<'_>,
        source_id: SourceId,
        generation: SlotGeneration,
        slot_active: bool,
    ) -> Result<(Self, u64), IngressError> {
        let source_key = as_bigint(source_id.get())?;
        let generation_key = as_bigint(generation.get())?;
        let database = transaction.query_one(
            "SELECT database.oid::bigint, database.datname::text
             FROM pg_catalog.pg_database AS database
             WHERE database.datname = pg_catalog.current_database()",
            &[],
        )?;
        let database_oid: i64 = database.get(0);
        let database_name: String = database.get(1);

        let authority =
            load_config_authority(transaction, source_key, generation_key, database_oid)?;

        let class_oids = transaction.query_one(
            "SELECT 'pg_catalog.pg_publication'::regclass::oid::bigint,
                    'pg_catalog.pg_class'::regclass::oid::bigint",
            &[],
        )?;
        let expected_publication_class: i64 = class_oids.get(0);
        let relation_class: i64 = class_oids.get(1);
        if authority.publication_classid != expected_publication_class {
            return Err(IngressError::Governance(
                "publication ObjectAddress class mismatch",
            ));
        }
        reject_invalidations(transaction, source_key)?;

        let publication = load_live(
            transaction,
            authority.publication_oid,
            authority.relation_oid,
        )?;
        if publication != authority.publication {
            return Err(IngressError::Governance("publication snapshot drifted"));
        }
        validate_generation_continuity(transaction, source_key, generation_key)?;
        let source = derive_source(
            transaction,
            source_id,
            generation,
            relation_class,
            authority.relation_oid,
            &publication.attnums,
        )?;
        let confirmed_lsn =
            validate_slot(transaction, &authority.slot_name, database_oid, slot_active)?;

        Ok((
            Self {
                source_id,
                generation,
                database_name,
                publication_name: publication.name.clone(),
                slot_name: authority.slot_name,
                source,
                streamed_admitted: publication.attnums == [1],
                database_oid,
                publication_oid: authority.publication_oid,
                relation_oid: authority.relation_oid,
                publication_snapshot: publication,
            },
            confirmed_lsn,
        ))
    }
}

fn load_config_authority(
    transaction: &mut Transaction<'_>,
    source_id: i64,
    generation: i64,
    database_oid: i64,
) -> Result<ConfigAuthority, IngressError> {
    let row = transaction
        .query_opt(
            "SELECT config.database_oid::bigint, config.source_binding_kind,
                    config.source_binding_objsubid,
                    config.publication_classid::bigint,
                    config.publication_objid::bigint, config.publication_objsubid,
                    config.publication_name::text, config.publication_insert,
                    config.publication_update, config.publication_delete,
                    config.publication_truncate, config.publication_via_root,
                    config.publication_attnums,
                    config.slot_name::text, config.slot_generation,
                    binding.address_objid::bigint
             FROM shiba_internal.source_ingress_config AS config
             JOIN shiba_internal.source_binding AS binding
               ON binding.source_id = config.source_id
              AND binding.binding_kind = 'relation'
              AND binding.address_objsubid = 0
             WHERE config.source_id = $1",
            &[&source_id],
        )?
        .ok_or(IngressError::Governance("source ingress config is missing"))?;
    if row.get::<_, i64>(0) != database_oid || row.get::<_, i64>(14) != generation {
        return Err(IngressError::Governance("database or generation mismatch"));
    }
    if row.get::<_, &str>(1) != "relation" || row.get::<_, i32>(2) != 0 || row.get::<_, i32>(5) != 0
    {
        return Err(IngressError::Governance(
            "config ObjectAddress shape mismatch",
        ));
    }
    Ok(ConfigAuthority {
        publication_classid: row.get(3),
        publication_oid: row.get(4),
        publication: PublicationSnapshot {
            name: row.get(6),
            insert: row.get(7),
            update: row.get(8),
            delete: row.get(9),
            truncate: row.get(10),
            via_root: row.get(11),
            attnums: row.get(12),
        },
        slot_name: row.get(13),
        relation_oid: row.get(15),
    })
}

fn reject_invalidations(
    transaction: &mut Transaction<'_>,
    source_id: i64,
) -> Result<(), IngressError> {
    let invalid: bool = transaction
        .query_one(
            "SELECT EXISTS (
                 SELECT 1 FROM shiba_internal.source_invalidation WHERE source_id = $1
                 UNION ALL
                 SELECT 1 FROM shiba_internal.source_ingress_invalidation WHERE source_id = $1
             )",
            &[&source_id],
        )?
        .get(0);
    if invalid {
        return Err(IngressError::Governance(
            "source or publication is invalidated",
        ));
    }
    Ok(())
}

fn validate_generation_continuity(
    transaction: &mut Transaction<'_>,
    source_id: i64,
    generation: i64,
) -> Result<(), IngressError> {
    let mismatch: bool = transaction
        .query_one(
            "SELECT EXISTS (
                 SELECT 1 FROM shiba_internal.source_continuation
                 WHERE source_id = $1 AND slot_generation <> $2
                 UNION ALL
                 SELECT 1 FROM shiba_internal.applied_insert
                 WHERE source_id = $1 AND slot_generation <> $2
             )",
            &[&source_id, &generation],
        )?
        .get(0);
    if mismatch {
        return Err(IngressError::Governance("durable generation mismatch"));
    }
    Ok(())
}

fn validate_slot(
    transaction: &mut Transaction<'_>,
    slot_name: &str,
    database_oid: i64,
    active: bool,
) -> Result<u64, IngressError> {
    let row = transaction
        .query_opt(
            "SELECT slot.confirmed_flush_lsn::text
             FROM pg_catalog.pg_replication_slots AS slot
             WHERE slot.slot_name = $1 AND slot.slot_type = 'logical'
               AND slot.plugin = 'pgoutput' AND slot.datoid = $2::bigint::oid
               AND NOT slot.temporary AND slot.active = $3",
            &[&slot_name, &database_oid, &active],
        )?
        .ok_or(IngressError::Governance("slot state is not admitted"))?;
    let lsn: Option<&str> = row.get(0);
    let lsn = lsn.ok_or(IngressError::Governance("slot has no confirmed LSN"))?;
    PostgresLsn::from_str(lsn)
        .map(PostgresLsn::as_u64)
        .map_err(|_| IngressError::Governance("slot LSN is invalid"))
}

fn as_bigint(value: u64) -> Result<i64, IngressError> {
    i64::try_from(value).map_err(|_| IngressError::Governance("identity exceeds bigint"))
}
