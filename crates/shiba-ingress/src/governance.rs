use core::str::FromStr;

use postgres::{Client, IsolationLevel, Transaction};
use shiba_protocol::{GraphId, PostgresLsn, SlotGeneration, SourceId};
use shiba_runtime::{PgoutputGraph, PgoutputSource};

use crate::{
    IngressError,
    publication::{PublicationSnapshot, load_live},
    source_shape::derive_source,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GovernedConfig {
    pub(crate) graph_id: GraphId,
    pub(crate) generation: SlotGeneration,
    pub(crate) database_name: String,
    pub(crate) publication_name: String,
    pub(crate) slot_name: String,
    pub(crate) graph: PgoutputGraph,
    pub(crate) streamed_admitted: bool,
    database_oid: i64,
    publication_oid: i64,
    publication_snapshot: PublicationSnapshot,
    graph_digest: [u8; 32],
}

struct ConfigAuthority {
    publication_classid: i64,
    publication_oid: i64,
    publication: PublicationSnapshot,
    slot_name: String,
    graph_digest: [u8; 32],
    members: Vec<(SourceId, i64, Vec<i16>)>,
}

impl GovernedConfig {
    pub(crate) fn load(
        client: &mut Client,
        graph_id: GraphId,
        generation: SlotGeneration,
        slot_active: bool,
    ) -> Result<(Self, u64), IngressError> {
        let mut transaction = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()?;
        let loaded = Self::load_snapshot(&mut transaction, graph_id, generation, slot_active)?;
        transaction.commit()?;
        Ok(loaded)
    }

    pub(crate) fn revalidate(
        &self,
        client: &mut Client,
        slot_active: bool,
    ) -> Result<(), IngressError> {
        let (current, _) = Self::load(client, self.graph_id, self.generation, slot_active)?;
        if current != *self {
            return Err(IngressError::Governance("graph configuration drifted"));
        }
        Ok(())
    }

    fn load_snapshot(
        transaction: &mut Transaction<'_>,
        graph_id: GraphId,
        generation: SlotGeneration,
        slot_active: bool,
    ) -> Result<(Self, u64), IngressError> {
        let graph_key = as_bigint(graph_id.get())?;
        let generation_key = as_bigint(generation.get())?;
        let database = transaction.query_one(
            "SELECT oid::bigint, datname::text FROM pg_catalog.pg_database
             WHERE datname = pg_catalog.current_database()",
            &[],
        )?;
        let database_oid: i64 = database.get(0);
        let database_name: String = database.get(1);
        let authority =
            load_config_authority(transaction, graph_key, generation_key, database_oid)?;
        let class_oids = transaction.query_one(
            "SELECT 'pg_catalog.pg_publication'::regclass::oid::bigint,
                    'pg_catalog.pg_class'::regclass::oid::bigint",
            &[],
        )?;
        if authority.publication_classid != class_oids.get::<_, i64>(0) {
            return Err(IngressError::Governance(
                "publication ObjectAddress class mismatch",
            ));
        }
        reject_invalidations(transaction, graph_key)?;
        let live = load_live(transaction, authority.publication_oid)?;
        if live != authority.publication {
            return Err(IngressError::Governance("publication snapshot drifted"));
        }
        validate_generation_continuity(
            transaction,
            graph_key,
            generation_key,
            &authority.graph_digest,
        )?;
        let mut sources = Vec::<PgoutputSource>::with_capacity(authority.members.len());
        for (source_id, relation_oid, attnums) in &authority.members {
            sources.push(derive_source(
                transaction,
                *source_id,
                generation,
                class_oids.get(1),
                *relation_oid,
                attnums,
            )?);
        }
        let streamed_admitted = sources.len() == 1 && authority.members[0].2.len() == 1;
        let graph = PgoutputGraph::new(graph_id, generation, sources)?;
        let confirmed_lsn =
            validate_slot(transaction, &authority.slot_name, database_oid, slot_active)?;
        Ok((
            Self {
                graph_id,
                generation,
                database_name,
                publication_name: live.name.clone(),
                slot_name: authority.slot_name,
                graph,
                streamed_admitted,
                database_oid,
                publication_oid: authority.publication_oid,
                publication_snapshot: live,
                graph_digest: authority.graph_digest,
            },
            confirmed_lsn,
        ))
    }
}

fn load_config_authority(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    generation: i64,
    database_oid: i64,
) -> Result<ConfigAuthority, IngressError> {
    let row = transaction
        .query_opt(
            "SELECT config.database_oid::bigint, config.publication_classid::bigint,
                config.publication_objid::bigint, config.publication_objsubid,
                config.publication_name::text, config.publication_insert,
                config.publication_update, config.publication_delete,
                config.publication_truncate, config.publication_via_root,
                config.slot_name::text, config.slot_generation, config.graph_digest
         FROM shiba_internal.graph_ingress_config AS config WHERE config.graph_id = $1",
            &[&graph_id],
        )?
        .ok_or(IngressError::Governance("graph ingress config is missing"))?;
    if row.get::<_, i64>(0) != database_oid
        || row.get::<_, i64>(11) != generation
        || row.get::<_, i32>(3) != 0
    {
        return Err(IngressError::Governance("database or generation mismatch"));
    }
    let digest = exact_digest(row.get(12))?;
    let publication_oid: i64 = row.get(2);
    let live = load_live(transaction, publication_oid)?;
    let member_rows = transaction.query(
        "SELECT member.source_id, binding.address_objid::bigint,
                ingress.publication_attnums
         FROM shiba_internal.graph_source_member AS member
         JOIN shiba_internal.graph_ingress_source AS ingress
           ON (ingress.graph_id, ingress.source_id) = (member.graph_id, member.source_id)
         JOIN shiba_internal.source_binding AS binding
           ON binding.source_id = member.source_id AND binding.binding_kind = 'relation'
          AND binding.address_objsubid = 0
         WHERE member.graph_id = $1 ORDER BY member.input_ordinal",
        &[&graph_id],
    )?;
    if member_rows.is_empty() || member_rows.len() > 2 || member_rows.len() != live.members.len() {
        return Err(IngressError::Governance(
            "graph member authority is incomplete",
        ));
    }
    let members = member_rows
        .into_iter()
        .map(|member| {
            let raw: i64 = member.get(0);
            let source_id = u64::try_from(raw)
                .ok()
                .and_then(|value| SourceId::new(value).ok())
                .ok_or(IngressError::Governance("source identity is invalid"))?;
            let relation_oid: i64 = member.get(1);
            let attnums: Vec<i16> = member.get(2);
            if live.members.get(&relation_oid) != Some(&attnums) {
                return Err(IngressError::Governance(
                    "publication member snapshot drifted",
                ));
            }
            Ok((source_id, relation_oid, attnums))
        })
        .collect::<Result<Vec<_>, IngressError>>()?;
    let stored = PublicationSnapshot {
        name: row.get(4),
        insert: row.get(5),
        update: row.get(6),
        delete: row.get(7),
        truncate: row.get(8),
        via_root: row.get(9),
        members: live.members.clone(),
    };
    Ok(ConfigAuthority {
        publication_classid: row.get(1),
        publication_oid,
        publication: stored,
        slot_name: row.get(10),
        graph_digest: digest,
        members,
    })
}

fn reject_invalidations(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
) -> Result<(), IngressError> {
    let invalid: bool = transaction
        .query_one(
            "SELECT EXISTS (
             SELECT 1 FROM shiba_internal.graph_ingress_invalidation WHERE graph_id = $1
             UNION ALL
             SELECT 1 FROM shiba_internal.source_invalidation AS invalid
             JOIN shiba_internal.graph_source_member AS member USING (source_id)
             WHERE member.graph_id = $1)",
            &[&graph_id],
        )?
        .get(0);
    if invalid {
        return Err(IngressError::Governance(
            "graph source or publication is invalidated",
        ));
    }
    Ok(())
}

fn validate_generation_continuity(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    generation: i64,
    digest: &[u8; 32],
) -> Result<(), IngressError> {
    let mismatch: bool = transaction
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM shiba_internal.graph_continuation
         WHERE graph_id = $1 AND (slot_generation <> $2 OR graph_digest <> $3))",
            &[&graph_id, &generation, &digest.as_slice()],
        )?
        .get(0);
    if mismatch {
        return Err(IngressError::Governance(
            "durable graph generation mismatch",
        ));
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
            "SELECT confirmed_flush_lsn::text FROM pg_catalog.pg_replication_slots
         WHERE slot_name = $1 AND slot_type = 'logical' AND plugin = 'pgoutput'
           AND datoid = $2::bigint::oid AND NOT temporary AND active = $3
           AND NOT two_phase AND NOT failover AND NOT synced",
            &[&slot_name, &database_oid, &active],
        )?
        .ok_or(IngressError::Governance("slot state is not admitted"))?;
    let lsn: Option<&str> = row.get(0);
    PostgresLsn::from_str(lsn.ok_or(IngressError::Governance("slot has no confirmed LSN"))?)
        .map(PostgresLsn::as_u64)
        .map_err(|_| IngressError::Governance("slot LSN is invalid"))
}

fn exact_digest(value: Vec<u8>) -> Result<[u8; 32], IngressError> {
    value
        .try_into()
        .map_err(|_| IngressError::Governance("graph digest is invalid"))
}

fn as_bigint(value: u64) -> Result<i64, IngressError> {
    i64::try_from(value).map_err(|_| IngressError::Governance("identity exceeds bigint"))
}
