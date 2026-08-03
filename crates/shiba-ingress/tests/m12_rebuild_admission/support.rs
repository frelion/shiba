use std::{num::NonZeroU64, time::Duration};

use postgres::Client;
use shiba_ingress::{BootstrapOptions, RebuildIdentity, RebuildSpec};
use shiba_operator::OperatorId;
use shiba_protocol::{BootstrapId, SlotGeneration, SourceId};

#[path = "../m12_rebuild_contract/support.rs"]
#[allow(dead_code)]
mod active_support;

pub(crate) use active_support::{OLD_SLOT, authority_snapshot, establish_active_source, required};

pub(crate) const TARGET_SLOT: &str = "shiba_m12_admission_new";
pub(crate) const TARGET_PUBLICATION: &str = "shiba_m12_admission_pub";
pub(crate) const BAD_PUBLICATION: &str = "shiba_m12_admission_bad_pub";

#[derive(Clone, Copy)]
pub(crate) struct IdentityCoordinates {
    pub(crate) relation: u32,
    pub(crate) identity_index: u32,
    pub(crate) publication: u32,
}

pub(crate) struct RebuildFixture {
    pub(crate) old: IdentityCoordinates,
    pub(crate) target: IdentityCoordinates,
    pub(crate) bad_target: IdentityCoordinates,
}

impl RebuildFixture {
    pub(crate) fn install(client: &mut Client, old_publication: u32) -> Self {
        client
            .batch_execute(&format!(
                "CREATE SCHEMA target;
                 CREATE TABLE target.events (
                     id bigint PRIMARY KEY,
                     payload bigint NULL
                 );
                 INSERT INTO target.events VALUES (10, 100), (11, NULL);
                 CREATE PUBLICATION {TARGET_PUBLICATION} FOR TABLE target.events
                   WITH (publish = 'insert, update, delete');

                 CREATE TABLE target.bad_events (
                     id bigint PRIMARY KEY,
                     payload text NULL
                 );
                 CREATE PUBLICATION {BAD_PUBLICATION} FOR TABLE target.bad_events
                   WITH (publish = 'insert, update, delete');"
            ))
            .expect("install explicit valid and invalid rebuild targets");

        let old = IdentityCoordinates {
            relation: object_oid(client, "source.events"),
            identity_index: identity_index_oid(client, "source.events"),
            publication: old_publication,
        };
        let target = IdentityCoordinates {
            relation: object_oid(client, "target.events"),
            identity_index: object_oid(client, "target.events_pkey"),
            publication: publication_oid(client, TARGET_PUBLICATION),
        };
        let bad_target = IdentityCoordinates {
            relation: object_oid(client, "target.bad_events"),
            identity_index: object_oid(client, "target.bad_events_pkey"),
            publication: publication_oid(client, BAD_PUBLICATION),
        };
        Self {
            old,
            target,
            bad_target,
        }
    }

    pub(crate) fn spec(&self) -> RebuildSpec {
        self.spec_with(self.old, self.target, 1, 2, 1, 2, 3)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spec_with(
        &self,
        old: IdentityCoordinates,
        target: IdentityCoordinates,
        count_operator: u64,
        sum_operator: u64,
        old_bootstrap: u64,
        new_bootstrap: u64,
        new_generation: u64,
    ) -> RebuildSpec {
        let _ = self;
        RebuildSpec {
            source_id: SourceId::new(1).expect("source ID"),
            expected: RebuildIdentity {
                bootstrap_id: BootstrapId::new(old_bootstrap).expect("old bootstrap ID"),
                relation_oid: old.relation,
                identity_index_oid: old.identity_index,
                publication_oid: old.publication,
                slot_name: OLD_SLOT.to_owned(),
                slot_generation: SlotGeneration::new(2).expect("old generation"),
            },
            target: RebuildIdentity {
                bootstrap_id: BootstrapId::new(new_bootstrap).expect("new bootstrap ID"),
                relation_oid: target.relation,
                identity_index_oid: target.identity_index,
                publication_oid: target.publication,
                slot_name: TARGET_SLOT.to_owned(),
                slot_generation: SlotGeneration::new(new_generation).expect("new generation"),
            },
            count_operator_id: OperatorId::new(
                NonZeroU64::new(count_operator).expect("count operator ID"),
            ),
            sum_operator_id: OperatorId::new(
                NonZeroU64::new(sum_operator).expect("sum operator ID"),
            ),
        }
    }
}

pub(crate) fn options() -> BootstrapOptions {
    BootstrapOptions::new(2, Duration::from_secs(5)).expect("bounded rebuild options")
}

pub(crate) fn full_authority_snapshot(client: &mut Client) -> Vec<Vec<String>> {
    let mut snapshot = authority_snapshot(client);
    for query in [
        "SELECT row_to_json(x)::text FROM (
             SELECT * FROM shiba_internal.source_invalidation ORDER BY source_id
         ) x",
        "SELECT row_to_json(x)::text FROM (
             SELECT * FROM shiba_internal.source_ingress_invalidation ORDER BY source_id
         ) x",
        "SELECT row_to_json(x)::text FROM (
             SELECT * FROM shiba_internal.operator_definition ORDER BY operator_id
         ) x",
    ] {
        snapshot.push(
            client
                .query(query, &[])
                .expect("snapshot complete rebuild authority")
                .into_iter()
                .map(|row| row.get(0))
                .collect(),
        );
    }
    snapshot
}

pub(crate) fn grant_prepare(client: &mut Client, role: &str) {
    client
        .batch_execute(&format!(
            "GRANT USAGE ON SCHEMA shiba_internal TO {role};
             GRANT EXECUTE ON FUNCTION shiba_internal.prepare_source_rebuild(
                 bigint, bigint, oid, oid, oid, name, bigint,
                 bigint, bigint, integer,
                 bigint, regclass, regclass, oid, name, bigint
             ) TO {role};"
        ))
        .expect("grant only rebuild admission entrypoint");
}

fn object_oid(client: &mut Client, name: &str) -> u32 {
    client
        .query_one("SELECT $1::text::regclass::oid", &[&name])
        .expect("resolve exact object address")
        .get(0)
}

fn publication_oid(client: &mut Client, name: &str) -> u32 {
    client
        .query_one(
            "SELECT oid FROM pg_catalog.pg_publication WHERE pubname = $1",
            &[&name],
        )
        .expect("resolve exact publication identity")
        .get(0)
}

fn identity_index_oid(client: &mut Client, relation: &str) -> u32 {
    client
        .query_one(
            "SELECT indexrelid
             FROM pg_catalog.pg_index
             WHERE indrelid = $1::text::regclass AND indisreplident",
            &[&relation],
        )
        .or_else(|_| {
            client.query_one(
                "SELECT indexrelid
                 FROM pg_catalog.pg_index
                 WHERE indrelid = $1::text::regclass AND indisprimary",
                &[&relation],
            )
        })
        .expect("resolve current replica identity index")
        .get(0)
}
