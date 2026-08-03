use std::{num::NonZeroU64, time::Duration};

use postgres::Client;
use shiba_ingress::{BootstrapOptions, PreparedRebuild, RebuildIdentity, RebuildSpec};
use shiba_operator::OperatorId;
use shiba_protocol::{BootstrapId, SlotGeneration, SourceId};

#[path = "../m12_rebuild_admission/support.rs"]
#[allow(dead_code, unused_imports)]
mod admission;

pub(crate) use admission::{OLD_SLOT, RebuildFixture, TARGET_SLOT, establish_active_source};

pub(crate) const SECOND_SLOT: &str = "shiba_m12_identity_next";
pub(crate) const SECOND_PUBLICATION: &str = "shiba_m12_identity_next_pub";

pub(crate) fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!("scripts/test-m12-rebuild-identity-authority.sh must set {name}")
    })
}

pub(crate) fn options() -> BootstrapOptions {
    BootstrapOptions::new(2, Duration::from_secs(5)).expect("bounded rebuild options")
}

pub(crate) fn resume(
    database_url: &str,
    replication_url: &str,
    bootstrap: u64,
    generation: u64,
) -> Result<PreparedRebuild, shiba_ingress::IngressError> {
    PreparedRebuild::resume_prepared(
        database_url,
        replication_url,
        SourceId::new(1).expect("source ID"),
        BootstrapId::new(bootstrap).expect("bootstrap ID"),
        SlotGeneration::new(generation).expect("slot generation"),
        options(),
    )
}

pub(crate) fn assert_exact_identity(client: &mut Client, relation_oid: u32, index_oid: u32) {
    let rows = client
        .query(
            "SELECT binding_kind, address_classid::bigint, address_objid::bigint,
                    address_objsubid
             FROM shiba_internal.source_binding
             WHERE source_id = 1
             ORDER BY binding_kind, address_objsubid",
            &[],
        )
        .expect("read exact durable source identity");
    let pg_class: i64 = client
        .query_one("SELECT 'pg_class'::regclass::oid::bigint", &[])
        .expect("read pg_class address")
        .get(0);
    assert_eq!(
        rows.into_iter()
            .map(|row| {
                (
                    row.get::<_, String>(0),
                    row.get::<_, i64>(1),
                    row.get::<_, i64>(2),
                    row.get::<_, i32>(3),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            ("column".to_owned(), pg_class, i64::from(relation_oid), 1),
            ("column".to_owned(), pg_class, i64::from(relation_oid), 2),
            (
                "identity_index".to_owned(),
                pg_class,
                i64::from(index_oid),
                0
            ),
            ("relation".to_owned(), pg_class, i64::from(relation_oid), 0),
        ]
    );
    let approved: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_index
                WHERE indexrelid = $1::bigint::oid
                  AND indrelid = $2::bigint::oid
                  AND indisprimary AND indisunique AND indisvalid AND indisready
                  AND indnkeyatts = 1 AND indnatts = 1
                  AND (indkey::smallint[])[0] = 1
                  AND indexprs IS NULL AND indpred IS NULL
             )",
            &[&i64::from(index_oid), &i64::from(relation_oid)],
        )
        .expect("validate approved identity index")
        .get(0);
    assert!(
        approved,
        "durable identity binding must name the approved live index"
    );
}

pub(crate) fn assert_prepared_closed(client: &mut Client, target_slot: &str) {
    let row = client
        .query_one(
            "SELECT phase,
                    last_batch_ordinal = 0
                    AND last_source_row_id IS NULL
                    AND last_batch_digest IS NULL
                    AND consistent_point IS NULL
                    AND catchup_fence_lsn IS NULL
                    AND activation_end_lsn IS NULL
             FROM shiba_internal.source_bootstrap WHERE source_id = 1",
            &[],
        )
        .expect("read prepared lifecycle");
    assert_eq!(row.get::<_, &str>(0), "rebuild_prepared");
    assert!(
        row.get::<_, bool>(1),
        "prepared checkpoint must remain empty"
    );
    let durable = client
        .query_one(
            "SELECT
                (SELECT count(*) FROM shiba_internal.source_row_state WHERE source_id = 1),
                (SELECT count(*) FROM shiba_internal.source_continuation WHERE source_id = 1),
                (SELECT count(*) FROM shiba_internal.operator_state WHERE value_bigint <> 0),
                (SELECT count(*) FROM shiba.operator_result
                 WHERE result_status <> 'building' OR value_bigint IS NOT NULL),
                (SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1)",
            &[&target_slot],
        )
        .expect("prove no scan, Apply, publication, or target slot entry");
    for column in 0..5 {
        assert_eq!(durable.get::<_, i64>(column), 0);
    }
}

pub(crate) fn prepared_snapshot(client: &mut Client, target_slot: &str) -> Vec<String> {
    let mut snapshot = Vec::new();
    for query in [
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.source_binding ORDER BY binding_kind, address_objsubid) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.source_bootstrap WHERE source_id = 1) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.source_ingress_config WHERE source_id = 1) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.operator_state ORDER BY operator_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba.operator_result ORDER BY operator_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.source_continuation ORDER BY commit_lsn) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.source_invalidation ORDER BY source_id) x",
    ] {
        snapshot.extend(
            client
                .query(query, &[])
                .expect("snapshot prepared authority")
                .into_iter()
                .map(|row| row.get(0)),
        );
    }
    snapshot.push(
        client
            .query_one(
                "SELECT count(*)::text FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&target_slot],
            )
            .expect("snapshot target physical slot")
            .get(0),
    );
    snapshot
}

pub(crate) fn activate_prepared_fixture(client: &mut Client, prepared: PreparedRebuild) {
    prepared
        .detach()
        .expect("release prepared worker before test-owned activation fixture");
    client
        .execute(
            "SELECT pg_catalog.pg_drop_replication_slot($1)",
            &[&OLD_SLOT],
        )
        .expect("retire exact old test slot");
    let lsn: String = client
        .query_one(
            "SELECT lsn::text
             FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&TARGET_SLOT],
        )
        .expect("create exact target fixture slot at a real nonzero LSN")
        .get(0);
    client
        .execute(
            "UPDATE shiba_internal.source_bootstrap
             SET phase = 'active', consistent_point = $1::text::pg_lsn,
                 catchup_fence_lsn = $1::text::pg_lsn,
                 activation_end_lsn = $1::text::pg_lsn
             WHERE source_id = 1 AND bootstrap_id = 2
               AND slot_name = $2::text::name AND slot_generation = 3
               AND phase = 'rebuild_prepared'",
            &[&lsn, &TARGET_SLOT],
        )
        .expect("promote the same durable target identity for CAS-only fixture");
    client
        .batch_execute(&format!(
            "INSERT INTO shiba_internal.source_row_state
                (source_id, source_row_id, source_row_sub_id,
                 payload_present, payload_int8, payload_text)
             VALUES
                (1, 10, NULL, true, 100, NULL),
                (1, 11, NULL, true, NULL, NULL);
             UPDATE shiba_internal.operator_state
             SET value_bigint = CASE operator_id WHEN 1 THEN 2 ELSE 100 END
             WHERE operator_id IN (1, 2);
             UPDATE shiba.operator_result
             SET result_status = 'active',
                 value_bigint = CASE operator_id WHEN 1 THEN 2 ELSE 100 END
             WHERE operator_id IN (1, 2);
             INSERT INTO shiba_internal.source_continuation
                 (source_id, slot_generation, commit_lsn, ingress_transaction_id)
             VALUES (1, 3, '{lsn}'::pg_lsn, 1);"
        ))
        .expect("install non-pristine active state for second old-identity CAS");
}

pub(crate) fn install_second_target(client: &mut Client, fixture: &RebuildFixture) -> RebuildSpec {
    client
        .batch_execute(&format!(
            "CREATE SCHEMA target_next;
             CREATE TABLE target_next.events (id bigint PRIMARY KEY, payload bigint NULL);
             INSERT INTO target_next.events VALUES (20, 4), (21, NULL);
             CREATE PUBLICATION {SECOND_PUBLICATION} FOR TABLE target_next.events
               WITH (publish = 'insert, update, delete');"
        ))
        .expect("install second explicit rebuild target");
    let relation = oid(client, "target_next.events");
    let index = oid(client, "target_next.events_pkey");
    let publication: u32 = client
        .query_one(
            "SELECT oid FROM pg_catalog.pg_publication WHERE pubname = $1",
            &[&SECOND_PUBLICATION],
        )
        .expect("read second publication OID")
        .get(0);
    RebuildSpec {
        source_id: SourceId::new(1).expect("source ID"),
        expected: RebuildIdentity {
            bootstrap_id: BootstrapId::new(2).expect("old bootstrap ID"),
            relation_oid: fixture.target.relation,
            identity_index_oid: fixture.target.identity_index,
            publication_oid: fixture.target.publication,
            slot_name: TARGET_SLOT.to_owned(),
            slot_generation: SlotGeneration::new(3).expect("old generation"),
        },
        target: RebuildIdentity {
            bootstrap_id: BootstrapId::new(3).expect("new bootstrap ID"),
            relation_oid: relation,
            identity_index_oid: index,
            publication_oid: publication,
            slot_name: SECOND_SLOT.to_owned(),
            slot_generation: SlotGeneration::new(4).expect("new generation"),
        },
        count_operator_id: OperatorId::new(NonZeroU64::new(1).expect("count ID")),
        sum_operator_id: OperatorId::new(NonZeroU64::new(2).expect("sum ID")),
    }
}

pub(crate) fn oid(client: &mut Client, object: &str) -> u32 {
    client
        .query_one("SELECT $1::text::regclass::oid", &[&object])
        .expect("resolve object OID")
        .get(0)
}
