use std::time::Duration;

use shiba_ingress::{
    AttachOptions, BootstrapOptions, BootstrapSession, BootstrapSpec, GovernedGraphSession,
    ReplicationMode,
};
use shiba_operator::TypedValue;
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration};

#[path = "m12_rebuild_contract/support.rs"]
mod support;

use support::{
    FOREIGN_SLOT, NEW_SLOT, OLD_SLOT, authority_snapshot, establish_active_source,
    recreate_foreign_slot, required, shiba_authority_snapshot,
};

#[test]
#[ignore = "requires scripts/test-m12-rebuild-contract.sh"]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered failure-first authority proof"
)]
fn active_source_rejects_pristine_replacement_without_mutation() {
    let database_url = required("SHIBA_M12_REBUILD_DATABASE_URL");
    let replication_url = required("SHIBA_M12_REBUILD_REPLICATION_URL");
    let (mut admin, active) = establish_active_source(&database_url, &replication_url);

    let active_fact = admin
        .query_one(
            "SELECT bootstrap.phase, bootstrap.bootstrap_id, config.slot_generation,
                    (SELECT count(*) FROM shiba_internal.source_row_state WHERE source_id = 1),
                    (SELECT array_agg(encode(state.state_payload, 'hex')
                                      ORDER BY state.node_id, state.namespace,
                                               state.partition_key_payload, state.item_key_payload)
                     FROM shiba_internal.graph_node_state state
                     WHERE state.graph_id = 1
                       AND state.partition_key_payload = $1
                       AND state.item_key_payload = $2),
                    (SELECT array_agg(
                         (SELECT CASE WHEN convert_from(row_payload,'UTF8')::jsonb #>> '{values,0,type}'='null'
                                      THEN NULL ELSE (convert_from(row_payload,'UTF8')::jsonb #>> '{values,0,value}')::bigint END
                          FROM shiba.graph_result_rows row
                          WHERE row.graph_id=result.graph_id AND row.result_id=result.result_id
                            AND result.result_id IN (4,5))
                         ORDER BY result.result_id)
                     FROM shiba.graph_result result WHERE result.graph_id = 1),
                    (SELECT count(*) FROM shiba_internal.graph_continuation WHERE graph_id = 1)
             FROM shiba_internal.graph_bootstrap bootstrap
             JOIN shiba_internal.graph_ingress_config config USING (graph_id)
             WHERE bootstrap.graph_id = 1",
            &[
                &TypedValue::Bool(true)
                    .to_canonical_json()
                    .expect("canonical scalar state partition"),
                &b"null".as_slice(),
            ],
        )
        .expect("prove active non-pristine authority");
    assert_eq!(active_fact.get::<_, &str>(0), "active");
    assert_eq!(active_fact.get::<_, i64>(1), 1);
    assert_eq!(active_fact.get::<_, i64>(2), 2);
    assert!(active_fact.get::<_, i64>(3) > 0);
    assert_eq!(
        active_fact.get::<_, Vec<String>>(4),
        vec![
            "0000000000000004",
            "0000000000000004",
            "0000000000000004",
            "00000000000000030000000000000020",
        ]
    );
    assert_eq!(
        active_fact.get::<_, Vec<Option<i64>>>(5),
        vec![Some(4), Some(32), None]
    );
    assert!(active_fact.get::<_, i64>(6) > 0);
    let old_slot = admin
        .query_one(
            "SELECT active, confirmed_flush_lsn IS NOT NULL
             FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&OLD_SLOT],
        )
        .expect("query inactive acknowledged old slot");
    assert!(!old_slot.get::<_, bool>(0));
    assert!(old_slot.get::<_, bool>(1));

    let before = authority_snapshot(&mut admin);
    assert!(
        admin
            .execute(
                "SELECT shiba_internal.rotate_graph_ingress_slot(1, 2, $1::text::name)",
                &[&NEW_SLOT],
            )
            .is_err(),
        "M10 pristine rotation must not rebuild active state"
    );
    assert_eq!(authority_snapshot(&mut admin), before);

    assert!(
        admin
            .execute(
                "SELECT shiba_internal.replace_pristine_graph_bootstrap(
                    1, 1, $1::text::name, 2, 2, $2::bigint::oid, $3::text::name, 3)",
                &[&OLD_SLOT, &i64::from(active.publication_oid), &NEW_SLOT],
            )
            .is_err(),
        "M11 pre-scan replacement must not rebuild active state"
    );
    assert_eq!(authority_snapshot(&mut admin), before);

    assert!(
        GovernedGraphSession::attach(
            &database_url,
            &replication_url,
            GraphId::new(1).expect("graph ID"),
            SlotGeneration::new(1).expect("stale generation"),
            AttachOptions::new(ReplicationMode::Committed, Duration::from_secs(5))
                .expect("attach options"),
        )
        .is_err(),
        "a worker presenting a generation other than the durable authority must fail"
    );
    assert_eq!(authority_snapshot(&mut admin), before);

    admin
        .query_one(
            "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&FOREIGN_SLOT],
        )
        .expect("preoccupy observable foreign target slot");
    let shiba_before_replacement = shiba_authority_snapshot(&mut admin);
    let (observable_before, observable_after) = recreate_foreign_slot(&mut admin);
    assert_eq!(
        observable_after, observable_before,
        "PG17/18 expose no immutable birth identity for a same-shape slot replacement"
    );
    assert_eq!(
        shiba_authority_snapshot(&mut admin),
        shiba_before_replacement,
        "the test-owned transport replacement must not mutate Shiba authority"
    );
    let with_foreign = authority_snapshot(&mut admin);
    let replacement = BootstrapSpec {
        graph_id: active.graph_id,
        bootstrap_id: BootstrapId::new(2).expect("replacement bootstrap ID"),
        publication_oid: active.publication_oid,
        slot_name: FOREIGN_SLOT.to_owned(),
        slot_generation: SlotGeneration::new(3).expect("replacement generation"),
    };
    assert!(
        BootstrapSession::restart_abandoned(
            &database_url,
            &replication_url,
            &active,
            replacement,
            BootstrapOptions::new(2, Duration::from_secs(5)).expect("bootstrap options"),
        )
        .is_err(),
        "an observable preoccupied target slot must never be adopted"
    );
    assert_eq!(authority_snapshot(&mut admin), with_foreign);

    admin
        .execute(
            "SELECT pg_catalog.pg_drop_replication_slot($1)",
            &[&FOREIGN_SLOT],
        )
        .expect("drop test-owned foreign slot");
    assert_eq!(authority_snapshot(&mut admin), before);
}
