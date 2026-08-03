use shiba_ingress::PreparedRebuild;
use shiba_protocol::{
    IngressTransactionId, InputSequence, PostgresLsn, SlotGeneration, SourceId, SourceTransactionId,
};
use shiba_runtime::{M2Error, SourceInsert, SourcePayload, SourceTransaction, process};

#[path = "m12_rebuild_identity_authority/support.rs"]
mod support;

#[test]
#[ignore = "requires scripts/test-m12-rebuild-identity-authority.sh"]
#[allow(
    clippy::too_many_lines,
    reason = "ordered identity authority failure proof"
)]
fn rebuild_identity_is_durable_and_revalidated_before_slot_work() {
    let database_url = support::required("SHIBA_M12_IDENTITY_DATABASE_URL");
    let replication_url = support::required("SHIBA_M12_IDENTITY_REPLICATION_URL");
    let (mut admin, active) = support::establish_active_source(&database_url, &replication_url);
    let fixture = support::RebuildFixture::install(&mut admin, active.publication_oid);

    PreparedRebuild::prepare(
        &database_url,
        &replication_url,
        fixture.spec(),
        support::options(),
    )
    .expect("prepare exact target identity")
    .detach()
    .expect("release initial prepared worker");
    support::assert_exact_identity(
        &mut admin,
        fixture.target.relation,
        fixture.target.identity_index,
    );
    support::assert_prepared_closed(&mut admin, support::TARGET_SLOT);

    admin
        .batch_execute("ALTER INDEX target.events_pkey RENAME TO events_identity_renamed")
        .expect("rename exact identity index without changing ObjectAddress");
    assert_eq!(
        support::oid(&mut admin, "target.events_identity_renamed"),
        fixture.target.identity_index
    );
    let rename_invalidation = admin
        .query_one(
            "SELECT count(*), min(address_objid)::bigint
             FROM shiba_internal.source_invalidation WHERE source_id = 1",
            &[],
        )
        .expect("read exact rename invalidation");
    assert_eq!(rename_invalidation.get::<_, i64>(0), 1);
    assert_eq!(
        rename_invalidation.get::<_, Option<i64>>(1),
        Some(i64::from(fixture.target.identity_index))
    );
    support::resume(&database_url, &replication_url, 2, 3)
        .expect("stable identity index OID remains resumable after rename")
        .detach()
        .expect("detach renamed-index resume proof");
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba_internal.source_invalidation WHERE source_id = 1",
                &[],
            )
            .expect("rename invalidation must be consumed exactly")
            .get::<_, i64>(0),
        0
    );

    admin
        .batch_execute(
            "CREATE INDEX events_payload_unrelated ON target.events(payload);
             DROP INDEX target.events_payload_unrelated;",
        )
        .expect("perform unrelated index DDL");
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba_internal.source_invalidation WHERE source_id = 1",
                &[],
            )
            .expect("unrelated DDL must not invalidate target authority")
            .get::<_, i64>(0),
        0
    );
    support::resume(&database_url, &replication_url, 2, 3)
        .expect("unrelated index DDL must not contaminate exact authority")
        .detach()
        .expect("detach unrelated-index resume proof");

    let identity = i64::from(fixture.target.identity_index);
    let relation = i64::from(fixture.target.relation);
    for (corrupt, restore, label) in [
        (
            "DELETE FROM shiba_internal.source_binding
             WHERE source_id = 1 AND binding_kind = 'identity_index'"
                .to_owned(),
            format!(
                "INSERT INTO shiba_internal.source_binding VALUES
                 (1, 'identity_index', 'pg_class'::regclass, {identity}::oid, 0)"
            ),
            "missing identity binding",
        ),
        (
            "CREATE INDEX events_payload_wrong_object ON target.events(payload);
                 UPDATE shiba_internal.source_binding
                 SET address_objid = 'target.events_payload_wrong_object'::regclass
                 WHERE source_id = 1 AND binding_kind = 'identity_index'"
                .to_owned(),
            format!(
                "UPDATE shiba_internal.source_binding
                 SET address_objid = {identity}::oid
                 WHERE source_id = 1 AND binding_kind = 'identity_index';
                 DROP INDEX target.events_payload_wrong_object"
            ),
            "wrong identity object OID",
        ),
        (
            format!(
                "UPDATE shiba_internal.source_binding
                 SET binding_kind = 'column', address_objid = {relation}::oid,
                     address_objsubid = 3
                 WHERE source_id = 1 AND binding_kind = 'identity_index'"
            ),
            format!(
                "UPDATE shiba_internal.source_binding
                 SET binding_kind = 'identity_index', address_objid = {identity}::oid,
                     address_objsubid = 0
                 WHERE source_id = 1 AND binding_kind = 'column' AND address_objsubid = 3"
            ),
            "wrong binding kind, object and sub-id",
        ),
        (
            "UPDATE shiba_internal.source_binding SET source_id = 2
             WHERE source_id = 1 AND binding_kind = 'identity_index'"
                .to_owned(),
            "UPDATE shiba_internal.source_binding SET source_id = 1
             WHERE source_id = 2 AND binding_kind = 'identity_index'"
                .to_owned(),
            "foreign source binding",
        ),
    ] {
        admin
            .batch_execute(&corrupt)
            .expect("manufacture binding anomaly");
        assert!(
            support::resume(&database_url, &replication_url, 2, 3).is_err(),
            "fresh worker must reject {label}"
        );
        support::assert_prepared_closed(&mut admin, support::TARGET_SLOT);
        admin
            .batch_execute(&restore)
            .expect("restore exact test fixture");
    }

    admin
        .execute(
            "INSERT INTO shiba_internal.source_binding VALUES
               (1, 'column', 'pg_class'::regclass, $1::bigint::oid, 3)",
            &[&relation],
        )
        .expect("manufacture constraint-valid fifth binding for absent column 3");
    assert!(support::resume(&database_url, &replication_url, 2, 3).is_err());
    support::assert_prepared_closed(&mut admin, support::TARGET_SLOT);
    admin
        .execute(
            "DELETE FROM shiba_internal.source_binding
             WHERE source_id = 1 AND binding_kind = 'column'
               AND address_objid = $1::bigint::oid AND address_objsubid = 3",
            &[&relation],
        )
        .expect("restore extra-binding fixture");

    admin
        .batch_execute(
            "ALTER TABLE shiba_internal.source_binding
               DROP CONSTRAINT source_binding_address_classid_check;
             UPDATE shiba_internal.source_binding
             SET address_classid = 'pg_attribute'::regclass
             WHERE source_id = 1 AND binding_kind = 'identity_index';",
        )
        .expect("manufacture wrong address class under test owner");
    assert!(support::resume(&database_url, &replication_url, 2, 3).is_err());
    support::assert_prepared_closed(&mut admin, support::TARGET_SLOT);
    admin
        .batch_execute(
            "UPDATE shiba_internal.source_binding
             SET address_classid = 'pg_class'::regclass
             WHERE source_id = 1 AND binding_kind = 'identity_index';
             ALTER TABLE shiba_internal.source_binding
               ADD CONSTRAINT source_binding_address_classid_check
               CHECK (address_classid = 'pg_class'::regclass);",
        )
        .expect("restore address-class constraint and authority");

    let plan_digest: Vec<u8> = admin
        .query_one(
            "SELECT plan_digest FROM shiba_internal.operator_definition WHERE operator_id = 3",
            &[],
        )
        .expect("read durable ProjectRows plan digest")
        .get(0);
    admin
        .execute(
            "UPDATE shiba_internal.operator_definition
             SET plan_digest = decode(repeat('00', 32), 'hex') WHERE operator_id = 3",
            &[],
        )
        .expect("inject prepared plan-set drift");
    let drifted = support::prepared_snapshot(&mut admin, support::TARGET_SLOT);
    assert!(
        support::resume(&database_url, &replication_url, 2, 3).is_err(),
        "prepared worker must reject corrupt durable plan digest"
    );
    support::assert_prepared_closed(&mut admin, support::TARGET_SLOT);
    assert_eq!(
        support::prepared_snapshot(&mut admin, support::TARGET_SLOT),
        drifted
    );
    admin
        .execute(
            "UPDATE shiba_internal.operator_definition SET plan_digest = $1 WHERE operator_id = 3",
            &[&plan_digest],
        )
        .expect("restore exact ProjectRows plan digest");

    let resumed = support::resume(&database_url, &replication_url, 2, 3)
        .expect("exact restored authority resumes");
    support::activate_prepared_fixture(&mut admin, resumed);
    support::assert_exact_identity(
        &mut admin,
        fixture.target.relation,
        fixture.target.identity_index,
    );

    let second_spec = support::install_second_target(&mut admin, &fixture);
    support::assert_exact_identity(
        &mut admin,
        second_spec.expected.relation_oid,
        second_spec.expected.identity_index_oid,
    );
    PreparedRebuild::prepare(
        &database_url,
        &replication_url,
        second_spec.clone(),
        support::options(),
    )
    .expect("repeat rebuild consumes exact old four-row CAS")
    .detach()
    .expect("release second prepared worker");
    support::assert_exact_identity(
        &mut admin,
        second_spec.target.relation_oid,
        second_spec.target.identity_index_oid,
    );
    let old_index = second_spec.target.identity_index_oid;
    admin
        .batch_execute(
            "ALTER TABLE target_next.events DROP CONSTRAINT events_pkey;
             ALTER TABLE target_next.events ADD CONSTRAINT events_pkey PRIMARY KEY (id);",
        )
        .expect("replace same-name same-shape identity index");
    let replacement = support::oid(&mut admin, "target_next.events_pkey");
    assert_ne!(
        replacement, old_index,
        "drop/recreate must produce a fresh OID"
    );
    let replacement_invalidation = admin
        .query_one(
            "SELECT count(*), min(address_objid)::bigint
             FROM shiba_internal.source_invalidation WHERE source_id = 1",
            &[],
        )
        .expect("read replacement invalidation");
    assert_eq!(replacement_invalidation.get::<_, i64>(0), 1);
    assert_eq!(
        replacement_invalidation.get::<_, Option<i64>>(1),
        Some(i64::from(old_index))
    );
    let before = support::prepared_snapshot(&mut admin, support::SECOND_SLOT);
    assert!(
        support::resume(&database_url, &replication_url, 3, 4).is_err(),
        "fresh worker must reject same-name same-shape index replacement"
    );
    support::assert_prepared_closed(&mut admin, support::SECOND_SLOT);
    assert_eq!(
        support::prepared_snapshot(&mut admin, support::SECOND_SLOT),
        before
    );

    admin
        .execute(
            "DELETE FROM shiba_internal.source_invalidation
             WHERE source_id = 1 AND address_objid = $1::bigint::oid",
            &[&i64::from(old_index)],
        )
        .expect("remove only the test-manufactured replacement invalidation");
    let runtime_before = support::prepared_snapshot(&mut admin, support::SECOND_SLOT);
    let identity = SourceTransactionId::new(
        SourceId::new(1).expect("source ID"),
        SlotGeneration::new(4).expect("replacement generation"),
        PostgresLsn::from_u64(0x40_0000),
        IngressTransactionId::new(404).expect("ingress transaction ID"),
    )
    .expect("nonzero Runtime identity");
    let input = SourceTransaction::new(
        identity,
        vec![SourceInsert::with_payload(
            InputSequence::new(1).expect("input sequence"),
            404,
            SourcePayload::Null,
        )],
    )
    .expect("valid nullable-int8 INSERT transaction");
    assert!(matches!(
        process(&mut admin, &input),
        Err(M2Error::SourceInvalidated)
    ));
    support::assert_prepared_closed(&mut admin, support::SECOND_SLOT);
    assert_eq!(
        support::prepared_snapshot(&mut admin, support::SECOND_SLOT),
        runtime_before,
        "failed Runtime Apply must not mutate checkpoint, continuation, state, or result"
    );
}
