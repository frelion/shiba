use shiba_ingress::PreparedRebuild;

use crate::support::{
    IdentityCoordinates, RebuildFixture, assert_building, authority_snapshot,
    establish_active_source, options,
};

pub(crate) fn prove_relation_replacement_is_not_adopted(database_url: &str, replication_url: &str) {
    let (mut admin, active) = establish_active_source(database_url, replication_url);
    let fixture = RebuildFixture::install(&mut admin, active.publication_oid);
    let before = authority_snapshot(&mut admin);
    admin
        .batch_execute(
            "DROP TABLE target.events;
             CREATE TABLE target.events (id bigint PRIMARY KEY, payload bigint NULL);
             ALTER PUBLICATION shiba_m12_admission_pub ADD TABLE target.events;",
        )
        .expect("replace target relation with same name and SQL shape");
    assert!(
        PreparedRebuild::prepare(database_url, replication_url, fixture.spec(), options()).is_err(),
        "a same-name relation replacement cannot satisfy the approved ObjectAddress"
    );
    assert_eq!(authority_snapshot(&mut admin), before);
}

pub(crate) fn prove_publication_drift_requires_explicit_new_admission(
    database_url: &str,
    replication_url: &str,
) {
    let (mut admin, active) = establish_active_source(database_url, replication_url);
    let fixture = RebuildFixture::install(&mut admin, active.publication_oid);
    let before = authority_snapshot(&mut admin);
    admin
        .batch_execute(
            "DROP PUBLICATION shiba_m12_admission_pub;
             CREATE PUBLICATION shiba_m12_admission_pub FOR TABLE target.events
                WITH (publish = 'insert, update, delete');",
        )
        .expect("replace publication with the same name");
    assert!(
        PreparedRebuild::prepare(database_url, replication_url, fixture.spec(), options()).is_err(),
        "a same-name publication replacement cannot be adopted by an old OID request"
    );
    assert_eq!(authority_snapshot(&mut admin), before);

    let publication_oid: u32 = admin
        .query_one(
            "SELECT oid FROM pg_catalog.pg_publication WHERE pubname = 'shiba_m12_admission_pub'",
            &[],
        )
        .expect("read replacement publication ObjectAddress")
        .get(0);
    let fixture = fixture_with_publication(fixture, publication_oid);
    let prepared =
        PreparedRebuild::prepare(database_url, replication_url, fixture.spec(), options())
            .expect("explicitly approved replacement publication may prepare");
    admin
        .batch_execute(
            "ALTER PUBLICATION shiba_m12_admission_pub DROP TABLE target.events;
             ALTER PUBLICATION shiba_m12_admission_pub ADD TABLE target.events;",
        )
        .expect("change target publication membership after durable prepare");
    assert!(
        prepared.into_bootstrap().is_err(),
        "post-prepare publication membership drift must fail closed before snapshot"
    );
    assert_building(&mut admin);
}

pub(crate) fn prove_identity_shape_and_operator_plan(database_url: &str, replication_url: &str) {
    let (mut admin, active) = establish_active_source(database_url, replication_url);
    let mut fixture = RebuildFixture::install(&mut admin, active.publication_oid);
    let before = authority_snapshot(&mut admin);

    admin
        .batch_execute("ALTER TABLE target.events REPLICA IDENTITY FULL")
        .expect("change target replica identity shape");
    assert!(
        PreparedRebuild::prepare(database_url, replication_url, fixture.spec(), options()).is_err(),
        "non-default replica identity must fail before destructive prepare"
    );
    assert_eq!(authority_snapshot(&mut admin), before);
    admin
        .batch_execute("ALTER TABLE target.events REPLICA IDENTITY DEFAULT")
        .expect("restore default replica identity for next preflight");

    admin
        .batch_execute("ALTER TABLE target.events ALTER COLUMN payload TYPE text")
        .expect("change target payload shape");
    assert!(
        PreparedRebuild::prepare(database_url, replication_url, fixture.spec(), options()).is_err(),
        "column type drift must fail before destructive prepare"
    );
    assert_eq!(authority_snapshot(&mut admin), before);

    admin
        .batch_execute(
            "ALTER TABLE target.events ALTER COLUMN payload TYPE bigint USING payload::bigint;
             ALTER TABLE target.events DROP CONSTRAINT events_pkey;
             ALTER TABLE target.events ADD CONSTRAINT events_pkey PRIMARY KEY (id);",
        )
        .expect("restore shape while replacing primary-key ObjectAddress");
    let replacement_identity: u32 = admin
        .query_one("SELECT 'target.events_pkey'::regclass::oid", &[])
        .expect("read new primary-key ObjectAddress")
        .get(0);
    assert_ne!(replacement_identity, fixture.target.identity_index);
    assert!(
        PreparedRebuild::prepare(database_url, replication_url, fixture.spec(), options()).is_err(),
        "same-name primary-key replacement must not satisfy a stale target OID"
    );
    assert_eq!(authority_snapshot(&mut admin), before);

    assert!(
        PreparedRebuild::prepare(
            database_url,
            replication_url,
            fixture.spec_with(fixture.old, fixture.target, 2, 1, 1, 2, 3),
            options(),
        )
        .is_err(),
        "operator IDs are exact target-plan coordinates"
    );
    assert_eq!(authority_snapshot(&mut admin), before);

    fixture.target = IdentityCoordinates {
        identity_index: replacement_identity,
        ..fixture.target
    };
    admin
        .batch_execute(
            "ALTER TABLE target.events RENAME TO events_renamed;
             ALTER INDEX target.events_pkey RENAME TO events_renamed_pkey;
             CREATE INDEX target_events_payload_unrelated ON target.events_renamed (payload);",
        )
        .expect("apply ObjectAddress-stable and unrelated target DDL");
    let prepared =
        PreparedRebuild::prepare(database_url, replication_url, fixture.spec(), options())
            .expect("exact durable identities survive rename and ignore unrelated index DDL");
    assert_building(&mut admin);
    let binding: i64 = admin
        .query_one(
            "SELECT address_objid::bigint FROM shiba_internal.source_binding
             WHERE source_id = 1 AND binding_kind = 'identity_index'",
            &[],
        )
        .expect("read durable target identity index")
        .get(0);
    assert_eq!(binding, i64::from(replacement_identity));
    let sum_binding = admin
        .query_one(
            "SELECT input_objid::bigint, input_objsubid FROM shiba_internal.operator_definition
             WHERE operator_id = 2",
            &[],
        )
        .expect("read compiled SumInt8 target binding");
    assert_eq!(
        sum_binding.get::<_, i64>(0),
        i64::from(fixture.target.relation)
    );
    assert_eq!(sum_binding.get::<_, i32>(1), 2);
    prepared.detach().expect("release prepared rebuild owner");
}

fn fixture_with_publication(mut fixture: RebuildFixture, publication: u32) -> RebuildFixture {
    fixture.target = IdentityCoordinates {
        publication,
        ..fixture.target
    };
    fixture
}
