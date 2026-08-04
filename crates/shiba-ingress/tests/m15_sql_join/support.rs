use postgres::Client;
use shiba_ingress::GovernedGraphSession;
use shiba_operator::OperatorGraph;

#[allow(dead_code, unused_imports)]
#[path = "../m14_join_lifecycle/support.rs"]
mod m14;
#[path = "support/oracle.rs"]
mod oracle_support;
#[path = "support/rebuild.rs"]
mod rebuild;
#[path = "support/roles.rs"]
mod roles;

pub(crate) use m14::{assert_continuations, assert_feedback, assert_generation, slot_lsn};
pub(crate) use oracle_support::{assert_old_oracle, assert_target_oracle, oracle};
pub(crate) use rebuild::changed_object_rebuild;
pub(crate) use roles::{
    CONTROL_ROLE, READER_ROLE, RECEIVER_ROLE, as_role, assert_no_registered_graph,
    assert_reader_building, assert_reader_matches, assert_role_shape, grant_bootstrap_control,
    grant_registration_control, prove_missing_bootstrap_grant,
};

pub(crate) const OLD_SLOT: &str = "shiba_m15_sql_join_1";
pub(crate) const NEW_SLOT: &str = "shiba_m15_sql_join_2";
const OLD_PUBLICATION: &str = "shiba_m15_sql_join_old_pub";
const TARGET_PUBLICATION: &str = "shiba_m15_sql_join_target_pub";

pub(crate) struct Fixture {
    pub(crate) old: m14::Fixture,
    pub(crate) target_publication: u32,
    pub(crate) target_left_relation: u32,
    pub(crate) target_left_identity: u32,
    pub(crate) target_right_relation: u32,
    pub(crate) target_right_identity: u32,
}

pub(crate) fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("scripts/test-m15-sql-join.sh must set {name}"))
}

pub(crate) fn install(client: &mut Client) -> Fixture {
    let old = m14::install(client, OLD_PUBLICATION);
    client
        .batch_execute(
            "CREATE SCHEMA join_target_left;
             CREATE SCHEMA join_target_right;
             CREATE TABLE join_target_left.events (
                 id bigint PRIMARY KEY, right_key bigint NULL
             );
             CREATE TABLE join_target_right.events (
                 id bigint PRIMARY KEY, payload bigint NULL
             );
             INSERT INTO join_target_left.events VALUES (10,100),(11,200),(12,NULL);
             INSERT INTO join_target_right.events VALUES (100,300),(200,NULL);
             CREATE PUBLICATION shiba_m15_sql_join_target_pub
                 FOR TABLE join_target_left.events, join_target_right.events
                 WITH (publish='insert, update, delete');",
        )
        .expect("install changed-ObjectAddress SQL join target");
    roles::install(client);
    Fixture {
        old,
        target_publication: publication_oid(client, TARGET_PUBLICATION),
        target_left_relation: oid(client, "join_target_left.events"),
        target_left_identity: oid(client, "join_target_left.events_pkey"),
        target_right_relation: oid(client, "join_target_right.events"),
        target_right_identity: oid(client, "join_target_right.events_pkey"),
    }
}

pub(crate) fn assert_sql_registration(
    client: &mut Client,
    fixture: &m14::Fixture,
    graph: &OperatorGraph,
) {
    m14::assert_registered(client, fixture);
    let row = client
        .query_one(
            "SELECT spec_payload,graph_payload,graph_digest
             FROM shiba_internal.graph_definition WHERE graph_id=1",
            &[],
        )
        .expect("read SQL join authority");
    let spec: Vec<u8> = row.get(0);
    let text = std::str::from_utf8(&spec).expect("canonical QuerySpec JSON");
    assert!(!text.contains("SELECT"));
    assert!(!text.contains("left_source"));
    assert!(!text.contains("right_source"));
    assert_eq!(row.get::<_, Vec<u8>>(1), graph.canonical_payload);
    assert_eq!(row.get::<_, Vec<u8>>(2), graph.digest.to_vec());
}

pub(crate) fn scan_all(bootstrap: &mut shiba_ingress::BootstrapSession, client: &mut Client) {
    m14::scan_all(bootstrap, client);
}

pub(crate) fn assert_building(client: &mut Client) {
    m14::assert_building(client);
}

pub(crate) fn assert_one_snapshot_two_sources(client: &mut Client) {
    let row = client
        .query_one(
            "SELECT consistent_point IS NOT NULL,
                    (SELECT count(*) FROM shiba_internal.graph_bootstrap_checkpoint
                     WHERE graph_id=bootstrap.graph_id),
                    (SELECT count(DISTINCT source_id)
                     FROM shiba_internal.source_row_state WHERE source_id IN (1,2))
             FROM shiba_internal.graph_bootstrap AS bootstrap WHERE graph_id=1",
            &[],
        )
        .expect("read one exported snapshot and both scanned sources");
    assert!(row.get::<_, bool>(0));
    assert_eq!(row.get::<_, i64>(1), 2);
    assert_eq!(row.get::<_, i64>(2), 2);
}

pub(crate) fn invalidate_identity_and_assert_fail_closed(
    client: &mut Client,
    session: &mut GovernedGraphSession,
) {
    let before = durable_snapshot(client);
    let feedback = slot_lsn(client, OLD_SLOT);
    let old_identity = oid(client, "right_source.events_pkey");
    client
        .batch_execute(
            "ALTER TABLE right_source.events DROP CONSTRAINT events_pkey;
             ALTER TABLE right_source.events ADD CONSTRAINT events_pkey PRIMARY KEY (id);
             BEGIN;
             UPDATE right_source.events SET payload=220 WHERE id=10;
             UPDATE left_source.events SET right_key=20 WHERE id=3;
             COMMIT;",
        )
        .expect("invalidate identity and commit both-source WAL");
    let replacement_identity = oid(client, "right_source.events_pkey");
    assert_ne!(replacement_identity, old_identity);
    let invalidation = client
        .query_one(
            "SELECT address_objid::bigint,address_objsubid
             FROM shiba_internal.source_invalidation WHERE source_id=2",
            &[],
        )
        .expect("read exact identity-index invalidation");
    assert_eq!(invalidation.get::<_, i64>(0), i64::from(old_identity));
    assert_eq!(invalidation.get::<_, i32>(1), 0);
    assert!(session.receive_and_apply_one().is_err());
    assert_eq!(durable_snapshot(client), before);
    assert_eq!(slot_lsn(client, OLD_SLOT), feedback);
}

pub(crate) fn assert_target_authority(
    client: &mut Client,
    fixture: &Fixture,
    expected_digest: [u8; 32],
) {
    let bindings = client
        .query(
            "SELECT source_id,binding_kind,address_objid::bigint
             FROM shiba_internal.source_binding
             WHERE binding_kind IN ('relation','identity_index')
               AND source_id IN (1,2) ORDER BY source_id,binding_kind",
            &[],
        )
        .expect("read rebuilt SQL join authorities")
        .into_iter()
        .map(|row| {
            (
                row.get::<_, i64>(0),
                row.get::<_, String>(1),
                row.get::<_, i64>(2),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        bindings,
        vec![
            (
                1,
                "identity_index".to_owned(),
                i64::from(fixture.target_left_identity),
            ),
            (
                1,
                "relation".to_owned(),
                i64::from(fixture.target_left_relation),
            ),
            (
                2,
                "identity_index".to_owned(),
                i64::from(fixture.target_right_identity),
            ),
            (
                2,
                "relation".to_owned(),
                i64::from(fixture.target_right_relation),
            ),
        ]
    );
    let authority = client
        .query_one(
            "SELECT definition.graph_digest=config.graph_digest,
                    definition.graph_digest=$1,
                    config.slot_generation=2 AND config.slot_name=$2::text::name
             FROM shiba_internal.graph_definition AS definition
             JOIN shiba_internal.graph_ingress_config AS config USING (graph_id)
             WHERE graph_id=1",
            &[&&expected_digest[..], &NEW_SLOT],
        )
        .expect("read target graph digest and generation authority");
    assert!(authority.get::<_, bool>(0));
    assert!(authority.get::<_, bool>(1));
    assert!(authority.get::<_, bool>(2));
}

fn durable_snapshot(client: &mut Client) -> String {
    client
        .query_one(
            "SELECT jsonb_build_object(
                'rows',(SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY source_id,source_row_id),'[]')
                        FROM shiba_internal.source_row_state AS s WHERE source_id IN (1,2)),
                'state',(SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY node_id,partition_key_payload),'[]')
                         FROM shiba_internal.graph_node_state AS s WHERE graph_id=1),
                'results',(SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY row_identity),'[]')
                           FROM shiba_internal.graph_result_row AS r WHERE graph_id=1),
                'continuation',(SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY commit_lsn),'[]')
                                FROM shiba_internal.graph_continuation AS c WHERE graph_id=1)
             )::text",
            &[],
        )
        .expect("read durable SQL join snapshot")
        .get(0)
}

fn oid(client: &mut Client, name: &str) -> u32 {
    client
        .query_one("SELECT $1::text::regclass::oid", &[&name])
        .expect("resolve object address")
        .get(0)
}

fn publication_oid(client: &mut Client, name: &str) -> u32 {
    client
        .query_one(
            "SELECT oid FROM pg_catalog.pg_publication WHERE pubname=$1",
            &[&name],
        )
        .expect("resolve publication identity")
        .get(0)
}
