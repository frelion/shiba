use postgres::Client;
use shiba_ingress::{
    BootstrapCatchupProgress, PreparedRebuild, RebuildIdentity, RebuildMemberIdentity, RebuildSpec,
    SnapshotProgress,
};
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration, SourceId};
use shiba_runtime::{ProcessOutcome, RebuildSourceTarget, compile_rebuild_graph};

use super::{Fixtures, GraphFixture, assert_building, assert_oracle, options, wait_for_slot_lsn};

const TARGET_SLOT: &str = "m15_agg_group_sum_2";

pub(crate) fn rebuild_grouped_sum(
    database_url: &str,
    replication_url: &str,
    client: &mut Client,
    fixtures: &Fixtures,
) {
    let old = &fixtures.graphs[3];
    let spec = spec(client, old, fixtures);
    let prepared = PreparedRebuild::prepare(database_url, replication_url, &spec, options())
        .expect("prepare SQL aggregate changed-ObjectAddress rebuild");
    assert_building(client, old.graph);
    let mut bootstrap = prepared
        .into_bootstrap()
        .expect("export aggregate target snapshot");
    client
        .batch_execute(
            "BEGIN;
             UPDATE agg_group_sum_target.rows SET payload=200 WHERE id=12;
             INSERT INTO agg_group_sum_target.rows VALUES (13,NULL);
             DELETE FROM agg_group_sum_target.rows WHERE id=10;
             COMMIT;",
        )
        .expect("commit aggregate rebuild catch-up WAL");
    while let SnapshotProgress::BatchApplied { rows, .. } =
        bootstrap.scan_next().expect("scan aggregate target")
    {
        assert!((1..=2).contains(&rows));
        assert_building(client, old.graph);
    }
    let mut catchup = bootstrap
        .into_catchup()
        .expect("enter aggregate rebuild catch-up");
    assert_eq!(
        catchup.catch_up_next().expect("apply target aggregate WAL"),
        BootstrapCatchupProgress::TransactionApplied
    );
    assert_eq!(
        catchup.catch_up_next().expect("activate aggregate target"),
        BootstrapCatchupProgress::Active
    );
    let target = GraphFixture {
        schema: "agg_group_sum_target",
        publication_oid: fixtures.target_publication,
        slot: TARGET_SLOT,
        ..old.clone()
    };
    assert_oracle(client, &target);
    let relation: i64 = client
        .query_one(
            "SELECT address_objid::bigint FROM shiba_internal.source_binding
             WHERE source_id=4 AND binding_kind='relation'",
            &[],
        )
        .expect("read target aggregate relation authority")
        .get(0);
    assert_eq!(relation, i64::from(fixtures.target_relation));

    let mut live = catchup
        .into_live()
        .expect("enter rebuilt aggregate live receiver");
    client
        .batch_execute(
            "BEGIN;
             UPDATE agg_group_sum_target.rows SET payload=300 WHERE id=12;
             UPDATE agg_group_sum_target.rows SET id=14 WHERE id=13;
             COMMIT;",
        )
        .expect("commit post-rebuild aggregate WAL");
    let token = live
        .receive_and_apply_one()
        .expect("apply post-rebuild aggregate transaction");
    assert_eq!(token.outcome(), ProcessOutcome::Applied);
    assert_oracle(client, &target);
    live.acknowledge(&token)
        .expect("ACK rebuilt aggregate transaction");
    wait_for_slot_lsn(client, TARGET_SLOT, token.end_lsn());
    live.detach().expect("detach rebuilt aggregate graph");
}

fn spec(client: &mut Client, old: &GraphFixture, fixtures: &Fixtures) -> RebuildSpec {
    let old_digest: Vec<u8> = client
        .query_one(
            "SELECT graph_digest FROM shiba_internal.graph_definition WHERE graph_id=4",
            &[],
        )
        .expect("read SQL aggregate graph digest")
        .get(0);
    let old_digest: [u8; 32] = old_digest.try_into().expect("32-byte graph digest");
    let old_relation = relation_oid(client, old.source);
    let old_identity = identity_oid(client, old.source);
    let target = RebuildSourceTarget {
        source_id: SourceId::new(old.source).expect("source ID"),
        relation_id: fixtures.target_relation,
        identity_index_id: fixtures.target_identity,
    };
    let mut transaction = client.transaction().expect("open target compilation");
    let artifact = compile_rebuild_graph(
        &mut transaction,
        GraphId::new(old.graph).expect("graph ID"),
        core::slice::from_ref(&target),
    )
    .expect("rebind durable aggregate QuerySpec");
    transaction.rollback().expect("rollback read-only compile");
    RebuildSpec {
        graph_id: GraphId::new(old.graph).expect("graph ID"),
        expected: identity(
            old.graph,
            old_digest,
            old.source,
            old_relation,
            old_identity,
            old.publication_oid,
            old.slot,
            1,
        ),
        target: identity(
            old.graph + 1,
            artifact.graph_digest,
            old.source,
            fixtures.target_relation,
            fixtures.target_identity,
            fixtures.target_publication,
            TARGET_SLOT,
            2,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn identity(
    bootstrap: u64,
    digest: [u8; 32],
    source: u64,
    relation: u32,
    index: u32,
    publication: u32,
    slot: &str,
    generation: u64,
) -> RebuildIdentity {
    RebuildIdentity {
        bootstrap_id: BootstrapId::new(bootstrap).expect("bootstrap ID"),
        graph_digest: digest,
        members: vec![RebuildMemberIdentity {
            source_id: SourceId::new(source).expect("source ID"),
            relation_oid: relation,
            identity_index_oid: index,
        }],
        publication_oid: publication,
        slot_name: slot.to_owned(),
        slot_generation: SlotGeneration::new(generation).expect("generation"),
    }
}

fn relation_oid(client: &mut Client, source: u64) -> u32 {
    binding_oid(client, source, "relation")
}

fn identity_oid(client: &mut Client, source: u64) -> u32 {
    binding_oid(client, source, "identity_index")
}

fn binding_oid(client: &mut Client, source: u64, kind: &str) -> u32 {
    client
        .query_one(
            "SELECT address_objid FROM shiba_internal.source_binding
             WHERE source_id=$1 AND binding_kind=$2",
            &[&i64::try_from(source).expect("source ID fits"), &kind],
        )
        .expect("read exact source binding")
        .get(0)
}
