use postgres::Client;
use shiba_ingress::{RebuildIdentity, RebuildMemberIdentity, RebuildSpec};
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration, SourceId};
use shiba_runtime::{RebuildSourceTarget, compile_rebuild_graph};

use super::Fixture;

pub(crate) fn same_binding_rebuild(
    client: &mut Client,
    fixture: &Fixture,
    old_slot: &str,
    new_slot: &str,
) -> RebuildSpec {
    let digest: Vec<u8> = client
        .query_one(
            "SELECT graph_digest FROM shiba_internal.graph_definition WHERE graph_id=1",
            &[],
        )
        .expect("read active graph digest")
        .get(0);
    let digest: [u8; 32] = digest.try_into().expect("32-byte graph digest");
    let targets = [
        RebuildSourceTarget {
            source_id: SourceId::new(1).expect("left source ID"),
            relation_id: fixture.left_relation,
            identity_index_id: fixture.left_identity,
        },
        RebuildSourceTarget {
            source_id: SourceId::new(2).expect("right source ID"),
            relation_id: fixture.right_relation,
            identity_index_id: fixture.right_identity,
        },
    ];
    let mut transaction = client.transaction().expect("open compilation transaction");
    let artifact = compile_rebuild_graph(
        &mut transaction,
        GraphId::new(1).expect("graph ID"),
        &targets,
    )
    .expect("compile same-binding rebuild graph");
    transaction
        .rollback()
        .expect("rollback read-only compilation");
    assert_eq!(artifact.graph_digest, digest);
    let members = vec![
        RebuildMemberIdentity {
            source_id: SourceId::new(1).expect("left source ID"),
            relation_oid: fixture.left_relation,
            identity_index_oid: fixture.left_identity,
        },
        RebuildMemberIdentity {
            source_id: SourceId::new(2).expect("right source ID"),
            relation_oid: fixture.right_relation,
            identity_index_oid: fixture.right_identity,
        },
    ];
    RebuildSpec {
        graph_id: GraphId::new(1).expect("graph ID"),
        expected: rebuild_identity(fixture, digest, members.clone(), old_slot, 1, 1),
        target: rebuild_identity(fixture, digest, members, new_slot, 2, 2),
    }
}

fn rebuild_identity(
    fixture: &Fixture,
    digest: [u8; 32],
    members: Vec<RebuildMemberIdentity>,
    slot: &str,
    bootstrap: u64,
    generation: u64,
) -> RebuildIdentity {
    RebuildIdentity {
        bootstrap_id: BootstrapId::new(bootstrap).expect("bootstrap ID"),
        graph_digest: digest,
        members,
        publication_oid: fixture.publication_oid,
        slot_name: slot.to_owned(),
        slot_generation: SlotGeneration::new(generation).expect("slot generation"),
    }
}

pub(crate) fn assert_generation(client: &mut Client, generation: i64, slot: &str) {
    let row = client
        .query_one(
            "SELECT bootstrap.slot_generation,bootstrap.slot_name::text,
                    bool_and(continuation.slot_generation=$2)
             FROM shiba_internal.graph_bootstrap AS bootstrap
             JOIN shiba_internal.graph_continuation AS continuation USING (graph_id)
             WHERE bootstrap.graph_id=1 AND bootstrap.slot_name=$1::text::name
             GROUP BY bootstrap.slot_generation,bootstrap.slot_name",
            &[&slot, &generation],
        )
        .expect("read sole graph generation and continuation");
    assert_eq!(row.get::<_, i64>(0), generation);
    assert_eq!(row.get::<_, &str>(1), slot);
    assert!(row.get::<_, bool>(2));
}

pub(crate) fn assert_continuations(client: &mut Client, generation: i64, count: i64) {
    let row = client
        .query_one(
            "SELECT count(*), COALESCE(bool_and(slot_generation=$1), true)
             FROM shiba_internal.graph_continuation WHERE graph_id=1",
            &[&generation],
        )
        .expect("read graph continuation cardinality");
    assert_eq!(row.get::<_, i64>(0), count);
    assert!(row.get::<_, bool>(1));
}

pub(crate) fn assert_feedback(client: &mut Client, slot: &str, expected: u64) {
    for _ in 0..10_000 {
        if slot_lsn(client, slot) >= expected {
            return;
        }
    }
    panic!("slot {slot} did not confirm exact durable end LSN {expected:#x}");
}

pub(crate) fn slot_lsn(client: &mut Client, slot: &str) -> u64 {
    let value: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_catalog.pg_replication_slots
             WHERE slot_name=$1",
            &[&slot],
        )
        .expect("read slot confirmed flush LSN")
        .get(0);
    let (high, low) = value.split_once('/').expect("PostgreSQL LSN shape");
    (u64::from_str_radix(high, 16).expect("LSN high") << 32)
        | u64::from_str_radix(low, 16).expect("LSN low")
}
