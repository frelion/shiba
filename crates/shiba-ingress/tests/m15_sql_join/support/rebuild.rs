use postgres::Client;
use shiba_ingress::{RebuildIdentity, RebuildMemberIdentity, RebuildSpec};
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration, SourceId};
use shiba_runtime::{RebuildSourceTarget, compile_rebuild_graph};

use super::{Fixture, NEW_SLOT, OLD_SLOT};

pub(crate) fn changed_object_rebuild(client: &mut Client, fixture: &Fixture) -> RebuildSpec {
    let digest: Vec<u8> = client
        .query_one(
            "SELECT graph_digest FROM shiba_internal.graph_definition WHERE graph_id=1",
            &[],
        )
        .expect("read active SQL join digest")
        .get(0);
    let digest: [u8; 32] = digest.try_into().expect("32-byte graph digest");
    let targets = [
        RebuildSourceTarget {
            source_id: SourceId::new(1).expect("left source ID"),
            relation_id: fixture.target_left_relation,
            identity_index_id: fixture.target_left_identity,
        },
        RebuildSourceTarget {
            source_id: SourceId::new(2).expect("right source ID"),
            relation_id: fixture.target_right_relation,
            identity_index_id: fixture.target_right_identity,
        },
    ];
    let mut transaction = client.transaction().expect("open target graph compilation");
    let artifact = compile_rebuild_graph(
        &mut transaction,
        GraphId::new(1).expect("graph ID"),
        &targets,
    )
    .expect("compile SQL declaration against target ObjectAddresses");
    transaction
        .rollback()
        .expect("rollback read-only target compilation");
    assert_ne!(artifact.graph_digest, digest);
    RebuildSpec {
        graph_id: GraphId::new(1).expect("graph ID"),
        expected: RebuildIdentity {
            bootstrap_id: BootstrapId::new(1).expect("bootstrap ID"),
            graph_digest: digest,
            members: vec![
                member(1, fixture.old.left_relation, fixture.old.left_identity),
                member(2, fixture.old.right_relation, fixture.old.right_identity),
            ],
            publication_oid: fixture.old.publication_oid,
            slot_name: OLD_SLOT.to_owned(),
            slot_generation: SlotGeneration::new(1).expect("generation"),
        },
        target: RebuildIdentity {
            bootstrap_id: BootstrapId::new(2).expect("bootstrap ID"),
            graph_digest: artifact.graph_digest,
            members: vec![
                member(
                    1,
                    fixture.target_left_relation,
                    fixture.target_left_identity,
                ),
                member(
                    2,
                    fixture.target_right_relation,
                    fixture.target_right_identity,
                ),
            ],
            publication_oid: fixture.target_publication,
            slot_name: NEW_SLOT.to_owned(),
            slot_generation: SlotGeneration::new(2).expect("generation"),
        },
    }
}

fn member(source: u64, relation: u32, identity: u32) -> RebuildMemberIdentity {
    RebuildMemberIdentity {
        source_id: SourceId::new(source).expect("source ID"),
        relation_oid: relation,
        identity_index_oid: identity,
    }
}
