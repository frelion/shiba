use postgres::Client;
use shiba_ingress::{RebuildIdentity, RebuildMemberIdentity, RebuildSpec};
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration, SourceId};
use shiba_runtime::{RebuildSourceTarget, compile_rebuild_graph};

use super::{NEW_SLOT, OLD_SLOT};

pub(crate) struct Fixture {
    pub(crate) old_relation: u32,
    pub(crate) old_identity: u32,
    pub(crate) old_publication: u32,
    pub(crate) target_relation: u32,
    pub(crate) target_identity: u32,
    pub(crate) target_publication: u32,
}

pub(crate) fn changed_object_rebuild(client: &mut Client, fixture: &Fixture) -> RebuildSpec {
    let old_digest: Vec<u8> = client
        .query_one(
            "SELECT graph_digest FROM shiba_internal.graph_definition WHERE graph_id=1",
            &[],
        )
        .expect("read active SQL graph digest")
        .get(0);
    let old_digest: [u8; 32] = old_digest.try_into().expect("32-byte graph digest");
    let target = RebuildSourceTarget {
        source_id: SourceId::new(1).expect("source ID"),
        relation_id: fixture.target_relation,
        identity_index_id: fixture.target_identity,
    };
    let mut transaction = client
        .transaction()
        .expect("open target compile transaction");
    let artifact = compile_rebuild_graph(
        &mut transaction,
        GraphId::new(1).expect("graph ID"),
        core::slice::from_ref(&target),
    )
    .expect("compile target graph from durable QuerySpec");
    transaction
        .rollback()
        .expect("rollback read-only target compile");
    RebuildSpec {
        graph_id: GraphId::new(1).expect("graph ID"),
        expected: identity(
            1,
            old_digest,
            fixture.old_relation,
            fixture.old_identity,
            fixture.old_publication,
            OLD_SLOT,
            1,
        ),
        target: identity(
            2,
            artifact.graph_digest,
            fixture.target_relation,
            fixture.target_identity,
            fixture.target_publication,
            NEW_SLOT,
            2,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn identity(
    bootstrap_id: u64,
    graph_digest: [u8; 32],
    relation_oid: u32,
    identity_index_oid: u32,
    publication_oid: u32,
    slot_name: &str,
    generation: u64,
) -> RebuildIdentity {
    RebuildIdentity {
        bootstrap_id: BootstrapId::new(bootstrap_id).expect("bootstrap ID"),
        graph_digest,
        members: vec![RebuildMemberIdentity {
            source_id: SourceId::new(1).expect("source ID"),
            relation_oid,
            identity_index_oid,
        }],
        publication_oid,
        slot_name: slot_name.to_owned(),
        slot_generation: SlotGeneration::new(generation).expect("generation"),
    }
}
