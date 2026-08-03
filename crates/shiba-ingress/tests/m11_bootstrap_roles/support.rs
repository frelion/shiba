use std::num::NonZeroU64;

use postgres::Client;
use shiba_compiler::{OPERATOR_SPEC_VERSION, OperatorOperationV1, OperatorSpecV1};
use shiba_ingress::BootstrapSpec;
use shiba_operator::OperatorId;
use shiba_protocol::{BootstrapId, SlotGeneration, SourceId};
use shiba_runtime::compile_and_register;

pub const SLOT: &str = "shiba_m11_roles_slot";
pub const SWAPPED_SLOT: &str = "shiba_m11_roles_swapped_slot";
pub const PUBLICATION: &str = "shiba_m11_roles_pub";
pub const SWAPPED_PUBLICATION: &str = "shiba_m11_roles_swapped_pub";
pub const CONTROL_ROLE: &str = "shiba_m11_bootstrap_control";
pub const RECEIVER_ROLE: &str = "shiba_m11_bootstrap_receiver";
pub const READER_ROLE: &str = "shiba_m11_result_reader";

pub fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("scripts/test-m11-bootstrap-roles.sh must set {name}"))
}

pub fn as_role(conninfo: &str, role: &str) -> String {
    format!("{conninfo} user={role}")
}

pub fn bootstrap_spec(
    source_id: u64,
    bootstrap_id: u64,
    publication_oid: u32,
    slot: &str,
) -> BootstrapSpec {
    BootstrapSpec {
        source_id: SourceId::new(source_id).expect("source ID"),
        bootstrap_id: BootstrapId::new(bootstrap_id).expect("bootstrap ID"),
        publication_oid,
        slot_name: slot.to_owned(),
        slot_generation: SlotGeneration::new(1).expect("slot generation"),
    }
}

pub fn install_fixture(admin: &mut Client) {
    admin
        .batch_execute(&format!(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source;
             CREATE TABLE source.events (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE TABLE source.swapped (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE PUBLICATION {PUBLICATION} FOR TABLE source.events
               WITH (publish = 'insert, update, delete');
             CREATE PUBLICATION {SWAPPED_PUBLICATION} FOR TABLE source.swapped
               WITH (publish = 'insert, update, delete');
             SELECT shiba_internal.register_source(1, 'source.events'::regclass);
             SELECT shiba_internal.register_source(2, 'source.swapped'::regclass);
             INSERT INTO source.events VALUES (1, 10), (2, NULL), (3, 30);
             INSERT INTO source.swapped VALUES (1, 1);
             CREATE ROLE {CONTROL_ROLE} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
             CREATE ROLE {RECEIVER_ROLE} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE REPLICATION;
             CREATE ROLE {READER_ROLE} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
             GRANT USAGE ON SCHEMA shiba_internal, shiba, source TO {CONTROL_ROLE};
             GRANT SELECT, UPDATE ON shiba_internal.source_binding TO {CONTROL_ROLE};
             GRANT SELECT ON shiba_internal.source_invalidation,
                 shiba_internal.source_ingress_config,
                 shiba_internal.source_ingress_invalidation,
                 shiba_internal.operator_definition TO {CONTROL_ROLE};
             GRANT SELECT, UPDATE ON shiba_internal.source_bootstrap TO {CONTROL_ROLE};
             GRANT SELECT, INSERT, UPDATE ON shiba_internal.source_continuation TO {CONTROL_ROLE};
             GRANT SELECT, INSERT, UPDATE, DELETE ON shiba_internal.source_row_state TO {CONTROL_ROLE};
             GRANT USAGE ON SEQUENCE shiba_internal.source_row_state_row_state_id_seq TO {CONTROL_ROLE};
             GRANT SELECT, UPDATE ON shiba_internal.operator_state TO {CONTROL_ROLE};
             GRANT SELECT, UPDATE ON shiba.operator_result TO {CONTROL_ROLE};
             GRANT SELECT ON source.events, source.swapped TO {CONTROL_ROLE};
             GRANT USAGE ON SCHEMA source TO {RECEIVER_ROLE};
             GRANT SELECT ON source.events, source.swapped TO {RECEIVER_ROLE};
             GRANT USAGE ON SCHEMA shiba TO {READER_ROLE};
             GRANT SELECT ON shiba.operator_result TO {READER_ROLE};"
        ))
        .expect("install sources and split roles");
    for (source_id, first_operator) in [(1, 1), (2, 3)] {
        compile_and_register(
            admin,
            &operator_spec(source_id, first_operator, OperatorOperationV1::CountRows),
        )
        .expect("register CountRows");
        compile_and_register(
            admin,
            &operator_spec(
                source_id,
                first_operator + 1,
                OperatorOperationV1::SumInt8 {
                    input_column: "payload".to_owned(),
                },
            ),
        )
        .expect("register SumInt8");
    }
}

fn operator_spec(
    source_id: u64,
    operator_id: u64,
    operation: OperatorOperationV1,
) -> OperatorSpecV1 {
    OperatorSpecV1 {
        version: OPERATOR_SPEC_VERSION,
        operator_id: OperatorId::new(NonZeroU64::new(operator_id).expect("operator ID")),
        source_id: SourceId::new(source_id).expect("source ID"),
        operation,
    }
}

pub fn load_publication_oid(client: &mut Client, publication: &str) -> u32 {
    client
        .query_one(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .expect("read publication")
        .get(0)
}

pub fn assert_results(
    client: &mut Client,
    first_operator: i64,
    status: &str,
    values: [Option<i64>; 2],
) {
    let second_operator = first_operator + 1;
    let actual = client
        .query(
            "SELECT result_status, value_bigint
             FROM shiba.operator_result
             WHERE operator_id IN ($1, $2) ORDER BY operator_id",
            &[&first_operator, &second_operator],
        )
        .expect("query public results")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect::<Vec<(String, Option<i64>)>>();
    assert_eq!(
        actual,
        values
            .into_iter()
            .map(|value| (status.to_owned(), value))
            .collect::<Vec<_>>()
    );
}

pub fn assert_no_apply(client: &mut Client, source_id: i64, expected_phase: Option<&str>) {
    assert_eq!(
        client
            .query_one(
                "SELECT count(*) FROM shiba_internal.source_row_state WHERE source_id = $1",
                &[&source_id],
            )
            .expect("count private rows")
            .get::<_, i64>(0),
        0
    );
    assert_eq!(
        client
            .query_one(
                "SELECT count(*) FROM shiba_internal.source_continuation WHERE source_id = $1",
                &[&source_id],
            )
            .expect("count continuations")
            .get::<_, i64>(0),
        0
    );
    let phase = client
        .query_opt(
            "SELECT phase FROM shiba_internal.source_bootstrap WHERE source_id = $1",
            &[&source_id],
        )
        .expect("query optional bootstrap")
        .map(|row| row.get::<_, String>(0));
    assert_eq!(phase.as_deref(), expected_phase);
    assert_eq!(
        client
            .query_one(
                "SELECT count(*)
                 FROM shiba_internal.operator_definition AS definition
                 JOIN shiba_internal.operator_state AS state USING (operator_id)
                 WHERE definition.source_id = $1
                   AND state.state_payload <> decode('0000000000000000', 'hex')",
                &[&source_id],
            )
            .expect("count changed private operator states")
            .get::<_, i64>(0),
        0
    );
}
