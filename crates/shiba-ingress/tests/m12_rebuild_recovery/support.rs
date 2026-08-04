use std::{process::Command, time::Duration};

use postgres::Client;
use shiba_ingress::{BootstrapOptions, BootstrapSpec};
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration};

#[path = "../m12_rebuild_admission/support.rs"]
#[allow(dead_code, unused_imports)]
mod admission;

pub(crate) use admission::{OLD_SLOT, RebuildFixture, TARGET_SLOT, establish_active_source};

pub(crate) const RECOVERY_SLOTS: [&str; 4] = [
    "shiba_m12_recovery_1",
    "shiba_m12_recovery_2",
    "shiba_m12_recovery_3",
    "shiba_m12_recovery_4",
];
pub(crate) const APPLICATION: &str = "shiba_m12_recovery_receiver";

#[derive(Clone, Copy)]
pub(crate) struct Attempt {
    pub(crate) bootstrap: u64,
    pub(crate) generation: u64,
    pub(crate) slot: &'static str,
    pub(crate) publication: u32,
}

impl Attempt {
    pub(crate) fn spec(self) -> BootstrapSpec {
        BootstrapSpec {
            graph_id: GraphId::new(1).expect("graph ID"),
            bootstrap_id: BootstrapId::new(self.bootstrap).expect("bootstrap ID"),
            publication_oid: self.publication,
            slot_name: self.slot.to_owned(),
            slot_generation: SlotGeneration::new(self.generation).expect("generation"),
        }
    }
}

pub(crate) fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("scripts/test-m12-rebuild-recovery.sh must set {name}"))
}

pub(crate) fn options() -> BootstrapOptions {
    BootstrapOptions::new(2, Duration::from_secs(5)).expect("bounded recovery options")
}

pub(crate) fn extend_target(client: &mut Client) {
    client
        .batch_execute(
            "INSERT INTO target.events VALUES
               (12, 30), (13, NULL), (14, -5), (15, 9);",
        )
        .expect("install three bounded snapshot batches");
}

pub(crate) fn evidence(client: &mut Client) -> Vec<Vec<String>> {
    [
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.source_binding ORDER BY binding_kind, address_objsubid) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.graph_ingress_config ORDER BY graph_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.graph_bootstrap ORDER BY graph_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.source_row_state ORDER BY source_row_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.graph_node_state ORDER BY graph_id, node_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba.graph_result ORDER BY graph_id, result_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.graph_continuation ORDER BY slot_generation, commit_lsn) x",
        "SELECT row_to_json(x)::text FROM (
           SELECT slot_name, slot_type, plugin, database, temporary, active,
                  two_phase, failover, synced, restart_lsn::text, confirmed_flush_lsn::text
           FROM pg_catalog.pg_replication_slots ORDER BY slot_name) x",
    ]
    .into_iter()
    .map(|query| {
        client
            .query(query, &[])
            .expect("capture rebuild recovery evidence")
            .into_iter()
            .map(|row| row.get(0))
            .collect()
    })
    .collect()
}

pub(crate) fn assert_building(client: &mut Client) {
    let row = client
        .query_one(
            "SELECT count(*) FILTER (WHERE result_status = 'building'),
                    count(*) FROM shiba.graph_result",
            &[],
        )
        .expect("query rebuilding visibility");
    assert_eq!(row.get::<_, i64>(0), row.get::<_, i64>(1));
}

pub(crate) fn assert_oracle(client: &mut Client) {
    let oracle = client
        .query_one(
            "SELECT count(*), COALESCE(sum(payload), 0)::bigint FROM target.events",
            &[],
        )
        .expect("query SQL oracle");
    let result = client
        .query(
            "SELECT (convert_from(row_payload, 'UTF8')::jsonb #>> '{values,0,value}')::bigint
             FROM shiba.graph_result_rows
             WHERE graph_id = 1 AND result_id IN (4,5) ORDER BY result_id",
            &[],
        )
        .expect("query active results");
    assert_eq!(result[0].get::<_, i64>(0), oracle.get::<_, i64>(0));
    assert_eq!(result[1].get::<_, i64>(0), oracle.get::<_, i64>(1));
    let expected = client
        .query("SELECT id, payload FROM target.events ORDER BY id", &[])
        .expect("query ProjectRows recovery oracle")
        .into_iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, Option<i64>>(1)))
        .collect::<Vec<_>>();
    let actual = client
        .query(
            "SELECT (convert_from(row_payload,'UTF8')::jsonb #>> '{values,0,value}')::bigint,
                    CASE WHEN convert_from(row_payload,'UTF8')::jsonb #>> '{values,1,type}'='null'
                         THEN NULL ELSE (convert_from(row_payload,'UTF8')::jsonb #>> '{values,1,value}')::bigint END
             FROM shiba.graph_result_rows WHERE graph_id = 1 AND result_id = 6 ORDER BY 1",
            &[],
        )
        .expect("query recovered ProjectRows")
        .into_iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, Option<i64>>(1)))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

pub(crate) fn restart_postgres(mode: &str) {
    let stopped = Command::new(required("SHIBA_TEST_PG_CTL"))
        .args([
            "-D",
            &required("SHIBA_TEST_PG_DATA"),
            "-m",
            mode,
            "-w",
            "stop",
        ])
        .status()
        .expect("stop PostgreSQL deterministically");
    assert!(stopped.success());
    let started = Command::new(required("SHIBA_TEST_PG_CTL"))
        .args(["-D", &required("SHIBA_TEST_PG_DATA"), "-o"])
        .arg(format!(
            "-k {} -p {}",
            required("SHIBA_TEST_PG_SOCKET"),
            required("SHIBA_TEST_PG_PORT")
        ))
        .args(["-w", "start"])
        .status()
        .expect("restart PostgreSQL deterministically");
    assert!(started.success());
}

pub(crate) fn install_receiver_kill_trigger(client: &mut Client, target: &str, event: &str) {
    client
        .batch_execute(&format!(
            "CREATE FUNCTION public.kill_m12_receiver() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
               PERFORM pg_catalog.pg_terminate_backend(pid)
               FROM pg_catalog.pg_stat_replication
               WHERE application_name = '{APPLICATION}';
               RETURN NEW;
             END $$;
             CREATE TRIGGER kill_m12_receiver {event} ON {target}
             FOR EACH ROW EXECUTE FUNCTION public.kill_m12_receiver();"
        ))
        .expect("install deterministic receiver kill trigger");
}

pub(crate) fn remove_kill_trigger(client: &mut Client, target: &str) {
    client
        .batch_execute(&format!(
            "DROP TRIGGER kill_m12_receiver ON {target};
             DROP FUNCTION public.kill_m12_receiver();"
        ))
        .expect("remove receiver kill trigger");
}
