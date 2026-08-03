use postgres::{Client, NoTls};
use shiba_protocol::GraphId;
use shiba_sql_registration::compile_sql_and_register;

#[path = "m15_sql_aggregates/support.rs"]
mod support;

const COUNT_SQL: &str = "SELECT count(*) FROM agg_count.rows";
const SUM_SQL: &str = "SELECT sum(r.payload) FROM agg_sum.rows AS r";
const GROUPED_COUNT_SQL: &str = "SELECT r.id, count(*) FROM agg_group_count.rows AS r \
     WHERE r.payload > 0 GROUP BY r.id";
const GROUPED_SUM_SQL: &str =
    "SELECT r.payload, sum(r.id) FROM agg_group_sum.rows AS r GROUP BY r.payload";

#[test]
#[ignore = "requires scripts/test-m15-sql-aggregates.sh"]
fn sql_aggregates_share_production_lifecycle_and_postgresql_semantics() {
    let database_url = support::required("SHIBA_M15_SQL_AGGREGATES_DATABASE_URL");
    let replication_url = support::required("SHIBA_M15_SQL_AGGREGATES_REPLICATION_URL");
    let mut admin = Client::connect(&database_url, NoTls).expect("connect test database");
    let fixtures = support::install(&mut admin);

    for (graph, sql) in [
        (1, COUNT_SQL),
        (2, SUM_SQL),
        (3, GROUPED_COUNT_SQL),
        (4, GROUPED_SUM_SQL),
    ] {
        compile_sql_and_register(&mut admin, GraphId::new(graph).expect("graph ID"), sql)
            .unwrap_or_else(|error| {
                panic!("compile and register SQL aggregate graph {graph}: {error}")
            });
    }
    support::assert_registration_contracts(&mut admin);

    for fixture in &fixtures.graphs {
        support::bootstrap_and_detach(&database_url, &replication_url, fixture, &mut admin);
        support::assert_oracle(&mut admin, fixture);
    }

    support::scalar::exercise_count(
        &database_url,
        &replication_url,
        &mut admin,
        &fixtures.graphs[0],
    );
    support::scalar::exercise_nullable_sum(
        &database_url,
        &replication_url,
        &mut admin,
        &fixtures.graphs[1],
    );
    support::grouped::exercise_filtered_count(
        &database_url,
        &replication_url,
        &mut admin,
        &fixtures.graphs[2],
    );
    support::grouped::exercise_sum_and_recovery(
        &database_url,
        &replication_url,
        &mut admin,
        &fixtures.graphs[3],
    );
    support::rebuild::rebuild_grouped_sum(&database_url, &replication_url, &mut admin, &fixtures);
}
