CREATE SCHEMA shiba_internal;
REVOKE ALL ON SCHEMA shiba_internal FROM PUBLIC;

-- Rust keeps JSONB as the extension ABI, while PostgreSQL converts every
-- commit to this typed row shape before a physical operator consumes it.
-- Keeping the type internal lets physical operators move to set-based SQL
-- without exposing a user-facing composite type or changing the WAL inbox.
CREATE TYPE shiba_internal.delta_event AS (
    source_oid oid,
    delta integer,
    row_data jsonb
);

CREATE TABLE shiba_internal.stream_views (
    result_oid oid PRIMARY KEY,
    view_kind text NOT NULL DEFAULT 'aggregate'
      CHECK (view_kind IN ('aggregate','window','distinct','topn')),
    source_oid oid NOT NULL,
    group_column name,
    result_group_column name,
    count_column name,
    count_distinct boolean NOT NULL DEFAULT false,
    count_input_source text CHECK (count_input_source IN ('left','right')),
    count_input_column name,
    sum_input_column name,
    sum_column name,
    activation_lsn pg_lsn NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE shiba_internal.stream_filters (
    result_oid oid NOT NULL REFERENCES shiba_internal.stream_views(result_oid) ON DELETE CASCADE,
    input_side text NOT NULL CHECK (input_side IN ('left', 'right')),
    source_oid oid NOT NULL,
    phase text NOT NULL DEFAULT 'pre' CHECK (phase IN ('pre','post')),
    predicate_sql text NOT NULL,
    PRIMARY KEY (result_oid, input_side)
);

CREATE TABLE shiba_internal.stream_having (
    result_oid oid PRIMARY KEY REFERENCES shiba_internal.stream_views(result_oid) ON DELETE CASCADE,
    predicate_sql text NOT NULL
);

CREATE TABLE shiba_internal.stream_join_filters (
    result_oid oid PRIMARY KEY REFERENCES shiba_internal.stream_views(result_oid) ON DELETE CASCADE,
    predicate_sql text NOT NULL
);

CREATE TABLE shiba_internal.inner_join_views (
    result_oid oid PRIMARY KEY REFERENCES shiba_internal.stream_views(result_oid) ON DELETE CASCADE,
    join_type text NOT NULL CHECK (
      join_type IN ('inner','left','right','full','semi','anti','null_anti')
    ),
    right_source_oid oid NOT NULL,
    left_join_column name NOT NULL,
    right_join_column name NOT NULL,
    group_source text NOT NULL CHECK (group_source IN ('left', 'right')),
    group_column name NOT NULL,
    sum_source text NOT NULL CHECK (sum_source IN ('left', 'right'))
);

-- A bag, not a set: identical source rows must retain their multiplicity.
CREATE TABLE shiba_internal.join_arrangements (
    result_oid oid NOT NULL REFERENCES shiba_internal.inner_join_views(result_oid) ON DELETE CASCADE,
    input_side text NOT NULL CHECK (input_side IN ('left', 'right')),
    join_key text NOT NULL,
    row_data jsonb NOT NULL,
    multiplicity bigint NOT NULL CHECK (multiplicity > 0),
    PRIMARY KEY (result_oid, input_side, join_key, row_data)
);

CREATE INDEX shiba_join_arrangements_probe_idx
    ON shiba_internal.join_arrangements (result_oid, input_side, join_key);

-- The durable logical graph is independent of the currently implemented
-- physical operators.  PostgreSQL plans the SELECT; Shiba stores that plan as
-- a directed graph so new incremental operators do not require a new DDL
-- syntax or another SQL-text parser.
CREATE TABLE shiba_internal.stream_graphs (
    result_oid oid PRIMARY KEY REFERENCES shiba_internal.stream_views(result_oid) ON DELETE CASCADE,
    plan jsonb NOT NULL,
    logical_plan jsonb NOT NULL DEFAULT '{}'::jsonb,
    analyzed_query jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE shiba_internal.stream_graph_nodes (
    result_oid oid NOT NULL REFERENCES shiba_internal.stream_graphs(result_oid) ON DELETE CASCADE,
    node_id text NOT NULL,
    operator text NOT NULL,
    properties jsonb NOT NULL,
    PRIMARY KEY (result_oid, node_id)
);

CREATE TABLE shiba_internal.stream_graph_edges (
    result_oid oid NOT NULL REFERENCES shiba_internal.stream_graphs(result_oid) ON DELETE CASCADE,
    upstream_node_id text NOT NULL,
    downstream_node_id text NOT NULL,
    PRIMARY KEY (result_oid, upstream_node_id, downstream_node_id),
    FOREIGN KEY (result_oid, upstream_node_id)
        REFERENCES shiba_internal.stream_graph_nodes(result_oid, node_id) ON DELETE CASCADE,
    FOREIGN KEY (result_oid, downstream_node_id)
        REFERENCES shiba_internal.stream_graph_nodes(result_oid, node_id) ON DELETE CASCADE
);

CREATE TABLE shiba_internal.operator_instances (
    result_oid oid NOT NULL REFERENCES shiba_internal.stream_graphs(result_oid) ON DELETE CASCADE,
    node_id text NOT NULL,
    operator text NOT NULL,
    config jsonb NOT NULL,
    stateful boolean NOT NULL,
    PRIMARY KEY (result_oid, node_id)
);

CREATE TABLE shiba_internal.aggregate_state (
    result_oid oid NOT NULL REFERENCES shiba_internal.stream_views(result_oid) ON DELETE CASCADE,
    group_key jsonb NOT NULL,
    row_count bigint NOT NULL CHECK (row_count >= 0),
    count_value bigint NOT NULL CHECK (count_value >= 0),
    sum_nonnull_count bigint NOT NULL CHECK (sum_nonnull_count >= 0),
    sum_value numeric NOT NULL,
    PRIMARY KEY (result_oid, group_key)
);

CREATE TABLE shiba_internal.distinct_state (
    result_oid oid NOT NULL REFERENCES shiba_internal.stream_views(result_oid) ON DELETE CASCADE,
    group_key jsonb NOT NULL,
    value_key jsonb NOT NULL,
    multiplicity bigint NOT NULL CHECK (multiplicity > 0),
    PRIMARY KEY(result_oid,group_key,value_key)
);

CREATE TABLE shiba_internal.window_views (
    result_oid oid PRIMARY KEY REFERENCES shiba_internal.stream_views(result_oid) ON DELETE CASCADE,
    partition_column name NOT NULL,
    result_partition_column name NOT NULL,
    order_column name NOT NULL,
    order_direction text NOT NULL CHECK (order_direction IN ('asc','desc')),
    nulls_first boolean NOT NULL,
    output_columns name[] NOT NULL,
    target_expressions text[] NOT NULL,
    CHECK (cardinality(output_columns)=cardinality(target_expressions))
);

CREATE TABLE shiba_internal.window_rows (
    result_oid oid NOT NULL REFERENCES shiba_internal.window_views(result_oid) ON DELETE CASCADE,
    partition_key jsonb NOT NULL,
    row_data jsonb NOT NULL,
    multiplicity bigint NOT NULL CHECK (multiplicity>0),
    PRIMARY KEY(result_oid,partition_key,row_data)
);

CREATE TABLE shiba_internal.distinct_views (
    result_oid oid PRIMARY KEY REFERENCES shiba_internal.stream_views(result_oid) ON DELETE CASCADE,
    source_columns name[] NOT NULL,
    output_columns name[] NOT NULL,
    CHECK (cardinality(source_columns)=cardinality(output_columns))
);

CREATE TABLE shiba_internal.projection_state (
    result_oid oid NOT NULL REFERENCES shiba_internal.distinct_views(result_oid) ON DELETE CASCADE,
    row_key jsonb NOT NULL,
    multiplicity bigint NOT NULL CHECK (multiplicity>0),
    PRIMARY KEY(result_oid,row_key)
);

CREATE TABLE shiba_internal.topn_views (
    result_oid oid PRIMARY KEY REFERENCES shiba_internal.stream_views(result_oid) ON DELETE CASCADE,
    order_column name NOT NULL,
    order_direction text NOT NULL CHECK (order_direction IN ('asc','desc')),
    nulls_first boolean NOT NULL,
    limit_count bigint NOT NULL CHECK (limit_count>0),
    limit_offset bigint NOT NULL DEFAULT 0 CHECK (limit_offset>=0),
    source_columns name[] NOT NULL,
    output_columns name[] NOT NULL,
    CHECK (cardinality(source_columns)=cardinality(output_columns))
);

CREATE TABLE shiba_internal.topn_rows (
    result_oid oid NOT NULL REFERENCES shiba_internal.topn_views(result_oid) ON DELETE CASCADE,
    row_data jsonb NOT NULL,
    multiplicity bigint NOT NULL CHECK (multiplicity>0),
    PRIMARY KEY(result_oid,row_data)
);

CREATE TABLE shiba_internal.view_progress (
    result_oid oid PRIMARY KEY REFERENCES shiba_internal.stream_views(result_oid) ON DELETE CASCADE,
    applied_lsn pg_lsn,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Logical slots can replay committed transactions after a crash.  The Router
-- records its routing decision with the inbox rows in one transaction.
CREATE TABLE shiba_internal.routed_transactions (
    commit_lsn pg_lsn PRIMARY KEY,
    routed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Each result DAG receives a private, durable, commit-ordered input stream.
CREATE TABLE shiba_internal.dag_inbox (
    result_oid oid NOT NULL REFERENCES shiba_internal.stream_views(result_oid) ON DELETE CASCADE,
    commit_lsn pg_lsn NOT NULL,
    sequence integer NOT NULL CHECK (sequence > 0),
    source_oid oid NOT NULL,
    delta integer NOT NULL CHECK (delta IN (-1, 1)),
    row_data jsonb NOT NULL,
    PRIMARY KEY (result_oid, commit_lsn, sequence)
);

CREATE INDEX shiba_dag_inbox_pending_idx
    ON shiba_internal.dag_inbox (result_oid, commit_lsn, sequence);

CREATE TABLE shiba_internal.dag_worker_state (
    result_oid oid PRIMARY KEY REFERENCES shiba_internal.stream_views(result_oid) ON DELETE CASCADE,
    active boolean NOT NULL DEFAULT true,
    last_heartbeat timestamptz,
    last_requested_at timestamptz
);

CREATE TABLE shiba_internal.worker_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    active boolean NOT NULL DEFAULT false,
    last_heartbeat timestamptz,
    last_requested_at timestamptz
);

INSERT INTO shiba_internal.worker_state (singleton) VALUES (true);
