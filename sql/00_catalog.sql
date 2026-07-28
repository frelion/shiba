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
    -- Composite-to-JSONB identity must use the same type-output settings at
    -- registration/backfill and later Runtime apply.
    execution_settings jsonb NOT NULL DEFAULT jsonb_build_object(
      'TimeZone',current_setting('TimeZone'),
      'DateStyle',current_setting('DateStyle'),
      'IntervalStyle',current_setting('IntervalStyle'),
      'extra_float_digits',current_setting('extra_float_digits'),
      'bytea_output',current_setting('bytea_output')
    ) CHECK (
      jsonb_typeof(execution_settings)='object'
      AND execution_settings ?& ARRAY[
        'TimeZone','DateStyle','IntervalStyle','extra_float_digits','bytea_output'
      ]
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Only indexes created through shiba.create_index are removable through the
-- user API.  PostgreSQL does not support foreign keys into pg_class/pg_authid,
-- so the lifecycle event trigger removes rows when an index is dropped and
-- every API operation revalidates the stored object identity by OID.
CREATE TABLE shiba_internal.managed_indexes (
    index_oid oid PRIMARY KEY,
    result_oid oid NOT NULL
      REFERENCES shiba_internal.stream_views(result_oid) ON DELETE CASCADE,
    index_name name NOT NULL,
    index_columns name[] NOT NULL,
    creator_oid oid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (result_oid, index_name)
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
    -- JSONB preserves SQL NULL separately from an empty string and retains
    -- equality semantics for typed scalar keys such as numeric 1 and 1.0.
    join_key jsonb NOT NULL,
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

-- A logical graph has one versioned physical plan.  plan_id is a stable,
-- database-wide identity; version identifies the physical-plan format.
CREATE TABLE shiba_internal.physical_plans (
    result_oid oid PRIMARY KEY
        REFERENCES shiba_internal.stream_graphs(result_oid) ON DELETE CASCADE,
    plan_id bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    version integer NOT NULL CHECK (version > 0),
    plan jsonb NOT NULL CHECK (jsonb_typeof(plan) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (result_oid, plan_id)
);

-- Every cataloged v1 Stage relation is typed and UNLOGGED. Inline and
-- statement-materialized Stages live only in the physical-plan descriptor.
-- A relation-backed Stage is a rebuildable commit-scoped cache, never durable
-- operator state.
CREATE TABLE shiba_internal.physical_stages (
    result_oid oid NOT NULL,
    plan_id bigint NOT NULL,
    stage_id integer NOT NULL CHECK (stage_id >= 0),
    stage_name text NOT NULL CHECK (btrim(stage_name) <> ''),
    storage text NOT NULL CHECK (storage = 'unlogged'),
    relation_oid oid NOT NULL UNIQUE,
    relation_name name NOT NULL,
    schema_spec jsonb NOT NULL CHECK (jsonb_typeof(schema_spec) = 'array'),
    index_spec jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(index_spec) = 'array'),
    PRIMARY KEY (result_oid, stage_id),
    UNIQUE (result_oid, stage_name),
    FOREIGN KEY (result_oid, plan_id)
        REFERENCES shiba_internal.physical_plans(result_oid, plan_id)
        ON DELETE CASCADE
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
-- records its routing decision with the shared payload and DAG references in
-- one transaction.  This row is the transaction header used by bounded GC.
CREATE TABLE shiba_internal.routed_transactions (
    commit_lsn pg_lsn PRIMARY KEY,
    routed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Every decoded row delta is stored once, independent of the number of DAGs
-- that consume it. Routing normalizes pgoutput text fields to typed JSONB once
-- after the complete commit is present. Operator state and results remain
-- LOGGED.
CREATE TABLE shiba_internal.change_log (
    commit_lsn pg_lsn NOT NULL,
    sequence integer NOT NULL CHECK (sequence > 0),
    source_oid oid NOT NULL,
    delta integer NOT NULL CHECK (delta IN (-1, 1)),
    row_data jsonb NOT NULL,
    PRIMARY KEY (commit_lsn, sequence),
    FOREIGN KEY (commit_lsn)
        REFERENCES shiba_internal.routed_transactions(commit_lsn)
        ON DELETE CASCADE
);

-- A DAG inbox row is transaction-level work, not another payload copy.
-- RESTRICT makes deleting a routed transaction (and therefore its change-log
-- payload) impossible while any DAG still needs that source transaction.
CREATE TABLE shiba_internal.dag_inbox (
    result_oid oid NOT NULL
        REFERENCES shiba_internal.stream_views(result_oid) ON DELETE CASCADE,
    commit_lsn pg_lsn NOT NULL
        REFERENCES shiba_internal.routed_transactions(commit_lsn) ON DELETE RESTRICT,
    PRIMARY KEY (result_oid, commit_lsn)
);

CREATE INDEX shiba_dag_inbox_commit_idx
    ON shiba_internal.dag_inbox (commit_lsn);

-- DAGs are logical runtimes scheduled cooperatively by the Runtime.  This
-- table intentionally contains no PostgreSQL-process lease or heartbeat.
CREATE TABLE shiba_internal.dag_runtime_state (
    result_oid oid PRIMARY KEY REFERENCES shiba_internal.stream_views(result_oid) ON DELETE CASCADE,
    active boolean NOT NULL DEFAULT true,
    last_scheduled_at timestamptz,
    last_error text,
    failed_at timestamptz,
    CHECK ((last_error IS NULL) = (failed_at IS NULL))
);

-- One dynamic "shiba runtime" process owns routing, DAG scheduling, apply, and
-- change-log GC for this database.
CREATE TABLE shiba_internal.runtime_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    active boolean NOT NULL DEFAULT false,
    owner_pid integer CHECK (owner_pid > 0),
    started_at timestamptz,
    last_heartbeat timestamptz,
    last_requested_at timestamptz,
    launch_generation bigint NOT NULL DEFAULT 0 CHECK (launch_generation >= 0),
    pending_launch_xid xid8,
    pending_since timestamptz,
    CHECK ((owner_pid IS NULL) = (started_at IS NULL)),
    CHECK ((pending_launch_xid IS NULL) = (pending_since IS NULL)),
    CHECK (owner_pid IS NULL OR pending_launch_xid IS NULL)
);

INSERT INTO shiba_internal.runtime_state (singleton) VALUES (true);
