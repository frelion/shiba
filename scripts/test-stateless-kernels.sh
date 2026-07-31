#!/usr/bin/env bash
set -euo pipefail

# Real PostgreSQL acceptance gate for the only stateless execution path:
# source stream -> Scan -> Filter -> Project -> Sink.

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pg_config_path="${PG_CONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
pg_bin_dir="$("${pg_config_path}" --bindir)"
pg_data_dir="$(mktemp -d /tmp/shiba-stateless-data.XXXXXX)"
pg_socket_dir="$(mktemp -d /tmp/shiba-stateless-socket.XXXXXX)"
pg_log_file="${pg_data_dir}/postgresql.log"
pg_port="${SHIBA_STATELESS_TEST_PORT:-$((60000 + $$ % 3000))}"
database_name="shiba_stateless"
wait_attempts="${SHIBA_STATELESS_WAIT_ATTEMPTS:-600}"

psql_gate() {
  PGOPTIONS="-c statement_timeout=30000 -c lock_timeout=10000" \
    "${pg_bin_dir}/psql" \
      -X -v ON_ERROR_STOP=1 \
      -h "${pg_socket_dir}" -p "${pg_port}" -d "${database_name}" "$@"
}

test_name="stateless kernel gate"
test_psql_command=psql_gate
test_log_lines=160
test_wait_attempts="${wait_attempts}"
test_wait_sleep=0.1
test_retain_log=1
source "${project_root}/scripts/test-lib.sh"
trap cleanup EXIT

base_difference="
WITH expected AS (
  SELECT id,
         CASE amount
           WHEN 15 THEN NULLIF(amount, 15)
           ELSE amount * 2
         END AS projected,
         upper(label) AS label,
         amount IS DISTINCT FROM 999 AS ordinary
  FROM public.stateless_source
  WHERE amount >= 10 AND label IS NOT NULL
),
actual AS (
  SELECT id, projected, label, ordinary
  FROM shiba.stateless_result
),
difference AS (
  (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
  UNION ALL
  (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
)
SELECT count(*) FROM difference"

cd "${project_root}"
cargo pgrx install \
  --pg-config "${pg_config_path}" \
  --features pg_test

"${pg_bin_dir}/initdb" \
  -D "${pg_data_dir}" --no-locale --encoding=UTF8 >/dev/null
{
  printf "session_preload_libraries = 'shiba'\n"
  printf "wal_level = logical\n"
  printf "max_replication_slots = 4\n"
  printf "max_worker_processes = 16\n"
  printf "listen_addresses = ''\n"
  printf "unix_socket_directories = '%s'\n" "${pg_socket_dir}"
  printf "port = %s\n" "${pg_port}"
  printf "shiba.batch_rows = 2\n"
  printf "shiba.batch_bytes = 512\n"
  printf "shiba.replication_conninfo = 'host=%s port=%s dbname=%s user=%s'\n" \
    "${pg_socket_dir}" "${pg_port}" "${database_name}" "$(id -un)"
} >>"${pg_data_dir}/postgresql.conf"

"${pg_bin_dir}/pg_ctl" \
  -D "${pg_data_dir}" -l "${pg_log_file}" \
  -o "-k ${pg_socket_dir} -p ${pg_port}" -t 30 -w start >/dev/null
"${pg_bin_dir}/createdb" \
  -h "${pg_socket_dir}" -p "${pg_port}" "${database_name}"

psql_gate -qc "CREATE EXTENSION shiba"
psql_gate -qc "SELECT shiba.activate()"
wait_for_query "1" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba runtime'" \
  "the singleton Runtime"

psql_gate -qc "
  CREATE TABLE public.shiba_runtime_failpoints (
    kind text PRIMARY KEY,
    runtime_pid integer,
    result_oid oid,
    stage_id integer,
    commit_lsn pg_lsn,
    pause_ms integer NOT NULL DEFAULT 0 CHECK (pause_ms >= 0),
    fired boolean NOT NULL DEFAULT false
  );
  CREATE TABLE public.stateless_source (
    id bigint PRIMARY KEY,
    amount integer NOT NULL,
    label text
  );
  CREATE TABLE public.release_stateless_registration (
    singleton boolean PRIMARY KEY CHECK (singleton)
  );
  INSERT INTO public.stateless_source
  SELECT id,
         id::integer,
         'initial-' || id
  FROM generate_series(1, 25) AS id;
"

# PostgreSQL creates only the result schema. In the same registration
# transaction each Scan spools the locked source snapshot into its own typed
# state relation and publishes no result row. Holding that transaction open
# must block a concurrent source write until the activation point commits.
psql_gate -qc "
  BEGIN;
  CREATE TABLE shiba.stateless_result AS
  SELECT id,
         CASE amount
           WHEN 15 THEN NULLIF(amount, 15)
           ELSE amount * 2
         END AS projected,
         upper(label) AS label,
         amount IS DISTINCT FROM 999 AS ordinary
  FROM public.stateless_source
  WHERE amount >= 10 AND label IS NOT NULL;
  DO \$registration\$
  DECLARE
    bootstrap_relation text;
    bootstrap_rows bigint;
  BEGIN
    IF (SELECT count(*) FROM shiba.stateless_result) <> 0 THEN
      RAISE EXCEPTION 'standard CTAS populated a Shiba result';
    END IF;
    SELECT format('%I.%I', namespace.nspname, relation.relname)
    INTO STRICT bootstrap_relation
    FROM shiba_internal.operator_state_relations AS storage
    JOIN pg_catalog.pg_class AS relation
      ON relation.oid = storage.relation_oid
    JOIN pg_catalog.pg_namespace AS namespace
      ON namespace.oid = relation.relnamespace
    WHERE storage.result_oid = 'shiba.stateless_result'::regclass
      AND storage.stage_id = 0
      AND storage.state_slot = 0;
    EXECUTE format('SELECT count(*) FROM %s', bootstrap_relation)
    INTO bootstrap_rows;
    IF bootstrap_rows <> 25 THEN
      RAISE EXCEPTION 'Scan snapshot spool has % rows, expected 25',
        bootstrap_rows;
    END IF;
    IF NOT (
      SELECT checkpoint.has_continuation
      FROM shiba_internal.operator_checkpoints AS checkpoint
      WHERE checkpoint.result_oid = 'shiba.stateless_result'::regclass
        AND checkpoint.stage_id = 0
    ) THEN
      RAISE EXCEPTION 'Scan bootstrap is not initially runnable';
    END IF;
  END
  \$registration\$;
  INSERT INTO public.shiba_runtime_failpoints(
    kind,result_oid,stage_id,pause_ms
  )
  VALUES(
    'operator_step_after_commit',
    'shiba.stateless_result'::regclass,
    0,
    3000
  );
  DO \$hold_registration\$
  BEGIN
    WHILE NOT EXISTS (
      SELECT 1
      FROM public.release_stateless_registration
      WHERE singleton
    ) LOOP
      PERFORM pg_sleep(0.05);
    END LOOP;
  END
  \$hold_registration\$;
  COMMIT" &
registration_shell_pid=$!

wait_for_query "1" "
  SELECT count(*)
  FROM pg_locks
  WHERE relation = 'public.stateless_source'::regclass
    AND mode = 'ShareRowExclusiveLock'
    AND granted" \
  "the registration transaction to hold its source lock"

psql_gate -qc "
  INSERT INTO public.stateless_source
  VALUES (1000, 50, 'after-activation')" &
writer_shell_pid=$!
wait_for_query "1" "
  SELECT count(*)
  FROM pg_locks
  WHERE relation = 'public.stateless_source'::regclass
    AND mode = 'RowExclusiveLock'
    AND NOT granted" \
  "a live source write to wait behind registration"

psql_gate -qc "
  INSERT INTO public.release_stateless_registration VALUES (true)"
wait "${registration_shell_pid}" ||
  fail "the held registration transaction failed"
wait "${writer_shell_pid}" ||
  fail "the post-activation source write failed"

assert_query "scan,filter,filter,project,sink" "
  SELECT string_agg(
           stage.value -> 'spec' ->> 'operator',
           ',' ORDER BY stage.ordinality
         )
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(
    dataflow.plan -> 'stages'
  ) WITH ORDINALITY AS stage(value, ordinality)
  WHERE dataflow.result_oid = 'shiba.stateless_result'::regclass"
assert_query "5" "
  SELECT count(*)
  FROM shiba_internal.operator_checkpoints
  WHERE result_oid = 'shiba.stateless_result'::regclass"
psql_gate -qc "
  CREATE ROLE shiba_reader;
  CREATE ROLE shiba_denied;
  GRANT USAGE ON SCHEMA shiba TO shiba_reader, shiba_denied;
  GRANT SELECT ON shiba.stateless_result TO shiba_reader"
assert_query "object|1" "
  SET ROLE shiba_reader;
  SELECT jsonb_typeof(
           shiba.explain_dataflow('shiba.stateless_result')
         )
         || '|' ||
         (SELECT count(*) FROM shiba.progress('shiba.stateless_result'))"
expect_failure "SELECT privilege" "
  SET ROLE shiba_denied;
  SELECT shiba.explain_dataflow('shiba.stateless_result')"
expect_failure "SELECT privilege" "
  SET ROLE shiba_denied;
  SELECT * FROM shiba.progress('shiba.stateless_result')"
wait_for_query "t" "
  SELECT fired
  FROM public.shiba_runtime_failpoints
  WHERE kind = 'operator_step_after_commit'" \
  "the first committed Scan bootstrap step"
bootstrap_relation="$(psql_gate -Atqc "
  SELECT format('%I.%I', namespace.nspname, relation.relname)
  FROM shiba_internal.operator_state_relations AS storage
  JOIN pg_catalog.pg_class AS relation
    ON relation.oid = storage.relation_oid
  JOIN pg_catalog.pg_namespace AS namespace
    ON namespace.oid = relation.relnamespace
  WHERE storage.result_oid = 'shiba.stateless_result'::regclass
    AND storage.stage_id = 0
    AND storage.state_slot = 0")"
bootstrap_rows="$(psql_gate -Atqc "
  SELECT count(*) FROM ${bootstrap_relation}")"
if test "${bootstrap_rows}" -lt 1 || test "${bootstrap_rows}" -ge 25; then
  fail "the crashed bootstrap retained ${bootstrap_rows} of 25 rows"
fi
bootstrap_runtime_pid="$(psql_gate -Atqc "
  SELECT runtime_pid
  FROM public.shiba_runtime_failpoints
  WHERE kind = 'operator_step_after_commit'")"
wait_for_runtime_replacement "${bootstrap_runtime_pid}"
wait_for_query "0" "${base_difference}" \
  "the snapshot bootstrap and post-activation live delta"
assert_query "0" "SELECT count(*) FROM ${bootstrap_relation}"

# One source transaction becomes several ingress chunks. Updates exercise
# negative and positive weights; the filtered row produces no output data.
psql_gate -qc "
  BEGIN;
  INSERT INTO public.stateless_source
  VALUES
    (30, 30, 'three'),
    (31, 7, repeat('filtered', 700));
  UPDATE public.stateless_source
  SET amount = 15, label = 'changed'
  WHERE id = 1;
  UPDATE public.stateless_source
  SET amount = 6
  WHERE id = 2;
  COMMIT"
wait_for_query "0" "${base_difference}" "split insert/update effects"

# The logical effect-byte helper must make a TOASTed row the first indivisible
# row of a bounded step, not reject it or disagree after storage. Updating only
# amount then forces pgoutput unchanged-TOAST reconstruction for label.
psql_gate -qc "
  INSERT INTO public.stateless_source
  VALUES (40, 40, repeat('wide', 2000));
  UPDATE public.stateless_source
  SET amount = 41
  WHERE id = 40"
wait_for_query "0" "${base_difference}" "an oversized TOASTed effect row"

# A transaction whose rows all fail the predicate still has to propagate its
# frontier through Filter and Project to the Sink.
psql_gate -qc "
  INSERT INTO public.stateless_source
  VALUES (41, 1, 'filtered-only')"
wait_for_query "t" "
  SELECT sink.consumed_frontier_lsn >= replay.published_lsn
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(
    dataflow.plan -> 'stages'
  ) WITH ORDINALITY AS stage(value, ordinality)
  JOIN shiba_internal.effect_stream_consumers AS sink
    ON sink.result_oid = dataflow.result_oid
   AND sink.consumer_stage_id = stage.ordinality - 1
  CROSS JOIN shiba_internal.ingress_replay_state AS replay
  WHERE dataflow.result_oid = 'shiba.stateless_result'::regclass
    AND stage.value -> 'spec' ->> 'operator' = 'sink'
    AND replay.state = 'active'
    AND replay.published_lsn IS NOT NULL" \
  "a frontier-only path to reach the Sink"
assert_query "0" "${base_difference}"

# Crash immediately after one Sink checkpoint commits. The replacement must
# rebuild only from durable checkpoint/continuation rows and converge without
# duplicating the already-committed result mutation.
runtime_pid="$(psql_gate -Atqc "
  SELECT pid
  FROM pg_stat_activity
  WHERE backend_type = 'shiba runtime'")"
result_oid="$(psql_gate -Atqc "
  SELECT 'shiba.stateless_result'::regclass::oid::integer")"
sink_stage_id="$(psql_gate -Atqc "
  SELECT stage.ordinality - 1
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(
    dataflow.plan -> 'stages'
  ) WITH ORDINALITY AS stage(value, ordinality)
  WHERE dataflow.result_oid = ${result_oid}::oid
    AND stage.value -> 'spec' ->> 'operator' = 'sink'")"
sink_revision_before="$(psql_gate -Atqc "
  SELECT revision
  FROM shiba_internal.operator_checkpoints
  WHERE result_oid=${result_oid}::oid
    AND stage_id=${sink_stage_id}")"
psql_gate -qc "
  DELETE FROM public.shiba_runtime_failpoints
  WHERE kind = 'operator_step_after_commit'"
psql_gate -qc "
  INSERT INTO public.shiba_runtime_failpoints(
    kind,runtime_pid,result_oid,stage_id,pause_ms
  )
  VALUES(
    'operator_step_after_commit',
    ${runtime_pid},
    ${result_oid}::oid,
    ${sink_stage_id},
    0
  )"
psql_gate -qc "
  INSERT INTO public.stateless_source
  SELECT id, id, 'restart-' || id
  FROM generate_series(100, 109) AS id"
wait_for_query "t" "
  SELECT fired
  FROM public.shiba_runtime_failpoints
  WHERE kind = 'operator_step_after_commit'" \
  "the post-checkpoint crash"
wait_for_runtime_replacement "${runtime_pid}"
wait_for_query "0" "${base_difference}" "post-crash exact recovery"
assert_query "t" "
  SELECT revision-${sink_revision_before} <= 8
  FROM shiba_internal.operator_checkpoints
  WHERE result_oid=${result_oid}::oid
    AND stage_id=${sink_stage_id}"

# Multiple active dataflows deliberately use different generated composite
# types. Sink keeps each typed row inside its dynamic statement, so switching
# between these DAGs cannot reuse a prepared polymorphic record expression.
psql_gate -qc "
  CREATE TABLE public.typed_source (
    key integer PRIMARY KEY,
    score numeric(8,2) NOT NULL,
    note varchar(80) NOT NULL,
    active boolean NOT NULL
  );
  INSERT INTO public.typed_source
  VALUES
    (1, 1.25, 'alpha', true),
    (2, 2.50, 'beta', false),
    (3, 3.75, 'gamma', true);
  CREATE TABLE shiba.typed_result AS
  SELECT key, score * 2 AS doubled, upper(note) AS note
  FROM public.typed_source
  WHERE active"
wait_for_query "0" "
  WITH expected AS (
    SELECT key, score * 2 AS doubled, upper(note) AS note
    FROM public.typed_source
    WHERE active
  ),
  difference AS (
    (SELECT * FROM expected
     EXCEPT ALL
     SELECT * FROM shiba.typed_result)
    UNION ALL
    (SELECT * FROM shiba.typed_result
     EXCEPT ALL
     SELECT * FROM expected)
  )
  SELECT count(*) FROM difference" \
  "a second generated Sink composite to bootstrap"
psql_gate -qc "
  UPDATE public.stateless_source
  SET label = 'cross-dag'
  WHERE id = 10;
  INSERT INTO public.typed_source
  VALUES (4, 4.50, 'delta', true);
  UPDATE public.typed_source
  SET active = false
  WHERE key = 1"
wait_for_query "0" "${base_difference}" \
  "the first Sink after switching generated composite types"
wait_for_query "0" "
  WITH expected AS (
    SELECT key, score * 2 AS doubled, upper(note) AS note
    FROM public.typed_source
    WHERE active
  ),
  difference AS (
    (SELECT * FROM expected
     EXCEPT ALL
     SELECT * FROM shiba.typed_result)
    UNION ALL
    (SELECT * FROM shiba.typed_result
     EXCEPT ALL
     SELECT * FROM expected)
  )
  SELECT count(*) FROM difference" \
  "the second Sink after switching generated composite types"

# Sink negative mutation must delete duplicate result rows by multiplicity, not
# by a source primary key. This also exercises NULL-safe matching: half of the
# result rows share a NULL label and the other half share the same non-NULL
# label. Raise the page budget so the delete reaches the batched ctid ranking
# path with many negative actions in one mutation page.
psql_gate -qc "
  CREATE TABLE public.duplicate_source (
    id bigint PRIMARY KEY,
    amount integer NOT NULL,
    label text
  );
  INSERT INTO public.duplicate_source
  SELECT id, 42, CASE WHEN id <= 64 THEN 'duplicate' END
  FROM generate_series(1, 128) AS id;
  CREATE TABLE shiba.duplicate_result AS
  SELECT amount, label
  FROM public.duplicate_source"
wait_for_query "128" "
  SELECT count(*) FROM shiba.duplicate_result" \
  "a duplicate-heavy Sink result to bootstrap"
psql_gate -qc "ALTER SYSTEM SET shiba.batch_rows = '256'"
psql_gate -qc "ALTER SYSTEM SET shiba.batch_bytes = '1048576'"
psql_gate -qc "SELECT pg_reload_conf()"
psql_gate -qc "DELETE FROM public.duplicate_source"
wait_for_query "0" "
  SELECT count(*) FROM shiba.duplicate_result" \
  "duplicate-heavy and NULL-safe negative Sink mutation"
psql_gate -qc "ALTER SYSTEM SET shiba.batch_rows = '2'"
psql_gate -qc "ALTER SYSTEM SET shiba.batch_bytes = '512'"
psql_gate -qc "SELECT pg_reload_conf()"

# An empty source still owns one bootstrap continuation at registration. One
# scheduled Scan step consumes that authority without fabricating a data
# chunk, then later live effects use the same source-stream path.
psql_gate -qc "
  CREATE TABLE public.empty_source (
    id smallint PRIMARY KEY,
    payload text NOT NULL
  );
  CREATE TABLE shiba.empty_result AS
  SELECT id, payload
  FROM public.empty_source"
wait_for_query "t" "
  SELECT checkpoint.revision > 0
         AND NOT checkpoint.has_continuation
  FROM shiba_internal.operator_checkpoints AS checkpoint
  WHERE checkpoint.result_oid = 'shiba.empty_result'::regclass
    AND checkpoint.stage_id = 0" \
  "the empty Scan bootstrap step to finish"
assert_query "0" "SELECT count(*) FROM shiba.empty_result"
psql_gate -qc "
  INSERT INTO public.empty_source VALUES (7, 'live-after-empty')"
wait_for_query "7|live-after-empty" "
  SELECT id || '|' || payload
  FROM shiba.empty_result" \
  "the empty source to continue with its live stream"

# Removing the last consumer removes the source stream and its typed payload.
# A later source schema creates a new stream from the new schema; no old shape
# is retained or adapted.
psql_gate -qc "DROP TABLE shiba.stateless_result"
assert_query "0|0" "
  SELECT
    (
      SELECT count(*)
      FROM shiba_internal.effect_streams
      WHERE producer_kind = 'source'
        AND source_oid = 'public.stateless_source'::regclass
    )
    || '|' ||
    (
      SELECT count(*)
      FROM shiba_internal.effect_streams AS stream
      WHERE stream.producer_kind = 'source'
        AND stream.source_oid = 'public.stateless_source'::regclass
        AND stream.relation_oid IS NOT NULL
    )"
psql_gate -qc "
  ALTER TABLE public.stateless_source
  ADD COLUMN generation integer NOT NULL DEFAULT 7;
  CREATE TABLE shiba.stateless_result_v2 AS
  SELECT id, amount * generation AS value
  FROM public.stateless_source
  WHERE amount >= 10"
psql_gate -qc "
  INSERT INTO public.stateless_source(id,amount,label,generation)
  VALUES (2000, 50, 'new-shape', 9)"
wait_for_query "0" "
  WITH expected AS (
    SELECT id, amount * generation AS value
    FROM public.stateless_source
    WHERE amount >= 10
  ),
  actual AS (
    SELECT id, value FROM shiba.stateless_result_v2
  ),
  difference AS (
    (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
    UNION ALL
    (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
  )
  SELECT count(*) FROM difference" \
  "drop, source ALTER, and registration against the new source shape"

# SECURITY DEFINER kernels never execute user-defined immutable code.
psql_gate -qc "
  CREATE FUNCTION public.user_immutable(integer)
  RETURNS integer
  LANGUAGE sql
  IMMUTABLE
  AS 'SELECT \$1'"
if psql_gate -qc "
  CREATE TABLE shiba.forbidden AS
  SELECT public.user_immutable(amount)
  FROM public.stateless_source" >/dev/null 2>&1; then
  fail "a user-defined immutable function reached a kernel"
fi
assert_query "|" "
  SELECT
    coalesce(to_regclass('shiba.forbidden')::text, '')
    || '|' ||
    coalesce((
      SELECT result_oid::regclass::text
      FROM shiba_internal.dataflows
      WHERE result_oid = to_regclass('shiba.forbidden')
    ), '')"

# A trusted outer result type must not hide an untrusted type inside the scalar
# AST. PostgreSQL keeps the domain check as an inner CoerceToDomain even though
# the visible Project output is cast back to integer.
psql_gate -qc "
  CREATE DOMAIN public.user_positive_integer AS integer
  CHECK (VALUE >= 0)"
if psql_gate -qc "
  CREATE TABLE shiba.forbidden_domain AS
  SELECT ((amount::public.user_positive_integer)::integer) AS amount
  FROM public.stateless_source" >/dev/null 2>&1; then
  fail "a user-defined nested domain reached a kernel"
fi
assert_query "|" "
  SELECT
    coalesce(to_regclass('shiba.forbidden_domain')::text, '')
    || '|' ||
    coalesce((
      SELECT result_oid::regclass::text
      FROM shiba_internal.dataflows
      WHERE result_oid = to_regclass('shiba.forbidden_domain')
    ), '')"

printf '%s\n' "stateless Scan/Filter/Project/Sink gate passed"
