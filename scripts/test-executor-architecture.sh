#!/usr/bin/env bash
set -euo pipefail

# Acceptance gate for the single database-scoped Runtime. The historical file
# name is retained because callers use it, but no Executor process or pool is
# part of the asserted topology.
#
# Required implementation contract:
#   backend_type = 'shiba runtime'
#   shiba_internal.runtime_state singleton ownership row
#   shiba_internal.effective_change_log(commit_lsn,sequence,source_oid,delta,row_data)
#   shiba_internal.dag_inbox(result_oid,commit_lsn), with no payload columns
#
# Tunables:
#   SHIBA_ARCH_FAIRNESS_COMMITS   commits queued per fairness DAG (default 24)
#   SHIBA_ARCH_TEST_PORT          isolated PostgreSQL port
#   SHIBA_KEEP_TEST_CLUSTER=1     retain cluster after the run

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pg_config_path="${PG_CONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
pg_bin_dir="$("${pg_config_path}" --bindir)"
pg_data_dir="$(mktemp -d /tmp/shiba-runtime-data.XXXXXX)"
pg_socket_dir="$(mktemp -d /tmp/shiba-runtime-socket.XXXXXX)"
pg_log_file="${pg_data_dir}/postgresql.log"
pg_port="${SHIBA_ARCH_TEST_PORT:-$((57000 + $$ % 5000))}"
database_name="shiba_runtime_architecture"
fairness_commits="${SHIBA_ARCH_FAIRNESS_COMMITS:-24}"

cleanup() {
  if test "${SHIBA_KEEP_TEST_CLUSTER:-0}" = "1"; then
    printf 'Retained test cluster: %s\n' "${pg_data_dir}" >&2
    printf 'Retained test socket: %s\n' "${pg_socket_dir}" >&2
    return
  fi
  "${pg_bin_dir}/pg_ctl" -D "${pg_data_dir}" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "${pg_data_dir}" "${pg_socket_dir}"
}
trap cleanup EXIT

psql_arch() {
  PGOPTIONS="-c statement_timeout=30000 -c lock_timeout=10000" \
    "${pg_bin_dir}/psql" \
      -X -v ON_ERROR_STOP=1 \
      -h "${pg_socket_dir}" -p "${pg_port}" -d "${database_name}" "$@"
}

fail() {
  printf 'single-Runtime architecture gate failed: %s\n' "$1" >&2
  printf 'PostgreSQL log: %s\n' "${pg_log_file}" >&2
  exit 1
}

assert_query() {
  local expected="$1"
  local query="$2"
  local actual
  actual="$(psql_arch -Atqc "${query}")"
  if test "${actual}" != "${expected}"; then
    fail "expected [${expected}], got [${actual}] for: ${query}"
  fi
}

wait_for_query() {
  local expected="$1"
  local query="$2"
  local description="$3"
  local actual=""
  local attempt
  for attempt in {1..600}; do
    if actual="$(psql_arch -Atqc "${query}" 2>/dev/null)" &&
       test "${actual}" = "${expected}"; then
      return 0
    fi
    sleep 0.05
  done
  fail "timed out waiting for ${description}; last value was [${actual}]"
}

assert_runtime_process_topology() {
  wait_for_query "1|0|0" "
    SELECT
      count(*) FILTER (WHERE backend_type='shiba runtime')
      || '|' ||
      count(*) FILTER (WHERE backend_type='shiba router')
      || '|' ||
      count(*) FILTER (WHERE backend_type='shiba executor')
    FROM pg_stat_activity" \
    "exactly one Runtime and no Router/Executor processes"
}

assert_one_runtime() {
  assert_runtime_process_topology
  assert_query "1|1|1" "
    SELECT count(*) || '|' || count(DISTINCT state.owner_pid) || '|' ||
           count(activity.pid)
    FROM shiba_internal.runtime_state state
    LEFT JOIN pg_stat_activity activity
      ON activity.pid=state.owner_pid
     AND activity.backend_type='shiba runtime'
    WHERE state.singleton AND state.active"
}

set_dag_active() {
  local relation="$1"
  local active="$2"
  psql_arch -qc "
    UPDATE shiba_internal.dag_runtime_state
    SET active=${active}
    WHERE result_oid='${relation}'::regclass"
}

wait_for_result() {
  local source_relation="$1"
  local result_relation="$2"
  wait_for_query "0" "
    WITH expected AS (
      SELECT group_id,count(*)::bigint AS row_count,sum(amount)::bigint AS total_amount
      FROM ${source_relation} GROUP BY group_id
    ),
    actual AS (
      SELECT group_id,row_count::bigint,total_amount::bigint
      FROM ${result_relation}
    ),
    difference AS (
      (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
      UNION ALL
      (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
    )
    SELECT count(*) FROM difference" \
    "${result_relation} to equal PostgreSQL recomputation"
}

if ! test "${fairness_commits}" -ge 12 2>/dev/null; then
  fail "SHIBA_ARCH_FAIRNESS_COMMITS must be an integer >= 12"
fi

cd "${project_root}"

# Physical DAG edges are PostgreSQL query relations. Operator kernels must not
# turn them into long-lived Runtime-session catalog objects by default.
if grep -Ein \
  'CREATE[[:space:]]+(TEMP|TEMPORARY)[[:space:]]+TABLE[[:space:]]+shiba_|pg_temp\.shiba_' \
  sql/21_operator_aggregate.sql \
  sql/22_operator_unary_batches.sql \
  sql/23_operator_join_batch.sql; then
  fail "operator execution contains an explicit pg_temp scratch relation"
fi

# UNLOGGED Stage DDL is registration-time plan finalization. Commit execution
# may resolve, truncate, populate, and consume a Stage, but must never perform
# catalog DDL on the hot path.
if grep -Ein \
  'CREATE[[:space:]]+UNLOGGED[[:space:]]+TABLE' \
  sql/21_operator_aggregate.sql \
  sql/22_operator_unary_batches.sql \
  sql/23_operator_join_batch.sql \
  sql/24_operator_dispatch.sql \
  sql/26_physical_stages.sql; then
  fail "commit execution contains per-commit UNLOGGED Stage DDL"
fi
if ! grep -Eiq \
  'CREATE[[:space:]]+UNLOGGED[[:space:]]+TABLE' \
  sql/30_registration.sql; then
  fail "registration does not own typed UNLOGGED Stage creation"
fi

cargo pgrx install --pg-config "${pg_config_path}"

"${pg_bin_dir}/initdb" -D "${pg_data_dir}" --no-locale --encoding=UTF8 >/dev/null
{
  printf "session_preload_libraries = 'shiba'\n"
  printf "wal_level = logical\n"
  printf "max_replication_slots = 4\n"
  printf "max_worker_processes = 16\n"
  printf "listen_addresses = ''\n"
  printf "unix_socket_directories = '%s'\n" "${pg_socket_dir}"
  printf "port = %s\n" "${pg_port}"
  printf "shiba.replication_conninfo = 'host=%s port=%s dbname=%s user=%s'\n" \
    "${pg_socket_dir}" "${pg_port}" "${database_name}" "$(id -un)"
} >>"${pg_data_dir}/postgresql.conf"

"${pg_bin_dir}/pg_ctl" \
  -D "${pg_data_dir}" -l "${pg_log_file}" \
  -o "-k ${pg_socket_dir} -p ${pg_port}" -t 30 -w start >/dev/null
"${pg_bin_dir}/createdb" \
  -h "${pg_socket_dir}" -p "${pg_port}" "${database_name}"
psql_arch -qc "CREATE EXTENSION shiba"

printf '\n==> Concurrent activation creates one Runtime identity\n'
activation_output="$(mktemp /tmp/shiba-runtime-activation.XXXXXX)"
PGAPPNAME="shiba_activation_holder" \
PGOPTIONS="-c statement_timeout=30000 -c lock_timeout=10000" \
  "${pg_bin_dir}/psql" \
    -X -v ON_ERROR_STOP=1 \
    -h "${pg_socket_dir}" -p "${pg_port}" -d "${database_name}" \
    -qc "
      BEGIN;
      SELECT shiba.activate() FROM generate_series(1,8);
      SELECT pg_sleep(3);
      COMMIT" >"${activation_output}" 2>&1 &
activation_holder_pid=$!
wait_for_query "1" "
  SELECT count(*) FROM pg_stat_activity
  WHERE application_name='shiba_activation_holder'
    AND xact_start IS NOT NULL
    AND wait_event='PgSleep'" \
  "the uncommitted activation transaction"
assert_runtime_process_topology
if ! wait "${activation_holder_pid}"; then
  sed -n '1,120p' "${activation_output}" >&2
  rm -f "${activation_output}"
  fail "the activation transaction failed"
fi
rm -f "${activation_output}"
assert_one_runtime
assert_query "1" "
  SELECT launch_generation
  FROM shiba_internal.runtime_state
  WHERE singleton"

printf '\n==> Shared payload fanout and transaction-level inbox references\n'
psql_arch -qc "
  CREATE TABLE public.arch_shared_source (
    event_id integer PRIMARY KEY,
    group_id integer NOT NULL,
    amount integer NOT NULL
  );
  INSERT INTO public.arch_shared_source VALUES (1,0,10);
  CREATE TABLE shiba.arch_shared_a AS
  SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount
  FROM public.arch_shared_source GROUP BY group_id;
  CREATE TABLE shiba.arch_shared_b AS
  SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount
  FROM public.arch_shared_source GROUP BY group_id;
  UPDATE shiba_internal.dag_runtime_state
  SET active=false
  WHERE result_oid IN (
    'shiba.arch_shared_a'::regclass,
    'shiba.arch_shared_b'::regclass
  )"
assert_one_runtime

# One source transaction emits INSERT + UPDATE-old + UPDATE-new. Two DAGs
# consume it, but payload must be stored once rather than copied per DAG.
psql_arch -qc "
  BEGIN;
  INSERT INTO public.arch_shared_source VALUES (700001,7,70);
  UPDATE public.arch_shared_source
  SET group_id=8,amount=80
  WHERE event_id=700001;
  COMMIT"
wait_for_query "1" "
  SELECT count(DISTINCT commit_lsn)
  FROM shiba_internal.effective_change_log
  WHERE source_oid='public.arch_shared_source'::regclass
    AND row_data->>'event_id'='700001'" \
  "the shared source transaction to be routed"
shared_lsn="$(psql_arch -Atqc "
  SELECT min(commit_lsn)
  FROM shiba_internal.effective_change_log
  WHERE source_oid='public.arch_shared_source'::regclass
    AND row_data->>'event_id'='700001'")"
assert_query "3|3|{1,-1,1}" "
  SELECT count(*) || '|' || count(DISTINCT sequence) || '|' ||
         array_agg(delta ORDER BY sequence)::text
  FROM shiba_internal.effective_change_log
  WHERE commit_lsn='${shared_lsn}'::pg_lsn
    AND source_oid='public.arch_shared_source'::regclass
    AND row_data->>'event_id'='700001'"
assert_query "2|2" "
  SELECT count(*) || '|' || count(DISTINCT result_oid)
  FROM shiba_internal.dag_inbox
  WHERE commit_lsn='${shared_lsn}'::pg_lsn
    AND result_oid IN (
      'shiba.arch_shared_a'::regclass,
      'shiba.arch_shared_b'::regclass
    )"
assert_query "0" "
  SELECT count(*)
  FROM information_schema.columns
  WHERE table_schema='shiba_internal'
    AND table_name='dag_inbox'
    AND column_name IN ('sequence','source_oid','delta','row_data','payload')"

# Applying one consumer must not collect payload still referenced by the other.
set_dag_active "shiba.arch_shared_a" true
wait_for_query "0" "
  SELECT count(*) FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.arch_shared_a'::regclass
    AND commit_lsn='${shared_lsn}'::pg_lsn" \
  "the first shared-payload consumer"
assert_query "1|3" "
  SELECT
    (SELECT count(*) FROM shiba_internal.dag_inbox
     WHERE result_oid='shiba.arch_shared_b'::regclass
       AND commit_lsn='${shared_lsn}'::pg_lsn)
    || '|' ||
    (SELECT count(*) FROM shiba_internal.effective_change_log
     WHERE commit_lsn='${shared_lsn}'::pg_lsn)"

set_dag_active "shiba.arch_shared_b" true
wait_for_query "0" "
  SELECT count(*) FROM shiba_internal.dag_inbox
  WHERE commit_lsn='${shared_lsn}'::pg_lsn" \
  "the final shared-payload reference to be acknowledged"
wait_for_query "0" "
  SELECT count(*) FROM shiba_internal.effective_change_log
  WHERE commit_lsn='${shared_lsn}'::pg_lsn" \
  "bounded GC to remove unreferenced shared payload"
wait_for_query "0" "
  SELECT count(*) FROM shiba_internal.ingress_transactions
  WHERE commit_lsn='${shared_lsn}'::pg_lsn" \
  "bounded GC to remove the unreferenced transaction header"
wait_for_result "public.arch_shared_source" "shiba.arch_shared_a"
wait_for_result "public.arch_shared_source" "shiba.arch_shared_b"

printf '\n==> Round-robin DAG fairness and one-commit transaction boundaries\n'
psql_arch -qc "
  CREATE TABLE public.arch_fair_source_a (
    event_id integer PRIMARY KEY,group_id integer NOT NULL,amount integer NOT NULL
  );
  CREATE TABLE public.arch_fair_source_b (
    event_id integer PRIMARY KEY,group_id integer NOT NULL,amount integer NOT NULL
  );
  INSERT INTO public.arch_fair_source_a VALUES (1,0,1);
  INSERT INTO public.arch_fair_source_b VALUES (1,0,1);
  CREATE TABLE shiba.arch_fair_result_a AS
  SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount
  FROM public.arch_fair_source_a GROUP BY group_id;
  CREATE TABLE shiba.arch_fair_result_b AS
  SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount
  FROM public.arch_fair_source_b GROUP BY group_id;
  UPDATE shiba_internal.dag_runtime_state
  SET active=false
  WHERE result_oid IN (
    'shiba.arch_fair_result_a'::regclass,
    'shiba.arch_fair_result_b'::regclass
  );

  CREATE TABLE public.arch_runtime_audit (
    audit_id bigserial PRIMARY KEY,
    result_oid oid NOT NULL,
    applied_lsn pg_lsn NOT NULL,
    runtime_xid bigint NOT NULL
  );
  CREATE FUNCTION public.capture_arch_runtime_progress()
  RETURNS trigger LANGUAGE plpgsql AS \$trigger\$
  BEGIN
    IF NEW.result_oid IN (
      'shiba.arch_fair_result_a'::regclass,
      'shiba.arch_fair_result_b'::regclass
    ) THEN
      INSERT INTO public.arch_runtime_audit(result_oid,applied_lsn,runtime_xid)
      VALUES (NEW.result_oid,NEW.applied_lsn,txid_current());
    END IF;
    RETURN NEW;
  END
  \$trigger\$;
  CREATE TRIGGER capture_arch_runtime_progress
  AFTER UPDATE ON shiba_internal.view_progress
  FOR EACH ROW EXECUTE FUNCTION public.capture_arch_runtime_progress()"

for ((commit=1; commit<=fairness_commits; commit++)); do
  psql_arch -qc "
    INSERT INTO public.arch_fair_source_a
    VALUES ($((710000 + commit)),$((commit % 7)),$commit)"
  psql_arch -qc "
    INSERT INTO public.arch_fair_source_b
    VALUES ($((720000 + commit)),$((commit % 9)),$commit)"
done
wait_for_query "${fairness_commits}|${fairness_commits}" "
  SELECT
    count(*) FILTER (WHERE result_oid='shiba.arch_fair_result_a'::regclass)
    || '|' ||
    count(*) FILTER (WHERE result_oid='shiba.arch_fair_result_b'::regclass)
  FROM shiba_internal.dag_inbox" \
  "both fairness backlogs to route"

psql_arch -qc "
  UPDATE shiba_internal.dag_runtime_state
  SET active=true
  WHERE result_oid IN (
    'shiba.arch_fair_result_a'::regclass,
    'shiba.arch_fair_result_b'::regclass
  )"
wait_for_query "0" "
  SELECT count(*) FROM shiba_internal.dag_inbox
  WHERE result_oid IN (
    'shiba.arch_fair_result_a'::regclass,
    'shiba.arch_fair_result_b'::regclass
  )" \
  "both fair-scheduled DAGs to drain"

assert_query "$((fairness_commits * 2))|$((fairness_commits * 2))" "
  SELECT count(*) || '|' || count(DISTINCT runtime_xid)
  FROM public.arch_runtime_audit"
assert_query "0" "
  WITH ordered AS (
    SELECT result_oid,applied_lsn,
           lag(applied_lsn) OVER (
             PARTITION BY result_oid ORDER BY audit_id
           ) AS previous_lsn
    FROM public.arch_runtime_audit
  )
  SELECT count(*) FROM ordered
  WHERE previous_lsn IS NOT NULL AND applied_lsn<=previous_lsn"
assert_query "t" "
  WITH transitions AS (
    SELECT audit_id,result_oid,
           result_oid IS DISTINCT FROM lag(result_oid) OVER (ORDER BY audit_id)
             AS starts_run
    FROM public.arch_runtime_audit
  ),
  runs AS (
    SELECT result_oid,
           sum(starts_run::integer) OVER (ORDER BY audit_id) AS run_id
    FROM transitions
  ),
  lengths AS (
    SELECT result_oid,run_id,count(*) AS run_length
    FROM runs GROUP BY result_oid,run_id
  )
  SELECT coalesce(max(run_length),0)<=1 FROM lengths"
wait_for_result "public.arch_fair_source_a" "shiba.arch_fair_result_a"
wait_for_result "public.arch_fair_source_b" "shiba.arch_fair_result_b"
assert_one_runtime

printf '\n==> Long apply exposes single-process head-of-line blocking\n'
psql_arch -qc "
  CREATE TABLE public.arch_long_apply_gate(enabled boolean NOT NULL);
  INSERT INTO public.arch_long_apply_gate VALUES (true);
  CREATE FUNCTION public.delay_arch_runtime_apply()
  RETURNS trigger LANGUAGE plpgsql AS \$trigger\$
  BEGIN
    IF NEW.result_oid='shiba.arch_fair_result_a'::regclass
       AND (SELECT enabled FROM public.arch_long_apply_gate) THEN
      PERFORM pg_sleep(4);
    END IF;
    RETURN NEW;
  END
  \$trigger\$;
  CREATE TRIGGER delay_arch_runtime_apply
  BEFORE UPDATE ON shiba_internal.view_progress
  FOR EACH ROW EXECUTE FUNCTION public.delay_arch_runtime_apply();
  INSERT INTO public.arch_fair_source_a VALUES (730001,1,100)"
wait_for_query "1" "
  SELECT count(*) FROM pg_stat_activity
  WHERE backend_type='shiba runtime' AND wait_event='PgSleep'" \
  "the Runtime to enter the long apply"
runtime_pid="$(psql_arch -Atqc "
  SELECT pid FROM pg_stat_activity WHERE backend_type='shiba runtime'")"

# The source commit succeeds, but the same Runtime cannot route it while its SPI
# transaction is sleeping in the preceding DAG apply.
psql_arch -qc "
  INSERT INTO public.arch_fair_source_b VALUES (730002,2,200)"
assert_query "0" "
  SELECT count(*) FROM shiba_internal.effective_change_log
  WHERE source_oid='public.arch_fair_source_b'::regclass
    AND row_data->>'event_id'='730002'"
assert_query "${runtime_pid}|1" "
  SELECT min(pid) || '|' || count(*)
  FROM pg_stat_activity
  WHERE backend_type='shiba runtime'"
wait_for_query "0" "
  WITH expected AS (
    SELECT group_id,count(*)::bigint AS row_count,sum(amount)::bigint AS total_amount
    FROM public.arch_fair_source_b GROUP BY group_id
  ),
  actual AS (
    SELECT group_id,row_count::bigint,total_amount::bigint
    FROM shiba.arch_fair_result_b
  ),
  difference AS (
    (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
    UNION ALL
    (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
  )
  SELECT count(*) FROM difference" \
  "routing and apply to resume after the long apply"
wait_for_result "public.arch_fair_source_a" "shiba.arch_fair_result_a"
wait_for_result "public.arch_fair_source_b" "shiba.arch_fair_result_b"
assert_query "${runtime_pid}" "
  SELECT pid FROM pg_stat_activity WHERE backend_type='shiba runtime'"

printf '\n==> Graceful-stop escalation restarts a Runtime stuck in PostgreSQL\n'
psql_arch -qc "
  UPDATE public.arch_long_apply_gate SET enabled=true;
  INSERT INTO public.arch_fair_source_a VALUES (730003,3,300)"
wait_for_query "1" "
  SELECT count(*) FROM pg_stat_activity
  WHERE backend_type='shiba runtime' AND wait_event='PgSleep'" \
  "the Runtime to enter an apply that cannot observe graceful SIGINT"
stuck_runtime_pid="$(psql_arch -Atqc "
  SELECT pid FROM pg_stat_activity WHERE backend_type='shiba runtime'")"
psql_arch -qc "UPDATE public.arch_long_apply_gate SET enabled=false"
psql_arch -qc "SELECT shiba._stop_runtime_for_deactivation()"
wait_for_query "1" "
  SELECT count(*) FROM pg_stat_activity
  WHERE backend_type='shiba runtime' AND pid<>${stuck_runtime_pid}" \
  "the escalated Runtime to restart while lifecycle remains active"
wait_for_result "public.arch_fair_source_a" "shiba.arch_fair_result_a"
assert_query "t" "
  SELECT active FROM shiba_internal.runtime_state WHERE singleton"

psql_arch -qc "
  UPDATE public.arch_long_apply_gate SET enabled=false;
  DROP TRIGGER delay_arch_runtime_apply ON shiba_internal.view_progress;
  DROP FUNCTION public.delay_arch_runtime_apply();
  DROP TABLE public.arch_long_apply_gate"

printf '\n==> Poison DAG retains shared input; repair replays it\n'
psql_arch -qc "
  CREATE TABLE public.arch_poison_source (
    event_id integer PRIMARY KEY,group_id integer NOT NULL,amount integer NOT NULL
  );
  CREATE TABLE public.arch_healthy_source (
    event_id integer PRIMARY KEY,group_id integer NOT NULL,amount integer NOT NULL
  );
  INSERT INTO public.arch_poison_source VALUES (1,1,1);
  INSERT INTO public.arch_healthy_source VALUES (1,1,1);
  CREATE TABLE shiba.arch_poison_result AS
  SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount
  FROM public.arch_poison_source GROUP BY group_id;
  CREATE TABLE shiba.arch_healthy_result AS
  SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount
  FROM public.arch_healthy_source GROUP BY group_id;
  CREATE FUNCTION public.reject_arch_poison_apply()
  RETURNS trigger LANGUAGE plpgsql AS \$trigger\$
  BEGIN
    RAISE EXCEPTION 'intentional poison DAG apply failure';
  END
  \$trigger\$;
  CREATE TRIGGER reject_arch_poison_apply
  BEFORE INSERT OR UPDATE OR DELETE ON shiba.arch_poison_result
  FOR EACH STATEMENT EXECUTE FUNCTION public.reject_arch_poison_apply();
  INSERT INTO public.arch_poison_source VALUES (740001,4,40);
  INSERT INTO public.arch_healthy_source VALUES (740002,4,40)"

wait_for_query "false|true" "
  SELECT active || '|' ||
         (last_error LIKE '%intentional poison DAG apply failure%')
  FROM shiba_internal.dag_runtime_state
  WHERE result_oid='shiba.arch_poison_result'::regclass" \
  "only the poison DAG to be quarantined"
poison_lsn="$(psql_arch -Atqc "
  SELECT commit_lsn FROM shiba_internal.effective_change_log
  WHERE source_oid='public.arch_poison_source'::regclass
    AND row_data->>'event_id'='740001'")"
assert_query "1|2" "
  SELECT
    (SELECT count(*) FROM shiba_internal.dag_inbox
     WHERE result_oid='shiba.arch_poison_result'::regclass
       AND commit_lsn='${poison_lsn}'::pg_lsn)
    || '|' ||
    (SELECT count(*) FROM shiba_internal.effective_change_log
     WHERE commit_lsn='${poison_lsn}'::pg_lsn)"
wait_for_result "public.arch_healthy_source" "shiba.arch_healthy_result"
psql_arch -Atqc "SELECT shiba.activate()" >/dev/null
assert_query "false|true|1" "
  SELECT runtime.active || '|' || (runtime.last_error IS NOT NULL) || '|' ||
         (SELECT count(*) FROM shiba_internal.dag_inbox inbox
          WHERE inbox.result_oid=runtime.result_oid)
  FROM shiba_internal.dag_runtime_state runtime
  WHERE result_oid='shiba.arch_poison_result'::regclass"
assert_one_runtime

psql_arch -qc "
  DROP TRIGGER reject_arch_poison_apply ON shiba.arch_poison_result;
  DROP FUNCTION public.reject_arch_poison_apply();
  UPDATE shiba_internal.dag_runtime_state
  SET active=true,last_error=NULL,failed_at=NULL
  WHERE result_oid='shiba.arch_poison_result'::regclass"
wait_for_query "0" "
  SELECT count(*) FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.arch_poison_result'::regclass" \
  "the repaired DAG to replay retained input"
wait_for_result "public.arch_poison_source" "shiba.arch_poison_result"
wait_for_query "0" "
  SELECT count(*) FROM shiba_internal.effective_change_log
  WHERE commit_lsn='${poison_lsn}'::pg_lsn" \
  "GC after repaired poison input is acknowledged"
wait_for_query "0" "
  SELECT count(*) FROM shiba_internal.ingress_transactions
  WHERE commit_lsn='${poison_lsn}'::pg_lsn" \
  "GC of the repaired poison transaction header"

printf '\n==> DROP removes DAG references and permits shared-log GC\n'
psql_arch -qc "
  CREATE TABLE public.arch_drop_source (
    event_id integer PRIMARY KEY,group_id integer NOT NULL,amount integer NOT NULL
  );
  INSERT INTO public.arch_drop_source VALUES (1,1,1);
  CREATE TABLE shiba.arch_drop_result AS
  SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount
  FROM public.arch_drop_source GROUP BY group_id;
  UPDATE shiba_internal.dag_runtime_state
  SET active=false
  WHERE result_oid='shiba.arch_drop_result'::regclass;
  INSERT INTO public.arch_drop_source VALUES (750001,5,50)"
wait_for_query "1" "
  SELECT count(*) FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.arch_drop_result'::regclass" \
  "the DROP target to retain a pending reference"
drop_result_oid="$(psql_arch -Atqc "
  SELECT 'shiba.arch_drop_result'::regclass::oid::integer")"
drop_lsn="$(psql_arch -Atqc "
  SELECT commit_lsn FROM shiba_internal.effective_change_log
  WHERE source_oid='public.arch_drop_source'::regclass
    AND row_data->>'event_id'='750001'")"
psql_arch -qc "DROP TABLE shiba.arch_drop_result"
assert_query "0" "
  SELECT sum(row_count) FROM (
    SELECT count(*) AS row_count FROM shiba_internal.stream_views WHERE result_oid=${drop_result_oid}::oid
    UNION ALL SELECT count(*) FROM shiba_internal.stream_filters WHERE result_oid=${drop_result_oid}::oid
    UNION ALL SELECT count(*) FROM shiba_internal.stream_having WHERE result_oid=${drop_result_oid}::oid
    UNION ALL SELECT count(*) FROM shiba_internal.stream_join_filters WHERE result_oid=${drop_result_oid}::oid
    UNION ALL SELECT count(*) FROM shiba_internal.inner_join_views WHERE result_oid=${drop_result_oid}::oid
    UNION ALL SELECT count(*) FROM shiba_internal.join_arrangements WHERE result_oid=${drop_result_oid}::oid
    UNION ALL SELECT count(*) FROM shiba_internal.stream_graphs WHERE result_oid=${drop_result_oid}::oid
    UNION ALL SELECT count(*) FROM shiba_internal.stream_graph_nodes WHERE result_oid=${drop_result_oid}::oid
    UNION ALL SELECT count(*) FROM shiba_internal.stream_graph_edges WHERE result_oid=${drop_result_oid}::oid
    UNION ALL SELECT count(*) FROM shiba_internal.operator_instances WHERE result_oid=${drop_result_oid}::oid
    UNION ALL SELECT count(*) FROM shiba_internal.aggregate_state WHERE result_oid=${drop_result_oid}::oid
    UNION ALL SELECT count(*) FROM shiba_internal.distinct_state WHERE result_oid=${drop_result_oid}::oid
    UNION ALL SELECT count(*) FROM shiba_internal.window_views WHERE result_oid=${drop_result_oid}::oid
    UNION ALL SELECT count(*) FROM shiba_internal.window_rows WHERE result_oid=${drop_result_oid}::oid
    UNION ALL SELECT count(*) FROM shiba_internal.distinct_views WHERE result_oid=${drop_result_oid}::oid
    UNION ALL SELECT count(*) FROM shiba_internal.projection_state WHERE result_oid=${drop_result_oid}::oid
    UNION ALL SELECT count(*) FROM shiba_internal.topn_views WHERE result_oid=${drop_result_oid}::oid
    UNION ALL SELECT count(*) FROM shiba_internal.topn_rows WHERE result_oid=${drop_result_oid}::oid
    UNION ALL SELECT count(*) FROM shiba_internal.view_progress WHERE result_oid=${drop_result_oid}::oid
    UNION ALL SELECT count(*) FROM shiba_internal.dag_inbox WHERE result_oid=${drop_result_oid}::oid
    UNION ALL SELECT count(*) FROM shiba_internal.dag_runtime_state WHERE result_oid=${drop_result_oid}::oid
  ) residual"
wait_for_query "0" "
  SELECT count(*) FROM shiba_internal.effective_change_log
  WHERE commit_lsn='${drop_lsn}'::pg_lsn" \
  "DROP to release the final reference for GC"
wait_for_query "0" "
  SELECT count(*) FROM shiba_internal.ingress_transactions
  WHERE commit_lsn='${drop_lsn}'::pg_lsn" \
  "DROP to permit transaction-header GC"
assert_one_runtime

architecture_log_errors="$(mktemp /tmp/shiba-runtime-log-errors.XXXXXX)"
expected_escalation_count="$(
  rg -c 'FATAL:  terminating background worker "shiba runtime" due to administrator command' \
    "${pg_log_file}" || true
)"
if test "${expected_escalation_count}" != "1"; then
  fail "expected exactly one deliberate Runtime SIGTERM escalation, saw ${expected_escalation_count}"
fi
rg -n 'WARNING|ERROR|FATAL|PANIC' "${pg_log_file}" \
  | rg -v 'FATAL:  terminating background worker "shiba runtime" due to administrator command' \
  >"${architecture_log_errors}" || true
if test -s "${architecture_log_errors}"; then
  sed -n '1,120p' "${architecture_log_errors}" >&2
  rm -f "${architecture_log_errors}"
  fail "PostgreSQL log contains warning-or-higher messages"
fi
rm -f "${architecture_log_errors}"

printf '\nSingle-Runtime architecture gate passed.\n'
