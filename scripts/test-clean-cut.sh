#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

reject_matches() {
  local pattern="$1"
  local message="$2"
  shift 2

  local matches
  local status
  set +e
  matches="$(git grep --untracked -n -E -e "$pattern" -- "$@" 2>&1)"
  status=$?
  set -e

  if test "$status" -eq 0; then
    printf '%s\n%s\n' "$matches" "$message" >&2
    exit 1
  fi
  if test "$status" -ne 1; then
    printf '%s\n' "$matches" >&2
    exit "$status"
  fi
}

old_symbols='ExecutionPipeline|ExecutionDescriptor|QueryAnalysis|ValidatedQuery|LogicalPlan|LogicalNode|LogicalEdge|PhysicalDagPlan|physical_plan|physical_stage_id|explain_physical|DagRuntime|max_cached_dags|DagStep|NextApplyOutcome|compile_physical_plan|index_ddl_invoker|query_text|target_query_text|_register_(stream|inner_join|subquery|window|distinct|topn)_stream_table|_begin_stream_registration|_prepare_stream_drop|_apply_next_dag_change_log|_apply_claimed_dag_batch|_step_operator|_provision_[a-z_]+|checkpoint_operator|peek_effect_stream|aggregate_catalog_capability|trusted_slot_type_sql|trusted_btree_comparison|shiba_internal\.(stream_views|inner_join_views|stream_graphs|stream_graph_nodes|stream_graph_edges|stream_filters|stream_having|stream_join_filters|join_arrangements|aggregate_state|distinct_state|window_views|window_rows|distinct_views|projection_state|topn_views|topn_rows|physical_plans|physical_stages|operator_instances|dag_runtime_state|ingress_decode_batches|view_progress|dag_inbox|routing_tasks)\b|shiba_physical_stages|quarantined|resource_blocked'

reject_matches \
  "$old_symbols" \
  "old execution architecture is still present in code, tests, or docs" \
  src sql scripts docs README.md CONTRIBUTING.md \
  ':(exclude)scripts/test-clean-cut.sh'
reject_matches \
  'shiba\.(ingress_batch_rows|ingress_batch_bytes|stage_chunk_rows|stage_chunk_bytes|stage_admission_rows|stage_admission_bytes)\b|shiba_internal\.(ingress_apply_batches|effect_stream_payloads)\b' \
  "removed batching GUCs and payload catalog tables must not re-enter the clean-cut architecture" \
  src sql scripts docs README.md CONTRIBUTING.md \
  ':(exclude)scripts/test-clean-cut.sh'
reject_matches \
  'CREATE FUNCTION shiba\.(activate|deactivate|_ensure_runtime|_ensure_logical_slot)\(' \
  "database lifecycle control has one Rust implementation, not a PL/pgSQL wrapper or fallback" \
  sql
reject_matches \
  'CREATE FUNCTION shiba_internal\.publish_source_batch\(' \
  "source publication control flow has one Rust implementation" \
  sql
reject_matches \
  'shiba_internal\.publish_source_batch' \
  "the Runtime must call the Rust source publisher directly" \
  src/worker.rs
reject_matches \
  'CREATE FUNCTION shiba_internal\.insert_ingress_events\(' \
  "bounded ingress admission has one Rust implementation" \
  sql
reject_matches \
  'shiba_internal\.insert_ingress_events' \
  "the Runtime must call Rust ingress admission directly" \
  src/worker.rs
reject_matches \
  'CREATE FUNCTION (shiba\._prepare_dataflow_drops|shiba_internal\._lock_all_dataflows_for_utility)\(' \
  "dataflow DROP lock planning has one Rust implementation" \
  sql
reject_matches \
  'own\.row_value|existing\.row_value' \
  "Join own-row identity must use its indexed binary row_key" \
  src/execution/join
reject_matches \
  'record_send\(input_row\.row_value\)' \
  "Join input identity must canonicalize through canonical_row_key_sql" \
  src/execution/join
reject_matches \
  'aggregate_seen|Aggregate DISTINCT.*seen state' \
  "Aggregate DISTINCT must use its ordered durable tuple cursor, not an unbounded seen relation" \
  src/execution
reject_matches \
  'IS NOT DISTINCT FROM' \
  "Distinct SQL keys must use their resolved exact B-tree equality" \
  src/execution/distinct
reject_matches \
  'pg_catalog\.record_send' \
  "kernel row identity must use the shared canonical_row_key_sql helper" \
  src/execution ':(exclude)src/execution/storage.rs'
reject_matches \
  'jsonb_populate_record|to_jsonb' \
  "canonical row identity must use one named-composite text roundtrip" \
  src/execution/storage.rs
reject_matches \
  'pg_catalog\.to_jsonb' \
  "ingress must retain original per-column pgoutput text JSON" \
  sql/11_ingress.sql
reject_matches \
  'jsonb_populate_record|to_jsonb|pg_column_size' \
  "effect row bytes must measure the complete binary record" \
  sql/12_effect_stream.sql
reject_matches \
  'fn (topn_index_order|window_index_order|resolve_order|resolve_window_order|resolve_binary_operator|resolve_window_binary_operator)\b' \
  "ordered kernels must use the shared B-tree capability resolver" \
  src/execution
reject_matches \
  '#\[cfg\(any\(\)\)\]|\bobsolete\b' \
  "disabled or obsolete kernel paths must be deleted, not retained" \
  src/execution
reject_matches \
  'StepTxn|StepExecution|StepOutcome|StepContext::begin|\.commit\(' \
  "operator algorithms must return StepReceipt through KernelRunner" \
  src/execution/linear src/execution/sink src/execution/distinct \
  src/execution/join src/execution/aggregate src/execution/window src/execution/topn
reject_matches \
  'StepReceipt::new|StepReceipt \{|BackgroundWorker::transaction|StartTransaction|CommitTransaction|AbortOutOfAnyTransaction' \
  "operator algorithms must not forge transitions or manage PostgreSQL transactions" \
  src/execution/linear src/execution/sink src/execution/distinct \
  src/execution/join src/execution/aggregate src/execution/window src/execution/topn

operator_roots=(
  src/execution/linear
  src/execution/sink
  src/execution/distinct
  src/execution/join
  src/execution/aggregate
  src/execution/window
  src/execution/topn
)
reject_matches \
  'append_effect_stream_chunk' \
  "operator SQL must publish data through StepContext, not append the effect stream directly" \
  "${operator_roots[@]}"
reject_matches \
  'INSERT[[:space:]]+INTO[[:space:]]+shiba_internal\.(effect_stream_chunks|effect_streams)|UPDATE[[:space:]]+shiba_internal\.(effect_stream_chunks|effect_streams)|DELETE[[:space:]]+FROM[[:space:]]+shiba_internal\.(effect_stream_chunks|effect_streams)' \
  "operators must not mutate effect-stream catalog rows outside the shared publication primitive" \
  "${operator_roots[@]}"
reject_matches \
  'INSERT[[:space:]]+INTO[[:space:]]+shiba_internal\.operator_checkpoints|UPDATE[[:space:]]+shiba_internal\.operator_checkpoints|DELETE[[:space:]]+FROM[[:space:]]+shiba_internal\.operator_checkpoints' \
  "operators must not mutate the shared checkpoint directly" \
  "${operator_roots[@]}"
reject_matches \
  'next_chunk_seq[[:space:]]*=' \
  "operators must not maintain the shared output cursor directly" \
  "${operator_roots[@]}"
reject_matches \
  'record_published_output|record_published_frontier' \
  "operators must use the current StepContext output boundary" \
  "${operator_roots[@]}"
for output_operator in linear distinct join aggregate window topn; do
  if ! rg -q 'record_output_append' "src/execution/${output_operator}" --glob '*.rs'; then
    printf 'data-producing operator is not wired to the shared output boundary: %s\n' \
      "${output_operator}" >&2
    exit 1
  fi
done

receipt_definitions=$(rg -l 'pub\(crate\) struct StepReceipt' src/execution --glob '*.rs' | wc -l | tr -d ' ')
if test "$receipt_definitions" -ne 1; then
  printf 'StepReceipt must have exactly one definition, found %s\n' "$receipt_definitions" >&2
  exit 1
fi
if rg -n 'StepReceipt::new|StepReceipt \{' src/execution --glob '*.rs' --glob '!step.rs'; then
  printf 'StepReceipt must only be constructed inside step.rs\n' >&2
  exit 1
fi
reject_matches \
  'StepTxn|linear/join/distinct/aggregate/window/topn/sink::execute|LoadedDataflow::step\b|without waiting for the trailing pgoutput|Only committed source WAL reaches|已发布部分 source chunks.*Commit|peak_queued_bytes|queue high-water' \
  "documentation must describe the current streaming and KernelRunner architecture" \
  README.md docs

for removed_file in \
  src/query_analysis.rs \
  src/query_tree.rs \
  src/planner/compile.rs \
  src/planner/persist.rs \
  src/planner/physical.rs \
  sql/20_operator_filters.sql \
  sql/21_operator_aggregate.sql \
  sql/22_operator_unary_batches.sql \
  sql/23_operator_join_batch.sql \
  sql/20_operator_kernels.sql \
  sql/21_join_kernel.sql \
  sql/22_aggregate_distinct_kernels.sql \
  sql/23_window_topn_kernels.sql \
  sql/24_operator_dispatch.sql \
  sql/26_physical_stages.sql \
  docs/MVP.md
do
  if test -e "$removed_file"; then
    printf 'old architecture file still exists: %s\n' "$removed_file" >&2
    exit 1
  fi
done

printf '%s\n' "clean-cut architecture guard passed"
