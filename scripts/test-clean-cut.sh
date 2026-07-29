#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

old_symbols='ExecutionPipeline|ExecutionDescriptor|QueryAnalysis|ValidatedQuery|LogicalPlan|LogicalNode|LogicalEdge|PhysicalDagPlan|physical_plan|physical_stage_id|explain_physical|DagRuntime|max_cached_dags|DagStep|NextApplyOutcome|compile_physical_plan|index_ddl_invoker|query_text|target_query_text|_register_(stream|inner_join|subquery|window|distinct|topn)_stream_table|_begin_stream_registration|_prepare_stream_drop|_apply_next_dag_change_log|_apply_claimed_dag_batch|_step_operator|_provision_[a-z_]+|checkpoint_operator|peek_effect_stream|aggregate_catalog_capability|trusted_slot_type_sql|trusted_btree_comparison|shiba_internal\.(stream_views|inner_join_views|stream_graphs|stream_graph_nodes|stream_graph_edges|stream_filters|stream_having|stream_join_filters|join_arrangements|aggregate_state|distinct_state|window_views|window_rows|distinct_views|projection_state|topn_views|topn_rows|physical_plans|physical_stages|operator_instances|dag_runtime_state|ingress_decode_batches|view_progress|dag_inbox|routing_tasks)\b|shiba_physical_stages|quarantined|resource_blocked'

if matches="$(
  rg -n "$old_symbols" \
    src sql scripts docs README.md CONTRIBUTING.md \
    -g '!scripts/test-clean-cut.sh'
)"; then
  printf '%s\n' "$matches" >&2
  printf '%s\n' \
    "old execution architecture is still present in code, tests, or docs" >&2
  exit 1
fi

if matches="$(rg -n 'own\.row_value|existing\.row_value' src/kernel/join.rs)"; then
  printf '%s\n' "$matches" >&2
  printf '%s\n' \
    "Join own-row identity must use its indexed binary row_key" >&2
  exit 1
fi

if matches="$(rg -n 'record_send\(input_row\.row_value\)' src/kernel/join.rs)"; then
  printf '%s\n' "$matches" >&2
  printf '%s\n' \
    "Join input identity must canonicalize through canonical_row_key_sql" >&2
  exit 1
fi

if matches="$(rg -n 'aggregate_seen|Aggregate DISTINCT.*seen state' src/kernel)"; then
  printf '%s\n' "$matches" >&2
  printf '%s\n' \
    "Aggregate DISTINCT must use its ordered durable tuple cursor, not an unbounded seen relation" >&2
  exit 1
fi

if matches="$(rg -n 'IS NOT DISTINCT FROM' src/kernel/distinct.rs)"; then
  printf '%s\n' "$matches" >&2
  printf '%s\n' \
    "Distinct SQL keys must use their resolved exact B-tree equality" >&2
  exit 1
fi

if matches="$(
  rg -n 'pg_catalog\.record_send' src/kernel -g '!storage.rs'
)"; then
  printf '%s\n' "$matches" >&2
  printf '%s\n' \
    "kernel row identity must use the shared canonical_row_key_sql helper" >&2
  exit 1
fi

if matches="$(
  rg -n 'jsonb_populate_record|to_jsonb' src/kernel/storage.rs
)"; then
  printf '%s\n' "$matches" >&2
  printf '%s\n' \
    "canonical row identity must use one named-composite text roundtrip" >&2
  exit 1
fi

if matches="$(rg -n 'pg_catalog\.to_jsonb' sql/11_ingress.sql)"; then
  printf '%s\n' "$matches" >&2
  printf '%s\n' \
    "ingress must retain original per-column pgoutput text JSON" >&2
  exit 1
fi

if matches="$(
  rg -n 'jsonb_populate_record|to_jsonb|pg_column_size' \
    sql/12_effect_stream.sql
)"; then
  printf '%s\n' "$matches" >&2
  printf '%s\n' \
    "effect row bytes must measure the complete binary record" >&2
  exit 1
fi

if matches="$(
  rg -n \
    'fn (topn_index_order|window_index_order|resolve_order|resolve_window_order|resolve_binary_operator|resolve_window_binary_operator)\b' \
    src/kernel
)"; then
  printf '%s\n' "$matches" >&2
  printf '%s\n' \
    "ordered kernels must use the shared B-tree capability resolver" >&2
  exit 1
fi

if matches="$(rg -n '#\[cfg\(any\(\)\)\]|\bobsolete\b' src/kernel)"; then
  printf '%s\n' "$matches" >&2
  printf '%s\n' \
    "disabled or obsolete kernel paths must be deleted, not retained" >&2
  exit 1
fi

for removed_file in \
  src/query_analysis.rs \
  src/query_tree.rs \
  src/logical/compile.rs \
  src/logical/persist.rs \
  src/logical/physical.rs \
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
