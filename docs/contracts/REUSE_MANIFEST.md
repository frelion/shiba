# Clean-room reuse manifest

This manifest is the review record for evidence from
`/Users/zzhang/Documents/Shiba` at commit
`6af593c3f30a4519592c53f0abca96ff74a89e66` (the evidence snapshot is 2026-08-02).
Legacy gate scripts use their bytes at that commit; the common harness is
`/Users/zzhang/Documents/Shiba/scripts/lib/v2-pg-gate.sh` (banner
`shiba-v2-pg-gate-source-v1`). PG17 is 17.10 and PG18 is 18.4 in the recorded
environment. Only the Protocol JSON/schema and canonical digest rows below have
been migrated as data-only fixtures and re-proved by clean-room tests. The PG
manifest is an evidence index: its scenarios remain deferred.

| 成果 | 来源 | 分类A/B/C | 复用方式 | 证据 | 未证明边界 |
|---|---|---|---|---|---|
| Protocol JSON/schema 样例 | `/Users/zzhang/Documents/Shiba/crates/shiba-protocol/src/lib/tests.rs`; `/Users/zzhang/Documents/Shiba/crates/shiba-protocol/src/primitive.rs` | A | 已迁移为纯数据 fixture `tests/fixtures/protocol/canonical-v1.json`；不复制旧实现或文档段落 | `canonical-v1.provenance.md`；`PG_CONFIG=/opt/homebrew/opt/postgresql@17/bin/pg_config ./scripts/test-l0.sh` 联合 Protocol 定向测试在 clean-room 重证明固定 JSON shape、严格解码与 round-trip | 跨进程 transport 与消费者不存在 |
| canonical digest 测试向量 | `/Users/zzhang/Documents/Shiba/crates/shiba-protocol`; `/Users/zzhang/Documents/Shiba/scripts/test-contract-surface.sh` | A | 已迁移为同一纯数据 fixture；在新 `WireEnvelope`/domain-separated SHA-256 边界独立实现 | `canonical-v1.provenance.md`；L0 fixture 校验与 `canonical_wire_vector_is_stable_and_roundtrips` 共同重证明 canonical bytes、digest 与语义敏感性 | 非 Phase-1 message、跨语言实现未证明 |
| PG17/18 行为差分样例 | `/Users/zzhang/Documents/Shiba/docs/v2/TEST_MATRIX.md`; `/Users/zzhang/Documents/Shiba/scripts/test-v2-pg-gate-harness.sh` | A | 仅迁移为 `tests/fixtures/pg/deferred-evidence.json` 证据索引；场景仍 **Deferred** | PG17.10/PG18.4；索引仅保留 provenance；尚无 clean-room 差分结果 | 所有 PG 行为差分场景尚未重证明 |
| pgoutput 解码边界样例 | `/Users/zzhang/Documents/Shiba/scripts/test-pgoutput-tuple-shapes-v2.sh` | A | **Deferred** frame expectations only | PG17/18; absolute script path plus each absolute pg_config | Decoder absent |
| TOAST、NULL、空 tuple、streaming transaction 向量 | `/Users/zzhang/Documents/Shiba/scripts/test-pgoutput-toast-*-v2.sh`; `/Users/zzhang/Documents/Shiba/scripts/test-pgoutput-tuple-shapes-v2.sh`; `/Users/zzhang/Documents/Shiba/scripts/test-replication-ingress.sh` | A | **Deferred** fixtures with individual provenance | PG17/18; each absolute script path plus absolute pg_config | Ingress/parser absent |
| source identity / replica identity 规则 | `/Users/zzhang/Documents/Shiba/docs/v2/source-identity-registry.md`; `/Users/zzhang/Documents/Shiba/scripts/test-source-identity-*-v2.sh` | A | **Deferred** contract examples, not registry code | PG17/18; each absolute script path plus absolute pg_config | Admission/lifecycle absent |
| catalog binding 字段语义 | `/Users/zzhang/Documents/Shiba/docs/v2/source-registry-apply-protocol.md`; `/Users/zzhang/Documents/Shiba/scripts/test-catalog-bindings-v2.sh` | A | 仅进入 PG 证据索引，字段语义 **Deferred**；Phase-1 singleton version authority 是新边界，不等同于旧 binding 实现 | PG17/18；旧脚本绝对路径加绝对 pg_config | Live binding inspector absent |
| continuation/CAS 不变量 | `/Users/zzhang/Documents/Shiba/docs/v2/INVARIANTS.md`; `/Users/zzhang/Documents/Shiba/scripts/test-continuation-store-v2.sh` | A | **Deferred** as requirements for a new store | PG17/18; absolute script path plus absolute pg_config | Store, crash proof absent |
| DDL invalidation ObjectAddress 语义 | `/Users/zzhang/Documents/Shiba/scripts/test-ddl-invalidation-v2.sh`; `/Users/zzhang/Documents/Shiba/docs/v2/INVARIANTS.md` | A | **Deferred** scenario fixtures; no name-based substitute | PG17/18; absolute script path plus absolute pg_config | DDL observer absent |
| crash/rollback/concurrency/performance 矩阵 | `/Users/zzhang/Documents/Shiba/docs/v2/TEST_MATRIX.md`; `/Users/zzhang/Documents/Shiba/scripts/test-fanout-recovery.sh`; `/Users/zzhang/Documents/Shiba/scripts/performance-matrix.py` | A | 已迁移证据索引；extension install rollback 为 **partially reproved**，其余仍 **Deferred** | PG17/18；`test-empty-install.sh` 只证明 Phase-1 rollback；旧矩阵命令保留为来源 | Runtime crash/concurrency/performance 与组件 rollback 未证明 |
| 已验证 failure cases | `/Users/zzhang/Documents/Shiba/docs/v2/REQUIREMENT_EVIDENCE.json`; `/Users/zzhang/Documents/Shiba/scripts/test-*-v2.sh` | A | **Deferred** negative-case registry | Legacy commit; each matched absolute gate command and PG major recorded on import | Need case-by-case clean-room reproduction |
| 文档正确性不变量 | `/Users/zzhang/Documents/Shiba/docs/v2/INVARIANTS.md` | A | Re-expressed independently in these contracts; individual evidence stays deferred | Legacy commit; `/Users/zzhang/Documents/Shiba/scripts/test-v2-doc-contract.sh` | Runtime-level proof absent |
| 可审计移植规则 | This clean-room decision; prospective sources must be recorded per change | B | No B implementation in Phase 1. A later commit must state source file, exact function/algorithm, deleted legacy dependencies, new boundary, clean-room equivalence evidence, LOC budget, and unproved boundary | Review + targeted clean-room test; no legacy test alone suffices | Every B candidate is unapproved until reviewed |
| V1/V2 巨石、`sql/00_catalog.sql`、old authorities | `/Users/zzhang/Documents/Shiba/sql/00_catalog.sql` and monolith modules | C | Reference only; never migrate | L0 gate; manual review | None: explicitly prohibited |
| publications/change logs; runtime/operator SQL; registration mixes | `/Users/zzhang/Documents/Shiba/sql/10_runtime.sql`, `11_ingress.sql`, `12_effect_stream.sql`, `30_registration.sql` | C | Reference only; never migrate | L0 gate; manual review | None: explicitly prohibited |
| fallback/alias/adapter/dual-write and locally tested unowned code | Legacy paths and any candidate lacking recovery/transaction proof | C | Never migrate | L0 gate plus ownership review | None: explicitly prohibited |

Review rule: the main agent audits every row before a fixture or B-class rewrite is
accepted. An absolute legacy path is provenance, never an import path.
