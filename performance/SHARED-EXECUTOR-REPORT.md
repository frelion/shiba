# Single Runtime + Physical Stage 重构验收报告

> **状态：FUNCTIONAL PASS / FINAL PERFORMANCE PENDING**
>
> correctness、事务、恢复和单 Runtime 拓扑已通过。下述 v4 数据来自最终
> review 修复前的完整矩阵，只作为中间性能证据；当前源码恢复了与 baseline
> 一致的 100ms 资源采样，并新增 lifecycle、类型 identity 和 large→small
> Stage 修复，必须在后续最终矩阵完成后更新结论。

## 被测版本与证据

- 工作区基点：`967ffd5c48511d57f5dff3d6f00331d2dbad9447` 加本次未提交 patch；
- PostgreSQL：17.10 (Homebrew)；
- 参数：20,000 初始行、100 groups、20 mutations、40 latency probes、
  5 秒查询负载、4 clients、5,000 行大事务、3 randomized repetitions；
- baseline：`performance/matrix-results/20260728-single-runtime-final`；
- 中间 candidate：`performance/matrix-results/20260728-stage-final-v4`；
- candidate manifest：81 个普通 scenario runs、3,801 个 correctness checks、
  0 correctness failures、0 pgbench failures、0 PostgreSQL log errors；
- operator coverage：16/16，完整；
- topology 的 `actual_runtime_counts=[1]`，
  `legacy_worker_counts=[0]`。

每个结果目录都包含 environment、工作区 patch、workload checksum、raw
metrics、resource samples、runtime topology、PostgreSQL 配置和日志。

## 架构结果

- 每个 active database 只有一个真实 `shiba runtime` BGW；
- Router、调度、DAG apply 和 GC 都是该 backend 内的串行阶段；
- `DagRuntime` 只是进程内 plan/scheduling 抽象，不拥有 source payload 或
  operator state；
- source transaction payload 在 logged `change_log` 中只存一份；
- `dag_inbox(result_oid, commit_lsn)` 只保存事务级引用；
- 一个 DAG 消费一个 source commit 是一个 PostgreSQL transaction，state、
  sink、progress 和 inbox ack 一起提交或回滚；
- logical graph 在注册时编译为确定性的 versioned physical plan；
- Join 的跨 statement 中间结果使用 typed per-DAG UNLOGGED Stage；
- Stage 不是 authority；crash 后可以清空并从 logged inbox/change-log 重放。

## 正确性与恢复

最终代码通过：

```text
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --lib                         86 passed
scripts/test-e2e.sh
scripts/test-differential-single.sh      120 rounds
scripts/test-join-differential.sh        68 commits / 424 comparisons
scripts/test-concurrency-recovery.sh
scripts/test-executor-architecture.sh
scripts/test-failpoint-recovery.sh
scripts/test-all.sh
```

覆盖包括 source commit 的双侧 Join cross term、outer/semi/anti/null-aware
边界、并发 activation、round-robin 公平性、poison DAG 隔离、DROP/GC、route
提交后 slot advance 前 crash，以及 apply 修改后 ack/commit 前 crash。

## 性能结果

与 baseline 的三轮汇总：

| 指标 | 最终结果 |
| --- | ---: |
| visibility p50 | 27/27 改善；场景 delta 中位数 **-61.1%** |
| visibility mean | 27/27 改善；场景 delta 中位数 **-52.1%** |
| visibility p95 | 24/27 改善；场景 delta 中位数 **-39.7%** |
| visibility p99 | 26/27 改善；场景 delta 中位数 **-35.9%** |
| backfill wall | 18/27 改善；场景 delta 中位数 **-1.3%** |
| warm result TPS | 场景 delta 中位数 **-0.4%** |
| single-client ingress E2E rows/s | **+472%** |
| four-client ingress E2E rows/s | **+917%** |
| multi-DAG fanout deliveries/s | **+2.2%** |

p95 的三个负向点是 RIGHT JOIN `+10.4%`、FULL JOIN `+5.7%` 和
inner fanout `+0.8%`；它们的 p50、mean 和 p99 均改善。backfill 最差为
SEMI `+6.9%`，其余主要 Join 注册为 `+4%` 到 `+6.5%`。

早期 candidate 因新 relation 没有统计信息，outer/semi p95 曾回归
27%–39%。最终实现将 state `ANALYZE` 放到 `DagRuntime` 首次加载、物理
program 执行之前，既不延长用户 CTAS transaction，又消除了持续冷计划。

## 资源与 Stage

- Stage 在所有采样点都是 `live_tuples=0`；
- 最大 Stage 文件是 FULL JOIN 的 2,580,480 bytes；
- 所有 Stage 都低于 64 MiB compaction threshold；
- 强制低阈值 E2E 验证了仅空 Stage 会被 TRUNCATE，正常 commit 不改变
  relfilenode；
- Join authority state 因 `join_key text` 改为保留 NULL/typed equality 的
  JSONB，20k 行场景 payload 增加约 25%–30%（绝对增加约 0.65–0.76 MiB）；
- Runtime RSS 高水位相对 baseline 约增加 6–19 MiB。该内存来自 PostgreSQL
  集合化 query 的 sort/hash/JSONB working set，不是 Rust 缓存 payload；
  TopN 的 `next_state` 已改为流式消费，避免固定 materialization。

## 性能证据限制

连续多轮完整矩阵显示特殊场景有明显机器状态漂移：

- 同一 Stage candidate 的 5,000 行事务相对 baseline 曾测得 `+44.8%`
  rows/s，也曾测得 `-23.8%`；
- multi-DAG fanout 曾测得 `+31.5%`，最终长跑为 `+2.2%`；
- 同期普通 warm-query TPS 从约 `+11%` 漂移到约 `0%`，说明绝对吞吐受持续
  负载影响。

因此延迟、correctness 和 topology 结论可信；大事务/fanout 的绝对 baseline
delta 不足以证明稳定提升或稳定回归。要做严格 release performance gate，
应在受控 power mode 下用 baseline/candidate 交错执行，而不是先完整跑
baseline、数小时后再完整跑 candidate。

## 结论

架构和功能目标已完成，且常规 commit visibility 显著改善。当前残余不是
内存爆炸：operator payload 仍在 PostgreSQL relation 中，Stage 最终为空，
Runtime 只有一个进程；但集合化执行确实以更高 PostgreSQL backend working
set 换取更低延迟和更高并发 ingress 吞吐。

在把“性能无回归”定义为所有绝对吞吐和 RSS 指标都不得变差时，本报告不能
宣称完全通过；在接受上述 bounded PostgreSQL working-set tradeoff，并以
visibility/ingress 为主要目标时，候选可以进入提交。
