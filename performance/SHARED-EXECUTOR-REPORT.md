# Single Runtime + Physical Stage 重构验收报告

> **状态：FUNCTIONAL PASS / PERFORMANCE CONDITIONAL**
>
> correctness、事务、恢复、单 Runtime 拓扑和最终完整性能矩阵均通过。
> commit visibility、warm query、并发 ingress 和 fanout 明显改善；但
> backfill、大事务吞吐和 Runtime RSS 存在回归，且大事务有明显轮间漂移，
> 因此不能宣称所有性能指标零回归。

## 被测版本与证据

- candidate commit：`3d769e9a52d05fafc124c2b6198f8920c7b0c900`；
- PostgreSQL：17.10 (Homebrew)；
- 参数：20,000 初始行、100 groups、20 mutations、40 latency probes、
  5 秒查询负载、4 clients、5,000 行大事务、3 randomized repetitions；
- baseline：`performance/matrix-results/20260728-single-runtime-final`；
- 最终 candidate：`performance/matrix-results/20260728T120356Z`；
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
| visibility p50 | 27/27 改善；场景 delta 中位数 **-60.0%** |
| visibility mean | 27/27 改善；场景 delta 中位数 **-51.1%** |
| visibility p95 | 25/27 改善；场景 delta 中位数 **-39.9%** |
| visibility p99 | 27/27 改善；场景 delta 中位数 **-38.8%** |
| backfill wall | 6/27 改善；场景 delta 中位数 **+3.7%** |
| warm result TPS | 22/27 改善；场景 delta 中位数 **+7.8%** |
| single-client ingress E2E rows/s | **+532.3%** |
| four-client ingress E2E rows/s | **+859.6%** |
| multi-DAG fanout deliveries/s | **+5.2%** |
| 5,000-row transaction rows/s | **-11.4%** |

p95 的两个负向点是 inner fanout `+10.3%` 和 LEFT JOIN `+0.2%`；
它们的 p50、mean 和 p99 均改善。backfill 最差为 SEMI IN `+15.8%`、
SEMI EXISTS `+12.4%`、NULL-aware anti `+12.3%` 和 RIGHT JOIN
`+12.2%`。

早期 candidate 因新 relation 没有统计信息，outer/semi p95 曾回归
27%–39%。最终实现只在共享 state relation 尚无统计信息时于
`DagRuntime` load 分析；Join Stage 达到 1,024 行时在 consume 前按实际
cardinality 分析，并在 consume 后按空 Stage 重置统计。

## 资源与 Stage

- Stage 在所有采样点都是 `live_tuples=0`；
- 最大 Stage 文件是 LEFT JOIN 的 2,580,480 bytes；
- 所有 Stage 都低于 64 MiB compaction threshold；
- 强制低阈值 E2E 验证了仅空 Stage 会被 TRUNCATE，正常 commit 不改变
  relfilenode；
- Join authority state 因 `join_key text` 改为保留 NULL/typed equality 的
  JSONB，20k 行场景 payload 增加约 25%–30%（绝对增加约 0.65–0.76 MiB）；
- 5,000 行事务的 PostgreSQL RSS 中位峰值从 139,360 KiB 增至
  146,016 KiB（`+4.8%`），Runtime RSS 从 34,288 KiB 增至
  39,664 KiB（`+15.7%`）。该内存来自 PostgreSQL 集合化 query 的
  sort/hash/JSONB working set，不是 Rust 缓存 payload；
- TopN 的 `NOT MATERIALIZED` pipeline 避免额外 tuplestore，但 V1 仍扫描、
  展开并排序完整 retained multiset，不是增量 indexed TopN。

## 性能证据限制

最终矩阵恢复了与 baseline 一致的 100ms 资源采样，且 scenario catalog
SHA-256 完全相同。driver 本身因 singleton topology、Runtime PID 和共享
payload instrumentation 不同而 checksum 不同，因此仍不是同二进制
harness 的严格 A/B。

连续运行显示特殊场景有明显机器状态漂移：

- candidate 的 5,000 行事务三轮为 18,867、15,603、13,485 rows/s；
  baseline 为 17,304、17,609、18,961 rows/s。candidate 首轮提升 `9.0%`，
  三轮中位数却回归 `11.4%`；
- candidate multi-DAG fanout 三轮为 3,170、2,517、2,443 deliveries/s，
  但三轮均高于对应 baseline，三轮中位数提升 `5.2%`。

因此延迟、correctness 和 topology 结论可信；大事务/fanout 的绝对 baseline
delta 不足以证明稳定提升或稳定回归。要做严格 release performance gate，
应在受控 power mode 下用 baseline/candidate 交错执行，而不是先完整跑
baseline、数小时后再完整跑 candidate。

## 结论

架构和功能目标已完成，且常规 commit visibility 显著改善。当前残余不是
内存爆炸：operator payload 仍在 PostgreSQL relation 中，Stage 最终为空，
Runtime 只有一个进程；但集合化执行确实以更高 PostgreSQL backend working
set 换取更低延迟和更高并发 ingress 吞吐。

在把“性能无回归”定义为所有绝对吞吐、backfill 和 RSS 指标都不得变差时，
本报告明确不通过；在接受 bounded PostgreSQL working-set、以 commit
visibility、warm query 和并发 ingress 为主要目标时，候选功能架构可以
接受。若大事务吞吐是 release blocker，应在稳定机器上交错执行 A-B-B-A
后再签字。
