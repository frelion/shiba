# Shiba 全算子、全链路性能基线报告

## 1. 报告状态

本报告是 Shiba 当前实现的正式性能基线，覆盖源码
`src/logical.rs::OperatorKind` 中全部 **16 类逻辑算子**，并贯穿：

```text
CTAS snapshot/backfill
→ publication/WAL
→ WAL Router
→ durable dag_inbox
→ legacy per-result executor process
→ operator state
→ protected sink
→ result query
```

正式运行结果：

| 验收项 | 结果 |
|---|---:|
| 场景 | 27 |
| 重复轮次 | 3 |
| 场景实例 | 81 |
| 正确性门禁 | 3,801 |
| 双向 `EXCEPT ALL` 差异 | 0 |
| pgbench 失败事务 | 0 |
| 非空最终 inbox | 0 |
| PostgreSQL `ERROR/FATAL/PANIC` | 0 |
| OperatorKind 覆盖 | 16 / 16 |
| 原始可见延迟样本 | 3,240（每场景 120） |

正式 run 为 `20260725T160025Z`，状态为 **passed**。所有结论均取三轮中位数；
延迟 p50/p95/p99 直接从三轮合并后的原始样本计算，不是“每轮分位数的中位数”。
独立 reviewer 已完成阻断式复审并签字通过，签字记录保存在正式结果目录的
`REVIEW.md`。

## 2. 最重要的结论

1. **读取收益取决于结果压缩率。** 低基数 Aggregate、Join Aggregate、
   COUNT DISTINCT 等结果查询比源 SQL 快 **37.5–147.3 倍**；高基数 Aggregate、
   顶层 DISTINCT 和 Window 只快 **2.27–4.09 倍**，因为 sink 行数接近源表，
   甚至包含更宽的窗口列。
2. **小事务持续追平仍受 commit 调度限制。** 10 行/事务时，1 client 和
   4 clients 的端到端消费率分别为 **33.86** 和 **34.18 commits/s**，
   对应 **338.6** 和 **341.8 source rows/s**。这与当时 legacy per-result
   executor process 每 25 ms
   最多消费一个 commit 的结构一致。
3. **批量事务明显更高效。** 单个 5,000 行事务端到端中位数
   **969.6 ms**，应用速率 **5,588 rows/s**，是 10 行小事务行速率的约
   **16.4 倍**。
4. **TopN 是稳定的高成本算子。** 20 行事务的 commit-to-apply p50 为
   **749–760 ms**，p95 约 **839–840 ms**；当前实现对每个 delta 重建
   bounded sink。
5. **Window 成本由受影响分区大小决定。** 普通 Window p50 为
   **198–232 ms**；80% 数据集中在一个 partition 时 p50 为 **267 ms**、
   p95 为 **406 ms**，语义 UPDATE/DELETE 中位数分别达到
   **1.90 s / 2.56 s**。
6. **Outer Join 的 dimension-side 0↔1 边界昂贵。** Left/Full Join 的
   right-side first-match 中位数约 **1.08 s**，明显高于 fact-side 的
   57–127 ms。
7. **Null-aware Anti Join 的右侧 multiplicity 路径异常昂贵。**
   即使 1→2、2→1 不改变 public result，20 行动作仍约
   **1.52–1.84 s**，应作为 join 优化的首要剖析对象。

## 3. 测试机器与软件

| 项目 | 值 |
|---|---|
| Git commit | `b64a90c045d5c4eca70ef7f308604bafb1d89ee6` |
| OS | macOS 26.5.1 / arm64 |
| 机器 | Mac17,3 |
| CPU | 10 logical CPUs |
| 内存 | 24 GiB |
| PostgreSQL | 17.10 Homebrew |
| Rust | 1.96.0 |
| Cargo | 1.96.0 |
| pgrx | 0.19.1 |
| 构建 | `cargo pgrx install --release` |
| 连接 | 本机 Unix socket |
| 运行时间 | 2026-07-25 16:00:25Z–16:45:44Z |

正式规模：

| 参数 | 值 |
|---|---:|
| 初始行/主 source | 20,000 |
| 默认 groups | 100 |
| 每次语义 DML | 20 source rows |
| 延迟 probes / 场景 / run | 40 |
| 查询 clients | 4 |
| 源/结果查询时长 | 各 5 s |
| 重复次数 | 3 |
| 固定 seed | 20260725 |

PostgreSQL 使用 `shared_buffers=1GB`、`work_mem=64MB`、`jit=off`、
`fsync=on`、`synchronous_commit=on`、`track_io_timing=on`、
`track_wal_io_timing=on`、`track_commit_timestamp=on`。

## 4. 算子与语义覆盖

| OperatorKind | 独立/代表场景 |
|---|---|
| Scan / Project / Sink | 所有 27 场景；Project 包含 alias 和多类型列 |
| Filter | AND/OR/NOT、比较、IS NULL、boolean、bigint |
| Aggregate | COUNT、SUM；低/高基数和 80% hotspot |
| Distinct | 顶层 DISTINCT；低/高基数 COUNT(DISTINCT) |
| Having | 单表 threshold；复合 Join+Distinct+Having |
| InnerJoin | 1:1、fanout=4、cross-input filter |
| LeftJoin / RightJoin / FullJoin | 各自独立场景 |
| SemiJoin | EXISTS、IN |
| AntiJoin | NOT EXISTS |
| NullAwareAntiJoin | NOT IN，含右侧 NULL 全局边界 |
| TopN | LIMIT；OFFSET+LIMIT；ASC/DESC；NULLS FIRST/LAST |
| Window | row_number/rank/dense_rank/count/sum/avg/min/max |
| Window frames | default、ROWS、RANGE、GROUPS |
| Window distribution | peer、小分区、80% skewed 大分区 |

每个普通场景都执行：

- CTAS backfill；
- rollback；
- INSERT、UPDATE、DELETE；
- 关键边界动作；
- 暂停当时的 legacy per-result executor process 后 Router enqueue + inbox
  drain；
- 40 个独立已提交延迟 probes；
- 每个动作后的双向 `EXCEPT ALL`；
- warm pgbench 源查询与 sink 查询；
- PostgreSQL shared-buffer-cold 的 `EXPLAIN ANALYZE`；
- state/source/result bytes、WAL、I/O、CPU、RSS。

Join 两侧均变更；Semi/Anti/Null-aware Anti 覆盖
`0→1→2→1→0` multiplicity、NULL 和无可见结果的事务；TopN 覆盖榜内、榜外、
边界、offset；Window UPDATE 同时改变 partition/order key。

另有：

- 同一个 source 扇出到 Aggregate、Filter Aggregate、Distinct、TopN 四个 DAG；
- facts/dims 两个 source 扇出到两个单源 Aggregate 和一个 Join DAG；
- 1 client、4 clients、小事务及单个 5,000 行大事务。

## 5. 方法与指标口径

### 5.1 隔离与重复

- 每次 scenario 使用全新 database、extension、publication 和 logical slot。
- 整个正式 run 使用临时 PostgreSQL cluster，不访问开发者已有数据库。
- baseline schema 与 source schema 使用相同建表、数据和 DML。
- 三轮 scenario 顺序以固定 seed 分别随机化。
- 查询 source/result 的先后顺序按 repetition 交替。

### 5.2 延迟

源事务通过 `track_commit_timestamp` 获取 PostgreSQL 的真实 commit timestamp。
Router 时间取 `routed_transactions.routed_at`；结果时间优先取
`view_progress.updated_at`。如果事务被算子完全过滤、没有 public delta，则用
对应 commit 的 inbox 删除被客户端观察到的时刻作为 apply 上界，原始样本的
`apply_timestamp_source` 会明确标注这一点。

因此可拆分：

- commit → route；
- route → apply/inbox acknowledgement；
- commit → apply。

### 5.3 WAL / I/O

- `baseline_ingress_observed_*` 与 `shiba_ingress_observed_*` 分别包围各自提交；
- `combined_phase_*` 包含 baseline、Shiba、Router、apply 和观测轮询，名称明确
  不将其归因给单一组件；
- PostgreSQL-wide start/end 统计用于描述整套 harness 的总 I/O/WAL，不代表
  单独 Shiba 成本。

### 5.4 冷热状态

- warm query：显式预热后运行 4-client pgbench；
- buffer-cold query：重启 PostgreSQL 清空 shared buffers 后执行；
- macOS 文件系统 page cache 未清空，因此不能称为 storage-cold。

## 6. 回填、查询收益与空间

下表均为三轮中位数。状态大小是该 result 对应 operator-state row payload；
source/result 包含 relation 与 index 总大小。

| 场景 | Backfill ms | Warm 查询延迟加速 | State KiB | Source KiB | Result KiB |
|---|---:|---:|---:|---:|---:|
| Aggregate low cardinality | 86.3 | 71.69× | 2.2 | 1,256 | 24 |
| Aggregate high cardinality | 151.1 | 2.27× | 1,564.1 | 1,256 | 1,560 |
| Aggregate hotspot | 77.6 | 61.95× | 3.1 | 1,256 | 24 |
| Filter + Project | 389.2 | 37.50× | 8.1 | 1,256 | 24 |
| Having | 79.2 | 82.19× | 9.0 | 1,256 | 24 |
| COUNT DISTINCT low | 220.0 | 121.65× | 1,397.7 | 1,256 | 24 |
| COUNT DISTINCT high | 218.5 | 118.25× | 1,397.8 | 1,256 | 24 |
| Top-level DISTINCT | 176.2 | 2.59× | 1,873.2 | 1,256 | 1,024 |
| TopN LIMIT | 249.6 | 67.59× | 3,575.7 | 1,256 | 2,248 |
| TopN OFFSET | 255.1 | 49.31× | 3,575.7 | 1,256 | 1,824 |
| Window all functions | 308.7 | 2.93× | 4,003.6 | 1,256 | 22,496 |
| Window ROWS | 275.7 | 3.59× | 4,003.6 | 1,256 | 10,576 |
| Window RANGE | 271.2 | 4.09× | 3,984.7 | 1,256 | 10,568 |
| Window GROUPS | 291.0 | 3.88× | 3,984.7 | 1,256 | 10,568 |
| Window peers | 273.8 | 2.88× | 3,908.3 | 1,256 | 15,600 |
| Window skewed partition | 289.5 | 3.16× | 3,924.4 | 1,256 | 34,776 |
| Inner Join 1:1 | 372.1 | 57.54× | 2,444.4 | 936 | 656 |
| Inner Join fanout=4 | 556.5 | 147.31× | 2,465.9 | 968 | 656 |
| Left Join | 575.2 | 104.97× | 2,452.7 | 936 | 880 |
| Right Join | 411.7 | 91.27× | 2,453.0 | 936 | 656 |
| Full Join | 562.0 | 110.97× | 2,453.1 | 936 | 880 |
| Join+Filter+Distinct+Having | 1,606.7 | 138.84× | 2,936.6 | 936 | 216 |
| EXISTS Semi Join | 256.7 | 54.06× | 2,124.4 | 928 | 440 |
| IN Semi Join | 247.4 | 53.68× | 2,124.4 | 928 | 440 |
| NOT EXISTS Anti Join | 239.6 | 53.30× | 2,124.4 | 928 | 448 |
| NOT IN Null-aware Anti | 195.9 | 70.73× | 2,120.5 | 928 | 416 |
| bigint Filter Aggregate | 314.6 | 40.27× | 10.5 | 1,256 | 32 |

最快 backfill 是热点 Aggregate 的 77.6 ms；最慢是复合 Join 的 1.607 s。
高基数 Aggregate/顶层 DISTINCT 的查询收益只有约 2.3–2.6 倍，说明“维护结果”
并不自动等于巨大查询收益：结果 cardinality 和 row width 是决定因素。

buffer-cold（仅 PostgreSQL shared buffers cold）下，同样呈现该趋势：
高基数 Aggregate、顶层 DISTINCT 约 2.85–2.95 倍；低基数 Aggregate
约 99–109 倍；Join 聚合约 126–257 倍；Window 约 5.4–8.2 倍。

## 7. Commit-to-apply 延迟

每个样本是一个 20-row source 事务；下表由每场景 120 个原始样本计算。

| 场景 | p50 ms | p95 ms | p99 ms | max ms |
|---|---:|---:|---:|---:|
| Aggregate high cardinality | 79.4 | 127.2 | 140.4 | 143.7 |
| Aggregate hotspot | 74.1 | 130.8 | 139.6 | 168.5 |
| Aggregate low cardinality | 74.9 | 124.2 | 131.5 | 135.0 |
| Filter + Project | 82.9 | 131.2 | 136.4 | 276.2 |
| Having | 62.9 | 119.1 | 136.0 | 223.3 |
| COUNT DISTINCT high | 75.9 | 123.2 | 129.0 | 130.9 |
| COUNT DISTINCT low | 76.7 | 120.3 | 134.1 | 249.5 |
| Top-level DISTINCT | 77.8 | 127.8 | 142.0 | 145.7 |
| Inner Join 1:1 | 82.3 | 124.8 | 150.8 | 159.3 |
| Inner Join fanout | 81.0 | 120.9 | 145.2 | 153.9 |
| Left Join | 93.2 | 134.6 | 146.0 | 168.5 |
| Right Join | 74.3 | 120.1 | 143.9 | 158.0 |
| Full Join | 72.0 | 127.1 | 138.8 | 147.3 |
| Join composed | 77.6 | 129.3 | 140.7 | 151.0 |
| EXISTS Semi | 73.3 | 121.0 | 136.8 | 142.2 |
| IN Semi | 73.9 | 119.2 | 146.6 | 153.9 |
| NOT EXISTS Anti | 79.7 | 129.4 | 139.7 | 249.1 |
| NOT IN Null-aware Anti | 75.9 | 121.5 | 127.4 | 131.6 |
| bigint Filter Aggregate | 70.8 | 113.5 | 131.9 | 133.4 |
| TopN LIMIT | 759.7 | 840.0 | 853.6 | 868.0 |
| TopN OFFSET | 749.0 | 839.1 | 859.9 | 866.9 |
| Window all functions | 231.5 | 298.0 | 336.4 | 371.5 |
| Window ROWS | 211.0 | 280.4 | 301.6 | 304.2 |
| Window RANGE | 198.2 | 295.9 | 319.9 | 324.6 |
| Window GROUPS | 206.9 | 269.9 | 291.0 | 300.2 |
| Window peers | 211.4 | 290.1 | 328.6 | 392.6 |
| Window skewed | 267.1 | 406.3 | 522.7 | 565.5 |

除 TopN/Window 外，大多数普通 probes 的 p50 为 63–93 ms、p95
为 113–135 ms。这仍反映 Router 100 ms 与当时 legacy per-result executor
process 25 ms 的轮询结构。

注意 Null-aware Anti 的 latency probes 是 left-side 插入；它们没有体现昂贵的
right-side multiplicity 动作。语义动作结果见下一节。

## 8. DML 语义边界

所有普通动作是 20 source rows；以下是三轮中位数：

| 关键动作 | Commit-to-apply |
|---|---:|
| Left Join fact insert/update/delete | 98–125 ms |
| Left Join dimension first match | 1,083 ms |
| Left Join dimension update | 898 ms |
| Full Join dimension first match | 1,081 ms |
| Inner Join 1:1 语义动作中位数 | 260 ms |
| Inner Join fanout 语义动作中位数 | 277 ms |
| Null-aware Anti right 0→1 | 1,431 ms |
| Null-aware Anti right 1→2（无结果变化） | 1,836 ms |
| Null-aware Anti right 2→1（无结果变化） | 1,519 ms |
| Null-aware Anti right 1→0 | 1,551 ms |
| Null-aware Anti 删除/插入 NULL | 690 / 661 ms |
| TopN insert/update/delete | 838 / 1,364 / 770 ms |
| Skewed Window insert/update/delete | 220 / 1,895 / 2,562 ms |

`source_rows_per_second` 只使用源动作行数，不声称代表 join/window 产生的
output delta 数。尤其 outer-join 与 window partition rebuild 会产生远多于
20 个内部/结果变化。

## 9. 源写入、并发与批量

固定 SQL 为每事务插入 10 行：

| 场景 | Baseline TPS | Shiba TPS | Shiba/Baseline | 端到端 commits/s | 端到端 rows/s |
|---|---:|---:|---:|---:|---:|
| 1 client | 16,858 | 4,000 | 0.237 | 33.86 | 338.6 |
| 4 clients | 39,534 | 13,497 | 0.328 | 34.18 | 341.8 |

源表的 client-visible 写入 TPS 随并发增加，但 streaming result 的持续追平速度
几乎不变。这是“source ingest”与“end-to-end sustainable throughput”的明确
分离。

大事务：

| 指标 | 中位数 | CV |
|---|---:|---:|
| 5,000 行源提交 | 76.1 ms | 0.55% |
| 端到端 | 969.6 ms | 6.96% |
| 应用速率 | 5,588 rows/s | 7.90% |

## 10. 多 DAG

| 场景 | 负载 | 端到端中位数 | WAL | State |
|---|---|---:|---:|---:|
| 同源 4 DAG | 200 source rows → Aggregate/Filter/Distinct/TopN | 7,910.7 ms | 2.63 MiB | 3.62 MiB |
| 双源 3 DAG | 100 facts + 100 dims → 两个 Aggregate + Join | 157.4 ms | 497 KiB | 2.33 MiB |

同源 4-DAG 被 TopN 的逐 delta sink rebuild 主导，不能把 7.9 s 解释为 Router
纯扇出成本。两个多 DAG 场景均逐结果 `EXCEPT ALL=0`、最终 inbox=0，并采集
CPU/RSS/WAL/I/O/state。

## 11. 资源与全局 I/O

15,974 个 100 ms process-tree 样本：

| 指标 | 结果 |
|---|---:|
| PostgreSQL CPU peak | 400%（约 4 cores） |
| PostgreSQL RSS peak | 273.3 MiB |
| source warm query 平均 CPU | 351.4% |
| result warm query 平均 CPU | 264.2% |
| fanout apply 平均 CPU | 103.2% |

最高 RSS 出现在 skewed Window，其次为同源多 DAG、Window all-functions。

整个 45 分钟 harness（包含 baseline、数据库反复创建/销毁、checkpoint、查询、
Shiba）产生约：

- WAL：2.92 GiB；
- read：1.63 GiB；
- write：1.24 GiB；
- extend：1.10 GiB；
- requested checkpoints：252；
- fsync：32,255。

这些是 cluster-wide 总量，不能单独归因给 Shiba。逐动作可归因窗口见
`action-samples.csv` 的 `baseline_ingress_observed_*`、
`shiba_ingress_observed_*` 和 `combined_phase_*`。

## 12. 稳定性与方差

关键容量指标较稳定：

- 1/4 client 端到端 commits/s CV 约 1%；
- 大事务 source commit CV 0.55%；
- 大事务 end-to-end CV 6.96%；
- 同源多 DAG end-to-end CV 6.39%。

部分极短 warm read 指标方差较高：

- Null-aware Anti source TPS CV 34.3%；
- Null-aware Anti result TPS CV 28.4%；
- RANGE Window source TPS CV 20.2%。

因此优化这些读路径时，应延长单场查询时长或增加重复次数；不能只用当前
三轮的微小百分比变化宣称改善。默认门禁：

- >10% 且三轮方向一致：可信变化；
- 5–10%：增加到至少 5 runs；
- <5%：默认视为噪声；
- correctness、inbox、failed transactions、log errors 任一非零：run 无效。

## 13. 优化优先级

### P0：legacy per-result executor process busy-drain

当前小事务极限约 34 commits/s，直接对应每 25 ms 一次、每轮一个 commit。
应让该 legacy executor process 在 inbox 非空时连续处理 commit，仅空闲时
等待，并设置行数/时间 budget 防止单 DAG 饥饿其他工作。

验收指标：

- `ingress_concurrency_and_batching/*/end_to_end_commits_per_second`；
- 普通算子 p50/p95；
- CPU/WAL；
- 3,801 correctness checks。

### P1：TopN 增量 sink

当前每个 delta 删除并重建 bounded sink；20-row transaction 需要约 0.75–1.36 s，
且同源多 DAG 被 TopN 主导。应维护边界候选和只更新变化行。

### P1：Window partition update 合并

同一个 source commit 内对同一 partition 的多个 delta 当前重复 rebuild。
应按 commit 收集受影响 partition，每个 partition 最多重建一次。skewed Window
UPDATE/DELETE 是直接验收场景。

### P1：Null-aware Anti / Outer Join boundary

优先剖析：

- Null-aware Anti 1→2 / 2→1 无可见变化仍需 1.5–1.8 s；
- Left/Full dimension first-match 约 1.08 s。

需要记录 arrangement probe rows、emitted deltas、aggregate groups touched，避免
只靠 wall time 推断。

### P2：Router latency

普通 p50/p95 仍明显包含 100 ms Router 周期。可评估 source-trigger latch、
busy drain 和空闲退避；必须保持 durable route/slot-advance 两阶段协议。

## 14. 可复现与未来对比

正式复现：

```bash
./scripts/performance-matrix.py
```

固定 run ID：

```bash
SHIBA_MATRIX_RUN_ID=after-optimization ./scripts/performance-matrix.py
```

优化前后必须保持：

- workload checksum；
- PostgreSQL 配置与版本；
- 20k rows / 100 groups / 20 mutations；
- 40 probes / 5 s query / 4 clients / 3 repetitions；
- seed、机器、电源与明显后台负载；
- warm/buffer-cold 口径。

正式结果保存了：

- 被测 commit 与完整 git status；
- tracked working-tree binary patch；
- untracked files archive；
- Cargo.lock、Cargo.toml、runner、scenario catalog 精确副本；
- SHA-256；
- 每轮随机顺序；
- per-scenario SQL、pgbench 原文、EXPLAIN JSON、operator graph；
- 原始 actions/resources/metrics；
- PostgreSQL 配置和完整日志。

本次 workload SHA-256：

```text
performance-matrix.py  3b728f07250e7019392ce446e921d68e0f78b55b72e472d07df254d75647ca35
operator_matrix.py     506e3adeed98425fe0ea2ee6acc827551f130086055c33b2ba700e2c5b1f6e23
Cargo.lock             03c7a259b353c2a1362e5d19cba3603c40c5686638d9f3af690e4589c99eb4eb
```

## 15. 适用边界

- 这是单机 PostgreSQL、Unix socket、warm / shared-buffer-cold 基线，不包含网络。
- macOS OS page cache 未清空，不是 storage-cold。
- 每个普通 scenario 只保持一个 result DAG，以隔离算子；多 DAG 另行测量。
- 20k 是可比较的中等规模，不是容量上限；超内存、长时间 soak、checkpoint
  饱和和生产混合 workload 仍需专项测试。
- 当前支持范围之外的 set operation、non-equi join、self join 等不进入性能
  矩阵；注册拒绝由 correctness suite 负责。
- 当前 commit-to-apply probe 是 20 行事务；不要解释成单行延迟。
- 部分很短的 DML phase 只有 2–4 个 100 ms CPU/RSS 样本，资源数字只适合
  判断方向，不适合作为精确差异。
- `observed ingress` WAL/I/O 窗口仍可能夹带并发 Router/apply 活动，不等于
  纯写放大。
- 全程 252 次 requested checkpoints 主要来自 database lifecycle 和
  buffer-cold restart，也会给共享 cluster 统计带来噪声。

## 16. 证据索引

正式结果目录：`performance/matrix-results/20260725T160025Z/`

- `manifest.json`：最终验收状态；
- `operator-coverage.json`：16/16 覆盖；
- `scenario-catalog.json`：完整 SQL/DML/边界；
- `metrics-raw.csv`：130,863 条原始指标；
- `metrics-summary.csv`：三轮 median/mean/stdev/CV；
- `action-samples.csv`：3,609 个动作样本；
- `latency-summary.csv`：每场景 120 原始 probes 的总体分位数；
- `resources.csv`：15,974 个 CPU/RSS 样本；
- `scenario-summaries.csv`：81 个场景实例；
- `postgres-stats-start.json` / `postgres-stats-end.json`；
- `postgresql.conf` / `postgresql.log` / `log-errors.json`；
- `cargo-test.txt`：52 个 Rust/pgrx 测试的持久化输出；
- `REVIEW.md`：独立 reviewer 最终签字；
- `run-1/`、`run-2/`、`run-3/`：per-scenario 原始证据；
- `workload/`、`checksums.sha256`、`working-tree.patch`、
  `untracked-files.tar.gz`。
