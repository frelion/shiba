# Shiba Aggregate Commit-Batch 集合化执行报告

## 1. 结论

本阶段把普通单源 Aggregate 的大 commit 从“commit 级批量传输、逐行执行”
推进到“commit 级批量传输、PostgreSQL 内集合化聚合”。它没有改变 WAL、
Router、durable inbox、commit 级事务或 progress/ack 原子性，只替换了满足
条件的物理算子执行路径。

在与 commit-batch executor 基线相同的机器、PostgreSQL 配置、数据规模、
固定 seed 和 3 次重复下，5,000 行、100 个 group 的单事务结果为：

| 指标 | 基线 `20260726T011500Z` | 本阶段 `20260726T043000Z` | 变化 |
|---|---:|---:|---:|
| commit → ack | 854.48 ms | 196.08 ms | **-77.1%，4.36× faster** |
| end-to-end wall | 869.14 ms | 211.98 ms | **-75.6%** |
| apply rows/s | 5,851.5 | 25,500.3 | **+335.8%，4.36×** |
| source commit wall | 15.35 ms | 16.94 ms | +10.4% |

source commit 发生在异步 apply 之前，代码没有修改 source 写入路径；本次
1.60 ms 差异不能归因于 Aggregate fast path。

正式矩阵为 **passed**：27 个场景、3 次重复、81 个场景实例、16/16 类
逻辑算子、3,801 项正确性检查，正确性差异、pgbench 失败事务和 PostgreSQL
错误均为 0。

本阶段的适用边界必须明确：

- 只集合化单 source、普通 `GROUP BY + COUNT/SUM` Aggregate；
- batch 至少 64 个 delta 才启用；
- `COUNT(DISTINCT)`、Join、TopN、Window、顶层 Distinct 等继续走严格有序
  的逐 delta fallback；
- JSONB 仍是 Rust/SPI 的 transport ABI；进入 PostgreSQL 后才恢复成 typed
  source row，并不是二进制列式 batch；
- sink 由“每个 delta 一次”降为“每个受影响 group 一次”，尚未改成单条
  set-based sink upsert。

因此，这一阶段证明的是：低/中 group 基数的大 Aggregate commit 已经消除
主要逐行 SPI/PL/pgSQL 放大；它不代表所有算子已经向量化。

## 2. 架构变化

### 2.1 基线执行路径

```text
source commit
→ WAL / Router / durable dag_inbox
→ Rust 按 commit 构造 JSONB DeltaBatch
→ 一次 SPI 调用 _apply_dag_delta_batch
→ PL/pgSQL 按 ordinality 循环
→ 每个 delta 更新 Aggregate state
→ 每个 delta 同步 sink
→ progress 一次 + inbox ack（同一事务）
```

基线已经消除了“每行一次 SPI”和“每 commit 固定等待”的调度问题，但 SQL
内部仍逐 delta 执行。对 5,000 行事务，它仍产生 5,000 次 state/sink 处理。

### 2.2 本阶段 fast path

```text
source commit
→ WAL / Router / durable dag_inbox
→ Rust 按 commit 构造 JSONB DeltaBatch
→ 一次 SPI 调用 _apply_dag_delta_batch
→ jsonb_populate_record 恢复 typed source rows
→ 一条 GROUP BY 合并每个 group 的 count/sum 增量
→ 每个 group 更新一次 Aggregate state
→ 每个受影响 group 同步一次 sink
→ progress 一次 + inbox ack（同一事务）
```

5,000 行、100 个 group 的测试中，state/sink 工作量的数量级由行数降到受影响
group 数。64 行同一 group 的 pgrx 测试通过 sink audit trigger 证明第一次
batch 只写一次 sink；随后 32 条撤回和 32 条迁入另一个 group，只再写两个
sink group。

### 2.3 自适应分发与 fallback

`_apply_dag_delta_batch` 先获取原有 DAG transaction advisory lock。只有
`jsonb_array_length(events) >= 64` 时才读取物理 metadata 并判断 fast path：

1. `view_kind = 'aggregate'`；
2. 不是 `COUNT(DISTINCT)`；
3. 没有 Join metadata；
4. batch 中每个 event 的 `source_oid` 都属于该单 source view。

不满足条件时继续调用原有 `_apply_dag_delta_state`，保留 ordinality 和
UPDATE `-1/+1` 顺序。小 commit 不执行额外的 metadata dispatch query。

阈值 64 是当前保守工程阈值，不是完整阈值 sweep 得出的硬件最优值。下一阶段
应在不同 row/group 比例上做 16/32/64/128 的隔离 A/B，再决定是否配置化。

## 3. 正确性与事务语义

### 3.1 不变量

- 一个 source commit 仍对应一个 executor 数据库事务；
- operator state、sink、`view_progress` 和 inbox ack 仍原子提交；
- 失败会回滚整个 commit，inbox 可重试；
- progress 仍只推进一次；
- batch 中 `delta` 只允许 `-1/+1`，`row_data` 必须是 JSON object；
- fast path 在写 state 前验证最终 row count、COUNT state、SUM 非 NULL 计数
  均合法，撤回不存在的行会以 Shiba 专用 `P0S01` SQLSTATE 失败；
- group key 使用 JSONB 表示，SQL NULL 继续映射为 JSON `null`，与现有 state
  key 语义一致。

Aggregate 的 count/sum 增量在不含 DISTINCT 和 Join 时对同一事务内的输入
是可交换、可合并的，所以 fast path 可以不依赖 delta ordinality。需要
0↔1 multiplicity 边界或全局顺序的算子没有进入该分支。

### 3.2 回归门禁

最终代码通过：

- `cargo fmt --all -- --check`；
- `cargo clippy --all-targets -- -D warnings`；
- `cargo test --lib`：58/58；
- 普通 writer 权限 E2E；
- 单 source 120 轮确定性 differential；
- Join differential：66 次 committed mutations、408 次 comparison；
- concurrency、transaction、persistent-slot recovery；
- durable transaction boundary、跨 source WAL 顺序、UPDATE ordering；
- backlog architecture gate：160 commits，2,169.20 commits/s；
- 正式性能矩阵全部 correctness checks。

开发过程中，扩展测试实际捕获并阻止了一个动态 SQL 参数编号错误：fast path
曾把 event `source_oid` 与 result OID 比较，导致事件被静默过滤。修复后重新
运行了上述完整门禁。

独立 reviewer 随后发现第二个 blocker：PostgreSQL `numeric` 合法支持
`NaN` / `Infinity`，删除这类 group 的最后一行时中间 SUM 可能为 `NaN`；
旧路径按 `row_count=0` 删除 group，新校验却曾错误要求中间 `sum_value=0`。
现在零行 group 与旧路径一样只校验计数不变量并直接删除，同时新增 64-event
NaN/Infinity 回归测试。修复后再次运行了完整门禁和最终正式矩阵。
reviewer 复核确认 blocker 已关闭，未发现新的提交阻断项。

## 4. 可复现性

### 4.1 对比对象

| 项目 | executor 基线 | Aggregate batch |
|---|---|---|
| Run | `20260726T011500Z` | `20260726T043000Z` |
| Git / patch | commit-batch 重构 captured tree；后续基线 commit `88d9ed5` | `88d9ed5` + captured working-tree patch |
| 状态 | passed | passed |
| 场景实例 | 81 | 81 |
| 重复 | 3 | 3 |
| Seed | 20260725 | 20260725 |
| 主 source 初始行数 | 20,000 | 20,000 |
| latency probes / 场景 / run | 40 | 40 |

两个 run 保存的四个 workload 文件 SHA-256 完全一致。PostgreSQL 配置只在
随机生成的临时 Unix socket 目录名上不同；其余被测参数一致。结果目录包含
environment、场景目录、随机顺序、原始 action samples、资源样本、数据库
日志、working-tree patch、untracked archive 和 checksums。

### 4.2 测试机器

| 项目 | 值 |
|---|---|
| 平台 | macOS 26.5.1，arm64 |
| CPU | 10 logical CPUs |
| 内存 | 24 GiB |
| PostgreSQL / pgbench | 17.10 Homebrew |
| Rust / Cargo | 1.96.0 |
| matrix 参数 | rows=20,000，groups=100，mutations=20 |
| 重复 / seed | 3 / 20260725 |

PostgreSQL 使用 `shared_buffers=1GB`、`work_mem=64MB`、`jit=off`、
`fsync=on`、`synchronous_commit=on`、`track_io_timing=on`、
`track_wal_io_timing=on` 和 `track_commit_timestamp=on`。

### 4.3 重放

```bash
./scripts/test-all.sh
SHIBA_MATRIX_RUN_ID=<UTC_RUN_ID> \
SHIBA_MATRIX_REPETITIONS=3 \
./scripts/performance-matrix.py
```

正式数据：

- 基线：`performance/matrix-results/20260726T011500Z/`
- 本阶段：`performance/matrix-results/20260726T043000Z/`

## 5. 性能结果

### 5.1 核心收益：大 Aggregate commit

正式 large transaction 是向同一个 source 表单事务插入 5,000 行，均匀分布
到 100 个 group；结果与对 source 表重新 `GROUP BY` 做双向差分，三轮差异
均为 0。

| 指标 | 基线中位数 | 新中位数 | 变化 |
|---|---:|---:|---:|
| source commit wall | 15.35 ms | 16.94 ms | +10.4% |
| commit → ack | 854.48 ms | 196.08 ms | **-77.1%** |
| end-to-end wall | 869.14 ms | 211.98 ms | **-75.6%** |
| apply rows/s | 5,851.5 | 25,500.3 | **+335.8%** |

三次新 run 的 commit→ack 范围为 190.91–213.89 ms，均快于基线范围
830.25–1,017.87 ms，不是由单次最好样本造成。

### 5.2 小 commit 延迟

普通 operator scenario 每个 action 通常只有 20 个 affected rows，低于阈值，
应该走原有有序 fallback。对全部 3,609 个 action samples：

| 延迟边界 | 基线 p50 | 新 p50 | 变化 | 基线 p95 | 新 p95 | 变化 |
|---|---:|---:|---:|---:|---:|---:|
| commit → route | 59.37 ms | 58.09 ms | -2.2% | 116.57 ms | 118.41 ms | +1.6% |
| route → apply | 24.20 ms | 24.70 ms | +2.1% | 665.95 ms | 636.53 ms | -4.4% |
| commit → apply | 101.42 ms | 100.40 ms | -1.0% | 725.64 ms | 691.71 ms | -4.7% |

整体没有可确认的小 commit 回归或收益。`aggregate_low_cardinality` 的
route→apply p50 从 16.51 ms 到 19.07 ms（+15.5%），但 p95 只从 30.13 ms
到 30.64 ms（+1.7%），而 commit→apply p50 从 66.15 ms 到 52.36 ms
（-20.8%）、p95 从 129.21 ms 到 121.08 ms（-6.3%）。分段指标方向相反，
且 20-row action 不触发 fast path，不能把差异归因于新算子。

### 5.3 入口 TPS：绝对值不能归因于本阶段

| 指标 | 基线 | 新 run | 变化 |
|---|---:|---:|---:|
| 1 client Shiba source TPS | 10,371 | 9,832 | -5.2% |
| 1 client plain-PG TPS | 17,419 | 15,642 | -10.2% |
| 4 clients Shiba source TPS | 23,549 | 22,391 | -4.9% |
| 4 clients plain-PG TPS | 36,298 | 31,368 | -13.6% |
| 1 client Shiba/plain-PG ratio | 0.618 | 0.618 | +0.05% |
| 4 clients Shiba/plain-PG ratio | 0.649 | 0.731 | +12.6% |

本阶段没有改 source 写入或小 commit apply 路径，且同 run 的 plain PostgreSQL
control 降幅更大。归一化 ratio 单 client 基本持平、4 clients 改善 12.6%；
这不支持“小事务发生结构性回归”，也不能证明 fast path 提升了小事务。
control-inclusive mixed-stage commits/s 下降 11.3% / 4.1%，但该 phase 串行
包含 plain-PG control，不是纯 Shiba 端到端容量，不能用来否定或证明 fast
path。

### 5.4 多 DAG / 多 source

| 指标 | 基线 | 新 run | 变化 |
|---|---:|---:|---:|
| multi-DAG fanout wall | 7,625.63 ms | 7,005.18 ms | -8.1% |
| fanout source deliveries/s | 104.91 | 114.20 | +8.9% |
| multi-source multi-DAG wall | 147.85 ms | 151.42 ms | +2.4% |

multi-DAG fanout 同时含 Aggregate、Distinct、Filter 和 TopN，只有其中的普通
Aggregate 可能使用 fast path；8.1% 改善不能全部归因于新实现。多 source
场景包含 Join，因此走 fallback，2.4% 差异视为稳定区间内噪声。

## 6. 已知限制与下一阶段

1. **高 group 基数大 batch 尚未被隔离测试。** 正式
   `aggregate_high_cardinality` 的 action 只有 20 行，低于 fast-path 阈值；
   large transaction 只有 100 个 group。下一步要增加 5,000 行/5,000 group
   和不同 row/group 比率的专项矩阵。
2. **sink 仍逐 group 调用。** 低基数已显著改善，但高基数时每 group 一次
   `_sync_aggregate_sink` 可能成为新瓶颈，应改成 affected-groups relation
   驱动的一条 set-based upsert/delete。
3. **JSONB transport 有解析成本。** 当前 JSONB 保持 Rust/SQL ABI 稳定，
   PostgreSQL 内已经 typed；后续可比较 composite arrays、temporary relation
   或 binary `COPY` 风格输入，但要先量化 JSON parse 占比。
4. **只覆盖 COUNT/SUM 普通 Aggregate。** `COUNT(DISTINCT)` 的 multiplicity
   state、Join 的 0↔1 边界、TopN/Window 的排序/分区维护需要各自的集合化
   算法，不能直接复用交换律。
5. **并发同一 DAG 仍串行。** transaction advisory lock 是正确性边界，也会
   限制热点 DAG 的并发上限；在单 DAG 内做并行之前，应先消除逐 group sink，
   再用 profile 判断是否值得引入更复杂的分片锁。
6. **定向边界测试还可继续加密。** 当前已有 64-event、撤回、group 迁移、
   sink/progress 写次数和非有限 numeric；63/64 阈值两侧、NULL group、
   HAVING fast path 和 fast path 中途故障注入仍应补成独立测试。

推荐下一阶段顺序：

1. 增加 Aggregate batch 的 row-count × group-cardinality × threshold sweep；
2. 把 Aggregate sink 同步集合化，并做低/高基数隔离 A/B；
3. 将相同 typed batch 框架扩展到 Filter/Project + Aggregate 组合；
4. 单独设计 Join 的按 join-key commit coalescing，保持 multiplicity 边界；
5. 最后处理 TopN/Window，因为它们需要排序索引和受影响分区级重算策略。
