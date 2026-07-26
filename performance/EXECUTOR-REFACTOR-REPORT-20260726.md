# Shiba Commit-Batch Executor 重构与性能对比报告

## 1. 结论

本轮重构已经达到预定目标：去除了 DAG worker “每 25 ms 最多处理一个
source commit”的结构性限速，同时保持 source commit 的边界、WAL 顺序、
UPDATE 的 `-1/+1` 顺序和 progress 原子提交语义。

在与旧基线相同的机器、PostgreSQL 配置、数据规模、固定 seed 和 3 次重复下：

| 指标 | 旧基线 | 重构后 | 变化 |
|---|---:|---:|---:|
| 单 client 混合阶段有效率 | 33.86 commits/s | 432.37 commits/s | **12.77×** |
| 4 clients 混合阶段有效率 | 34.18 commits/s | 523.28 commits/s | **15.31×** |
| 单 client 混合阶段有效行速率 | 338.6 rows/s | 4,323.7 rows/s | **12.77×** |
| 4 clients 混合阶段有效行速率 | 341.8 rows/s | 5,232.8 rows/s | **15.31×** |
| 单 client source 写入 | 4,000 TPS | 10,371 TPS | **2.59×** |
| 4 clients source 写入 | 13,497 TPS | 23,549 TPS | **1.74×** |
| 5,000 行事务 source commit | 76.07 ms | 15.35 ms | **-79.8%** |
| 简单 backlog executor drain | 理论约 40 commits/s（旧版未复跑专项 gate） | 2,209 commits/s | 独立门禁 |

正式全矩阵 run `20260726T011500Z` 为 **passed**：27 个场景、3 次重复、
81 个场景实例、16/16 类逻辑算子、3,801 项正确性检查，差异、失败事务和
PostgreSQL 错误均为 0。

这次优化解决的是 commit 调度和入口唤醒放大，不是把所有算子改成了向量化
执行。TopN、Window 等算子的核心 SQL 算法没有重写，因此它们只取得小幅改善，
并继续决定复杂 DAG 的性能上限。

## 2. 被测架构

重构后的完整数据链路为：

```text
source DML commit
→ PostgreSQL publication / WAL
→ Router 按 WAL commit 分组
→ 每个相关 DAG 写入 durable dag_inbox
→ DAG worker 按最小 commit_lsn 取出完整 commit
→ Rust 构造跨 source、保持 sequence 顺序的 DeltaBatch
→ 一次 SPI 调用 _apply_dag_delta_batch
→ SQL 按 ordinality 顺序应用该 commit 内的 deltas
→ operator state + protected sink
→ progress 更新一次并与 inbox ack 原子提交
```

关键不变量：

1. 一个 source commit 对一个 executor 数据库事务，不把两个 source commit
   合并成一个事务。
2. 同一 commit 内可以包含多个 source；按全局 `sequence` 相对顺序应用。
   某个 DAG 看不到无关 source，因此它观察到的 sequence 允许有间隙。
3. UPDATE 仍以旧行 `diff=-1`、新行 `diff=+1` 的顺序进入批次。
4. operator state、sink、`view_progress` 和 inbox 删除在同一事务提交。
5. 失败时整个 executor 事务回滚，inbox 保留，随后可以重试。

## 3. 实现变化

### 3.1 Busy-drain 调度

- Router 在有工作时使用零等待连续 drain，但每轮受 16 个 batch 或 50 ms
  预算限制；空闲时仍等待 100 ms。
- DAG worker 在有 backlog 时连续处理，受 64 个 commit 或 50 ms 预算限制；
  空闲时等待 25 ms。
- 每个 commit 单独进入 `BackgroundWorker::transaction`，所以 busy-drain
  没有破坏 commit 级原子性。
- DAG plan 在 worker 启动时加载一次，而不是每个 commit 重复解析。

预算是公平性边界：它允许繁忙 worker 高吞吐处理，同时定期返回主循环执行
heartbeat、active 状态检查和退出响应。

### 3.2 Commit batch

- Rust 的 `DeltaRow` 带 source 标识，一个 `DeltaBatch` 可以保存同一 commit
  中交错出现的多 source 事件。
- worker 一次读取该 DAG 对应 commit 的全部 inbox rows，只进行一次
  `_apply_dag_delta_batch` SPI 调用。
- SQL batch wrapper 只获取一次 DAG advisory lock，按 JSON ordinality 顺序
  调用内部 state apply，所有 delta 成功后只写一次 progress。
- 旧的单 delta 入口保留为兼容层。

当前 batch 是“commit 级协议批处理”，不是“集合化算子执行”：SQL 仍在
PL/pgSQL 循环内逐 delta 调用算子逻辑。这是下一轮优化最重要的边界。

### 3.3 Statement-level wakeup

- 五条注册路径的 `_request_worker` trigger 从 `FOR EACH ROW` 改为
  `FOR EACH STATEMENT`。
- `_request_worker` 使用受限 `SECURITY DEFINER` 和固定
  `search_path=pg_catalog, shiba_internal`。
- 普通 writer 只需要业务 source 表的 DML 权限，不需要 `shiba` /
  `shiba_internal` schema 权限，也不能直接调用 worker 管理函数。
- 数据捕获仍只走 publication/WAL；trigger 只负责低成本唤醒，不复制数据。

5,000 行单语句的 source commit 从 76.07 ms 降到 15.35 ms，与 worker 请求
从每行一次变成每条 statement 一次一致，主要归因于该变化；本轮没有为这一项
单独做只切换 trigger 粒度的隔离 A/B，因此不将其表述为唯一因果。

## 4. 可复现性

### 4.1 对比 run

| 项目 | 旧基线 | 重构后 |
|---|---|---|
| Run | `20260725T160025Z` | `20260726T011500Z` |
| Git base | `b64a90c045d5c4eca70ef7f308604bafb1d89ee6` | `e4d3faa84c246c1312726cecb1c62026443d3dfd` + captured patch |
| 状态 | passed | passed |
| 场景实例 | 81 | 81 |
| 重复 | 3 | 3 |
| Seed | 20260725 | 20260725 |
| 初始主 source 行数 | 20,000 | 20,000 |
| 延迟 probes / 场景 / run | 40 | 40 |

新 run 的 `environment.json`、`working-tree.patch`、`untracked-files.tar.gz`
和 workload 精确副本均在结果目录中。即使后续代码继续变化，也可以恢复本次
被测工作树和负载。`checksums.sha256` 只覆盖四个 workload 文件；这四项已
重新校验通过，不能把该校验外推为 patch 和 tar 的内容校验。

### 4.2 测试机器

| 项目 | 值 |
|---|---|
| 机器 | Mac17,3，arm64 |
| CPU | 10 logical CPUs |
| 内存 | 24 GiB |
| OS | macOS 26.5.1 |
| PostgreSQL / pgbench | 17.10 Homebrew |
| Rust / Cargo | 1.96.0 |
| pgrx | 0.19.1 |
| 构建 | `cargo pgrx install --release` |

PostgreSQL 两次均使用 `shared_buffers=1GB`、`work_mem=64MB`、`jit=off`、
`fsync=on`、`synchronous_commit=on`、`track_io_timing=on`、
`track_wal_io_timing=on` 和 `track_commit_timestamp=on`。

### 4.3 重放命令

```bash
./scripts/test-all.sh
./scripts/test-executor-architecture.sh
./scripts/performance-matrix.py --repetitions 3
```

正式矩阵会创建隔离的临时 PostgreSQL cluster；每个 scenario 使用新的
database、extension、publication 和 logical slot。详细场景定义、随机顺序、
每轮原始样本、资源样本及数据库日志均随 run 保存。

## 5. 正确性和全链路门禁

`./scripts/test-all.sh` 已整体通过，包括：

- `cargo fmt --check`；
- `cargo clippy --all-targets -- -D warnings`；
- 57 个 Rust / pgrx 测试；
- 普通 writer 权限 E2E；
- 单 source 120 轮确定性 differential；
- Join 66 次 mutation / 408 次 comparison；
- concurrency、transaction 和 persistent-slot recovery；
- executor architecture 专项门禁。

专项门禁另外验证：

- 一个跨 source 事务中 left/right 事件和无关 published source 交错；
- 目标 Join DAG 观察到合法 sequence 间隙 `1,3,4,7,8`；
- 同一 LSN、UPDATE `-1/+1` 顺序和最终 Join 结果正确；
- 160 个 backlog commits 被 160 个不同 executor txid 应用；
- progress、sink 和 inbox ack 的提交快照原子一致；
- 双向 `EXCEPT ALL` 差异为 0；
- 独立 reviewer 实测简单 backlog drain 为 **2,209.09 commits/s**。

正式矩阵结果：

| 验收项 | 结果 |
|---|---:|
| OperatorKind | 16 / 16 |
| 场景 | 27 |
| 场景实例 | 81 |
| 正确性检查 | 3,801 |
| 正确性失败 | 0 |
| pgbench 失败事务 | 0 |
| 最终 inbox 非空 | 0 |
| PostgreSQL ERROR/FATAL/PANIC | 0 |

## 6. 性能对比

### 6.1 小事务和入口

小事务场景每个 commit 写 10 行。旧实现的 33–34 commits/s 与固定 25 ms
等待高度吻合；新实现不再有这个平台上限。

| 指标 | 旧中位数 | 新中位数 | 变化 |
|---|---:|---:|---:|
| 1 client 混合阶段有效 commits/s | 33.86 | 432.37 | +1,177% |
| 4 clients 混合阶段有效 commits/s | 34.18 | 523.28 | +1,431% |
| 1 client source TPS | 4,000 | 10,371 | +159% |
| 4 clients source TPS | 13,497 | 23,549 | +74% |
| 1 client Shiba / plain-PG source TPS | 0.237 | 0.618 | +0.381 |
| 4 clients Shiba / plain-PG source TPS | 0.328 | 0.649 | +0.322 |

这里必须说明正式 harness 的计时边界：phase timer 在 plain-PG control
pgbench 之前启动，随后串行执行 control 和 Shiba workload，等 Shiba 追平后
结束；分子只使用 Shiba transaction count。三轮会交替两者的执行先后顺序。
因此 432–523 commits/s 是 **control-inclusive 混合阶段有效率**，不是纯
Shiba 端到端容量。旧、新 run 使用完全相同的 workload，所以 12.77–15.31×
适合做同口径回归比较，但不能作为生产吞吐承诺。

2,209 commits/s 的 architecture gate 则预先构造简单 Aggregate backlog，
只测 executor drain。它独立证明旧的约 40 commits/s 调度平台上限已移除。
本报告没有从现有混合阶段指标推导纯 Shiba 全链路吞吐；要得到该指标，需要
修正 harness 的计时边界并建立新的前后基线。

### 6.2 大事务

| 指标 | 旧中位数 | 新中位数 | 变化 |
|---|---:|---:|---:|
| source commit | 76.07 ms | 15.35 ms | -79.8% |
| commit → ack | 894.74 ms | 854.48 ms | -4.5% |
| end-to-end wall | 969.60 ms | 869.14 ms | -10.4% |
| apply rows/s | 5,588 | 5,852 | +4.7% |

大事务原本已经只支付一次 worker 调度成本，因此 executor busy-drain 对它的
收益有限；显著下降的是 statement-level wakeup 带来的 source commit 成本。
剩余约 5.9k rows/s 反映逐 delta JSON/SPI/SQL state apply 的上限。

### 6.3 代表性 commit-to-apply 延迟

每个样本是一个 20-row source 事务；每场景合并 3 轮共 120 个原始样本。
负数变化代表更快。

| 场景 | 旧 p50 ms | 新 p50 ms | p50 变化 | 旧 p95 ms | 新 p95 ms | p95 变化 |
|---|---:|---:|---:|---:|---:|---:|
| Aggregate low cardinality | 74.9 | 66.1 | -11.7% | 124.2 | 127.9 | +3.0% |
| Aggregate high cardinality | 79.4 | 76.8 | -3.3% | 127.2 | 127.6 | +0.3% |
| Filter + Project | 82.9 | 71.4 | -13.9% | 131.2 | 118.7 | -9.5% |
| Inner Join 1:1 | 82.3 | 59.4 | -27.8% | 124.8 | 119.4 | -4.3% |
| Left Join | 93.2 | 62.3 | -33.2% | 134.6 | 123.5 | -8.3% |
| NOT IN null-aware | 75.9 | 82.5 | +8.7% | 121.5 | 126.9 | +4.4% |
| TopN LIMIT | 759.7 | 738.4 | -2.8% | 840.0 | 820.4 | -2.3% |
| TopN OFFSET | 749.0 | 721.4 | -3.7% | 839.1 | 808.2 | -3.7% |
| Window all functions | 231.5 | 197.9 | -14.5% | 298.0 | 276.1 | -7.4% |
| Window skewed partition | 267.1 | 262.8 | -1.6% | 406.3 | 495.8 | +22.0% |

小样本尾延迟仍会受 Router 轮询、系统调度和算子数据分布影响。NOT IN 和
skewed Window 的回退没有伴随正确性错误，但应作为后续重复和 profile 对象，
不能用总体吞吐改善掩盖。

### 6.4 复合链路

| 场景 | 旧 end-to-end | 新 end-to-end | 变化 |
|---|---:|---:|---:|
| 单 source → 4 DAG fanout | 7,910.7 ms | 7,625.6 ms | -3.6% |
| 2 source → 单源 DAG + Join DAG | 157.4 ms | 147.8 ms | -6.1% |

fanout 场景包含 TopN DAG，整体仍被 TopN 的逐 delta重建成本主导，所以没有
随简单 Aggregate backlog 一起获得数量级提升。

## 7. 为什么性能仍不是“特别高”

架构瓶颈已经从固定 commit 调度上限下移到算子执行路径：

1. `_apply_dag_delta_batch` 仍把 JSONB 数组在 PL/pgSQL 中按行展开和循环，
   没有将同类 delta 聚合成集合 SQL。
2. 每个 delta 仍要经过 JSON decode、动态 SQL/SPI、state lookup/upsert 和
   sink 维护。
3. TopN 会在 delta 后重建 bounded sink，约 0.72–0.74 s 的 p50 基本未变。
4. Window 会重算受影响 partition；80% hotspot partition 的尾延迟仍高。
5. Join 虽然是增量匹配，但复杂 multiplicity、outer unmatched row 和
   null-aware 全局状态会产生额外 state/sink 写放大。
6. Router 空闲时仍使用 100 ms 等待，因此无 backlog 的尾延迟没有被完全消除。

所以本轮结果应解读为：**commit 吞吐的架构天花板被抬高约一个数量级以上，
但算子内核还不是集合化/编译化执行器。**

## 8. 下一轮建议

优先级建议：

1. 为 batch 建立 typed staging relation 或数组参数，按 operator/source 聚合，
   用集合 SQL 替代 PL/pgSQL 逐 delta 循环。
2. 先优化 TopN：维护候选区和边界，不在每个 delta 后重建完整 bounded sink。
3. Window 按受影响 partition 合并同一 commit 内的多次变化，只重算一次。
4. Join 对同一 key 的 commit deltas 先做净化简，减少 multiplicity 和 sink
   往返写；保持原始顺序语义的测试作为门禁。
5. 为 Router 增加真实 latch wakeup/自适应退避，并单独观测 idle p95。
6. 增加两个确定性故障注入点：executor state apply 后、commit 前；Router
   route commit 后、slot advance 前。现有测试覆盖恢复行为和事务原子性，
   但尚未精确停在这两个指令窗口。

后续任何优化必须复跑相同正式矩阵，并至少同时比较：同口径混合阶段有效率、
source TPS ratio、commit-to-apply p50/p95/p99、large transaction rows/s、
operator correctness、WAL/I/O 和 state bytes。下一版 harness 还应增加计时
边界正确的纯 Shiba 端到端 commits/s。
