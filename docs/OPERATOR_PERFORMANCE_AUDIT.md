# 算子性能审计

## 结论

当前实现的“有界执行”和“可恢复”做得扎实，但整体还不能称为所有工作负载都高效。准确的判断是：

- Linear（Scan/Filter/Project）是目前最接近高效热路径的实现，主要成本是每个 step 的 PostgreSQL/SPI/锁/checkpoint 固定开销和 typed row bytes 计算。
- Distinct、Aggregate、Window、TopN 都通过 continuation 把单步工作限制住，但大 key、hot group、large partition 或频繁 dirty update 的总复杂度仍会随状态规模增长。
- Join 的 generic/theta fallback 仍按 row_id 分页扫描，但直接 AND equi-join 已增加 typed key 列和复合 B-tree access path；大选择性 miss 已有同机 A/B 证据。
- Sink 的负向动作按完整结果行找 ctid；结果表没有可保证存在的内部 identity index 时，delete-heavy workload 可能重复扫描结果表。

所以现在适合继续做正确性和协议演进，但在声明“生产级高效”前，应先解决 Join 访问路径、Sink 删除路径，并补齐 hot-key/partition/large-offset baseline。

## 已完成的实测

命令：

    ./scripts/performance-benchmark.sh --profile smoke       --json-out /tmp/shiba-perf-smoke.json

环境：PostgreSQL 17.10，Shiba 0.1.0，batch_rows=16384，batch_bytes=16MB，release build，Apple 本机。该结果证明测量链路和正确性，不是回归 baseline。

| 场景 | 输入/结果 | 收敛时间 | 吞吐 | 其他观察 |
| --- | ---: | ---: | ---: | --- |
| Large ingress | 10,000 / 10,000 | 0.416 s | 24,017 rows/s | 6 output chunks，10 checkpoint advances，峰值 buffer 20,000 rows |
| Join fanout | 1 / 64 | 0.083 s | 776 rows/s | 16 output chunks，峰值 buffer 258 rows |
| Complex DAG | 200 / 16 | 0.218 s | 73 rows/s | 30 output chunks，state 约 1.28 MB，57 checkpoint advances |

复杂 DAG 的低吞吐不能直接归因给单一算子；它包含两次 Join、Aggregate、Window、TopN、Project 和 Sink，且测试规模很小，固定 step 开销占比很高。

## 风险排序与建议

### P0：Join 等值 key access path（第一阶段已落地）

[join/provision.rs](../src/execution/join/provision.rs) 现在在 keyed arrangement 中内嵌 typed key 列，并创建 `(key_0,...,row_id)` 复合 index。[join/runtime.rs](../src/execution/join/runtime.rs) 的 keyed path 先按 key 查找，再用 residual Join condition 和 row_id cursor 完成有界处理；generic/theta path 保持原有 `O(N)` fallback。

当前证据：PostgreSQL 17.10、release、对侧 1,000,000 行、miss key 的同机 A/B 中，keyed 0.571 s，generic 0.754 s，正确性和 keyed `EXPLAIN` index-scan 门槛均通过；约 24.2% 收敛时间收益。仍需持续关注低选择性 fanout 的 index maintenance 成本，以及 OR/cast/function/NullAwareAnti fallback。

### P0：Sink delete 可能重复扫描结果表

[sink/runtime.rs](../src/execution/sink/runtime.rs) 的负向路径用完整 row 的 NULL-safe predicate、ORDER BY ctid、OFFSET 和 LIMIT 选择 victims。结果表没有 Shiba 统一创建的可覆盖索引时，P 个 delete action 对 R 行结果表可能接近 O(P*R)。

建议评估 side identity table 或注册时可证明的匹配索引。不能只依赖用户恰好创建的主键；需要覆盖 NULL、duplicate 和 update old/new row 的恢复测试。

### P1：状态型算子的总工作不是增量常数

Aggregate 以 dirty group 为单位重建 aggregate transition，约为 O(A*G)；Window 对 dirty partition 经过 enumeration/peers/frames/fold/diff 多阶段，约为 O(P*W+K)；TopN 每次 generation selection 仍需遍历 active input，约为 O(N+K)。continuation 只把这些工作切成小事务，并没有消除总工作。

建议把这些工作负载加入独立 benchmark：hot group、large partition、large OFFSET/WITH TIES、频繁更新排序边界，并记录 pages、最大 step 时间、state bytes、output rows/s 和 post-commit convergence。

### P1：固定 step/transaction 成本

每个 StepContext 会锁 checkpoint、input consumers 和 output stream；提交时还会做 output publication 和 checkpoint CAS。小 page 或 metadata-only phase 下，固定成本会主导延迟。增大 batch 可以提高吞吐，但会增加锁持有、背压和恢复重放粒度，不能只看 rows/s 调参。

建议记录每个 stage 的 checkpoint_advances、output chunks、平均 rows/step、backpressure time，并用同机 baseline 比较 batch_rows/bytes 的 trade-off。

## 做得好的部分

- row/byte 双预算和“单个不可拆分 row 可超 byte target”规则明确；
- continuation 使用 typed cursor，主要路径采用 keyset，而不是依赖脆弱的 OFFSET；
- output、state、cursor、continuation 和 checkpoint 在同一事务提交；
- Aggregate/Window/TopN 已为大 work 保存 durable cursor，不会把大 group 一次放进一个 PostgreSQL statement；
- protocol tests 和真实 PostgreSQL tests 已覆盖 crash、frontier、backpressure。

## 后续性能证据门槛

任何算子优化 PR 都必须同时更新对应实现页：说明访问路径变化、复杂度变化、索引/ABI 变化、测试和同机 benchmark。没有 baseline 的数字只能写“诊断观察”，不能写“提升了 X%”。
