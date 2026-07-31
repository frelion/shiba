# Distinct 算子实现

## 1. 语义与范围

Distinct 对指定 key 做增量去重，维护每个 key 的总 multiplicity 和一个确定的输出 representative。输入/输出均为 weighted row；只有 occupancy 从 0 变 1 或从 1 变 0 时才产生外部差分。SQL-equal 但物理 binary representation 不同的 row 也可能触发 representative replacement。

## 2. 持久状态与索引

| Relation | 用途 | 关键约束 |
| --- | --- | --- |
| distinct_groups | key、当前代表 row、总 multiplicity | key 的 UNIQUE NULLS NOT DISTINCT |
| distinct_bag | 一个 key 下的物理代表候选和 multiplicity | group_state_id、output_key UNIQUE |
| distinct_touched | 当前 Apply prefix 改变的 group 集合 | group_state_id PRIMARY KEY |
| distinct_effect_queue | 待向下游发送的 -1/+1 effect、bytes、causal LSN | queue_id PRIMARY KEY |
| continuation | Apply/Drain/Frontier 与输入 row cursor | singleton + input chunk FK |

key index 是 exact B-tree capability 的一部分；不能随意替换成只按 bytea hash 的近似 key。

## 3. 生命周期

    Apply prefix -> reconcile touched representatives -> Drain effect queue -> Frontier

Apply 只吸收一个 input prefix，更新 group/bag/touched；如果产生代表变化，继续进入 Drain。Drain 每次从 queue 取有序 bounded page，写 output payload，再删除已经发出的 queue 行。Frontier 只有 queue 为空时才允许转发。

## 4. Primitive 与复杂度

run_prefix 按 input chunk/ordinal 取 bounded page，按 key 聚合 signed weight，通过 state key index 找 group，并更新 bag/touched。reconcile_representatives 只处理 touched groups，选择 canonical representative，必要时入队负向旧代表和正向新代表。drain_queue 通过 queue_id keyset 取页，输出后原子删除相同 queue 行。

设 input page 为 P、touched key 数为 T、每个 key 的物理候选数为 B、待发效果为 Q：Apply 约为 O(P log T) 加索引更新，reconcile 约为 O(sum B_t)，Drain 总成本为 O(Q)；最坏仍受单个高重复 key 的 bag 扫描影响。队列按 identity page，不使用 OFFSET，恢复 cursor 不会随前缀删除而漂移。

## 5. 事务与恢复

Apply 的 group/bag/touched/queue 修改和 continuation 一起提交；Drain 的 payload 写入、queue 删除、output publication 和 continuation 一起提交。代表 replacement 在一行 output budget 下拆成两个 Drain leg，避免把 -old/+new 拆坏。crash 前后分别重放同一个 Apply 或 queue page，不会丢失或重复 canonical effect。

## 6. 测试与性能证据

对应 scripts/test-aggregate-distinct-kernels.sh 和 src/execution/distinct/tests.rs，重点覆盖 multiplicity 边界、replacement、宽 row、crash、backpressure、frontier 和大 key。性能上应分别测高基数 key、单 hot key、大量 physical representatives 和长 effect queue。

## 7. 已知限制

- representative reconciliation 不是纯 O(1) lookup；高重复 key 会反复处理 bag。
- queue/drain 仍需要独立 checkpoint，短 page 下固定事务开销会变大。
- canonical row key 的 text/binary round-trip 保障语义，但会增加 CPU。
