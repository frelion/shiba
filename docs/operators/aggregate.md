# Aggregate 算子实现

## 1. 语义与范围

Aggregate 支持 grouped/global aggregate、多个 aggregate expression、FILTER、DISTINCT 和 aggregate-local ORDER BY。它维护 PostgreSQL transition state，最终输出的是 group key 加 aggregate final value 的 typed row；输入变化不会重新扫描 source table，而是更新持久 bag 并把受影响 group 放入 dirty queue。

## 2. 持久状态与索引

| Relation | 用途 | 关键索引/游标 |
| --- | --- | --- |
| aggregate_groups | group identity、published/pending output | group key 的 UNIQUE NULLS NOT DISTINCT |
| aggregate_bag | group 内输入 row、multiplicity、ORDER BY/DISTINCT 值 | group_state_id、row_id；每个 aggregate 的 effective-order index |
| aggregate_work_aN | 每个 aggregate 的 transition state、rebuild cursor | group primary key；cursor 保存 order/distinct key + row_id |
| aggregate_dirty | dirty group 和 causal LSN | queue_id primary key、group unique |
| continuation | Apply、DrainRebuild、DrainEmit、Frontier | group queue id、aggregate ordinal、typed cursor |

## 3. 生命周期

    Apply input prefix
      -> DrainRebuild(group × aggregate)
      -> DrainEmit(unchanged / insert / delete / replacement legs)
      -> resume Apply or Frontier

Apply 更新 bag 和 dirty group。Rebuild 对每个 aggregate 按 durable cursor 重新折叠 group 的 bag；所有 aggregate rebuild 完成后，Emit 比较 pending/published typed output，必要时先发旧值 -1，再发新值 +1。global aggregate 在空输入的 frontier 路径中也必须创建并 materialize 空 group。

## 4. Primitive 与复杂度

step_apply 按 input row/byte budget 更新 dynamic typed transition state 和 dirty queue。aggregate_rebuild_page 使用 group/order index 取一个 keyset page，更新 transition state 和 cursor；aggregate_append_output 只处理一个 group 的一个可见差分。Grouped page rebuild 还会先把本页 dirty group 物化，再通过 `(group_state_id,row_id)` bag index 的 `DISTINCT ON` 批量选 representative，避免对每个 group 单独执行一次 `LATERAL ... LIMIT 1`。每页工作 bounded，但 group 重建不是增量 delta fold。

设 dirty group 大小为 G、aggregate 数为 A、一个 rebuild page 为 P、输出差分数为 K：单次 group 更新的重建总成本约为 O(A*G)（每个 aggregate 还可能有 ORDER BY/DISTINCT index 和 transition 函数 CPU），Emit 约为 O(K)。representative 批量选择对本页 group 是一次 join 加 bag index 顺序访问，约为 O(G_page + B_page)，而不是 G_page 次独立状态/representative lookup。因此 hot group 是主要性能风险；扩大 step 只改变 commit 次数，不改变 A*G 总工作。

## 5. 事务与恢复

transition state、bag/cursor、dirty queue、payload、output chunk 和 continuation 一起提交。rebuild continuation 的 aggregate ordinal 和 order/distinct cursor 都是恢复真相；不能用 checkpoint revision 代替它。replacement 的两条 leg 必须分别可提交，避免 crash 后重复插入新值。

## 6. 测试与性能证据

scripts/test-aggregate-distinct-kernels.sh 及 src/execution/aggregate/tests.rs 覆盖 catalog-driven aggregate ABI、GROUP BY、global、FILTER/DISTINCT/ORDER BY、delete rebuild、large group、crash 和 frontier。性能必须至少包含：小组多组、高频单 hot group、长 DISTINCT set、ordered aggregate。

在 PostgreSQL 17.10 的本机 SQL A/B fixture 中，4096 个 group 各有一条 bag row：旧的逐 group `LATERAL` representative lookup 读取 12,334 个 shared blocks、3.183 ms；批量 `DISTINCT ON` join 读取 76 个 blocks、3.150 ms。这个小 fixture 的 wall time 差异有限，但随机索引访问已从每组一次降为一次批量 bag 访问；更宽 payload 或更大 group page 应以 blocks 和端到端收敛时间共同复测。

## 7. 已知限制

- 当前 dirty group 通常需要按 aggregate 重建整个 group；尚未实现真正的 delta transition 或共享多 aggregate 扫描。
- transition/final function 是 PostgreSQL 动态类型调用，CPU/TOAST 成本不能只用 SQL 行数估计。
- 长 group 受 bounded continuation 保护，但端到端延迟仍可能随 G 线性增长。
