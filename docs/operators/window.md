# Window 算子实现

## 1. 语义与范围

Window 先按 partition/order 把输入变成可定位的逻辑行，再计算 peer、frame 和窗口函数输出。支持多 partition/order key、NULL ordering、peer、frame mode、多个窗口函数及有状态 aggregate fold；窗口结果以 typed weighted row 差分输出。

## 2. 持久状态与索引

| Relation | 用途 | 关键索引 |
| --- | --- | --- |
| partitions | partition key、dirty、row count | partition key unique、dirty partition index |
| input | canonical input row、multiplicity、partition | partition/entry order index |
| ordered | 展开 multiplicity 后的 ordinal | ordinal PK、entry/copy unique、peer_id+ordinal |
| peers | peer range | peer_id PK、ordinal/range lookup |
| frames/accumulator | frame work 与 aggregate fold | ordinal/cursor indexes |
| candidate/visible | 当前计算结果和已发表结果 | partition/output key unique、page index |
| continuation | Admit、Enumeration、Peers、Frames、Fold、Diff、Cleanup、Frontier | typed cursor/leg |

具体 relation 由 [window/provision.rs](../../src/execution/window/provision.rs) 按窗口函数 capability 创建；ordered/peer/frame 的逻辑在 [window/primitives.rs](../../src/execution/window/primitives.rs)。

## 3. 生命周期

    Admit input
     -> Enumerate ordered rows
     -> Peers
     -> Frames
     -> Fold aggregate/window functions
     -> Diff visible vs candidate
     -> Cleanup partition
     -> Frontier

每个阶段都可在 row/byte budget 下暂停。dirty partition 在没有新 input 的 step 中也可继续 Drain；只有 diff 和 cleanup 完成后才允许 frontier。

## 4. Primitive 与复杂度

Admission 更新 partition/input，并保存下一输入位置。Enumeration、Peers、Frames 使用 ordinal/keyset cursor 构造 ordered/peer/frame state。Fold 以有限 work-item 预算推进 aggregate state；Diff 按 visible/candidate identity page 生成 -old/+new 差分；Cleanup 删除已经完成的 partition work。

设 dirty partition 逻辑行数为 P、窗口函数/aggregate 数为 W、可见输出为 K：一次 dirty partition 的总工作通常是 O(P*W + K)，frame interval 查找还会增加按 order key 的索引查找与 PostgreSQL expression CPU。Aggregate Fold 的三个 interval CTE 各自有 row/byte 上界；interval 阶段已经取出的 typed `row_value` 会随 selected row 传入递归 fold，fold 不再按 `entry_id` 对 input relation 做第二次回查，只在 SQL 内展开复合值供 transition/filter 表达式使用。Continuation 让单页有界，但不消除同一 partition 的多阶段重建成本。

## 5. 事务与恢复

partition/input/ordered/peer/frame/fold/visible state、payload、output append、cleanup cursor 和 continuation 原子提交。Diff cursor 在零差分时也必须前进，否则会无限重复；frontier 必须等待两条 diff leg 和 cleanup 都完成。

## 6. 测试与性能证据

scripts/test-window-topn-kernels.sh 和 src/execution/window/tests.rs 覆盖排序、NULL/peer/frame、多窗口函数、大 partition、fold continuation、diff 重复、cleanup、crash、backpressure 和 Window->TopN 链。性能应比较小 partition 高频更新与单个超大 partition；至少记录 continuation pages、最大 step 时间、输出 rows/s 和 state bytes。Fold 优化还必须验证 aggregate/native window、空 frame、NULL payload、多个 interval、超大 partition 和 crash recovery 的输出与 PostgreSQL fresh recomputation 完全一致。

## 7. 已知限制

- 一次 partition 变化会触发 enumeration/peer/frame/fold/diff 多阶段工作。
- 复杂 frame 和多 aggregate 的总 CPU 随 partition size 与函数数增长。
- 当前集成门禁覆盖 large-partition/fold recovery，但仍应补充独立 Window large-partition baseline，隔离固定 step/transaction 成本与 interval/fold 本身的 CPU。
