# Join 算子实现

## 1. 语义与范围

Join 是双输入、单输出的增量 join，支持 inner、left/right/full outer、semi、anti 和 null-aware anti 语义。每个输入变化都被转成一个 signed event；结果变化可能是 pair row、保留的单边 row，或其负向撤回。重复值通过 multiplicity 处理，NULL 的 TRUE/FALSE/UNKNOWN 分支通过 planner 生成的条件保留。

## 2. Plan contract

Join 有两个 Operator input ports，port 0/1 分别对应左右输入，输出是一个 EffectStream。planner 在 [join/planner.rs](../../src/execution/join/planner.rs) 中固定输入绑定、JoinMode、条件、输出 slot 和 continuation ABI；runtime 入口是 [join/runtime.rs](../../src/execution/join/runtime.rs)。

`JoinSpec` 还保存 `equi_keys`。planner 只从完整条件中提取满足以下条件的键：直接的 `Input = Input`、两侧 binding 相反、类型/typmod/collation 完全一致，并且位于顶层 `AND` 中。完整 condition 始终保留为 residual predicate；`OR`、cast、函数表达式和 `NullAwareAnti` 不会进入该快速路径。

## 3. 持久状态与索引

每个 Join 有两张 arrangement relation，左右各一张。含有 `equi_keys` 时，键列直接内嵌在对应主 arrangement 中，避免额外的同步表和额外写入：

| 字段 | 用途 |
| --- | --- |
| row_id | 候选分页和 continuation keyset |
| row_key | 完整 typed row 的 canonical identity，定位当前输入自己的 state |
| row_value | 后续条件判断和输出投影 |
| key_0, key_1, ... | planner 提取的 typed equality key；允许 NULL，NULL 仍由 SQL equality 的 UNKNOWN 语义处理 |
| multiplicity | 该逻辑 row 的正占用 |
| match_count / unknown_count | outer/semi/anti eligibility and complete SQL three-valued comparison accounting |

所有 arrangement 都有 `row_id` primary key 和 `row_key UNIQUE`。含键 arrangement 另外创建 `(key_0, key_1, ..., row_id)` 复合 B-tree index；`row_id` 作为末列保证同 key 下仍可使用 continuation 的 keyset cursor。state ABI 会校验键列的位置、类型、typmod、collation 和 index。

## 4. 生命周期与 continuation

    Process:
      load event -> preflight own state -> probe candidate pages
      -> append actions / update arrangement -> next event
    Frontier:
      两个 input frontier 都满足且没有 pending event/action -> emit frontier

continuation 保存 owner side、两个 input positions、当前 event facts、candidate row_id cursor 和 pending action。一个 fanout event 可跨多个 transaction，候选 cursor 只能单调增加；动作计划按 output row/byte budget 截断。

## 5. Primitive 与复杂度

load_event 和 load_own_expectation 通过 payload 主键/row_key 定位当前行。对含 `equi_keys` 的 Join，严格相等候选与可能产生 UNKNOWN 的 NULL 候选分成两个互斥的 SQL 分支：严格分支按 typed key 访问复合 index，NULL 分支只访问可能含 NULL key 的候选；两个分支合并后再执行完整 Join condition。这样 NULL-aware 语义不会把普通非 NULL miss 退化成一个带 `OR` 的顺序扫描。`candidate.row_id > cursor ORDER BY row_id` 仍负责恢复分页。没有安全 equality key 的 Join 保留原来的 row-id page scan fallback。append_actions 用 action ordinal 一次性写入 typed payload，并更新候选和当前侧 state。

设当前事件数为 E、对侧 arrangement 行数为 N、复合 index 命中的候选数为 H、每页候选数为 P、生成结果为 F：

- typed equality path 的严格候选访问约为 `O(log N + H)`，NULL 候选分支额外为可能含 NULL key 的 `N_null`；跨页时为 `O(log N + H + N_null + F)`；无 NULL 的选择性 miss 近似 `O(log N)`；
- generic theta path 最坏仍为 `O(N)`，E 个事件最坏为 `O(E*N + F)`；
- key path 仍会计算 residual condition，因此不能把索引命中误认为完整 SQL Join 语义已经由 key 替代。

索引维护的成本是每个 keyed state row 多一个 typed key index entry；当匹配选择性低、对侧状态很大时，避免全表候选扫描的收益应超过该存储和写入成本。

## 6. 事务与恢复

一个 action page 的 arrangement 更新、typed output payload、output append、输入 cursor 和 continuation 一起提交。commit 前崩溃会重放同一个 event/candidate page；commit 后 cursor 已推进。StepContext 的 output target 确保一个 page 不会超过 output chunk row/byte target；下游背压会在 Join primitive 前阻止执行。

## 7. 测试与性能证据

scripts/test-fanout-recovery.sh 覆盖所有 JoinMode、NULL、残余条件、复合 key、重复、删除、key 更新、fanout、crash、backpressure、链式 Join 和 frontier，并检查 keyed Join 的复合 index ABI。benchmark 额外提供 keyed/generic A/B：同一台 PostgreSQL 17.10/release 环境、对侧 1,000,000 行、miss key，当前测得 keyed 0.571 秒、generic 0.754 秒，约 24.2% 更低的收敛时间；两者均通过正确性和 `EXPLAIN` index-scan 门槛。固定 smoke profile 使用 100,000 行，适合快速诊断；full profile 使用 1,000,000 行。

## 8. 已知限制

- 只有 planner 能安全识别的直接 AND equality 才使用 key index；OR、cast、函数、theta predicate 和 NullAwareAnti 仍可能退化为重复全扫描。带 NULL 的 equality 会额外访问 NULL 候选分支；如果 NULL key 占比很高，收益会下降。
- key index 增加每行存储和 DML 成本；低选择性、高 fanout workload 的收益可能不明显，必须和 generic fallback 同机比较。
- 输出 fanout 产生的 chunk/transaction 数与 F 相关，虽然不是每个 output row 都单独提交。
- 动态 composite row 的 condition evaluation 和 row-byte 计算占用显著 CPU。
