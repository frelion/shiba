# TopN 算子实现

## 1. 语义与范围

TopN 根据 planner 解析的排序 key、OFFSET、LIMIT 和 WITH TIES 生成有序结果。输入是带 multiplicity 的 typed row；结果通过 candidate generation 与 visible state 的差分输出。排序方向、NULL 顺序和 tie boundary 都是 continuation 的一部分语义，不能只用 row id 近似。

## 2. 持久状态与索引

| Relation | 用途 | 关键索引 |
| --- | --- | --- |
| input | canonical row、multiplicity、排序表达式 | order key + entry_id index |
| candidate | 当前 generation 的选中结果 | generation_id、output_key UNIQUE；generation/candidate id |
| visible | 已发表的 TopN 结果 | visible_id PK、output key unique |
| control | dirty、causal LSN、generation control | singleton |
| continuation | Admit、Select、Diff remove/add、Cleanup、Frontier | sort/tie/diff cursor |

## 3. 生命周期

    Admit input -> Select ranked candidate generation
     -> Diff remove -> Diff add -> Cleanup -> resume Admit or Frontier

每次 dirty update 使用新的 generation，selection cursor 按排序 keyset 前进；WITH TIES 额外保存 tie boundary。Diff 将 candidate 与 visible 的 multiplicity 差异拆成 bounded remove/add legs，cleanup 后才清除旧 generation。

## 4. Primitive 与复杂度

run_topn_admission 更新 input/order state 并记录 dirty causal LSN。run_topn_selection 按 order index 取 bounded page，计算 OFFSET/LIMIT/TIES 的可用 multiplicity，将结果聚合到 candidate。run_topn_diff 按 visible/candidate identity cursor 生成 payload 和 visible mutation；run_topn_cleanup 删除已消费 work。

设 active input rows 为 N、候选/可见输出为 K、selection page 为 P：一次 dirty update 的 selection 最坏仍需扫描/排序遍历 N，总体约为 O(N + K)（排序表达式和 ties 会改变常数）；diff/cleanup 约为 O(K)。order index 和 keyset 避免了 selection/diff continuation 使用 OFFSET，但 generation 重建意味着每个 dirty update 不能只维护受影响的前 K 行。

## 5. 事务与恢复

input/candidate/visible/control state、payload、output append、diff cursor、cleanup 和 checkpoint 一起提交。generation 使用 checkpoint revision 作为 seed，但不是第二个 completion authority。diff 在零差分时也必须推进 cursor；crash 后从同一 leg/identity cursor 重放。

## 6. 测试与性能证据

scripts/test-window-topn-kernels.sh 和 src/execution/topn/tests.rs 覆盖 NULL 排序、方向、large OFFSET/LIMIT、WITH TIES、零 LIMIT、多页 diff、cleanup、crash、backpressure 和链式 DAG。性能至少要测 N 很大但 K 很小、频繁更新同一排序边界、以及 ties 很宽的情况。

## 7. 已知限制

- 每次 dirty update 都会创建 generation 并重做 selection，TopN 不是增量排名树。
- large OFFSET 会消耗大量 selection input budget，虽然能恢复但延迟随 offset 增长。
- WITH TIES 可能使输出远大于 LIMIT，必须依赖 byte/row budget 和 continuation。
