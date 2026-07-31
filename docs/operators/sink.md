# Sink 算子实现

## 1. 语义与范围

Sink 消费上游 EffectStream，把 signed weighted rows 应用到用户可见的结果表。正 weight 插入多重副本，负 weight 删除匹配的既有副本；它不再产生下游 EffectStream。Sink 是用户可见 exactly-once 的最终边界。

## 2. Plan contract 与状态

Sink 有一个 Operator input、没有 output stream。continuation 保存 input stream、chunk、row ordinal 和尚未应用的 remaining_weight。结果表 schema/binding 在每次 live execution 前校验，防止 plan、payload 和实际表列错配。

## 3. 生命周期

    Data chunk -> read effect heads -> plan signed weight page -> mutate result
              -> next row/remaining weight or consume chunk
    Frontier chunk -> advance cursor/frontier -> done

一个 effect row 的大 weight 可以拆成多个 page；remaining weight 必须是同号的 durable suffix，不能切换符号或超过原 weight。

## 4. Primitive 与复杂度

effect_heads 按 stream_id、chunk_seq、row_ordinal keyset 读取 bounded rows。plan_weight_page 同时受 input/output row/byte budget 限制。mutate_result_page 把 page 转为 VALUES，正 weight 使用 generate_series 插入副本；负 weight 通过完整结果 row 的 IS NOT DISTINCT FROM 条件选择 ctid，再按 ctid 删除。

设 effect page 行数为 P、实际变更副本数为 C、结果表行数为 R：正向成本约为 O(P + C)；负向删除在没有可用结果索引时最坏为 O(P*R)，并且每个负 action 有 OFFSET/ctid victim selection。结果表是否有用户索引不能作为 Shiba 的通用性能保证，因此删除路径是 Sink 的主要风险。

后续可考虑维护 Shiba-owned 的结果 identity/multiset side table，或在注册时建立可证明覆盖 delete predicate 的索引；设计必须保留 NULL-safe equality 和 duplicate deletion 顺序。

## 5. 事务与恢复

结果 DML、input cursor、remaining weight、continuation 和 checkpoint 在同一个 transaction 中提交。commit 前崩溃回滚 DML 并重放同一 suffix；commit 后 cursor 已经越过 suffix，不会重复应用。Sink 不做 output publication，但仍使用 shared transition/checkpoint boundary。

## 6. 测试与性能证据

scripts/test-stateless-kernels.sh 和 src/execution/sink/tests.rs 覆盖正/负 weight、最小 bigint、宽 row、crash-before/after-commit、backpressure 和 schema binding。性能必须分别测全是 insert、全是 delete、duplicate-heavy delete 以及结果表有/无匹配索引的场景。

## 7. 已知限制

- 负向 DML 依赖结果表按值寻找 victim，不能假设存在主键或唯一索引。
- generate_series 的副本数受 budget 限制，但大量 multiplicity 仍需要多次提交。
- 结果表上的用户索引会影响 DML 成本；benchmark 必须记录其 schema/index。
