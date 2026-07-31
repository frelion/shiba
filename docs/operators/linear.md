# Linear：Scan、Filter、Project 算子实现

## 1. 语义与范围

三个逻辑算子共享一个 bounded linear kernel：Scan 从 source EffectStream 读取 typed weighted rows；Filter 保留 predicate 为 TRUE 的行；Project 按 planner 绑定生成新的 composite row。Filter/Project 保留输入 weight，Scan bootstrap 的初始快照使用 +1，之后的 WAL 行沿用 ingress 产生的权重。

## 2. Plan contract

| 算子 | 输入 | 输出 | 关键约束 |
| --- | --- | --- | --- |
| Scan | 1 个 Source stream | EffectStream | source OID 和 row type 必须匹配 |
| Filter | 1 个 Operator stream | EffectStream | predicate 使用已验证的 typed SQL |
| Project | 1 个 Operator stream | EffectStream | output binding/type 与 composite schema 匹配 |

入口是 [linear/runtime.rs](../../src/execution/linear/runtime.rs) 的 SCAN_KERNEL、TRANSFORM_KERNEL 和 step；表达式编译在 [linear/storage.rs](../../src/execution/linear/storage.rs) 中完成。

## 3. 持久状态

Linear 没有业务 state。Scan 使用 bootstrap relation 保存 CTAS 快照剩余行；Scan/Transform 使用 singleton typed continuation 保存输入 stream、chunk、row ordinal 和 bootstrap/frontier phase。输入和输出 payload relation 的主键是 stream_id、chunk_seq、row_ordinal，因此 continuation 可以按 row ordinal keyset 读取。

## 4. 生命周期

    Scan: Bootstrap -> SnapshotFrontier -> Data -> SourceFrontier -> done
    Live linear: Data -> same chunk Data | next chunk | Frontier -> done

bootstrap 在 activation LSN 之前物化快照；snapshot frontier 之后的 live source chunk 才会被 Scan 发出。一个 input chunk 内如果没有处理完，continuation 只保存下一个 row ordinal；处理完后推进 consumer cursor。

## 5. Primitive 与复杂度

run_transform_primitive 在一个 SQL statement 中完成：按 chunk/ordinal 读取 input prefix，计算 input bytes，执行 predicate/project，按 output row/byte budget 选择前缀，再插入 typed output payload。一个 primitive 还返回 first/last ordinal、输入/输出 rows/bytes；shared context 负责发表 chunk。

设 P 为本页处理行数，I 为 input payload 中当前 chunk 的索引访问成本，则单页约为 O(P)，并带有 composite 构造、effect_row_bytes 和两个窗口累计的 CPU 成本。它不按结果表重新执行原查询。代价是每个 step 都要重新执行 CTE 和 metadata/checkpoint 锁，过滤全空页也会产生一次 durable cursor/frontier 工作。

bootstrap 用 bootstrap_seq >= cursor ORDER BY bootstrap_seq LIMIT 取页，写入 output 后删除同一页，单页也是 O(P)；它依赖 bootstrap sequence 的有序访问。

## 6. 事务与恢复

payload、output chunk metadata、input cursor、continuation 和 checkpoint CAS 在同一 step transaction 中提交。commit 前崩溃会从旧 ordinal 重放；commit 后 continuation/cursor 已前进，不会重复输出。Filter 全部丢弃时仍推进 input，但不生成 data chunk；frontier 只能由 frontier phase 发表。

## 7. 测试与性能证据

对应测试入口是 scripts/test-stateless-kernels.sh，覆盖 bootstrap 多页、空 filter、宽 TOAST row、source schema change、Sink crash 和 backpressure。性能主要由 step transaction 数、payload bytes、shiba.batch_rows/bytes 和 output chunk target 决定；不要用单次 SQL 查询耗时替代端到端数据流指标。

## 8. 已知限制

- 每个 page 都重复计算 row bytes 和窗口累计，宽 composite row 的 CPU 成本高。
- page 很小时，StepContext::begin 的 metadata 锁、output publication 和 checkpoint CAS 会成为固定开销。
- 这是线性路径，不适合通过增加复杂 SQL 来承担 Join/Aggregate 的状态语义。
