# Shiba 算子实现总览

这组文档描述当前唯一执行架构中的物理算子。公共生命周期、checkpoint、continuation 和 output publication 不在每页重复定义，统一见 [OPERATOR_PROTOCOL.md](../OPERATOR_PROTOCOL.md)。

## 算子目录

| 逻辑/物理算子 | 实现页 | 代码入口 | 当前性能判断 |
| --- | --- | --- | --- |
| Scan / Filter / Project（Linear） | [linear.md](linear.md) | execution/linear | 线性热路径；固定 step/SQL 开销仍明显 |
| Join | [join.md](join.md) | execution/join | 有界 fanout；generic own-state 的 TRUE/UNKNOWN 计数共享一次扫描 |
| Distinct | [distinct.md](distinct.md) | execution/distinct | key/state 和 representative 均按 page 批量 join |
| Aggregate | [aggregate.md](aggregate.md) | execution/aggregate | Emit 直接复用持久 group identity；dirty group 仍按 aggregate 重建 |
| Window | [window.md](window.md) | execution/window | Fold/Evaluate 复用已物化 payload；大 partition 仍多阶段重建 |
| TopN | [topn.md](topn.md) | execution/topn | selection 已复用 bounded terminal row；每次 dirty update 仍重做排名选择 |
| Sink | [sink.md](sink.md) | execution/sink | 负向 page 已批量 ctid ranking；无 identity index 时仍需扫描结果表 |

## 统一运行模型

每个 step 都在一个 PostgreSQL transaction 中完成一组有界工作：

    lock metadata -> load continuation/state -> bounded SQL primitive
    -> write typed payload/state -> record output -> advance cursor
    -> replace continuation -> checkpoint CAS -> commit

shiba.batch_rows 和 shiba.batch_bytes 同时约束输入/输出 step；单个不可拆分 row 可以超过 byte target。下游背压会在 primitive 执行前阻止 output operator。这些机制保证恢复边界和资源上限，但不自动保证整个 workload 的低复杂度。

## 性能阅读顺序

先看每页的“复杂度与访问路径”，再看 [OPERATOR_PERFORMANCE_AUDIT.md](../OPERATOR_PERFORMANCE_AUDIT.md)。各算子页中的 A/B 数字用于说明局部 SQL 访问路径变化；端到端 release smoke 仍需与同机 workload baseline 分开解读。

## 新算子文档规范

新增或改变算子算法时，代码与实现文档必须在同一个变更中落地。文档至少包含：语义与 NULL/重复/排序范围、plan contract、持久 relation 与索引、生命周期和 typed continuation、单步事务边界、每个 primitive 的复杂度与访问路径、crash/frontier/backpressure 恢复语义、Rust 与 PostgreSQL 测试、同机 baseline/A-B 性能证据，以及已知限制和后续计划。没有性能基线时只能写诊断观察，不能宣称收益。
