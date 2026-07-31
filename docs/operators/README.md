# Shiba 算子实现总览

这组文档描述当前唯一执行架构中的物理算子。公共生命周期、checkpoint、continuation 和 output publication 不在每页重复定义，统一见 [OPERATOR_PROTOCOL.md](../OPERATOR_PROTOCOL.md)。

## 算子目录

| 逻辑/物理算子 | 实现页 | 代码入口 | 当前性能判断 |
| --- | --- | --- | --- |
| Scan / Filter / Project（Linear） | [linear.md](linear.md) | execution/linear | 线性热路径；固定 step/SQL 开销仍明显 |
| Join | [join.md](join.md) | execution/join | 有界 fanout；当前候选探测最坏为全 arrangement 扫描 |
| Distinct | [distinct.md](distinct.md) | execution/distinct | key index 正确；Apply/Drain 有额外重排和队列成本 |
| Aggregate | [aggregate.md](aggregate.md) | execution/aggregate | 增量 admission；dirty group 按 aggregate 重建 |
| Window | [window.md](window.md) | execution/window | 状态和 keyset 完整；大 partition 多阶段重建成本高 |
| TopN | [topn.md](topn.md) | execution/topn | 排序索引可分页；每次 dirty update 仍重做排名选择 |
| Sink | [sink.md](sink.md) | execution/sink | 正向插入简单；负向删除可能按结果表扫描 |

## 统一运行模型

每个 step 都在一个 PostgreSQL transaction 中完成一组有界工作：

    lock metadata -> load continuation/state -> bounded SQL primitive
    -> write typed payload/state -> record output -> advance cursor
    -> replace continuation -> checkpoint CAS -> commit

shiba.batch_rows 和 shiba.batch_bytes 同时约束输入/输出 step；单个不可拆分 row 可以超过 byte target。下游背压会在 primitive 执行前阻止 output operator。这些机制保证恢复边界和资源上限，但不自动保证整个 workload 的低复杂度。

## 性能阅读顺序

先看每页的“复杂度与访问路径”，再看 [OPERATOR_PERFORMANCE_AUDIT.md](../OPERATOR_PERFORMANCE_AUDIT.md)。当前 release smoke 的真实性能数据只用于建立测量路径，尚未形成同机 baseline。
