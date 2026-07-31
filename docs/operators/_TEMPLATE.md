# <Operator> 算子实现

> 状态：draft / reviewed / current
> 对应代码：src/execution/<module>/
> 最后核对：YYYY-MM-DD

## 1. 语义与范围

说明输入 weighted row、输出 weighted row、NULL/重复值/排序语义，以及不支持的 SQL 形态。

## 2. Plan contract

| 项目 | 约束 |
| --- | --- |
| 输入 ports |  |
| 输入 producer |  |
| 输出 |  |
| lowering/registration |  |

## 3. 持久状态与索引

| Relation | 用途 | 主键/唯一键 | 关键索引 | 写入阶段 |
| --- | --- | --- | --- | --- |
|  |  |  |  |  |

## 4. 生命周期与 continuation

    Admit -> Process -> Drain -> Frontier

列出每个 phase 的输入、状态修改、输出和 continuation 字段。

## 5. 单步事务流程

1. 锁定 input cursor、output stream 和 checkpoint。
2. 读取 typed continuation 和 operator state。
3. 执行 bounded primitive。
4. 记录 output append/frontier。
5. 推进 cursor、替换 continuation，交给 shared checkpoint CAS。

## 6. Primitive 与复杂度

| Primitive | 输入页 | 状态/输出 | Cursor | 复杂度与访问路径 |
| --- | --- | --- | --- | --- |
|  |  |  |  |  |

明确 P、N、G、K 等变量和最坏情况。

## 7. 正确性与恢复

- crash-before-commit：
- crash-after-commit：
- frontier/causal LSN：
- multiplicity/NULL/排序：
- backpressure：

## 8. 测试与性能证据

- Rust tests：
- PostgreSQL tests：
- Benchmark：
- 结果/基线：

## 9. 已知限制与 roadmap

| 优先级 | 触发场景 | 当前成本 | 计划 |
| --- | --- | --- | --- |
|  |  |  |  |
