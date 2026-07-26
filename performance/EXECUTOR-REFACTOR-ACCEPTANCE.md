# Executor 重构验收标准

本文档定义 executor 调度和批处理重构的阻断门槛。它补充全算子性能矩阵，
不以最终结果一致性替代事务边界、原子性和 backlog 调度检查。

## 可直接运行的命令

最小架构 gate：

```bash
./scripts/test-executor-architecture.sh
```

完整正确性 gate：

```bash
./scripts/test-all.sh
```

全算子可复现性能矩阵：

```bash
python3 scripts/performance-matrix.py
```

缩短开发反馈时间时，可以降低已有差分测试轮次，但不能用于最终验收：

```bash
SHIBA_DIFF_ROUNDS=20 ./scripts/test-differential-single.sh
```

## 阻断条件

以下任何一项失败都必须阻断合并：

1. 一个 source commit 在 `dag_inbox` 中出现多个 `commit_lsn`，或同一
   DAG/commit 的 `sequence` 不唯一、非正数、没有保持相关事件的 WAL 相对顺序。
   `sequence` 是整个 source transaction 的全局事件序号；某个 DAG 只接收其
   相关事件时，起始值不为 1 或存在 gap 都是合法的。
2. UPDATE 的旧行 `-1` 和新行 `+1` 不属于同一 `commit_lsn`，顺序不是
   `-1,+1`，或中间穿插其他事件。
3. executor 可见地删除了 `commit_lsn <= applied_lsn` 范围内的一部分 inbox
   行，说明 progress 与 inbox acknowledgement 不在同一个原子事务。
4. backlog drain 后结果与 PostgreSQL 从 source 全量重算的 `EXCEPT ALL`
   比较不为零。
5. backlog drain 低于 60 commits/s。该默认门槛明确高于旧实现
   `25ms wait × 每轮一个 commit` 的约 40 commits/s 结构上限；机器较慢时
   可以通过环境变量调整开发运行门槛，但正式结果必须保留默认值和原始输出。
6. 测试 PostgreSQL 日志出现 `WARNING`、`ERROR`、`FATAL` 或 `PANIC`。
7. `scripts/test-all.sh` 或全算子矩阵的正确性、inbox 清空检查失败。

## 必须保持的执行语义

- busy-drain 可以连续领取 commit，但每个 source commit 必须是一个独立的
  PostgreSQL executor 事务。不要把多个 source commit 包在一个
  `BackgroundWorker::transaction` 中。
- 一个 commit 内的 delta 必须严格按 `sequence` 执行；commit N+1 不能在
  commit N 成功提交前执行。
- state、sink、`view_progress` 更新和当前 commit 的 inbox 删除必须一起
  提交或一起回滚。
- worker crash 后允许重新执行仍在 inbox 中的完整 commit，不能只保留或确认
  它的一部分。
- 性能测量必须从 backlog 已经完整路由且 worker 停止的状态开始，到 inbox
  为零结束；生产 source commit 的时间不能混入 drain 时间。

## 已知覆盖边界

架构 gate 通过反复读取 MVCC 快照验证可见的 progress/inbox 原子关系，并由
已有 recovery gate 验证 PostgreSQL immediate restart 后的完整恢复。要精确
命中“算子已经修改 state、但事务尚未 commit”的进程终止窗口，需要 executor
提供仅测试构建启用的 failpoint。加入 failpoint 后，应再增加：

1. 在指定 commit 的第 N 个 delta 后终止 worker；
2. 断言 sink、state 和 progress 保持 commit 前快照；
3. 断言该 commit 的全部 inbox 行仍存在；
4. 重启 worker，断言只重放一次且最终 `EXCEPT ALL=0`。

在 failpoint 落地前，不应宣称已经完成确定性的 mid-commit crash 注入覆盖。
Router 的“inbox 路由事务已提交、slot advance 尚未提交”精确窗口同样还没有
确定性 failpoint；现有 persistent-slot recovery gate 覆盖进程重启与重放，
但不能替代对这一条指令边界的定点终止测试。
