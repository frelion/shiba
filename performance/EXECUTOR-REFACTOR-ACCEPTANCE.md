# Single Runtime 重构验收标准

本文档保留原文件名以维持已有链接，内容定义单 Runtime、共享 change log
重构的阻断门槛。算子状态改为 `UNLOGGED` 不属于本次重构。

## 目标资源与数据模型

- 每个 active database 恰好有一个真实 PostgreSQL background worker，
  backend type 为 `shiba runtime`。
- Router、scheduler、DAG apply 和 GC 是该 backend 内串行执行的阶段，不是
  独立进程或 Rust 线程。
- 每个 DAG 只对应一个缓存的逻辑 `DagRuntime`；增加 DAG 不能增加 backend
  数量。
- `change_log(commit_lsn, sequence, source_oid, delta, row_data)` 对每个源
  delta 只保存一次 payload。
- `dag_inbox(result_oid, commit_lsn)` 对每个相关 DAG/source transaction
  只保存一个轻量待办。
- 算子 SQL 直接读取 `change_log`；Rust 不得把一个 source transaction
  收集成 payload `Vec` 或重新编码为完整 JSON 数组。

当前设计契约见 `docs/ARCHITECTURE.md`。

## 可直接运行的命令

```bash
./scripts/test-executor-architecture.sh
./scripts/test-all.sh
./scripts/performance-matrix.py
```

脚本 `test-executor-architecture.sh` 暂时保留历史文件名。正式性能验收必须使用
完整未过滤 scenario 集、默认规模和三次 randomized repetitions；smoke、
filtered 或 aggregate-only 运行只能作为开发反馈。

## 阻断条件

以下任一项失败都阻断提交：

1. `runtime_state.owner_pid` 与 `pg_stat_activity` 中的唯一
   `shiba runtime` 不一致，或仍出现 `shiba worker`、`shiba dag worker`、
   `shiba router`、`shiba executor`。
2. DAG 数量或 backlog 增长导致 Shiba backend 数量增长。
3. 一个源 delta 因多个 DAG 订阅而在 `change_log` 中复制 payload。
4. 同一 DAG/source transaction 出现多个 inbox 行，或者 transaction 内
   sequence、UPDATE 的 `-1,+1` 相对顺序丢失。
5. Rust apply 路径按事件数量构造 `Vec<InboxEvent>`、`Vec<DeltaRow>` 或整批
   JSON；正式大事务测试必须观测到受控 RSS。
6. 持续积压 DAG 能饿死另一个 runnable DAG。调度必须在 ingress batch 之间
   round-robin；已经运行的 PostgreSQL statement 不可抢占。
7. 一次非末批 apply 没有将 state、result 和 batch cursor 一起提交或一起回滚；
   或末批没有再把 `view_progress` 和 inbox acknowledgement 纳入同一事务。
8. transient error 删除待办或部分提交；deterministic error 终止 Runtime、
   删除 poison 待办、阻止健康 DAG，或 `activate()` 自动清除 quarantine。
9. Router 在 durable change log/inbox 提交前推进 logical slot，或 crash
   replay 生成重复 payload/待办。
10. GC 删除仍被任一 DAG inbox 引用的 change-log transaction；DROP 后无引用
    payload 又无法被最终清理。
11. backlog drain 后与 PostgreSQL 全量重算的 `EXCEPT ALL` 不为零。
12. 普通 correctness/performance 日志出现未预期的 `WARNING`、`ERROR`、
    `FATAL` 或 `PANIC`；failpoint 只允许精确武装的 Runtime crash。
13. 未运行完整 correctness gate 和匹配环境的正式性能对比，或存在未解释、
    未明确接受的统计上有意义性能回归。

## 必须保持的事务语义

Router routing 与 DAG apply 使用不同 PostgreSQL transactions。Router 原子写
`routed_transactions`、共享 payload 和所有 DAG 待办，提交后才能推进 slot。

每个 DAG apply transaction 只处理一个
`(result_oid, commit_lsn, batch_ordinal)`：

1. 锁定并复核待办；
2. 从 `ingress_apply_batches` 取得稳定的 sequence 范围；
3. 从 `change_log` 读取这个范围内的相关事件；
4. 更新正式 state 和 result；
5. 非末批推进 `dag_inbox.next_batch_ordinal`；
6. 末批推进 progress 并删除该 DAG 的 inbox 行；
7. 原子提交。

失败重试从当前 batch 边界开始。已提交 batch 的结果保持可见；失败 batch 的
state、result 和 cursor 一起回滚。一个大 source transaction 会被多个 apply
transactions 消费，其他 runnable DAG 可以在 batch 之间运行。性能报告必须同时
记录单个 batch statement 的 head-of-line latency 和整笔 source transaction 的
完成延迟。

## 恢复与 GC 覆盖

确定性 failpoint 至少覆盖：

- route 已提交、slot 尚未推进时 Runtime crash，重启后无重复 payload/待办；
- apply 已修改当前 batch 的 state/result、提交前 Runtime crash，当前 batch
  效果回滚且 cursor/inbox 保留；之前提交的 batch 不回滚；
- replacement Runtime 重放一次，最终结果正确；
- poison DAG quarantine、健康 DAG 继续、修复后显式 retry；
- PostgreSQL postmaster restart 后由 source statement 或 `activate()` 恢复
  dynamic Runtime；
- 最后一个 DAG reference 消失前 payload 不被 GC，消失后最终被清理。

## 正式性能对比

最终报告至少逐场景提供：

- source-write 和 apply/backlog-drain throughput；
- visibility latency 的中位数和尾延迟；
- source/result query throughput；
- PostgreSQL CPU、Runtime RSS、WAL 和 I/O；
- 大事务峰值 RSS；
- change-log payload 行数/字节数与 DAG fanout；
- correctness、失败事务、日志和最终 inbox/change-log 数量；
- 相对匹配 baseline 的绝对值、百分比变化和三轮离散程度。

矩阵中的 fanout case 必须先将所有参与 DAG 标记为 inactive，等待暂停状态
可见，再提交源事务。恢复 DAG 前必须从同一个 `commit_lsn` 记录并断言：

- `change_log` 行数等于源 delta 数，`payload_rows_per_source_delta=1`；
- `dag_inbox` 行数等于参与 DAG 数，
  `inbox_references_per_dag_transaction=1`。

大事务 case 默认单事务写入 5,000 行，并在暂停 DAG 后覆盖 route 和重新启用后
apply 的完整区间。`resources.csv` 必须同时含 PostgreSQL 进程树
`rss_kib` 和唯一 Runtime PID 的 `runtime_rss_kib`；汇总必须输出
`rss_peak` 与 `runtime_rss_peak`。改变大事务规模会使结果不能与默认正式
baseline 直接比较。

比较必须保持机器、power mode、PostgreSQL/Rust/pgrx 版本、数据库配置、
workload checksum、场景规模和重复次数一致。历史 per-result 或
Router+Executor 报告只能作为历史背景，不能证明 Single Runtime 通过。
