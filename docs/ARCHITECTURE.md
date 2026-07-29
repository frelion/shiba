# Shiba 是怎么运作的

本文是当前实现的唯一架构说明。先不要记事务边界、状态分类或表名，只记住这
一句：

> PostgreSQL 把已提交的源表变化写进 WAL；Shiba 保存一份共享输入，给每个
> 受影响的结果留一张待办卡，再由数据库里唯一的 Runtime 逐张处理。

产品范围见 [`MVP.md`](MVP.md)，测试方法见 [`TESTING.md`](TESTING.md)。第一次
读 Rust 代码可以跟随 [`LEARNING_RUST.md`](LEARNING_RUST.md)。

## 从一笔源事务看完整过程

假设已经声明了一个由 Shiba 维护的结果表：

```sql
CREATE TABLE shiba.sales_by_product AS
SELECT product_id, count(*) AS rows, sum(amount) AS total
FROM sales
GROUP BY product_id;
```

现在应用提交一笔普通事务：

```sql
BEGIN;
INSERT INTO sales VALUES (7, 30);
INSERT INTO sales VALUES (7, 20);
COMMIT;
```

结果表不会在这笔应用事务里同步更新。提交后，数据沿着下面这条路向前走：

```text
sales COMMIT
    -> PostgreSQL WAL
    -> 一份持久的共享输入
    -> 每个受影响结果的一张待办卡
    -> 按保存的规则更新状态和结果
```

这里只有一个搬运者：当前数据库的 `shiba runtime` 后台进程。下面把这张图逐步
展开。

### 1. PostgreSQL 确认源事务

应用事务提交后，PostgreSQL 才会通过逻辑解码交出它的最终变化。已中止的事务、
回滚的 savepoint 和回滚的子事务都不会进入 Shiba 的结果。

源表 trigger 只在提交后唤醒 Runtime，不携带行数据，也不计算结果；WAL 是唯一
数据来源。

### 2. Runtime 把 WAL 变成持久输入

Runtime 使用独立的逻辑复制连接读取 `pgoutput`。它把一笔源事务写成：

- `ingress_transactions`：这笔事务的身份和提交位置；
- `change_log`：按顺序保存这笔事务里的行映像。

一笔很大的事务可以分多个有界批次写入，不需要整体留在 Rust heap。最后一个
批次才把事务标成已提交、创建路由任务并推进持久 LSN。

只有整笔源事务、全部行映像和路由任务都已经提交后，Runtime 才向逻辑复制 slot
反馈消费位置。这个反馈只表示“输入已进入 Shiba 的持久队列”，不表示结果表
已经更新。

稳定的事务和行身份会把 WAL 重发变成幂等重试，不会制造第二份输入。

### 3. 每个结果表收到一张“待办卡”

行映像在 `change_log` 中只保存一次。路由阶段根据这笔事务涉及的源表，给每个
相关结果表插入一条待办引用（`dag_inbox`）。

例如十个结果表都依赖 `sales`，仍然只有一份 `change_log` 数据，外加十条很小
的待办引用。路由按页提交，并保存下一页的位置。

### 4. Runtime 取出一张待办卡并更新结果

Runtime 在有待办的结果之间轮流调度。它为一个结果表取最老的 inbox，加载注册
时保存的物理计划，然后调用集合化 SQL kernel。

以本例为例，kernel 会把 `product_id = 7` 的两条增量合并到聚合状态——也就是
为后续增量保留的内部计数和总和——再更新 `shiba.sales_by_product`。下面这些
动作在同一个 PostgreSQL 事务里完成：

```text
读取最老待办和共享输入
    -> 更新内部状态
    -> 更新结果行
    -> 推进处理进度
    -> 删除这张待办卡
    -> COMMIT
```

提交后，第 7 组的 `rows` 增加 2、`total` 增加 50，之后的 `SELECT` 可以看到
新结果。如果更新失败，整次处理会回滚，待办卡不会消失；具体恢复方式见后文。

## 这份结果表最初是怎样建立的

上面的过程有一个前提：Shiba 已经知道查询、初始结果和后续维护方式。这发生在
声明结果表时。

```text
CREATE TABLE shiba... AS SELECT...
    │
    ├── PostgreSQL 解析并类型化查询
    ├── Shiba 复制成自有的 Rust 数据
    ├── 验证查询并锁住源表
    ├── 创建并回填结果表
    ├── 保存 metadata、内部状态和进度
    ├── 编译并持久化 LogicalPlan 和 PhysicalDagPlan
    └── 加入 publication 并安装保护和唤醒 trigger
```

Shiba 不靠自行解析 SQL 文本来理解查询语义。`src/query_tree.rs` 是唯一读取
PostgreSQL Query 指针的边界；离开它之后，分析使用自有的 Rust 类型。受支持的
查询形状是封闭 enum，不能表示的查询会在注册完成前失败。

注册期间，Shiba 持有源表锁；原生 CTAS 回填、增量起点、计划、状态、publication
membership 和保护 trigger 在同一用户事务中建立。事务提交后才释放源表锁，
所以初始结果与随后读取的 WAL 增量之间没有写入缺口。

计划只在注册时编译。Runtime 启动或重启时加载并复验已持久化的
`PhysicalDagPlan`；处理每笔源事务时不会重新编译或猜执行方式。

部分计划需要在多个 SQL statement 之间复用中间结果。物理计划会为此使用
预创建的 UNLOGGED Stage。Stage 只是工作区：成功后为空，崩溃后也可以从持久
输入和内部状态重建。

## Runtime 实际上有多简单

每个启用 Shiba 的数据库只有一个 Shiba Runtime。PostgreSQL logical decoding 仍有
自己的 walsender，但 Shiba 没有 Router 进程、Executor pool、每 DAG worker、
线程池或连接池。Runtime 反复执行：

```rust
loop {
    ingest_and_route_some_work();
    apply_ready_results_round_robin();
    collect_some_finished_input();
    wait_if_idle();
}
```

每一轮都有工作预算，但一次已经开始的结果更新不会被中途切走。所有 SPI 事务都
在同一个 backend 中串行执行。

这个选择消除了多个 worker 同时更新结果和协调 checkpoint 的问题，也让运行路径
可以从一个函数读完。代价同样直接：一次很慢的 apply 会暂时挡住其他结果、WAL
ingress 和 GC。

处理完成的共享输入不会立刻删除。只有路由完成、所有待办引用消失、保留时间
到期，并且复制 slot 的真实 `confirmed_flush_lsn` 已安全越过该输入后，GC 才会
回收它。暂停或 quarantined 的结果仍有待办引用，因此修复后需要的输入也还在。

## 崩溃后从哪里继续

恢复逻辑只有三个顺序：

1. **先保存输入，再反馈 WAL。** 反馈前可以重发，反馈后的输入已经持久化。
2. **先创建所有引用，再回收共享输入。** 每个结果处理完成前，输入都还在。
3. **结果更新和删除待办一起提交。** 失败时两者一起回滚，成功时一起前进。

因此恢复不需要猜进程刚才执行到哪一行，只需查看持久状态：

| 状态 | 放在哪里 | Runtime 或服务器重启后 |
| --- | --- | --- |
| 计划、订阅、注册信息 | LOGGED catalog | 保留 |
| 源事务、`change_log`、重放位置 | LOGGED relation | 保留 |
| 路由位置、`dag_inbox` | LOGGED relation | 保留 |
| 内部状态、结果、进度 | LOGGED relation | 保留 |
| logical slot 的 `confirmed_flush_lsn` | PostgreSQL slot | 保留 |
| physical Stage | UNLOGGED relation | 内容可清空，在 apply 时重算 |
| relation cache、已加载计划、prepared program | Runtime memory | 重新加载 |

未完成的待办仍在 inbox 中，Runtime 会重新调度它：

- 输入提交前崩溃：slot 从旧反馈位置重发；
- 输入提交后、反馈前崩溃：稳定身份去重；
- 路由中途崩溃：从持久位置继续；
- 结果更新中途崩溃：PostgreSQL 回滚，inbox 保留；
- Stage 被清空：从持久输入重建；
- Runtime 异常退出：postmaster 仍在时由 PostgreSQL 重启；
- 整个 PostgreSQL 重启：下一次源表写入或 `shiba.activate()` 重新建立
  dynamic Runtime。

处理失败后，短暂冲突会自动重试；明确的配额超限会暂停该结果，调高配额后可用
`shiba.resume()` 继续。确定性 plan 或 operator 错误会 quarantine 该结果，不能
resume；修复原因后必须 drop 并重新注册、重新回填。系统错误会让 Runtime 退出
并由 PostgreSQL 重启。处理决定作出前，待办和共享输入都不会静默丢失。

## 它会在哪里变慢或停下

Shiba 的目标是边界明确，不是假装资源无限：

- 单 Runtime 会产生 head-of-line blocking；
- Join fan-out 可能产生很大的 delta；
- Window 会重建受影响 partition；
- TopN 需要排序 retained multiset；
- retained change log 会消耗磁盘；
- PostgreSQL 一个 query 中多个 plan node 可分别使用 `work_mem`。

输入批次有行数和字节数的软目标；完整 CopyData message 或 tuple 不可拆，可能
超过目标。relation cache、loaded-plan cache、SQL work/temp memory、单次提交
和 Stage rows 则有明确上限。一次完整结果更新也不可拆，因此 backend RSS 不是
严格常数。

超过明确的源事务或 Stage 配额时，Shiba 回滚这次 apply、保留 inbox
并暂停该结果；系统级资源错误会让 Runtime 退出并由 PostgreSQL 重启。两者都不
会让半个源事务对用户可见。

## 从哪里开始读代码

先沿着一次源事务读，不要按目录从上到下读：

| 现在追到哪一步 | 文件 |
| --- | --- |
| Runtime 主循环 | `src/worker.rs::shiba_runtime_main` |
| logical replication transport | `src/replication.rs` |
| `pgoutput` bytes 变成 message | `src/pgoutput.rs` |
| message 变成 bounded ingress batch | `src/ingress.rs` |
| input、routing、inbox 和 GC | `sql/10_runtime.sql`, `sql/11_ingress.sql` |
| physical plan 的运行桥 | `src/logical/runtime.rs` |
| 集合化 operator kernel | `sql/21_operator_aggregate.sql` 到 `sql/24_operator_dispatch.sql` |

理解运行路径后，再回头看声明路径：

| 问题 | 文件 |
| --- | --- |
| CTAS 怎样被拦截 | `src/ddl.rs` |
| PostgreSQL Query 指针怎样离开 unsafe 边界 | `src/query_tree.rs` |
| Query 怎样变成封闭 Rust 类型 | `src/query_analysis.rs` |
| logical/physical plan 怎样生成 | `src/logical/` |
| activation、registration 和 lifecycle | `sql/10_runtime.sql`, `sql/30_registration.sql`, `sql/40_lifecycle.sql` |

查看某个结果实际保存的 physical plan：

```sql
SELECT shiba.explain_physical('shiba.sales_by_product');
```
