# Shiba 架构

本文是当前实现的唯一架构说明。产品能力见 [`MVP.md`](MVP.md)，测试方法见
[`TESTING.md`](TESTING.md)，第一次读代码请走
[`LEARNING_RUST.md`](LEARNING_RUST.md)。

## Shiba 的承诺

Shiba 接受一条普通 SQL 查询，创建结果表，然后异步维护它：

```sql
CREATE TABLE shiba.sales_by_product AS
SELECT product_id, count(*) AS rows, sum(amount) AS total
FROM sales
GROUP BY product_id;
```

它对每个结果表作出一个核心承诺：

> 一次已提交的源表事务会成为一份持久、可重放的输入；该输入对结果、算子状态和
> 进度的影响，要么一起提交，要么一起回滚。

整个架构都服务于这句话。

## 一张图看完

```text
                         declaration
SQL ──> QueryAnalysis ──> LogicalPlan ──> PhysicalDagPlan
 │                                              │
 │ backfill                                     │ persisted once
 ▼                                              ▼
result table <── operator state <── SQL kernels <── DagRuntime
     ▲                                               ▲
     │                                               │
     └──────── atomic apply ───── dag_inbox ─────────┘
                                      ▲
                                      │ lightweight reference
source COMMIT ──> WAL ──> change_log ─┴──> routing
                       durable once
```

可以把 Shiba 看成三个很小的系统：

1. 编译器：把 PostgreSQL Query 变成封闭、持久化的执行计划；
2. durable queue：把 committed WAL 变成只存一次的关系数据和订阅引用；
3. 增量执行器：按计划用集合化 SQL 更新状态和结果。

Rust 负责 PostgreSQL 边界、协议、类型和调度；PostgreSQL 关系负责持久状态；
SQL 负责数据量相关的集合运算。

## 第一部分：一条查询如何成为可执行结果

PostgreSQL 先正常分析 CTAS。Shiba 不解析 SQL 字符串，而是读取 PostgreSQL 已经
解析和类型化的 Query tree：

```text
PostgreSQL Query
-> QueryAnalysis
-> ValidatedQuery
-> LogicalPlan
-> PhysicalDagPlan
-> catalog + initial state + result backfill
```

### 安全边界

`src/query_tree.rs` 是唯一接触 PostgreSQL 原始 Query 指针和 node walker 的
适配层。它把数据复制进 `src/query_analysis.rs` 的 owned Rust 类型后，后续
验证不再依赖 PostgreSQL 指针生命周期。

`ValidatedQuery` 不是一堆互相约束的布尔值，而是受支持查询族的封闭集合：
Aggregate、Join、decorrelated subquery、Window、Distinct 或 TopN。无法表示成
其中一种的 Query 在回填前失败。

### 计划不是运行时猜测

`src/logical/` 把合法查询依次变成：

- `LogicalPlan`：算子及其语义连接；
- typed execution descriptor：完整、封闭的 kernel 输入；
- `PhysicalDagPlan`：fusion、consumer 和 Stage storage 决策。

计划在注册时编译、验证并按 `plan_id` 持久化。Runtime 只加载并复验这份计划，
不会为每次源表提交重新编译，也不会从另一组目录字符串猜出不同执行路径。

### 回填和增量从同一状态出发

注册事务创建结果表、operator state、必要的 Stage、publication membership 和
初始 progress。回填完成后，WAL 增量从注册时捕获的位置继续；结果不会处在“表
已经可见但增量身份尚未建立”的半注册状态。

## 第二部分：跟随一次 source commit

下面从一笔源表事务提交开始，沿着真实数据路径走到结果表。

### 1. PostgreSQL 只交付最终有效变化

Runtime 通过 libpq replication connection 读取 `pgoutput` v2。logical
walsender 持有 decoding context；`shiba runtime` 在自己的 backend 中执行
SPI，两者是不同 PostgreSQL session。

transaction streaming 关闭，所以 top-level abort、savepoint rollback 和
subtransaction rollback 已由 PostgreSQL 过滤。decoding reorder buffer 可按
`logical_decoding_work_mem` 落盘，Runtime 不需要实现第二套回滚日志。

replication socket 的 read/write 永远发生在 SPI transaction 之外。

### 2. WAL 先变成 durable input

Runtime 逐个完整 CopyData message 解码，并按行数、字节数、Commit 或暂时无
更多消息形成有界 batch。一个很大的 source transaction 可以分多次写入
PostgreSQL，因此不必整体进入 Rust heap。

数据模型只有两层：

```text
ingress_transactions   一次 source transaction 的 envelope
change_log             该事务的 ordered row images
```

稳定 identity 让 crash replay 成为 no-op，而不是重复输入：

```text
transaction = (slot_generation, source_xid, final_lsn)
row image   = (ingress_txn_id, change_lsn, change_ordinal, image_ordinal)
```

在看到 Commit 前，batch 只能持久化 row image。最后一批才会把 envelope 标为
committed、创建 routing work 并推进 `persisted_lsn`；三者在同一个 PostgreSQL
事务中提交。之后 Runtime 才向 walsender 反馈该 LSN。

这条顺序解决最危险的 crash window：数据库提交后、feedback 前崩溃只会导致
重发；feedback 不可能越过尚未 durable 的输入。

### 3. 一份 payload，多个引用

`change_log` 不按 DAG 复制。十个结果订阅同一个 source commit，payload 仍只写
一份。

`routing_tasks` 查看该事务涉及的 source OID，按 result OID 分页，为每个相关
DAG 插入一条引用：

```text
dag_inbox(result_oid, ingress_txn_id, commit_lsn)
```

分页 cursor 与本页 inbox 一起提交。routing 中途崩溃时从 cursor 继续；重复
插入由 identity 去重。DAG DROP 只移除自己的引用，不改写共享 payload。

### 4. 一个 Runtime 选择下一个 DAG

每个 active database 只有一个 `shiba runtime` Background Worker：

```text
poll WAL -> persist input -> route references -> apply ready DAGs -> GC
```

没有 Router 进程、Executor pool、每 DAG worker、线程池或连接池。DAG 只是
catalog state 加一个可淘汰的 `DagRuntime` 计划缓存项。

Runtime 在 commit 边界 round-robin：

- 同一 DAG 严格按 `commit_lsn`；
- 不同 DAG 轮流取得一个 commit；
- 一个 apply 开始后不会被 time-slice；
- 所有 SPI work 在这个 backend 中串行。

这个模型没有并发 DAG 写入和跨 worker 协调，代价是长 apply 会暂时阻塞 WAL
ingress、其他 DAG 和 GC。

### 5. Physical plan 驱动一次原子 apply

Runtime 锁定 DAG 和最老 inbox，加载其已验证的 `PhysicalDagPlan`，然后调用
对应 SQL kernel。operator 直接从 `effective_change_log` 集合化读取这次输入。

```text
oldest dag_inbox
-> physical kernel
-> operator state
-> result rows
-> view_progress
-> delete this inbox reference
-> COMMIT
```

以上修改处在一个 PostgreSQL transaction 中。任何 operator error 都会回滚
state、result、progress 和 acknowledgement，inbox 保留，下一次从同一 durable
input 完整重试。

Rust 不把完整 source commit 收集进 `Vec`，不把它重新包装成完整 JSON 传给
SPI，也不逐行调用 operator。数据量相关的工作留在 PostgreSQL relation 中。

## Stage 为什么存在

Stage 只回答一个问题：同一份 relational delta 需要复用多久？

| storage | 何时使用 |
| --- | --- |
| `inline` | 只有一个 consumer，可直接融合 |
| `statement_materialized` | 同一 SQL statement 内多次使用 |
| `unlogged` | 必须跨 SQL statement 复用 |

UNLOGGED Stage 在注册时以明确 schema 预创建。apply 热路径不做 DDL，也不创建
temporary table。

当前 Join 需要两条集合化 statement：

1. 计算精确 `new output - old output`，把 `join_delta` 写入 Stage，同时更新
   durable arrangements；
2. 消费 `join_delta`，更新 downstream state 和 result。

Stage 是可丢缓存，不是恢复权威。成功 apply 后它为空；crash 清空 UNLOGGED
relation 也不影响恢复，因为 inbox、change log、operator state、result 和
progress 都是 LOGGED 的。

## 三条正确性定律

Shiba 的 exactly-once 效果不依赖神奇的消息语义，只依赖三个可检查的顺序。

### Durable before feedback

source transaction identity、全部 row image 和 committed envelope durable 以后，
replication feedback 才能前进。反馈前崩溃可以重放，反馈后不会缺输入。

### Referenced before collect

共享 input 只有在 routing complete、没有 inbox 引用、retention 已过且真实 slot
`confirmed_flush_lsn` 已越过它时才能 GC。quarantined DAG 会保留引用，也就保留
修复后重放所需输入。

### Result and acknowledgement together

operator state、result、progress 与 inbox 删除处在同一 apply transaction。
失败时没有半个 commit 可见，也不会先 ack 后丢结果。

这三条定律比任何具体表名或 kernel 实现更稳定。架构改动如果破坏其中一条，就
不是重构，而是语义变化。

## 持久的和可丢的

恢复设计可以用一条规则判断：

> 无法从 LOGGED 数据重建的状态，不能只存在于 Rust heap 或 UNLOGGED Stage。

| 内容 | 位置 | crash 后 |
| --- | --- | --- |
| plan、registration metadata | LOGGED catalog | 保留 |
| transaction envelope、`change_log` | LOGGED relation | 保留 |
| routing cursor、`dag_inbox` | LOGGED relation | 保留 |
| arrangements、operator state、result、progress | LOGGED relation | 保留 |
| typed Stage | UNLOGGED relation | 可清空并重建 |
| relation metadata、DagRuntime、prepared plans | process memory | 重新加载 |

## 失败如何收敛

| 失败位置 | 系统行为 |
| --- | --- |
| ingress DB commit 前 | slot 从旧 feedback 位置重发 |
| ingress commit 后、feedback 前 | stable identity 去重 |
| routing page 中途 | 从 durable cursor 继续 |
| apply 中途 | PostgreSQL 回滚，inbox 保留 |
| 确定性 plan/operator 错误 | 只 quarantine 当前 DAG |
| crash 清空 Stage | 从 inbox 和 change log 重建 |
| Runtime 异常退出 | PostgreSQL 在 postmaster 存活时重启 |
| postmaster restart | source trigger 或 `shiba.activate()` 重建动态 Runtime |

`shiba.activate()` 不会自动清除 quarantine。修复原因后必须显式恢复，避免 poison
input 不断重试并拖垮唯一 Runtime。

apply、reload 和 DROP 使用相同锁序：

```text
DAG advisory transaction lock
-> runtime state
-> oldest inbox
-> Stage relations（stage_id 顺序）
-> durable operator state
-> result
```

## 诚实的性能边界

单 Runtime 让正确性简单，但不是无限吞吐架构。当前主要代价是：

- 一个长 apply 阻塞所有 Runtime phase；
- Join fan-out 可能产生很大的 delta；
- Window 会重建受影响 partition；
- TopN 仍对完整 retained multiset 排序；
- retained change log 消耗磁盘；
- PostgreSQL 一个 query 中多个 plan node 可分别使用 `work_mem`。

ingress batch、relation cache、DagRuntime cache、SQL work/temp memory、单 commit
行数/字节和 Stage fan-out 都有配置边界。但“有配置边界”不代表 backend RSS 是
精确常数：一个 CopyData message、一个 tuple 和一个完整 DAG apply 仍是不可拆
单位。

当前超限行为是回滚 apply、保留 inbox 并暂停该 DAG。Shiba 尚未实现把一个 source
commit 拆成多个可恢复、可部分可见的 apply transaction。

## 从哪里读代码

| 想回答的问题 | 文件 |
| --- | --- |
| 扩展如何装入 PostgreSQL | `src/lib.rs` |
| CTAS 如何被拦截和验证 | `src/ddl.rs`, `src/query_tree.rs` |
| Query 如何变成封闭类型 | `src/query_analysis.rs` |
| logical/physical plan 如何生成 | `src/logical/` |
| WAL 如何读取和解析 | `src/replication.rs`, `src/pgoutput.rs` |
| committed changes 如何成批 | `src/ingress.rs` |
| 唯一 Runtime 如何调度 | `src/worker.rs` |
| durable input 和 routing | `sql/11_ingress.sql` |
| operator 如何集合化执行 | `sql/21_operator_aggregate.sql` 到 `sql/24_operator_dispatch.sql` |
| Stage 如何创建和检查 | `sql/26_physical_stages.sql`, `sql/30_registration.sql` |
| lifecycle 和用户 API | `sql/40_lifecycle.sql` |

查看某个结果的真实物理计划：

```sql
SELECT shiba.explain_physical('shiba.sales_by_product');
```

任何已实现的架构变化都应直接更新本文并通过：

```bash
./scripts/test-all.sh
```

不要再为“当前架构”创建第二份 design/spec。尚未实现的方案属于 issue 或 PR；
进入实现并验收后，再修改这份唯一说明。
