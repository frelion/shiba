# 算子实现文档与开发规范

这份规范是新增或修改执行算子时的必需检查项。算子代码、测试和实现文档必须在同一个变更中保持一致；没有实现文档的算子不能算作完成。

## 文档组织

- 公共执行协议见 [OPERATOR_PROTOCOL.md](OPERATOR_PROTOCOL.md)。
- 所有算子实现页见 [operators/README.md](operators/README.md)。
- 新算子从 [operators/_TEMPLATE.md](operators/_TEMPLATE.md) 复制一份，文件名使用物理算子名，例如 operators/hash_join.md。
- 如果几个逻辑算子共享同一个物理状态机，可以合并成一页，但必须明确列出每个逻辑算子、分支条件和差异；当前 Scan、Filter、Project 即采用这种方式。

## 每个算子页面的必填内容

### 1. 语义与边界

说明输入/输出 weighted row 的语义、支持的 SQL 形态、NULL/重复值/排序/tie 行为，以及不负责的语义。不要只写“实现了某个 SQL 算子”。

### 2. Plan contract

记录输入 port 数量、输入 producer 类型、输出类型、关键 schema/binding，以及注册和 lowering 入口。必须能回答“这个算子收到什么 row，生成什么 row”。

### 3. 持久化状态与索引

用表格列出每个 relation 的用途、主键/唯一键、读写阶段、因果 LSN 和索引。每个 bounded 查询都必须说明它依赖的索引；如果查询可能退化为全表扫描，必须明确写出，而不是用“分页”掩盖。

### 4. 生命周期与 continuation

画出或列出 Admit、Process、Drain、Frontier 的状态转移，并给出每个 continuation 字段的含义、合法范围和恢复位置。说明一个 step 在何处推进 input cursor、何处创建/删除 continuation。

### 5. 单步事务流程

按实际顺序记录：读取输入、读取/锁定状态、执行 primitive、写 payload、记录 output append、推进 cursor、替换 continuation、checkpoint CAS。算子不能自建事务、checkpoint 或 output publication 协议。

### 6. Primitive 与复杂度

每个 SQL primitive 至少记录：

- 输入页如何由 row/byte budget 截断；
- state/output 修改和返回 facts；
- continuation 如何前进；
- 单页复杂度和一次输入事件的最坏/摊销复杂度；
- 是否会排序、窗口聚合、重复扫描、OFFSET 或全表扫描。

复杂度必须使用可观察变量表达，例如 P（输入页）、G（group 大小）、N（候选状态行）、K（可见输出行），不能只写“有界”。

### 7. 正确性与恢复不变量

至少覆盖：

- crash-before-commit 和 crash-after-commit 的行为；
- state、cursor、continuation、payload/chunk、checkpoint 的原子关系；
- causal LSN/frontier 顺序；
- multiplicity 不会下溢/溢出；
- continuation 不会重复应用旧 action；
- backpressure 时不会产生半个 output。

### 8. 测试与性能证据

列出对应的 Rust tests、PostgreSQL script、故障注入场景和 benchmark case。性能声明必须附带：PostgreSQL 版本、配置、数据规模、profile、原始 JSON/CSV 路径和对比基线。一次 smoke 运行只能作为诊断证据，不能作为回归结论。

### 9. 已知限制与后续优化

把当前已知的 O(N) 扫描、重建、索引缺失、固定事务开销和内存/临时文件风险写成明确 backlog，并注明触发它们的工作负载。不要把已知退化隐藏在“未来优化”中。

## 新算子 Definition of Done

- [ ] OperatorSpec、lowering、validation、dispatcher 和 storage provision 已登记。
- [ ] 每个持久 relation 都有主键/唯一键/外键和索引理由。
- [ ] 所有数据输出 primitive 都通过 StepContext 的 output boundary。
- [ ] 每个可中断阶段都有 typed continuation 和 ABI 校验。
- [ ] row/byte budget、oversized single row、backpressure 已覆盖。
- [ ] crash-before-commit、crash-after-commit、frontier 和重启恢复已覆盖。
- [ ] 有一个真实 PostgreSQL 端到端用例；fan-in/fanout 算子还要有链式 DAG 用例。
- [ ] 实现页已填写复杂度、索引访问路径、性能证据和已知限制。
- [ ] cargo fmt、cargo clippy、cargo test --lib 与相关 scripts/test-*.sh 通过。

## Review 原则

评审先看持久化状态机和访问路径，再看 Rust/SQL 代码风格。下面几类说法需要证据或明确限定：

- “分页所以是高效的”：还要说明 cursor 是否 keyset、查询是否走索引；
- “单步有界所以不会慢”：还要说明处理完一批输入的总复杂度；
- “状态在 PostgreSQL 所以可扩展”：还要说明热 key、热 partition 和 fanout；
- “测试通过所以没有性能问题”：正确性 gate 与性能 baseline 是两套证据。
