# flying-orm 专业正式能力

本页说明基础设施接入、复杂查询和运维治理能力。它们是 `3.2.0` 的公开能力，并继续复用统一 SQL、参数绑定、Scope、事务参与、超时、观测和错误分类管线。

## DatabaseOperator 链式 DML

`DatabaseOperator` 和 `SyncDatabaseOperator` 提供链式查询与 DML，适合实体或表单主路径之外的程序化组合。Operator 不拥有连接池或第二套 SQL 内核；标识符、参数、Scope 和执行保护仍使用 flying-orm 的共享规则。

## 注册 SQL 模板

`SqlTemplateRegistry` 在应用装配阶段注册模板，运行阶段通过声明的参数提供器执行。模板适合稳定、需要集中审核的 SQL；模板标识符槽与业务值参数分离，业务值不能通过字符串替换进入 SQL。

模板注册表构建完成后按只读方式共享，不应在请求热路径反复解析和注册。

## 受控原生 SQL

`NativeSqlOperator` 用于数据库专有语法或无法由表单规格清晰表达的查询。原生 SQL 仍受单语句边界、参数绑定、事务参与、执行超时、资源清理和观测约束。

原生 SQL 不是绕过安全规则的快捷入口：表名、列名和排序等动态标识符必须来自受控映射，业务值必须绑定为参数。

## JDBC、R2DBC 与连接池

- flying-orm 使用上层提供的 JDBC `DataSource` 或 R2DBC `ConnectionFactory`，不实现连接池。
- JDBC 是原生同步执行模式；R2DBC 是端到端响应式执行模式，不用 JDBC 加线程池模拟响应式。
- 驱动、连接池、连接地址、凭据、路由、健康检查和池参数由上层服务决定。
- flying-orm 只在获得连接后管理当前 ORM 操作需要的 Statement、结果和资源终态。
- 查询规格可以提前暴露路由意图，但实际数据源选择、读写分离和故障摘除仍由上层完成。

## 外部事务参与

- 检测到外部事务时，复用外部事务提供的连接，不再次 begin、commit 或 rollback。
- 没有外部事务时，普通操作遵循连接和驱动的提交语义；需要原子性的批量由 flying-orm 在自有连接上管理事务。
- `INDEPENDENT` 批量与不兼容的外部事务边界同时出现时会拒绝执行，避免“看似独立、实际被整批提交”的语义漂移。
- 事务参与 SPI 只描述连接和完成通知，不把上层事务管理器搬进 ORM。

`writeBatchEvidence(...)` 和底层 `executeBatchEvidence(...)` 只提供分片、输入位置、执行状态和影响行数证据。外部事务中返回时的提交事实通常是 `PENDING_EXTERNAL`；它不等待 completion，不猜测已提交，也不延长事务生命周期。只有上层事务管理器才能在最终提交后发布业务成功。

## 事务内锁定读取

`LockingReadSpec` 在不改动 `QuerySpec` 的前提下，只提供受控的 UPDATE 锁与 WAIT/NOWAIT/SKIP_LOCKED 组合，不接受任意 hint 字符串。

```java
LockingReadSpec locked = LockingReadSpec.of(query, ReadLock.updateNowait());
QueryRoutingIntent intent = locked.routingIntent(); // PRIMARY_REQUIRED

Flux<DynamicRow> rows = forms.lockingRead(locked);
```

上层先读取 `routingIntent()`，选择主数据源并开启外部事务，再把同一规格交给 ORM。JDBC 在获取自有连接前检查外部事务；R2DBC 在订阅时检查事务上下文。没有外部事务、方言能力未声明或组合不受支持时，在 SQL/自有连接前 fail closed。flying-orm 不为锁定读取开启、提交、回滚或重试事务。

## 超时、取消与资源清理

执行选项区分 SQL 执行、批量、连接获取参与边界、清理和 LOB 生命周期。超时只治理 flying-orm 实际拥有的阶段；连接池排队和外部事务生命周期仍由上层基础设施负责。

R2DBC 取消信号沿 Publisher 链传播；JDBC 中断会尝试取消 Statement，并按资源所有权关闭结果、Statement 和自有连接。外部事务连接不会被 ORM 擅自关闭。

## SQL 观测与错误分类

正式观测能力覆盖 SQL、批量、资源清理和 Schema 迁移。上层可以接入 `SqlExecutionObserver`、`BatchExecutionObserver`、迁移观察者或日志 sink。

观测对象保存结构化分类、耗时、行数和阶段信息；SQL 文本和参数展示由显式日志策略控制。生产配置应保持敏感值关闭或脱敏，并对日志长度设置边界。

## 缓存与精确失效

SQL 结构计划、条件计划和元数据相关缓存使用有界策略。缓存键只包含会改变 SQL 结构的身份，不包含租户值、业务值或实体实例。

Schema 变更后应通过正式失效入口清理相关表计划；应用可以读取缓存快照接入自己的监控体系。flying-orm 不创建后台监控线程。

## 方言、descriptor、codec、JSON 与向量

五个数据库方言共享统一逻辑类型和 SQL 计划，再在执行边界处理 bind marker、DDL 和驱动差异。正式扩展点包括：

- `ValueCodec` 与驱动值适配器。
- 数据库类型与元数据映射。
- 数组、JSON、LOB、原生 Java 时间类型和向量。
- 结构化条件 term 和 SQL term 包。

实体关系 DDL 也消费同一冻结方言：五个内置方言可从 `RelationalTableDefinition` 生成列、命名 PK/UK/index/FK/CHECK、默认值/生成方式以及表列注释。当前数据库无法无损表达的动作会进入 `requiresManualAction`，不会猜 SQL；例如 SQL Server 扩展属性注释要求 schema 限定表名，MySQL 自增列必须是主键首列。

DDL 审核还冻结 `SchemaSnapshotCoverage`。内置五方言 metadata reader 均声明并实现 complete coverage；第三方 reader 若只具备部分回读能力，或审阅后 coverage 发生漂移，会在元数据前置检查阶段失败，执行器不会发送任何 DDL。

上层可配置的 governed term 必须提供 `TermExtensionDescriptor`：稳定 ID、固定 `FILTER` 用途、所需方言 capability、最大参数数和复杂度成本。`ValueCodecDescriptor` 只声明稳定 ID、Java 类型、逻辑类型和适用 capability。注册表在装配时冻结并缓存指纹；JSON/vector 是现有真实扩展的验证样本。

旧的自定义 handler/codec 继续作为 trusted startup extension。它们缺少 descriptor 时不会被偷偷暴露给可配置查询；governed 路径会在 SQL/连接前拒绝。请求只能选择装配时已允许的 term，不能传入 renderer、handler、codec 或任意 SQL。扩展必须保持参数化 SQL、稳定类型语义和资源所有权；不通过反射扫描或后台线程把轻量内核变成容器框架。

当前没有生产调用链证据的全文、空间、窗口/CTE、复杂索引、分区、物化视图、RLS、触发器和 COPY，不预建万能 SPI。

## 性能调优与验证

性能参数必须按真实工作负载决定：

- 查询：结果规模、fetch size、映射类型和 LOB 比例。
- 批量：分片大小、并发度、内存预算、事务模式和驱动批处理能力。
- 连接池：由上层按数据库容量、请求并发和事务时长配置。
- 缓存：按 SQL 形状数量和元数据变更频率设置有界容量。

任何性能结论都必须绑定生产 class、固定基线 JAR、硬件、数据库版本、数据量、并发度、预热时间、轮次、错误率、GC、堆和连接归还证据。单次耗时、单元测试、静态结构证据或理论推导不能作为通用性能结论。

## 公共 API 与数据库认证

`3.2.0` 的发版验证覆盖公共 API/ABI 比较、完整质量门禁、发布制品检查，以及 PostgreSQL、MySQL、Oracle 和 SQL Server 的 Docker 实库往返认证。H2 由自动化测试覆盖；静态方言合同和 Mock 驱动结果不替代真实数据库证据。

## 继续阅读

- [五分钟上手](README.md#五分钟上手)
- [常用正式能力](CAPABILITIES.md)
