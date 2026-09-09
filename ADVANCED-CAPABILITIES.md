# flying-orm 专业正式能力

本页说明 `4.0.0` 的进阶 ORM 能力与外部基础设施接入边界，已同步 **2026-09-09 本轮职责调整**，尚未执行本轮回归和质量门禁。各入口复用统一 SQL、参数绑定、Scope、外部事务参与、观测和错误分类管线；整批事务、时限、会话锁等待、回执恢复、连接池、分布式协调和部署治理归上层。仅显式 INDEPENDENT 分片允许每片自有局部事务。

## DatabaseOperator 链式 DML

`DatabaseOperator` 和 `SyncDatabaseOperator` 提供链式查询与 DML，适合实体或表单主路径之外的程序化组合。Operator 不拥有连接池或第二套 SQL 内核；标识符、参数、Scope 和执行保护仍使用 flying-orm 的共享规则。

从 `clients.operator()` 或 `clients.syncOperator()` 获取入口：`dml()` 操作动态模型，`dml(Entity.class)` 使用实体映射，`ddl()` 提供程序化结构入口。已有实体关系模型时优先使用实体 Schema 同步，不并行维护第二套表定义。响应式入口返回 Reactor 类型，同步入口直接返回结果；不使用 hsweb-easy-orm 的 `.sync()` 切换执行方式。

## 注册 SQL 模板

`SqlTemplateRegistry` 在应用装配阶段注册模板，运行阶段通过声明的参数提供器执行。模板适合稳定、需要集中审核的 SQL；模板标识符槽与业务值参数分离，业务值不能通过字符串替换进入 SQL。

模板注册表构建完成后按只读方式共享，不应在请求热路径反复解析和注册。

## 受控原生 SQL

`NativeSqlOperator` 用于数据库专有语法或无法由表单规格清晰表达的查询。原生 SQL 仍受单语句边界、参数绑定、事务参与、资源清理和观测约束；总执行时限由上层或基础设施实施，ORM 不创建本地截止任务。

原生 SQL 不是绕过安全规则的快捷入口：表名、列名和排序等动态标识符必须来自受控映射，业务值必须绑定为参数。

## JDBC、R2DBC 与连接池

可直接复制的最小配置见 [Spring Boot 接入](README.md#例一spring-boot-最小接入) 和 [普通 Java 接入](README.md#例二普通-java-最小接入)。Spring Boot 示例只声明客户端 Bean，不把 Spring 事务自动装配误当作 ORM 已接入外部事务。

- flying-orm 使用上层提供的 JDBC `DataSource` 或 R2DBC `ConnectionFactory`，不实现连接池。
- JDBC 是原生同步执行模式；R2DBC 是端到端响应式执行模式，不用 JDBC 加线程池模拟响应式。
- 驱动、连接池、连接地址、凭据、路由、健康检查和池参数由上层服务决定。
- flying-orm 只在获得连接后管理当前 ORM 操作需要的 Statement、结果和资源终态。
- 查询规格可以提前暴露路由意图，但实际数据源选择、读写分离和故障摘除仍由上层完成。

## 外部事务参与

- 检测到外部事务时，复用外部事务提供的连接，不再次 begin、commit 或 rollback。
- 没有外部事务时，普通单条操作遵循连接和驱动的提交语义；非空 ATOMIC 整批及要求多语句一致性的普通保护写入则在业务 SQL 前明确拒绝，不自行开事务，也不退回逐语句自动提交。
- 只有显式 `INDEPENDENT` 分片允许每片 begin/commit/rollback。它与不兼容的外部事务边界同时出现时会拒绝执行，避免“看似独立、实际被整批提交”的语义漂移；该例外不授权通用事务治理。
- 事务参与 SPI 只描述连接和完成通知，不把上层事务管理器搬进 ORM。

带 Scope 的批量 UPSERT 在 SQL 内检查冲突目标行，不执行“先查权限再写入”的两阶段操作。目标条件只有 TRUE 才允许更新；FALSE/NULL 走既有数据库失败报告。独立分片接口仍可能返回 `FAILED` 分片而不是抛异常，调用方必须读取每片结果；已提交的前片不会因为后片失败而回滚。Scope 每批编译一次并复用参数，保护条件的物理字段与关系子查询沿用同一编译链。没有冲突的新行使用原 INSERT 规则，无更新列时保持 no-op；这些能力不新增事务传播、连接池或业务权限管理。

`writeBatchEvidence(...)` 和底层 `executeBatchEvidence(...)` 只提供分片、输入位置、执行状态和影响行数证据。外部事务中返回时的提交事实通常是 `PENDING_EXTERNAL`；它不等待 completion，不猜测已提交，也不延长事务生命周期。只有上层事务管理器才能在最终提交后发布业务成功。

Schema 对应使用 `EXTERNAL_TRANSACTION_PENDING` 表示事务内回读已通过但尚无最终提交事实，不能按 `SUCCESS` 处理。批量已经确定的提交事实也不会被清理、观察者通知或随后到达的超时降级为 FAILED/UNKNOWN；数据库未确认的结果仍如实保留未知。

## 事务内锁定读取

`LockingReadSpec` 在不改动 `QuerySpec` 的前提下，只提供受控的 UPDATE 锁与 WAIT/NOWAIT/SKIP_LOCKED 组合，不接受任意 hint 字符串。

```java
LockingReadSpec locked = LockingReadSpec.of(query, ReadLock.updateNowait());
QueryRoutingIntent intent = locked.routingIntent(); // PRIMARY_REQUIRED

Flux<DynamicRow> rows = forms.lockingRead(locked);
```

上层先读取 `routingIntent()`，选择主数据源并开启外部事务，再把同一规格交给 ORM。JDBC 在获取自有连接前检查外部事务；R2DBC 在订阅时检查事务上下文。没有外部事务、方言能力未声明或组合不受支持时，在 SQL/自有连接前 fail closed。flying-orm 不为锁定读取开启、提交、回滚或重试事务。

## 超时、取消与资源清理

总执行、同步监听等待、资源清理、LOB 生命周期和 Schema 会话锁等待的时间政策由上层或基础设施拥有。ORM 不创建这些本地截止任务，也不通过 SET/RESET 接管会话锁等待设置。

普通 SQL、批量、清理、同步监听及迁移治理预算默认 `Duration.ZERO`。保留公开签名不等于旧治理行为仍受支持：正批量总 timeout、正 cleanupTimeout、正迁移执行/锁等待预算、R2DBC 正执行预算及公开 LOB codec 的正读取预算均明确报不支持，不静默忽略。旧同步等待签名仅接受零值并交给无 ORM 时限的同步完成桥；事件线程保护和异常传播保留。

普通单条 JDBC SQL 的显式 Statement 超时参数仍可传给驱动，这不是 ORM 自己维护总执行时限。多语句原子保护写入不能把旧总预算偷偷换成各语句独立预算。应用需要总时限时，应在自己的操作生命周期或基础设施中实施。

R2DBC 取消信号沿 Publisher 链传播；JDBC 中断会尝试取消 Statement，并按资源所有权关闭结果、Statement 和自有连接。外部事务连接不会被 ORM 擅自关闭。

## 回执与事务结果恢复边界

幂等重放、回执回查、事务结果恢复和重试裁决归上层。旧 `RECEIPT` 配置与 `resolveUnknown` 暂保留公开签名，但明确抛出不支持错误；ORM 不再建立回执恢复工作流，也不会因保留值模型而继续自动回查。迁移时由上层处理存量任务及回执数据，本轮没有删除数据库中的存量回执。

正常执行证据和真实的已提交、待外部事务确认、失败及未知结果继续保留。未获得数据库或上层的确定事实时，不能把结果改报成功；移除恢复治理不等于丢弃结果真相。

## SQL 观测与错误分类

正式观测能力覆盖 SQL、批量、资源清理和 Schema 迁移。上层可以接入 `SqlExecutionObserver`、`BatchExecutionObserver`、迁移观察者或日志 sink。

观测对象保存结构化分类、耗时、行数和阶段信息；SQL 文本和参数展示由显式日志策略控制。生产配置应保持敏感值关闭或脱敏，并对日志长度设置边界。

## 缓存与精确失效

SQL 结构计划、条件计划和元数据相关缓存使用有界策略。缓存键只包含会改变 SQL 结构的身份，不包含租户值、业务值或实体实例。

Schema 变更后应通过正式失效入口清理相关表计划；应用可以读取缓存快照接入自己的监控体系。flying-orm 不创建后台监控线程。

Schema 元数据缓存和失效优先使用完整 `RelationIdentity`。catalog、schema 和 table 分段参与身份，表名中的字面点号不会被重新解释为限定符。旧 String 入口保持原契约，但关系 Schema 主链路不通过有损字符串回退。

面对只能接收字符串的旧缓存适配，catalog 或字面点号等无法无损表达的身份会保守地全量失效，而不是错误定位另一张表。该内部适配保障不等于应用自行实现的字符串回调也能识别完整关系身份。

## 方言、descriptor、codec、JSON 与向量

五个数据库方言共享统一逻辑类型和 SQL 计划，再在执行边界处理 bind marker、DDL 和驱动差异。正式扩展点包括：

- `ValueCodec` 与驱动值适配器。
- 数据库类型与元数据映射。
- 数组、JSON、LOB、原生 Java 时间类型和向量。
- 结构化条件 term 和 SQL term 包。

实体关系 DDL 也消费同一冻结方言：五个内置方言可从 `RelationalTableDefinition` 生成列、命名 PK/UK/index/FK/CHECK、默认值/生成方式以及表列注释。当前数据库无法无损表达的动作会进入 `requiresManualAction`，不会猜 SQL；例如 SQL Server 扩展属性注释要求 schema 限定表名，MySQL 自增列必须是主键首列。

DDL 审核还冻结 `SchemaSnapshotCoverage`。内置五方言 metadata reader 按实际版本与可表示形状声明 coverage，而非同一数据库名称下所有版本都完整；例如 Oracle 12c 明确缺失默认值、生成方式和排序规则的完整回读。内置或第三方 reader 若只具备部分回读能力，或审阅后 coverage 发生漂移，会在审核或执行前置检查阶段停止，不发送 DDL。

PostgreSQL metadata reader 同时保留逻辑类型与 catalog 证明的物理类型。CRUD、默认值和 CHECK 继续按逻辑类型解释；Schema diff、指纹和 DDL 则保留 `pg_catalog` 内置别名、schema-qualified domain/enum/citext/自定义类型、interval、真实数组和 pgvector 扩展来源。`DATE` 会丢弃 catalog 返回的无意义小数秒精度，`TIME`/`TIMESTAMP` 的零精度不会被吞掉。catalog 无法恢复数组声明维数时只证明数组身份；不能用安全类型文法无损表示的 quoted 或 mixed-case 类型会失败关闭，不会降成近似类型。

受控关系演进按实际方言渲染，不再只为 PostgreSQL 放行：已支持的显式 DROP、单事实列 CHANGE 和索引替换可以进入冻结计划。PG/MySQL 候选键使用单条 ALTER；H2 / SQL Server PK/UK 使用先保护、后交接的受审 SQL，Oracle 已支持的 UK 替换使用唯一索引接管；SQL Server 显式 DISTINCT 同名同策略替换使用 DROP_EXISTING。H2 / SQL Server FK 替换的临时保护不带级联动作，避免重复级联路径；不承诺变更期间业务写入零失败。所有步骤仍由上层提供连接与事务边界，ORM 不管理事务、不将 DDL 执行冒充最终提交，也不在失败后擅自删除剩余保护对象。

Oracle 同列 FK 变化也能生成可执行审核计划，但两条 DDL 之间不具有原子约束保护。
调用方通过 `plan.requiresWritesQuiesced()` 识别前提，并在相关写入确已静止后显式批准：

```java
SchemaMigrationApproval approval = SchemaMigrationApproval.approveWithWritesQuiesced(
        plan, "相关父子表尚未交付写入，按审核计划初始化关系");
Mono<SchemaExecutionReport> execution = schema.executeReviewed(plan, metadataReader, approval);
```

这里的 `schema` 为 `ReactiveSchemaClient`；`JdbcSchemaClient` 的同名入口直接返回报告。
实体同步使用现有 approvals Map 传入同一种批准。新约束强制 ENABLE VALIDATE，普通批准仍拒绝执行；
执行前指纹核验和执行后 diff 不变，失败不会自动补偿或重试。新数据库尚无写入者时即可使用，
不要求应用先实现停写系统；已有写入时由调用方保持静止窗口，直至成功或完成失败恢复。

支持范围不是任意结构迁移：窄化、生成策略或混合事实、未证明安全的候选键替换和不安全 FK 依赖继续保留人工步骤。Oracle PK 仅接纳保留全部旧主键列的扩展，用唯一索引与非空 CHECK 双保护完成交接，不推断主键列退出后的非空所有权。SQL Server 默认约束名由完整 reader 回读并冻结，未提供名称时仍拒绝猜测；更多边界见 [Schema 能力](CAPABILITIES.md#schema-与元数据)。整表删除必须通过 `reviewRelationalAbsent(...)` 明确表达 ABSENT 目标并在执行后回读验证，不会因模型扫描漏掉表而推断删除。

上层可配置的 governed term 必须提供 `TermExtensionDescriptor`：稳定 ID、固定 `FILTER` 用途、所需方言 capability、最大参数数和复杂度成本。`ValueCodecDescriptor` 只声明稳定 ID、Java 类型、逻辑类型和适用 capability。注册表在装配时冻结并缓存指纹；JSON/vector 是现有真实扩展的验证样本。

自关联 JOIN 仍使用现有 `JoinQuerySpec`：同一 `DynamicForm` 可多次加入，每次返回独立的 `JoinSource`。
字段治理键始终是 source + field，不以表名、form ID 或结果别名合并；Scope、PROJECT/FILTER/SORT/JOIN
及 FULL/MASKED/HIDDEN 分别生效。此增强不增加公共来源类型，不修改普通 CRUD 的授权或执行路径。
使用方式与简便门面的歧义边界见 [轻量 JOIN](CAPABILITIES.md#轻量-join)。

旧的自定义 handler/codec 继续作为 trusted startup extension。它们缺少 descriptor 时不会被偷偷暴露给可配置查询；governed 路径会在 SQL/连接前拒绝。请求只能选择装配时已允许的 term，不能传入 renderer、handler、codec 或任意 SQL。扩展必须保持参数化 SQL、稳定类型语义和资源所有权；不通过反射扫描或后台线程把轻量内核变成容器框架。

当前没有生产调用链证据的全文、空间、窗口/CTE、复杂索引、物化视图、RLS、触发器和 COPY，不预建万能 SPI。

### 保护字段与关系 Schema 单一投影

实体关系 Schema 与 CRUD 共用同一份最终物理关系模型。实体上的 `@EncryptedField` 不再只影响写入和查询改写：Schema 冷路径会按启用的保护模式投影密文列、EXACT/SUFFIX 搜索列以及按需的 CONTAINS 辅助表，再由同一个关系模型生成指纹、差异、DDL、回读和执行后验证。

只有能够保持原语义的唯一约束和等值索引才会投影到保护列。主键、外键、分区键、范围约束以及无法保持语义的复合保护索引会在 SQL 发送前明确拒绝，不会静默替换列名。未启用字段保护的实体直接复用原关系定义，不增加普通 CRUD 热路径成本。

SQL Server 可通过 `@TableUnique(..., nullPolicy = UniqueNullPolicy.DISTINCT)` 选择“多个 NULL、非空唯一”。保护字段上的单列声明投影到跨密钥轮换稳定的唯一令牌；上层不需要声明隐藏列。完整 reader 只把所有键列 `IS NOT NULL` 合取的受控唯一过滤索引回读为该语义，不能把任意 partial index、缺失过滤定义或禁用索引报告为完整支持。旧 `DEFAULT`、构造入口和默认指纹保持不变；新语义不是通用外键候选键。SQL Server 创建及使用过滤索引所要求的会话 SET 选项由数据源配置负责，ORM 不修改会话或管理事务，参见 [SQL Server 过滤索引约束](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/create-filtered-indexes?view=sql-server-ver17)。

同一 DISTINCT 声明适用于 PG/MySQL/H2/Oracle：PG/MySQL 使用原生 UNIQUE 的等价语义，H2 显式写出 NULLS DISTINCT；Oracle 的普通组合 UNIQUE 不等价，因而为每个索引键生成 `CASE WHEN <所有键均非空> THEN <本键> END`。Oracle 完整 reader 只认可这一个固定形状并核对真实来源列；系统索引派生隐藏列不作为业务列回读，用户隐藏/虚拟列或其他表达式仍不被猜测支持。创建、追加和验证共享目标物理列顺序；无法通过保留旧列并在表尾追加新列实现的重排会在执行前转为人工步骤。

### 实体分区父表

4.0.0 保留 `@TablePartition` 受控关系原语，当前只开放 PostgreSQL 单列时间 `RANGE`：

```java
@TablePartition(strategy = TablePartition.Strategy.RANGE, property = "createdAt")
class JobEntity {
    private Instant createdAt;
}
```

分区键必须是持久化、未加密的 DATE/TIMESTAMP 属性；策略和物理列名进入关系元数据、指纹、DDL、回读和差异。分区表上的主键、唯一约束和唯一索引必须包含分区键。其他方言会在 SQL 前明确失败关闭，不静默创建普通表。ORM 只负责父表和元数据；下一分区创建时点、范围、留存、归档、删除和协调由上层编排。

## 性能调优与验证

性能参数必须按真实工作负载决定：

- 查询：结果规模、fetch size、映射类型和 LOB 比例。
- 批量：分片大小、并发度、内存预算、事务模式和驱动批处理能力。
- 连接池：由上层按数据库容量、请求并发和事务时长配置。
- 缓存：按 SQL 形状数量和元数据变更频率设置有界容量。

任何性能结论都必须绑定生产 class、固定基线 JAR、硬件、数据库版本、数据量、并发度、预热时间、轮次、错误率、GC、堆和连接归还证据。单次耗时、单元测试、静态结构证据或理论推导不能作为通用性能结论。

## 公共 API 与数据库认证

4.0.0 保留 DynamicForm、CRUD、batch、Repository、实体注解、同步/响应式客户端和 Schema 同步的常用使用主路径，但不是“所有 public 类型及构造器完全不变”。3.3.0 中有 11 个用于 ORM 内部编排、但曾公开的实现类型在 4.0.0 收回包内或删除；直接引用它们的代码需要迁移到下列入口并重新编译。标注 `@InternalApi` 的协作接口不应成为业务依赖：

| 3.3.0 直接引用 | 4.0.0 正式替代入口 | 迁移说明 |
| --- | --- | --- |
| `SchemaDependencyGraph` | `MultiTableSchemaPlanner.plan(RelationalSchemaDefinition)` | 从返回的 `Plan.firstPhase()`、`secondPhase()` 和 `operations()` 读取稳定依赖顺序。 |
| `SchemaStronglyConnectedComponents` | `MultiTableSchemaPlanner.Plan` | 使用 `cycleSupport()` 与 `requiresManualAction()` 消费外键环处置结果，不再依赖内部 SCC 表示。 |
| `SchemaRiskClassifier` | `JdbcSchemaClient.reviewRelational(...)` / `ReactiveSchemaClient.reviewRelational(...)`；旧表单路径使用 `reviewCreateOrAlter(...)` | 从审核结果读取已经结合真实快照与方言能力的风险。 |
| `SchemaSnapshotFingerprint` | `ReviewedSchemaPlan.actualFingerprint()`；完整关系定义比较使用 `SchemaSnapshot.completeTable()` 与 `RelationalMetadataFingerprint.of(...)` | TOCTOU 指纹由审核结果拥有，上层不再复制内部快照编码协议。 |
| `RelationalSchemaPlanReviewer` | `JdbcSchemaClient.reviewRelational(...)` / `ReactiveSchemaClient.reviewRelational(...)` | 客户端统一读取快照、使用方言审核并返回 `ReviewedSchemaPlan`。 |
| `SchemaMigrationReviewer` | `JdbcSchemaClient.reviewCreateOrAlter(...)` / `ReactiveSchemaClient.reviewCreateOrAlter(...)` | 旧 DynamicForm 迁移继续从客户端取得 `ReviewedSchemaMigrationPlan`。 |
| `VerifiedSchemaPlanExecutor` | `JdbcSchemaClient.executeReviewed(...)` / `ReactiveSchemaClient.executeReviewed(...)` | 执行、执行前核验、回读与终态由正式客户端入口统一编排。 |
| `ProtectedIndexProjection` | `JdbcSchemaClient` / `ReactiveSchemaClient` 的 Schema 计划与执行入口；实体使用 `EntitySchemaSynchronizer` | 上层继续提交逻辑字段和索引，由 ORM 自动投影稳定 EXACT 列。 |
| `R2dbcBatchDeadline` | 上层应用或基础设施的执行时限策略 | 本轮删除 ORM 本地截止实现；旧 `BatchWriteOptions.withTimeout` 正预算不再受支持，不把总预算静默改成逐语句预算。 |
| `FieldUseGuard` | 业务代码使用 `FormAggregatePlanner`；内部聚合扩展使用 `FormAggregateReadSupport.approveAggregate(...)` / `approveTermExtension(...)` | 字段用途、查询形状与扩展 term 仍由同一 form 内核审批。 |
| `SqlBindMarkerCompiler` | `SqlStatementCompiler.compile(...)`，并消费返回的 `SqlStatementPlan` | 单语句验证与驱动参数标记编译由同一 owner 完成，正式执行入口不变。 |

另需按使用方式检查以下迁移，不把方法级 ABI 报告当成所有 Java 源码形状的证明：

- `TableUnique.nullPolicy()` 有默认值，普通注解使用无需改动；手写实现 `TableUnique` 接口的类型需实现新增方法。
- `UniqueConstraintDefinition` 增加 `nullPolicy`、`SchemaMigrationApproval` 增加 `writesQuiesced`。两者保留旧两参构造器，但旧二元 record 解构需调整；依赖 record 成分的工具也应核对。
- `BatchWriteOptions` 包含 `maxRowBytes`。从缺少该成分的旧构造方式迁移时需补参数并重新编译；建议使用 `atomic(...)` / `independent(...)` 及 `with...` 方法。
- Schema 批准仍绑定精确计划和指纹，升级后应按当前模型重新审核。批量回执恢复已交上层，旧恢复入口明确拒绝；由上层处理存量任务与回执，不让新 ORM 自动重放旧计划。

### 当前验证快照

**本轮职责修改尚未运行测试、质量门禁、API/ABI 比较或制品安装。以下仅保留历史记录，不覆盖本轮改动，也不能据此声明当前源码可发版。**

下列历史证据绑定 **2026-09-08 的 4.0.0 工作区生产代码及本轮职责修改前在 2026-09-09 完成的本地安装后质量门禁**，不是当前代码的验证或发布声明：

| 检查 | 最近结果 | 不能由此推出 |
| --- | --- | --- |
| 完整质量门禁 | Core 245 项、RDB 2,036 项全部通过，零失败/错误/跳过；Checkstyle、SpotBugs、JaCoCo、发版依赖分析通过。 | 模块门禁分段完成；首次旧 collation 断言失败已修正并复核。专用 3.1.0 冻结基线现已验证，不再跳过。 |
| 后续聚焦回归 | 同生产快照的 68 项及 49 项分别全部通过，覆盖执行证据、缓存、模板、治理、编译及绑定。 | 不是再次执行完整门禁，也不是性能测试。 |
| API/ABI 比较 | 已与本地 3.3.0、已安装 4.0.0 JAR 作 JApiCmp 比较；既有迁移限制保留，最近修复仅新增两个内部命名方法，无新增不兼容项。 | 使用了 `ignoreMissingClasses`；未重新编译完整上层消费者，不证明任意消费者无须迁移。 |
| 构建制品 | sources 的 785 个 Java 与源码、main 的 1,295 个 class 与编译输出逐项匹配；主包、源码、Javadoc 与三份 POM 已安装到本地 Maven，九个文件哈希一致。 | 本地安装不等于 GitHub 或 Maven Central 已同步。 |
| 数据库与性能 | 本次源码复核未重新运行外部四库/Oracle 实例或性能基准。 | 方言 SQL 合同、Mock 驱动和 H2 内存用例不能冒充四库实测或吞吐认证。 |

上述历史任务曾按授权完成本地安装和质量门禁，没有签名、提交或远程发布。本轮职责修改没有重新执行这些操作，不能把历史“审查范围内无开放确认根因”或旧制品哈希套用到新代码。最终交付仍需本轮回归、质量门禁及按要求完成的消费者兼容证据；性能和实库结论必须另有对应实测。

## 继续阅读

- [五分钟上手](README.md#五分钟上手)
- [常用正式能力](CAPABILITIES.md)
