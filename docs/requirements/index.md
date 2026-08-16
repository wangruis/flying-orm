# flying-orm 需求索引

这份索引用来对齐“我们到底要做什么、做到哪了、后面还差什么”。编号只代表需求登记顺序，不代表实现优先级。

## R-001 高性能 ORM 内核

- 状态：V2.0.0 双执行内核代码、认证记录和发布门禁已完成
- 来源：用户要求自研一个为动态表单而生的简单 ORM。
- 目标：覆盖动态表单应用的主要功能，同时以内核全新设计追求可验证的性能、高并发、高吞吐量和稳定性。
- 当前进展：动态表单元数据、条件 DSL、SQL 渲染、双执行内核、批量写入、五库方言、Repository、执行保护、执行观测、Caffeine 元数据缓存、乐观锁、逻辑删除、Scope、受控原生 SQL 和 DDL 审核均已形成主链路。V2 的四种生产数据库兼容与并发认证已在固定环境连续复跑三轮；历史报告不作为后续新代码的自动证明。

## R-002 少模块架构

- 状态：已完成首版
- 来源：用户反馈新架构模块过多。
- 目标：保持主项目轻，避免为了“看起来完整”拆出过多模块。
- 当前进展：当前采用 core、rdb、testkit 与 benchmark 的紧凑模块布局。应用框架适配只放在上层系统或平级示例，
  core/rdb 不反向依赖 Spring，模块依赖方向保持单向。

## R-003 API 可重新设计

- 状态：已确认
- 来源：用户确认 API 不强兼容，可以大胆优化架构。
- 目标：不背负历史 API 兼容，只保证目标功能覆盖、概念可迁移和使用体验更简单。
- 当前进展：已形成以 `ReactiveFormClient`、`SyncFormClient`、`DatabaseOperator`、`FlyingOrmClients`、Repository、结构化条件和服务端 `DataScope` 为主的新 API 形态。

## R-004 轻量测试策略

- 状态：已确认
- 来源：用户多次要求初期和当前阶段适当减少测试验证。
- 目标：脚手架和文档变更只做必要校验；核心生产逻辑保留关键契约测试；涉及性能结论时再补 JMH、专项压测和真实库验证。
- 当前进展：已按“小步实现 + 少量关键测试 + 编译验证”的节奏推进；JMH 已覆盖条件、渲染、批量计划、实体映射、元数据缓存和执行器包装开销，并建立同机对比报告模板，但正式性能结论还不能提前写死。

## R-005 轻 SQL、重 Java

- 状态：已完成首版
- 来源：用户强调“轻SQL，重java”。
- 目标：业务侧优先通过 Java DSL、结构化条件树、元数据和类型系统表达意图，SQL 只是渲染结果和高级逃生口。
- 当前进展：条件、DDL、DML、分页、批量、Repository 和 operator 门面都走 Java API；原生 SQL 保留为明确逃生口，不作为主业务编排方式。

## R-006 动态表单与动态表结构

- 状态：主项目边界已完成
- 来源：用户强调动态表单需要动态维护表结构并支持增删改查。
- 目标：围绕元数据、DDL、DML 和运行时表模型建立闭环，支持字段增删改、表结构变更、数据增删改查和后续迁移策略。
- 当前进展：动态表单 CRUD、分页、排序、count、逻辑删除、乐观锁和批量写入已经闭环；DDL 支持建表、安全 `createOrAlter`、字段及索引差异、显式重命名和危险变更审核，并能返回正向计划、回滚计划、风险、批准指纹和逐步执行结果。五种数据库的当前 DDL 契约已经完成真实库认证；更高级在线迁移仍属于后续版本。

## R-007 参数驱动动态条件与可扩展通用条件

- 状态：已完成首版
- 来源：用户强调参数驱动动态条件是重点，并给出 `where("userId","user-in-org",orgId)` 示例。
- 目标：条件系统必须支持任意 term id 注册与解析，不局限于 `=`、`>`、`like` 等固定符号；前端也能透传结构化条件，同时不能带来 SQL 注入风险。
- 当前进展：已实现参数编译器、前端结构化条件输入模型、严格安全策略、OR 条件组、默认值、参数转换器、自定义 term、`SqlTermPackage` 命名包、`relationExists` / `relationNotExists` 关系型 SQL term SPI，并提供 `user-in-org` 示例条件包。条件值按 `NONE`、`SCALAR`、`COLLECTION`、`RANGE` 和显式 `SCALAR_OR_COLLECTION` 处理：未知业务 term 只按单值处理，集合、区间和无值形状必须明确声明；实际执行主链路直接复用 `SqlRenderer` 汇总出的 term 元数据，Java Operator、参数条件包和前端结构化条件不再各维护一份值形状。独立使用 core 参数编译器时仍可显式附带自己的 term 注册表。普通 `where(...)` 遇到空值直接拒绝，`whereIfPresent(...)` 才会忽略空值；字符串会去掉前后空白，集合会清理空项，区间会校验长度、类型和顺序。`is-null` / `is-not-null` 不再借用普通 `= null` 表达。条件值继续接入 `ValueCodecRegistry.write()`，SQL 参数仍走绑定。结构化条件失败时提供稳定错误码、前端路径、字段、operator 和统一的 `toErrorReport()`，比如 `conditions[2].value` 可以直接定位到前端条件行。

## R-008 真响应式 R2DBC/Reactor 支持

- 状态：已完成首版
- 来源：用户强调“真响应式支持，封装r2dbc.reactor”。
- 目标：R2DBC/Reactor 是 flying-orm 的真响应式执行路线；同步 API 使用原生 JDBC，不把 JDBC 包进 Reactor，也不通过 R2DBC 阻塞桥伪装同步执行。
- 当前进展：`R2dbcSqlExecutor` 和原生 JDBC executor 已形成双执行内核，响应式与同步 FormClient、Repository、DDL/DML operator、批量写入分别复用对应执行链；两条执行链共享条件、渲染、方言、参数、映射和安全规则。

## R-009 RDB 方言统一入口

- 状态：主项目边界已完成
- 来源：动态表结构维护和动态数据查询都需要数据库差异抽象。
- 目标：以轻量 `RdbDialect` 聚合结构 SQL、分页 SQL、upsert、类型映射和后续能力差异，避免到处散落数据库判断。
- 当前进展：H2、MySQL、PostgreSQL、Oracle、SQL Server 方言已接入 schema、form、metadata、分页、upsert、类型和异常分类；`RdbDialectResolver` 可从配置或连接工厂自动识别。五库真实数据库认证、并发和性能证据已归档，驱动或执行器变更后仍需按同一清单复跑。OpenGauss、Kingbase 当前不规划。

## R-010 外部应用框架适配边界

- 状态：已完成
- 来源：用户要求 Spring 项目自动组装、继承即用，同时保持纯 Java 用户可独立使用。
- 目标：core/rdb 保持框架无关；上层系统或平级示例提供自己的薄适配，不在主项目增加 Spring 或 starter。
- 当前进展：`FlyingOrmClients` 支持方言自动识别和统一客户端组装；平级 Spring 示例已验证实体发现、泛型
  Repository、响应式/同步继承式 Service 和外部事务。所有 Spring 代码都留在示例中，不进入主项目。
- 决策记录：`docs/requirements/2026-07-29-upper-spring-boot-autoconfiguration-boundary.md`。

## R-011 响应式动态表单批量写入

- 状态：已完成首版
- 来源：高性能、高并发、高吞吐量目标，以及动态表单和真响应式支持要求。
- 目标：动态表单批量写入只生成一条 SQL、复用一套字段布局，并通过 R2DBC 原生批处理提交多组参数，不退化成阻塞或逐行执行。
- 当前进展：批量请求已经统一为 `BatchWriteRequest`，List 和 Publisher 输入都进入同一条 `writeBatch(...)` 执行链，不再保留另一套只返回总行数的低层批量契约。响应式与原生 JDBC 批量入口共享默认 `ATOMIC`、显式 `INDEPENDENT`、`PARTIAL` 汇总、`UNKNOWN` 和批量观测语义；事务回执确认与安全重放属于 R2DBC 内核，JDBC 使用上层业务幂等事实确认。

## R-012 传统 JDBC 风格阻塞桥接

- 状态：原生 JDBC 同步内核已接入，R2DBC 阻塞桥不存在
- 来源：V2 要求同时支持传统 JDBC 和真响应式 R2DBC，两条链路共享 ORM 语义但分别执行。
- 目标：提供传统 Java/JDBC 使用习惯的同步入口，并以 `DataSource + java.sql.Connection` 作为正式执行内核；响应式入口继续使用 R2DBC/Reactor。
- 当前进展：原生 JDBC 查询、写入、批量、Schema、FormClient、Repository 和 Operator 已接入；旧 R2DBC 阻塞同步桥已删除。调用方使用虚拟线程时，可以在虚拟线程里承接同步门面的等待。
- V2 决策：不保留 R2DBC 阻塞同步桥接或兼容开关。同步调用必须由 `DataSource + java.sql.Connection` 原生执行，
  响应式调用必须由 `ConnectionFactory` 原生执行；共享 ORM 语义，但不互相包装。
- 决策记录：`docs/requirements/2026-07-24-r2dbc-first-blocking-bridge.md`。

## R-013 RDB 异常统一分类

- 状态：已完成首版
- 来源：生产数据库兼容需要让上层稳定判断错误类型，而不是直接解析驱动异常。
- 目标：将 R2DBC/JDBC 驱动异常翻译成 flying-orm 自己的错误分类，覆盖唯一键、完整性约束、SQL 语法/对象、连接、超时、死锁、锁等待、取消和未知错误。
- 当前进展：已新增 `RdbException`、`RdbErrorKind` 和 `RdbExceptionTranslator`；支持解开 `CompletionException` / `ExecutionException` 这类透明异步包装，并按 SQLState 优先、MySQL/PostgreSQL/Oracle/SQL Server 错误码兜底分类。带业务结果的外层异常不会被错误拆掉。查询、写入、批量结果和观测链路使用同一分类，`BatchChunkResult.Failure.kind()` 可直接供上层处理。内核不自动重试写入。

## R-014 对象映射与类型化查询

- 状态：已完成
- 来源：动态表单 Map 链路已经跑通，后续需要让普通 Java 对象也能方便接入。
- 目标：在不影响动态表单低层 Map 能力的前提下，提供 record/JavaBean 映射、预计算映射计划和 Repository 基础能力。
- 当前进展：`RowMapper`、`EntityValues`、响应式/同步 Repository、flying-orm 自有实体注解、生命周期契约和字段感知 codec 已经闭环。JSON、PostgreSQL Array、BLOB/CLOB 和 Vector 的当前契约均已完成真实库验证；内部 `MappingPlan` 保持包级可见，不进入正式公共 API。

## R-015 Operator 易用门面

- 状态：已完成首版
- 来源：用户希望支持类似 `operator.ddl().createOrAlter(...).addColumn()...` 和 `operator.dml().query()...` 的使用体验。
- 目标：提供直观的链式 operator API，但只作为易用门面，不改变 R2DBC 主内核和动态表单底座。
- 当前进展：已新增 `DatabaseOperator`、`SyncDatabaseOperator`、响应式 DDL/DML builder 和同步门面；DDL `createOrAlter` 支持 `commit()` / `commitDetailed()` / `plan()`，DML query 支持 `select/from/where/fetchMap`，DML update/delete 已有链式门面并可显式传 `OptimisticLockOptions`；query/update/delete operator 可显式声明逻辑删除字段和值，delete 可用 `physical()` 强制物理删除；query/update/delete operator 可显式传 `DataScope` 做服务端数据范围收窄；查询、更新和删除可显式传 `SqlExecutionOptions`。

## R-016 企业级元数据缓存

- 状态：Caffeine 企业版首版已完成
- 来源：动态表单元数据高频读取，以及企业级高并发、高吞吐量目标。
- 目标：先跑通缓存、失效和 DDL 联动语义；性能阶段直接依赖 Caffeine，实现更成熟的并发缓存、TTL、容量淘汰、DDL 失效和指标钩子。
- 当前进展：元数据、实体映射和 SQL 结构计划统一使用 `OrmCachePolicy` / `CacheRegionPolicy`。权重、单项最大权重、访问后过期和统计开关均原样落到 Caffeine；实体映射容量配置已接入客户端实例注册表。DDL 在成功、失败或取消且已开始执行时都会使相关缓存失效，避免继续读取不确定结构。

## R-017 执行保护策略

- 状态：已完成首版
- 来源：企业级稳定性要求，防止慢查询、大结果集和超大批量把服务拖垮。
- 目标：统一查询、更新、批量写入的 timeout、最大返回行数、结果总内存、批量最大输入行数和取消行为，并能从客户端/Operator/Repository 创建入口统一设置默认值。
- 当前进展：普通 SQL 默认启用可关闭的总超时、最大结果行数、累计结果内存和单个 LOB 大小；连接获取等待由上层连接池配置。动态表单解码后的 BLOB/CLOB 会重新检查总内存，避免物化后绕过限制。批量写入同时限制总行数、分片、并发、内存与结果分片数；Repository 需要保留生命周期实体时，实体和映射后的参数对象共用同一内存预算。确需解除 ORM 执行保护时必须显式选择 `unlimited()`。

## R-018 执行观测与错误分类

- 状态：已完成首版
- 来源：企业级稳定性和后续性能优化需要知道 SQL 类型、耗时、影响行数、失败分类和批量分片状态。
- 目标：在不绑定具体日志/指标框架的前提下，提供轻量 hook，让上层自行接入日志、指标、链路追踪或告警。
- 当前进展：已新增 `SqlExecutionObserver`、`SqlExecutionObservation`、`SqlExecutionOperation`、`SqlExecutionStatus`、`SqlFailureCategory`、`SqlFailureClassifier`、`SqlStatementType`、`SqlExecutionObservers`，并新增 `BatchExecutionObserver`、`BatchExecutionObservation`、`BatchExecutionEventType`；`R2dbcSqlExecutor` 和 `ObservedReactiveSqlExecutor` 已接入普通 SQL、批量 chunk、summary、UNKNOWN recovery 的观测事件。

## R-019 动态表单 DML 乐观锁

- 状态：单行与批量主链路已完成
- 来源：企业级动态表单会有多人、多服务并发修改同一行数据的场景，不能让后提交的数据静默覆盖先提交的数据。
- 目标：支持基于 `version`、`revision` 或 `updated_at` 的乐观锁字段策略；update/delete 时由 flying-orm 生成版本条件，update 成功时递增或刷新版本值；影响行数为 0 时返回稳定的并发冲突结果或异常，让上层业务能明确感知并决定重试、提示用户或放弃本次写入。
- 当前进展：已新增 `OptimisticLockOptions`、`OptimisticLockMode` 和 `OptimisticLockConflictException`；动态表单 update/delete 已支持显式乐观锁，数字版本可渲染为 `version = version + 1`，时间或调用方新值可用 ASSIGN 模式写入；影响行数为 0 时会抛稳定冲突异常。批量执行层支持 `EXACTLY_ONE`、精确输入偏移、`CONFLICTED` 分片和乐观锁观测分类；FormClient 可用 `BatchOptimisticUpdate` 为每行携带更新值、条件和旧版本，Repository 可从实体 `@TableId` 与 `@Version` 自动生成批量更新。默认 `ATOMIC` 在任一行冲突时整批回滚，显式 `INDEPENDENT` 会返回成功分片与冲突分片。批量路径仍会统一应用租户、DataScope、FieldScope 和逻辑删除，并且整批只读取一次动态 scope 快照。H2、MySQL、PostgreSQL 的同版本并发竞争和冲突观测已纳入 V2 真实库认证。

## R-022 逻辑删除

- 状态：动态表单主链路首版已完成
- 来源：企业业务常见“删除可恢复、可审计”的要求，且用户确认可以显式声明逻辑删除字段和标志值。
- 目标：默认删除不丢数据；动态表单和实体都可以显式声明逻辑删除字段、未删除值和已删除值；查询、分页、更新、删除自动带上未删除条件；真要物理删除时必须显式调用物理删除入口。
- 当前进展：core 已新增 `LogicDeleteDefinition`，`DynamicForm.Builder.logicDelete(...)` 可声明字段和值；`ReactiveFormClient` 和 `SyncFormClient` 的 select/page/update/delete 已自动加未删除条件，默认 `delete(...)` 会渲染为 update 删除标记；`physicalDelete(...)` 保留真正物理删除。已新增 `@FlyingLogicDelete`，可标在字段上，也可标在类上用 `field` 指明字段，实体元数据生成 `DynamicForm` 时会携带逻辑删除配置；Repository 会避免重复追加未删除条件。逻辑删除和 `@Version` 可一起使用，传实体或显式锁删除时仍能带乐观锁。DML query/update/delete operator 已支持显式 `.logicDelete(...)`，delete operator 可用 `.physical()` 强制物理删除。
## R-020 租户隔离与 DataScope 安全内核

- 状态：首版已完成
- 来源：多租户、动态表单、前端结构化条件和业务数据权限需要统一落到安全 SQL 上；用户已确认 flying-orm 不做完整权限系统，只做安全数据访问内核。
- 目标：提供可选的租户和通用数据范围能力，支持 `TenantScope`、`DataScope`、`FieldScope`、`TimeScope` 与普通动态条件安全合并。`DataScope` 首版深化覆盖全部数据、所在组织及下级组织数据、所在组织数据、仅本人数据四类预设；这些预设既能用于无租户系统，也能与 `TenantScope` 组合用于 SaaS。select/page/count/update/delete/Repository/Operator 走同一套隔离语义，前端条件不能伪造或绕过服务端 scope。
- 当前进展：共享表租户列、DataScope、FieldScope、TimeScope、逻辑删除和乐观锁已经在 FormClient、Repository、Operator、单条和批量入口保持一致。`RoutingConnectionFactory` 进一步补齐 schema、独立数据库与数据库原生 RLS 协作：Reactor Context 携带隔离结果，连接工厂按稳定数据库键选池，连接借出后设置 schema/RLS，归还前清理；PostgreSQL 已提供参数化 `set_config` 实现，并完成真实连接池下的 Schema/RLS 清理认证。上层仍负责用户识别、鉴权结果和连接池生命周期，flying-orm 不依赖 Spring，也不把业务权限规则写进内核。

- 错误模型补充：租户 scope 缺失、手动租户字段缺失、租户值冲突、重复租户字段和字段范围拒绝统一抛出 `ScopeAccessException`。它仍是 `IllegalArgumentException`，但额外提供稳定 `ScopeErrorCode`、`formId` 和 `field`；直接 `DatabaseOperator` 查询不可读字段也使用同一错误。结构化条件、Scope 和数据库异常都实现 `OrmErrorReportProvider`，可输出统一 `OrmErrorReport`，上层不再解析异常文本。

## R-021 业务授权协作示例

- 状态：文档首版已完成
- 来源：IoT 场景里设备归属、工程商授权、用户分享和告警可见范围要和数据访问配合，但不应该变成 ORM 内核职责。
- 目标：在上层示例或文档中说明业务服务如何用 flying-orm 查询设备、租户、分享关系和告警记录，并把业务权限结果转换成 R-020 的 scope；业务授权规则由上层计算，flying-orm 只承接最终的数据范围。
- 当前进展：已新增 `docs/business-scope-collaboration.md`，说明无租户系统和 SaaS 如何把设备归属、组织树、用户分享、告警时间与字段范围转换成通用 Scope。示例只表达协作方式，不在 ORM 内核增加设备、分享或告警专用模型。告警信息如果在业务数据库中，可以作为普通业务事件表查询和过滤；温湿度等连续采集值仍由专门时序组件或上层客户端处理。

## R-023 高级能力增强

- 状态：已完成
- 目标：补 PostgreSQL Vector、受控 SQL 模板逃生口、更完整的 flying-orm 自有注解映射语义、schema/独立库/RLS 隔离，以及迁移回滚和在线 DDL 审核。
- 当前进展：五项代码能力和最终认证均已完成。SQL 模板只能由服务端注册并参数绑定；迁移审核会生成回滚计划、风险和精确批准指纹；pgvector、Schema/RLS 会话设置及连接池清理已经在真实 PostgreSQL 环境通过。

## R-024 发布后企业增强

- 目标：补复合游标分页、数据级迁移补偿、正式 Oracle/SQL Server 支持、缓存指标桥和驱动特殊值/列别名适配。
- 当前进展：五项均已完成。游标分页不执行 count 且不随页深增长 offset；数据迁移失败后逆序补偿已成功步骤；Oracle/SQL Server 按归档实库证据提升为正式支持；缓存指标和驱动适配保持框架无关。

## R-025 原生参数化 SQL 与注册复杂查询

- 状态：已完成
- 来源：复杂联表、窗口函数、CTE 和数据库专有语法需要一个不受结构化 DML 能力限制的后端代码入口。
- 目标：允许业务代码直接写一条 SQL，通过命名参数安全绑定后交给对应 JDBC/R2DBC 执行器；查询可返回 Map 或实体，写入返回影响行数，并保留执行保护、观测和错误分类。
- 当前进展：临时 SQL 使用显式命名的 `unsafeNativeSql(...)` 响应式与同步入口。需要稳定复用的复杂查询在启动阶段注册到 `SqlTemplateRegistry`，通过 `DatabaseOperator.sqlTemplate(id)` 执行；租户、用户等服务端参数在每次订阅或调用时由统一提供器读取，普通参数不能覆盖。两个入口都复用参数绑定、codec、执行保护、观测、异常分类和对应 JDBC/R2DBC 执行链。模板入口只允许查询，分页、游标和统计使用显式 SQL 模板，不自动改写复杂 SQL。

## R-026 一体化易用性增强

- 状态：已实现并完成验收
- 来源：用户要求实体 Lambda、自动元数据、外部示例装配和启动期安全 Schema 能力一次到位。
- 目标：遵循“简单易用、稳定与安全优先，同时追求高性能、低延迟、高吞吐”，完成实体 Lambda、自动元数据与表单、Spring 继承即用、自动组装和启动期安全建表改表。
- 当前进展：实体 getter Lambda 已覆盖查询、投影、分组、排序、嵌套条件、更新、原子增减、删除和乐观锁；
  实体元数据自动生成 `DynamicForm`。平级 Spring 示例负责发现实体并注册 Repository/Service；主项目的 schema
  纯 Java 能力支持校验、创建和安全变更，并通过迁移审核、危险操作拒绝和锁超时保持 fail-closed。

## R-027 全局类规模约束与大型类治理

- 状态：全局规则与 flying-orm 大型类治理均已完成
- 来源：用户明确要求“开发和修改最好不能有大型的类”，并进一步确认该要求适用于现在和以后所有项目，
  不是 flying-orm 单项目约束。
- 目标：生产类型通常不超过 300 个物理源码行和 20 个可调用方法；达到该规模必须进行职责审查，不得新增或
  继续扩大达到 400 行或 30 个方法的生产类型。公开 API 通过稳定门面保持简单，内部按行为和职责拆分，不能用
  无意义转发层、重复同步/响应式实现或额外热路径对象满足形式指标。
- 当前进展：全局规则已写入 `AGENTS.md`。大型类型按真实职责治理，没有把职责搬进新的生产大类；功能、SQL、
  参数顺序、Scope、事务、取消、缓存和批量结果语义由契约与真实库认证保护。

## R-028 外部事务与内部事务统一

- 状态：框架无关内核、Spring 示例和真实 PostgreSQL/MySQL 最终认证均已完成
- 来源：上层响应式后台系统使用 Spring 统一事务管理；用户明确要求事务一致性必须由 flying-orm 从架构上解决，
  不能要求业务代码自行避开冲突，必要时允许破坏性重构。
- 目标：一次调用只能有一个事务控制者。存在外部事务时，CRUD、Repository、FormClient、原生 SQL 和 `ATOMIC`
  批量加入同一事务绑定连接，flying-orm 不再 begin/commit/rollback；不存在外部事务时，`ATOMIC` 继续由
  flying-orm 保证整批原子性。外部事务中的 `INDEPENDENT` 在执行 SQL 前明确拒绝。
- 安全结果：外部事务里的批量只能报告已经加入和已经执行，在外部真正提交前不能谎报 `COMMITTED`；最终提交、
  回滚或未知状态通过外部事务终止信号和统一事务观测表达。动态数据源路由在事务开始前固定，单个本地事务不承诺
  跨物理数据库原子性。
- 架构边界：core/rdb 提供框架无关的连接与事务参与契约，不依赖 Spring；外部框架适配器使用各自受支持的事务
  感知连接机制接入。Spring 业务只使用标准 `@Transactional`，不增加 Flying 专用事务注解或所有权配置。
- 当前进展：普通 SQL、原生 SQL、FormClient、Repository 和 ATOMIC 共用外部事务连接，外部事务不由 ORM
  begin、commit、rollback 或 close；INDEPENDENT 在 SQL 前拒绝。H2 契约与真实 MySQL/PostgreSQL 均已验证
  提交、回滚、ENLISTED 最终状态、事务路由锁定、单连接复用和数据结果。

## R-029 Java 生态统一接入契约

- 状态：框架无关启动契约和 Spring Boot 首个接入示例已完成，其他 Java 生态可复用同一能力适配。
- 来源：用户明确要求 flying-orm 不只能方便接入 Spring Boot，其他 Java 管理系统也必须能够简单、完整地接入。
- 目标：提供框架无关的类型化配置、连接、外部事务、Scope/调用上下文、observer 和运行时装配契约。纯 Java 可以
  直接使用；Spring、Micronaut、Quarkus、Jakarta CDI、自研容器只编写薄适配器，不复制 ORM 内核。
- 优先级：Spring/Spring Boot 是首个完整示例和认证对象，先验证 WebFlux、R2DBC、动态数据源和标准
  `@Transactional`；其他 Java 生态随后复用同一契约，不能反过来让核心依赖 Spring。
- 配置约定：配置来源可以是 YAML、Properties、MicroProfile Config 或框架配置对象，但规范根前缀固定为
  `flying-orm`，字段名称和语义保持一致。`dialect` 可选：明确配置时使用配置方言，未配置时从 R2DBC URL 或
  `ConnectionFactory` metadata 自动识别，无法可靠识别时启动失败。
- 易用性目标：适配器自动装配 Executor、FormClient、Repository、DatabaseOperator、Schema 和 Metadata；
  Spring 优先支持继承业务基类直接使用 `createQuery()`/`createUpdate()`、按泛型注入
  `ReactiveRepository<T, ID>`，以及复用统一事务和执行保护的参数化原生 SQL。
- 完整接入标准：不仅能够创建客户端，还必须通过连接复用、提交、回滚、取消、动态路由锁定、Repository 注册、
  Scope 上下文、错误传播、日志和 observer 隔离契约。框架适配器不得成为 core/rdb 的传递依赖。

## R-030 可配置 SQL 执行日志

- 状态：框架无关日志策略、上层映射、真实库与性能回归均已完成
- 目标：默认不展示完整 SQL 和参数；开启后统一脱敏、截断并限制总长度。支持 SQL 类型、状态、耗时、
  返回/影响行数、事务来源及批量 CHUNK/SUMMARY/RECOVERY 事件，日志故障不能改变数据库结果。
- 筛选语义：上层可分别关闭行数、耗时和三类批量事件，也可配置慢 SQL 阈值。阈值只过滤成功的快 SQL，
  失败、取消和 UNKNOWN 始终保留，避免为了降日志量丢失故障证据。
- 接入边界：core/rdb 只提供不可变配置、observer 和日志出口契约，不读取 YAML 或依赖日志框架。上层统一使用
  `flying-orm.sql-log` 映射同一份配置语义。
- V2 演进：常规使用改由上层 `com.flying.orm.sql` 日志级别直接控制，不再要求维护 `full-sql`、`parameters` 和
  长度类 YAML 开关；原有框架无关 observer 继续作为底层观测能力，详见 R-031。

## R-031 V2.0.0 JDBC/R2DBC 双执行内核与统一接入

- 状态：V2 代码能力和发布门禁已完成；MySQL、PostgreSQL、Oracle、SQL Server 已完成真实数据库认证
- 来源：用户要求同时提供真正的 JDBC 与 R2DBC 执行内核，保持所有业务入口统一，并能在两者之间低成本切换。
- 目标：JDBC 和 R2DBC 共享动态表单、Repository、DatabaseOperator、DDL/DML、条件 AST、Scope、SQL 渲染、方言、
  codec、参数顺序、批量结果、执行保护、错误和观测；同步入口使用原生 JDBC，响应式入口继续使用真正非阻塞的 R2DBC。
- 执行边界：项目不提供 R2DBC 阻塞同步桥接。没有 `DataSource` 就不装配同步能力，没有 `ConnectionFactory` 就不装配
  响应式能力；需要双轨时同时提供两种连接能力，不允许任一执行方式自动退回另一条链路。
- 易用性：上层只提供 `DataSource` 或 `ConnectionFactory`，业务代码不手工创建 executor、renderer、dialect 或客户端。
  显式 `flying-orm.dialect` 优先，缺省时从连接 metadata 自动识别，无法可靠确定时启动失败。
- 事务：上层事务管理器是唯一控制者。外部事务中所有操作复用事务连接，flying-orm 不重复 begin、commit、rollback
  或 close；`ATOMIC` 先返回 `ENLISTED`，`INDEPENDENT` 在执行 SQL 前拒绝。无外部事务时仍保留明确的内部事务能力。
- 日志：常规使用由上层 `com.flying.orm.sql` 日志级别控制。DEBUG 自动输出安全 SQL、参数、耗时、行数、事务来源和
  JDBC/R2DBC 执行方式；慢 SQL 及故障按 WARN/ERROR 保留证据。脱敏、截断和总长度限制使用内建安全默认值。
- 框架边界：core/rdb 不依赖 Spring，也不新增 starter；纯 Java 和所有 Java 容器使用同一框架无关接入契约，Spring
  Boot 只作为平级 `flying-orm-example` 中的首个完整验证示例。
- 详细规划：`docs/v2.0.0-roadmap.md`。

## R-032 V2.0.0 实体注解、主键生成和字段策略补齐

- 状态：V2 自有实体注解、AUTO 生成键回写、ASSIGN_ID、ASSIGN_UUID、@EnumValue、FieldFill、FieldStrategy 和 OrderBy 已完成
- 来源：用户明确要求 V2 不使用 Jakarta Persistence 实体注解，改用 flying-orm 自有注解，并让熟悉 MyBatis-Plus 的
  用户可以凭熟悉的名称和常用语义直接上手。
- 注解入口：`com.flying.orm.core.annotation` 提供 `@TableName`、`@TableId`、`@TableField`、`@Version`、
  `@TableLogic`、`@EnumValue` 和 `@KeySequence`；V2 移除 `JakartaAnnotationReader` 和 Jakarta 实体注解读取路径。
- 说明：V2 不读取 Jakarta Persistence 的实体映射或生命周期注解；实体生命周期扩展使用 flying-orm 自己的生命周期契约。
- 主键策略：`IdType` 提供 `NONE`、`AUTO`、`INPUT`、`ASSIGN_ID`、`ASSIGN_UUID`。`AUTO` 必须省略自增列、读取并
  返回生成键；可变实体允许安全回填，不可变实体通过插入结果取得。
- 雪花边界：`ASSIGN_ID` 使用框架无关 `IdGenerator` SPI；worker/node id 必须可靠配置，节点冲突风险和不安全时钟
  回拨必须 fail-fast。没有显式或全局配置时不偷偷生成主键。
- 字段策略：`TableField.exist/select/fill/insertStrategy/updateStrategy` 统一控制非表字段、默认查询排除、填充和写入
  空值语义；单条与批量必须保持一致，批量列布局分组和计划缓存必须有界。填充值来自框架无关 provider，前端不能借
  投影重新放开受保护字段，最终仍受 FieldScope 约束。
- 枚举：`@EnumValue` 指定数据库值；元数据解析时拒绝 null、重复值和不支持类型。
- 默认排序：增加字段级 `@OrderBy(asc, sort)`，只引用当前映射字段；显式类型安全 DSL 排序优先，不接受原始 SQL。
- 详细规划：`docs/v2.0.0-roadmap.md` 第 3.3 节和阶段 4。

## R-033 V2.0.0 启动期实体扫描与 Schema 安全同步

- 状态：V2.0.0 阶段 4 已完成，实体集合接入和 Schema 同步编排已实现
- 目标：服务启动时可扫描 flying-orm 实体注解，对表、字段、主键和索引生成结构差异，并按显式策略校验或同步。
- 框架边界：能力位于 core/rdb 的框架无关契约中；Spring Boot 示例从 `flying-orm.schema` 读取配置，其他 Java 生态和
  纯 Java 使用同一类型化配置对象，不把 Spring 放入主项目。
- 策略：`OFF` 默认关闭；`VALIDATE` 只校验；`SAFE_UPDATE` 只执行新建和兼容新增；`FULL_UPDATE` 才允许破坏性变化，
  且必须经过现有 DDL 风险审核和批准指纹。
- 复用：自动同步只负责编排，继续复用 DynamicForm、Schema diff、方言渲染、DDL 计划、结构化结果和精确缓存失效。
- 兼容边界：现有 DDL/DML、Repository、FormClient 和 DatabaseOperator 的调用入口不变。
- 详细规划：`docs/v2.0.0-roadmap.md` 第 3.4 节和阶段 4。

## R-034 V2.0.0 最终审查加固

- 状态：已完成。
- 目标：在不新增功能、不改变公共 API、SQL、方言和正常事务语义的前提下，收紧批量参数数组所有权、回调异常事务收尾和公开迁移错误脱敏。
- 当前进展：JDBC/R2DBC 均在接收每行时保存独立参数快照；生成键等事务内回调异常不能绕过回滚；回滚未确认继续报告 `UNKNOWN` 并隔离连接；迁移补偿、Schema 重命名、SQL 模板注册及 SQL Server 注释目标错误不再回显无界原始输入。
- 决策记录：`docs/requirements/2026-08-08-v2-final-review-hardening.md`。

## R-035 V2.0.0 轻量 JOIN 与显式受保护字段

- 状态：代码主链路与 H2 JDBC/R2DBC 契约已实现；四种生产数据库的新能力认证已完成。
- JOIN：DynamicForm 和实体 Lambda 提供 `join`、`leftJoin`、`rightJoin`，由框架生成内部别名；结果别名只用于
  解决投影重名和 DTO 映射。每个源继续应用 Scope、逻辑删除和字段保护。
- 加密：只有 `@EncryptedField` 或 DynamicForm `encrypted(...)` 显式声明的字段才加密。上层只提供 current +
  readable 的 32 字节版本化主密钥，flying-orm 不读取部署配置、不对接 KMS/Vault/HSM。
- 搜索：EXACT、固定长度 SUFFIX 使用 HMAC 盲索引；CONTAINS 使用 Unicode trigram 辅助表和解密复核。侧索引与
  业务写入共享 JDBC/R2DBC 事务，候选超过 1000 稳定失败。
- 脱敏：`@MaskedField` 或 DynamicForm `masked(...)` 可声明任意业务字段；查询支持 declared、强制 masked 和可信
  `showSensitive`，但完整展示不会放宽日志、异常和观测脱敏。
- 轮换：`ProtectedFieldReprotection` 识别旧版本密文，上层以稳定游标调度并通过普通安全更新写回；Schema 同步不
  静默转换历史明文。
- 设计与用法：`docs/join-and-protected-fields.md`、`docs/superpowers/specs/2026-08-09-lightweight-join-design.md`、
  `docs/superpowers/specs/2026-08-09-protected-fields-design.md`。
