# 公共 API 稳定边界

本文同时保存 V1 历史基线和当前 V2.0.0 正式基线。V1 文件是迁移与审计证据，不表示 V2 继续保留已经明确删除的
同步桥或 Jakarta 实体注解读取路径；当前使用方式以 V2 精确基线和 V2 路线图为准。

## V1.0.0 API 基线

`flying-orm-rdb/src/test/resources/api/v1.0.0-public-api.txt` 是 V1.0.0 的公开 API 基线。它从编译后的
`flying-orm-core` 和 `flying-orm-rdb` 字节码提取有效公开类型，以及公开或受保护的构造器、字段、方法、
泛型、继承关系、枚举值和 record 组件。源码排版、注释和包内实现不会造成误报。

V1.0.0 文件在 V2 中只检查“文件存在且非空”，确保迁移审查证据不会丢失。V2 不再提供改写这个历史文件的
系统属性，普通测试和 V2 基线更新都不会碰它：

```shell
mvn -pl flying-orm-rdb -am -Dtest=V1PublicApiBaselineTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

不能只为让测试变绿而改动历史基线。V1 文件已经冻结，新增、删除和签名变化统一记录在 V2 精确快照及迁移说明中。

## V1.0.1 精确基线

`flying-orm-rdb/src/test/resources/api/v1.0.1-public-api.txt` 是 V1.0.1 收口后的历史快照。V2 不再拿它约束
当前签名，也不提供自动更新参数；它和 V1.0.0 文件一起只用于迁移说明、差异审查和历史复核。

V1.0.1 新增的配置、事务参与和 SQL 日志入口都是增量能力，没有删除 V1.0.0 的可达能力。

## V2.0.0 正式基线

`flying-orm-rdb/src/test/resources/api/v2.0.0-public-api.txt` 是当前 V2.0.0 源码公开 API 的精确快照。
V2 已完成对 V1 同步桥和旧实体注解路径的破坏性清理，并纳入轻量 JOIN、实体 Lambda JOIN、受保护字段声明、
密钥环、保护搜索、展示控制和历史密文 reprotect 协作入口。基线确认后不能只为让测试通过而直接改写快照；
后续版本如需调整公开签名，必须先完成 API 设计与迁移审查。

```shell
mvn -pl flying-orm-rdb -am -Dtest=V1PublicApiBaselineTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

普通测试只比较快照，不会自动改写。V1 基线继续保留原样，确保迁移说明可以准确指出删除和替代关系。
公共 API 契约仍由 `V1PublicApiBaselineTest` 检查；当前能力不得暴露包内 renderer、连接、侧索引或事务协作类型。

## V1 历史稳定入口

使用方和适配项目优先依赖这些入口：

- `com.flying.orm.core.condition`：条件 AST、结构化条件输入、安全策略和稳定错误码。
- `com.flying.orm.core.codec`：应用值 codec SPI 和构造后只读的 `ValueCodecRegistry`。
- `com.flying.orm.core.form`：`DynamicForm`、动态字段、租户和逻辑删除定义。
- `com.flying.orm.core.page`、`com.flying.orm.core.scope`：分页与数据范围模型。
- `com.flying.orm.core.sql.render`：`SqlRenderer`、参数化 SQL 请求和业务 term SPI。
- `com.flying.orm.rdb.bootstrap.FlyingOrmClients`：纯 Java 统一组装入口。
- `com.flying.orm.rdb.form`：响应式与同步动态表单客户端。
- `com.flying.orm.rdb.sync`：V1 中桥接到 R2DBC 的同步契约；V2 只保留原生 JDBC 同步执行契约。
- `com.flying.orm.rdb.mapping`：实体元数据、映射事件和 `RowMapper` 扩展入口；实体反射解析与取值计划由实例级注册表在内部管理。
- `com.flying.orm.rdb.batch`、`execution`、`observation`、`exception`：统一的 `BatchWriteRequest`/批量结果、执行保护、观测和错误模型。
- `com.flying.orm.rdb.operator`、`repository`、`schema`：链式操作、薄 Repository 和动态 DDL。
- `com.flying.orm.rdb.migration`：参数化数据迁移、补偿执行结果和明确的回滚失败状态。
- `com.flying.orm.rdb.reactive.ReactiveSqlExecutor` 与 `R2dbcSqlExecutor`：响应式执行契约和默认实现。

稳定表示：1.x 小版本不随意删除类型、修改方法含义或改变默认安全策略。确需替换时，先提供可迁移的新入口并保留一段兼容期。

## 扩展入口

方言、JSON、PostgreSQL Array、codec、元数据 reader 和结构化条件 resolver 允许扩展。SPI 契约会尽量稳定，但数据库驱动差异可能要求增加新方法；新增方法优先提供默认实现，避免已有扩展立即失效。

## 不建议直接依赖

以下对象即使当前是 `public`，也属于实现协作类，不应成为使用方代码依赖：

- `R2dbcBatchWriter`、默认 options/observer 包装器等执行器内部组合类已经收回包内可见性。
- 各数据库 `*ReactiveFormMetadataReader` 的具体实现已经收回包内；上层使用 `ReactiveFormMetadataReaders` 工厂。
- SQL 渲染过程中的 builder 实现类、缓存 key、批量内部异常、`MappingPlan` 和映射计划内部 writer。
- 条件值规范化器、SQL 结构计划、实体反射解析器和内置分页实现位于 `internal` 或包级实现中，不属于 1.x 兼容承诺。

其余暂时无法缩小的实现协作类型以本文作为兼容边界，不承诺其构造器和实现细节稳定。

## V2 同步入口

同步调用使用 `SyncSqlExecutor`、`SyncFormClient`、`SyncFormRepository` 和 `SyncDatabaseOperator`，底层直接执行
原生 JDBC。响应式入口继续使用 `ReactiveSqlExecutor` 和 R2DBC；两条链只共享渲染、参数、Scope、codec、
错误、事务结果和观测契约，不共享连接，也不会互相等待或自动退回。

## 默认行为

- 客户端组装统一走 `FlyingOrmBootstrap` 与 `FlyingOrmEnvironment`；只提供 `DataSource` 时装配 JDBC，
  只提供 `ConnectionFactory` 时装配 R2DBC，同时提供时装配双内核。
- 实体 Repository 优先从 `clients.repository(Entity.class)` 或 `syncRepository(Entity.class)` 创建，不手工解析实体元数据和 DynamicForm。
- 结构 SQL 渲染器必须显式传 `RdbDialect` 或 `SchemaDialect`，没有隐含默认方言，也不同时开放构造器和等价工厂。
- FormClient 接收已经组装好的 `FormDataSqlRenderer`；表单渲染器必须拿到完整 `RdbDialect`，不会偷偷套用 H2 的分页或 upsert 规则。
- 元数据缓存只公开 `ReactiveFormMetadataCache` 能力接口和 `ReactiveFormMetadataReaders.cached(...)` 工厂，具体 Caffeine 包装类留在包内。
- Repository 只保留默认实体映射和自定义映射两个工厂；默认执行保护先配置到 FormClient，再由 Repository 原样复用。
- 数据迁移未显式传执行选项时继承 executor 的默认保护；带 Scope 或乐观锁的便捷重载也不能把默认保护改成 `unlimited`。
- 数据迁移补偿失败通过 `ROLLBACK_FAILED` 和稳定的 `rollbackFailure` 说明表达；公开结果不得回显底层驱动异常原文。
- 批量只保留 `BatchWriteRequest + writeBatch(...)` 契约；List 输入只是便捷适配，不会走另一套执行和结果语义。
- 批量 Publisher 的参数数组在每次接收时保存快照；调用方可以复用数组，但不能在发送后继续影响已接收行。
- 批量默认 `ATOMIC`；`INDEPENDENT` 必须显式开启。
- 严格 `where(...)` 不忽略空值；可选条件使用 `whereIfPresent(...)`。
- 前端结构化条件只接受字段、operator 和参数值，不接受 SQL。
- 租户、数据、字段、时间范围和逻辑删除条件与调用方 where 安全 AND；重复设置默认 `DataScope` 也只能继续收紧，不能覆盖已有范围。
- 显式方言优先；未配置时从 JDBC/R2DBC metadata 识别，双内核识别冲突或无法识别时启动失败。
- observer 默认不申请参数值或事务来源；只有覆盖对应能力开关的 observer 才承担参数展示或事务上下文解析成本。

这些默认值属于安全契约，1.x 不会在小版本中静默改成更宽松的行为。

错误处理时，调用方分支优先使用 `OrmErrorReport`、`RdbErrorKind` 和具体执行保护异常；指标与批量状态汇总使用 `SqlExecutionResultKind`。不要按异常消息或 observer 文本做分支。

## Javadoc 发布门禁

上述稳定包已经提供包级 Javadoc，先说明职责、线程安全边界和不能绕过的安全规则，再由具体类型和方法说明调用细节。
`release-artifacts` Profile 会生成源码包和 Javadoc 包；坏链接、坏 HTML 或其他 Javadoc 警告会直接中止发布构建。

历史公开方法的缺失注释继续分批补齐，暂时不把 `missing` 检查设为全局阻断项。这样不会为了消除告警一次性写入大量空泛注释，
但每次新增或修改公共 API 都必须写清参数、返回值、默认行为、线程安全和失败边界。

`operator` 稳定包的响应式与同步链式 API 已完成逐方法校对：构建器是单次调用对象，`DatabaseOperator` 与
`SyncDatabaseOperator` 可以共享；同步入口直接走 JDBC；显式 Scope 只能继续收紧默认范围；DDL 的 `plan()` 不执行 SQL，只有成功的
`commit` 才会让对应元数据缓存失效。没有业务含义且没有调用方的 builder getter 不作为 V1 公共契约保留。

`form`、`repository`、`schema`、`mapping` 已完成第二轮边界校对：响应式入口订阅前不获取连接，同步入口
直接获取 JDBC 连接并按所有权关闭；Repository 和客户端构造后可共享，DDL 按顺序执行但不承诺跨数据库原子回滚；
内部反射映射计划不再公开，业务通过 `RowMapper` 和实体元数据入口扩展。
