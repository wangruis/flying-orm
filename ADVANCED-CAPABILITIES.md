# flying-orm 高级能力

本文面向已经掌握 [README](README.md) 主路径和 [其他能力](CAPABILITIES.md) 的使用者，说明复杂查询、运行时接入、
事务、超时、观测、扩展和性能治理。高级能力是按需使用的现有入口，不是普通 CRUD 的必选配置，也不是第二套 ORM。

## 何时需要高级能力

遇到以下场景时再阅读对应章节：

- DynamicForm 和 FormClient 无法自然表达数据库专有查询。
- 应用需要接入现有 JDBC/R2DBC 连接池或外部事务管理器。
- 需要调整执行兜底、清理、LOB 或批量边界。
- 需要统一 SQL 日志、慢 SQL、错误分类和批量观测。
- 需要扩展方言、codec、JSON、数组或向量能力。
- 需要基于真实工作负载调整 fetchSize、批大小和连接池。

如果 FormClient 已能清楚表达需求，应优先保留主路径，不要为了接近手写 SQL 而提前进入高级入口。

## DatabaseOperator 链式 DML

### 适用场景

`DatabaseOperator` 适合需要接近 SQL 的链式表达、实体 Lambda 条件或数据库操作编排，同时仍希望复用参数绑定、方言、
执行保护和事务上下文的可信后端代码。

### 最小示例

```java
DatabaseOperator operator = clients.operator();

Flux<DynamicRow> rows = operator.dml()
        .query()
        .select("id", "name")
        .from("users")
        .where(where -> where.is("enabled", true)
                             .term("age", ">", 18))
        .fetchMap();
```

同步代码使用 `clients.syncOperator()`。实体 Lambda DML 继续使用 flying-orm 自有实体元数据，不读取 Jakarta Persistence。

### 边界

- Operator 是高级表达门面，不替代 DynamicForm/FormClient 主路径。
- 标识符通过结构化 DSL 校验，业务值全部参数化绑定。
- Scope、逻辑删除、字段权限和乐观锁仍由统一规划器执行。
- 不提供可绕过方言、事务或错误分类的裸 Statement 入口。

## 注册 SQL 模板

### 适用场景

复杂联表、CTE、聚合、窗口函数或数据库专有查询适合在启动阶段注册为可信 SQL 模板。业务调用只使用稳定模板 ID，
不在每次请求中传递 SQL 正文。

### 最小示例

```java
SqlTemplateRegistry templates = SqlTemplateRegistry.builder()
        .register(SqlTemplate.query(
                "monthly-sales",
                """
                select department_id, sum(amount) total
                from sales
                where tenant_id = :tenantId and created_at >= :startTime
                group by department_id
                order by total desc
                """,
                Set.of()),
                Set.of("tenantId"))
        .build();

FlyingOrmClients clients = FlyingOrmClients.builder(connectionFactory)
        .sqlTemplates(templates)
        .sqlTemplateParameterProvider((templateId, names) -> Mono.deferContextual(context ->
                Mono.just(Map.of("tenantId", context.get("tenantId")))))
        .build();

Flux<DynamicRow> report = clients.operator()
        .sqlTemplate("monthly-sales")
        .bind("startTime", startTime)
        .options(SqlExecutionOptions.maxRows(10_000)
                .withTimeout(Duration.ofSeconds(5)))
        .query();
```

### 边界

- 模板只用于查询，SQL 正文只能来自可信启动代码或配置。
- 服务端安全参数由统一提供器生成，普通 `bind(...)` 不能覆盖。
- 复杂 SQL 的分页、游标和统计应显式注册，不由 ORM 猜测或重写。
- 模板仍使用命名参数、执行保护、事务参与、观测和错误分类。

## 受控原生 SQL

### 适用场景

临时后台查询或不值得注册模板的数据库专有 SQL 可以使用受控原生入口。名称中的 `unsafe` 表示 SQL 结构由可信代码
负责审核，不表示业务值可以拼接。

### 最小示例

```java
Flux<DynamicRow> rows = clients.operator()
        .unsafeNativeSql(
                "select id, name from users where tenant_id = :tenant and enabled = :enabled")
        .bind("tenant", tenantId)
        .bind("enabled", true)
        .options(SqlExecutionOptions.maxRows(1000)
                .withTimeout(Duration.ofSeconds(3)))
        .query();
```

### 边界

- 业务值必须使用命名参数；缺少参数、多余参数和多语句会在执行前拒绝。
- 原生 SQL 不自动推断 DynamicForm Scope；调用方必须使用可信 SQL 结构显式表达安全条件。
- 执行仍复用方言参数标记、连接与事务、结果映射、超时、观测和错误分类。
- 高频或安全关键查询应优先注册模板，避免 SQL 结构散落在业务代码中。

## JDBC、R2DBC 与连接池

### 运行方式

- 同步 API 使用原生 JDBC，不经过 Reactor 或 R2DBC 阻塞桥。
- 响应式 API 使用原生 R2DBC/Reactor，不用线程池包装 JDBC 冒充非阻塞。
- 应用可以只提供 `DataSource`、只提供 `ConnectionFactory`，或同时提供两者。

```java
FlyingOrmClients jdbc = FlyingOrmClients.builder(dataSource).build();
FlyingOrmClients r2dbc = FlyingOrmClients.builder(connectionFactory).build();
FlyingOrmClients both = FlyingOrmClients.builder(dataSource, connectionFactory).build();
```

### 所有权边界

- flying-orm 使用上层提供的 JDBC `DataSource` 或 R2DBC `ConnectionFactory`，不实现连接池。
- JDBC 连接池通常由 HikariCP 等上层组件提供；R2DBC 连接池通常由 `r2dbc-pool` 或容器提供。
- ORM 负责取得和归还本次操作使用的连接，并在取消、污染或结果不确定时请求失效。
- 应用或容器负责连接池大小、凭据、健康检查、路由和生命周期。
- flying-orm 不关闭自己不拥有的数据源或连接池。

详细接入方式见 [V2.0.0 纯 Java 接入示例](docs/v2.0.0-java-integration.md)。

## 外部事务参与

### 基本规则

一次数据库调用只能有一个事务控制者：

- 检测到外部事务时，普通 CRUD、原生 SQL、Repository、FormClient 和 ATOMIC 批量复用外部事务连接。
- flying-orm 不在外部事务中再次 begin、commit、rollback 或关闭外部连接。
- 没有外部事务时，普通操作遵循连接默认提交语义；ATOMIC 批量由 flying-orm 自己管理事务。
- INDEPENDENT 与外部整批事务冲突，会在执行 SQL 前拒绝。
- 动态数据源必须在事务开始前完成路由，事务期间不能切换物理数据库。

### 超时协作

外部事务和 flying-orm 本地执行保护同时存在时，先到期的边界终止当前工作。本地超时不会延长外部事务；希望完全
由外部事务控制 SQL 执行时间时，可以把本地执行 `timeout` 配置为 `Duration.ZERO`，但仍应保留必要的清理与容量保护。

外部事务提交或回滚前，批量结果只能报告已经加入和执行，不能提前伪装成最终 COMMITTED。

详细语义见 [V2.0.0 JDBC/R2DBC 双执行契约](docs/v2.0.0-execution-contract.md)。

## 超时、取消与资源清理

`SqlExecutionOptions` 将不同生命周期边界分开配置：

| 选项 | 默认值 | `Duration.ZERO` 的含义 |
| --- | ---: | --- |
| `timeout` | 30 秒 | 不设置本地 SQL 总执行截止时间 |
| `cleanupTimeout` | 5 秒 | 不限制结果确定后的资源清理等待时间 |
| `fetchSize` | 0 | 不覆盖驱动默认抓取策略 |

连接排队、获取超时和健康检查不是 ORM 执行选项，由 DataSource、r2dbc-pool 或其他上层连接池配置。

```java
SqlExecutionOptions options = SqlExecutionOptions.safeDefaults()
        .withTimeout(Duration.ofSeconds(3))
        .withCleanupTimeout(Duration.ofSeconds(2))
        .withFetchSize(256);
```

### 选择规则

- `SqlExecutionOptions.timeout(Duration.ZERO)` 只取消 SQL 总执行截止，仍保留默认行数、结果内存、LOB 和
  清理保护。
- `SqlExecutionOptions.unlimited()` 会同时解除超时和容量保护，只能用于已经有等价外部治理的受控基础设施入口。
- 查询取消直接向驱动传播；ORM 只释放已登记的 LOB，并请求连接池淘汰状态不可证明干净的自有连接。
- 清理超时保护的是资源生命周期，不得把已经确认的 COMMITTED 或 ROLLED_BACK 改写成 UNKNOWN。
- JDBC/R2DBC 的连接排队、获取超时和健康检查均由上层连接池管理；ORM 不再叠加第二套获取定时器。

批量默认总超时为 5 分钟，并继续受最大输入行数、chunk、内存和并发限制；连接等待服从连接池。批量的
`Duration.ZERO` 只应在上层已经提供可靠边界时显式使用。

## SQL 观测与错误分类

### 观测入口

`SqlExecutionObserver` 观察普通 SQL，`BatchExecutionObserver` 观察批量分片和汇总，`SchemaMigrationObserver` 观察
Schema 计划执行。观察者只接收有界、脱敏的结构化信息，不拥有连接或事务。

通过客户端 Builder 可以一次装配观察者；也可以使用内置安全 SQL 日志观察者，由上层日志级别决定是否输出 DEBUG。

### 日志边界

- DEBUG 可以记录参数化 SQL、受限参数摘要、耗时、行数、事务来源和执行方式。
- 慢 SQL、超时、取消、连接异常和 UNKNOWN 至少进入 WARN。
- 密钥、明文敏感字段、LOB、二进制、完整连接串和无界驱动消息不会进入日志。
- 普通观察者异常被隔离；异常图中的 JVM 致命错误保持原对象传播。

### 错误分类

上层通过异常类型和 `OrmErrors.report(...)` 读取稳定的 category、code、resource、path 和 field。不要依赖不同数据库
驱动的原始异常文本完成业务分支。

- [错误码手册](docs/error-code-reference.md)
- [公共 API 稳定边界](docs/public-api-stability.md)

## 缓存与精确失效

flying-orm 使用有界缓存保存元数据、SQL 结构计划、条件计划和实体映射。缓存策略由不可变 `OrmCachePolicy` 配置，
不会使用无上限全局 Map 保存业务维度数据。

```java
FlyingOrmClients clients = FlyingOrmClients.builder(connectionFactory)
        .cachePolicy(OrmCachePolicy.safeDefaults())
        .build();

OrmCacheSnapshot sql = clients.sqlPlanCacheSnapshot();
OrmCacheSnapshot conditions = clients.conditionPlanCacheSnapshot();
```

Schema 迁移成功后会精确失效主表、受保护字段侧索引表和相关元数据读取缓存。应用不应依赖长时间自然过期掩盖结构变化。

## 方言、codec、数组、JSON 与向量

### 方言

方言遵循“显式配置优先、缺省自动识别”。无法识别、显式值与物理数据库冲突，或同一客户端图的 JDBC/R2DBC 方言
不一致时启动失败。动态数据源中的全部物理库必须使用同一最终方言。

### codec

业务值转换通过不可变 `ValueCodecRegistry` 扩展。同一注册表应同时服务条件编译、动态表单写入、批量、实体映射和结果
读取，不能在每条路径各自维护转换规则。

### 数组、JSON 与向量

- PostgreSQL 原生数组、数组条件和 pgvector 使用显式方言能力，不在其他数据库静默模拟。
- JSON 写入接受经过校验的 Map/List；JSON 条件使用注册的固定 SQL 模板和结构化路径。
- 向量查询要求显式维度、距离函数、候选数和执行保护；不绕过表单 Scope 与逻辑删除。
- 数据库不支持某项扩展时应在 SQL 执行前失败，不降级成语义不同的查询。

## 性能调优与验证

性能调优必须从真实工作负载开始，不能只依据某次偶然跑分修改安全或事务语义。

### fetchSize

默认 `fetchSize=0`，即不覆盖驱动默认策略。只有大结果流或已经确认驱动默认抓取不适合时才设置正数。正数只是给驱动
的抓取提示，不保证每个数据库和驱动采用相同实现；普通短查询盲目增大 fetchSize 可能增加预取、内存或尾延迟。

### 批量与连接池

- chunkSize 决定每次批量语句规模，应结合单行宽度、驱动限制和事务时长选择。
- INDEPENDENT 并发数不能超过连接池和数据库能够稳定承受的并发。
- 连接池大小不是越大越快，应结合数据库连接上限、CPU、磁盘和目标延迟测量。
- JDBC 与 R2DBC 的线程、调度和背压模型不同，不能只比较单连接低并发吞吐后推导海量并发结论。

### 验证证据

正式性能结论至少记录数据库与驱动版本、硬件、连接池、数据规模、预热、轮次中位数、吞吐、P95、P99、错误数和连接
泄漏。性能结果与正确性、事务、取消和资源释放认证必须同时成立。

- [性能基线计划](docs/performance-baseline-plan.md)
- [性能比较模板](docs/performance-comparison-template.md)
- [真实数据库认证方法](docs/real-database-certification.md)

## 公共 API 与数据库认证

当前源码基线为 2.0.0。公开 API 的正式边界、历史稳定入口、内部实现类型和 Javadoc 门禁见
[公共 API 稳定边界](docs/public-api-stability.md)。

代码中存在方言不等于已经完成真实数据库认证。MySQL、PostgreSQL、Oracle 和 SQL Server 的版本边界、认证场景和
当前证据分别见：

- [数据库支持矩阵](docs/database-support-matrix.md)
- [真实数据库认证方法](docs/real-database-certification.md)
- [已知限制](docs/known-limitations.md)

## 进一步阅读

- [其他能力](CAPABILITIES.md)
- [V2.0.0 纯 Java 接入示例](docs/v2.0.0-java-integration.md)
- [V2.0.0 JDBC/R2DBC 双执行契约](docs/v2.0.0-execution-contract.md)
- [错误码手册](docs/error-code-reference.md)
- [数据库支持矩阵](docs/database-support-matrix.md)
- [真实数据库认证方法](docs/real-database-certification.md)
