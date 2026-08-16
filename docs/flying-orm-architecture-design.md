# flying-orm 架构设计

## 1. 项目定义

flying-orm 是一个独立、全新的 ORM。它首先要做到简单、易用、稳定和安全，在这些前提下追求高性能、
低延迟、高并发和高吞吐量。动态表单不是附加能力，而是整个设计的主要使用场景。

项目不承接其他 ORM 的历史 API，也不假设存在需要迁移的上层业务。公开入口围绕 flying-orm 自己的概念设计，
内部实现可以持续优化，但不能用牺牲安全边界、事务事实或可维护性换取表面上的跑分。

## 2. 模块边界

主项目保持 2 个生产模块和 2 个验证模块：

```text
flying-orm
  flying-orm-core       数据库无关模型、条件、Scope、codec 和 SQL 渲染契约
  flying-orm-rdb        JDBC/R2DBC 执行、方言、动态表单、Repository 和 DDL/DML
  flying-orm-testkit    方言、事务、故障和真实数据库契约测试
  flying-orm-benchmark  JMH 与真实数据库性能运行器
```

`core` 和 `rdb` 不依赖 Spring、Spring Boot、日志框架或其他应用容器。框架接入代码放在上层系统或平级示例项目，
只把连接、事务参与者、配置和 observer 适配到主项目已有契约。

## 3. 运行主链

结构化查询和写入统一经过下面的路径：

```text
调用入口
  -> 条件 AST / 写入模型 / Scope
  -> 安全校验与值规范化
  -> SQL 计划与方言渲染
  -> SqlRequest（SQL + 有序参数）
  -> SyncSqlExecutor（JDBC）/ ReactiveSqlExecutor（R2DBC）
  -> DynamicRow / 实体 / 影响行数 / 批量结果
```

字段名、operator、JSON path、排序和分页都先进入结构化模型；普通参数只进入绑定参数，不直接拼进 SQL。
原生 SQL 是明确的高级入口，同样使用命名参数、执行保护、事务、观测和错误分类。

## 4. 统一装配入口

纯 Java 使用 `FlyingOrmBootstrap` 和 `FlyingOrmEnvironment` 创建一次运行环境。应用提供 `DataSource`、
`ConnectionFactory` 或同时提供两者，启动器统一处理：

- 显式方言优先；未配置时从 JDBC/R2DBC metadata 自动识别；无法确定或双内核识别冲突时直接启动失败。
- `SqlRenderer`、同步/响应式 Executor、FormClient、Repository、DatabaseOperator 和 Schema 客户端共享同一方言。
- 默认执行保护、批量内存限制、Scope 提供者和 observer 只配置一次，后续入口不能绕开。
- 构建后的客户端和无状态协作对象可以并发复用；单次 DSL builder 不跨请求共享。

应用框架只需要把自己的配置对象转换成 `FlyingOrmConfiguration`，把当前连接和事务参与能力转换成
`FlyingOrmEnvironment`。规范配置根名为 `flying-orm`，但主项目本身不读取 YAML。

## 5. 动态表单

`DynamicForm` 描述运行时表、字段、主键、索引、租户、逻辑删除和乐观锁信息。动态表单主链覆盖：

- 建表、改表、迁移计划和缓存失效。
- 查询、分页、计数、插入、更新、逻辑删除、物理删除和 upsert。
- 单行与流式批量写入。
- Map 风格数据、紧凑 `DynamicRow` 和显式实体映射。

公开 Map 结果保持 Java 使用习惯，底层行数据使用列布局与值数组，避免每行复制一份 HashMap 桶结构。
只有调用方明确要求可变 Map 时才创建新的 Map。

## 6. 条件与 Scope 安全

条件系统通过 term id 扩展，不局限于 `=`、`<>`、`like` 等固定操作。前端结构化条件只表达字段、operator、
值和嵌套关系；字段白名单、operator 白名单、类型、深度和路径都会在生成 SQL 前校验。

`TenantScope`、`DataScope`、`FieldScope`、`TimeScope`、逻辑删除和调用方条件最终使用安全 AND 合并。
前端条件不能伪造租户字段、删除标记或受保护字段，也不能通过 OR 改写服务端范围。

严格 `where(...)` 保留调用者明确表达的 null 语义；可选筛选使用 `whereIfPresent(...)`，并按 operator 规则处理
null、空字符串和去掉前后空格后的空值。

## 7. 执行内核

V2.0.0 同时提供真正的 `DataSource + java.sql.Connection` 同步执行内核和原生 R2DBC 响应式执行内核。JDBC 和 R2DBC
共享条件 AST、SQL 渲染、方言、参数顺序、Scope、codec、执行保护、错误、批量结果和事务状态；同步 JDBC
热路径不经过 Reactor，响应式路径也不会包装 JDBC。业务层继续使用同一套 FormClient、Repository、Operator、
DDL/DML 和安全原生 SQL。项目不提供 R2DBC 阻塞同步桥、兼容开关或自动退回；具体实施与验收边界见
[`v2.0.0-roadmap.md`](v2.0.0-roadmap.md)。

### 7.1 执行职责边界

| 能力 | 真正负责人 | flying-orm 的边界 |
| --- | --- | --- |
| 连接池大小、排队、健康检查、获取超时 | 上层应用和连接池 | 借用、归还；状态污染或结果不确定时通知失效 |
| SQL 执行和协议取消 | JDBC/R2DBC 驱动 | 组合调用或 Publisher，不实现驱动协议 |
| 外部事务 | 上层事务管理器 | 识别并复用连接，不自行提交、回滚或关闭外部连接 |
| 无外部事务的 `ATOMIC` 批量 | flying-orm | 为自己承诺的整批原子性管理内部事务 |
| SQL 总执行超时 | 上层为主 | 提供一个可关闭的兜底截止，不覆盖连接池等待策略 |
| 行数、结果内存和 LOB 上限 | flying-orm | 防止 ORM 物化结果时拖垮应用 |
| SQL、映射、Scope 和安全校验 | flying-orm | 作为 ORM 核心职责统一实现 |

普通 SQL 使用标准 JDBC/R2DBC 调用链；批量、LOB 和 ORM 自有事务才进入对应的专用保护。不得为了兜底能力在每个
阶段重复创建超时、取消或连接状态机，也不得用后台 drain 模拟驱动消费协议。

## 8. 事务一致性

一次调用只有一个事务控制者。

### 8.1 没有外部事务

- 普通 CRUD 使用自动提交连接。
- `ATOMIC` 批量由 flying-orm 开启、提交或回滚事务，默认整批原子。
- `INDEPENDENT` 必须显式开启，每个分片独立提交并返回可解释的分片结果。

### 8.2 存在外部事务

- CRUD、原生 SQL、FormClient、Repository、Schema 安全操作和 `ATOMIC` 共用上层绑定连接。
- flying-orm 不重复 begin、commit、rollback 或 close 外部连接。
- `ATOMIC` 先返回 `ENLISTED`，外部事务真正结束后再通过完成回调报告 `COMMITTED`、`ROLLED_BACK` 或 `UNKNOWN`。
- `INDEPENDENT` 无法满足“分片独立提交”又服从同一个外部事务，因此在获取连接、订阅输入和执行 SQL 前拒绝。
- 动态数据源必须在事务开始前完成路由；事务期间锁定物理数据库并从主库读取。

外部框架按执行方式把自己的事务生命周期实现成 `JdbcTransactionParticipant` 或 `R2dbcTransactionParticipant`。
主项目只认统一事务事实，不识别
`@Transactional` 或其他框架注解。

## 9. 批量内存与恢复

批量输入使用 Publisher 按分片消费，不先收集整批数据。每个分片只保留固定字段布局、当前参数和有界结果摘要。
上游可以复用同一个参数数组，因此 JDBC/R2DBC 都必须在接收每次 `onNext` 时立即复制当前行；分片形成后只冻结列表结构，避免数据串行污染和重复复制。

- 默认 `ATOMIC`，显式 `INDEPENDENT`。
- 批量最大输入行数、分片大小、分片并发和保留结果数都有上限。
- `UNKNOWN` 只表示提交结果无法确认，不会伪装成成功或失败。
- recovery token、幂等查询和恢复结果使用统一模型。
- observer 只接收计数、状态和错误分类，不复制输入 Publisher 或完整参数集合。
- 生成键等事务内回调抛出异常时仍进入与驱动失败相同的事务收尾；回滚未确认时隔离连接并报告 `UNKNOWN`。

## 10. 映射、类型与缓存

实体映射在元数据解析后生成可复用计划，热路径避免重复反射。动态行使用紧凑列布局；record、JavaBean、Map 和
自定义 `RowMapper` 共用 codec 规则。

V2 使用 `com.flying.orm.core.annotation` 下的自有实体注解，不依赖或反射读取 Jakarta Persistence。`@TableName`、
`@TableId`、`@TableField`、`@Version`、`@TableLogic`、`@EnumValue` 和 `@KeySequence` 使用 Java ORM 用户熟悉的名称，
但元数据模型和执行逻辑由 flying-orm 独立实现。`IdType.AUTO` 必须省略自增列并读取数据库生成键；可变 JavaBean 可以
安全回填，不可变实体从插入结果读取。雪花生成器必须显式或通过全局策略启用，并校验 worker/node id 和时钟边界。

实体扫描可在启动期生成 DynamicForm，并复用 Schema diff、DDL 风险审核和缓存失效完成结构校验或同步。默认关闭；
`VALIDATE` 只检查，`SAFE_UPDATE` 只执行安全新增，`FULL_UPDATE` 的删除、类型缩窄和索引重建必须经过批准指纹。
DDL/DML 门面不因自动同步或注解切换而改变。填充 provider、ID generator 和枚举访问计划保持框架无关，并由
JDBC/R2DBC 共享；实体默认排序不接收原始 SQL，继续使用类型安全 DSL。

Caffeine 用于有明确收益且可失效的元数据和计划缓存。每个缓存必须具备：

- 容量上限和淘汰策略。
- 动态改表后的精确失效。
- 命中、未命中、淘汰和负载失败指标出口。
- 不保留批量输入、连接、事务上下文或请求级对象。

## 11. 方言与数据库

统一 `RdbDialect` 负责标识符、分页、DDL、upsert、JSON、Array、Vector、类型映射和能力边界。

- MySQL、PostgreSQL、H2 是主要支持数据库。
- Oracle、SQL Server 使用同一方言与执行契约，并保留版本差异和真实驱动限制说明。
- ANSI 表示 SQL 标准概念，不作为数据库方言或数据库类型暴露。
- OpenGauss 不在当前支持范围。

## 12. 观测与 SQL 日志

执行器发出结构化 SQL、批量和资源清理事件。observer 的异常会被隔离，不能改变数据库结果。

- 日志默认关闭，关闭时不创建完整 SQL 或参数副本。
- 普通指标 observer 不解析事务上下文；只有明确申请事务来源的 observer 才承担这部分成本。
- 开启 SQL/参数展示后统一脱敏、截断并限制总长度，展示文本绝不回流成执行 SQL。
- 错误、取消和 `UNKNOWN` 不受慢 SQL 采样过滤。
- 日志实现和输出框架由上层选择，core/rdb 只提供不可变事件与 sink 契约。

## 13. 类规模与扩展边界

生产类型通常不超过 300 个物理源码行和 20 个可调用方法。达到阈值先检查能否按查询、写入、校验、映射、
事务、生命周期或基础设施职责拆分。公开 API 保持少量稳定门面，内部协作类型优先使用包级可见性。

不能为了行数制造无意义转发类、同步/响应式重复实现、额外热路径对象或更深调用链。testkit 和 benchmark 的
声明式场景表、协议代理或独立运行器按验证用途单独审查，不进入生产运行时。

## 14. 质量与性能门禁

- `quality`：全量测试、覆盖率、SpotBugs、Checkstyle、依赖收敛和公开 API 基线。
- `release-artifacts`：主 JAR、源码包、Javadoc 包和直接依赖分析。
- 真实数据库：功能、事务、故障、取消、连接池恢复和有界并发。
- JMH：条件、渲染、映射、缓存、批量计划、DynamicRow 和执行包装开销。
- 真实库性能：固定参数至少三轮，报告吞吐、P50/P95/P99、错误率、堆和连接池峰值。

性能结论必须带环境、参数和原始结果。单轮最好值、降低事务持久性或隐藏失败都不能用于发布结论。
