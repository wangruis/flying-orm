# flying-orm V2.0.0 能力矩阵

这份矩阵按当前 V2.0.0 代码说明动态表单 ORM 的实际覆盖范围。V2.0.0 使用真正独立的 JDBC 同步内核和
R2DBC 响应式内核，两者共享 ORM 规则，但不互相桥接。

状态说明：

- `已覆盖首版`：flying-orm 已有可用能力，但后面还要继续打磨性能和兼容性。
- `部分覆盖`：主链路有了，细节、数据库实测或高级场景还没完整。
- `后续实现`：目标内能力，但还没开始或只在计划里。
- `不规划`：当前目标不需要，明确不做。

## 总览

| 能力 | 相关概念 | flying-orm 当前状态 | 目标优先级 | 备注 |
| --- | --- | --- | --- | --- |
| 核心 DSL | 条件与 SQL AST | 已覆盖主链路 | P0 | 已有条件 DSL、参数条件、结构化前端条件、条件安全校验和可扩展 term；参数统一进入 `ValueCodecRegistry.write()`。 |
| 动态表/字段元数据 | `DynamicForm` / `DynamicField` | 已覆盖主链路 | P0 | `DynamicForm` / `DynamicField`、方言类型映射、动态行解码和五库元数据读取已接入；当前认证状态见数据库支持矩阵。 |
| 动态表结构维护 DDL | `DatabaseOperator.ddl()` / Schema Migration | 已覆盖 V2 安全主链路 | P0 | 已有建表、保守 createOrAlter、缺失字段和索引补齐、字段注释、显式重命名、类型变更、删列、删索引和索引重建；危险操作必须显式放开并通过审核指纹。主键和外键变更返回结构化人工步骤，不伪造跨数据库通用自动执行；V2 认证覆盖当前支持矩阵，在线无锁 DDL 仍不在承诺范围。 |
| 查询条件构建 | Condition AST / TermHandler | 已覆盖首版 | P0 | 已支持结构化条件、安全策略、OR、参数包、自定义 term，如 `user-in-org`；内置 `like-ignore-case` / `not-like-ignore-case` 以 `lower(字段) ... lower(?)` 为 H2、MySQL、PostgreSQL、Oracle、SQL Server 提供统一的参数化忽略大小写 LIKE；条件值统一按无值、标量、集合、区间和显式单值或集合处理，未知 term 不再猜测集合语义，参数条件包同时携带 term 元数据；严格条件拒绝空值，可选条件显式使用 `whereIfPresent(...)`，字符串和集合项会先清理，区间错误带稳定路径；Reactive/Sync 表单客户端可配置结构化条件 resolver，支持多个 customizer 组合，并已有常用预设工厂。 |
| 轻量多表查询 | `join/leftJoin/rightJoin` | 已覆盖 V2.0.0 主链路 | P1 | DynamicForm 与实体 Lambda 共享 JOIN AST、Scope、逻辑删除、方言、JDBC/R2DBC 和扁平 DynamicRow；覆盖显式投影、排序、offset/page/count，首版不提供 cursor page、FULL/CROSS、自连接和写 JOIN。 |
| 字段加密与脱敏 | `@EncryptedField` / `@MaskedField` / DynamicForm | 已覆盖 V2.0.0 主链路 | P1 | 只有显式声明字段启用 AES-GCM、EXACT/SUFFIX/CONTAINS 保护搜索和通用 masking；上层提供版本化主密钥环，多版本唯一字段或受保护 R2DBC 回执额外提供稳定 `uniqueSearchKey`；单条/批量和 JDBC/R2DBC 共享原子侧索引维护，随机密文批量以稳定 HMAC 回执身份安全重放，CONTAINS 辅助表迁移幂等；四库新能力认证已完成，证据见 `real-database-certification.md`。 |
| insert/update/delete | FormClient / Repository / DML operator | 已覆盖首版 | P0 | 动态表单 Map 链路、Repository 实体入口和 DML operator 链式 update/delete 都已有；后续补更多 SQL 方言差异实测。 |
| 动态表单 DML 乐观锁 | `OptimisticLockOptions` / `@Version` | 已覆盖主链路 | P1 | 单行、批量、FormClient、Repository 和 DML operator 已统一使用版本条件、冲突分片和观测分类；`ATOMIC` 冲突整批回滚，`INDEPENDENT` 返回成功与冲突分片，租户、DataScope、FieldScope 和逻辑删除保护保持不变。H2、MySQL、PostgreSQL 的相关真实库场景已纳入 V2 认证。 |
| 逻辑删除 | `logicDelete(...)` / `@FlyingLogicDelete` | 已覆盖首版 | P1 | `DynamicForm.Builder.logicDelete(...)` 和 `@FlyingLogicDelete` 都能显式声明字段名、未删除值和已删除值；FormClient/Repository 查询、分页、更新、默认删除会自动过滤未删除数据，默认删除会改标记，物理删除必须显式调用 `physicalDelete(...)`；DML query/update/delete operator 可显式 `.logicDelete(...)`，delete operator 可 `.physical()`。 |
| 租户隔离与 DataScope | Tenant/Data/Field/Time Scope | 已覆盖主链路 | P1 | 共享表租户字段、DataScope、FieldScope、TimeScope 已贯通 FormClient、Repository、Operator 和批量；独立库路由、Schema/RLS 会话清理和事务内路由锁定已有统一契约，PostgreSQL 连接池隔离已完成 V2 认证。 |
| upsert/saveOrUpdate | DML upsert / batch upsert | 已覆盖目标矩阵 | P0 | H2/MySQL/PostgreSQL/Oracle/SQL Server 的渲染、参数绑定和批量事务已覆盖；四种生产数据库均已完成目标版本实库认证。 |
| 批量写入 | Batch plan / BatchWriteOptions | 已覆盖首版并增强冲突模型 | P0 | 已有批量 insert/upsert、ATOMIC/INDEPENDENT、UNKNOWN、JMH 入口；R2DBC 提供事务回执恢复，JDBC 使用业务幂等事实确认；共享结果模型可返回行级乐观锁冲突偏移和稳定的 `RdbErrorKind`；两内核在接收每行时快照可复用的参数数组，生成键回调异常也必须完成事务收尾，普通写入仍走驱动批处理。 |
| 同步执行器 | `SyncSqlExecutor` | 已覆盖 V2 主链路 | P1 | 同步 API 使用原生 JDBC；已删除 R2DBC 阻塞同步桥。同步与响应式入口共享条件、渲染、方言、参数、映射和安全规则。 |
| R2DBC 执行器 | `ReactiveSqlExecutor` / `R2dbcSqlExecutor` | 已覆盖 V2 主链路 | P0 | `R2dbcSqlExecutor` 是原生 R2DBC 执行内核，查询、写入、批量均走 Reactor/R2DBC；同步路径由独立的原生 JDBC 内核执行。 |
| 外部框架集成 | 框架无关接入契约 | 已覆盖框架无关主链路 | P2 | flying-orm 本体不依赖外部应用框架，也不包含框架自动装配；纯 Java `FlyingOrmClients.builder(DataSource)`、`builder(ConnectionFactory)` 或双数据源入口统一组装客户端，独立适配项目可据此注册上层框架 Bean。 |
| 值编解码 | `rdb.codec` | 已覆盖主链路 | P1 | `ValueCodecRegistry` 覆盖 Enum、Boolean、Number、UUID、Java time、二进制和文本值，并新增 `DriverValueAdapter` 让上层显式解包可选驱动特殊返回值；应用级 codec 继续贯通条件、单条、批量和实体回读。JSON、LOB、Array、Vector 等需要字段信息的类型仍走字段感知 codec。 |
| 查询结果包装 | `executor.wrapper` | 已覆盖主链路 | P1 | Map 行结果和 RowMapper 已完成；`RowMapper.withAliases(...)` 支持显式列别名，默认映射兼容大小写、snake/camel、表限定符和常见引用符。 |
| record/Bean 映射 | `mapping.defaults`、`mapping.defaults.record` | 已覆盖首版 | P1 | 已有 `MappingPlan`、`EntityValues`、`ReactiveFormRepository`、`SyncFormRepository`；读库映射已接入 `ValueCodecRegistry` 做目标类型转换；实体元数据可自动生成 `DynamicForm`；写入实体时会按实体元数据输出数据库列名，读库时同时识别 Java 字段名和数据库列名。 |
| flying-orm 实体注解解析 | `com.flying.orm.core.annotation` | 已覆盖 V2 主链路 | P2 | 自有 `@TableName`、`@TableId`、`@TableField`、`@Version`、`@TableLogic`、`@EnumValue`、`@KeySequence` 和 `@OrderBy` 已接入；`AUTO`、`ASSIGN_ID`、`ASSIGN_UUID`、FieldFill、FieldStrategy 和启动期实体模型生成已覆盖 JDBC/R2DBC。V2 不读取 Jakarta Persistence 实体注解。 |
| 映射事件 | `mapping.events`、`operator.event` | 已覆盖首版 | P2 | `EntityMappingListener` 提供写前和读后事件；监听器包在共享映射计划外，不污染缓存，也不依赖应用框架。 |
| 数据库方言 | `supports.*` | 已覆盖代码、契约与实库主线 | P0/P1 | 当前代码覆盖 H2、MySQL、PostgreSQL、Oracle、SQL Server；`RdbDialectResolver` 可根据配置名、JDBC/R2DBC 连接信息自动识别。四种生产数据库均已完成 V2 真实库认证。OpenGauss、Kingbase 不规划。 |
| JSON 条件和 JSON 类型 | `supports.json` | 已覆盖 V2 主链路 | P2 | 内置方言 JSON 逻辑类型、字段 codec 和结构化条件已在 JDBC/R2DBC 共享；路径、比较值和 JSON 文本全部参数化，H2、MySQL、PostgreSQL、Oracle、SQL Server 已纳入 V2 真实库清单。 |
| PostgreSQL array | `supports.postgres.array` | 已覆盖 V2 主链路 | P2 | 支持 `VARCHAR[]`、`BIGINT[]` 等原生数组的动态表单/实体编解码、批量参数和元数据恢复，以及 contains、contained-by、overlaps、any-equals 条件；PostgreSQL JDBC/R2DBC 往返已完成认证。 |
| PostgreSQL vector | `supports.postgres.vector` | 已覆盖 V2 主链路 | P2 | 已有维度和数值校验、`float[]` 参数绑定与读取、L2/余弦/内积条件和最近邻排序；pgvector 真实库认证已完成。 |
| 数据库元数据解析 | `metadata.parser`、各 `supports.*TableMetaParserTest` | 已覆盖 V2 主链路 | P1 | 五库字段、主键、注释、索引和外键读取已进入统一 reader，并已纳入 V2 真实库验证；完整认证状态见数据库支持矩阵。 |
| 索引元数据解析 | `metadata.key`、各 `IndexMetadataParserTest` | 已覆盖 V2 主链路 | P1 | 普通/唯一索引 SQL 合同、目标数据库读取和 DDL 后缓存失效已纳入 V2 认证。 |
| 异常翻译 | `supports.mysql/postgres *ExceptionTranslation*`、`utils.ExceptionUtils` | 已覆盖主链路 | P1 | `RdbExceptionTranslator` 覆盖唯一键、完整性约束、SQL 错、连接、普通超时、死锁、锁等待超时、取消和未知，并识别透明异步包装且保留携带业务结果的外层异常；普通执行、批量结果和观测使用同一分类。`StructuredConditionException`、`ScopeAccessException`、`RdbException` 都可输出统一 `OrmErrorReport`，上层不必解析异常文本。 |
| SQL 工具和模板 | `utils.SqlUtils`、`executor.SqlTemplateParser` | 已覆盖受控逃生口 | P2 | 模板只能服务端注册；值参数绑定，标识符槽单独声明和校验，拒绝前端 SQL 与多语句。 |
| 事件/上下文 | `context`、`events` | 已覆盖必要首版 | P2 | 实体映射事件和 R2DBC 隔离上下文已实现，保持纯 Java/Reactor 边界，不引入全局 ThreadLocal。 |

## 持续质量门禁

以下是每次改动都要持续执行的质量门禁，不表示对应功能仍未实现：

1. 公共 API、Javadoc、包边界和废弃入口保持可检查，公开入口不绕过统一安全配置。
2. testkit 继续覆盖 UNKNOWN、取消、连接中断、死锁/锁超时等可重复故障场景。
3. JMH 和真实数据库性能报告使用固定参数复跑，持续关注吞吐、P95/P99、内存和连接占用。
4. 数据库驱动或执行器发生变更时，按真实数据库认证清单重新验证，不把历史报告当成新代码的自动证明。

## 高级能力

这些能力已经纳入当前版本，但不应拖慢普通 SQL 热路径：

1. JSON 查询条件和 JSON 字段类型：首版已完成。
2. PostgreSQL array / vector：代码和 pgvector 实库认证均已完成，后续只跟踪驱动和数据库版本差异。
3. flying-orm 自有实体注解：V2 主链路已完成，后续只跟踪明确的映射边界和驱动差异，不读取 Jakarta Persistence 实体注解。
4. Oracle、SQL Server 高级类型和实库深度兼容已完成；后续只跟踪明确的驱动或数据库版本差异。
5. 事件模型、上下文传播、操作监听：实体映射事件和隔离上下文首版已完成。
6. SQL 模板逃生口：受控模板注册表和渲染器首版已完成。
7. 动态表单 DML 乐观锁：单行 update/delete、DML operator、批量执行层、冲突观测和 FormClient/Repository 高层批量更新入口均已完成主链路；后续重点是真实数据库并发压力验证。
8. 租户隔离与 DataScope：首版已完成，已支持租户字段、普通 DataScope 条件收窄、默认 scope 注入、读字段裁剪、写字段保护、动态表单 `TenantStrategy`、批量租户值兜底、必需 tenant scope 保护、前端结构化条件防伪、四类语义化 DataScope 预设、参数化 TimeScope，以及 Repository/FormClient/Operator 组合边界。

## 当前不规划

- OpenGauss：用户已明确当前不需要。
- Kingbase：当前目标数据库不包含它。
- R2DBC 阻塞同步桥：V2 已删除，不再规划通过 R2DBC 承担同步 JDBC 入口。

## 最近同步

- MySQL、PostgreSQL、Oracle、SQL Server 的生产元数据 reader 已补上外键查询 SQL 合同。
- 这一步只验证 SQL 标记、参数顺序和 `ForeignKeyMetadata` 共享转换，不连真实生产库。
- MySQL、PostgreSQL 已有外部兼容测试入口和独立测试驱动 profile；V2 认证已覆盖 CRUD、元数据、JSON 往返以及批量乐观锁提交与 ATOMIC 冲突回滚。
- core 已加入首版 `ValueCodecRegistry`，先统一 Enum、Boolean、Number、Java time 的安全转换。
- `MappingPlan` 的 record/Bean 读库映射已接入 `ValueCodecRegistry`，数据库返回 `String`、`BigDecimal`、`Timestamp` 时可以按目标字段类型转换。
- `EntityValues` 写入实体时已按 `@TableField` 或 snake_case 推断输出数据库列名；`MappingPlan` 读库时同时认 `name` 和 `user_name`，上层服务不需要再手写字段名转换。
- 动态 Map、批量写入和 Repository 写入参数已走 `ValueCodecRegistry.write()`，业务枚举会在交给 R2DBC 前转成稳定字符串。
- 条件参数也已走 `ValueCodecRegistry.write()`，例如 `where("status", "=", Status.ACTIVE)` 会绑定 `"ACTIVE"`。
- 逻辑删除已从 Repository 扩到动态表单主链路：`DynamicForm` 可声明删除标记，Reactive/Sync FormClient、Repository 和 DML operator 查询/更新/默认删除都能自动带未删除条件，物理删除走显式入口。
- R-020 DataScope 首版已开始：`DataScope.tenant(...)` / `DataScope.where(...)` 能和前端/业务 where 做 AND 合并，FormClient、Repository、DML operator 都可显式传入 scope；FormClient、DatabaseOperator 和 FlyingOrmClients 还能挂默认 scope，FieldScope 已能裁剪读字段并保护写字段，前端条件只能收窄不能绕过服务端范围；`DataScope.all()`、`orgAndChildren(...)`、`orgOnly(...)`、`self(...)` 已补齐，`orgAndChildren` 只生成业务 term，不把组织树 SQL 写死在内核；`DataScope.time(TimeScope)` 已能把明确时间窗口接入同一条 AND 链路。
- 内置方言已增加 JSON 逻辑类型占位：MySQL/H2 为 `JSON`，PostgreSQL 为 `JSONB`，Oracle 为 `CLOB`，SQL Server 为 `NVARCHAR(max)`。
- MySQL/PostgreSQL 已有 JSON 条件包，覆盖嵌套 path 等值、JSON 包含和路径存在；结构化输入统一使用内置 JSON operator。
- 前端可用 `Map` 形态传 JSON 条件；路径会拆成已校验的 key 段，路径、比较值和 JSON 文本都走参数绑定，不安全路径会在 SQL 渲染前失败。
- Reactive/Sync 表单客户端已经能挂载 JSON 结构化条件 resolver，调用方可以直接把前端结构化条件交给客户端查询入口；JSON 条件和业务 term 放行可通过 composite resolver 或 `StructuredConditionResolvers` 预设一起生效。
- JSON 字段写入会先通过 Jackson 校验并压缩成稳定文本，单行、批量 insert/upsert 使用同一规则；动态表单查询会还原成 Map/List，实体 Map/List/JsonNode 字段也会自动映射。
- H2 JSON 写入会自动补 `FORMAT JSON`，避免驱动把整个 JSON 文本存成字符串；真实 H2 R2DBC 往返已覆盖单行 insert 和批量 upsert。
- flying-orm 本体不包含应用框架自动配置；JSON term、业务 term、resolver 组合都以纯 Java API 提供，上层服务或独立适配器自行装配。
- PostgreSQL 数组已接入字段感知 codec、批量布局、动态 Map/实体读取和元数据 reader；数组 contains、contained-by、overlaps、any-equals 通过固定 SQL 模板渲染。
- 通用值编解码已补 UUID、`OffsetTime`，并让整型 `Number` 转换绕开 double，避免大整数读取成 `BigInteger` 时丢精度。
- 租户 scope 缺失、租户值冲突、重复租户字段和字段不可写等失败已统一为 `ScopeAccessException`，并提供稳定 `ScopeErrorCode`、表单和字段。
- 真实数据库外键兼容性仍放到后续外部兼容测试或 Testcontainers 阶段。

## 性能基准候选

后续建立正式性能基线时，先选这些场景：

| 场景 | flying-orm 当前入口 | 基线工作负载 |
| --- | --- | --- |
| 条件编译 | `StructuredConditionBenchmark` | 相同字段、节点数、嵌套深度和操作符集合 |
| SQL where 渲染 | `SqlRenderBenchmark` | 相同条件 AST、方言和参数数量 |
| 批量 insert/upsert 计划 | `BatchInsertPlanBenchmark` | 相同列数、行数和字段布局 |
| record/Bean 映射 | `EntityMappingBenchmark` | 相同字段数、值类型和映射目标 |
| 元数据查找与缓存 | Caffeine 企业级缓存 + benchmark 首版 | 相同表数量、命中率和失效频率 |
| 值编解码 | 普通类型、JSON、BLOB/CLOB 响应式读取已接入；外部库 LOB smoke 入口已就绪，继续做真实执行与数组 | 相同逻辑类型、数据大小和转换方向 |

性能阶段已明确直接依赖 Caffeine，不再把它只作为可选适配。Caffeine 包装实现留在包内，公开 API 只暴露 `ReactiveFormMetadataCache` 能力接口和 `ReactiveFormMetadataReaders.cached(...)` 工厂；缓存支持高并发读、TTL、容量淘汰、DDL 失效和统计快照。`invalidate("schema.table")` 只清对应 schema，`invalidate("table")` 会清各 schema 下的同名表，createOrAlter 只在 DDL 成功后触发失效；`MetadataCacheBenchmark` 已提供热表读取、多表轮询和读写失效混合的 JMH 入口，缓存快照可由上层接入指标系统。
