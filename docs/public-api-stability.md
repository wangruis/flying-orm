# 公共 API 稳定边界

flying-orm 当前只维护 V2.0.0 正式公开 API，不保留旧版本兼容快照或兼容门禁。

## V2.0.0 正式基线

`flying-orm-rdb/src/test/resources/api/v2.0.0-public-api.txt` 是当前 Core 与 RDB 公开 API 的精确快照。
它从编译后的字节码提取有效公开类型，以及公开或受保护的构造器、字段、方法、泛型、继承关系、枚举值和
record 组件；源码排版、注释和包内实现不会造成误报。

```shell
mvn -pl flying-orm-rdb -am -Dtest=PublicApiBaselineTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

普通测试只比较快照，不会自动改写。基线确认后，不能只为让测试通过而直接修改快照；公开签名调整必须先完成
API 设计、影响分析和迁移审查。当前能力不得暴露包内 renderer、连接、侧索引或事务协作类型。

## 稳定入口

- `com.flying.orm.core.condition`：条件 AST、结构化条件输入、安全策略和稳定错误码。
- `com.flying.orm.core.codec`：应用值 codec SPI 和构造后只读的 `ValueCodecRegistry`。
- `com.flying.orm.core.form`：`DynamicForm`、动态字段、租户和逻辑删除定义。
- `com.flying.orm.core.page`、`com.flying.orm.core.scope`：分页与数据范围模型。
- `com.flying.orm.core.sql.render`：`SqlRenderer`、参数化 SQL 请求和业务 term SPI。
- `com.flying.orm.rdb.bootstrap.FlyingOrmClients`：纯 Java 统一组装入口。
- `com.flying.orm.rdb.form`：响应式与同步动态表单客户端。
- `com.flying.orm.rdb.mapping`：实体元数据、映射事件和 `RowMapper` 扩展入口。
- `com.flying.orm.rdb.batch`、`execution`、`observation`、`exception`：批量、执行保护、观测和错误模型。
- `com.flying.orm.rdb.operator`、`repository`、`schema`：链式操作、Repository 和动态 DDL。
- `com.flying.orm.rdb.migration`：参数化数据迁移、补偿结果和明确的回滚失败状态。
- `com.flying.orm.rdb.reactive.ReactiveSqlExecutor` 与 `R2dbcSqlExecutor`：响应式执行契约和默认实现。
- `com.flying.orm.rdb.sync.SyncSqlExecutor`：原生 JDBC 同步执行契约。

稳定表示：2.0.x 小版本不随意删除类型、改变方法语义或放宽默认安全策略。确需替换时，应提供明确迁移方式，
不得为未知历史业务保留重复入口。

## 扩展入口

方言、JSON、PostgreSQL Array、codec、元数据 reader 和结构化条件 resolver 允许扩展。新增 SPI 方法优先提供
默认实现，避免无必要地破坏已有扩展；涉及安全、事务或资源所有权的变化仍须显式审查。

## 不建议直接依赖

- JDBC/R2DBC 批量 writer、连接 lease、清理 deadline 和执行器内部组合类。
- 各数据库元数据 reader 的具体实现；上层使用公开工厂。
- SQL 渲染 builder、缓存 key、映射计划、批量内部异常和分页内部实现。
- `internal` 包及标记为 `@InternalApi` 的类型或方法。

## 默认边界

- 同步入口直接使用 JDBC，响应式入口直接使用 R2DBC，不互相桥接。
- 连接池负责容量、排队、健康检查和获取超时；ORM 只借用、归还，连接污染时通知失效。
- 外部事务由上层控制；ORM 只复用当前连接，不自行提交或回滚。
- 没有外部事务的 `ATOMIC` 批量由 ORM 保证原子性。
- SQL、参数、Scope、安全校验、行数、内存和 LOB 上限属于 ORM 核心职责。
- 严格 `where(...)` 不忽略空值；可选条件使用 `whereIfPresent(...)`。
- 前端结构化条件不能携带 SQL，业务值必须参数化绑定。
- 数据迁移未显式传执行选项时继承 executor 的默认保护，不能偷偷切换为 `unlimited`。
- observer 默认不申请参数值或事务来源，只有明确开启的能力承担对应开销。

## 发布门禁

`release-artifacts` Profile 会生成源码包和 Javadoc 包；坏链接、坏 HTML 或其他 Javadoc 警告会中止发布构建。
每次新增或修改公共 API 都必须写清参数、返回值、默认行为、线程安全和失败边界。
