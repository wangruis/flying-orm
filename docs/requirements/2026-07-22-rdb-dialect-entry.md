# 2026-07-22 RDB 方言统一入口进展

## 背景

动态表结构维护和动态数据查询都存在数据库差异。此前结构 SQL 使用 `SchemaDialect`，
分页 SQL 使用 `PaginationDialect`，两者分散在不同调用入口。为了当前优先支持 H2、MySQL、
PostgreSQL，并为后续 Oracle、SQL Server 预留扩展点，需要先建立一个轻量统一入口。

## 本次实现

- 在 `flying-orm-rdb` 新增 `RdbDialect`，聚合方言名称、`SchemaDialect` 和 `PaginationDialect`。
- 内置 `h2()`、`mysql()`、`postgresql()` 三个当前数据库方言工厂方法。
- `SchemaDialect.standard()` 表示结构 SQL 的基础写法：名字不加引号，字段类型原样输出。
- `FormSchemaSqlRenderer` 新增 `create(RdbDialect)`，从统一方言入口取得结构 SQL 方言。
- `FormDataSqlRenderer` 新增 `create(SqlRenderer, RdbDialect)`，从统一方言入口取得分页 SQL 方言。
- `ReactiveSchemaClient` 新增 `create(executor, RdbDialect)`，响应式动态表结构入口可直接接收数据库方言。
- `ReactiveFormClient` 新增 `create(executor, conditionRenderer, RdbDialect)`，响应式动态表单入口可直接接收数据库方言并复用分页策略。
- 上层应用可根据 `ConnectionFactory` 的驱动名称识别当前方言，也可以直接指定 `RdbDialect`；flying-orm 本体只提供方言工厂和纯 Java 组装入口。
- 保留原有 `SchemaDialect` 和 `PaginationDialect` 入口，避免早期 API 快速迭代造成大面积重写。

## 边界

- 当前 `RdbDialect` 只聚合已落地的结构和分页能力，不引入复杂模块或数据库矩阵配置。
- SQL 标准规范不是数据库，不能出现在内置 RDB 数据库方言列表中。
- MySQL 初始使用反引号标识符和 `limit/offset`；PostgreSQL 初始使用双引号标识符和 `limit/offset`。
- `ReactiveFormClient` 仍要求显式传入 `SqlRenderer`，避免默认条件集合掩盖业务自定义 term 注册。
- Oracle、SQL Server、更完整的类型映射、DDL 语法差异和分页排序兜底留作后续阶段。
- OpenGauss 当前业务用不到，不进入内置方言和测试计划。
