# 2026-07-22 动态表单分页查询进展

## 背景

动态表单列表查询不能无边界返回全量数据。为了支撑高吞吐、高并发和稳定性，列表查询需要统一分页模型、
排序约束、总数查询和响应式返回结果。

## 本次实现

- 在 `flying-orm-core` 新增 `PageQuery`，使用一基页码并限制单页最大数量为 1000。
- 在 `flying-orm-core` 新增 `PageSort`，排序字段保留为结构化 Java 模型，不在 core 层拼接 SQL。
- 在 `flying-orm-core` 新增 `PageResult<T>`，冻结当前页行数据，并提供 `totalPages()`、`hasNext()` 等派生信息。
- `FormDataSqlRenderer` 新增 `select(form, where, page)`，渲染 `order by ... limit ? offset ?`，分页参数进入参数列表。
- `FormDataSqlRenderer` 新增 `count(form, where)`，分页总数查询复用同一条件树。
- 在 `flying-orm-rdb` 新增 `PaginationDialect`，默认使用 `limit ? offset ?`，并提供 `offset ? rows fetch next ? rows only` 扩展基础。
- `FormDataSqlRenderer` 支持注入分页方言，不再把分页 SQL 语法写死在动态表单 CRUD 主流程中。
- `ReactiveFormClient` 新增 `page(...)` 系列方法，支持 `ConditionGroup`、`ParameterConditionCompiler` 和 `ParameterConditionPackage` 三种入口，返回 `Mono<PageResult<DynamicRow>>`。

## 边界

- 当前默认分页方言使用通用 `limit ? offset ?` 形态，优先覆盖 H2、MySQL 和 PostgreSQL。
- `offset/fetch` 方言已作为 Oracle 和 SQL Server 后续适配基础进入扩展点，后续可继续补齐版本差异和排序兜底策略。
- 排序字段会通过动态表单字段元数据校验，避免任意字段文本直接进入 SQL。
- 深分页优化、游标分页和 count 缓存/跳过策略留作后续阶段。
