# 结构化条件与传统 JDBC 风格桥接对齐记录

> 历史记录。结构化条件契约仍然有效，但文中的 R2DBC 阻塞同步桥已在 V2.0.0 删除；
> 当前同步入口使用原生 JDBC，响应式入口使用原生 R2DBC。

## 状态

历史阶段已完成；当前执行方式以 V2.0.0 文档为准。

## 目标

flying-orm 是一个简单 ORM 工具，为动态表单而生。前端可以透传结构化动态条件，但不能透传 SQL；条件 AST、SQL 渲染器、方言、参数顺序和批量结果模型都以 R2DBC/Reactor 内核为准。传统 JDBC 风格只是给同步调用方使用的阻塞桥接层，不再单独发展 `DataSource + java.sql.Connection` 执行内核。

## 当前实现

- `StructuredConditionInput`：前端条件输入模型，支持 term 和 and/or 分组。
- `StructuredConditionPolicy`：外部字段、操作符和大小限制策略。
- `StructuredConditionCompiler`：把前端输入编译成 `ConditionGroup` / `TermCondition`。
- `ReactiveFormClient`：新增结构化条件查询入口，仍返回 `Flux` / `Mono`，是正式主线客户端。
- `SyncSqlExecutor.bridge(...)`：正式同步 SQL 桥接入口，内部仍然走 R2DBC/Reactor，具体实现类不公开。
- `SyncFormClient`：正式同步表单入口，内部复用 `ReactiveFormClient`。
- 批量结果模型已从 `com.flying.orm.rdb.reactive.batch` 调整到 `com.flying.orm.rdb.batch`；它不是“双内核共享模型”，而是 R2DBC 内核与同步桥接层共同对外暴露的结果契约。

## 安全边界

- 前端操作符默认只开放 `eq`、`gt`、`lt`、`like`、`in`，业务操作符必须显式 `allowOperator(...)`。
- 外部 `eq` 会映射到内部 `=`，业务也可以把外部名字映射到内部 term id。
- 字段必须能在 `DynamicForm` 中找到；策略可以进一步 `allowOnlyFields(...)`。
- 不支持前端传 `NativeSql`、原生 SQL、函数选项或 `$` 字段简写。
- 树深、节点数、集合大小和字符串长度都有上限，避免条件过大拖垮编译或数据库。

## 结构化条件的取舍

条件使用树形结构，term 类型可扩展，最终由 SQL 片段构建器处理。

flying-orm 不照搬它的宽松行为：前端入口默认严格，未知字段和未知操作符直接失败，不静默忽略；原始 SQL 只允许框架内部或后续明确的服务端扩展点使用。

## 命名边界

响应式 API 使用 `Reactive*` / `R2dbc*`，同步门面使用 `Sync*`。API 名称直接反映执行模型，不提供容易被误解为第二套 JDBC 内核的别名。
