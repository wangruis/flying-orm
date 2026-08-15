# Lightweight JOIN Implementation Plan

> 实施状态（2026-08-10）：INNER/LEFT/RIGHT、DynamicForm 与实体 Lambda、Scope/逻辑删除、保护字段、
> 投影/排序、offset page/count、公共 API 与文档已完成并通过质量/发布门禁。cursor page 因跨源唯一键与外连接
> null 排序尚无统一四库契约，明确留作后续；四库 V2.0.0 新能力实库认证已经完成，证据见 `docs/real-database-certification.md`。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 DynamicForm 与实体 Lambda 提供轻量、类型安全的 `join`、`leftJoin`、`rightJoin` 查询，并让 JDBC 与 R2DBC 共享同一不可变 AST、SQL 请求、Scope 和结果语义。

**Architecture:** core 只保存无 SQL 字符串的 JOIN AST；rdb 的链式 builder 负责把 DynamicForm 字段或实体 getter 解析成 AST；`ReactiveFormClient` 与 `SyncFormClient` 内部新增 JOIN 查询协作者，复用既有 Scope、逻辑删除、codec、执行保护和 `DynamicRow` 解码。JOIN 使用独立渲染器，不扩张已接近门禁的 `RdbDialect`，也不修改单表查询热路径。

**Tech Stack:** Java 21、Maven 3.9.16、Reactor、JDBC、R2DBC SPI、JUnit Jupiter、H2。

## Global Constraints

- 每个任务严格执行 RED → GREEN → 聚焦回归；没有失败证据不得写生产实现。
- 不改变现有单表 CRUD、事务、批量、SQL 模板或公共错误语义。
- 不接受原始 SQL、用户别名、ON 常量、ON OR、函数或子查询。
- 第一版禁止自连接；内部 SQL 别名固定按源顺序生成 `t0`、`t1`……。
- 多表查询必须显式投影；结果别名规范化后必须唯一。
- 每个生产类型保持小于 300 物理行和 20 个可调用方法；达到边界前按 AST、渲染、Scope、执行和 Lambda 解析拆分。
- 当前工作区包含用户未提交基线变更。只修改本计划列出的文件，不自动提交；Git 提交必须再次取得用户授权。

---

### Task 1: 建立不可变 JOIN AST 与验证边界

**Files:**
- Create: `flying-orm-core/src/main/java/com/flying/orm/core/join/JoinType.java`
- Create: `flying-orm-core/src/main/java/com/flying/orm/core/join/JoinSource.java`
- Create: `flying-orm-core/src/main/java/com/flying/orm/core/join/JoinFieldRef.java`
- Create: `flying-orm-core/src/main/java/com/flying/orm/core/join/JoinFieldPair.java`
- Create: `flying-orm-core/src/main/java/com/flying/orm/core/join/JoinClause.java`
- Create: `flying-orm-core/src/main/java/com/flying/orm/core/join/JoinProjection.java`
- Create: `flying-orm-core/src/main/java/com/flying/orm/core/join/JoinOrder.java`
- Create: `flying-orm-core/src/main/java/com/flying/orm/core/join/JoinQuerySpec.java`
- Create: `flying-orm-core/src/test/java/com/flying/orm/core/join/JoinQuerySpecTest.java`

- [ ] 先写 AST 直接契约：拒绝重复源、未加入源字段、缺失字段、空投影、规范化别名冲突和空 ON；验证输入列表与条件快照不可被调用方修改。
- [ ] 运行 `JoinQuerySpecTest`，确认因类型不存在或契约未实现而 RED。
- [ ] 最小实现上述八个小类型；字段名通过 `DynamicForm.findField` 验证，结果别名通过统一普通标识符规则验证，异常只使用固定分类消息。
- [ ] 重跑 `JoinQuerySpecTest` 并执行 core 编译；记录各生产类型行数和可调用方法数。

### Task 2: 实现共享 JOIN SQL 渲染器

**Files:**
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/JoinQuerySqlRenderer.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/JoinSourceSqlRenderer.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/FormDataSqlRenderer.java`
- Create: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/form/JoinQuerySqlRendererTest.java`

- [ ] 先写 MySQL/H2 方言契约：INNER、LEFT OUTER、RIGHT OUTER、复合 ON、显式投影、稳定 `tN` 别名、参数顺序、字段/别名预执行拒绝。
- [ ] 写外连接语义 RED：LEFT 可选右侧的逻辑删除/租户条件必须在 ON；RIGHT 可选左侧条件不得进入最终 WHERE；链式外连接无法局部下推时使用受控派生关系。
- [ ] 运行聚焦测试确认 RED。
- [ ] 在 form 包内实现独立渲染器；通过 `FormDataSqlRenderer` 的包内协作入口复用现有 condition renderer、identifier renderer、codec 和结构计划缓存，不给 `RdbDialect` 增加职责。
- [ ] 为每个源创建限定标识符 renderer，使现有 term handler 仍处理值形状与参数化绑定；禁止把 `source.field` 字符串送入普通标识符入口。
- [ ] 重跑聚焦测试，并回归 `FormDataSqlRendererTest`。

### Task 3: 在 FormClient 内复用 Scope、执行保护和结果解码

**Files:**
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/ReactiveJoinQueryOperations.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/SyncJoinQueryOperations.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/ReactiveFormOperations.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/ReactiveFormClient.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/SyncFormClient.java`
- Create: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/form/ReactiveJoinQueryOperationsTest.java`
- Create: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/form/SyncJoinQueryOperationsTest.java`

- [ ] 先写同一 `JoinQuerySpec` 在响应式与同步入口生成完全相同 `SqlRequest` 的 RED；响应式返回冷 `Flux`，同步直接使用 `SyncSqlExecutor`。
- [ ] 写每源 FieldScope、TenantScope、DataScope、逻辑删除契约以及默认执行保护传递契约。
- [ ] 最小实现两个内部协作者；调用既有 `FormScopeSupport` 计算每源只读表单和条件，调用既有 `ReactiveFormResultSupport` 解码投影字段。
- [ ] 重跑聚焦测试，并回归 `ReactiveFormClientTest`、`SyncFormClientTest`。

### Task 4: 提供 DynamicForm 链式 API

**Files:**
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/operator/JoinQueryOperator.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/operator/JoinWhere.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/operator/SyncJoinQueryOperator.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/operator/DmlOperator.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/operator/SyncDmlOperator.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/operator/DatabaseOperator.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/operator/SyncDatabaseOperator.java`
- Create: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/operator/JoinQueryOperatorTest.java`

- [ ] 先写用户形态 RED：`dml().joinQuery(userForm).leftJoin(...).andOn(...).select(...).selectAs(...).where(...).executeRows()`；同步构建语义相同。
- [ ] 验证 `join()` 只输出 INNER JOIN，`leftJoin()` 与 `rightJoin()` 输出显式 OUTER；不提供重复的 outer 方法。
- [ ] 最小实现一次性可变 builder，在执行时生成不可变 `JoinQuerySpec`；任何源/字段/投影问题必须在请求交给 executor 前失败。
- [ ] 重跑 `JoinQueryOperatorTest`、`DatabaseOperatorTest` 和同步 operator 测试。

### Task 5: 提供实体 Lambda JOIN API

**Files:**
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/operator/EntityJoinQueryOperator.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/operator/EntityJoinSources.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/operator/DmlOperator.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/operator/SyncDmlOperator.java`
- Create: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/operator/EntityJoinQueryOperatorTest.java`

- [ ] 先写已确认 API 的 RED：`joinQuery(User.class).leftJoin(Order.class, User::getId, Order::getUserId)`，以及 INNER/RIGHT、复合 ON、select/selectAs/where。
- [ ] 写错误契约：跨实体 getter、计算 Lambda、重复实体、自连接、未加入实体引用和非持久化字段在 SQL 生成前失败。
- [ ] 复用 `EntityModelRegistry` 与 `EntityPropertyResolver`，只把 getter 解析为已验证 `JoinFieldRef`；不得新增第二份反射或无界缓存。
- [ ] 执行返回 `DynamicRow`，另提供现有 `RowMapper<T>` 的平面 DTO 映射入口；不创建嵌套实体或懒加载。
- [ ] 重跑实体 JOIN 测试和 `EntityLambdaDmlOperatorTest`。

### Task 6: 排序、分页、count 与保护字段互操作

**Files:**
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/join/JoinQuerySpec.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/JoinQuerySqlRenderer.java`
- Modify: JOIN operator types from Tasks 4-5
- Create: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/form/JoinPaginationTest.java`

- [ ] 先写 ORDER BY、offset/page、cursor、count 的 SQL/参数/结果 RED；cursor 没有可证明唯一排序时必须拒绝。
- [ ] 最小实现排序和页码分页；count 必须复用同一 JOIN 与 Scope，只替换投影。
- [ ] 在字段保护能力落地后，增加契约：受保护字段可投影；EXACT/SUFFIX 可过滤；ON、ORDER BY、GROUP BY 和 JOIN 中 CONTAINS 稳定拒绝。
- [ ] 重跑 JOIN 全部测试与相关分页测试。

### Task 7: 公共 API、文档和质量门禁

**Files:**
- Modify: `flying-orm-rdb/src/test/resources/public-api-v2.txt` 或项目实际使用的下一版本 API 基线文件
- Modify: `README.md`
- Modify: `docs/requirements/index.md`
- Modify: JOIN 相关使用文档与四库认证矩阵

- [ ] 运行公共 API 快照测试，确认新增入口是有意变更且没有暴露内部 renderer/support 类型。
- [ ] 补充 README 的实体与 DynamicForm 示例、安全边界和非目标。
- [ ] 运行 `mvn -Pquality clean verify`、`mvn -Prelease-artifacts verify`。
- [ ] 对 MySQL 8.4、PostgreSQL、Oracle Free 23、SQL Server 2022 执行 JOIN 认证三轮，记录版本、驱动、用例、结果和资源泄漏证据。
- [ ] 执行 `git diff --check`，清点所有新增生产类型行数/方法数；仅报告验证事实，不自动提交。
