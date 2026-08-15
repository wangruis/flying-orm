# Case-insensitive LIKE Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. The active collaboration rule forbids subagent dispatch, so execution remains inline.

**Goal:** 为现有条件 AST 增加跨数据库一致的 `like-ignore-case` 和 `not-like-ignore-case`，并保持参数化、Scope、安全策略、缓存和 API 基线不变。

**Architecture:** 两个 operator 作为标准 SCALAR term 注册，SQL handler 用 `lower(identifier) ... lower(?)` 渲染。结构化条件和计划缓存只扩展现有白名单，不引入方言分支、新生产类型或快捷 API。

**Tech Stack:** Java 21、JUnit Jupiter、Maven 3.9.16、现有 Condition AST、SqlRenderer、StructuralPlanCaches。

## Global Constraints

- 不新增生产类型，不新增或删除 public/protected API。
- 普通 `like`/`not-like`、参数值和通配符语义保持不变。
- 所有业务值参数化；字段继续通过现有标识符渲染器。
- 修改类必须继续低于 400 行和 30 个可调用方法。
- 先 RED、后 GREEN；不得通过放宽旧断言制造绿色。
- 不执行 Git 暂存、提交或推送；不运行真实数据库认证。

---

### Task 1: 标准 term 与 SQL 渲染

**Files:**
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/condition/TermRegistry.java`
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/sql/render/SqlTermHandler.java`
- Test: `flying-orm-core/src/test/java/com/flying/orm/core/sql/render/SqlRendererTest.java`

**Interfaces:**
- Consumes: `ConditionGroup.where(field, operator, value)` 与 `SqlRenderer.builder().addDefaultTerms()`。
- Produces: 标准 SCALAR operator `like-ignore-case`、`not-like-ignore-case`。

- [ ] **Step 1:** 在 `SqlRendererTest` 写 RED，断言两个 operator 分别生成 `lower(name) like lower(?)` 与 `lower(email) not like lower(?)`，参数保持原值且不进入 SQL。
- [ ] **Step 2:** 运行 `SqlRendererTest`，确认因标准 term 不存在而失败。
- [ ] **Step 3:** 在 `TermRegistry.STANDARD` 声明两个 SCALAR term；在 `SqlTermHandler.defaults()` 注册两个私有大小写不敏感 handler。
- [ ] **Step 4:** 重跑 `SqlRendererTest`，确认新旧 LIKE 契约全部通过。

### Task 2: 前端结构化条件

**Files:**
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/condition/StructuredConditionPolicy.java`
- Test: `flying-orm-core/src/test/java/com/flying/orm/core/condition/StructuredConditionCompilerTest.java`

**Interfaces:**
- Consumes: `StructuredConditionInput.term(field, operator, value)`。
- Produces: 默认策略将两个外部 operator 映射到同名标准 term。

- [ ] **Step 1:** 写 RED，使用默认策略编译正向和否定条件并交给默认 SQL renderer，断言 SQL、参数及字段/operator 路径稳定。
- [ ] **Step 2:** 运行测试，确认以 `OPERATOR_NOT_ALLOWED` 失败。
- [ ] **Step 3:** 在默认 operator 映射中加入两个同名映射，不改变字段级白名单优先级。
- [ ] **Step 4:** 重跑结构化条件测试类。

### Task 3: RDB 方言与结构缓存

**Files:**
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/internal/plan/StructuralPlanCaches.java`
- Test: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/plan/StructuralPlanCachesTest.java`
- Test: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/form/FormDataSqlRendererTest.java`

**Interfaces:**
- Consumes: 标准 term、方言标识符渲染器和有界条件计划缓存。
- Produces: 两个 operator 的单参数结构计划可复用；五种内置方言生成安全标识符。

- [ ] **Step 1:** 写缓存 RED，两个不同请求值应复用同一计划、保留各自参数且只加载一次。
- [ ] **Step 2:** 写方言 RED，H2/MySQL/PostgreSQL/Oracle/SQL Server 分别断言方言引用后的 `lower(column)` SQL。
- [ ] **Step 3:** 运行两类测试，确认缓存旁路造成加载计数不满足契约，而 SQL handler 已由 Task 1 提供。
- [ ] **Step 4:** 将两个 operator 加入 `STRUCTURAL_TERMS`；重跑两类 RDB 测试。

### Task 4: 文档与完整门禁

**Files:**
- Modify: `README.md`
- Modify: `docs/target-api-examples.md`
- Modify: `docs/source-feature-matrix.md`

**Interfaces:**
- Consumes: 已验证的 operator 名称和 SQL/索引边界。
- Produces: 使用示例、前端用法和性能说明。

- [ ] **Step 1:** 在现有可选搜索示例旁增加 `like-ignore-case`，说明 `not-like-ignore-case`、通配符和函数索引边界。
- [ ] **Step 2:** 运行聚焦 Core/RDB 测试与 API 基线。
- [ ] **Step 3:** 检查修改生产类的行数、方法数和 `git diff --check`。
- [ ] **Step 4:** 运行 `-Pquality -pl flying-orm-core,flying-orm-rdb -am verify` 并读取真实退出码与 Surefire 汇总。
