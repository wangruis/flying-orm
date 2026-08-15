# PostgreSQL Array And JSON Conditions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 连续完成 PostgreSQL Array 类型基础层、数组条件 AST，以及 MySQL/PostgreSQL JSON 结构化查询增强。

**Architecture:** 继续复用 `TermCondition`、`SqlTermHandler` 和 `StructuredConditionResolver`，不再增加一套平行查询框架。Array/JSON 对外只接收不可变值对象或前端 Map，适配阶段完成类型和路径校验，渲染阶段只生成固定 SQL 模板和参数槽。

**Tech Stack:** Java 21、Reactor、R2DBC SPI、Jackson、JUnit 5、Maven。

## Global Constraints

- 主项目不依赖 Spring。
- 所有条件值参数化，禁止前端传 SQL、操作符片段或未校验 JSON path。
- MySQL、PostgreSQL、H2 主线保持可编译；数据库专有操作在不支持方言上不静默降级。
- 每个任务只保留 1 到 3 个关键合同测试，三个任务结束后统一回归和批量提交。
- 新增代码写自然、直接的中文注释，并清理无用 import、变量和重复校验。

---

### Task 1: PostgreSQL Array 类型基础层

**Files:**
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/codec/ArrayValueCodec.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/FormDataSqlRenderer.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/ReactiveFormClient.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/metadata/PostgreSqlReactiveFormMetadataReader.java`
- Test: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/codec/ArrayValueCodecTest.java`

**Interfaces:**
- Produces: `ArrayValueCodec.isArrayDataType(String)`, `write(Object,String)`, `read(Object)`, `read(Object,Class<?>)`, `parameterType(String)`。
- Array 逻辑类型使用 PostgreSQL 原生形式，例如 `VARCHAR[]`、`BIGINT[]`、`BOOLEAN[]`。

- [x] 写 codec 失败测试：List 写成匹配元素类型的 Java 数组，驱动数组读成 List，实体目标数组按组件类型转换。
- [x] 实现数组类型解析、空集合、null 元素、基础元素转换和稳定错误。
- [x] 接入动态表单单行/批量/upsert 写入、Map 查询读取和 PostgreSQL 元数据 `_text`/`ARRAY` 恢复。
- [x] 运行 `ArrayValueCodecTest,FormDataSqlRendererTest,ReactiveFormClientTest`。

### Task 2: 数组条件 AST 与 PostgreSQL 渲染

**Files:**
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/array/ArrayConditionValue.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/array/ArrayStructuredConditions.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/array/ArrayTermHandlers.java`
- Test: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/array/ArrayTermHandlersTest.java`

**Interfaces:**
- Produces operators: `array-contains` (`@>`), `array-contained-by` (`<@`), `array-overlaps` (`&&`), `array-any-eq` (`= any(...)`)。
- 前端值只接受集合/数组或 `{values:[...]}`，空值和嵌套集合在 SQL 前拒绝。

- [x] 写一个渲染合同测试和一个前端适配安全测试并确认失败。
- [x] 实现不可变值对象、结构化条件 customizer/resolver 和 PostgreSQL term package。
- [x] 验证字段名校验、单数组参数绑定和参数顺序。

### Task 3: JSON 结构化条件增强

**Files:**
- Create/Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/json/JsonConditionValue.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/json/JsonStructuredConditions.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/json/JsonTermHandlers.java`
- Test: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/json/JsonTermHandlersTest.java`

**Interfaces:**
- Adds: `json-path-eq`、`json-contains`、`json-exists`。
- MySQL 使用 `json_extract/json_contains/json_contains_path`；PostgreSQL 使用 `#>>/@>/?`，JSON path 只允许经过校验的 key 段。

- [x] 写 MySQL/PostgreSQL 参数化渲染合同测试并确认失败。
- [x] 扩展 JSON 值对象和结构化适配，不把 path 或 JSON 文本直接拼进 SQL。
- [x] 运行 Array/JSON/结构化条件聚焦回归。

### Task 4: 收口与交付

**Files:**
- Modify: `docs/flying-orm-phased-implementation-plan.md`
- Modify: `docs/source-feature-matrix.md`
- Modify: `docs/requirements/index.md`
- Modify: `docs/target-api-examples.md`

- [x] 更新状态、DSL 示例、支持矩阵和未验证边界。
- [x] 运行聚焦测试、`mvn -DskipTests package` 和 `git diff --check`。
- [x] 清理无用 import/变量，刷新 `flying-orm-current` 知识图谱。
- [x] 将 Task 1 到 Task 3 作为一个批量提交推送。
