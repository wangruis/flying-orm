# Unified Condition Value Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让参数驱动、前端结构化条件、Java DSL 和安全 Scope 使用同一套空值清理与值形状校验规则，并为数据库 NULL 提供明确 term。

**Architecture:** core 增加无状态 `ConditionValueNormalizer`，现有 `TermHandler/TermRegistry` 保存 term 的值形状。`ConditionGroup.Builder` 在创建 AST 前执行严格或可选策略，参数驱动与前端编译器复用同一归一化器；SQL handler 只渲染已经校验过的值。

**Tech Stack:** Java 21、Maven、JUnit 5、flying-orm 条件 AST、参数化 SQL renderer

## Global Constraints

- 先完成 `docs/superpowers/plans/2026-07-31-time-scope.md`，本计划复用其中新增的 `>=`、`<=` term。
- 主项目不依赖 Spring，也不扫描 Spring；本计划不修改 POM 和模块结构。
- 字符串统一使用 Java 21 `strip()`；不改变字符串内部空白、大小写或 `like` 通配符。
- `where(...)`、前端结构化条件和安全 Scope 使用 `REJECT_EMPTY`；参数驱动和 `whereIfPresent(...)` 使用 `IGNORE_EMPTY`。
- `null`、空字符串、纯空白字符串、空集合、空数组，以及清理后为空的集合都算空值。
- `is-null`、`is-not-null` 不绑定参数；其他值继续参数化，不能拼进 SQL。
- 未声明值形状的 term 默认是 `SCALAR`；需要单值或集合时显式使用 `SCALAR_OR_COLLECTION`，其他可选形状为 `NONE`、`COLLECTION` 和 `RANGE`。
- Java 类型、公开方法和复杂分支写自然、能直接看懂的中文注释；清理无用 import、变量和重复校验。
- 只做少量聚焦测试；每个批次完成后提交，最后再做一次全模块打包。
- 设计依据：`docs/superpowers/specs/2026-07-31-condition-value-policy-design.md`。

## File Map

- Create: `flying-orm-core/src/main/java/com/flying/orm/core/condition/ConditionValueShape.java`，定义四种 term 值形状。
- Create: `flying-orm-core/src/main/java/com/flying/orm/core/condition/ConditionValuePolicy.java`，定义空值忽略或拒绝策略。
- Create: `flying-orm-core/src/main/java/com/flying/orm/core/condition/ConditionValueException.java`，提供稳定 core 错误类型。
- Create: `flying-orm-core/src/main/java/com/flying/orm/core/condition/ConditionValueNormalizer.java`，集中执行 strip、集合清理和区间校验。
- Modify: `TermHandler.java`、`SimpleTermHandler.java`、`TermRegistry.java`，保存并查找值形状。
- Modify: `ConditionGroup.java`、`WhereDsl.java`，接入严格和可选 DSL。
- Modify: `SqlTermHandler.java`，补齐标准比较、否定、集合、区间和 NULL term。
- Modify: `ParameterConditionCompiler.java`，删除私有空值分支并复用归一化器。
- Modify: `StructuredConditionPolicy.java`、`StructuredConditionCompiler.java`、`StructuredConditionErrorCode.java`，共享 term 元数据并保持前端稳定错误码和 path。
- Modify: 条件、参数、SQL renderer、operator 的现有测试类，只增加代表性用例。
- Modify: `docs/requirements/index.md`、`docs/source-feature-matrix.md`、`docs/flying-orm-phased-implementation-plan.md`、`docs/target-api-examples.md`，记录行为和迁移方式。

---

### Task 1: Core 值模型与 term 形状注册

**Files:**
- Create: `flying-orm-core/src/main/java/com/flying/orm/core/condition/ConditionValueShape.java`
- Create: `flying-orm-core/src/main/java/com/flying/orm/core/condition/ConditionValuePolicy.java`
- Create: `flying-orm-core/src/main/java/com/flying/orm/core/condition/ConditionValueException.java`
- Create: `flying-orm-core/src/main/java/com/flying/orm/core/condition/ConditionValueNormalizer.java`
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/condition/TermHandler.java`
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/condition/SimpleTermHandler.java`
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/condition/TermRegistry.java`
- Create: `flying-orm-core/src/test/java/com/flying/orm/core/condition/ConditionValueNormalizerTest.java`
- Modify: `flying-orm-core/src/test/java/com/flying/orm/core/condition/TermRegistryTest.java`

**Interfaces:**
- Produces: `ConditionValueShape { NONE, SCALAR, COLLECTION, RANGE, SCALAR_OR_COLLECTION }`。
- Produces: `ConditionValuePolicy { IGNORE_EMPTY, REJECT_EMPTY }`。
- Produces: `ConditionValueException.Error { NULL_VALUE, BLANK_VALUE, COLLECTION_EMPTY, SHAPE_NOT_ALLOWED, RANGE_SIZE_INVALID, RANGE_TYPE_MISMATCH, RANGE_ORDER_INVALID }`。
- Produces: `ConditionValueNormalizer.normalize(ConditionValueShape, Object, ConditionValuePolicy)`，返回嵌套 `Result`，包含 `present()`、`value()`、`ignored()`。
- Produces: `TermHandler.shape()`、`TermHandler.simple(String, ConditionValueShape)`、`TermRegistry.standard()`。

- [ ] **Step 1: 写归一化器和注册表失败测试**

在 `ConditionValueNormalizerTest` 用一个参数化测试覆盖四种形状，并增加两个独立异常测试：

```java
@Test
void stripsScalarsAndCleansCollections() {
    assertEquals("张三", normalize(SCALAR, "  张三  ", REJECT_EMPTY).value());
    assertEquals(List.of("u1", "u2"),
                 normalize(COLLECTION, List.of(" ", " u1 ", "u2"), REJECT_EMPTY).value());
    assertFalse(normalize(SCALAR, "   ", IGNORE_EMPTY).present());
}

@Test
void validatesNoneAndRangeShapes() {
    assertTrue(normalize(NONE, null, REJECT_EMPTY).present());
    assertEquals(List.of(1, 2), normalize(RANGE, List.of(1, 2), REJECT_EMPTY).value());
    assertError(RANGE_SIZE_INVALID, () -> normalize(RANGE, List.of(1), REJECT_EMPTY));
    assertError(RANGE_ORDER_INVALID, () -> normalize(RANGE, List.of(2, 1), REJECT_EMPTY));
}
```

在 `TermRegistryTest` 断言 `in` 为 `COLLECTION`、`between` 为 `RANGE`、`is-null` 为 `NONE`，并断言 `TermHandler.simple("user-in-org")` 默认为 `SCALAR`。另用 `ConditionGroupTest` 证明显式声明为 `SCALAR_OR_COLLECTION` 的扩展 term 能接收单值和集合。

- [ ] **Step 2: 运行测试确认 RED**

```powershell
mvn -pl flying-orm-core "-Dtest=ConditionValueNormalizerTest,TermRegistryTest" test
```

Expected: 测试编译失败，缺少新类型和 `TermHandler.shape()`；失败原因不能来自已有测试。

- [ ] **Step 3: 实现最小 core 模型**

`TermHandler` 保持兼容，并增加：

```java
default ConditionValueShape shape() {
    return ConditionValueShape.SCALAR;
}

static TermHandler simple(String id, ConditionValueShape shape) {
    return new SimpleTermHandler(id, shape);
}
```

`SimpleTermHandler` 改成 `record SimpleTermHandler(String id, ConditionValueShape shape)`，保留单参数构造器并默认 `SCALAR`。`TermRegistry.standard()` 使用不可变单例显式登记：

```java
NONE: is-null, is-not-null
SCALAR: =, !=, <>, >, >=, <, <=, like, not-like
COLLECTION: in, not-in
RANGE: between, not-between
```

`ConditionValueNormalizer` 必须满足：

```java
Result normalize(Shape.NONE, null, policy)       -> present(null)
Result normalize(Shape.NONE, nonEmpty, policy)   -> SHAPE_NOT_ALLOWED
Result normalize(Shape.SCALAR, blank, IGNORE)    -> ignored
Result normalize(Shape.SCALAR, blank, REJECT)    -> BLANK_VALUE
Result normalize(Shape.COLLECTION, values, any)  -> immutable cleaned List
Result normalize(Shape.RANGE, twoValues, any)     -> immutable two-value List
```

数组和 `Iterable` 只遍历一次。区间非空但数量不是 2 时总是失败；一端为空时 `IGNORE_EMPTY` 忽略整个区间，`REJECT_EMPTY` 按原值抛 `NULL_VALUE` 或 `BLANK_VALUE`；两端必须同类型且实现 `Comparable`。

- [ ] **Step 4: 运行聚焦测试确认 GREEN**

```powershell
mvn -pl flying-orm-core "-Dtest=ConditionValueNormalizerTest,TermRegistryTest" test
```

Expected: 两个测试类全部通过，无 failure/error。

- [ ] **Step 5: 清理并提交 Task 1**

```powershell
git diff --check
git add flying-orm-core/src/main/java/com/flying/orm/core/condition flying-orm-core/src/test/java/com/flying/orm/core/condition
git commit -m "Add unified condition value model"
```

提交前确认没有未使用 import、变量，也没有把测试外的无关文件加入提交。

---

### Task 2: 严格 DSL、可选 DSL 与标准 SQL term

**Files:**
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/condition/ConditionGroup.java`
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/sql/render/SqlTermHandler.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/operator/WhereDsl.java`
- Modify: `flying-orm-core/src/test/java/com/flying/orm/core/condition/ConditionGroupTest.java`
- Modify: `flying-orm-core/src/test/java/com/flying/orm/core/sql/render/SqlRendererTest.java`
- Modify: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/operator/DatabaseOperatorTest.java`

**Interfaces:**
- Consumes: Task 1 的 normalizer、标准 `TermRegistry` 和值形状。
- Produces: `ConditionGroup.and(TermRegistry)`、`or(TermRegistry)`。
- Produces: `Builder.whereIfPresent(...)`、`whereNull(...)`、`whereNotNull(...)`。
- Produces: `WhereDsl.isIfPresent(...)`、`termIfPresent(...)`、`isNull(...)`、`isNotNull(...)`。
- Produces: 标准 SQL term：`!=`、`<>`、`not-like`、`not-in`、`between`、`not-between`、`is-null`、`is-not-null`。

- [ ] **Step 1: 写 DSL 和 renderer 失败测试**

在 `ConditionGroupTest` 增加：

```java
ConditionGroup group = ConditionGroup.and()
        .where("name", "like", "  张三  ")
        .whereIfPresent("status", "=", "   ")
        .whereNull("deleted_at")
        .build();

assertEquals(2, group.children().size());
assertEquals("张三", ((TermCondition) group.children().get(0)).value());
assertNull(((TermCondition) group.children().get(1)).value());
assertThrows(ConditionValueException.class,
             () -> ConditionGroup.and().where("name", "=", " "));
```

在 `SqlRendererTest` 用一个条件组同时断言否定、区间和 NULL SQL：

```java
ConditionGroup where = ConditionGroup.and()
        .where("status", "<>", "disabled")
        .where("id", "not-in", List.of(1, 2))
        .where("score", "between", List.of(60, 100))
        .whereNotNull("updated_at")
        .build();
```

Expected SQL: `status <> ? and id not in (?, ?) and score between ? and ? and updated_at is not null`，参数顺序为 `disabled, 1, 2, 60, 100`。

- [ ] **Step 2: 运行测试确认 RED**

```powershell
mvn -pl flying-orm-rdb -am "-Dtest=ConditionGroupTest,SqlRendererTest,DatabaseOperatorTest" "-Dsurefire.failIfNoSpecifiedTests=false" test
```

Expected: 缺少新 DSL 方法或标准 SQL handler。

- [ ] **Step 3: 接入 ConditionGroup.Builder**

`where(...)` 查找 term 形状并使用 `REJECT_EMPTY`；`whereIfPresent(...)` 使用 `IGNORE_EMPTY`。`Result.present()` 为 false 时不添加节点。`whereNull` 和 `whereNotNull` 分别委托 `is-null`、`is-not-null`。嵌套 `and/or` 必须传递同一个扩展 `TermRegistry`，空嵌套组不加入父组。

构建条件时先查标准 term，再查当前 renderer 提供的扩展 term；都找不到时按 `SCALAR` 校验，渲染阶段会明确报告 term 未注册。

- [ ] **Step 4: 补齐标准 SQL handler 和 operator 短 DSL**

所有 handler 使用参数化 SQL；`NONE` 不创建参数，`COLLECTION` 和 `RANGE` 只消费已归一化的不可变 List。`WhereDsl` 方法只做字段标识符校验并委托 core builder，不复制空值逻辑。

- [ ] **Step 5: 运行聚焦测试确认 GREEN**

```powershell
mvn -pl flying-orm-rdb -am "-Dtest=ConditionGroupTest,SqlRendererTest,DatabaseOperatorTest" "-Dsurefire.failIfNoSpecifiedTests=false" test
```

Expected: 三个测试类通过；原 `keepsNullValueInSqlParameters` 测试删除或改为断言严格失败和 `is-null` 迁移行为。

- [ ] **Step 6: 清理并提交 Task 2**

```powershell
git diff --check
git add flying-orm-core/src flying-orm-rdb/src/main/java/com/flying/orm/rdb/operator/WhereDsl.java flying-orm-rdb/src/test/java/com/flying/orm/rdb/operator/DatabaseOperatorTest.java
git commit -m "Apply condition values to Java DSL"
```

---

### Task 3: 参数驱动与前端结构化条件统一接入

**Files:**
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/param/ParameterConditionCompiler.java`
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/condition/StructuredConditionPolicy.java`
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/condition/StructuredConditionCompiler.java`
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/condition/StructuredConditionErrorCode.java`
- Modify: `flying-orm-core/src/test/java/com/flying/orm/core/param/ParameterConditionCompilerTest.java`
- Modify: `flying-orm-core/src/test/java/com/flying/orm/core/condition/StructuredConditionCompilerTest.java`

**Interfaces:**
- Consumes: Task 1 normalizer 和 term registry，Task 2 严格 AST 行为。
- Produces: `ParameterConditionCompiler.Builder.terms(TermRegistry)`。
- Produces: `StructuredConditionPolicy.Builder.terms(TermRegistry)` 和 `termRegistry()`。
- Produces: 前端 range 错误码 `VALUE_RANGE_SIZE_INVALID`、`VALUE_RANGE_TYPE_MISMATCH`、`VALUE_RANGE_ORDER_INVALID`。

- [ ] **Step 1: 写参数驱动失败测试**

增加一个测试证明字符串和集合都被清理，清理后为空的条件被忽略：

```java
ConditionGroup result = compiler.compile(Map.of(
        "name", "  张三  ",
        "ids", List.of(" ", " u1 ", "u2"),
        "status", "   "));

assertTerms(result,
            List.of("name", "id"),
            List.of("张三", List.of("u1", "u2")));
```

自定义 `user-in-org` 不声明形状时继续按标量工作；显式注册 `COLLECTION` 的业务 term 必须清理集合。

- [ ] **Step 2: 写前端错误与 path 失败测试**

覆盖三个代表场景：

```java
blank scalar       -> VALUE_BLANK, conditions[0].value
range size != 2    -> VALUE_RANGE_SIZE_INVALID, conditions[0].value
range order bad    -> VALUE_RANGE_ORDER_INVALID, conditions[0].value
```

再增加 `is-null` 输入 value 为 null 成功，以及 `is-null` 携带 `"ignored"` 返回 `VALUE_SHAPE_NOT_ALLOWED`。

- [ ] **Step 3: 运行测试确认 RED**

```powershell
mvn -pl flying-orm-core "-Dtest=ParameterConditionCompilerTest,StructuredConditionCompilerTest" test
```

Expected: 归一化后的值或新错误码断言失败。

- [ ] **Step 4: 替换参数编译器私有空值逻辑**

保留默认值和 converter 顺序：先取请求值，空时尝试默认值，再转换，最后按 term 形状使用 `IGNORE_EMPTY` 归一化。删除 `isEmptyValue` 和 `EmptyValue` 重复实现；用 normalizer 的 `Result` 决定是否加入条件。

- [ ] **Step 5: 接入结构化条件编译器**

字段白名单、operator 白名单、深度、节点数、字符串长度和集合大小限制保持不变。operator alias 解析后，从 policy registry 获取值形状，再使用 `REJECT_EMPTY` normalizer；转换数据库字段类型后再做 range 类型和顺序校验。

将 `ConditionValueException.Error` 映射为稳定前端错误码。集合元素的类型或形状错误 path 使用 `conditions[n].value[i]`；被清理的 null/空白元素不单独报错。

- [ ] **Step 6: 运行聚焦测试确认 GREEN**

```powershell
mvn -pl flying-orm-core "-Dtest=ParameterConditionCompilerTest,StructuredConditionCompilerTest" test
```

Expected: 两个测试类通过，现有字段/operator/深度保护用例不回归。

- [ ] **Step 7: 清理并提交 Task 3**

```powershell
git diff --check
git add flying-orm-core/src/main/java/com/flying/orm/core flying-orm-core/src/test/java/com/flying/orm/core
git commit -m "Unify external condition values"
```

---

### Task 4: 安全 Scope 复核、文档和收口验证

**Files:**
- Inspect without planned changes: `flying-orm-core/src/main/java/com/flying/orm/core/scope/TenantScope.java`
- Inspect without planned changes: `flying-orm-core/src/main/java/com/flying/orm/core/scope/DataScope.java`
- Inspect without planned changes: `flying-orm-core/src/main/java/com/flying/orm/core/scope/TimeScope.java`
- Modify: `flying-orm-core/src/test/java/com/flying/orm/core/scope/DataScopeTest.java`
- Modify: `docs/requirements/index.md`
- Modify: `docs/source-feature-matrix.md`
- Modify: `docs/flying-orm-phased-implementation-plan.md`
- Modify: `docs/target-api-examples.md`

**Interfaces:**
- Consumes: 严格 `ConditionGroup.Builder.where(...)` 和已完成的 TimeScope。
- Produces: 安全 Scope 空值 SQL 前失败的回归证据，以及公开迁移示例。

- [ ] **Step 1: 增加一个安全 Scope 组合测试**

测试同一组中 `TenantScope + DataScope + TimeScope` 的有效值正常组合；空租户、空数据范围值和空时间边界分别在创建或构建 AST 时抛出，不能因为 `IGNORE_EMPTY` 消失。

```java
assertThrows(IllegalArgumentException.class, () -> DataScope.tenant("tenant_id", " "));
assertThrows(ConditionValueException.class,
             () -> DataScope.where(ConditionGroup.and().where("org_id", "=", " ").build()));
assertThrows(NullPointerException.class, () -> TimeScope.from("created_at", null));
```

如果现有 Scope 构造器已经在更早位置抛出同等明确异常，只保留测试，不重复增加校验。

- [ ] **Step 2: 运行 Scope 和端到端 renderer 测试**

```powershell
mvn -pl flying-orm-rdb -am "-Dtest=DataScopeTest,TimeScopeTest,SqlRendererTest,ReactiveFormClientTest" "-Dsurefire.failIfNoSpecifiedTests=false" test
```

Expected: 四个测试类通过；最终 SQL 参数顺序保持业务 where、tenant、time 边界的现有顺序。

- [ ] **Step 3: 更新文档和迁移示例**

文档明确写出：

```java
where("name", "like", request.name())       // 严格，空值失败
whereIfPresent("name", "like", request.name()) // 可选，空值忽略
whereNull("deleted_at")                       // 数据库 NULL
```

同时记录参数驱动会 `strip()` 字符串和清理集合元素，前端结构化条件空值返回稳定错误码；将 R-007 对应条目标记为完成。

- [ ] **Step 4: 运行最终聚焦测试**

```powershell
mvn -pl flying-orm-rdb -am "-Dtest=ConditionValueNormalizerTest,TermRegistryTest,ConditionGroupTest,ParameterConditionCompilerTest,StructuredConditionCompilerTest,SqlRendererTest,DataScopeTest,TimeScopeTest,DatabaseOperatorTest,ReactiveFormClientTest" "-Dsurefire.failIfNoSpecifiedTests=false" test
```

Expected: 所列测试全部通过，无 failure/error。

- [ ] **Step 5: 运行全模块编译打包**

```powershell
mvn -DskipTests package
```

Expected: reactor 所有模块 `SUCCESS`。

- [ ] **Step 6: 清理、提交、推送和刷新索引**

```powershell
git diff --check
git status --short
git add docs flying-orm-core/src flying-orm-rdb/src
git commit -m "Document unified condition behavior"
git push origin main
```

推送后刷新 `flying-orm` codebase-memory 索引，确认工作区为空。本计划没有 POM、模块或 Spring 适配器变更，因此不做 Spring 扫描。

## 后续主线

本计划完成后直接继续 R-020：Repository/Operator 下 TenantScope、DataScope、FieldScope、TimeScope、逻辑删除和乐观锁的组合边界复核，然后补业务授权协作文档。
