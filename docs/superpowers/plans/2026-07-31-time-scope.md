# TimeScope Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 flying-orm 增加可与 TenantScope、DataScope、FieldScope 安全组合的参数化时间范围，并兼容左闭右开、全闭和单边窗口。

**Architecture:** `TimeScope` 作为 `flying-orm-core` 中的不可变服务端范围模型，把时间边界编译成普通 `ConditionGroup`；`DataScope.time(...)` 将它接入现有 AND 合并链路。SQL 渲染层只补充标准 `>=` 和 `<=` term handler，R2DBC、同步桥接、Repository、Operator 和方言层不增加分支。

**Tech Stack:** Java 21、Maven、JUnit 5、Reactor Test、flying-orm 条件 AST 与参数化 SQL renderer。

## Global Constraints

- 主项目不依赖 Spring，也不增加新模块或第三方依赖。
- `between(...)` 固定表示 `[startInclusive, endExclusive)`；`closed(...)` 固定表示 `[startInclusive, endInclusive]`。
- `from(...)` 表示 `[startInclusive, +infinity)`；`before(...)` 表示 `(-infinity, endExclusive)`。
- 所有边界值走 SQL 参数绑定，不能拼接进 SQL 文本。
- 双边窗口必须使用同一种可比较类型；空窗口或反向窗口必须在 SQL 前失败。
- 新增或修改的 Java 类型、公共方法和测试方法使用自然、明确的中文注释；删除无用 import、变量和重复校验。
- 测试保持少量聚焦，不增加真实数据库测试，也不重复扫描 Spring。
- 设计依据：`docs/superpowers/specs/2026-07-31-time-scope-design.md`。

## File Map

- Create: `flying-orm-core/src/main/java/com/flying/orm/core/scope/TimeScope.java`，负责时间边界校验和条件 AST 生成。
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/scope/DataScope.java`，增加 `time(TimeScope)` 组合入口。
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/sql/render/SqlTermHandler.java`，增加 `>=` 和 `<=` 标准 handler。
- Create: `flying-orm-core/src/test/java/com/flying/orm/core/scope/TimeScopeTest.java`，验证时间窗口语义和前置校验。
- Modify: `flying-orm-core/src/test/java/com/flying/orm/core/scope/DataScopeTest.java`，验证租户、时间和字段范围组合。
- Modify: `flying-orm-core/src/test/java/com/flying/orm/core/sql/render/SqlRendererTest.java`，验证新比较 term 的参数化 SQL。
- Modify: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/form/ReactiveFormClientTest.java`，验证动态表单最终 SQL、字段裁剪和参数顺序。
- Modify: `docs/requirements/index.md`、`docs/source-feature-matrix.md`、`docs/target-api-examples.md`，同步 R-020 状态和使用示例。

---

### Task 1: 标准包含边界比较 term

**Files:**
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/sql/render/SqlTermHandler.java`
- Modify: `flying-orm-core/src/test/java/com/flying/orm/core/sql/render/SqlRendererTest.java`

**Interfaces:**
- Consumes: `SqlTermHandler.of(String, SqlTermRenderer)`、`SqlRenderer.Builder.addDefaultTerms()`。
- Produces: `SqlTermHandler.greaterThanOrEqual()`、`SqlTermHandler.lessThanOrEqual()`，term id 分别为 `>=`、`<=`；`SqlTermHandler.defaults()` 默认注册两者。

- [ ] **Step 1: 扩展默认 term 测试，先表达包含边界 SQL**

在 `rendersWhereConditionWithDefaultTerms()` 中加入两个条件并更新手工推导的 SQL 与参数断言：

```java
SqlFragment fragment = renderer.renderWhere(ConditionGroup.and()
                                                      .where("age", ">", 18)
                                                      .where("score", "<", 100)
                                                      .where("created_at", ">=", "2026-07-01T00:00:00")
                                                      .where("updated_at", "<=", "2026-07-31T23:59:59")
                                                      .where("name", "like", "王%")
                                                      .where("id", "in", List.of("u1", "u2"))
                                                      .build());

assertEquals("age > ? and score < ? and created_at >= ? and updated_at <= ? and name like ? and id in (?, ?)",
             fragment.sql());
assertEquals(List.of(18,
                     100,
                     "2026-07-01T00:00:00",
                     "2026-07-31T23:59:59",
                     "王%",
                     "u1",
                     "u2"),
             fragment.parameters());
```

- [ ] **Step 2: 运行测试并确认 RED**

Run:

```powershell
mvn -pl flying-orm-core "-Dtest=SqlRendererTest" test
```

Expected: `rendersWhereConditionWithDefaultTerms` 失败，错误说明没有为 `>=` 注册 SQL term handler；不能是编译错误或测试夹具错误。

- [ ] **Step 3: 实现两个标准比较 handler**

在 `greaterThan()` 与 `lessThan()` 附近增加：

```java
/**
 * 创建大于等于 term handler，时间范围和数字范围都可以复用。
 *
 * @return 大于等于 term handler
 */
static SqlTermHandler greaterThanOrEqual() {
    return of(">=", (term, context) -> SqlFragment.of(context.identifier(term.field()) + " >= ?", term.value()));
}

/**
 * 创建小于等于 term handler，主要用于显式全闭区间。
 *
 * @return 小于等于 term handler
 */
static SqlTermHandler lessThanOrEqual() {
    return of("<=", (term, context) -> SqlFragment.of(context.identifier(term.field()) + " <= ?", term.value()));
}
```

将默认集合改为：

```java
static List<SqlTermHandler> defaults() {
    return List.of(equalsTo(),
                   greaterThan(),
                   greaterThanOrEqual(),
                   lessThan(),
                   lessThanOrEqual(),
                   like(),
                   in());
}
```

- [ ] **Step 4: 运行聚焦测试并确认 GREEN**

Run:

```powershell
mvn -pl flying-orm-core "-Dtest=SqlRendererTest" test
```

Expected: `SqlRendererTest` 全部通过，输出中无 failure 或 error。

- [ ] **Step 5: 清理并提交 Task 1**

先执行 `git diff --check`，确认无无用 import 或变量，再提交：

```powershell
git add flying-orm-core/src/main/java/com/flying/orm/core/sql/render/SqlTermHandler.java flying-orm-core/src/test/java/com/flying/orm/core/sql/render/SqlRendererTest.java
git commit -m "Add inclusive comparison terms"
```

---

### Task 2: TimeScope 模型与统一 Scope 合并

**Files:**
- Create: `flying-orm-core/src/main/java/com/flying/orm/core/scope/TimeScope.java`
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/scope/DataScope.java`
- Create: `flying-orm-core/src/test/java/com/flying/orm/core/scope/TimeScopeTest.java`
- Modify: `flying-orm-core/src/test/java/com/flying/orm/core/scope/DataScopeTest.java`
- Modify: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/form/ReactiveFormClientTest.java`

**Interfaces:**
- Consumes: Task 1 提供的默认 `>=`、`<=` term handler，现有 `ConditionGroup.Builder.where(...)` 和 `DataScope.and(...)`。
- Produces: `TimeScope.between(String, Object, Object)`、`closed(...)`、`from(...)`、`before(...)`、`toCondition()`，以及 `DataScope.time(TimeScope)`。

- [ ] **Step 1: 写 TimeScope 行为和非法区间测试**

创建 `TimeScopeTest.java`，类型 Javadoc 使用 `@author wangr`、`@date 2026-07-31` 和当前版本号。核心测试如下：

```java
@Test
void buildsHalfOpenClosedAndSingleBoundaryConditions() {
    LocalDateTime start = LocalDateTime.of(2026, 7, 1, 0, 0);
    LocalDateTime end = LocalDateTime.of(2026, 8, 1, 0, 0);

    assertTerms(TimeScope.between("created_at", start, end), List.of(">=", "<"), List.of(start, end));
    assertTerms(TimeScope.closed("created_at", start, end), List.of(">=", "<="), List.of(start, end));
    assertTerms(TimeScope.from("created_at", start), List.of(">="), List.of(start));
    assertTerms(TimeScope.before("created_at", end), List.of("<"), List.of(end));
}

@Test
void rejectsEmptyReversedAndIncompatibleWindows() {
    LocalDateTime start = LocalDateTime.of(2026, 7, 1, 0, 0);
    LocalDateTime end = LocalDateTime.of(2026, 8, 1, 0, 0);

    assertThrows(IllegalArgumentException.class, () -> TimeScope.between("created_at", start, start));
    assertThrows(IllegalArgumentException.class, () -> TimeScope.closed("created_at", end, start));
    assertThrows(IllegalArgumentException.class, () -> TimeScope.between("created_at", start, end.toLocalDate()));
}

private static void assertTerms(TimeScope scope, List<String> operators, List<?> values) {
    List<TermCondition> terms = scope.toCondition()
                                     .children()
                                     .stream()
                                     .map(TermCondition.class::cast)
                                     .toList();
    assertEquals(operators, terms.stream().map(TermCondition::operator).toList());
    assertEquals(values, terms.stream().map(TermCondition::value).toList());
    assertTrue(terms.stream().allMatch(term -> term.field().equals("created_at")));
}
```

- [ ] **Step 2: 写 DataScope 组合测试**

在 `DataScopeTest` 增加一个测试，验证时间条件不会清掉租户条件，字段白名单也保留：

```java
@Test
void tenantTimeAndFieldScopesComposeWithoutWideningAccess() {
    LocalDateTime start = LocalDateTime.of(2026, 7, 1, 0, 0);
    LocalDateTime end = LocalDateTime.of(2026, 8, 1, 0, 0);

    DataScope scope = DataScope.tenant("tenant_id", "t1")
                               .and(DataScope.time(TimeScope.between("created_at", start, end)))
                               .withFields(FieldScope.readable("id", "name", "created_at"));

    assertEquals(3, scope.condition().orElseThrow().children().size());
    assertEquals("t1", scope.tenantScope("tenant_id").orElseThrow().value());
    assertTrue(scope.fields().canRead("created_at"));
    assertFalse(scope.fields().canRead("secret"));
}
```

- [ ] **Step 3: 写动态表单端到端测试**

在 `ReactiveFormClientTest` 增加 `LocalDateTime` 和 `TimeScope` import，并新增独立表单夹具，避免改变其他测试 SQL：

```java
@Test
void tenantTimeAndFieldScopesReachDynamicFormSqlTogether() {
    RecordingSqlExecutor executor = new RecordingSqlExecutor();
    LocalDateTime start = LocalDateTime.of(2026, 7, 1, 0, 0);
    LocalDateTime end = LocalDateTime.of(2026, 8, 1, 0, 0);
    DataScope scope = DataScope.tenant("tenant_id", "t1")
                               .and(DataScope.time(TimeScope.between("created_at", start, end)))
                               .withFields(FieldScope.readable("id", "name", "created_at"));
    ReactiveFormClient client = ReactiveFormClient.create(
            executor,
            FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(),
                    RdbDialect.h2()))
                                                  .withDefaultDataScope(scope);

    StepVerifier.create(client.select(timeScopedForm(),
                                      ConditionGroup.and().where("id", "=", "u1").build()))
                .expectNextCount(1)
                .verifyComplete();

    assertEquals("select id, name, created_at from Users where id = ? and tenant_id = ? and created_at >= ? and created_at < ?",
                 executor.request().sql());
    assertEquals(List.of("u1", "t1", start, end), executor.request().parameters());
}

private static DynamicForm timeScopedForm() {
    return DynamicForm.builder("userForm", "Users")
                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                      .addField(DynamicField.of("name", "VARCHAR"))
                      .addField(DynamicField.of("tenant_id", "VARCHAR"))
                      .addField(DynamicField.of("created_at", "TIMESTAMP"))
                      .tenant("tenant_id", TenantStrategy.AUTO)
                      .build();
}
```

- [ ] **Step 4: 运行测试并确认 RED**

Run:

```powershell
mvn -pl flying-orm-rdb -am "-Dtest=TimeScopeTest,DataScopeTest,ReactiveFormClientTest" "-Dsurefire.failIfNoSpecifiedTests=false" test
```

Expected: 测试编译失败，明确指出 `TimeScope` 和 `DataScope.time(...)` 尚不存在；不能由无关 import 或已有测试失败造成。

- [ ] **Step 5: 实现 TimeScope**

创建 `TimeScope.java`，使用 record 保持不可变，并把开放一侧表示为“值和边界类型同时为 null”。公共工厂不要求调用方传 null：

```java
package com.flying.orm.core.scope;

import com.flying.orm.core.condition.ConditionGroup;

import java.util.Objects;

/**
 * 服务端时间范围。它只保存已经算清楚的边界，并把边界转换成参数化条件。
 *
 * @param field         时间字段
 * @param start         开始值；没有开始边界时为 null
 * @param startBoundary 开始边界类型；没有开始边界时为 null
 * @param end           结束值；没有结束边界时为 null
 * @param endBoundary   结束边界类型；没有结束边界时为 null
 * @author wangr
 * @date 2026-07-31
 * @version v2.0
 */
public record TimeScope(String field,
                        Object start,
                        Boundary startBoundary,
                        Object end,
                        Boundary endBoundary) {

    public TimeScope {
        field = requireText(field, "time field");
        requireMatchingBoundary(start, startBoundary, "start");
        requireMatchingBoundary(end, endBoundary, "end");
        if (start == null && end == null) {
            throw new IllegalArgumentException("time scope needs at least one boundary");
        }
        validateRange(start, startBoundary, end, endBoundary);
    }

    public static TimeScope between(String field, Object startInclusive, Object endExclusive) {
        return new TimeScope(field,
                             Objects.requireNonNull(startInclusive, "time start must not be null"),
                             Boundary.INCLUSIVE,
                             Objects.requireNonNull(endExclusive, "time end must not be null"),
                             Boundary.EXCLUSIVE);
    }

    public static TimeScope closed(String field, Object startInclusive, Object endInclusive) {
        return new TimeScope(field,
                             Objects.requireNonNull(startInclusive, "time start must not be null"),
                             Boundary.INCLUSIVE,
                             Objects.requireNonNull(endInclusive, "time end must not be null"),
                             Boundary.INCLUSIVE);
    }

    public static TimeScope from(String field, Object startInclusive) {
        return new TimeScope(field,
                             Objects.requireNonNull(startInclusive, "time start must not be null"),
                             Boundary.INCLUSIVE,
                             null,
                             null);
    }

    public static TimeScope before(String field, Object endExclusive) {
        return new TimeScope(field,
                             null,
                             null,
                             Objects.requireNonNull(endExclusive, "time end must not be null"),
                             Boundary.EXCLUSIVE);
    }

    public ConditionGroup toCondition() {
        ConditionGroup.Builder builder = ConditionGroup.and();
        if (start != null) {
            builder.where(field, startBoundary == Boundary.INCLUSIVE ? ">=" : ">", start);
        }
        if (end != null) {
            builder.where(field, endBoundary == Boundary.INCLUSIVE ? "<=" : "<", end);
        }
        return builder.build();
    }

    public enum Boundary {
        INCLUSIVE,
        EXCLUSIVE
    }

    private static void requireMatchingBoundary(Object value, Boundary boundary, String name) {
        if ((value == null) != (boundary == null)) {
            throw new IllegalArgumentException("time " + name + " value and boundary must be provided together");
        }
    }

    private static void validateRange(Object start, Boundary startBoundary, Object end, Boundary endBoundary) {
        if (start == null || end == null) {
            return;
        }
        if (!start.getClass().equals(end.getClass()) || !(start instanceof Comparable<?> comparable)) {
            throw new IllegalArgumentException("time boundaries must use the same comparable type");
        }
        @SuppressWarnings("unchecked")
        int compared = ((Comparable<Object>) comparable).compareTo(end);
        boolean emptyAtSameValue = compared == 0
                && (startBoundary == Boundary.EXCLUSIVE || endBoundary == Boundary.EXCLUSIVE);
        if (compared > 0 || emptyAtSameValue) {
            throw new IllegalArgumentException("time start must be before time end");
        }
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }
}
```

- [ ] **Step 6: 把 TimeScope 接入 DataScope**

在 `DataScope.tenant(...)` 附近增加：

```java
/**
 * 把服务端时间窗口转换成普通数据范围，后续和租户、组织等范围继续 AND。
 *
 * @param scope 时间范围
 * @return 可继续组合的数据范围
 */
public static DataScope time(TimeScope scope) {
    return where(Objects.requireNonNull(scope, "time scope must not be null").toCondition());
}
```

不要给 `DataScope` 增加额外 time 字段；现有 `condition` 已能完整承接，避免复制状态。

- [ ] **Step 7: 运行聚焦测试并确认 GREEN**

Run:

```powershell
mvn -pl flying-orm-rdb -am "-Dtest=TimeScopeTest,DataScopeTest,ReactiveFormClientTest" "-Dsurefire.failIfNoSpecifiedTests=false" test
```

Expected: 三个测试类全部通过，动态表单 SQL 参数顺序严格为业务 where、tenant、start、end。

- [ ] **Step 8: 清理并提交 Task 2**

执行 `git diff --check`，确认新文件中文 Javadoc 完整、没有无用 import/变量，然后提交：

```powershell
git add flying-orm-core/src/main/java/com/flying/orm/core/scope/TimeScope.java flying-orm-core/src/main/java/com/flying/orm/core/scope/DataScope.java flying-orm-core/src/test/java/com/flying/orm/core/scope/TimeScopeTest.java flying-orm-core/src/test/java/com/flying/orm/core/scope/DataScopeTest.java flying-orm-rdb/src/test/java/com/flying/orm/rdb/form/ReactiveFormClientTest.java
git commit -m "Add composable time scopes"
```

---

### Task 3: R-020 文档收口与全模块验证

**Files:**
- Modify: `docs/requirements/index.md`
- Modify: `docs/requirements/index.md`
- Modify: `docs/source-feature-matrix.md`
- Modify: `docs/target-api-examples.md`

**Interfaces:**
- Consumes: Task 2 已验证的 `TimeScope` 和 `DataScope.time(...)` API。
- Produces: 可直接用于上层服务的时间范围示例，以及更新后的 R-020 状态。

- [ ] **Step 1: 更新阶段计划和需求索引**

在 Phase 12.5 / R-020 中把 TimeScope 标为完成，明确：

```text
TimeScope 已支持左闭右开、全闭和单边时间窗口；时间条件与 TenantScope、DataScope、FieldScope 统一 AND，所有值使用参数绑定。时间语义和时区由上层计算，flying-orm 不依赖 Spring。
```

后续 R-020 主线改为 Repository/Operator 组合边界复核和业务授权协作文档，不再把 TimeScope 列为未完成项。

- [ ] **Step 2: 更新能力矩阵和 API 示例**

在 `target-api-examples.md` 的 DataScope 段落加入：

```java
LocalDateTime start = LocalDateTime.of(2026, 7, 1, 0, 0);
LocalDateTime end = LocalDateTime.of(2026, 8, 1, 0, 0);

DataScope scope = DataScope.tenant("tenant_id", currentTenantId)
        .and(DataScope.time(TimeScope.between("created_at", start, end)))
        .withFields(FieldScope.readable("id", "name", "created_at"));
```

紧接示例说明：`between` 为 `[start, end)`，`closed` 为 `[start, end]`，`from` 和 `before` 为单边窗口；时间列应按实际查询模式建立数据库索引。

- [ ] **Step 3: 运行最终聚焦测试**

Run:

```powershell
mvn -pl flying-orm-rdb -am "-Dtest=TimeScopeTest,DataScopeTest,SqlRendererTest,ReactiveFormClientTest" "-Dsurefire.failIfNoSpecifiedTests=false" test
```

Expected: 四个测试类全部通过，无 failure、error 或 skipped。

- [ ] **Step 4: 运行全模块编译打包**

Run:

```powershell
mvn -DskipTests package
```

Expected: flying-orm、core、rdb、testkit、benchmark 五个模块全部 `SUCCESS`。

- [ ] **Step 5: 检查范围并提交文档**

分别执行 `git diff --check`、`git status --short` 和 `git diff --stat`。只应看到本计划列出的文档变更，然后提交：

```powershell
git add docs/requirements/index.md docs/source-feature-matrix.md docs/target-api-examples.md
git commit -m "Document TimeScope usage"
```

- [ ] **Step 6: 推送并刷新代码索引**

```powershell
git push origin main
```

随后刷新 `flying-orm` 的 codebase-memory 索引，并确认 `git status --short` 为空。此次没有修改 POM、模块或 Spring 适配器，所以不执行 Spring 扫描。
