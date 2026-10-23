package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.QueryRelation;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.page.CursorPosition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUseOrigin;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.lock.ReadLock;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.condition.StructuredConditionException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class OrdinaryRelationQueryTest {
    private final AtomicInteger resolutions = new AtomicInteger();
    private final DynamicForm department = DynamicForm.builder("departments", "departments")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("name", "VARCHAR")).build();
    private final DynamicForm users = DynamicForm.builder("users", "users")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("departmentId", "BIGINT"))
            .relation("department", QueryRelation.of(() -> {
                resolutions.incrementAndGet();
                return department;
            }, "departmentId", "id")).build();

    @Test
    void relationAssemblyBelongsToTheJoinPlannerNotScopeSupport() throws Exception {
        for (java.lang.reflect.Method method : FormScopeSupport.class.getDeclaredMethods()) {
            assertFalse(method.getName().startsWith("related"), "Scope must not own relation query assembly");
        }
        assertNotNull(JoinQueryPlanner.class.getDeclaredMethod("relatedQuery", QuerySpec.class, PageQuery.class));
        assertNotNull(JoinQueryPlanner.class.getDeclaredMethod("relatedKeysetQuery", QuerySpec.class, KeysetPageQuery.class));
    }

    @Test
    void ordinarySelectAndFirstUseDeclaredRelationsWithoutExplicitJoin() {
        QuerySpec query = QuerySpec.of(users, ConditionGroup.or().where("id", "=", 1L)
                .where("department.name", "=", "x' OR 1=1 --").build());
        FormReadPlanner.PlannedQuery selected = planner().select(query);
        assertEquals(1, resolutions.get());
        assertNotNull(selected.joinSpec());
        assertTrue(selected.request().sql().contains("left outer join"));
        assertFalse(selected.request().sql().contains("x' OR 1=1 --"));
        assertEquals(List.of(1L, "x' OR 1=1 --"), selected.request().parameters());
        assertEquals(List.of("id", "departmentId"), selected.form().fields().stream().map(DynamicField::name).toList());
        assertNotNull(planner().first(query).joinSpec());
        assertEquals(2, resolutions.get());
    }

    @Test
    void structuredPageAndCountShareRelationsAndTypedParameters() {
        QuerySpec query = QuerySpec.structured(users,
                StructuredConditionInput.term("department.id", "eq", "12"));
        FormReadPlanner.PlannedQuery page = planner().page(query,
                new PageQuery(1, 5, List.of(new PageSort("department.id", PageSort.Direction.ASC))));
        assertEquals(1, resolutions.get());
        assertEquals(List.of(12L), page.countRequest().parameters());
        assertTrue(page.request().sql().contains("\"t1\".\"id\" asc"));
        FormReadPlanner.PlannedCount count = planner().count(query, FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults(), false);
        assertEquals(page.countRequest(), count.request());
        assertEquals(2, resolutions.get());
    }

    @Test
    void unrelatedQueryDoesNotResolveOrBuildJoinState() {
        FormReadPlanner.PlannedQuery selected = planner().select(QuerySpec.of(users,
                ConditionGroup.and().where("id", "=", 1L).build()));
        assertNull(selected.joinSpec());
        assertEquals(0, resolutions.get());
        assertFalse(selected.request().sql().contains("join"));
    }

    @Test
    void relatedCountDoesNotValidateUnusedOutputColumns() {
        QuerySpec query = QuerySpec.of(users, ConditionGroup.and().where("department.id", "=", 12L).build())
                .withProjection(List.of("not_selected_by_count"), List.of());
        assertEquals(List.of(12L), planner().count(query, FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults(), true).request().parameters());
    }

    @Test
    void reactiveEntryBindsOncePerSubscriptionAndReusesPageCountSources() {
        List<SqlRequest> requests = new ArrayList<>();
        ReactiveSqlExecutor executor = (ReactiveSqlExecutor) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{ReactiveSqlExecutor.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("query")) {
                        SqlRequest request = (SqlRequest) arguments[0];
                        requests.add(request);
                        return Flux.just(DynamicRow.copyOf(request.sql().startsWith("select count")
                                ? Map.of("total", 1L) : Map.of("id", 7L, "departmentId", 12L)));
                    }
                    if (method.isDefault()) return java.lang.reflect.InvocationHandler.invokeDefault(proxy, method, arguments);
                    throw new AssertionError(method);
                });
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        QuerySpec query = QuerySpec.structured(users,
                StructuredConditionInput.term("department.id", "eq", "12"));
        Flux<DynamicRow> rows = client.select(query);
        assertEquals(0, resolutions.get());
        assertEquals(7L, rows.collectList().block().getFirst().get("id"));
        assertEquals(1, rows.collectList().block().size());
        assertEquals(2, resolutions.get());
        assertEquals(1L, client.page(query, PageQuery.of(1, 5)).block().total());
        assertEquals(3, resolutions.get());
        assertEquals(4, requests.size());
        assertTrue(requests.stream().allMatch(request -> request.sql().contains("left outer join")));
        Flux<DynamicRow> locked = client.lockingRead(query.withLock(ReadLock.update()));
        assertEquals(3, resolutions.get());
        assertEquals(1, locked.collectList().block().size());
        assertEquals(1, locked.collectList().block().size());
        assertEquals(5, resolutions.get());
        assertTrue(requests.getLast().sql().endsWith("FOR UPDATE OF \"t0\""));
    }

    @Test
    void defaultProjectionRespectsExclusionsAndFrontendRelatedFieldsRespectScope() {
        QuerySpec query = QuerySpec.of(users, ConditionGroup.and().where("department.id", "=", 12L).build())
                .withExcludedFields(List.of("departmentId"));
        assertEquals(List.of("id"), planner().selectGoverned(query, FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults()).plan().form().fields().stream().map(DynamicField::name).toList());
        FormReadPlanner restricted = new FormReadPlanner(renderer(), new FormScopeSupport(renderer(),
                StructuredConditionResolver.defaults(), DataScope.none().withFields(FieldScope.readable("id", "departmentId"))),
                SqlExecutionOptions.unlimited());
        assertThrows(StructuredConditionException.class, () -> restricted.select(QuerySpec.structured(users,
                StructuredConditionInput.term("department.name", "eq", "hidden"))));
    }

    @Test
    void relatedExpressionsBindSourcesAndPriorityValuesWithoutAddingSortColumns() {
        QuerySpec query = QuerySpec.of(users, ConditionGroup.and().build())
                .withExpression("combined", ValueExpression.field("department.id").add(ValueExpression.field("id")))
                .withOrderBy(ValueExpression.field("department.id").priority(12L), PageSort.Direction.ASC);
        FormReadPlanner.PlannedQuery plan = planner().select(query);
        assertEquals(1, resolutions.get());
        assertEquals(List.of("combined"), plan.form().fields().stream().map(DynamicField::name).toList());
        assertTrue(plan.request().sql().contains("\"t1\".\"id\" + \"t0\".\"id\""));
        assertTrue(plan.request().sql().contains("order by case"));
        assertEquals(List.of(12L), plan.request().parameters());
        SqlRequest count = planner().count(query, FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults(), false).request();
        assertTrue(count.sql().contains("left outer join"));
        assertEquals(List.of(), count.parameters());
    }

    @Test
    void relatedGroupedPageCountsGroupsAndSharesSourcesWithCount() {
        QuerySpec query = QuerySpec.of(users, ConditionGroup.and().where("department.id", ">", 1L).build())
                .withProjection(List.of("department.name"), List.of("department.name"))
                .withSorts(List.of(new PageSort("department.name", PageSort.Direction.ASC)));
        FormReadPlanner.PlannedQuery plan = planner().page(query, PageQuery.of(1, 5));
        assertEquals(1, resolutions.get());
        assertTrue(plan.request().sql().contains("group by \"t1\".\"name\" order by"));
        assertTrue(plan.countRequest().sql().contains("from (select 1 as grouped_value from"));
        assertTrue(plan.countRequest().sql().contains("group by \"t1\".\"name\""));
        assertFalse(plan.countRequest().sql().contains("order by"));
        assertEquals(List.of(1L), plan.countRequest().parameters());
        assertEquals(plan.countRequest(), planner().count(query, FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults(), true).request());
    }

    @Test
    void relatedGroupingChecksExpressionAndOrderingSources() {
        QuerySpec valid = QuerySpec.of(users, ConditionGroup.and().build())
                .withProjection(List.of("department.id"), List.of("department.id"))
                .withExpression("offset_id", ValueExpression.field("department.id").add(ValueExpression.value(2L)));
        assertTrue(planner().select(valid).request().sql().contains("group by"));
        QuerySpec invalid = valid.withExpression("root_id", ValueExpression.field("id"));
        assertThrows(IllegalArgumentException.class, () -> planner().select(invalid));
        assertThrows(IllegalArgumentException.class, () -> planner().select(valid.withSorts(
                List.of(new PageSort("id", PageSort.Direction.ASC)))));
    }

    @Test
    void relatedPriorityProjectionUsesTheActualFieldCodec() {
        DynamicField name = department.field("name");
        AtomicInteger writes = new AtomicInteger();
        ValueCodec codec = new ValueCodec() {
            public boolean supports(Class<?> type) { return type == String.class; }
            public Object write(Object value) { writes.incrementAndGet(); return "encoded:" + value; }
            public Object read(Object value, Class<?> type) { return value; }
        };
        EntityTypeMappingRegistry types = EntityTypeMappingRegistry.builder()
                .registerNamed("departmentName", String.class, name.databaseType(), codec).build();
        FormDataSqlRenderer renderer = renderer().withEntityFieldCodecs(Map.of(name, types.resolve("departmentName", String.class)));
        FormReadPlanner planner = new FormReadPlanner(renderer, new FormScopeSupport(renderer,
                StructuredConditionResolver.defaults(), DataScope.none()), SqlExecutionOptions.unlimited());
        QuerySpec query = QuerySpec.of(users, ConditionGroup.and().build())
                .withExpression("rank_value", ValueExpression.field("department.name").priority("A"));
        assertEquals(0, resolutions.get());
        assertEquals(0, writes.get());
        assertEquals(List.of("encoded:A"), planner.select(query).request().parameters());
        assertEquals(1, writes.get());
        assertEquals(1, resolutions.get());
    }

    @Test
    void relatedGroupingUsesSourcePermissionsAndCallerGroupBudget() {
        QuerySpec query = QuerySpec.of(users, ConditionGroup.and().build())
                .withProjection(List.of("department.id"), List.of("department.id"));
        assertThrows(IllegalArgumentException.class,
                () -> planner().selectGoverned(query, FieldUsePolicy.unrestricted(),
                        QueryShapeLimits.defaults().withMaxGroupCount(0)));
        JoinQuerySpec bound = planner().select(query).joinSpec();
        JoinSource root = bound.root();
        JoinSource related = bound.sources().get(1);
        FieldUsePolicy.Builder policy = FieldUsePolicy.builder()
                .joinVisibility(related.field("id"), FieldVisibility.FULL)
                .allowJoin(root.field("departmentId"), FieldUse.JOIN)
                .allowJoin(related.field("id"), FieldUse.JOIN);
        assertThrows(ScopeAccessException.class, () -> planner().selectGoverned(
                query, policy.build(), QueryShapeLimits.defaults()));
        policy.allowJoin(related.field("id"), FieldUse.GROUP);
        assertDoesNotThrow(() -> planner().selectGoverned(query, policy.build(), QueryShapeLimits.defaults()));
    }

    @Test
    void relatedReadLocksOnlyTheRootAndKeepsScopeOutsideNullableJoinSources() {
        QuerySpec query = QuerySpec.of(users, ConditionGroup.and().where("department.name", "=", "A").build())
                .withScope(DataScope.where(ConditionGroup.and().where("id", ">", 4L).build()))
                .withLock(ReadLock.updateNowait());
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormReadPlanner planner = planner(dialect);
            SqlRequest request = planner.lockingRead(query).request();
            assertEquals(List.of(4L, "A"), request.parameters());
            assertFalse(request.sql().contains("select * from"), "root scope must not turn the lock target into a derived table");
            assertTrue(request.sql().contains("left outer join"));
            String sql = request.sql();
            if (sql.contains("WITH (UPDLOCK")) {
                assertEquals(sql.indexOf("WITH (UPDLOCK"), sql.lastIndexOf("WITH (UPDLOCK"));
                assertTrue(sql.indexOf("WITH (UPDLOCK") < sql.indexOf("left outer join"));
                assertTrue(sql.contains("NOWAIT"));
            } else {
                assertTrue(sql.contains("FOR UPDATE OF "));
                assertTrue(sql.endsWith(" NOWAIT"));
            }
            SqlRequest count = planner.count(query, FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults(), false).request();
            assertFalse(count.sql().contains("FOR UPDATE"));
            assertFalse(count.sql().contains("UPDLOCK"));
        }
    }

    @Test
    void relatedLockUsesTheSameWindowAndGroupingRulesAsOrdinaryReads() {
        QuerySpec query = QuerySpec.structured(users, StructuredConditionInput.term("department.id", "eq", "12"))
                .withLock(ReadLock.updateSkipLocked());
        assertTrue(planner().first(query).request().sql().endsWith("FOR UPDATE OF \"t0\" SKIP LOCKED"));
        assertThrows(UnsupportedOperationException.class, () -> planner(RdbDialect.oracle()).first(query));
        assertThrows(UnsupportedOperationException.class, () -> planner().page(query, PageQuery.of(1, 5)));
        assertThrows(UnsupportedOperationException.class, () -> planner().select(query.withProjection(
                List.of("department.id"), List.of("department.id"))));
        assertTrue(planner().selectGoverned(query, FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults())
                .plan().request().sql().endsWith("FOR UPDATE OF \"t0\" SKIP LOCKED"));
    }

    @Test
    void relatedLockRetainsJoinedScopeAndRejectsNullableRootInsteadOfChangingRows() {
        FormDataSqlRenderer renderer = renderer();
        FormReadPlanner planner = new FormReadPlanner(renderer, new FormScopeSupport(renderer,
                StructuredConditionResolver.defaults(), DataScope.where(ConditionGroup.and().where("id", ">", 3L).build())),
                SqlExecutionOptions.unlimited());
        QuerySpec query = QuerySpec.of(users, ConditionGroup.and().where("department.name", "=", "A").build())
                .withScope(DataScope.where(ConditionGroup.and().where("id", ">", 4L).build())).withLock(ReadLock.update());
        SqlRequest request = planner.select(query).request();
        assertTrue(request.sql().contains("left outer join (select * from \"departments\""));
        assertEquals(List.of(3L, 3L, 4L, "A"), request.parameters());
        DynamicForm nullable = DynamicForm.builder("nullable", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .relation("department", QueryRelation.of(department, "id", "id")
                        .joinType(com.flying.orm.core.join.JoinType.RIGHT)).build();
        QuerySpec nullableQuery = QuerySpec.of(nullable, ConditionGroup.and().where("department.id", "=", 4L).build())
                .withLock(ReadLock.update());
        assertThrows(UnsupportedOperationException.class, () -> planner.select(nullableQuery));
        assertDoesNotThrow(() -> planner.count(nullableQuery, FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults(), false));
    }

    private FormReadPlanner planner() {
        return planner(RdbDialect.postgresql());
    }

    @Test
    void relatedKeysetBindsScopesBusinessAndCursorInSqlOrderAcrossDialects() {
        QuerySpec query = QuerySpec.of(users, ConditionGroup.or().where("id", "=", 1L)
                .where("department.name", "=", "safe' value").build())
                .withProjection(List.of("department.name"), List.of())
                .withScope(DataScope.where(ConditionGroup.and().where("id", ">", 4L).build()));
        KeysetPageQuery page = KeysetPageQuery.after(2, CursorPosition.of(List.of("A", 7L, 12L)),
                KeysetSort.asc("department.name", NullOrder.LAST));
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormReadPlanner.PlannedKeysetPage plan = planner(dialect).keysetPage(query, page);
            assertNotNull(plan.joinSpec());
            assertEquals(3, plan.page().sorts().size());
            assertTrue(plan.page().nullable(0));
            assertFalse(plan.page().nullable(1));
            assertTrue(plan.page().nullable(2), "LEFT joined primary key is nullable in the result");
            assertFalse(plan.request().sql().contains("safe' value"));
            assertEquals(List.of(4L, 1L, "safe' value", "A", "A", 7L, "A", 7L, 12L),
                    plan.request().parameters().subList(0, 9));
            assertTrue(plan.request().sql().contains("IS NULL"));
            if (dialect.name().equals("oracle")) {
                assertThrows(UnsupportedOperationException.class,
                        () -> planner(dialect).keysetPage(query.withLock(ReadLock.update()), page));
            } else {
                assertDoesNotThrow(() -> planner(dialect).keysetPage(query.withLock(ReadLock.update()), page));
            }
        }
    }

    @Test
    void relatedKeysetApprovesEveryPublishedPositionBySource() {
        QuerySpec query = QuerySpec.of(users, ConditionGroup.and().build())
                .withProjection(List.of("department.name"), List.of());
        JoinQuerySpec bound = planner().select(query).joinSpec();
        JoinSource root = bound.root();
        JoinSource related = bound.sources().get(1);
        KeysetPageQuery page = KeysetPageQuery.first(1, KeysetSort.asc("department.name", NullOrder.LAST));
        FieldUsePolicy.Builder policy = FieldUsePolicy.builder()
                .joinVisibility(related.field("name"), FieldVisibility.FULL)
                .allowJoin(related.field("name"), FieldUse.SORT)
                .allowJoin(root.field("departmentId"), FieldUse.JOIN)
                .allowJoin(related.field("id"), FieldUse.JOIN);
        assertThrows(ScopeAccessException.class, () -> planner().keysetPageGoverned(
                query, page, policy.build(), QueryShapeLimits.defaults()));
        policy.joinVisibility(root.field("id"), FieldVisibility.FULL)
                .joinVisibility(related.field("id"), FieldVisibility.FULL)
                .allowJoin(root.field("id"), FieldUseOrigin.INTERNAL_TIE_BREAKER, FieldUse.SORT)
                .allowJoin(related.field("id"), FieldUseOrigin.INTERNAL_TIE_BREAKER, FieldUse.SORT);
        assertDoesNotThrow(() -> planner().keysetPageGoverned(query, page, policy.build(), QueryShapeLimits.defaults()));
        policy.joinVisibility(related.field("name"), FieldVisibility.MASKED);
        assertThrows(ScopeAccessException.class, () -> planner().keysetPageGoverned(
                query, page, policy.build(), QueryShapeLimits.defaults()));
    }

    @Test
    void repeatedEquivalentRelatedSortsUseTheNormalizedShapeBudget() {
        QuerySpec query = QuerySpec.of(users, ConditionGroup.and().build())
                .withProjection(List.of("department.name"), List.of());
        KeysetSort sort = KeysetSort.asc("department.name", NullOrder.FIRST);
        KeysetPageQuery page = KeysetPageQuery.first(2, sort, sort, sort, sort);
        FormReadPlanner.PlannedKeysetPage plan = assertDoesNotThrow(() -> planner().keysetPageGoverned(
                query, page, FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults().withMaxSortCount(3))).plan();
        assertEquals(3, plan.page().sorts().size());
    }

    @Test
    void relatedKeysetKeepsOuterSourceNullsAndRequiresRealUniqueKeys() {
        for (JoinType type : List.of(JoinType.RIGHT, JoinType.FULL)) {
            DynamicForm form = DynamicForm.builder("outer", "users")
                    .addField(DynamicField.primaryKey("id", "BIGINT"))
                    .relation("department", QueryRelation.of(department, "id", "id").joinType(type)).build();
            QuerySpec query = QuerySpec.of(form, ConditionGroup.and().build())
                    .withProjection(List.of("department.name"), List.of());
            KeysetPageQuery page = KeysetPageQuery.after(2,
                    CursorPosition.of(java.util.Arrays.asList(null, null, 12L)),
                    KeysetSort.asc("department.name", NullOrder.LAST));
            FormReadPlanner.PlannedKeysetPage plan = planner().keysetPage(query, page);
            assertTrue(plan.page().nullable(1));
            assertEquals(type == JoinType.FULL, plan.page().nullable(2));
            assertThrows(UnsupportedOperationException.class,
                    () -> planner().keysetPage(query.withLock(ReadLock.update()), page));
        }
        DynamicForm unkeyed = DynamicForm.builder("unkeyed", "departments")
                .addField(DynamicField.of("id", "BIGINT")).build();
        DynamicForm form = DynamicForm.builder("root", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .relation("related", QueryRelation.of(unkeyed, "id", "id")).build();
        assertThrows(IllegalArgumentException.class, () -> planner().keysetPage(
                QuerySpec.of(form, ConditionGroup.and().build()).withProjection(List.of("related.id"), List.of()),
                KeysetPageQuery.first(2, KeysetSort.asc("id", NullOrder.LAST))));
    }

    @Test
    void relatedKeysetDoesNotPublishMaskedCursorValues() {
        DynamicForm masked = DynamicForm.builder("masked", "departments")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR"))
                .masked("name", MaskedFieldDefinition.builder("partial").prefix(1).suffix(1).build()).build();
        DynamicForm form = DynamicForm.builder("root", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .relation("related", QueryRelation.of(masked, "id", "id")).build();
        QuerySpec query = QuerySpec.of(form, ConditionGroup.and().build())
                .withProjection(List.of("related.name"), List.of());
        assertThrows(IllegalArgumentException.class, () -> planner().keysetPage(query,
                KeysetPageQuery.first(2, KeysetSort.asc("related.name", NullOrder.LAST))));
    }

    @Test
    void relatedCursorSharesJoinParametersAndFinalSortBudgetAcrossDialects() {
        DynamicForm child = DynamicForm.builder("child", "departments")
                .addField(DynamicField.primaryKey("childId", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR")).build();
        DynamicForm form = cursorForm(child, JoinType.INNER);
        String external = "x' OR 1=1 --";
        QuerySpec query = QuerySpec.of(form, ConditionGroup.or().where("id", "=", 5L)
                .where("related.name", "=", external).build())
                .withProjection(List.of("id", "related.childId"), List.of());
        KeysetSort sort = KeysetSort.desc("related.childId", com.flying.orm.core.page.NullOrder.LAST);
        KeysetPageQuery page = KeysetPageQuery.after(2, com.flying.orm.core.page.CursorPosition.of(List.of(9L, 3L)), sort, sort, sort);
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormReadPlanner.PlannedKeysetPage plan = planner(dialect).keysetPageGoverned(query, page,
                    FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults().withMaxSortCount(2)).plan();
            assertNotNull(plan.joinSpec());
            assertEquals(List.of(5L, external, 9L, 9L, 3L), plan.request().parameters().subList(0, 5));
            assertFalse(plan.request().sql().contains(external));
            assertEquals(List.of(KeysetSort.desc("childid", com.flying.orm.core.page.NullOrder.LAST), KeysetSort.desc("id", com.flying.orm.core.page.NullOrder.LAST)), plan.page().sorts());
            assertEquals(1, plan.page().callerSortCount());
        }
    }

    @Test
    void relatedKeysetSupportsHiddenNullableSortsAndUniqueKeys() {
        DynamicForm child = DynamicForm.builder("child", "departments")
                .addField(DynamicField.primaryKey("childId", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR")).build();
        KeysetPageQuery page = KeysetPageQuery.first(2, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST));
        QuerySpec query = QuerySpec.of(cursorForm(child, JoinType.INNER), ConditionGroup.and().build())
                .withProjection(List.of("id", "related.childId"), List.of());
        FormReadPlanner planner = planner();
        assertDoesNotThrow(() -> planner.keysetPage(query, page));
        assertTrue(planner.keysetPage(
                query.withProjection(List.of("related.childId"), List.of()), page).layout().hasHiddenSelections());
        assertTrue(planner.keysetPage(query,
                KeysetPageQuery.first(2, KeysetSort.asc("related.name", com.flying.orm.core.page.NullOrder.LAST)))
                .request().sql().contains("case when"));
        assertThrows(IllegalArgumentException.class, () -> planner.keysetPage(query,
                KeysetPageQuery.after(2, com.flying.orm.core.page.CursorPosition.of(List.of(1L)), KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST))));
        assertThrows(IllegalArgumentException.class, () -> planner.keysetPage(query,
                KeysetPageQuery.first(2, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST), KeysetSort.desc("id", com.flying.orm.core.page.NullOrder.LAST))));
        String lockedSql = planner.keysetPage(query.withLock(ReadLock.update()), page).request().sql();
        assertTrue(lockedSql.contains(" FOR UPDATE OF "), lockedSql);
        for (JoinType type : List.of(JoinType.LEFT, JoinType.RIGHT, JoinType.FULL)) {
            QuerySpec outer = QuerySpec.of(cursorForm(child, type), ConditionGroup.and().build())
                    .withProjection(List.of("id", "related.childId"), List.of());
            assertTrue(planner.keysetPage(outer, page).request().sql().contains("case when"));
        }
        DynamicForm noPrimary = DynamicForm.builder("child", "departments")
                .addField(DynamicField.of("childId", "BIGINT").withNullable(false).withUnique(true)).build();
        QuerySpec uniqueOnly = QuerySpec.of(cursorForm(noPrimary, JoinType.INNER), ConditionGroup.and().build())
                .withProjection(List.of("id", "related.childId"), List.of());
        assertEquals(2, planner.keysetPage(uniqueOnly, page).page().sorts().size());
    }

    @Test
    void relatedCursorApprovesSourceKeysBeforePublishingPositions() {
        DynamicForm child = DynamicForm.builder("child", "departments")
                .addField(DynamicField.primaryKey("childId", "BIGINT")).build();
        QuerySpec query = QuerySpec.of(cursorForm(child, JoinType.INNER), ConditionGroup.and().build())
                .withProjection(List.of("id", "related.childId"), List.of());
        KeysetPageQuery page = KeysetPageQuery.first(2, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST));
        JoinQuerySpec bound = planner().select(query).joinSpec();
        JoinSource root = bound.root();
        JoinSource related = bound.sources().get(1);
        FieldUsePolicy.Builder policy = FieldUsePolicy.builder()
                .joinVisibility(root.field("id"), FieldVisibility.FULL)
                .joinVisibility(related.field("childId"), FieldVisibility.FULL)
                .allowJoin(root.field("id"), FieldUse.JOIN, FieldUse.SORT)
                .allowJoin(related.field("childId"), FieldUse.JOIN);
        assertThrows(ScopeAccessException.class, () -> planner().keysetPageGoverned(
                query, page, policy.build(), QueryShapeLimits.defaults()));
        policy.allowJoin(related.field("childId"), FieldUseOrigin.INTERNAL_TIE_BREAKER, FieldUse.SORT);
        assertDoesNotThrow(() -> planner().keysetPageGoverned(query, page, policy.build(), QueryShapeLimits.defaults()));
        policy.joinVisibility(related.field("childId"), FieldVisibility.MASKED);
        assertThrows(ScopeAccessException.class, () -> planner().keysetPageGoverned(
                query, page, policy.build(), QueryShapeLimits.defaults()));
    }

    private static DynamicForm cursorForm(DynamicForm child, JoinType type) {
        return DynamicForm.builder("root", "users").addField(DynamicField.primaryKey("id", "BIGINT"))
                .relation("related", QueryRelation.of(child, "id", "childId").joinType(type)).build();
    }

    private FormReadPlanner planner(RdbDialect dialect) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), dialect);
        return new FormReadPlanner(renderer, new FormScopeSupport(renderer,
                StructuredConditionResolver.defaults(), DataScope.none()), SqlExecutionOptions.unlimited());
    }

    private FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }
}
