package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.dialect.PaginationDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DynamicQueryTerminalTest {
    @Test
    void defaultFetchTerminalIsConsistentAcrossDynamicAndEntityQueries() {
        assertDoesNotThrow(() -> QueryOperator.class.getMethod("fetch"));
        assertDoesNotThrow(() -> QueryOperator.class.getMethod("fetch", SqlExecutionOptions.class));
        assertDoesNotThrow(() -> SyncQueryOperator.class.getMethod("fetch"));
        assertDoesNotThrow(() -> SyncQueryOperator.class.getMethod("fetch", SqlExecutionOptions.class));
        assertDoesNotThrow(() -> EntityDmlQueryOperator.class.getMethod("fetch"));
        assertDoesNotThrow(() -> SyncEntityDmlQueryOperator.class.getMethod("fetch"));
    }

    @Test
    void wholeInputKeepsRawAndModelPaginationOnTheSamePublicTerminals() {
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            Fixture fixture = new Fixture(dialect);
            com.flying.orm.rdb.form.spec.QueryInput input = new com.flying.orm.rdb.form.spec.QueryInput(
                    null, List.of("name"), null, List.of(PageSort.desc("name")),
                    com.flying.orm.core.page.PageQuery.of(2, 3));
            fixture.sync(false).where("name", "Ada").input(input).fetch();
            SqlRequest expected = fixture.last();
            fixture.sync(true).where("name", "Ada").input(input).fetch();
            assertEquals(expected.sql(), fixture.last().sql());
            assertEquals(expected.parameters(), fixture.last().parameters());
            for (boolean model : List.of(false, true)) {
                fixture.reactive(model).where("name", "Ada").input(input).fetch().blockLast();
                assertEquals(expected.sql(), fixture.last().sql());
                assertEquals(expected.parameters(), fixture.last().parameters());
                fixture.sync(model).where("name", "Ada").input(input).page();
                assertEquals(expected.sql(), fixture.last().sql());
                fixture.reactive(model).where("name", "Ada").input(input).page(Name.class).block();
                assertEquals(expected.sql(), fixture.last().sql());
                fixture.sync(model).where("name", "Ada").input(input).page(1, 9);
                assertEquals(dialect.pagination().paginationParameters(List.of("Ada", 7L, 0),
                        com.flying.orm.core.page.PageQuery.of(1, 9)), fixture.last().parameters());
            }
        }
    }

    private static final DynamicForm FORM = DynamicForm.builder("users", "users")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("name", "VARCHAR"))
            .addField(DynamicField.of("tenant", "BIGINT"))
            .addField(DynamicField.of("deleted", "INTEGER")).logicDelete("deleted").build();
    private static final SqlExecutionOptions OPTIONS = SqlExecutionOptions.unlimited().withFetchSize(9);

    @Test
    void dynamicNormalStopSupportsRawTypedAndCustomMappingWithOrWithoutMetadata() {
        for (boolean governed : List.of(false, true)) {
            Fixture fixture = new Fixture(RdbDialect.postgresql());
            List<String> names = new ArrayList<>();
            assertEquals(1L, fixture.sync(governed).forEachWhile(row -> {
                names.add((String) row.get("name"));
                return false;
            }));
            assertEquals(1L, fixture.sync(governed).forEachWhile(Name.class, row -> {
                names.add(row.name());
                return false;
            }));
            assertEquals(1L, fixture.sync(governed).forEachWhile(row -> (String) row.get("name"), name -> {
                names.add(name);
                return false;
            }));
            assertEquals(List.of("Ada", "Ada", "Ada"), names);
            assertTrue(fixture.last().parameters().contains(7L));
        }
    }

    @Test
    void fluentQueriesExposeSortOnlyPriorityWithoutAProjectionAlias() {
        Fixture fixture = new Fixture(RdbDialect.postgresql());
        SyncQueryOperator sync = fixture.sync(true).select("id").orderByFirst("name", "Ada")
                .orderBy(ValueExpression.field("id"), PageSort.Direction.DESC);
        QueryOperator reactive = fixture.reactive(true).select("id").orderByFirst("name", "Ada")
                .orderBy(ValueExpression.field("id"), PageSort.Direction.DESC);
        sync.fetch();
        SqlRequest request = fixture.last();
        assertTrue(request.sql().startsWith("select \"id\" from"), request.sql());
        assertTrue(request.sql().contains("order by case \"name\""), request.sql());
        assertTrue(request.sql().endsWith("\"id\" desc"), request.sql());
        assertEquals("Ada", request.parameters().getLast());
        reactive.fetch().blockLast();
        assertEquals(request, fixture.last());
    }

    @Test
    void mixedSortExpressionsPreserveCallOrderBindingAndCopyOwnership() {
        Fixture fixture = new Fixture(RdbDialect.postgresql());
        Object[] preferred = { "Ada", "Lin" };
        SyncQueryOperator base = fixture.sync(true).select("id").orderByAsc("tenant")
                .orderByFirst("name", preferred).orderByDesc("id");
        preferred[0] = "changed";
        SyncQueryOperator copy = base.copy().orderByFirst("id", 8L, 9L);
        base.orderByFirst("id", 99L);
        copy.fetch();
        SqlRequest expected = fixture.last();
        assertTrue(expected.sql().contains("order by \"tenant\" asc, case \"name\""), expected.sql());
        assertTrue(expected.sql().contains("end asc, \"id\" desc, case \"id\""), expected.sql());
        assertEquals(List.of("Ada", "Lin", 8L, 9L), expected.parameters().subList(expected.parameters().size() - 4,
                expected.parameters().size()));
        fixture.reactive(true).select("id").orderByAsc("tenant").orderByFirst("name", "Ada", "Lin")
                .orderByDesc("id").orderByFirst("id", 8L, 9L).fetch().collectList().block();
        assertEquals(expected, fixture.last());
        copy.count();
        assertFalse(fixture.last().parameters().contains("Ada"));
        assertFalse(fixture.last().sql().contains("order by"));
        assertThrows(IllegalStateException.class, () -> fixture.sync(false).orderByFirst("name", "Ada").fetch());
        assertThrows(IllegalArgumentException.class, () -> fixture.sync(true).orderByFirst("name"));
    }

    @Test
    void priorityValuesUseDynamicArrayFieldEncoding() {
        Fixture fixture = new Fixture(RdbDialect.postgresql());
        DynamicForm form = DynamicForm.builder("array_sort", "array_sort")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("tenant", "BIGINT"))
                .addField(DynamicField.of("tags", "TEXT[]")).build();
        List<String> values = new ArrayList<>(List.of("A", "B"));
        QueryOperator query = fixture.reactive.query().from(form).select("id").orderByFirst("tags", values);
        Flux<DynamicRow> published = assertDoesNotThrow(() -> query.fetch());
        values.clear();
        published.collectList().block();
        Object parameter = fixture.last().parameters().getLast();
        assertInstanceOf(String[].class, parameter);
        assertArrayEquals(new String[] { "A", "B" }, (String[]) parameter);
        fixture.sync.query().from(form).select("id").orderByFirst("tags", List.of("A", "B")).fetch();
        assertArrayEquals((String[]) parameter, (String[]) fixture.last().parameters().getLast());
    }

    @Test
    void priorityValuesUseTheDialectStorageOfOffsetTime() {
        DynamicForm form = DynamicForm.builder("time_sort", "time_sort")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("tenant", "BIGINT"))
                .addField(DynamicField.of("time", "OFFSET_TIME")).build();
        java.time.OffsetTime value = java.time.OffsetTime.parse("12:30:00+08:00");
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(), RdbDialect.oracle(), RdbDialect.sqlServer())) {
            Fixture fixture = new Fixture(dialect);
            fixture.sync.query().from(form).select("id").orderByFirst("time", value).fetch();
            Object expected = dialect.name().equals("postgresql") ? value : value.toString();
            assertEquals(expected, fixture.last().parameters().getLast(), dialect.name());
            fixture.reactive.query().from(form).select("id").orderByFirst("time", value).fetch().collectList().block();
            assertEquals(expected, fixture.last().parameters().getLast(), dialect.name());
        }
    }

    @Test
    void composedPriorityKeepsFieldEncodingInsideArithmeticAndFunctions() {
        Fixture fixture = new Fixture(RdbDialect.postgresql());
        DynamicForm form = DynamicForm.builder("array_sort", "array_sort")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("tenant", "BIGINT"))
                .addField(DynamicField.of("tags", "TEXT[]")).build();
        ValueExpression priority = ValueExpression.field("tags").priority(List.of("A", "B"))
                .add(ValueExpression.value(1)).coalesce(ValueExpression.value(2));
        assertDoesNotThrow(() -> fixture.sync.query(form).select("id").orderBy(priority, PageSort.Direction.ASC).fetch());
        List<Object> parameters = fixture.last().parameters();
        assertArrayEquals(new String[] { "A", "B" }, (String[]) parameters.get(parameters.size() - 3));
        assertEquals(List.of(1, 2), parameters.subList(parameters.size() - 2, parameters.size()));
    }

    @Test
    void dynamicQueriesExposeProjectionExclusion() {
        Fixture fixture = new Fixture(RdbDialect.postgresql());
        java.util.function.Consumer<DmlQueryCommand<?>> projection =
                query -> query.select("id", "name").exclude("id").where("id", 8L);
        SyncQueryOperator sync = fixture.sync(true);
        QueryOperator reactive = fixture.reactive(true);
        projection.accept(sync);
        projection.accept(reactive);
        sync.fetch();
        SqlRequest request = fixture.last();
        assertTrue(request.sql().startsWith("select \"name\" from"), request.sql());
        assertTrue(request.parameters().containsAll(List.of(8L, 7L, 0)));
        reactive.fetch().blockLast();
        assertEquals(request, fixture.last());
    }

    @Test
    void implicitExclusionPreservesScopesConditionsAndFieldPolicy() {
        Fixture fixture = new Fixture(RdbDialect.postgresql());
        DataScope fields = DataScope.none().withFields(FieldScope.readable("id", "name"));
        fixture.sync(true).scope(fields).exclude("ID").where("id", 8L).orderByAsc("id").fetch();
        SqlRequest expected = fixture.last();
        assertTrue(expected.sql().startsWith("select \"name\" from"), expected.sql());
        assertTrue(expected.sql().contains("order by \"id\""));
        assertTrue(expected.parameters().containsAll(List.of(8L, 7L, 0)));
        fixture.reactive(true).scope(fields).exclude("id").where("id", 8L).orderByAsc("id").fetch().collectList().block();
        assertEquals(expected, fixture.last());
        FieldUsePolicy policy = FieldUsePolicy.builder().allow("name", FieldUse.FILTER).build();
        assertTrue(fixture.sync.query().from(FORM, policy).exclude("id").first().isEmpty());
        assertTrue(fixture.reactive.query().from(FORM, policy).exclude("id").first().block().isEmpty());
        assertThrows(ScopeAccessException.class,
                () -> fixture.sync.query().from(FORM, policy).select("name", "id").exclude("id").first());
    }

    @Test
    void exclusionDropsComputedBindingsAndKeepsGroupsAndCount() {
        Fixture fixture = new Fixture(RdbDialect.postgresql());
        fixture.sync(true).select("name").select(ValueExpression.value(123), "marker")
                .exclude("MARKER").groupBy("name").fetch();
        assertTrue(fixture.last().sql().startsWith("select \"name\" from"));
        assertFalse(fixture.last().parameters().contains(123));
        assertTrue(fixture.last().sql().contains("group by \"name\""));
        for (boolean metadata : List.of(false, true)) {
            assertEquals(3_000_000_000L, fixture.sync(metadata).select("name").exclude("name").count());
            assertEquals(3_000_000_000L, fixture.reactive(metadata).select("name").exclude("name").count().block());
        }
    }

    @Test
    void exclusionCopiesAndReactivePagesKeepIndependentSnapshots() {
        Fixture fixture = new Fixture(RdbDialect.postgresql());
        for (boolean metadata : List.of(false, true)) {
            QueryOperator base = fixture.reactive(metadata).select("name", "id", "tenant").exclude("tenant");
            QueryOperator branch = base.copy().exclude("id");
            Mono<PageResult<DynamicRow>> page = branch.page(1, 3);
            base.exclude("name");
            branch.exclude("name");
            for (int repeat = 0; repeat < 2; repeat++) {
                assertEquals("Ada", page.block().rows().getFirst().get("name"));
                assertTrue(fixture.last().sql().startsWith("select \"name\" from"));
            }
            base.fetch().collectList().block();
            assertTrue(fixture.last().sql().startsWith("select \"id\" from"));
            SyncQueryOperator sync = fixture.sync(metadata).select("name", "id").exclude("id");
            SyncQueryOperator copy = sync.copy();
            sync.exclude("name");
            copy.fetch();
            assertTrue(fixture.last().sql().startsWith("select \"name\" from"));
        }
    }

    @Test
    void rawExclusionUsesOnlyKnownColumnsAndNeverFallsBackToStar() {
        Fixture fixture = new Fixture(RdbDialect.postgresql());
        fixture.sync(false).scope(DataScope.none().withFields(FieldScope.readable("name", "id")))
                .exclude("id").fetch();
        assertTrue(fixture.last().sql().startsWith("select \"name\" from"));
        assertThrows(IllegalStateException.class, () -> fixture.sync(false).exclude("name").fetch());
        assertThrows(IllegalStateException.class, () -> fixture.reactive(false).select("users.*").exclude("name").fetch());
        assertThrows(IllegalArgumentException.class, () -> fixture.sync(false).select("name").exclude("NAME").fetch());
        assertThrows(IllegalArgumentException.class,
                () -> fixture.reactive(true).exclude("id", "name", "tenant", "deleted").fetch().collectList().block());
        assertThrows(IllegalArgumentException.class, () -> fixture.sync(true).exclude("name;drop table users"));
        fixture.sync(false).exclude().fetch();
        assertTrue(fixture.last().sql().startsWith("select * from"));
    }

    @Test
    void querySpecDerivationsRetainImmutableExclusionWithoutResurrectingAliases() {
        List<String> excluded = new ArrayList<>(List.of("ID", "marker"));
        QuerySpec spec = QuerySpec.structured(FORM, StructuredConditionInput.term("name", "=", "Ada"))
                .withExcludedFields(excluded);
        excluded.clear();
        spec = spec.withScope(DataScope.self("id", 1L)).withWhere(ConditionGroup.and().build())
                .withSorts(List.of(PageSort.asc("id"))).withExecutionOptions(OPTIONS)
                .withStructuredPolicy(StructuredConditionPolicy.defaults()).masked()
                .withProjection(List.of("id", "name"), List.of())
                .withExpressions(Map.of("marker", ValueExpression.value(1)))
                .withExpression("kept", ValueExpression.value(2));
        assertEquals(List.of("name", "kept"), spec.projections());
        assertEquals(java.util.Set.of("id", "marker"), spec.excludedFields());
        assertEquals(java.util.Set.of("kept"), spec.expressions().keySet());
        java.util.Set<String> frozen = spec.excludedFields();
        assertThrows(UnsupportedOperationException.class, frozen::clear);
    }

    @Test
    void dynamicQueriesExposeTypedCopyAlongsideEntityQueries() {
        assertAll(
                () -> assertEquals(QueryOperator.class,
                        assertDoesNotThrow(() -> QueryOperator.class.getMethod("copy")).getReturnType()),
                () -> assertEquals(SyncQueryOperator.class,
                        assertDoesNotThrow(() -> SyncQueryOperator.class.getMethod("copy")).getReturnType()));
    }

    @Test
    void syncCopiesKeepIndependentConditionsProjectionGroupingSortingAndScopes() {
        for (boolean metadata : List.of(false, true)) {
            Fixture fixture = new Fixture(RdbDialect.postgresql());
            List<Long> ids = new ArrayList<>(List.of(1L, 2L));
            SyncQueryOperator base = fixture.sync(metadata).select("name").groupBy("name").orderByAsc("name")
                    .where("id", "in", ids).scope(DataScope.self("id", 8L));
            SyncQueryOperator branch = base.copy();
            ids.set(0, 99L);
            base.select("id").groupBy("id").orderByDesc("id").where("name", "base");
            branch.where("name", "branch").scope(DataScope.self("id", 9L)).fetch();
            SqlRequest copied = fixture.last();
            assertFalse(copied.sql().contains("desc"));
            assertFalse(copied.sql().contains("\"name\", \"id\""));
            assertTrue(copied.parameters().containsAll(List.of(1L, 2L, "branch", 7L, 8L, 9L, 0)));
            assertFalse(copied.parameters().contains("base"));
            assertFalse(copied.parameters().contains(99L));
            base.fetch();
            assertTrue(fixture.last().sql().contains("\"id\" desc"));
            assertTrue(fixture.last().parameters().contains("base"));
            assertFalse(fixture.last().parameters().contains("branch"));
            assertFalse(fixture.last().parameters().contains(9L));
            branch.from("archived_users").logicDelete("deleted", 3, 4).fetch();
            assertTrue(fixture.last().sql().contains("\"archived_users\""));
            assertTrue(fixture.last().parameters().contains(3));
            base.fetch();
            assertFalse(fixture.last().sql().contains("archived_users"));
            assertTrue(fixture.last().parameters().contains(0));
        }
    }

    @Test
    void reactiveCopiesFreezeEachTerminalWithoutSharingLaterBuilderChanges() {
        for (boolean metadata : List.of(false, true)) {
            Fixture fixture = new Fixture(RdbDialect.postgresql());
            QueryOperator base = fixture.reactive(metadata).select("name").orderByAsc("name")
                    .where("id", 1L).scope(DataScope.self("id", 8L));
            QueryOperator branch = base.copy().where("name", "branch");
            Mono<PageResult<Name>> page = branch.page(1, 2, Name.class);
            base.replaceWhere(where -> where.is("id", 99L)).from("archived_users");
            branch.replaceWhere(where -> where.is("name", "later")).scope(DataScope.self("id", 9L));
            assertTrue(fixture.requests.isEmpty());
            for (int repeat = 0; repeat < 2; repeat++) {
                assertEquals(List.of(new Name("Ada")), page.block().rows());
                for (SqlRequest request : fixture.requests.subList(fixture.requests.size() - 2, fixture.requests.size())) {
                    assertTrue(request.parameters().containsAll(List.of(1L, "branch", 7L, 8L, 0)));
                    assertFalse(request.parameters().contains(99L));
                    assertFalse(request.parameters().contains(9L));
                    assertFalse(request.parameters().contains("later"));
                    assertFalse(request.sql().contains("archived_users"));
                }
            }
            branch.fetch().collectList().block();
            assertTrue(fixture.last().parameters().containsAll(List.of("later", 7L, 8L, 9L, 0)));
            base.fetch().collectList().block();
            assertTrue(fixture.last().sql().contains("archived_users"));
            assertTrue(fixture.last().parameters().contains(99L));
            assertFalse(fixture.last().parameters().contains("later"));
        }
    }

    @Test
    void copiedCommandRetainsComputedProjectionFilterPolicyAndDisplayMode() {
        SqlRenderer sql = SqlRenderer.builder().addDefaultTerms().build();
        PlanningDynamicQuery base = new PlanningDynamicQuery(sql, DataScope.tenant("tenant", 7L));
        FieldUsePolicy fields = FieldUsePolicy.unrestricted();
        QueryShapeLimits limits = QueryShapeLimits.defaults().withMaxProjectionCount(2);
        StructuredConditionPolicy inputPolicy = StructuredConditionPolicy.defaults();
        StructuredConditionInput input = StructuredConditionInput.term("name", "=", "Ada");
        base.from(FORM, fields, limits);
        base.select("name");
        base.select(ValueExpression.value(1), "marker");
        base.groupBy("name");
        base.orderBy(ValueExpression.field("name"), PageSort.Direction.ASC);
        base.filter(input, inputPolicy);
        base.display(SensitiveDisplayMode.MASKED);
        base.scope(DataScope.self("id", 8L));
        PlanningDynamicQuery branch = base.copy();
        base.select(ValueExpression.value(2), "extra");
        base.filter(StructuredConditionInput.term("name", "=", "Grace"), inputPolicy);
        base.display(SensitiveDisplayMode.FULL);
        base.from("archived_users");
        DmlQueryCommand.GovernedQuery copied = branch.governedQuery(null);
        assertSame(FORM, copied.spec().form());
        assertSame(fields, copied.policy());
        assertSame(limits, copied.limits());
        assertSame(input, copied.spec().structuredInput().orElseThrow());
        assertSame(inputPolicy, copied.spec().structuredPolicy().orElseThrow());
        assertEquals(List.of("name", "marker"), copied.spec().projections());
        assertEquals(List.of("name"), copied.spec().groups());
        assertEquals(List.of(PageSort.asc("name")), copied.spec().sorts());
        assertEquals(1, copied.spec().expressions().size());
        assertEquals(SensitiveDisplayMode.MASKED, copied.spec().sensitiveDisplayMode());
        Fixture fixture = new Fixture(RdbDialect.postgresql());
        assertThrows(IllegalArgumentException.class, () -> fixture.sync.query().from(FORM, fields, limits)
                .select("name").copy().select("id", "tenant").fetch());
        assertThrows(IllegalArgumentException.class, () -> fixture.reactive.query().from(FORM, fields, limits)
                .select("name").copy().select("id", "tenant").fetch().collectList().block());
        assertTrue(fixture.requests.isEmpty());
    }

    @Test
    void pageKeepsSyncAndReactiveTerminalsAlignedAcrossDialects() {
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(), RdbDialect.oracle(), RdbDialect.sqlServer())) {
            for (boolean metadata : List.of(false, true)) {
                Fixture fixture = new Fixture(dialect);
                SyncQueryOperator syncQuery = fixture.sync(metadata).select("name").where("id", 1L).orderByAsc("name");
                syncQuery.groupBy("name");
                PageResult<DynamicRow> result = syncQuery.page(3, 9);
                assertEquals(3_000_000_000L, result.total());
                assertEquals(3, result.page());
                assertEquals(9, result.size());
                assertEquals("Ada", result.rows().getFirst().get("name"));
                assertEquals(2, fixture.requests.size());
                SqlRequest count = fixture.requests.getFirst();
                SqlRequest data = fixture.last();
                assertTrue(count.sql().contains("group by"));
                assertFalse(count.sql().contains("order by"));
                assertTrue(data.parameters().containsAll(List.of(1L, 7L, 0)));
                assertEquals(dialect.name().equals("postgresql") || dialect.name().equals("mysql")
                                ? List.of(9, 18L) : List.of(18L, 9),
                        data.parameters().subList(data.parameters().size() - 2, data.parameters().size()));
                QueryOperator reactiveQuery = fixture.reactive(metadata).select("name").where("id", 1L).orderByAsc("name");
                reactiveQuery.groupBy("name");
                Mono<PageResult<DynamicRow>> reactive = reactiveQuery.page(3, 9);
                assertEquals(2, fixture.requests.size());
                for (int repeat = 0; repeat < 2; repeat++) {
                    assertEquals(result, reactive.block());
                    assertEquals(data.sql(), fixture.last().sql());
                    assertEquals(data.parameters(), fixture.last().parameters());
                    SqlRequest reactiveCount = fixture.requests.get(fixture.requests.size() - 2);
                    assertEquals(count.sql(), reactiveCount.sql());
                    assertEquals(count.parameters(), reactiveCount.parameters());
                }
                assertEquals(List.of(new Name("Ada")), fixture.sync(metadata).select("name").orderByAsc("name")
                        .page(1, 2, Name.class).rows());
                assertEquals(List.of(new Name("Ada")), fixture.reactive(metadata).select("name").orderByAsc("name")
                        .page(1, 2, Name.class).block().rows());
            }
        }
    }

    @Test
    void stringPageSnapshotsBothRequestsAndKeepsScopes() {
        Fixture fixture = new Fixture(RdbDialect.postgresql());
        QueryOperator query = fixture.reactive(false).select("name").scope(DataScope.self("id", 8L))
                .where("name", "Ada").orderByAsc("name");
        Mono<PageResult<Name>> page = query.page(2, 5, Name.class);
        query.replaceWhere(where -> where.is("name", "Grace")).scope(DataScope.self("id", 9L));
        assertTrue(fixture.requests.isEmpty());
        assertEquals(List.of(new Name("Ada")), page.block().rows());
        for (SqlRequest request : fixture.requests) {
            assertTrue(request.parameters().containsAll(List.of("Ada", 7L, 8L, 0)));
            assertFalse(request.parameters().contains("Grace"));
            assertFalse(request.parameters().contains(9L));
        }
    }

    @Test
    void stringPageRejectsInvalidOrGovernedRequestsBeforeExecution() {
        Fixture fixture = new Fixture(RdbDialect.postgresql());
        assertThrows(IllegalArgumentException.class, () -> fixture.sync(false).page(0, 10));
        assertThrows(IllegalArgumentException.class, () -> fixture.reactive(false).page(1, 0));
        assertThrows(ScopeAccessException.class, () -> fixture.sync(false).select("id")
                .scope(DataScope.none().withFields(FieldScope.readable("name"))).page(1, 10));
        assertThrows(ScopeAccessException.class, () -> fixture.reactive(false).select("name").orderByAsc("id")
                .scope(DataScope.none().withFields(FieldScope.readable("name"))).page(1, 10));
        assertThrows(IllegalStateException.class, () -> fixture.sync(false)
                .filter(StructuredConditionInput.term("name", "=", "Ada")).page(1, 10));
        assertThrows(IllegalStateException.class, () -> fixture.reactive(false).masked().page(1, 10));
        assertTrue(fixture.requests.isEmpty());
        Fixture sqlServer = new Fixture(RdbDialect.sqlServer());
        assertThrows(IllegalArgumentException.class, () -> sqlServer.sync(false).page(1, 10));
        assertThrows(IllegalArgumentException.class, () -> sqlServer.reactive(false).page(1, 10));
        assertTrue(sqlServer.requests.isEmpty());
    }

    @Test
    void stringPageSkipsDataForEmptyCountAndDoesNotHideFailuresOrCapSize() {
        Fixture fixture = new Fixture(RdbDialect.postgresql());
        fixture.empty = true;
        assertEquals(0L, fixture.sync(false).page(1, Integer.MAX_VALUE).total());
        assertEquals(1, fixture.requests.size());
        assertTrue(fixture.reactive(false).page(1, Integer.MAX_VALUE).block().rows().isEmpty());
        assertEquals(2, fixture.requests.size());
        fixture.empty = false;
        fixture.error = new IllegalStateException("count failed after row emission");
        assertSame(fixture.error, assertThrows(IllegalStateException.class,
                () -> fixture.reactive(false).page(1, 10).block()));
        assertEquals(3, fixture.requests.size());
        fixture.error = null;
        assertEquals(Integer.MAX_VALUE, fixture.sync(false).page(1, Integer.MAX_VALUE).size());
        assertEquals(Integer.MAX_VALUE, fixture.last().parameters().get(fixture.last().parameters().size() - 2));
    }

    @Test
    void firstAndCountKeepBothEntryPointsAlignedAcrossDialects() {
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(), RdbDialect.oracle(), RdbDialect.sqlServer())) {
            for (boolean metadata : List.of(false, true)) {
                Fixture fixture = new Fixture(dialect);
                assertEquals("Ada", fixture.sync(metadata).select("name").where("id", 1L).orderByAsc("name").first().get("name"));
                SqlRequest sync = fixture.last();
                int calls = fixture.requests.size();
                Mono<DynamicRow> first = fixture.reactive(metadata).select("name").where("id", 1L).orderByAsc("name").first(OPTIONS);
                assertEquals(calls, fixture.requests.size());
                assertEquals("Ada", first.block().get("name"));
                assertEquals("Ada", first.block().get("name"));
                assertEquals(calls + 2, fixture.requests.size());
                assertEquals(sync.sql(), fixture.last().sql());
                assertEquals(sync.parameters(), fixture.last().parameters());
                assertEquals(OPTIONS, fixture.options);
                assertTrue(sync.parameters().containsAll(List.of(1L, 7L, 0)), sync.parameters().toString());
                switch (dialect.name()) {
                    case "sqlserver" -> { assertTrue(sync.sql().startsWith("select top (?)")); assertEquals(1, sync.parameters().getFirst()); }
                    case "oracle" -> assertTrue(sync.sql().endsWith("offset ? rows fetch next ? rows only"));
                    default -> assertTrue(sync.sql().endsWith("limit ? offset ?"));
                }
                assertEquals(new Name("Ada"), fixture.sync(metadata).select("name").first(Name.class, OPTIONS));
                assertEquals(new Name("Ada"), fixture.reactive(metadata).select("name").first(Name.class).block());
                assertEquals(new Name("Ada"), fixture.sync(metadata).select("name").first(Name.class));
                assertEquals(new Name("Ada"), fixture.reactive(metadata).select("name").first(Name.class, OPTIONS).block());
                assertNotNull(fixture.sync(metadata).first(OPTIONS));
                assertNotNull(fixture.reactive(metadata).first().block());

                assertEquals(3_000_000_000L, fixture.sync(metadata).select("name").where("id", 1L).orderByAsc("name").count());
                SqlRequest count = fixture.last();
                assertTrue(count.sql().startsWith(dialect.name().equals("sqlserver") ? "select count_big(*)" : "select count(*)"));
                assertFalse(count.sql().contains("order by"));
                assertTrue(count.parameters().containsAll(List.of(1L, 7L, 0)));
                int before = fixture.requests.size();
                Mono<Long> total = fixture.reactive(metadata).where("id", 1L).count(OPTIONS);
                assertEquals(before, fixture.requests.size());
                assertEquals(3_000_000_000L, total.block());
                assertEquals(3_000_000_000L, total.block());
                assertEquals(count.sql(), fixture.last().sql());
                assertEquals(count.parameters(), fixture.last().parameters());
                assertEquals(OPTIONS, fixture.options);
                assertEquals(3_000_000_000L, fixture.sync(metadata).groupBy("name").count(OPTIONS));
                SqlRequest grouped = fixture.last();
                assertTrue(grouped.sql().contains("from (select 1 as group_marker from"), grouped.sql());
                assertTrue(grouped.sql().contains("group by"), grouped.sql());
                assertEquals(3_000_000_000L, fixture.reactive(metadata).groupBy("name").count().block());
                assertEquals(grouped.sql(), fixture.last().sql());
            }
        }
    }

    @Test
    void countKeepsFrontendFiltersAndGovernanceWithoutProjectPermission() {
        Fixture fixture = new Fixture(RdbDialect.postgresql());
        FieldUsePolicy policy = FieldUsePolicy.builder().allow("name", FieldUse.FILTER).build();
        assertEquals(3_000_000_000L, fixture.sync.query().from(FORM, policy)
                .filter(StructuredConditionInput.term("name", "=", "Ada")).count());
        assertTrue(fixture.last().parameters().containsAll(List.of("Ada", 7L, 0)));
        assertEquals(3_000_000_000L, fixture.reactive.query().from(FORM, policy)
                .filter(StructuredConditionInput.term("name", "=", "Ada")).count().block());
        int before = fixture.requests.size();
        assertThrows(ScopeAccessException.class, () -> fixture.reactive.query().from(FORM, policy).where("id", 1L).count().block());
        assertThrows(ScopeAccessException.class, () -> fixture.sync.query().from(FORM, policy).select("name").first());
        assertEquals(before, fixture.requests.size());
        assertTrue(fixture.sync.query().from(FORM, policy).first().isEmpty());
        assertTrue(fixture.reactive.query().from(FORM, policy).first().block().isEmpty());
        assertThrows(IllegalStateException.class, () -> fixture.sync(false)
                .filter(StructuredConditionInput.term("name", "=", "Ada")).count());
    }

    @Test
    void countIgnoresOutputFieldsButStillApprovesGroupFields() {
        Fixture fixture = new Fixture(RdbDialect.postgresql());
        DataScope scope = DataScope.none().withFields(FieldScope.readable("name"));
        for (boolean metadata : List.of(false, true)) {
            assertEquals(3_000_000_000L, fixture.sync(metadata).scope(scope).select("id").orderByAsc("id").count());
            assertEquals(3_000_000_000L, fixture.reactive(metadata).scope(scope).select("id").orderByAsc("id").count().block());
            int before = fixture.requests.size();
            Class<? extends RuntimeException> denied = metadata ? IllegalArgumentException.class : ScopeAccessException.class;
            assertThrows(denied, () -> fixture.sync(metadata).scope(scope).groupBy("id").count());
            assertThrows(denied, () -> fixture.reactive(metadata).scope(scope).groupBy("id").count().block());
            assertEquals(before, fixture.requests.size());
            assertEquals(3_000_000_000L, fixture.sync(metadata).scope(scope).groupBy("name").count());
        }
    }

    @Test
    void firstUsesTheConfiguredDialectExtension() {
        RdbDialect base = RdbDialect.postgresql();
        PaginationDialect pagination = new PaginationDialect() {
            public SqlRequest paginate(String sql, List<Object> values, com.flying.orm.core.page.PageQuery page) {
                throw new AssertionError("first must use its dedicated dialect hook");
            }
            public List<Object> paginationParameters(List<Object> values, com.flying.orm.core.page.PageQuery page) {
                throw new AssertionError("first must use its dedicated parameter hook");
            }
            public String firstSelectSql(String projection, String relation, List<Object> values) {
                return "select /* custom first */ " + projection + " " + relation + " limit ?";
            }
            public List<Object> firstSelectParameters(List<Object> values) {
                List<Object> parameters = new ArrayList<>(values);
                parameters.add(1);
                return parameters;
            }
        };
        Fixture fixture = new Fixture(RdbDialect.of("custom", base.schema(), pagination, base.upsert()));
        for (boolean metadata : List.of(false, true)) {
            fixture.sync(metadata).select("name").where("id", 8L).first();
            SqlRequest sync = fixture.last();
            assertTrue(sync.sql().startsWith("select /* custom first */"));
            assertEquals(1, sync.parameters().getLast());
            assertEquals(4, sync.parameters().size());
            fixture.reactive(metadata).select("name").where("id", 8L).first().block();
            assertEquals(sync.sql(), fixture.last().sql());
            assertEquals(sync.parameters(), fixture.last().parameters());
        }
    }

    @Test
    void terminalSnapshotsRetainDefaultAndExplicitScopes() {
        for (boolean metadata : List.of(false, true)) {
            Fixture fixture = new Fixture(RdbDialect.postgresql());
            DataScope scope = DataScope.self("id", 8L);
            QueryOperator query = fixture.reactive(metadata).select("name").scope(scope).where("name", "Ada");
            Mono<DynamicRow> first = query.first();
            Mono<Long> count = query.count();
            query.replaceWhere(where -> where.is("name", "Grace")).scope(DataScope.self("id", 9L));
            for (int repeat = 0; repeat < 2; repeat++) {
                assertEquals("Ada", first.block().get("name"));
                SqlRequest request = fixture.last();
                assertTrue(request.parameters().containsAll(List.of("Ada", 7L, 8L, 0)));
                assertFalse(request.parameters().contains("Grace"));
                assertFalse(request.parameters().contains(9L));
                assertEquals(3_000_000_000L, count.block());
                assertEquals(List.of("Ada", 7L, 8L, 0).size(), fixture.last().parameters().size());
                fixture.sync(metadata).scope(scope).where("name", "Ada").count();
                assertTrue(fixture.last().parameters().containsAll(List.of("Ada", 7L, 8L, 0)));
            }
        }
    }

    @Test
    void emptyResultsAndLateErrorsDoNotBecomeSuccessfulValues() {
        Fixture fixture = new Fixture(RdbDialect.postgresql());
        for (boolean metadata : List.of(false, true)) {
            fixture.empty = true;
            assertNull(fixture.sync(metadata).first());
            assertNull(fixture.reactive(metadata).first().block());
            assertEquals(0L, fixture.sync(metadata).count());
            assertEquals(0L, fixture.reactive(metadata).count().block());
            fixture.empty = false;
            fixture.error = new IllegalStateException("late executor failure");
            assertSame(fixture.error, assertThrows(IllegalStateException.class, () -> fixture.reactive(metadata).first().block()));
            assertSame(fixture.error, assertThrows(IllegalStateException.class, () -> fixture.reactive(metadata).count().block()));
            fixture.error = null;
        }
    }

    @Test
    void publicCountMapperKeepsExactLongConversion() {
        RowMapper<Long> mapper = RowMapper.count();
        assertEquals(3_000_000_000L, mapper.map(Map.of("TOTAL", new BigDecimal("3000000000"))));
        assertEquals(9L, mapper.map(Map.of("driver_label", "9")));
        assertThrows(IllegalArgumentException.class, () -> mapper.map(Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> mapper.map(java.util.Collections.singletonMap("total", null)));
        assertThrows(IllegalArgumentException.class, () -> mapper.map(Map.of("total", new BigDecimal("1.5"))));
        assertThrows(IllegalArgumentException.class, () -> mapper.map(Map.of("total", new BigDecimal("9223372036854775808"))));
    }

    @Test
    void publicCountMapperReusesExactLongAndPreservesNumericRejection() {
        RowMapper<Long> mapper = RowMapper.count();
        for (Long value : List.of(10_000L, Long.MIN_VALUE, Long.MAX_VALUE)) {
            assertSame(value, mapper.map(DynamicRow.copyOf(Map.of("total", value))));
        }
        for (Object value : List.of((byte) 7, (short) 7, 7, java.math.BigInteger.valueOf(7),
                new BigDecimal("7.000"), 7.0D, 7.0F, "7")) {
            assertEquals(7L, mapper.map(DynamicRow.copyOf(Map.of("total", value))));
        }
        for (Object value : List.of(new BigDecimal("1.5"), java.math.BigInteger.ONE.shiftLeft(63),
                java.math.BigInteger.valueOf(Long.MIN_VALUE).subtract(java.math.BigInteger.ONE),
                new BigDecimal("9223372036854775808"), 1.5D, Double.NaN,
                Double.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, "1.5", "9223372036854775808", true)) {
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> mapper.map(DynamicRow.copyOf(Map.of("total", value))));
            assertEquals("count result must be an exact long integer", failure.getMessage());
        }
    }

    public record Name(String name) { }

    static final class Fixture {
        final List<SqlRequest> requests = new ArrayList<>();
        final SyncDmlOperator sync;
        final DmlOperator reactive;
        boolean empty;
        int rowCount = 1;
        IllegalStateException error;
        SqlExecutionOptions options;
        Fixture(RdbDialect dialect) {
            SqlRenderer sql = SqlRenderer.builder().addDefaultTerms().build().withIdentifierRenderer(dialect.schema()::identifier);
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(sql, dialect);
            DataScope scope = DataScope.tenant("tenant", 7L);
            SyncSqlExecutor jdbc = new com.flying.orm.rdb.support.ListQueryFixture() {
                public List<DynamicRow> query(SqlRequest request) { return rows(request); }
                public List<DynamicRow> query(SqlRequest request, SqlExecutionOptions value) { options = value; return rows(request); }
                public long rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected write"); }
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions value, String generatedKeyColumn) { throw new AssertionError("unexpected write"); }
            };
            ReactiveSqlExecutor r2dbc = new ReactiveSqlExecutor() {
                public Flux<DynamicRow> query(SqlRequest request) {
                    return Flux.defer(() -> Flux.concat(Flux.fromIterable(rows(request)), error == null ? Flux.empty() : Flux.error(error)));
                }
                public Flux<DynamicRow> query(SqlRequest request, SqlExecutionOptions value) { options = value; return query(request); }
                public Mono<Long> rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected write"); }
            };
            sync = new SyncDmlOperator(SyncFormClient.create(jdbc, (request, completed) -> { throw new AssertionError(); }, renderer)
                    .withDefaultDataScope(scope), jdbc, sql, scope, dialect);
            reactive = DatabaseOperator.create(r2dbc, sql, dialect).withDefaultDataScope(scope).dml();
        }
        SyncQueryOperator sync(boolean metadata) { return metadata ? sync.query(FORM) : sync.query().from("users").logicDelete("deleted"); }
        QueryOperator reactive(boolean metadata) { return metadata ? reactive.query(FORM) : reactive.query().from("users").logicDelete("deleted"); }
        SqlRequest last() { return requests.getLast(); }
        List<DynamicRow> rows(SqlRequest request) {
            requests.add(request);
            return empty ? List.of() : java.util.Collections.nCopies(rowCount, DynamicRow.copyOf(request.sql().startsWith("select count")
                    ? Map.of("total", new BigDecimal("3000000000")) : Map.of("name", "Ada")));
        }
    }
}
