package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.FormQuerySqlRenderer;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.lock.ReadLock;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class QueryModelContractTest {
    private static final DynamicForm FORM = DynamicForm.builder("users", "users")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("name", "VARCHAR")).build();

    @Test
    void modelAndTrustedPublicTerminalsKeepSqlParametersAndSubscriptionSnapshots() {
        List<Function<SyncQueryOperator, Object>> sync = List.of(SyncQueryOperator::fetch,
                SyncQueryOperator::first, SyncQueryOperator::one, SyncQueryOperator::count, query -> query.page(2, 3));
        List<Function<QueryOperator, Mono<?>>> reactive = List.of(query -> query.fetch().collectList(),
                QueryOperator::first, QueryOperator::one, QueryOperator::count, query -> query.page(2, 3));
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            DynamicQueryTerminalTest.Fixture fixture = new DynamicQueryTerminalTest.Fixture(dialect);
            for (int terminal = 0; terminal < sync.size(); terminal++) {
                fixture.requests.clear();
                sync.get(terminal).apply(fixture.sync(false).select("name", "id").exclude("id")
                        .where("name", "").orderByDesc("name"));
                List<SqlRequest> expected = List.copyOf(fixture.requests);
                assertEquals(terminal == 4 ? 2 : 1, expected.size());
                assertTrue(expected.getFirst().parameters().contains(""), "empty business values must not be removed");
                for (boolean model : List.of(false, true)) {
                    fixture.requests.clear();
                    sync.get(terminal).apply(fixture.sync(model).select("name", "id").exclude("id")
                            .where("name", "").orderByDesc("name"));
                    assertRequests(expected, fixture.requests);
                    QueryOperator query = fixture.reactive(model).select("name", "id").exclude("id")
                            .where("name", "").orderByDesc("name");
                    Mono<?> published = reactive.get(terminal).apply(query);
                    query.where("id", 99L).orderByAsc("id");
                    for (int repeat = 0; repeat < 2; repeat++) {
                        fixture.requests.clear();
                        published.block();
                        assertRequests(expected, fixture.requests);
                    }
                }
            }
        }
    }

    @Test
    void modelAndTrustedLocksUseTheSameRowsFirstAndCountSql() {
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            SqlRenderer sql = SqlRenderer.builder().addDefaultTerms().build()
                    .withIdentifierRenderer(dialect.schema()::identifier);
            List<SqlRequest> requests = new ArrayList<>();
            SyncSqlExecutor executor = new com.flying.orm.rdb.support.ListQueryFixture() {
                public List<DynamicRow> query(SqlRequest request) { requests.add(request); return List.of(); }
                public long rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected write"); }
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
                    throw new AssertionError("unexpected write");
                }
            };
            SyncFormClient client = SyncFormClient.create(executor, (request, completed) -> {
                throw new AssertionError("unexpected batch");
            }, FormDataSqlRenderer.create(sql, dialect));
            QuerySpec.Builder builder = QuerySpec.builder(FORM).select("id", "name").exclude("name")
                    .where(ConditionGroup.and().where("name", "=", "Ada").build())
                    .sort(PageSort.desc("id")).lock(ReadLock.updateNowait());
            QuerySpec model = builder.build();
            QuerySpec trusted = builder.copy().from("users").build();
            for (FormQuerySqlRenderer.ReadShape shape : FormQuerySqlRenderer.ReadShape.values()) {
                if (shape == FormQuerySqlRenderer.ReadShape.FIRST
                        && !dialect.lockingReadDialect().supportsPagination(ReadLock.updateNowait())) {
                    assertThrows(UnsupportedOperationException.class, () -> client.first(model));
                    assertThrows(UnsupportedOperationException.class, () -> FormQuerySqlRenderer.trusted(
                            sql, trusted, FieldScope.unrestricted(), shape, dialect));
                    continue;
                }
                switch (shape) {
                    case ROWS -> client.select(model);
                    case FIRST -> client.first(model);
                    case COUNT -> client.count(model);
                }
                assertRequest(requests.getLast(), FormQuerySqlRenderer.trusted(
                        sql, trusted, FieldScope.unrestricted(), shape, dialect));
            }
        }
    }

    @Test
    void uniqueTerminalNeverSilentlyReturnsTheFirstOfSeveralRows() {
        DynamicQueryTerminalTest.Fixture fixture = new DynamicQueryTerminalTest.Fixture(RdbDialect.postgresql());
        for (boolean model : List.of(false, true)) {
            fixture.empty = true;
            assertNull(fixture.sync(model).select("name").one());
            assertNull(fixture.reactive(model).select("name").one().block());
            fixture.empty = false;
            fixture.rowCount = 2;
            assertThrows(RuntimeException.class, () -> fixture.sync(model).select("name").one());
            assertThrows(RuntimeException.class, () -> fixture.reactive(model).select("name").one().block());
            fixture.rowCount = 1;
        }
    }

    @Test
    void modelAndTrustedTableKeepTheSameSelectionConditionsAndGroups() {
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            SqlRenderer sql = SqlRenderer.builder().addDefaultTerms().build()
                    .withIdentifierRenderer(dialect.schema()::identifier);
            WhereDsl where = new WhereDsl(sql);
            where.is("name", "").or(nested -> nested.is("id", 7L).is("id", 8L));
            ConditionGroup conditions = where.build();
            QuerySpec model = QuerySpec.builder(FORM).select("id", "name").exclude("name")
                    .where(conditions).groupBy("id").sort(PageSort.desc("id")).build();
            PlanningDynamicQuery trusted = new PlanningDynamicQuery(sql, DataScope.none());
            trusted.select("id", "name");
            trusted.exclude("name");
            trusted.from("users");
            trusted.where(conditions);
            trusted.groupBy("id");
            trusted.orderBy(com.flying.orm.core.expression.ValueExpression.field("id"), PageSort.Direction.DESC);
            SqlRequest expected = FormDataSqlRenderer.create(sql, dialect).selectProjected(
                    FORM, conditions, model.projections(), model.groups(), model.sorts());
            assertRequest(expected, trusted.toRequest());
            assertEquals(List.of("", 7L, 8L), expected.parameters());
            assertFalse(trusted.toCountRequest(dialect).sql().contains("order by"));
            assertEquals(expected.parameters(), trusted.toCountRequest(dialect).parameters());
            SqlRequest first = trusted.toFirstRequest(dialect);
            assertEquals(dialect.pagination().firstSelectParameters(expected.parameters()), first.parameters());
            assertEquals(dialect.pagination().firstSelectSql(sql.projection("id"),
                    expected.sql().substring(expected.sql().indexOf("from ")), expected.parameters()), first.sql());
        }
    }

    @Test
    void sourceReplacementAndImmutableCopiesKeepIdentityWithoutInventingMetadata() {
        RelationIdentity identity = RelationIdentity.of(null, "sales", "user.records");
        QuerySpec.Builder builder = QuerySpec.builder().select("id").from(identity);
        QuerySpec trusted = builder.build();
        QuerySpec changed = trusted.withWhere(ConditionGroup.and().where("id", "=", 1L).build())
                .withScope(DataScope.none()).withProjection(List.of("id"), List.of()).withSorts(List.of(PageSort.asc("id")));
        QuerySpec.Builder branch = builder.copy().from(FORM);
        builder.from("legacy.users");
        assertTrue(trusted.model().isEmpty());
        assertThrows(IllegalStateException.class, trusted::form);
        assertSame(identity, changed.relationIdentity());
        assertSame(FORM, branch.build().form());
        assertEquals("legacy.users", builder.build().table());
        assertEquals(RelationIdentity.of(null, "legacy", "users"), builder.build().relationIdentity());
        RdbDialect dialect = RdbDialect.postgresql();
        SqlRenderer sql = SqlRenderer.builder().addDefaultTerms().build().withIdentifierRenderer(dialect.schema()::identifier);
        SqlRequest request = FormQuerySqlRenderer.trusted(sql, changed, FieldScope.unrestricted(),
                FormQuerySqlRenderer.ReadShape.ROWS, dialect);
        assertTrue(request.sql().contains("\"sales\".\"user.records\""), request.sql());
        assertEquals(List.of(1L), request.parameters());
        assertThrows(IllegalStateException.class, () -> QuerySpec.builder().select("id").build());
    }

    @Test
    void trustedSourceRetainsTheExplicitLockForRowsAndFirst() {
        QuerySpec spec = QuerySpec.builder("users").select("id").lock(ReadLock.updateNowait()).build();
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            SqlRenderer sql = SqlRenderer.builder().addDefaultTerms().build()
                    .withIdentifierRenderer(dialect.schema()::identifier);
            SqlRequest rows = FormQuerySqlRenderer.trusted(sql, spec, FieldScope.unrestricted(),
                    FormQuerySqlRenderer.ReadShape.ROWS, dialect);
            String relation = "from " + sql.identifier("users") + dialect.lockingReadDialect().tableHint(ReadLock.updateNowait());
            String suffix = dialect.lockingReadDialect().suffix(ReadLock.updateNowait());
            assertEquals("select " + sql.identifier("id") + " " + relation + suffix, rows.sql());
            if (dialect.lockingReadDialect().supportsPagination(ReadLock.updateNowait())) {
                SqlRequest first = FormQuerySqlRenderer.trusted(sql, spec, FieldScope.unrestricted(),
                        FormQuerySqlRenderer.ReadShape.FIRST, dialect);
                assertEquals(dialect.pagination().firstSelectSql(sql.identifier("id"), relation, List.of()) + suffix, first.sql());
                assertEquals(dialect.pagination().firstSelectParameters(List.of()), first.parameters());
            } else {
                assertThrows(UnsupportedOperationException.class, () -> FormQuerySqlRenderer.trusted(sql, spec,
                        FieldScope.unrestricted(), FormQuerySqlRenderer.ReadShape.FIRST, dialect));
            }
        }
    }

    private static void assertRequest(SqlRequest expected, SqlRequest actual) {
        assertEquals(expected.sql(), actual.sql());
        assertEquals(expected.parameters(), actual.parameters());
    }

    private static void assertRequests(List<SqlRequest> expected, List<SqlRequest> actual) {
        assertEquals(expected.size(), actual.size());
        for (int index = 0; index < expected.size(); index++) assertRequest(expected.get(index), actual.get(index));
    }
}
