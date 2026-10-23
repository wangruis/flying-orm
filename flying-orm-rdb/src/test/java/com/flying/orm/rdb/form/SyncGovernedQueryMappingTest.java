package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.lock.ReadLock;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.DynamicTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SyncGovernedQueryMappingTest {

    @Test
    void governedReadsUseTheMappedTerminalAndKeepTheOneRowLimit() {
        RecordingExecutor executor = new RecordingExecutor(List.of(
                DynamicRow.copyOf(java.util.Map.of("id", 1L, "name", "one")),
                DynamicRow.copyOf(java.util.Map.of("id", 2L, "name", "two")),
                DynamicRow.copyOf(java.util.Map.of("id", 3L, "name", "three"))));
        DynamicForm form = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR"))
                .build();
        QuerySpec spec = QuerySpec.of(form, ConditionGroup.and().build());

        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            SyncFormOperations operations = new SyncFormOperations(
                    executor,
                    new FormConfiguration(FormDataSqlRenderer.create(
                            SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql()), StructuredConditionResolver.defaults(), SqlExecutionOptions.unlimited(), models).withDataScope(DataScope.none()).withFieldUsePolicy(FieldUsePolicy.unrestricted()).withQueryShapeLimits(QueryShapeLimits.defaults().withMaxProjectionCount(16)));

            assertEquals(3, operations.select(spec).size());
            assertEquals(1, executor.mappedQueries);
            assertEquals(0, executor.plainQueries);
            assertEquals(0, executor.lastRowLimit);

            executor.reset();
            assertEquals(3, operations.select(spec, Result.class).size());
            assertEquals(1, executor.mappedQueries);
            assertEquals(0, executor.plainQueries);
            assertEquals(0, executor.lastRowLimit);

            executor.reset();
            IllegalStateException error = assertThrows(
                    IllegalStateException.class, () -> operations.selectOne(spec, Result.class));
            assertEquals("entity query expected zero or one row but received 2", error.getMessage());
            assertEquals(1, executor.mappedQueries);
            assertEquals(0, executor.plainQueries);
            assertEquals(2, executor.lastRowLimit);
        }
    }

    @Test
    void typedLockingReadUsesTheMappedTerminalWithoutAnIntermediateRowList() {
        RecordingExecutor executor = new RecordingExecutor(List.of(
                DynamicRow.copyOf(java.util.Map.of("id", 1L, "name", "one"))));
        DynamicForm form = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR"))
                .build();

        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            SyncFormOperations operations = new SyncFormOperations(
                    executor,
                    new FormConfiguration(FormDataSqlRenderer.create(
                            SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql()), StructuredConditionResolver.defaults(), SqlExecutionOptions.unlimited(), models).withDataScope(DataScope.none()));

            assertEquals(List.of(new Result(1L, "one")), operations.lockingRead(
                    QuerySpec.of(form, ConditionGroup.and().build()).withLock(ReadLock.update()),
                    Result.class));
            assertEquals(1, executor.mappedQueries);
            assertEquals(0, executor.plainQueries);
        }
    }

    @TestFactory
    Stream<DynamicTest> typedPagesMapInsideTheQueryTerminal() {
        return Stream.of(false, true).map(governed -> DynamicTest.dynamicTest(
                "typed page governed=" + governed, () -> {
            RecordingExecutor executor = new RecordingExecutor(List.of(
                    DynamicRow.copyOf(java.util.Map.of("id", 1L, "name", "one")),
                    DynamicRow.copyOf(java.util.Map.of("id", 2L, "name", "two"))));
            DynamicForm form = DynamicForm.builder("users", "users")
                    .addField(DynamicField.primaryKey("id", "BIGINT"))
                    .addField(DynamicField.of("name", "VARCHAR")).build();
            try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
                SyncFormOperations operations = new SyncFormOperations(executor,
                        new FormConfiguration(FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql()), StructuredConditionResolver.defaults(), SqlExecutionOptions.unlimited(), models).withDataScope(DataScope.none()).withFieldUsePolicy(FieldUsePolicy.unrestricted()).withQueryShapeLimits(governed
                                ? QueryShapeLimits.defaults().withMaxProjectionCount(16) : QueryShapeLimits.defaults()));
                PageResult<Result> result = operations.page(QuerySpec.of(form, ConditionGroup.and().build()),
                        PageQuery.of(1, 2), Result.class);
                assertEquals(List.of(new Result(1L, "one"), new Result(2L, "two")), result.rows());
                assertEquals(3L, result.total());
                assertEquals(1, executor.plainQueries, "only count should materialize raw rows");
                assertEquals(1, executor.mappedQueries);
                assertEquals(new Result(1L, "one"), executor.firstMappedResult);
            }
        }));
    }

    record Result(Long id, String name) { }

    private static final class RecordingExecutor implements com.flying.orm.rdb.support.ListQueryFixture {
        private final List<DynamicRow> rows;
        private int plainQueries;
        private int mappedQueries;
        private int lastRowLimit = -1;
        private Object firstMappedResult;

        private RecordingExecutor(List<DynamicRow> rows) {
            this.rows = rows;
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            plainQueries++;
            if (request.sql().toLowerCase(java.util.Locale.ROOT).contains("count(")) {
                return List.of(DynamicRow.copyOf(java.util.Map.of("total", 3L)));
            }
            return rows;
        }


        @Override
        public <T> List<T> queryMapped(SqlRequest request,
                                       SqlExecutionOptions options,
                                       RowMapper<T> mapper,
                                       int rowLimit) {
            mappedQueries++;
            lastRowLimit = rowLimit;
            int size = rowLimit == 0 ? rows.size() : Math.min(rowLimit, rows.size());
            List<T> mapped = new ArrayList<>(size);
            for (int index = 0; index < size; index++) {
                mapped.add(mapper.map(rows.get(index)));
            }
            firstMappedResult = mapped.isEmpty() ? null : mapped.getFirst();
            return List.copyOf(mapped);
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new AssertionError("unexpected write");
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(
                SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
            throw new AssertionError("unexpected write");
        }

        private void reset() {
            plainQueries = 0;
            mappedQueries = 0;
            lastRowLimit = -1;
            firstMappedResult = null;
        }
    }
}
