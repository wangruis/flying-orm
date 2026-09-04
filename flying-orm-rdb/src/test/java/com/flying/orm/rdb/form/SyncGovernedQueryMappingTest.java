package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.lock.LockingReadSpec;
import com.flying.orm.rdb.lock.ReadLock;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
                    FormDataSqlRenderer.create(
                            SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql()),
                    StructuredConditionResolver.defaults(),
                    DataScope.none(),
                    SqlExecutionOptions.safeDefaults(),
                    models,
                    FieldUsePolicy.unrestricted(),
                    QueryShapeLimits.defaults().withMaxProjectionCount(16));

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
                    FormDataSqlRenderer.create(
                            SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql()),
                    StructuredConditionResolver.defaults(), DataScope.none(),
                    SqlExecutionOptions.safeDefaults(), models);

            assertEquals(List.of(new Result(1L, "one")), operations.lockingRead(
                    LockingReadSpec.of(
                            QuerySpec.of(form, ConditionGroup.and().build()), ReadLock.update()),
                    Result.class));
            assertEquals(1, executor.mappedQueries);
            assertEquals(0, executor.plainQueries);
        }
    }

    record Result(Long id, String name) { }

    private static final class RecordingExecutor implements SyncSqlExecutor {
        private final List<DynamicRow> rows;
        private int plainQueries;
        private int mappedQueries;
        private int lastRowLimit = -1;

        private RecordingExecutor(List<DynamicRow> rows) {
            this.rows = rows;
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            plainQueries++;
            return rows;
        }

        @Override
        public Optional<JdbcTransactionContext> currentTransaction() {
            java.sql.Connection connection = (java.sql.Connection) java.lang.reflect.Proxy.newProxyInstance(
                    java.sql.Connection.class.getClassLoader(),
                    new Class<?>[]{java.sql.Connection.class},
                    (proxy, method, arguments) -> {
                        throw new UnsupportedOperationException(method.toString());
                    });
            return Optional.of(JdbcTransactionContext.external(connection));
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
            return List.copyOf(mapped);
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new AssertionError("unexpected write");
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(
                SqlRequest request, SqlExecutionOptions options) {
            throw new AssertionError("unexpected write");
        }

        private void reset() {
            plainQueries = 0;
            mappedQueries = 0;
            lastRowLimit = -1;
        }
    }
}
