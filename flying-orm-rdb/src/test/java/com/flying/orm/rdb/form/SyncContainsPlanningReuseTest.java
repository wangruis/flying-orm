package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.protection.MaskingPolicyRegistry;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.protection.ProtectedValueNormalizerRegistry;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.OffsetTime;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class SyncContainsPlanningReuseTest {

    @Test
    void rejectsTextBackedOffsetTimeContainsOrdering() {
        checkTemporalOrdering(false, (operations, spec) -> operations.select(
                spec.withSorts(List.of(PageSort.asc("remote_time")))));
    }

    @Test
    void rejectsTextBackedOffsetTimeContainsOffsetPage() {
        checkTemporalOrdering(false, (operations, spec) -> operations.page(
                spec, PageQuery.of(1, 10, PageSort.asc("remote_time"))));
    }

    @Test
    void rejectsTextBackedOffsetTimeContainsCursorPage() {
        checkTemporalOrdering(false, (operations, spec) -> operations.cursorPage(
                spec, CursorPageQuery.first(10, CursorSort.asc("remote_time"))));
        checkTemporalOrdering(false, (operations, spec) -> operations.cursorPage(spec,
                CursorPageQuery.after(10, List.of(OffsetTime.parse("10:00:00+14:00"), 1L),
                        CursorSort.asc("remote_time"))));
    }

    @Test
    void rejectsTextBackedOffsetTimeImplicitContainsPrimaryKeyOrdering() {
        checkTemporalOrdering(true, SyncFormOperations::select);
        checkTemporalOrdering(true, (operations, spec) -> operations.page(spec, PageQuery.of(1, 10)));
        checkTemporalOrdering(true, (operations, spec) -> operations.cursorPage(
                spec, CursorPageQuery.first(10, CursorSort.asc("sequence"))));
    }

    private static void checkTemporalOrdering(boolean temporalPrimaryKey,
                                             BiConsumer<SyncFormOperations, QuerySpec> action) {
        DynamicForm form = DynamicForm.builder("contains_times", "contains_times")
                .addField(DynamicField.primaryKey("id", temporalPrimaryKey ? "OFFSET_TIME" : "BIGINT"))
                .addField(DynamicField.of("remote_time", "OFFSET_TIME").withNullable(false))
                .addField(DynamicField.of("sequence", "BIGINT").withNullable(false))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
        for (RdbDialect dialect : List.of(RdbDialect.mysql(), RdbDialect.oracle(), RdbDialect.sqlServer(),
                RdbDialect.h2(), RdbDialect.postgresql())) {
            try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                    ProtectedFieldKeyRing.single("v1", new byte[32]));
                 EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
                FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                        SqlRenderer.builder().addDefaultTerms().build(), dialect).withProtectedFields(runtime);
                CountingExecutor executor = new CountingExecutor(List.of());
                SyncFormOperations operations = new SyncFormOperations(executor, renderer,
                        StructuredConditionResolver.defaults(), DataScope.none(),
                        SqlExecutionOptions.safeDefaults(), models);
                QuerySpec spec = QuerySpec.of(form, ConditionGroup.and()
                        .add(ProtectedConditions.contains("secret", "pha")).build());
                if (dialect.name().equals("h2") || dialect.name().equals("postgresql")) {
                    assertDoesNotThrow(() -> action.accept(operations, spec));
                } else {
                    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                            () -> action.accept(operations, spec));
                    assertTrue(failure.getMessage().contains("OFFSET_TIME"), failure.getMessage());
                    assertEquals(0, executor.queries, "invalid ordering must fail before querying candidates");
                }
            }
        }
    }

    @Test
    void typedListPreparesTheSameContainsQueryOnce() {
        checkPlanning(false);
    }

    @Test
    void typedOnePreparesTheSameContainsQueryOnceAndVerifiesFalsePositivesFirst() {
        checkPlanning(true);
    }

    private static void checkPlanning(boolean one) {
        AtomicInteger normalizations = new AtomicInteger();
        ProtectedValueNormalizerRegistry normalizers = ProtectedValueNormalizerRegistry.standard()
                .with("counted", value -> {
                    normalizations.incrementAndGet();
                    return value;
                });
        DynamicForm form = DynamicForm.builder("contains_users", "contains_users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .normalizer("counted").searchModes(EncryptedSearchMode.CONTAINS).build())
                .build();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]), normalizers, MaskingPolicyRegistry.standard());
             EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                    .withProtectedFields(runtime);
            CountingExecutor executor = new CountingExecutor(List.of(
                    encryptedRow(runtime, form, 1L, "goodbye"),
                    encryptedRow(runtime, form, 2L, "nothing"),
                    encryptedRow(runtime, form, 3L, "alphabet soup")));
            SyncFormOperations operations = new SyncFormOperations(executor, renderer,
                    StructuredConditionResolver.defaults(), DataScope.none(),
                    SqlExecutionOptions.safeDefaults(), models);
            QuerySpec spec = QuerySpec.of(form, ConditionGroup.and()
                    .add(ProtectedConditions.contains("secret", "pha")).build());

            normalizations.set(0);
            assertEquals(List.of(3L), operations.select(spec).stream().map(row -> row.get("id")).toList());
            int onePlanNormalizations = normalizations.get();
            assertTrue(onePlanNormalizations > 0);
            normalizations.set(0);
            executor.queries = 0;

            if (one) {
                assertEquals(new Result(3L, "alphabet soup"), operations.selectOne(spec, Result.class));
            } else {
                assertEquals(List.of(new Result(3L, "alphabet soup")), operations.select(spec, Result.class));
            }
            assertEquals(1, executor.queries);
            assertEquals(onePlanNormalizations, normalizations.get(),
                    "typed mapping must consume the existing plan, without repeating contains normalization");
        }
    }

    private static DynamicRow encryptedRow(ProtectedFieldRuntime runtime, DynamicForm form, long id, String secret) {
        return DynamicRow.copyOf(runtime.prepareWrite(form, Map.of("id", id, "secret", secret),
                DataScope.none(), ValueCodecRegistry.standard()).ownedValues());
    }

    record Result(Long id, String secret) { }

    private static final class CountingExecutor implements SyncSqlExecutor {
        private final List<DynamicRow> rows;
        private int queries;

        private CountingExecutor(List<DynamicRow> rows) {
            this.rows = rows;
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            queries++;
            return rows;
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new AssertionError("unexpected write");
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new AssertionError("unexpected write");
        }
    }
}
