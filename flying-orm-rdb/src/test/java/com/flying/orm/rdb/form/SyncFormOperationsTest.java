package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockConflictException;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证原生同步表单操作直接消费共享 SQL 计划，不经过 Reactor 或 R2DBC 桥。 */
class SyncFormOperationsTest {

    /** Form 计划必须把数据库生成主键的物理列名交给原生 JDBC 内部协作。 */
    @Test
    void passesGeneratedKeyColumnToNativeSyncExecutor() {
        RecordingSyncSqlExecutor executor = new RecordingSyncSqlExecutor();
        SyncFormOperations operations = operations(executor, SqlExecutionOptions.safeDefaults());
        DynamicForm generated = DynamicForm.builder("device", "device")
                                           .addField(DynamicField.primaryKey("id", "BIGINT")
                                                                 .withGeneration(ValueGeneration.identity()))
                                           .addField(DynamicField.of("profile", "JSON"))
                                           .build();

        operations.insertReturningKeys(WriteSpec.insert(generated, row("profile", "{}")));

        assertEquals("id", executor.generatedKeyColumn);
    }

    @Test
    void executesQueryPageAndJsonDecodingThroughSyncExecutor() {
        RecordingSyncSqlExecutor executor = new RecordingSyncSqlExecutor();
        SqlExecutionOptions options = SqlExecutionOptions.maxRows(20);
        SyncFormOperations operations = operations(executor, options);
        QuerySpec query = QuerySpec.of(form(), ConditionGroup.and().where("id", "=", 7L).build());

        List<DynamicRow> rows = operations.select(query);
        var page = operations.page(query, PageQuery.of(1, 10));

        assertEquals("select id, profile, version from device where id = ?", executor.requests.getFirst().sql());
        assertEquals(List.of(7L), executor.requests.getFirst().parameters());
        assertInstanceOf(Map.class, rows.getFirst().get("profile"));
        assertEquals(1L, page.total());
        assertEquals(3, executor.requests.size());
        executor.options.forEach(recorded -> assertEquals(options, recorded));
    }

    /** QuerySpec 的投影必须进入两种分页 SQL；当前不支持的分组分页必须在执行前稳定拒绝。 */
    @Test
    void appliesProjectionToBothPaginationModesAndRejectsGroupedPages() {
        RecordingSyncSqlExecutor executor = new RecordingSyncSqlExecutor();
        SyncFormOperations operations = operations(executor, SqlExecutionOptions.safeDefaults());
        QuerySpec projected = QuerySpec.of(form(), ConditionGroup.and().build())
                                       .withProjection(List.of("id"), List.of());

        operations.page(projected, PageQuery.of(1, 10, PageSort.asc("id")));
        assertEquals("select id from device order by id asc limit ? offset ?",
                     executor.requests.getLast().sql());
        assertEquals(List.of(10, 0L), executor.requests.getLast().parameters());

        operations.cursorPage(projected, CursorPageQuery.first(10, CursorSort.asc("id")));
        assertEquals("select id from device order by id asc limit ? offset ?",
                     executor.requests.getLast().sql());
        assertEquals(List.of(11, 0L), executor.requests.getLast().parameters());

        int requestsBeforeRejection = executor.requests.size();
        IllegalArgumentException cursorSortError = assertThrows(
                IllegalArgumentException.class,
                () -> operations.cursorPage(
                        projected.withSorts(List.of(PageSort.asc("id"))),
                        CursorPageQuery.first(10, CursorSort.asc("id"))));
        assertEquals("cursor pagination sorts must be declared with CursorPageQuery",
                     cursorSortError.getMessage());
        QuerySpec missingCursorField = QuerySpec.of(form(), ConditionGroup.and().build())
                                                .withProjection(List.of("profile"), List.of());
        IllegalArgumentException cursorProjectionError = assertThrows(
                IllegalArgumentException.class,
                () -> operations.cursorPage(
                        missingCursorField,
                        CursorPageQuery.first(10, CursorSort.asc("id"))));
        assertEquals("cursor projection must include every cursor sort field",
                     cursorProjectionError.getMessage());
        QuerySpec grouped = QuerySpec.of(form(), ConditionGroup.and().build())
                                     .withProjection(List.of("id"), List.of("id"));
        IllegalArgumentException pageGroupError = assertThrows(
                IllegalArgumentException.class,
                () -> operations.page(grouped, PageQuery.of(1, 10)));
        assertEquals("offset pagination does not support grouped QuerySpec",
                     pageGroupError.getMessage());
        IllegalArgumentException cursorGroupError = assertThrows(
                IllegalArgumentException.class,
                () -> operations.cursorPage(
                        grouped, CursorPageQuery.first(10, CursorSort.asc("id"))));
        assertEquals("cursor pagination does not support grouped QuerySpec",
                     cursorGroupError.getMessage());
        assertEquals(requestsBeforeRejection, executor.requests.size());
    }

    /** 游标分页依赖普通大小比较，JSON、Vector 和数据库 LOB 必须在首屏 SQL 前拒绝。 */
    @Test
    void rejectsFieldsWithoutOrdinaryCursorComparisonBeforeExecution() {
        RecordingSyncSqlExecutor executor = new RecordingSyncSqlExecutor();
        SyncFormOperations operations = operations(executor, SqlExecutionOptions.safeDefaults());

        for (String dataType : List.of("JSON", "VECTOR(3)", "BLOB", "CLOB", "TEXT",
                                           "BINARY(16)", "VARBINARY(32)", "BYTEA")) {
            DynamicForm unsupported = DynamicForm.builder("cursor-" + dataType, "cursor_values")
                                                 .addField(DynamicField.primaryKey("id", "BIGINT"))
                                                 .addField(DynamicField.of("value", dataType)
                                                                       .withNullable(false))
                                                 .build();

            IllegalArgumentException failure = assertThrows(
                    IllegalArgumentException.class,
                    () -> operations.cursorPage(
                            QuerySpec.of(unsupported, ConditionGroup.and().build()),
                            CursorPageQuery.first(10, CursorSort.asc("value"))));

            assertEquals("cursor ordering does not support this field type: value", failure.getMessage());
        }
        assertEquals(List.of(), executor.requests);
    }

    /** 简单字段分组只允许显式选择和排序分组字段，不能把跨库非法 SQL 推给驱动。 */
    @Test
    void enforcesCompleteGroupedQueryShapeBeforeExecution() {
        RecordingSyncSqlExecutor executor = new RecordingSyncSqlExecutor();
        SyncFormOperations operations = operations(executor, SqlExecutionOptions.safeDefaults());

        QuerySpec nonGroupedProjection = QuerySpec.of(form(), ConditionGroup.and().build())
                                                  .withProjection(List.of("profile"), List.of("id"));
        QuerySpec nonGroupedSort = QuerySpec.of(form(), ConditionGroup.and().build())
                                            .withProjection(List.of("id"), List.of("id"))
                                            .withSorts(List.of(PageSort.asc("profile")));

        assertThrows(IllegalArgumentException.class, () -> operations.select(nonGroupedProjection));
        assertThrows(IllegalArgumentException.class, () -> operations.select(nonGroupedSort));
        assertEquals(0, executor.requests.size());

        operations.select(QuerySpec.of(form(), ConditionGroup.and().build())
                                   .withProjection(List.of("id"), List.of("id"))
                                   .withSorts(List.of(PageSort.asc("id"))));

        assertEquals("select id from device group by id order by id asc",
                     executor.requests.getFirst().sql());
    }

    /** FieldScope 不可读字段不能通过排序或分组形成旁路，JDBC/R2DBC 共用同一规划器。 */
    @Test
    void rejectsUnreadableSortAndGroupFieldsBeforeSyncExecution() {
        RecordingSyncSqlExecutor executor = new RecordingSyncSqlExecutor();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addTerm(SqlTermHandler.equalsTo()).build(), RdbDialect.h2());
        SyncFormOperations operations = new SyncFormOperations(
                executor, renderer, StructuredConditionResolver.defaults(renderer.valueCodecs()),
                DataScope.none().withFields(FieldScope.readable("id", "profile")),
                SqlExecutionOptions.safeDefaults(),
                EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults()));
        QuerySpec hiddenSort = QuerySpec.of(form(), ConditionGroup.and().build())
                                        .withSorts(List.of(PageSort.asc("version")));
        QuerySpec hiddenGroup = QuerySpec.of(form(), ConditionGroup.and().build())
                                         .withProjection(List.of("id"), List.of("version"));

        assertThrows(IllegalArgumentException.class, () -> operations.select(hiddenSort));
        assertThrows(IllegalArgumentException.class, () -> operations.select(hiddenGroup));
        assertThrows(IllegalArgumentException.class,
                     () -> operations.page(
                             QuerySpec.of(form(), ConditionGroup.and().build()),
                             PageQuery.of(1, 10, PageSort.asc("version"))));
        assertThrows(IllegalArgumentException.class,
                     () -> operations.cursorPage(
                             QuerySpec.of(form(), ConditionGroup.and().build()),
                             CursorPageQuery.first(10, CursorSort.asc("version"))));

        DynamicForm composite = DynamicForm.builder("composite-device", "composite_device")
                                           .addField(DynamicField.primaryKey("tenant_id", "BIGINT"))
                                           .addField(DynamicField.primaryKey("id", "BIGINT"))
                                           .addField(DynamicField.of("created_at", "TIMESTAMP")
                                                                 .withNullable(false))
                                           .build();
        SyncFormOperations partialPrimaryKeyScope = new SyncFormOperations(
                executor, renderer, StructuredConditionResolver.defaults(renderer.valueCodecs()),
                DataScope.none().withFields(FieldScope.readable("tenant_id", "created_at")),
                SqlExecutionOptions.safeDefaults(),
                EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults()));
        IllegalArgumentException primaryKeyError = assertThrows(
                IllegalArgumentException.class,
                () -> partialPrimaryKeyScope.cursorPage(
                        QuerySpec.of(composite, ConditionGroup.and().build()),
                        CursorPageQuery.first(10, CursorSort.asc("created_at"))));
        assertEquals("cursor pagination requires every primary-key field to be readable: id",
                     primaryKeyError.getMessage());
        assertEquals(List.of(), executor.requests);
    }

    /** 随机密文没有可用的业务排序或分组语义，所有 QuerySpec 读取形态都必须在 SQL 前拒绝。 */
    @Test
    void rejectsEncryptedSortAndGroupFieldsBeforeSyncExecution() {
        ProtectedSyncExecutor executor = new ProtectedSyncExecutor();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32])) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                                           .withProtectedFields(
                                                                   ProtectedFieldRuntime.create(keys));
            DynamicForm form = protectedForm();
            SyncFormOperations operations = new SyncFormOperations(
                    executor, renderer, StructuredConditionResolver.defaults(renderer.valueCodecs()),
                    DataScope.none(), SqlExecutionOptions.safeDefaults(),
                    EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults()));
            QuerySpec encryptedSort = QuerySpec.of(form, ConditionGroup.and().build())
                                               .withSorts(List.of(PageSort.asc("contact")));
            QuerySpec encryptedGroup = QuerySpec.of(form, ConditionGroup.and().build())
                                                .withProjection(List.of("id"), List.of("contact"));

            IllegalArgumentException selectSortError = assertThrows(
                    IllegalArgumentException.class, () -> operations.select(encryptedSort));
            assertEquals("encrypted field must not be used for query ordering",
                         selectSortError.getMessage());
            IllegalArgumentException groupError = assertThrows(
                    IllegalArgumentException.class, () -> operations.select(encryptedGroup));
            assertEquals("encrypted field must not be used for query grouping", groupError.getMessage());
            IllegalArgumentException pageSortError = assertThrows(
                    IllegalArgumentException.class,
                    () -> operations.page(encryptedSort, PageQuery.of(1, 10)));
            assertEquals("encrypted field must not be used for query ordering",
                         pageSortError.getMessage());
            IllegalArgumentException cursorSortError = assertThrows(
                    IllegalArgumentException.class,
                    () -> operations.cursorPage(
                            QuerySpec.of(form, ConditionGroup.and().build()),
                            CursorPageQuery.first(10, CursorSort.asc("contact"))));
            assertEquals("encrypted field must not be used for cursor ordering",
                         cursorSortError.getMessage());

            DynamicForm maskedForm = DynamicForm.builder("masked-device", "masked_device")
                                                .addField(DynamicField.primaryKey("id", "BIGINT"))
                                                .addField(DynamicField.of("phone", "VARCHAR")
                                                                      .withNullable(false))
                                                .masked("phone", MaskedFieldDefinition.builder("partial")
                                                                                       .prefix(3)
                                                                                       .suffix(4)
                                                                                       .build())
                                                .build();
            IllegalArgumentException maskedCursorError = assertThrows(
                    IllegalArgumentException.class,
                    () -> operations.cursorPage(
                            QuerySpec.of(maskedForm, ConditionGroup.and().build()),
                            CursorPageQuery.first(1, CursorSort.asc("phone"))));
            assertEquals("masked field must not be used for cursor ordering",
                         maskedCursorError.getMessage());
            assertEquals(0, executor.queryCalls);
        }
    }

    @Test
    void keepsSharedOptimisticLockFailureSemantics() {
        RecordingSyncSqlExecutor executor = new RecordingSyncSqlExecutor();
        executor.updatedRows = 0L;
        SyncFormOperations operations = operations(executor, SqlExecutionOptions.safeDefaults());
        WriteSpec update = WriteSpec.update(
                form(), row("profile", "{\"name\":\"new\"}"),
                ConditionGroup.and().where("id", "=", 7L).build())
                .withLock(OptimisticLockOptions.increment("version", 3L));

        assertThrows(OptimisticLockConflictException.class, () -> operations.update(update));
        assertEquals(
                "update device set profile = ? format json, version = version + 1 where id = ? and version = ?",
                executor.requests.getFirst().sql());
        assertEquals(List.of("{\"name\":\"new\"}", 7L, 3L), executor.requests.getFirst().parameters());
    }

    /** 同步 JDBC 门面必须复用响应式入口相同的 CONTAINS 解密复核和投影语义。 */
    @Test
    void verifiesContainsCandidatesInTheNativeSyncPath() {
        ProtectedSyncExecutor executor = new ProtectedSyncExecutor();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32])) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                                           .withProtectedFields(
                                                                   ProtectedFieldRuntime.create(keys));
            DynamicForm form = protectedForm();
            byte[] first = (byte[]) renderer.protection().prepareWrite(
                    form, Map.of("id", 1L, "contact", "AlphaBeta"), DataScope.none()).values().get("contact");
            byte[] second = (byte[]) renderer.protection().prepareWrite(
                    form, Map.of("id", 2L, "contact", "AlphaGamma"), DataScope.none()).values().get("contact");
            executor.rows = List.of(DynamicRow.copyOf(Map.of("id", 1L, "contact", first)),
                                    DynamicRow.copyOf(Map.of("id", 2L, "contact", second)));
            SyncFormOperations operations = new SyncFormOperations(
                    executor, renderer, StructuredConditionResolver.defaults(renderer.valueCodecs()),
                    DataScope.none(), SqlExecutionOptions.safeDefaults(),
                    EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults()));
            QuerySpec query = QuerySpec.of(
                    form,
                    ConditionGroup.and().add(ProtectedConditions.contains("contact", "PHAB")).build())
                                       .withProjection(List.of("id"), List.of())
                                       .showSensitive();

            List<DynamicRow> result = operations.select(query);

            assertEquals(List.of(Map.of("id", 1L)), result.stream().map(DynamicRow::toMap).toList());
        }
    }

    private static SyncFormOperations operations(RecordingSyncSqlExecutor executor,
                                                 SqlExecutionOptions options) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addTerm(SqlTermHandler.equalsTo()).build(), RdbDialect.h2());
        return new SyncFormOperations(
                executor, renderer, StructuredConditionResolver.defaults(renderer.valueCodecs()),
                DataScope.none(), options,
                EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults()));
    }

    private static DynamicForm form() {
        return DynamicForm.builder("device", "device")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("profile", "JSON"))
                          .addField(DynamicField.of("version", "BIGINT"))
                          .build();
    }

    private static DynamicForm protectedForm() {
        return DynamicForm.builder("protected-device", "protected_device")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("contact", "VARCHAR").withNullable(false))
                          .encrypted("contact", EncryptedFieldDefinition.builder()
                                                                         .searchModes(
                                                                                 EncryptedSearchMode.CONTAINS)
                                                                         .normalizer("case-fold")
                                                                         .build())
                          .build();
    }

    private static Map<String, Object> row(Object... pairs) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int index = 0; index < pairs.length; index += 2) {
            values.put((String) pairs[index], pairs[index + 1]);
        }
        return values;
    }

    private static final class RecordingSyncSqlExecutor implements SyncSqlExecutor {

        private final List<SqlRequest> requests = new ArrayList<>();
        private final List<SqlExecutionOptions> options = new ArrayList<>();
        private long updatedRows = 1L;
        private String generatedKeyColumn;

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            return query(request, SqlExecutionOptions.safeDefaults());
        }

        @Override
        public List<DynamicRow> query(SqlRequest request, SqlExecutionOptions executionOptions) {
            requests.add(request);
            options.add(executionOptions);
            if (request.sql().startsWith("select count(*)")) {
                return List.of(DynamicRow.copyOf(Map.of("total", 1L)));
            }
            return List.of(DynamicRow.copyOf(row(
                    "id", 7L, "profile", "{\"name\":\"sensor\"}", "version", 3L)));
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            return rowsUpdated(request, SqlExecutionOptions.safeDefaults());
        }

        @Override
        public long rowsUpdated(SqlRequest request, SqlExecutionOptions executionOptions) {
            requests.add(request);
            options.add(executionOptions);
            return updatedRows;
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            return new SqlWriteResult(rowsUpdated(request, options), List.of());
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request,
                                                       SqlExecutionOptions options,
                                                       String generatedKeyColumn) {
            this.generatedKeyColumn = generatedKeyColumn;
            return rowsUpdatedReturningKeys(request, options);
        }
    }

    private static final class ProtectedSyncExecutor implements SyncSqlExecutor {

        private List<DynamicRow> rows = List.of();
        private int queryCalls;

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            queryCalls++;
            return rows;
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            return 0L;
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            return new SqlWriteResult(0L, List.of());
        }
    }
}
