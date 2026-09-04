package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.mapping.FlyingTenant;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RepositoryUpsertStageLayoutTest {

    @Test
    void keepsInsertAndConflictUpdateStrategiesInSeparateStages() {
        CapturingBatchExecutor executor = new CapturingBatchExecutor();
        ReactiveFormClient client = client(executor);
        ReactiveFormRepository<StageEntity> repository = ReactiveFormRepository.create(
                client,
                client.entityModels().metadata(StageEntity.class).toDynamicForm(),
                StageEntity.class);

        repository.upsertBatch(List.of(new StageEntity(7L, "insert value", "update value"))).block();

        assertStageLayout(executor.request.get(), executor.rows.get());
    }

    @Test
    void keepsInsertAndConflictUpdateStrategiesOnSyncRepositoryPath() {
        CapturingSyncBatchExecutor executor = new CapturingSyncBatchExecutor();
        SyncFormClient client = SyncFormClient.create(new NoopSyncSqlExecutor(), executor, renderer());
        SyncFormRepository<StageEntity> repository = SyncFormRepository.create(
                client,
                client.entityModels().metadata(StageEntity.class).toDynamicForm(),
                StageEntity.class);

        repository.upsertBatch(List.of(new StageEntity(7L, "insert value", "update value")));

        assertStageLayout(executor.request.get(), executor.rows.get());
    }

    private static void assertStageLayout(BatchWriteRequest request, List<Object[]> rows) {
        assertNotNull(request);
        String sql = request.sql().toLowerCase(Locale.ROOT);
        int conflict = sql.indexOf(" on conflict ");
        assertTrue(conflict > 0, sql);
        String insert = sql.substring(0, conflict);
        String update = sql.substring(conflict);
        assertFalse(insert.contains("\"update_only\""), sql);
        assertTrue(update.contains("\"update_only\""), sql);
        assertTrue(insert.contains("\"insert_only\""), sql);
        assertFalse(update.contains("\"insert_only\""), sql);

        assertNotNull(rows);
        List<Object> parameters = Arrays.asList(rows.getFirst());
        assertTrue(parameters.contains("update value"), parameters.toString());
        assertTrue(parameters.contains("insert value"), parameters.toString());
        assertTrue(parameters.contains(7L), parameters.toString());
    }

    @Test
    void rejectsAnIncompleteNonLeadingCompositeIdentityBeforeSqlExecution() {
        CapturingBatchExecutor executor = new CapturingBatchExecutor();
        ReactiveFormClient client = client(executor);
        ReactiveFormRepository<IncompleteCompositeEntity> repository = ReactiveFormRepository.create(
                client,
                client.entityModels().metadata(IncompleteCompositeEntity.class).toDynamicForm(),
                IncompleteCompositeEntity.class);

        MappingException error = assertThrows(
                MappingException.class,
                () -> repository.upsertBatch(List.of(new IncompleteCompositeEntity("Ada", 11L, null))).block());

        assertTrue(error.getMessage().contains("does not support AUTO or sequence-generated primary keys"));
        assertNull(executor.request.get());
    }

    @Test
    void rejectsConditionalConflictUpdateLayoutBeforeSqlExecution() {
        CapturingBatchExecutor executor = new CapturingBatchExecutor();
        ReactiveFormClient client = client(executor);
        ReactiveFormRepository<ConditionalUpdateEntity> repository = ReactiveFormRepository.create(
                client,
                client.entityModels().metadata(ConditionalUpdateEntity.class).toDynamicForm(),
                ConditionalUpdateEntity.class);

        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> repository.upsertBatch(List.of(new ConditionalUpdateEntity(7L, "present"))).block());

        assertTrue(error.getMessage().contains("stable column layout"));
        assertTrue(error.getMessage().contains("conditional_update"));
        assertNull(executor.request.get());
    }

    @Test
    void rejectsTenantUpsertWhenTenantIsNotPartOfThePrimaryKey() {
        CapturingBatchExecutor executor = new CapturingBatchExecutor();
        ReactiveFormClient client = client(executor)
                .withDefaultDataScope(DataScope.tenant("tenant_id", "tenant-a"));
        ReactiveFormRepository<UnsafeTenantEntity> repository = ReactiveFormRepository.create(
                client,
                client.entityModels().metadata(UnsafeTenantEntity.class).toDynamicForm(),
                UnsafeTenantEntity.class);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> repository.upsertBatch(
                        List.of(new UnsafeTenantEntity(7L, "tenant-a", "value"))).block());

        assertTrue(error.getMessage().contains("tenant field must be part of the primary key"));
        assertNull(executor.request.get());
    }

    @Test
    void keepsAutoTenantInCompositeConflictIdentityAndOutOfUpdateSet() {
        CapturingBatchExecutor executor = new CapturingBatchExecutor();
        ReactiveFormClient client = client(executor)
                .withDefaultDataScope(DataScope.tenant("tenant_id", "tenant-a"));
        ReactiveFormRepository<SafeTenantEntity> repository = ReactiveFormRepository.create(
                client,
                client.entityModels().metadata(SafeTenantEntity.class).toDynamicForm(),
                SafeTenantEntity.class);

        repository.upsertBatch(List.of(new SafeTenantEntity("tenant-a", 7L, "value"))).block();

        BatchWriteRequest request = executor.request.get();
        assertNotNull(request);
        String sql = request.sql().toLowerCase(Locale.ROOT);
        int conflictStart = sql.indexOf(" on conflict (");
        int updateStart = sql.indexOf(" do update set ");
        assertTrue(conflictStart > 0, sql);
        assertTrue(updateStart > conflictStart, sql);
        String conflict = sql.substring(conflictStart, updateStart);
        String update = sql.substring(updateStart);
        assertTrue(conflict.contains("\"tenant_id\""), sql);
        assertTrue(conflict.contains("\"id\""), sql);
        assertFalse(update.contains("tenant_id"), sql);
    }

    private static ReactiveFormClient client(ReactiveSqlExecutor executor) {
        return ReactiveFormClient.create(executor, renderer());
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }

    @TableName("stage_entities")
    private static final class StageEntity {

        @TableId(type = IdType.INPUT)
        private final Long id;

        @TableField(value = "insert_only",
                    insertStrategy = FieldStrategy.ALWAYS,
                    updateStrategy = FieldStrategy.NEVER)
        private final String insertOnly;

        @TableField(value = "update_only",
                    insertStrategy = FieldStrategy.NEVER,
                    updateStrategy = FieldStrategy.ALWAYS)
        private final String updateOnly;

        private StageEntity(Long id, String insertOnly, String updateOnly) {
            this.id = id;
            this.insertOnly = insertOnly;
            this.updateOnly = updateOnly;
        }

        public Long getId() {
            return id;
        }

        public String getInsertOnly() {
            return insertOnly;
        }

        public String getUpdateOnly() {
            return updateOnly;
        }
    }

    @TableName("composite_entities")
    private static final class IncompleteCompositeEntity {

        private final String name;

        @TableId(value = "input_key", type = IdType.INPUT)
        private final Long inputKey;

        @TableId(value = "generated_key", type = IdType.AUTO)
        private Long generatedKey;

        private IncompleteCompositeEntity(String name, Long inputKey, Long generatedKey) {
            this.name = name;
            this.inputKey = inputKey;
            this.generatedKey = generatedKey;
        }

        public Long getGeneratedKey() {
            return generatedKey;
        }

        public void setGeneratedKey(Long generatedKey) {
            this.generatedKey = generatedKey;
        }

        public Long getInputKey() {
            return inputKey;
        }

        public String getName() {
            return name;
        }
    }

    @TableName("conditional_entities")
    private static final class ConditionalUpdateEntity {

        @TableId(type = IdType.INPUT)
        private final Long id;

        @TableField(value = "conditional_update",
                    insertStrategy = FieldStrategy.ALWAYS,
                    updateStrategy = FieldStrategy.NOT_NULL)
        private final String conditionalUpdate;

        private ConditionalUpdateEntity(Long id, String conditionalUpdate) {
            this.id = id;
            this.conditionalUpdate = conditionalUpdate;
        }

        public Long getId() {
            return id;
        }

        public String getConditionalUpdate() {
            return conditionalUpdate;
        }
    }

    @TableName("unsafe_tenant_entities")
    private static final class UnsafeTenantEntity {

        @TableId(type = IdType.INPUT)
        private final Long id;

        @FlyingTenant
        @TableField("tenant_id")
        private final String tenantId;

        private final String value;

        private UnsafeTenantEntity(Long id, String tenantId, String value) {
            this.id = id;
            this.tenantId = tenantId;
            this.value = value;
        }

        public Long getId() {
            return id;
        }

        public String getTenantId() {
            return tenantId;
        }

        public String getValue() {
            return value;
        }
    }

    @TableName("safe_tenant_entities")
    private static final class SafeTenantEntity {

        @FlyingTenant
        @TableId(value = "tenant_id", type = IdType.INPUT)
        private final String tenantId;

        @TableId(type = IdType.INPUT)
        private final Long id;

        private final String value;

        private SafeTenantEntity(String tenantId, Long id, String value) {
            this.tenantId = tenantId;
            this.id = id;
            this.value = value;
        }

        public String getTenantId() {
            return tenantId;
        }

        public Long getId() {
            return id;
        }

        public String getValue() {
            return value;
        }
    }

    private static final class CapturingBatchExecutor implements ReactiveSqlExecutor {

        private final AtomicReference<BatchWriteRequest> request = new AtomicReference<>();
        private final AtomicReference<List<Object[]>> rows = new AtomicReference<>();

        @Override
        public Flux<DynamicRow> query(com.flying.orm.core.sql.render.SqlRequest request) {
            return Flux.error(new UnsupportedOperationException());
        }

        @Override
        public Mono<Long> rowsUpdated(com.flying.orm.core.sql.render.SqlRequest request) {
            return Mono.error(new UnsupportedOperationException());
        }

        @Override
        public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
            this.request.set(request);
            return Flux.from(request.rows()).collectList().map(parameterRows -> {
                rows.set(parameterRows);
                return new BatchWriteResult(
                        request.options().mode(),
                        BatchWriteResult.Status.COMMITTED,
                        parameterRows.size(),
                        parameterRows.size(),
                        List.of());
            });
        }
    }

    private static final class CapturingSyncBatchExecutor implements SyncBatchExecutor {

        private final AtomicReference<BatchWriteRequest> request = new AtomicReference<>();
        private final AtomicReference<List<Object[]>> rows = new AtomicReference<>();

        @Override
        public BatchWriteResult writeBatch(BatchWriteRequest request) {
            this.request.set(request);
            List<Object[]> parameterRows = Flux.from(request.rows()).collectList().block();
            rows.set(parameterRows);
            return committed(request, parameterRows);
        }

        @Override
        public List<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
            return writeBatch(request).chunks();
        }
    }

    private static final class NoopSyncSqlExecutor implements SyncSqlExecutor {

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new UnsupportedOperationException();
        }
    }

    private static BatchWriteResult committed(BatchWriteRequest request, List<Object[]> rows) {
        return new BatchWriteResult(
                request.options().mode(),
                BatchWriteResult.Status.COMMITTED,
                rows.size(),
                rows.size(),
                List.of());
    }
}
