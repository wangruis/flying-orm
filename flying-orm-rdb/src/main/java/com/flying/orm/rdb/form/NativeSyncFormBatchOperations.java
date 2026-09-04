package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchCommitFact;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import org.reactivestreams.Publisher;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * 把批量表单规格编译成共享 {@link BatchWriteRequest}，再交给原生 JDBC 批量执行器。
 *
 * <p>第一行只用于确定安全字段布局和 SQL 形状，之后仍从同一次订阅按背压逐行转换；不会收集整批，也不会创建
 * Reactor 类型。Scope、租户字段、乐观锁和参数顺序继续复用表单渲染器。</p>
 */
final class NativeSyncFormBatchOperations {

    private final SyncBatchExecutor executor;
    private final FormDataSqlRenderer renderer;
    private final FormScopeSupport scopes;
    private final BatchWriteOptions defaultOptions;
    private final FieldUsePolicy fieldUsePolicy;

    NativeSyncFormBatchOperations(SyncBatchExecutor executor,
                                  FormDataSqlRenderer renderer,
                                  StructuredConditionResolver resolver,
                                  DataScope defaultDataScope,
                                  BatchWriteOptions defaultOptions,
                                  FieldUsePolicy fieldUsePolicy) {
        this.executor = Objects.requireNonNull(executor, "sync batch executor must not be null");
        this.renderer = Objects.requireNonNull(renderer, "form data sql renderer must not be null");
        this.scopes = new FormScopeSupport(renderer, resolver, defaultDataScope);
        this.defaultOptions = Objects.requireNonNull(defaultOptions, "default batch options must not be null");
        this.fieldUsePolicy = Objects.requireNonNull(fieldUsePolicy, "field use policy must not be null");
    }

    BatchWriteResult writeBatch(BatchSpec spec) {
        BatchSpec safeSpec = Objects.requireNonNull(spec, "batch spec must not be null");
        BatchWriteOptions options = safeSpec.options().orElse(defaultOptions);
        try (SyncBatchHead<Object> rows = open(safeSpec)) {
            if (rows.isEmpty()) {
                return BatchWriteResult.empty(options.mode());
            }
            FormProtectedBatchRows.BatchLayout protectionLayout =
                    FormProtectedBatchRows.layout(safeSpec.form(), options);
            BatchWriteRequest request = request(safeSpec, rows, options, protectionLayout);
            return protectionLayout.contains() != null
                    ? executor.writeProtectedBatch(request) : executor.writeBatch(request);
        }
    }

    BatchExecutionEvidence writeBatchEvidence(BatchSpec spec) {
        BatchSpec safeSpec = Objects.requireNonNull(spec, "batch spec must not be null");
        BatchWriteOptions options = safeSpec.options().orElse(defaultOptions);
        try (SyncBatchHead<Object> rows = open(safeSpec)) {
            if (rows.isEmpty()) {
                return BatchExecutionEvidence.of(
                        options.mode(), BatchExecutionState.SUCCESS, BatchCommitFact.NOT_APPLICABLE, List.of());
            }
            FormProtectedBatchRows.BatchLayout protectionLayout =
                    FormProtectedBatchRows.layout(safeSpec.form(), options);
            BatchWriteRequest request = request(safeSpec, rows, options, protectionLayout);
            return protectionLayout.contains() != null
                    ? executor.writeProtectedBatchEvidence(request) : executor.writeBatchEvidence(request);
        }
    }

    List<BatchChunkResult> writeBatchChunks(BatchSpec spec) {
        BatchSpec safeSpec = Objects.requireNonNull(spec, "batch spec must not be null");
        BatchWriteOptions options = safeSpec.options().orElse(defaultOptions);
        if (options.mode() != BatchWriteOptions.Mode.INDEPENDENT) {
            throw new IllegalArgumentException("batch chunks require independent mode");
        }
        try (SyncBatchHead<Object> rows = open(safeSpec)) {
            if (rows.isEmpty()) {
                return List.of();
            }
            FormProtectedBatchRows.BatchLayout protectionLayout =
                    FormProtectedBatchRows.layout(safeSpec.form(), options);
            BatchWriteRequest request = request(safeSpec, rows, options, protectionLayout);
            return protectionLayout.contains() != null
                    ? executor.writeProtectedBatchChunks(request) : executor.writeBatchChunks(request);
        }
    }

    private BatchWriteRequest request(BatchSpec spec,
                                      SyncBatchHead<Object> rows,
                                      BatchWriteOptions options,
                                      FormProtectedBatchRows.BatchLayout protectionLayout) {
        return switch (spec.operation()) {
            case INSERT -> insertRequest(spec, rows, options, false, protectionLayout);
            case UPSERT -> insertRequest(spec, rows, options, true, protectionLayout);
            case UPDATE -> updateRequest(spec, rows, options, protectionLayout);
        };
    }

    private BatchWriteRequest insertRequest(BatchSpec spec,
                                            SyncBatchHead<Object> rows,
                                            BatchWriteOptions options,
                                            boolean upsert,
                                            FormProtectedBatchRows.BatchLayout protectionLayout) {
        DynamicForm form = spec.form();
        DataScope scope = scopes.effectiveScope(spec.scope());
        Map<String, Object> sourceFirstRow = requireMap(rows.first());
        FieldUseGuard.approveBatchInsert(form, sourceFirstRow, scope, upsert, fieldUsePolicy);
        Map<String, Object> firstValues = scopes.prepareWriteValues(form, sourceFirstRow, scope);
        DynamicForm physicalForm = renderer.protection().physicalForm(form);
        FormProtectionSqlSupport.WriteOperation protection = renderer.protection().writeOperation(
                form, physicalForm, scope, protectionLayout.contains());
        FormPreparedWrite first = protection.prepare(firstValues);
        BatchInsertPlan plan = upsert
                ? renderer.batchRenderer.upsertPlan(
                        form, firstValues, first.physicalForm(), first.values(), sourceFirstRow)
                : renderer.batchRenderer.insertPlan(first.physicalForm(), first.values());
        Publisher<Object[]> parameters = BatchPublishers.mapIndexed(rows, (row, index) -> {
            Map<String, Object> logical = index == 0L
                    ? firstValues : scopes.prepareWriteValues(form, requireMap(row), scope);
            FormPreparedWrite write = index == 0L
                    ? first : protection.prepare(logical);
            Object[] values = index == 0L
                    ? plan.firstParameters() : plan.parameters(write.values(), index);
            if (protectionLayout.contains() == null) {
                return values;
            }
            com.flying.orm.core.sql.render.SqlRequest request = new com.flying.orm.core.sql.render.SqlRequest(
                    plan.sql(), java.util.Arrays.asList(values), plan.bindMarkerStyle());
            ProtectedWriteWork work = protection.protectedWrite(
                    logical, request, null,
                    upsert ? ProtectedWriteWork.Kind.UPSERT : ProtectedWriteWork.Kind.INSERT).orElse(null);
            return work == null ? values : ProtectedBatchRows.extend(values, work);
        });
        return plan.request(parameters, options, spec.generatedKeys(), spec.completion());
    }

    private BatchWriteRequest updateRequest(BatchSpec spec,
                                            SyncBatchHead<Object> rows,
                                            BatchWriteOptions options,
                                            FormProtectedBatchRows.BatchLayout protectionLayout) {
        DynamicForm form = spec.form();
        DataScope scope = scopes.effectiveScope(spec.scope());
        BatchOptimisticUpdate sourceFirst = requireUpdate(rows.first());
        FieldUseGuard.approveBatchUpdate(renderer, form, sourceFirst, scope, fieldUsePolicy);
        DynamicForm physicalForm = renderer.protection().physicalForm(form);
        FormProtectionSqlSupport.WriteOperation protection = renderer.protection().writeOperation(
                form, physicalForm, scope, protectionLayout.contains());
        FormScopeSupport.PreparedBatchScope batchScope = scopes.prepareBatchScope(
                form, physicalForm, scope);
        FormScopeSupport.PreparedBatchUpdate first = scopes.prepareBatchUpdate(
                form, physicalForm, sourceFirst, batchScope, protection);
        BatchUpdatePlan plan = renderer.optimisticUpdatePlan(
                first.form(), first.values(), first.where(), first.lock(), first.request());
        Publisher<Object[]> parameters = BatchPublishers.mapIndexed(rows, (row, index) -> {
            FormScopeSupport.PreparedBatchUpdate prepared = index == 0L
                    ? first : scopes.prepareBatchUpdate(
                            form, first.form(), requireUpdate(row), batchScope, protection);
            Object[] values = plan.parameters(prepared.request(), index);
            if (protectionLayout.contains() == null) {
                return values;
            }
            ProtectedWriteWork work = protection.protectedWrite(
                    prepared.logicalValues(), prepared.request(), prepared.ownerQuery(),
                    ProtectedWriteWork.Kind.UPDATE).orElse(null);
            return work == null ? values : ProtectedBatchRows.extend(values, work);
        });
        return plan.request(parameters, options, spec.completion());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> requireMap(Object row) {
        if (!(row instanceof Map<?, ?>)) {
            throw new IllegalArgumentException("insert/upsert batch rows must be field maps");
        }
        return (Map<String, Object>) row;
    }

    private static BatchOptimisticUpdate requireUpdate(Object row) {
        if (!(row instanceof BatchOptimisticUpdate update)) {
            throw new IllegalArgumentException("update batch rows must be BatchOptimisticUpdate values");
        }
        return update;
    }

    @SuppressWarnings("unchecked")
    private static SyncBatchHead<Object> open(BatchSpec spec) {
        try {
            return SyncBatchHead.open((Publisher<Object>) spec.rows(), Duration.ZERO);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("batch input was interrupted before planning", error);
        } catch (TimeoutException error) {
            throw new IllegalStateException("batch input timed out before planning", error);
        }
    }

}
