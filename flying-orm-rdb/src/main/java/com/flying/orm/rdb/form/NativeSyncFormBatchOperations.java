package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import org.reactivestreams.Publisher;

import java.util.function.LongConsumer;
import java.util.Map;
import java.util.Objects;

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

    BatchExecutionEvidence writeBatch(BatchSpec spec, LongConsumer rowCompleted) {
        BatchSpec safeSpec = Objects.requireNonNull(spec, "batch spec must not be null");
        BatchWriteOptions options = safeSpec.options().orElse(defaultOptions);
        DataScope scope = scopes.effectiveScope(safeSpec.scope());
        try (SyncBatchHead<Object> rows = open(safeSpec, options)) {
            if (rows.isEmpty()) {
                return new BatchExecutionEvidence.Accumulator().snapshot(BatchExecutionState.SUCCESS, null);
            }
            FormProtectedBatchRows.BatchLayout protectionLayout =
                    FormProtectedBatchRows.layout(safeSpec.form(), options);
            BatchWriteRequest request = request(safeSpec, rows, options, protectionLayout, scope);
            return protectionLayout.contains() != null
                    ? executor.writeProtectedBatch(request, rowCompleted) : executor.writeBatch(request, rowCompleted);
        }
    }

    private BatchWriteRequest request(BatchSpec spec,
                                      SyncBatchHead<Object> rows,
                                      BatchWriteOptions options,
                                      FormProtectedBatchRows.BatchLayout protectionLayout,
                                      DataScope scope) {
        return switch (spec.operation()) {
            case INSERT -> insertRequest(spec, rows, options, false, protectionLayout, scope);
            case UPSERT -> insertRequest(spec, rows, options, true, protectionLayout, scope);
            case UPDATE -> updateRequest(spec, rows, options, protectionLayout, scope);
        };
    }

    private BatchWriteRequest insertRequest(BatchSpec spec,
                                            SyncBatchHead<Object> rows,
                                            BatchWriteOptions options,
                                            boolean upsert,
                                            FormProtectedBatchRows.BatchLayout protectionLayout,
                                            DataScope scope) {
        DynamicForm form = spec.form();
        Map<String, Object> sourceFirstRow = requireMap(rows.first());
        FieldUseGuard.approveBatchInsert(form, sourceFirstRow, scope, upsert, fieldUsePolicy);
        Map<String, Object> firstValues = scopes.prepareWriteValues(form, sourceFirstRow, scope);
        DynamicForm physicalForm = renderer.protection().physicalForm(form);
        FormProtectionSqlSupport.WriteOperation protection = renderer.protection().writeOperation(
                form, physicalForm, scope, protectionLayout.contains());
        FormPreparedWrite first = protection.prepare(firstValues);
        BatchInsertPlan plan = upsert
                ? renderer.batchRenderer.upsertPlan(
                        form, firstValues, first.physicalForm(), first.values(), sourceFirstRow,
                        scope.condition().isEmpty() ? null
                                : scopes.prepareBatchScope(form, physicalForm, scope).where())
                : renderer.batchRenderer.insertPlan(first.physicalForm(), first.values());
        Publisher<Object[]> parameters = BatchPublishers.mapIndexed(rows, (row, index) -> {
            Map<String, Object> logical = index == 0L
                    ? firstValues : scopes.prepareWriteValues(form, requireMap(row), scope);
            FormPreparedWrite write = index == 0L
                    ? first : protection.prepare(logical);
            Object[] values = index == 0L
                    ? plan.firstParameters() : plan.parameters(write.values(), index);
            return FormProtectedBatchRows.insert(
                    protection, logical, plan, values, upsert, protectionLayout);
        });
        return plan.request(parameters, options, spec.generatedKeys());
    }

    private BatchWriteRequest updateRequest(BatchSpec spec,
                                            SyncBatchHead<Object> rows,
                                            BatchWriteOptions options,
                                            FormProtectedBatchRows.BatchLayout protectionLayout,
                                            DataScope scope) {
        DynamicForm form = spec.form();
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
            return FormProtectedBatchRows.update(
                    protection, prepared, values, protectionLayout);
        });
        return plan.request(parameters, options);
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
    private static SyncBatchHead<Object> open(BatchSpec spec, BatchWriteOptions options) {
        try {
            return SyncBatchHead.open((Publisher<Object>) spec.rows());
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("batch input was interrupted before planning", error);
        }
    }

}
