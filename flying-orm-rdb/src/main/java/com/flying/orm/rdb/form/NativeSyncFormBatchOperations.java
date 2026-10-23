package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.form.spec.BatchOperation;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.internal.mapping.RepositoryUpsertValues;
import com.flying.orm.rdb.internal.batch.BatchPublishers;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import org.reactivestreams.Publisher;

import java.util.function.LongConsumer;
import java.util.Map;
import java.util.Objects;

/**
 * 把批量表单规格编译成共享 {@link BatchWriteRequest}，再交给原生 JDBC 批量执行器。
 *
 * <p>同一次订阅按背压逐行转换，相邻同布局行复用计划，布局变化时重新规划；不收集整批，
 * JDBC 始终在调用线程执行。Scope、租户字段、乐观锁和参数顺序继续复用表单渲染器。</p>
 */
final class NativeSyncFormBatchOperations {

    private final SyncBatchExecutor executor;
    private final FormDataSqlRenderer renderer;
    private final FormScopeSupport scopes;
    private final BatchWriteOptions defaultOptions;
    private final FieldUsePolicy fieldUsePolicy;

    NativeSyncFormBatchOperations(SyncBatchExecutor executor,
                                  FormDataSqlRenderer renderer,
                                  FormScopeSupport scopes,
                                  BatchWriteOptions defaultOptions,
                                  FieldUsePolicy fieldUsePolicy) {
        this.executor = Objects.requireNonNull(executor, "sync batch executor must not be null");
        this.renderer = Objects.requireNonNull(renderer, "form data sql renderer must not be null");
        this.scopes = Objects.requireNonNull(scopes, "form scope support must not be null");
        this.defaultOptions = Objects.requireNonNull(defaultOptions, "default batch options must not be null");
        this.fieldUsePolicy = Objects.requireNonNull(fieldUsePolicy, "field use policy must not be null");
    }

    BatchExecutionEvidence writeBatch(BatchSpec spec, LongConsumer rowCompleted) {
        BatchSpec safeSpec = Objects.requireNonNull(spec, "batch spec must not be null");
        BatchWriteOptions options = safeSpec.options().orElse(defaultOptions);
        DataScope scope = scopes.effectiveScope(safeSpec.scope());
        try (SyncBatchHead<Object> rows = open(safeSpec)) {
            if (rows.isEmpty()) {
                return new BatchExecutionEvidence.Accumulator().snapshot(BatchExecutionState.SUCCESS, null);
            }
            ProtectedContainsLayout protectionLayout = ProtectedContainsLayout.resolve(safeSpec.form()).orElse(null);
            BatchWriteRequest request = switch (safeSpec.operation()) {
                case INSERT, UPSERT -> insertRequest(safeSpec, rows, options, protectionLayout, scope);
                case UPDATE -> updateRequest(safeSpec, rows, options, protectionLayout, scope);
            };
            return protectionLayout != null
                    ? executor.writeProtectedBatch(request, rowCompleted) : executor.writeBatch(request, rowCompleted);
        }
    }

    private BatchWriteRequest insertRequest(BatchSpec spec,
                                            SyncBatchHead<Object> rows,
                                            BatchWriteOptions options,
                                            ProtectedContainsLayout protectionLayout,
                                            DataScope scope) {
        DynamicForm form = spec.form();
        boolean upsert = spec.operation() == BatchOperation.UPSERT;
        boolean ignoreConflicts = spec.ignoresConflicts();
        Map<String, Object> sourceFirstRow = insertRow(rows.first(), spec);
        FieldUseGuard.approveBatchInsert(form, sourceFirstRow, scope,
                upsert && !ignoreConflicts && !BatchInsertPlan.generatedInsert(spec, sourceFirstRow),
                fieldUsePolicy, spec.conflictFields());
        Map<String, Object> firstValues = scopes.prepareWriteValues(form, sourceFirstRow, scope);
        return FormProtectedBatchRows.insertRequest(renderer, scopes, spec, sourceFirstRow, firstValues,
                scope, options, fieldUsePolicy, protectionLayout, executor.supportsBatchRowLayouts(),
                mapper -> BatchPublishers.mapIndexed(rows.rows(), (row, index) -> mapper.map(
                        index == 0L ? sourceFirstRow : insertRow(row, spec), index)));
    }

    private BatchWriteRequest updateRequest(BatchSpec spec,
                                            SyncBatchHead<Object> rows,
                                            BatchWriteOptions options,
                                            ProtectedContainsLayout protectionLayout,
                                            DataScope scope) {
        DynamicForm form = spec.form();
        BatchUpdate sourceFirst = requireUpdate(rows.first());
        FieldUseGuard.approveBatchUpdate(renderer, form, sourceFirst, scope, fieldUsePolicy);
        return FormProtectedBatchRows.updateRequest(renderer, scopes,
                form, sourceFirst, scope, options, fieldUsePolicy, protectionLayout,
                executor.supportsBatchRowLayouts(), mapper -> BatchPublishers.mapIndexed(
                        rows.rows(), (row, index) -> mapper.map(requireUpdate(row), index)));
    }

    private static Map<String, Object> insertRow(Object row, BatchSpec spec) {
        return RepositoryUpsertValues.prepareInsert(spec.form(), requireMap(row),
                spec.operation() == BatchOperation.INSERT || spec.ignoresConflicts(), spec.ignoredUpdates());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> requireMap(Object row) {
        if (!(row instanceof Map<?, ?>)) {
            throw new IllegalArgumentException("insert/upsert batch rows must be field maps");
        }
        return (Map<String, Object>) row;
    }

    private static BatchUpdate requireUpdate(Object row) {
        if (!(row instanceof BatchUpdate update)) {
            throw new IllegalArgumentException("update batch rows must be BatchUpdate values");
        }
        return update;
    }

    @SuppressWarnings("unchecked")
    private static SyncBatchHead<Object> open(BatchSpec spec) {
        try {
            return SyncBatchHead.open((Publisher<Object>) spec.rows());
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("batch input was interrupted before planning", error);
        }
    }

}
