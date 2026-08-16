package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
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

    NativeSyncFormBatchOperations(SyncBatchExecutor executor,
                                  FormDataSqlRenderer renderer,
                                  StructuredConditionResolver resolver,
                                  DataScope defaultDataScope,
                                  BatchWriteOptions defaultOptions) {
        this.executor = Objects.requireNonNull(executor, "sync batch executor must not be null");
        this.renderer = Objects.requireNonNull(renderer, "form data sql renderer must not be null");
        this.scopes = new FormScopeSupport(renderer, resolver, defaultDataScope);
        this.defaultOptions = Objects.requireNonNull(defaultOptions, "default batch options must not be null");
    }

    BatchWriteResult writeBatch(BatchSpec spec) {
        BatchSpec safeSpec = Objects.requireNonNull(spec, "batch spec must not be null");
        BatchWriteOptions options = safeSpec.options().orElse(defaultOptions);
        try (SyncBatchHead<Object> rows = open(safeSpec)) {
            if (rows.isEmpty()) {
                return BatchWriteResult.empty(options.mode());
            }
            BatchWriteRequest request = request(safeSpec, rows, options);
            return renderer.protection().hasContainsIndex(safeSpec.form())
                    ? executor.writeProtectedBatch(request) : executor.writeBatch(request);
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
            BatchWriteRequest request = request(safeSpec, rows, options);
            return renderer.protection().hasContainsIndex(safeSpec.form())
                    ? executor.writeProtectedBatchChunks(request) : executor.writeBatchChunks(request);
        }
    }

    private BatchWriteRequest request(BatchSpec spec,
                                      SyncBatchHead<Object> rows,
                                      BatchWriteOptions options) {
        return switch (spec.operation()) {
            case INSERT -> insertRequest(spec, rows, options, false);
            case UPSERT -> insertRequest(spec, rows, options, true);
            case UPDATE -> updateRequest(spec, rows, options);
        };
    }

    private BatchWriteRequest insertRequest(BatchSpec spec,
                                            SyncBatchHead<Object> rows,
                                            BatchWriteOptions options,
                                            boolean upsert) {
        DynamicForm form = spec.form();
        DataScope scope = scopes.effectiveScope(spec.scope());
        Map<String, Object> firstValues = scopes.prepareWriteValues(form, requireMap(rows.first()), scope);
        ProtectedFieldRuntime.PreparedWrite first = renderer.protection().prepareWrite(form, firstValues, scope);
        BatchInsertPlan plan = upsert
                ? renderer.upsertPlan(first.physicalForm(), first.values())
                : renderer.insertPlan(first.physicalForm(), first.values());
        Publisher<Object[]> parameters = BatchPublishers.mapIndexed(rows, (row, index) -> {
            Map<String, Object> logical = index == 0L
                    ? firstValues : scopes.prepareWriteValues(form, requireMap(row), scope);
            ProtectedFieldRuntime.PreparedWrite write = index == 0L
                    ? first : renderer.protection().prepareWrite(form, logical, scope);
            Object[] values = plan.parameters(write.values(), index);
            if (!renderer.protection().hasContainsIndex(form)) {
                return values;
            }
            com.flying.orm.core.sql.render.SqlRequest request = new com.flying.orm.core.sql.render.SqlRequest(
                    plan.sql(), java.util.Arrays.asList(values), plan.bindMarkerStyle());
            ProtectedWriteWork work = renderer.protection().protectedWrite(
                    form, logical, scope, request, null,
                    upsert ? ProtectedWriteWork.Kind.UPSERT : ProtectedWriteWork.Kind.INSERT).orElse(null);
            return work == null ? values : ProtectedBatchRows.extend(values, work);
        });
        return plan.request(parameters, options, spec.generatedKeys(), spec.completion());
    }

    private BatchWriteRequest updateRequest(BatchSpec spec,
                                            SyncBatchHead<Object> rows,
                                            BatchWriteOptions options) {
        DynamicForm form = spec.form();
        DataScope scope = scopes.effectiveScope(spec.scope());
        FormScopeSupport.PreparedBatchUpdate first = scopes.prepareBatchUpdate(
                form, requireUpdate(rows.first()), scope);
        BatchUpdatePlan plan = renderer.optimisticUpdatePlan(
                first.form(), first.values(), first.where(), first.lock(), first.request());
        Publisher<Object[]> parameters = BatchPublishers.mapIndexed(rows, (row, index) -> {
            FormScopeSupport.PreparedBatchUpdate prepared = index == 0L
                    ? first : scopes.prepareBatchUpdate(form, requireUpdate(row), scope);
            Object[] values = plan.parameters(prepared.request(), index);
            if (!renderer.protection().hasContainsIndex(form)) {
                return values;
            }
            ProtectedWriteWork work = renderer.protection().protectedWrite(
                    form, prepared.logicalValues(), scope, prepared.request(), prepared.ownerQuery(),
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
