package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

/**
 * 负责逐行乐观锁批量更新及其分片结果流。
 *
 * <p>具体行为从原客户端原样迁移，SQL、参数、Scope、执行保护和响应式订阅语义不变。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class ReactiveFormBatchUpdateOperations extends ReactiveFormOperationSupport {

    ReactiveFormBatchUpdateOperations(ReactiveFormOperationSupport runtime) {
        super(runtime);
    }
    Mono<BatchWriteResult> updateBatch(DynamicForm form,
                                              Publisher<BatchOptimisticUpdate> updates) {
        return updateBatch(form, updates, DataScope.none(), defaultBatchWriteOptions, BatchWriteCompletion.noop());
    }

    Mono<BatchWriteResult> updateBatch(DynamicForm form,
                                              List<BatchOptimisticUpdate> updates) {
        return updateBatch(form,
                           Flux.fromIterable(Objects.requireNonNull(updates, "batch updates must not be null")));
    }

    Mono<BatchWriteResult> updateBatch(DynamicForm form,
                                              Publisher<BatchOptimisticUpdate> updates,
                                              BatchWriteOptions options) {
        return updateBatch(form, updates, DataScope.none(), options, BatchWriteCompletion.noop());
    }

    /**
     * 显式数据范围会和客户端默认范围做 AND。整批复用同一份合并结果，字段校验和每条 SQL 不会出现范围偏差。
     */
    Mono<BatchWriteResult> updateBatch(DynamicForm form,
                                       Publisher<BatchOptimisticUpdate> updates,
                                       DataScope scope,
                                       BatchWriteOptions options) {
        return updateBatch(form, updates, scope, options, BatchWriteCompletion.noop());
    }

    Mono<BatchWriteResult> updateBatch(DynamicForm form,
                                               Publisher<BatchOptimisticUpdate> updates,
                                               DataScope scope,
                                               BatchWriteOptions options,
                                               BatchWriteCompletion completion) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        BatchWriteOptions safeOptions = Objects.requireNonNull(options, "batch write options must not be null");
        Flux<BatchOptimisticUpdate> source = Flux.from(Objects.requireNonNull(updates,
                                                                             "batch updates must not be null"));
        return source.switchOnFirst((signal, replay) -> {
            if (signal.isOnError()) {
                return Mono.error(Objects.requireNonNull(signal.getThrowable()));
            }
            if (!signal.hasValue()) {
                return Mono.just(BatchWriteResult.empty(safeOptions.mode()));
            }
            DataScope effectiveScope = scopes.effectiveScope(scope);
            FormScopeSupport.PreparedBatchUpdate first = scopes.prepareBatchUpdate(
                    safeForm, signal.get(), effectiveScope);
            BatchUpdatePlan plan = renderer.optimisticUpdatePlan(first.form(),
                                                                 first.values(),
                                                                 first.where(),
                                                                 first.lock(),
                                                                 first.request());
            Flux<Object[]> parameters = replay.index()
                                              .map(indexed -> protectedParameters(
                                                      safeForm, effectiveScope, first, plan,
                                                      indexed.getT2(), indexed.getT1(), safeOptions));
            com.flying.orm.rdb.batch.BatchWriteRequest request = plan.request(
                    parameters, safeOptions, completion);
            return FormProtectedBatchRows.requiresProtectedExecution(
                    renderer.protection(), safeForm, safeOptions)
                    ? executor.writeProtectedBatch(request) : executor.writeBatch(request);
        }).single();
    }

    /**
     * INDEPENDENT 模式逐分片返回结果；冲突行会保留在分片结果中，不会把成功分片伪装成整批失败。
     */
    Flux<BatchChunkResult> updateBatchChunks(DynamicForm form,
                                                    Publisher<BatchOptimisticUpdate> updates,
                                                    BatchWriteOptions options) {
        return updateBatchChunks(form, updates, DataScope.none(), options);
    }

    Flux<BatchChunkResult> updateBatchChunks(DynamicForm form,
                                                    Publisher<BatchOptimisticUpdate> updates,
                                                    DataScope scope,
                                                    BatchWriteOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        BatchWriteOptions safeOptions = Objects.requireNonNull(options, "batch write options must not be null");
        if (safeOptions.mode() != BatchWriteOptions.Mode.INDEPENDENT) {
            return Flux.error(new IllegalArgumentException("batch chunks require independent mode"));
        }
        Flux<BatchOptimisticUpdate> source = Flux.from(Objects.requireNonNull(updates,
                                                                             "batch updates must not be null"));
        return source.switchOnFirst((signal, replay) -> {
            if (signal.isOnError()) {
                return Flux.error(Objects.requireNonNull(signal.getThrowable()));
            }
            if (!signal.hasValue()) {
                return Flux.empty();
            }
            DataScope effectiveScope = scopes.effectiveScope(scope);
            FormScopeSupport.PreparedBatchUpdate first = scopes.prepareBatchUpdate(
                    safeForm, signal.get(), effectiveScope);
            BatchUpdatePlan plan = renderer.optimisticUpdatePlan(first.form(),
                                                                 first.values(),
                                                                 first.where(),
                                                                 first.lock(),
                                                                 first.request());
            Flux<Object[]> parameters = replay.index()
                                              .map(indexed -> protectedParameters(
                                                      safeForm, effectiveScope, first, plan,
                                                      indexed.getT2(), indexed.getT1(), safeOptions));
            com.flying.orm.rdb.batch.BatchWriteRequest request = plan.request(parameters, safeOptions);
            return FormProtectedBatchRows.requiresProtectedExecution(
                    renderer.protection(), safeForm, safeOptions)
                    ? executor.writeProtectedBatchChunks(request) : executor.writeBatchChunks(request);
        });
    }

    private Object[] protectedParameters(DynamicForm form,
                                         DataScope scope,
                                         FormScopeSupport.PreparedBatchUpdate first,
                                         BatchUpdatePlan plan,
                                         BatchOptimisticUpdate update,
                                         long index,
                                         BatchWriteOptions options) {
        FormScopeSupport.PreparedBatchUpdate prepared = index == 0L
                ? first : scopes.prepareBatchUpdate(form, update, scope);
        Object[] parameters = plan.parameters(prepared.request(), index);
        return FormProtectedBatchRows.update(
                renderer.protection(), form, scope, prepared, plan, parameters, index, options);
    }

    /**
     * 独立分片批量新增动态表单数据，逐个返回分片结果。
     *
     * @param form    动态表单
     * @param rows    待写入数据流
     * @param options 批量选项，必须是 INDEPENDENT
     * @return 分片结果流
     */
}
