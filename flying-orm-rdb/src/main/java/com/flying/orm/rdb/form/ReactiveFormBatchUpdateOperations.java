package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.LongFunction;

/**
 * 负责普通及乐观锁批量更新，并保留各自的影响行数契约。
 *
 * <p>具体行为从原客户端原样迁移，SQL、参数、Scope、执行保护和响应式订阅语义不变。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class ReactiveFormBatchUpdateOperations {

    private final ReactiveFormOperations runtime;

    ReactiveFormBatchUpdateOperations(ReactiveFormOperations runtime) {
        this.runtime = Objects.requireNonNull(runtime, "shared form runtime must not be null");
    }

    /** 显式范围与客户端默认范围合并一次；各行条件和赋值独立规划，相邻同形行复用布局。 */
    Mono<BatchExecutionEvidence> updateBatch(DynamicForm form,
                                             Publisher<BatchUpdate> updates,
                                             DataScope scope,
                                             BatchWriteOptions options,
                                             LongFunction<? extends Publisher<Void>> rowCompleted) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        BatchWriteOptions safeOptions = Objects.requireNonNull(options, "batch write options must not be null");
        Flux<BatchUpdate> source = Flux.from(Objects.requireNonNull(updates,
                                                                             "batch updates must not be null"));
        return source.switchOnFirst((signal, replay) -> {
                if (signal.isOnError()) {
                    return Mono.error(Objects.requireNonNull(signal.getThrowable()));
                }
                if (!signal.hasValue()) {
                    return Mono.just(new BatchExecutionEvidence.Accumulator().snapshot(BatchExecutionState.SUCCESS, null));
                }
                BatchUpdate sourceFirst = signal.get();
                DataScope effectiveScope = runtime.scopes.effectiveScope(scope);
                FieldUseGuard.approveBatchUpdate(
                        runtime.renderer, safeForm, sourceFirst, effectiveScope, runtime.fieldUsePolicy);
                boolean protectedOperation = !safeForm.protections().encryptedFields().isEmpty();
                Function<Flux<BatchUpdate>, Publisher<BatchExecutionEvidence>> execute = batchRows -> {
                    ProtectedContainsLayout protectionLayout = ProtectedContainsLayout.resolve(safeForm).orElse(null);
                    BatchWriteRequest request = FormProtectedBatchRows.updateRequest(runtime.renderer, runtime.scopes,
                            safeForm, sourceFirst, effectiveScope, safeOptions, runtime.fieldUsePolicy,
                            protectionLayout, runtime.executor.supportsBatchRowLayouts(),
                            mapper -> batchRows.index().map(indexed -> mapper.map(indexed.getT2(), indexed.getT1())));
                    return protectionLayout != null
                            ? runtime.executor.writeProtectedBatch(request, rowCompleted) : runtime.executor.writeBatch(request, rowCompleted);
                };
                return protectedOperation ? ReactiveProtectionCpuBoundary.batch(replay, true, safeOptions.bufferSize())
                        .switchOnFirst((cpuSignal, cpuReplay) -> execute.apply(cpuReplay)) : execute.apply(replay);
            }).single();
    }
}
