package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.BatchOperation;
import com.flying.orm.rdb.internal.value.FormValueSnapshots;
import com.flying.orm.rdb.internal.mapping.RepositoryUpsertValues;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.LongFunction;

/**
 * 负责动态表单的批量插入与 upsert。
 *
 * <p>具体行为从原客户端原样迁移，SQL、参数、Scope、执行保护和响应式订阅语义不变。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class ReactiveFormBatchInsertOperations {

    private final ReactiveFormOperations runtime;

    ReactiveFormBatchInsertOperations(ReactiveFormOperations runtime) {
        this.runtime = Objects.requireNonNull(runtime, "shared form runtime must not be null");
    }

    Mono<BatchExecutionEvidence> writeBatch(BatchSpec spec, Publisher<Map<String, Object>> rows,
                                           BatchWriteOptions options,
                                           LongFunction<? extends Publisher<Void>> rowCompleted) {
        DynamicForm safeForm = spec.form();
        BatchWriteOptions safeOptions = Objects.requireNonNull(options, "batch write options must not be null");
        boolean upsert = spec.operation() == BatchOperation.UPSERT;
        boolean ignoreConflicts = spec.ignoresConflicts();
        Flux<Map<String, Object>> input = Flux.from(Objects.requireNonNull(rows, "batch rows must not be null"));
        Flux<Map<String, Object>> source = ignoreConflicts || !spec.ignoredUpdates().isEmpty()
                || !safeForm.runtimeDefaultFields().isEmpty()
                ? input.map(row -> RepositoryUpsertValues.prepareInsert(
                        safeForm, row, !upsert || ignoreConflicts, spec.ignoredUpdates())) : input;
        return Mono.defer(() -> {
            DataScope scope = runtime.scopes.effectiveScope(spec.scope());
            return source.switchOnFirst((signal, replay) -> {
                if (signal.isOnError()) {
                    return Mono.error(Objects.requireNonNull(signal.getThrowable()));
                }
                if (!signal.hasValue()) {
                    return Mono.just(new BatchExecutionEvidence.Accumulator().snapshot(BatchExecutionState.SUCCESS, null));
                }
                Map<String, Object> sourceFirstRow = signal.get();
                // 后续行可选择不同字段；表单存在加密字段时先快照，再在批量 CPU 边界规划。
                boolean protectedWrite = !safeForm.protections().encryptedFields().isEmpty();
                Map<String, Object> firstSourceValues = protectedWrite
                        ? snapshotRow(safeForm, sourceFirstRow) : sourceFirstRow;
                FieldUseGuard.approveBatchInsert(
                        safeForm, sourceFirstRow, scope,
                        upsert && !ignoreConflicts && !BatchInsertPlan.generatedInsert(spec, sourceFirstRow),
                        runtime.fieldUsePolicy, spec.conflictFields());
                Map<String, Object> firstValues = runtime.scopes.prepareWriteValues(safeForm, firstSourceValues, scope);
                Flux<Map<String, Object>> preparedRows = protectedWrite
                        ? replay.index().map(indexed -> indexed.getT1() == 0L
                                ? firstSourceValues : snapshotRow(safeForm, indexed.getT2()))
                        : replay;
                Function<Flux<Map<String, Object>>, Publisher<BatchExecutionEvidence>> execute = batchRows -> {
                    ProtectedContainsLayout protectionLayout = ProtectedContainsLayout.resolve(safeForm).orElse(null);
                    BatchWriteRequest request = FormProtectedBatchRows.insertRequest(runtime.renderer, runtime.scopes,
                            spec, sourceFirstRow, firstValues, scope, safeOptions, runtime.fieldUsePolicy,
                            protectionLayout, runtime.executor.supportsBatchRowLayouts(),
                            mapper -> batchRows.index().map(indexed -> mapper.map(indexed.getT2(), indexed.getT1())));
                    return protectionLayout != null
                            ? runtime.executor.writeProtectedBatch(request, rowCompleted) : runtime.executor.writeBatch(request, rowCompleted);
                };
                return protectedWrite ? ReactiveProtectionCpuBoundary.batch(preparedRows, true, safeOptions.bufferSize())
                        .switchOnFirst((cpuSignal, cpuReplay) -> execute.apply(cpuReplay)) : execute.apply(preparedRows);
            }).single();
        });
    }

    private Map<String, Object> snapshotRow(DynamicForm form, Map<String, Object> values) {
        Map<String, Object> snapshot = FormValueSnapshots.snapshot(
                form, values, field -> runtime.renderer.customFieldMapping(field) != null);
        return values instanceof RepositoryUpsertValues staged ? staged.withSnapshot(snapshot) : snapshot;
    }
}
