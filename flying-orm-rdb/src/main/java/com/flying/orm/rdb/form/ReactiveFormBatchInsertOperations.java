package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.internal.value.FormValueSnapshots;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Objects;
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
final class ReactiveFormBatchInsertOperations extends ReactiveFormOperationSupport {

    ReactiveFormBatchInsertOperations(ReactiveFormOperationSupport runtime) {
        super(runtime);
    }
    Mono<Long> insert(DynamicForm form, Map<String, Object> values) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        Map<String, Object> writeValues = scopes.prepareWriteValues(safeForm, values, scopes.effectiveScope(DataScope.none()));
        return executor.rowsUpdated(renderer.insert(safeForm, writeValues));
    }

    /**
     * 批量新增动态表单数据。List 只是方便调用的输入形式，内部仍走统一的流式批量执行模型。
     *
     * @param form 动态表单
     * @param rows 待写入数据
     * @return 包含已证明位置和影响行数的执行事实
     */
    Mono<BatchExecutionEvidence> insertBatch(DynamicForm form, List<Map<String, Object>> rows) {
        return insertBatch(form, rows, defaultBatchWriteOptions);
    }

    /**
     * 批量 upsert 动态表单数据。List 和 Publisher 共用相同的保护和执行事实语义。
     *
     * @param form 动态表单
     * @param rows 待写入数据
     * @return 包含已证明位置和影响行数的执行事实
     */
    Mono<BatchExecutionEvidence> upsertBatch(DynamicForm form, List<Map<String, Object>> rows) {
        return upsertBatch(form, rows, defaultBatchWriteOptions);
    }

    /**
     * 使用指定批量策略新增 List 数据。方法会立即冻结 List 和每一行 Map，避免订阅前的外部修改
     * 改变待执行内容；真正的字段校验、Scope 合并和数据库访问仍在订阅时发生。
     */
    Mono<BatchExecutionEvidence> insertBatch(DynamicForm form,
                                              List<Map<String, Object>> rows,
                                              BatchWriteOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        BatchWriteOptions safeOptions = Objects.requireNonNull(options, "batch write options must not be null");
        return executeBatch(safeForm, Flux.fromIterable(snapshotBatchRows(safeForm, rows)), safeOptions, false,
                            DataScope.none(), BatchGeneratedKeys.none(), null, true);
    }

    /**
     * 使用指定批量策略 upsert List 数据，并使用与响应式 Publisher 入口完全相同的执行模型。
     */
    Mono<BatchExecutionEvidence> upsertBatch(DynamicForm form,
                                              List<Map<String, Object>> rows,
                                              BatchWriteOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        BatchWriteOptions safeOptions = Objects.requireNonNull(options, "batch write options must not be null");
        return executeBatch(safeForm, Flux.fromIterable(snapshotBatchRows(safeForm, rows)), safeOptions, true,
                            DataScope.none(), BatchGeneratedKeys.none(), null, true);
    }

    /**
     * 使用客户端默认批量策略新增动态表单数据，输入可以是真正的响应式数据流。
     *
     * @param form 动态表单
     * @param rows 待写入数据流
     * @return 批量写入结果
     */
    Mono<BatchExecutionEvidence> insertBatch(DynamicForm form, Publisher<Map<String, Object>> rows) {
        return insertBatch(form, rows, defaultBatchWriteOptions);
    }

    /**
     * 使用客户端默认批量策略 upsert 动态表单数据，输入可以是真正的响应式数据流。
     *
     * @param form 动态表单
     * @param rows 待写入数据流
     * @return 批量写入结果
     */
    Mono<BatchExecutionEvidence> upsertBatch(DynamicForm form, Publisher<Map<String, Object>> rows) {
        return upsertBatch(form, rows, defaultBatchWriteOptions);
    }

    /**
     * 使用指定批量选项新增动态表单数据。
     *
     * @param form    动态表单
     * @param rows    待写入数据流
     * @param options 批量选项
     * @return 批量写入结果
     */
    Mono<BatchExecutionEvidence> insertBatch(DynamicForm form,
                                              Publisher<Map<String, Object>> rows,
                                              BatchWriteOptions options) {
        return writeBatch(form, rows, options, false, DataScope.none(), null);
    }

    /**
     * 使用指定批量选项 upsert 动态表单数据。
     *
     * @param form    动态表单
     * @param rows    待写入数据流
     * @param options 批量选项
     * @return 批量写入结果
     */
    Mono<BatchExecutionEvidence> upsertBatch(DynamicForm form,
                                              Publisher<Map<String, Object>> rows,
                                              BatchWriteOptions options) {
        return writeBatch(form, rows, options, true, DataScope.none(), null);
    }

    Mono<BatchExecutionEvidence> writeBatch(DynamicForm form,
                                               Publisher<Map<String, Object>> rows,
                                               BatchWriteOptions options,
                                               boolean upsert,
                                               DataScope requestedScope,
                                               LongFunction<? extends Publisher<Void>> rowCompleted) {
        return writeBatch(form, rows, options, upsert, requestedScope,
                          BatchGeneratedKeys.none(), rowCompleted);
    }

    Mono<BatchExecutionEvidence> writeBatch(DynamicForm form,
                                      Publisher<Map<String, Object>> rows,
                                      BatchWriteOptions options,
                                      boolean upsert,
                                      DataScope requestedScope,
                                      BatchGeneratedKeys generatedKeys,
                                      LongFunction<? extends Publisher<Void>> rowCompleted) {
        return executeBatch(form, rows, options, upsert, requestedScope, generatedKeys, rowCompleted, false);
    }


    private Mono<BatchExecutionEvidence> executeBatch(DynamicForm form,
                                     Publisher<Map<String, Object>> rows,
                                     BatchWriteOptions options,
                                     boolean upsert,
                                     DataScope requestedScope,
                                     BatchGeneratedKeys generatedKeys,
                                     LongFunction<? extends Publisher<Void>> rowCompleted,
                                     boolean sourceOwned) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        BatchWriteOptions safeOptions = Objects.requireNonNull(options, "batch write options must not be null");
        BatchGeneratedKeys safeGeneratedKeys = Objects.requireNonNull(
                generatedKeys, "batch generated keys must not be null");
        Flux<Map<String, Object>> source = Flux.from(Objects.requireNonNull(rows, "batch rows must not be null"));
        return Mono.defer(() -> {
            DataScope scope = scopes.effectiveScope(requestedScope);
            return source.switchOnFirst((signal, replay) -> {
                if (signal.isOnError()) {
                    return Mono.error(Objects.requireNonNull(signal.getThrowable()));
                }
                if (!signal.hasValue()) {
                    return Mono.just(new BatchExecutionEvidence.Accumulator().snapshot(BatchExecutionState.SUCCESS, null));
                }
                Map<String, Object> sourceFirstRow = signal.get();
                boolean protectedSource = ReactiveProtectionCpuBoundary.writesEncryptedField(
                        safeForm, sourceFirstRow);
                Map<String, Object> firstSourceValues = protectedSource && !sourceOwned
                        ? FormValueSnapshots.snapshot(safeForm, sourceFirstRow) : sourceFirstRow;
                FieldUseGuard.approveBatchInsert(
                        safeForm, firstSourceValues, scope, upsert, fieldUsePolicy);
                Map<String, Object> firstValues = scopes.prepareWriteValues(safeForm, firstSourceValues, scope);
                boolean protectedWrite = ReactiveProtectionCpuBoundary.writesEncryptedField(safeForm, firstValues);
                Map<String, Object> ownedFirstSource = protectedWrite && !protectedSource && !sourceOwned
                        ? FormValueSnapshots.snapshot(safeForm, firstSourceValues) : firstSourceValues;
                Flux<Map<String, Object>> preparedRows = protectedWrite
                        ? replay.index().map(indexed -> indexed.getT1() == 0L
                                ? firstValues : scopes.prepareWriteValues(safeForm,
                                        sourceOwned ? indexed.getT2()
                                                : FormValueSnapshots.snapshot(safeForm, indexed.getT2()), scope))
                        : replay;
                Flux<Map<String, Object>> cpuRows = ReactiveProtectionCpuBoundary.batch(
                        preparedRows,
                        protectedWrite,
                        safeOptions.bufferSize());
                return cpuRows.switchOnFirst((cpuSignal, cpuReplay) -> {
                    FormProtectedBatchRows.BatchLayout protectionLayout =
                            FormProtectedBatchRows.layout(safeForm, safeOptions);
                    DynamicForm physicalForm = renderer.protection().physicalForm(safeForm);
                    FormProtectionSqlSupport.WriteOperation protection = renderer.protection().writeOperation(
                            safeForm, physicalForm, scope, protectionLayout.contains());
                    FormPreparedWrite firstWrite = protection.prepare(firstValues);
                    BatchInsertPlan plan = upsert
                            ? renderer.batchRenderer.upsertPlan(
                                    safeForm, firstValues, firstWrite.physicalForm(),
                                    firstWrite.values(), ownedFirstSource,
                                    scope.condition().isEmpty() ? null
                                            : scopes.prepareBatchScope(safeForm, physicalForm, scope).where())
                            : renderer.batchRenderer.insertPlan(firstWrite.physicalForm(), firstWrite.values());
                    Flux<Object[]> parameters = cpuReplay.index().map(indexed -> {
                        Map<String, Object> logical = protectedWrite
                                ? indexed.getT2() : indexed.getT1() == 0L
                                        ? firstValues : scopes.prepareWriteValues(safeForm, indexed.getT2(), scope);
                        FormPreparedWrite write = indexed.getT1() == 0
                                ? firstWrite : protection.prepare(logical);
                        Object[] row = indexed.getT1() == 0L
                                ? plan.firstParameters() : plan.parameters(write.values(), indexed.getT1());
                        return FormProtectedBatchRows.insert(
                                protection, logical, plan, row, upsert, protectionLayout);
                    });
                    BatchWriteRequest request = plan.request(
                            parameters, safeOptions, safeGeneratedKeys);
                    return FormProtectedBatchRows.requiresProtectedExecution(protectionLayout)
                            ? executor.writeProtectedBatch(request, rowCompleted) : executor.writeBatch(request, rowCompleted);
                });
            }).single();
        });
    }





    /**
     * 冻结 List 便利入口在冷订阅前已经接收的行。每行保持原有迭代顺序；数组、缓冲区、可变文本和
     * 旧式可变时间值在入口处固定，避免订阅前修改改变批量内容。Publisher 入口在逐行接收时处理，
     * 不经过这里的预先收集或复制。
     */
    private static List<Map<String, Object>> snapshotBatchRows(DynamicForm form,
                                                                List<Map<String, Object>> rows) {
        return Objects.requireNonNull(rows, "batch rows must not be null")
                      .stream()
                      .map(row -> FormValueSnapshots.snapshot(form, Objects.requireNonNull(
                              row, "batch row must not be null")))
                      .toList();
    }
}
