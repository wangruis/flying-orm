package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.internal.value.FormValueSnapshots;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Objects;

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
     * @return 包含事务状态、分片结果和影响行数的批量结果
     */
    Mono<BatchWriteResult> insertBatch(DynamicForm form, List<Map<String, Object>> rows) {
        return insertBatch(form, rows, defaultBatchWriteOptions);
    }

    /**
     * 批量 upsert 动态表单数据。List 和 Publisher 共用相同的事务、保护和结果语义。
     *
     * @param form 动态表单
     * @param rows 待写入数据
     * @return 包含事务状态、分片结果和影响行数的批量结果
     */
    Mono<BatchWriteResult> upsertBatch(DynamicForm form, List<Map<String, Object>> rows) {
        return upsertBatch(form, rows, defaultBatchWriteOptions);
    }

    /**
     * 使用指定批量策略新增 List 数据。方法会立即冻结 List 和每一行 Map，避免订阅前的外部修改
     * 改变待执行内容；真正的字段校验、Scope 合并和数据库访问仍在订阅时发生。
     */
    Mono<BatchWriteResult> insertBatch(DynamicForm form,
                                              List<Map<String, Object>> rows,
                                              BatchWriteOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        BatchWriteOptions safeOptions = Objects.requireNonNull(options, "batch write options must not be null");
        return writeBatch(safeForm, Flux.fromIterable(snapshotBatchRows(safeForm, rows)), safeOptions, false,
                          DataScope.none(), BatchWriteCompletion.noop());
    }

    /**
     * 使用指定批量策略 upsert List 数据，并使用与响应式 Publisher 入口完全相同的执行模型。
     */
    Mono<BatchWriteResult> upsertBatch(DynamicForm form,
                                              List<Map<String, Object>> rows,
                                              BatchWriteOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        BatchWriteOptions safeOptions = Objects.requireNonNull(options, "batch write options must not be null");
        return writeBatch(safeForm, Flux.fromIterable(snapshotBatchRows(safeForm, rows)), safeOptions, true,
                          DataScope.none(), BatchWriteCompletion.noop());
    }

    /**
     * 使用客户端默认批量策略新增动态表单数据，输入可以是真正的响应式数据流。初始默认是 ATOMIC。
     *
     * @param form 动态表单
     * @param rows 待写入数据流
     * @return 批量写入结果
     */
    Mono<BatchWriteResult> insertBatch(DynamicForm form, Publisher<Map<String, Object>> rows) {
        return insertBatch(form, rows, defaultBatchWriteOptions);
    }

    /**
     * 使用客户端默认批量策略 upsert 动态表单数据，输入可以是真正的响应式数据流。初始默认是 ATOMIC。
     *
     * @param form 动态表单
     * @param rows 待写入数据流
     * @return 批量写入结果
     */
    Mono<BatchWriteResult> upsertBatch(DynamicForm form, Publisher<Map<String, Object>> rows) {
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
    Mono<BatchWriteResult> insertBatch(DynamicForm form,
                                              Publisher<Map<String, Object>> rows,
                                              BatchWriteOptions options) {
        return writeBatch(form, rows, options, false, DataScope.none(), BatchWriteCompletion.noop());
    }

    /**
     * 使用指定批量选项 upsert 动态表单数据。
     *
     * @param form    动态表单
     * @param rows    待写入数据流
     * @param options 批量选项
     * @return 批量写入结果
     */
    Mono<BatchWriteResult> upsertBatch(DynamicForm form,
                                              Publisher<Map<String, Object>> rows,
                                              BatchWriteOptions options) {
        return writeBatch(form, rows, options, true, DataScope.none(), BatchWriteCompletion.noop());
    }

    Mono<BatchWriteResult> writeBatch(DynamicForm form,
                                               Publisher<Map<String, Object>> rows,
                                               BatchWriteOptions options,
                                               boolean upsert,
                                               DataScope requestedScope,
                                               BatchWriteCompletion completion) {
        return writeBatch(form, rows, options, upsert, requestedScope,
                          BatchGeneratedKeys.none(), completion);
    }

    Mono<BatchWriteResult> writeBatch(DynamicForm form,
                                      Publisher<Map<String, Object>> rows,
                                      BatchWriteOptions options,
                                      boolean upsert,
                                      DataScope requestedScope,
                                      BatchGeneratedKeys generatedKeys,
                                      BatchWriteCompletion completion) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        BatchWriteOptions safeOptions = Objects.requireNonNull(options, "batch write options must not be null");
        BatchGeneratedKeys safeGeneratedKeys = Objects.requireNonNull(
                generatedKeys, "batch generated keys must not be null");
        Flux<Map<String, Object>> source = Flux.from(Objects.requireNonNull(rows, "batch rows must not be null"));
        return source.switchOnFirst((signal, replay) -> {
                if (signal.isOnError()) {
                    return Mono.error(Objects.requireNonNull(signal.getThrowable()));
                }
                if (!signal.hasValue()) {
                    return Mono.just(BatchWriteResult.empty(safeOptions.mode()));
                }
                Map<String, Object> sourceFirstRow = signal.get();
                DataScope scope = scopes.effectiveScope(requestedScope);
                Map<String, Object> firstValues = scopes.prepareWriteValues(safeForm, sourceFirstRow, scope);
                Flux<Map<String, Object>> cpuRows = ReactiveProtectionCpuBoundary.batch(
                        replay,
                        ReactiveProtectionCpuBoundary.writesEncryptedField(safeForm, firstValues),
                        safeOptions.chunkSize());
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
                                    firstWrite.values(), sourceFirstRow)
                            : renderer.batchRenderer.insertPlan(firstWrite.physicalForm(), firstWrite.values());
                    Flux<Object[]> parameters = cpuReplay.index().map(indexed -> {
                        Map<String, Object> logical = indexed.getT1() == 0
                                ? firstValues : scopes.prepareWriteValues(safeForm, indexed.getT2(), scope);
                        FormPreparedWrite write = indexed.getT1() == 0
                                ? firstWrite : protection.prepare(logical);
                        Object[] row = indexed.getT1() == 0L
                                ? plan.firstParameters() : plan.parameters(write.values(), indexed.getT1());
                        return FormProtectedBatchRows.insert(
                                protection, logical, write, plan, row,
                                indexed.getT1(), upsert, protectionLayout);
                    });
                    BatchWriteRequest request = plan.request(
                            parameters, safeOptions, safeGeneratedKeys, completion);
                    return FormProtectedBatchRows.requiresProtectedExecution(protectionLayout)
                            ? executor.writeProtectedBatch(request) : executor.writeBatch(request);
                });
            }).single();
    }

    /**
     * 批量执行带版本条件的更新。使用客户端默认批量策略，初始默认是 ATOMIC。
     */
    Flux<BatchChunkResult> insertBatchChunks(DynamicForm form,
                                                    Publisher<Map<String, Object>> rows,
                                                    BatchWriteOptions options) {
        return writeBatchChunks(form, rows, options, false, DataScope.none());
    }

    /**
     * 独立分片批量 upsert，逐个返回分片结果。
     *
     * @param form    动态表单
     * @param rows    待写入数据流
     * @param options 批量选项，必须是 INDEPENDENT
     * @return 分片结果流
     */
    Flux<BatchChunkResult> upsertBatchChunks(DynamicForm form,
                                                    Publisher<Map<String, Object>> rows,
                                                    BatchWriteOptions options) {
        return writeBatchChunks(form, rows, options, true, DataScope.none());
    }

    Flux<BatchChunkResult> writeBatchChunks(DynamicForm form,
                                                     Publisher<Map<String, Object>> rows,
                                                     BatchWriteOptions options,
                                                     boolean upsert,
                                                     DataScope requestedScope) {
        return writeBatchChunks(form, rows, options, upsert, requestedScope,
                                BatchGeneratedKeys.none(), BatchWriteCompletion.noop());
    }

    Flux<BatchChunkResult> writeBatchChunks(DynamicForm form,
                                            Publisher<Map<String, Object>> rows,
                                            BatchWriteOptions options,
                                            boolean upsert,
                                            DataScope requestedScope,
                                            BatchGeneratedKeys generatedKeys,
                                            BatchWriteCompletion completion) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        BatchWriteOptions safeOptions = Objects.requireNonNull(options, "batch write options must not be null");
        BatchGeneratedKeys safeGeneratedKeys = Objects.requireNonNull(
                generatedKeys, "batch generated keys must not be null");
        if (safeOptions.mode() != BatchWriteOptions.Mode.INDEPENDENT) {
            return Flux.error(new IllegalArgumentException("batch chunks require independent mode"));
        }
        Flux<Map<String, Object>> source = Flux.from(Objects.requireNonNull(rows, "batch rows must not be null"));
        return source.switchOnFirst((signal, replay) -> {
                if (signal.isOnError()) {
                    return Flux.error(Objects.requireNonNull(signal.getThrowable()));
                }
                if (!signal.hasValue()) {
                    return Flux.empty();
                }
                Map<String, Object> sourceFirstRow = signal.get();
                DataScope scope = scopes.effectiveScope(requestedScope);
                Map<String, Object> firstValues = scopes.prepareWriteValues(safeForm, sourceFirstRow, scope);
                Flux<Map<String, Object>> cpuRows = ReactiveProtectionCpuBoundary.batch(
                        replay,
                        ReactiveProtectionCpuBoundary.writesEncryptedField(safeForm, firstValues),
                        safeOptions.chunkSize());
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
                                    firstWrite.values(), sourceFirstRow)
                            : renderer.batchRenderer.insertPlan(firstWrite.physicalForm(), firstWrite.values());
                    Flux<Object[]> parameters = cpuReplay.index().map(indexed -> {
                        Map<String, Object> logical = indexed.getT1() == 0
                                ? firstValues : scopes.prepareWriteValues(safeForm, indexed.getT2(), scope);
                        FormPreparedWrite write = indexed.getT1() == 0
                                ? firstWrite : protection.prepare(logical);
                        Object[] row = indexed.getT1() == 0L
                                ? plan.firstParameters() : plan.parameters(write.values(), indexed.getT1());
                        return FormProtectedBatchRows.insert(
                                protection, logical, write, plan, row,
                                indexed.getT1(), upsert, protectionLayout);
                    });
                    BatchWriteRequest request = plan.request(parameters, safeOptions,
                                                             safeGeneratedKeys, completion);
                    return FormProtectedBatchRows.requiresProtectedExecution(protectionLayout)
                            ? executor.writeProtectedBatchChunks(request) : executor.writeBatchChunks(request);
                });
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
