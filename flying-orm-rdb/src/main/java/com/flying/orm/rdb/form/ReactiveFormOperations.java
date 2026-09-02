package com.flying.orm.rdb.form;

import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorPageResult;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.result.DynamicRow;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 动态表单内部操作总入口。
 *
 * <p>它只负责把不可变的 QuerySpec、WriteSpec、BatchSpec 分派到查询、分页、写入、删除和批量协作者。
 * 各协作者通过组合明确依赖关系，并共享本对象创建的 Scope 合并器、结果映射器和默认执行配置。这样既没有
 * 多层继承带来的隐式方法来源，也不会让同步门面复制一套 SQL、安全校验或执行保护逻辑。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class ReactiveFormOperations extends ReactiveFormOperationSupport {

    final ReactiveFormBatchInsertOperations batchInserts;
    final ReactiveFormBatchUpdateOperations batchUpdates;
    final ReactiveJoinQueryOperations joins;

    ReactiveFormOperations(ReactiveFormOperationContext context) {
        super(context);
        this.batchInserts = new ReactiveFormBatchInsertOperations(this);
        this.batchUpdates = new ReactiveFormBatchUpdateOperations(this);
        this.joins = new ReactiveJoinQueryOperations(this);
    }

    Flux<DynamicRow> selectJoin(JoinQuerySpec spec, com.flying.orm.rdb.execution.SqlExecutionOptions options) {
        return joins.select(spec, options);
    }

    Mono<PageResult<DynamicRow>> pageJoin(JoinQuerySpec spec,
                                          PageQuery page,
                                          com.flying.orm.rdb.execution.SqlExecutionOptions options) {
        return joins.page(spec, page, options);
    }

    Flux<DynamicRow> selectSpec(QuerySpec spec) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> planner.select(safeSpec)).flatMapMany(this::select)
                : select(planner.select(safeSpec));
    }

    private Flux<DynamicRow> select(FormOperationPlanner.PlannedQuery plan) {
        if (plan.contains()) {
            return executor.query(plan.request(), plan.options())
                           .collectList()
                           .flatMapMany(rows -> verifyContains(plan, rows));
        }
        return results.decodeRows(plan.form(), executor.query(plan.request(), plan.options()), plan.options(),
                                  plan.scope(), plan.displayMode(), plan.decodingFields());
    }

    Mono<PageResult<DynamicRow>> pageSpec(QuerySpec spec, PageQuery page) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        PageQuery safePage = Objects.requireNonNull(page, "page query must not be null");
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> planner.page(safeSpec, safePage)).flatMap(this::page)
                : page(planner.page(safeSpec, safePage));
    }

    private Mono<PageResult<DynamicRow>> page(FormOperationPlanner.PlannedPage plan) {
        if (plan.contains()) {
            return executor.query(plan.dataRequest(), plan.options())
                           .collectList()
                           .flatMap(rows -> verifyContains(plan, rows))
                           .map(rows -> containsPage(rows, plan.page()));
        }
        Mono<Long> total = executor.query(plan.countRequest(), plan.options())
                                   .next()
                                   .map(CountResultReader::read)
                                   .defaultIfEmpty(0L);
        return total.flatMap(count -> count == 0L
                ? Mono.just(PageResult.of(List.of(), 0L, plan.page()))
                : results.decodeRows(plan.form(), executor.query(plan.dataRequest(), plan.options()), plan.options(),
                                     plan.scope(), plan.displayMode(), plan.decodingFields())
                         .collectList()
                         .map(rows -> PageResult.of(rows, count, plan.page())));
    }

    private Flux<DynamicRow> verifyContains(FormOperationPlanner.PlannedQuery plan,
                                            List<DynamicRow> rawRows) {
        ProtectedContainsResultSupport.requireCandidateLimit(rawRows.size());
        return results.decodeRows(plan.form(), Flux.fromIterable(rawRows), plan.options(),
                                  plan.scope(), com.flying.orm.core.protection.SensitiveDisplayMode.FULL,
                                  plan.decodingFields())
                      .collectList()
                      .flatMapMany(rows -> Flux.fromIterable(containsResults.finish(
                              plan.form(), plan.containsQuery(), rows, plan.outputFields(), plan.displayMode())));
    }

    private Mono<List<DynamicRow>> verifyContains(FormOperationPlanner.PlannedPage plan,
                                                   List<DynamicRow> rawRows) {
        ProtectedContainsResultSupport.requireCandidateLimit(rawRows.size());
        return results.decodeRows(plan.form(), Flux.fromIterable(rawRows), plan.options(),
                                  plan.scope(), com.flying.orm.core.protection.SensitiveDisplayMode.FULL,
                                  plan.decodingFields())
                      .collectList()
                      .map(rows -> containsResults.finish(
                              plan.form(), plan.containsQuery(), rows, plan.outputFields(), plan.displayMode()));
    }

    private static PageResult<DynamicRow> containsPage(List<DynamicRow> rows, PageQuery page) {
        int from = (int) Math.min(page.offset(), rows.size());
        int to = Math.min(from + page.size(), rows.size());
        return PageResult.of(rows.subList(from, to), rows.size(), page);
    }

    Mono<CursorPageResult<DynamicRow>> cursorPageSpec(QuerySpec spec, CursorPageQuery page) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        CursorPageQuery safePage = Objects.requireNonNull(page, "cursor page query must not be null");
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> planner.cursorPage(safeSpec, safePage))
                                               .flatMap(this::cursorPage)
                : cursorPage(planner.cursorPage(safeSpec, safePage));
    }

    private Mono<CursorPageResult<DynamicRow>> cursorPage(FormOperationPlanner.PlannedCursorPage plan) {
        if (plan.contains()) {
            return executor.query(plan.request(), plan.options())
                           .collectList()
                           .flatMap(rows -> verifyContains(plan, rows))
                           .map(rows -> FormCursorResults.from(rows, plan.page()));
        }
        return results.decodeRows(plan.form(), executor.query(plan.request(), plan.options()), plan.options(),
                                  plan.scope(), plan.displayMode(), plan.decodingFields())
                      .collectList()
                      .map(rows -> FormCursorResults.from(rows, plan.page()));
    }

    private Mono<List<DynamicRow>> verifyContains(FormOperationPlanner.PlannedCursorPage plan,
                                                   List<DynamicRow> rawRows) {
        ProtectedContainsResultSupport.requireCandidateLimit(rawRows.size());
        return results.decodeRows(plan.form(), Flux.fromIterable(rawRows), plan.options(),
                                  plan.scope(), com.flying.orm.core.protection.SensitiveDisplayMode.FULL,
                                  plan.decodingFields())
                      .collectList()
                      .map(rows -> containsResults.finish(
                              plan.form(), plan.containsQuery(), rows, plan.outputFields(), plan.displayMode()));
    }

    Mono<Long> insertSpec(WriteSpec spec) {
        WriteSpec safeSpec = Objects.requireNonNull(spec, "insert spec must not be null");
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> planner.insert(safeSpec)).flatMap(this::write)
                : write(planner.insert(safeSpec));
    }

    private Mono<Long> write(FormOperationPlanner.PlannedWrite plan) {
        if (plan.protectedWriteRequired()) {
            return executor.atomicProtectedWrite(plan.protectedWrite(), plan.options())
                           .map(result -> plan.requireSuccess(result.affectedRows()));
        }
        return executor.rowsUpdated(plan.request(), plan.options()).map(plan::requireSuccess);
    }

    Mono<SqlWriteResult> insertReturningKeysSpec(WriteSpec spec) {
        WriteSpec safeSpec = Objects.requireNonNull(spec, "insert spec must not be null");
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> planner.insert(safeSpec))
                                               .flatMap(this::writeReturningKeys)
                : writeReturningKeys(planner.insert(safeSpec));
    }

    private Mono<SqlWriteResult> writeReturningKeys(FormOperationPlanner.PlannedWrite plan) {
        if (plan.protectedWriteRequired()) {
            return executor.atomicProtectedWrite(plan.protectedWrite(), plan.options())
                           .doOnNext(result -> plan.requireSuccess(result.affectedRows()));
        }
        Mono<SqlWriteResult> result = plan.generatedKeyColumn()
                                          .map(column -> executor.rowsUpdatedReturningKeys(
                                                  plan.request(), plan.options(), column))
                                          .orElseGet(() -> executor.rowsUpdatedReturningKeys(
                                                  plan.request(), plan.options()));
        return result.doOnNext(value -> plan.requireSuccess(value.affectedRows()));
    }

    Mono<Long> updateSpec(WriteSpec spec) {
        WriteSpec safeSpec = Objects.requireNonNull(spec, "update spec must not be null");
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> planner.update(safeSpec)).flatMap(this::write)
                : write(planner.update(safeSpec));
    }

    Mono<Long> deleteSpec(WriteSpec spec) {
        WriteSpec safeSpec = Objects.requireNonNull(spec, "delete spec must not be null");
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> planner.delete(safeSpec)).flatMap(this::write)
                : write(planner.delete(safeSpec));
    }

    Mono<Long> physicalDeleteSpec(WriteSpec spec) {
        WriteSpec safeSpec = Objects.requireNonNull(spec, "physical delete spec must not be null");
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> planner.physicalDelete(safeSpec)).flatMap(this::write)
                : write(planner.physicalDelete(safeSpec));
    }

    Mono<BatchWriteResult> writeBatchSpec(BatchSpec spec) {
        BatchSpec safeSpec = Objects.requireNonNull(spec, "batch spec must not be null");
        BatchWriteOptions options = safeSpec.options().orElse(defaultBatchWriteOptions);
        return switch (safeSpec.operation()) {
            case INSERT -> batchInserts.writeBatch(safeSpec.form(), mapRows(safeSpec), options, false,
                                                   safeSpec.scope(), safeSpec.generatedKeys(), safeSpec.completion());
            case UPSERT -> batchInserts.writeBatch(safeSpec.form(), mapRows(safeSpec), options, true,
                                                   safeSpec.scope(), safeSpec.generatedKeys(), safeSpec.completion());
            case UPDATE -> batchUpdates.updateBatch(safeSpec.form(), updateRows(safeSpec), safeSpec.scope(), options,
                                                     safeSpec.completion());
        };
    }

    Flux<BatchChunkResult> writeBatchChunksSpec(BatchSpec spec) {
        BatchSpec safeSpec = Objects.requireNonNull(spec, "batch spec must not be null");
        BatchWriteOptions options = safeSpec.options().orElse(defaultBatchWriteOptions);
        return switch (safeSpec.operation()) {
            case INSERT -> batchInserts.writeBatchChunks(safeSpec.form(), mapRows(safeSpec), options, false,
                                                         safeSpec.scope(), safeSpec.generatedKeys(), safeSpec.completion());
            case UPSERT -> batchInserts.writeBatchChunks(safeSpec.form(), mapRows(safeSpec), options, true,
                                                         safeSpec.scope(), safeSpec.generatedKeys(), safeSpec.completion());
            case UPDATE -> batchUpdates.updateBatchChunks(safeSpec.form(), updateRows(safeSpec), safeSpec.scope(),
                                                           options, safeSpec.completion());
        };
    }

    private boolean requiresProtectedPlanning(QuerySpec spec) {
        if (spec.form().protections().encryptedFields().isEmpty()) {
            return false;
        }
        DataScope effectiveScope = scopes.effectiveScope(spec.scope());
        return ReactiveProtectionCpuBoundary.usesEncryptedCondition(spec.form(), spec.where())
                || ReactiveProtectionCpuBoundary.usesEncryptedScope(spec.form(), effectiveScope)
                || spec.structuredInput().isPresent();
    }

    private boolean requiresProtectedPlanning(WriteSpec spec) {
        if (spec.form().protections().encryptedFields().isEmpty()) {
            return false;
        }
        DataScope effectiveScope = scopes.effectiveScope(spec.scope());
        return ReactiveProtectionCpuBoundary.writesEncryptedField(spec.form(), spec.ownedValues())
                || ReactiveProtectionCpuBoundary.usesEncryptedCondition(spec.form(), spec.where())
                || ReactiveProtectionCpuBoundary.usesEncryptedScope(spec.form(), effectiveScope);
    }

    @SuppressWarnings("unchecked")
    private static Publisher<Map<String, Object>> mapRows(BatchSpec spec) {
        return Flux.from(spec.rows()).map(row -> {
            if (!(row instanceof Map<?, ?>)) {
                throw new IllegalArgumentException("insert/upsert batch rows must be field maps");
            }
            return (Map<String, Object>) row;
        });
    }

    private static Publisher<BatchOptimisticUpdate> updateRows(BatchSpec spec) {
        return Flux.from(spec.rows()).map(row -> {
            if (!(row instanceof BatchOptimisticUpdate update)) {
                throw new IllegalArgumentException("update batch rows must be BatchOptimisticUpdate values");
            }
            return update;
        });
    }

}
