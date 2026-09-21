package com.flying.orm.rdb.form;

import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorPageResult;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

/**
 * R2DBC 分页计划的惰性执行与结果整理；不规划 SQL，也不读取事务状态。
 *
 * @author wangr
 * @version v3.2
 */
final class ReactiveFormPageResultSupport {

    private ReactiveFormPageResultSupport() {
    }

    static Mono<PageResult<DynamicRow>> pageSpec(
            ReactiveFormOperations operations, QuerySpec spec, PageQuery page) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        PageQuery safePage = Objects.requireNonNull(page, "page query must not be null");
        if (operations.governed) {
            return operations.requiresProtectedPlanning(safeSpec)
                    ? ReactiveProtectionCpuBoundary.plan(() -> operations.planner.pageGoverned(
                            safeSpec, safePage, operations.fieldUsePolicy, operations.queryShapeLimits))
                                                   .flatMap(envelope -> page(operations, envelope))
                    : page(operations, operations.planner.pageGoverned(
                            safeSpec, safePage, operations.fieldUsePolicy, operations.queryShapeLimits));
        }
        return operations.requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> operations.planner.page(safeSpec, safePage))
                                               .flatMap(plan -> page(operations, plan))
                : page(operations, operations.planner.page(safeSpec, safePage));
    }

    static Mono<PageResult<DynamicRow>> page(
            ReactiveFormOperations operations,
            GovernedPlanEnvelope<FormOperationPlanner.PlannedPage> envelope) {
        return page(operations, envelope.plan(), envelope.plan().displayMode())
                .map(result -> new PageResult<>(
                        result.rows().stream()
                              .map(row -> FieldUseGuard.applyVisibility(
                                      operations.renderer, envelope.plan().form(), row, envelope.fieldUse()))
                              .toList(),
                        result.total(), result.page(), result.size()));
    }

    static Mono<PageResult<DynamicRow>> page(
            ReactiveFormOperations operations, FormOperationPlanner.PlannedPage plan) {
        return page(operations, plan, plan.displayMode());
    }

    static Mono<CursorPageResult<DynamicRow>> cursorPageSpec(
            ReactiveFormOperations operations, QuerySpec spec, CursorPageQuery page) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        CursorPageQuery safePage = Objects.requireNonNull(page, "cursor page query must not be null");
        if (operations.governed) {
            return operations.requiresProtectedPlanning(safeSpec)
                    ? ReactiveProtectionCpuBoundary.plan(() -> operations.planner.cursorPageGoverned(
                            safeSpec, safePage, operations.fieldUsePolicy, operations.queryShapeLimits))
                                                   .flatMap(envelope -> cursorPage(operations, envelope))
                    : cursorPage(operations, operations.planner.cursorPageGoverned(
                            safeSpec, safePage, operations.fieldUsePolicy, operations.queryShapeLimits));
        }
        return operations.requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> operations.planner.cursorPage(safeSpec, safePage))
                                               .flatMap(plan -> cursorPage(operations, plan))
                : cursorPage(operations, operations.planner.cursorPage(safeSpec, safePage));
    }

    static Mono<CursorPageResult<DynamicRow>> cursorPage(
            ReactiveFormOperations operations,
            GovernedPlanEnvelope<FormOperationPlanner.PlannedCursorPage> envelope) {
        return cursorPage(operations, envelope.plan(), envelope.plan().displayMode())
                .map(result -> new CursorPageResult<>(
                        result.rows().stream()
                              .map(row -> FieldUseGuard.applyVisibility(
                                      operations.renderer, envelope.plan().form(), row, envelope.fieldUse()))
                              .toList(),
                        result.nextCursor(), result.hasMore()));
    }

    static Mono<CursorPageResult<DynamicRow>> cursorPage(
            ReactiveFormOperations operations, FormOperationPlanner.PlannedCursorPage plan) {
        return cursorPage(operations, plan, plan.displayMode());
    }

    static Mono<KeysetPageResult<DynamicRow>> keysetPageSpec(
            ReactiveFormOperations operations, QuerySpec spec, KeysetPageQuery page) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        KeysetPageQuery safePage = Objects.requireNonNull(page, "keyset page query must not be null");
        if (operations.governed) {
            return operations.requiresProtectedPlanning(safeSpec)
                    ? ReactiveProtectionCpuBoundary.plan(() -> operations.planner.keysetPageGoverned(
                            safeSpec, safePage, operations.fieldUsePolicy, operations.queryShapeLimits))
                                                   .flatMap(envelope -> keysetPage(operations, envelope))
                    : keysetPage(operations, operations.planner.keysetPageGoverned(
                            safeSpec, safePage, operations.fieldUsePolicy, operations.queryShapeLimits));
        }
        return operations.requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> operations.planner.keysetPage(safeSpec, safePage))
                                               .flatMap(plan -> keysetPage(operations, plan, null))
                : keysetPage(operations, operations.planner.keysetPage(safeSpec, safePage), null);
    }

    static Mono<KeysetPageResult<DynamicRow>> keysetPage(
            ReactiveFormOperations operations,
            GovernedPlanEnvelope<FormOperationPlanner.PlannedKeysetPage> envelope) {
        return keysetPage(operations, envelope.plan(), envelope.fieldUse());
    }

    static Mono<KeysetPageResult<DynamicRow>> keysetPage(
            ReactiveFormOperations operations,
            FormOperationPlanner.PlannedKeysetPage plan,
            FieldUseSnapshot fieldUse) {
        SensitiveDisplayMode displayMode = plan.displayMode();
        Flux<DynamicRow> physicalRows = operations.executor.query(plan.request(), plan.options());
        Flux<DynamicRow> rowsForDecoding = plan.layout().hasHiddenSelections()
                ? physicalRows.map(plan.layout()::logicalRowForDecoding) : physicalRows;
        return operations.results.decodeRows(
                        plan.form(), rowsForDecoding, plan.options(),
                        plan.scope(), displayMode, plan.layout().decodingFields())
                .transform(rows -> plan.layout().hasHiddenSelections()
                        ? rows.map(plan.layout()::physicalRowAfterDecoding) : rows)
                .collectList()
                .map(rows -> plan.layout().finish(
                        rows, plan.page().size(),
                        fieldUse == null
                                ? java.util.function.UnaryOperator.identity()
                                : row -> FieldUseGuard.applyVisibility(
                                        operations.renderer, plan.form(), row, fieldUse)));
    }

    private static Mono<PageResult<DynamicRow>> page(
            ReactiveFormOperations operations,
            FormOperationPlanner.PlannedPage plan,
            SensitiveDisplayMode displayMode) {
        if (plan.contains()) {
            return operations.executor.query(plan.dataRequest(), plan.options())
                    .collectList()
                    .flatMap(rows -> verifyContains(operations, plan, rows, displayMode))
                    .map(rows -> containsPage(rows, plan.page()));
        }
        Mono<Long> total = operations.executor.query(plan.countRequest(), plan.options())
                .next().map(CountResultReader::read).defaultIfEmpty(0L);
        return total.flatMap(count -> count == 0L
                ? Mono.just(PageResult.of(List.of(), 0L, plan.page()))
                : operations.results.decodeRows(
                        plan.form(), operations.executor.query(plan.dataRequest(), plan.options()),
                        plan.options(), plan.scope(), displayMode, plan.decodingFields())
                        .collectList().map(rows -> PageResult.of(rows, count, plan.page())));
    }

    private static Mono<CursorPageResult<DynamicRow>> cursorPage(
            ReactiveFormOperations operations,
            FormOperationPlanner.PlannedCursorPage plan,
            SensitiveDisplayMode displayMode) {
        if (plan.contains()) {
            return operations.executor.query(plan.request(), plan.options())
                    .collectList()
                    .flatMap(rows -> verifyContains(operations, plan, rows, displayMode))
                    .map(rows -> FormCursorResults.from(rows, plan.page()));
        }
        return operations.results.decodeRows(
                        plan.form(), operations.executor.query(plan.request(), plan.options()),
                        plan.options(), plan.scope(), displayMode, plan.decodingFields())
                .collectList().map(rows -> FormCursorResults.from(rows, plan.page()));
    }

    private static Mono<List<DynamicRow>> verifyContains(
            ReactiveFormOperations operations,
            FormOperationPlanner.PlannedPage plan,
            List<DynamicRow> rawRows,
            SensitiveDisplayMode displayMode) {
        ProtectedContainsResultSupport.requireCandidateLimit(rawRows.size());
        return operations.results.decodeRows(
                        plan.form(), Flux.fromIterable(rawRows), plan.options(),
                        plan.scope(), SensitiveDisplayMode.FULL, plan.decodingFields())
                .collectList()
                .map(rows -> operations.containsResults.finish(
                        plan.form(), plan.containsQuery(), rows, plan.outputFields(), displayMode));
    }

    private static Mono<List<DynamicRow>> verifyContains(
            ReactiveFormOperations operations,
            FormOperationPlanner.PlannedCursorPage plan,
            List<DynamicRow> rawRows,
            SensitiveDisplayMode displayMode) {
        ProtectedContainsResultSupport.requireCandidateLimit(rawRows.size());
        return operations.results.decodeRows(
                        plan.form(), Flux.fromIterable(rawRows), plan.options(),
                        plan.scope(), SensitiveDisplayMode.FULL, plan.decodingFields())
                .collectList()
                .map(rows -> operations.containsResults.finish(
                        plan.form(), plan.containsQuery(), rows, plan.outputFields(), displayMode));
    }

    private static PageResult<DynamicRow> containsPage(List<DynamicRow> rows, PageQuery page) {
        int from = (int) Math.min(page.offset(), rows.size());
        int to = Math.min(from + page.size(), rows.size());
        return PageResult.of(rows.subList(from, to), rows.size(), page);
    }
}
