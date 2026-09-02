package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

/**
 * 响应式 JOIN 查询执行协作者。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
final class ReactiveJoinQueryOperations extends ReactiveFormOperationSupport {

    private final JoinQueryPlanner joinPlanner;

    ReactiveJoinQueryOperations(ReactiveFormOperationSupport runtime) {
        super(runtime);
        this.joinPlanner = new JoinQueryPlanner(renderer, scopes, defaultExecutionOptions);
    }

    Flux<DynamicRow> select(JoinQuerySpec spec, SqlExecutionOptions options) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> joinPlanner.plan(safeSpec, options))
                                               .flatMapMany(this::select)
                : select(joinPlanner.plan(safeSpec, options));
    }

    private Flux<DynamicRow> select(JoinQueryPlanner.PlannedJoin plan) {
        Flux<DynamicRow> rows = results.decodeRows(
                plan.resultForm(), executor.query(plan.request(), plan.options()), plan.options());
        return plan.resultPlan().direct() ? rows
                : ReactiveProtectionCpuBoundary.sequence(
                        rows, plan.resultPlan().requiresCpuBoundary(),
                        ReactiveProtectionCpuBoundary.QUERY_PREFETCH)
                .map(plan.resultPlan()::transform);
    }

    Mono<PageResult<DynamicRow>> page(JoinQuerySpec spec,
                                      PageQuery page,
                                      SqlExecutionOptions options) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        PageQuery safePage = Objects.requireNonNull(page, "join page query must not be null");
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> joinPlanner.page(safeSpec, safePage, options))
                                               .flatMap(this::page)
                : page(joinPlanner.page(safeSpec, safePage, options));
    }

    private Mono<PageResult<DynamicRow>> page(JoinQueryPlanner.PlannedJoinPage plan) {
        Mono<Long> total = executor.query(plan.countRequest(), plan.options())
                                   .next()
                                   .map(CountResultReader::read)
                                   .defaultIfEmpty(0L);
        return total.flatMap(count -> count == 0L
                ? Mono.just(PageResult.of(List.of(), 0L, plan.page()))
                : pageRows(plan)
                        .collectList()
                        .map(rows -> PageResult.of(rows, count, plan.page())));
    }

    private Flux<DynamicRow> pageRows(JoinQueryPlanner.PlannedJoinPage plan) {
        Flux<DynamicRow> rows = results.decodeRows(
                plan.resultForm(), executor.query(plan.dataRequest(), plan.options()), plan.options());
        return plan.resultPlan().direct() ? rows
                : ReactiveProtectionCpuBoundary.sequence(
                        rows, plan.resultPlan().requiresCpuBoundary(),
                        ReactiveProtectionCpuBoundary.QUERY_PREFETCH)
                .map(plan.resultPlan()::transform);
    }

    private boolean requiresProtectedPlanning(JoinQuerySpec spec) {
        for (JoinSource source : spec.sources()) {
            DynamicForm form = source.form();
            if (form.protections().encryptedFields().isEmpty()) {
                continue;
            }
            com.flying.orm.core.scope.DataScope effectiveScope = scopes.effectiveScope(spec.scope(source));
            if (ReactiveProtectionCpuBoundary.usesEncryptedCondition(form, spec.where(source))
                    || ReactiveProtectionCpuBoundary.usesEncryptedScope(form, effectiveScope)) {
                return true;
            }
        }
        return false;
    }

}
