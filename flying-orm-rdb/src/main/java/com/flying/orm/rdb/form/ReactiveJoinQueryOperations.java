package com.flying.orm.rdb.form;

import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * 响应式 JOIN 查询执行协作者。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
final class ReactiveJoinQueryOperations extends ReactiveFormOperationSupport {

    private final JoinQueryPlanner joinPlanner;
    private final JoinResultProtector joinResults;

    ReactiveJoinQueryOperations(ReactiveFormOperationSupport runtime) {
        super(runtime);
        this.joinPlanner = new JoinQueryPlanner(renderer, scopes, defaultExecutionOptions);
        this.joinResults = new JoinResultProtector(renderer);
    }

    Flux<DynamicRow> select(JoinQuerySpec spec, SqlExecutionOptions options) {
        JoinQueryPlanner.PlannedJoin plan = joinPlanner.plan(spec, options);
        return results.decodeRows(plan.resultForm(),
                                  executor.query(plan.request(), plan.options()),
                                  plan.options())
                      .map(row -> joinResults.transform(
                              spec, row, plan.scopes(), spec.sensitiveDisplayMode()));
    }

    Mono<PageResult<DynamicRow>> page(JoinQuerySpec spec,
                                      PageQuery page,
                                      SqlExecutionOptions options) {
        JoinQueryPlanner.PlannedJoinPage plan = joinPlanner.page(spec, page, options);
        Mono<Long> total = executor.query(plan.countRequest(), plan.options())
                                   .next()
                                   .map(CountResultReader::read)
                                   .defaultIfEmpty(0L);
        return total.flatMap(count -> count == 0L
                ? Mono.just(PageResult.of(List.of(), 0L, plan.page()))
                : results.decodeRows(plan.resultForm(),
                                     executor.query(plan.dataRequest(), plan.options()),
                                     plan.options())
                         .map(row -> joinResults.transform(
                                 spec, row, plan.scopes(), spec.sensitiveDisplayMode()))
                         .collectList()
                         .map(rows -> PageResult.of(rows, count, plan.page())));
    }
}
