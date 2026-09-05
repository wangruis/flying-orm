package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.lock.OptimisticLockConflictException;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

/**
 * 负责动态行解码、实体映射和普通分页结果的收尾工作。
 *
 * <p>分页只收集单页记录，不会收集整个查询流；特殊字段解码仍复用 {@link FormResultDecoder} 的
 * 串行、有背压实现。乐观锁影响行数校验也集中在这里，避免更新和删除入口出现细微差异。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class ReactiveFormResultSupport {

    private final ReactiveSqlExecutor executor;
    private final FormDataSqlRenderer renderer;
    private final FormResultDecoder decoder;
    private final SqlExecutionOptions defaultExecutionOptions;

    ReactiveFormResultSupport(ReactiveSqlExecutor executor,
                              FormDataSqlRenderer renderer,
                              EntityModelRegistry entityModels,
                              SqlExecutionOptions defaultExecutionOptions) {
        this.executor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        this.renderer = Objects.requireNonNull(renderer, "form data sql renderer must not be null");
        this.decoder = new FormResultDecoder(renderer, Objects.requireNonNull(entityModels,
                                                                              "entity model registry must not be null"));
        this.defaultExecutionOptions = Objects.requireNonNull(defaultExecutionOptions,
                                                              "default sql execution options must not be null");
    }

    Mono<PageResult<DynamicRow>> pageWithActiveWhere(DynamicForm form,
                                                      ConditionGroup where,
                                                      PageQuery page) {
        return pageWithActiveWhere(form, where, page, defaultExecutionOptions);
    }

    Mono<PageResult<DynamicRow>> pageWithActiveWhere(DynamicForm form,
                                                      ConditionGroup where,
                                                      PageQuery page,
                                                      SqlExecutionOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        ConditionGroup safeWhere = Objects.requireNonNull(where, "where condition must not be null");
        PageQuery safePage = Objects.requireNonNull(page, "page query must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        Mono<Long> total = executor.query(renderer.count(safeForm, safeWhere), safeOptions)
                                   .next()
                                   .map(CountResultReader::read)
                                   .defaultIfEmpty(0L);
        return total.flatMap(count -> count == 0L
                ? Mono.just(PageResult.of(List.of(), 0L, safePage))
                : decodeRows(safeForm, executor.query(renderer.select(safeForm, safeWhere, safePage), safeOptions),
                             safeOptions)
                        .collectList()
                        .map(rows -> PageResult.of(rows, count, safePage)));
    }

    <T> RowMapper<T> rowMapper(Class<T> type, String nullMessage) {
        return decoder.rowMapper(type, nullMessage);
    }

    Flux<DynamicRow> decodeRows(DynamicForm form, Flux<DynamicRow> rows) {
        return decodeRows(form, rows, defaultExecutionOptions);
    }

    Flux<DynamicRow> decodeRows(DynamicForm form, Flux<DynamicRow> rows, SqlExecutionOptions options) {
        return decoder.decodeRows(form, rows, options);
    }

    Flux<DynamicRow> decodeRows(DynamicForm form,
                                Flux<DynamicRow> rows,
                                SqlExecutionOptions options,
                                DataScope scope,
                                SensitiveDisplayMode displayMode) {
        return decoder.decodeRows(form, rows, options, scope, displayMode);
    }

    Flux<DynamicRow> decodeRows(DynamicForm form,
                                Flux<DynamicRow> rows,
                                SqlExecutionOptions options,
                                DataScope scope,
                                SensitiveDisplayMode displayMode,
                                List<String> projectedFields) {
        return decoder.decodeRows(form, rows, options, scope, displayMode, projectedFields);
    }

    Flux<DynamicRow> decodeRows(DynamicForm form,
                                Flux<DynamicRow> rows,
                                SqlExecutionOptions options,
                                DataScope scope,
                                SensitiveDisplayMode displayMode,
                                FormFieldDecodingPlan decodingPlan) {
        return decoder.decodeRows(form, rows, options, scope, displayMode, decodingPlan);
    }

    Mono<Long> optimisticRowsUpdated(DynamicForm form, OptimisticLockOptions lock, SqlRequest request) {
        return executor.rowsUpdated(request).map(rows -> requireOptimisticSuccess(form, lock, rows));
    }

    long requireOptimisticSuccess(DynamicForm form, OptimisticLockOptions lock, long rows) {
        if (rows == 0) {
            throw new OptimisticLockConflictException(form.table(), lock.field(), lock.expectedValue());
        }
        return rows;
    }
}
