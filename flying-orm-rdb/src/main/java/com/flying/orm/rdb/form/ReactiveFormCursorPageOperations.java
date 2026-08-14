package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorPageResult;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 负责稳定游标分页。
 *
 * <p>具体行为从原客户端原样迁移，SQL、参数、Scope、执行保护和响应式订阅语义不变。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class ReactiveFormCursorPageOperations extends ReactiveFormOperationSupport {

    ReactiveFormCursorPageOperations(ReactiveFormOperationSupport runtime) {
        super(runtime);
    }
    /**
     * 使用稳定复合游标查询下一页。该入口只执行一条数据 SQL，不执行 count，也不会随着页数增大扫描 offset。
     */
    Mono<CursorPageResult<DynamicRow>> cursorPage(DynamicForm form,
                                                                  ConditionGroup where,
                                                                  CursorPageQuery page) {
        return cursorPage(form, where, page, DataScope.none(), defaultExecutionOptions);
    }

    Mono<CursorPageResult<DynamicRow>> cursorPage(DynamicForm form,
                                                                  ConditionGroup where,
                                                                  CursorPageQuery page,
                                                                  SqlExecutionOptions options) {
        return cursorPage(form, where, page, DataScope.none(), options);
    }

    Mono<CursorPageResult<DynamicRow>> cursorPage(DynamicForm form,
                                                                  ConditionGroup where,
                                                                  CursorPageQuery page,
                                                                  DataScope scope,
                                                                  SqlExecutionOptions options) {
        CursorPageQuery safePage = Objects.requireNonNull(page, "cursor page query must not be null");
        ScopedRead read = scopes.scopedRead(form, where, scope);
        return cursorPageWithActiveWhere(read.form(), read.where(), safePage, options);
    }

    /**
     * 接收已经合并好结构化条件、逻辑删除和数据范围的 WHERE，避免 QuerySpec 分页再次走普通条件入口。
     */
    Mono<CursorPageResult<DynamicRow>> cursorPageWithActiveWhere(DynamicForm form,
                                                                 ConditionGroup where,
                                                                 CursorPageQuery page,
                                                                 SqlExecutionOptions options) {
        CursorPageQuery safePage = Objects.requireNonNull(page, "cursor page query must not be null");
        CursorPageQuery normalizedPage = CursorPageNormalizer.normalize(form, safePage);
        return results.decodeRows(form, executor.query(renderer.select(form, where, normalizedPage), options), options)
                       .collectList()
                       .map(rows -> FormCursorResults.from(rows, normalizedPage));
    }

    /** 使用相同游标和 Scope 规则映射实体，不重新执行 SQL。 */
    <T> Mono<CursorPageResult<T>> cursorPage(DynamicForm form,
                                                    ConditionGroup where,
                                                    CursorPageQuery page,
                                                    Class<T> type,
                                                    SqlExecutionOptions options) {
        RowMapper<T> mapper = results.rowMapper(type, "cursor page result type must not be null");
        return cursorPage(form, where, page, options)
                .map(result -> new CursorPageResult<>(result.rows().stream().map(mapper::map).toList(),
                                                      result.nextCursor(), result.hasMore()));
    }

    /**
     * 分页查询动态表单数据，并显式传入执行保护。count 和当前页查询都会使用同一组选项。
     *
     * @param form    动态表单
     * @param where   查询条件
     * @param page    分页请求
     * @param options 执行保护选项
     * @return 分页结果
     */
}
