package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 负责普通 ConditionGroup 的页码分页。
 *
 * <p>具体行为从原客户端原样迁移，SQL、参数、Scope、执行保护和响应式订阅语义不变。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class ReactiveFormPlainPageOperations extends ReactiveFormOperationSupport {

    ReactiveFormPlainPageOperations(ReactiveFormOperationSupport runtime) {
        super(runtime);
    }

    Mono<PageResult<DynamicRow>> page(DynamicForm form, ConditionGroup where, PageQuery page) {
        PageQuery safePage = Objects.requireNonNull(page, "page query must not be null");
        ScopedRead read = scopes.scopedRead(form, where, DataScope.none());
        return results.pageWithActiveWhere(read.form(), read.where(), safePage);
    }

    Mono<PageResult<DynamicRow>> page(DynamicForm form,
                                                      ConditionGroup where,
                                                      PageQuery page,
                                                      SqlExecutionOptions options) {
        PageQuery safePage = Objects.requireNonNull(page, "page query must not be null");
        ScopedRead read = scopes.scopedRead(form, where, DataScope.none());
        return results.pageWithActiveWhere(read.form(), read.where(), safePage, options);
    }

    Mono<PageResult<DynamicRow>> page(DynamicForm form,
                                                      ConditionGroup where,
                                                      PageQuery page,
                                                      DataScope scope) {
        PageQuery safePage = Objects.requireNonNull(page, "page query must not be null");
        ScopedRead read = scopes.scopedRead(form, where, scope);
        return results.pageWithActiveWhere(read.form(), read.where(), safePage);
    }

    Mono<PageResult<DynamicRow>> page(DynamicForm form,
                                                      ConditionGroup where,
                                                      PageQuery page,
                                                      DataScope scope,
                                                      SqlExecutionOptions options) {
        PageQuery safePage = Objects.requireNonNull(page, "page query must not be null");
        ScopedRead read = scopes.scopedRead(form, where, scope);
        return results.pageWithActiveWhere(read.form(), read.where(), safePage, options);
    }

    /**
     * 分页查询动态表单数据并映射成业务对象。
     *
     * @param form  动态表单
     * @param where 查询条件
     * @param page  分页请求
     * @param type  目标对象类型
     * @param <T>   目标对象类型
     * @return 分页结果
     */
    <T> Mono<PageResult<T>> page(DynamicForm form, ConditionGroup where, PageQuery page, Class<T> type) {
        RowMapper<T> mapper = results.rowMapper(type, "form page result type must not be null");
        return page(form, where, page)
                .map(result -> new PageResult<>(result.rows().stream().map(mapper::map).toList(),
                                                result.total(),
                                                result.page(),
                                                result.size()));
    }

    <T> Mono<PageResult<T>> page(DynamicForm form,
                                        ConditionGroup where,
                                        PageQuery page,
                                        Class<T> type,
                                        SqlExecutionOptions options) {
        RowMapper<T> mapper = results.rowMapper(type, "form page result type must not be null");
        return page(form, where, page, options)
                .map(result -> new PageResult<>(result.rows().stream().map(mapper::map).toList(),
                                                result.total(),
                                                result.page(),
                                                result.size()));
    }

    /**
     * 带完整数据范围分页并映射实体。总数查询和数据查询会复用同一个 scoped where。
     */
    <T> Mono<PageResult<T>> page(DynamicForm form,
                                        ConditionGroup where,
                                        PageQuery page,
                                        DataScope scope,
                                        Class<T> type) {
        RowMapper<T> mapper = results.rowMapper(type, "form page result type must not be null");
        return page(form, where, page, scope)
                .map(result -> new PageResult<>(result.rows().stream().map(mapper::map).toList(),
                                                result.total(),
                                                result.page(),
                                                result.size()));
    }

    <T> Mono<PageResult<T>> page(DynamicForm form,
                                        ConditionGroup where,
                                        PageQuery page,
                                        DataScope scope,
                                        Class<T> type,
                                        SqlExecutionOptions options) {
        RowMapper<T> mapper = results.rowMapper(type, "form page result type must not be null");
        return page(form, where, page, scope, options)
                .map(result -> new PageResult<>(result.rows().stream().map(mapper::map).toList(),
                                                result.total(),
                                                result.page(),
                                                result.size()));
    }

    /**
     * 使用前端结构化条件分页查询动态表单数据。
     *
     * @param form   动态表单
     * @param input  前端结构化条件
     * @param policy 安全策略
     * @param page   分页请求
     * @return 分页结果
     */
}
