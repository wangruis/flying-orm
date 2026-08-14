package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;

import java.util.List;

/**
 * 负责普通条件、有序查询和安全投影。
 *
 * <p>具体行为从原客户端原样迁移，SQL、参数、Scope、执行保护和响应式订阅语义不变。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class ReactiveFormQueryOperations extends ReactiveFormOperationSupport {

    ReactiveFormQueryOperations(ReactiveFormOperationSupport runtime) {
        super(runtime);
    }
    Flux<DynamicRow> select(DynamicForm form, ConditionGroup where) {
        ScopedRead read = scopes.scopedRead(form, where, DataScope.none());
        return results.decodeRows(read.form(), executor.query(renderer.select(read.form(), read.where())));
    }

    /**
     * 查询动态表单数据，并显式传入执行保护。
     *
     * @param form    动态表单
     * @param where   查询条件
     * @param options 执行保护选项
     * @return 行数据流
     */
    Flux<DynamicRow> select(DynamicForm form,
                                            ConditionGroup where,
                                            SqlExecutionOptions options) {
        ScopedRead read = scopes.scopedRead(form, where, DataScope.none());
        return results.decodeRows(read.form(), executor.query(renderer.select(read.form(), read.where()), options), options);
    }

    Flux<DynamicRow> select(DynamicForm form, ConditionGroup where, DataScope scope) {
        ScopedRead read = scopes.scopedRead(form, where, scope);
        return results.decodeRows(read.form(), executor.query(renderer.select(read.form(), read.where())));
    }

    Flux<DynamicRow> select(DynamicForm form,
                                            ConditionGroup where,
                                            DataScope scope,
                                            SqlExecutionOptions options) {
        ScopedRead read = scopes.scopedRead(form, where, scope);
        return results.decodeRows(read.form(), executor.query(renderer.select(read.form(), read.where()), options), options);
    }

    /**
     * 查询动态表单数据并映射成业务对象。
     *
     * @param form  动态表单
     * @param where 查询条件
     * @param type  目标对象类型
     * @param <T>   目标对象类型
     * @return 业务对象流
     */
    <T> Flux<T> select(DynamicForm form, ConditionGroup where, Class<T> type) {
        RowMapper<T> mapper = results.rowMapper(type, "form result type must not be null");
        return select(form, where).map(mapper::map);
    }

    <T> Flux<T> select(DynamicForm form,
                              ConditionGroup where,
                              Class<T> type,
                              SqlExecutionOptions options) {
        RowMapper<T> mapper = results.rowMapper(type, "form result type must not be null");
        return select(form, where, options).map(mapper::map);
    }

    /**
     * 带完整数据范围查询并映射实体。字段裁剪必须发生在映射前，否则 Repository 会把 FieldScope 丢掉。
     */
    <T> Flux<T> select(DynamicForm form,
                              ConditionGroup where,
                              DataScope scope,
                              Class<T> type) {
        RowMapper<T> mapper = results.rowMapper(type, "form result type must not be null");
        return select(form, where, scope).map(mapper::map);
    }

    <T> Flux<T> select(DynamicForm form,
                              ConditionGroup where,
                              DataScope scope,
                              Class<T> type,
                              SqlExecutionOptions options) {
        RowMapper<T> mapper = results.rowMapper(type, "form result type must not be null");
        return select(form, where, scope, options).map(mapper::map);
    }

    /**
     * 带完整数据范围执行有序实体查询。排序字段由表单元数据验证，且不改变默认结果行数、超时和 LOB 上限。
     *
     * @param form 动态表单
     * @param where 查询条件
     * @param sorts 排序项
     * @param scope 本次附加数据范围
     * @param type 目标实体类型
     * @param <T> 实体类型
     * @return 保持背压与取消语义的实体流
     */
    <T> Flux<T> selectOrdered(DynamicForm form,
                                     ConditionGroup where,
                                     List<PageSort> sorts,
                                     DataScope scope,
                                     Class<T> type) {
        RowMapper<T> mapper = results.rowMapper(type, "form result type must not be null");
        ScopedRead read = scopes.scopedRead(form, where, scope);
        return results.decodeRows(read.form(), executor.query(renderer.selectOrdered(read.form(), read.where(), sorts)))
                .map(mapper::map);
    }

    /** 使用本次显式资源保护执行有序实体查询。 */
    <T> Flux<T> selectOrdered(DynamicForm form,
                                     ConditionGroup where,
                                     List<PageSort> sorts,
                                     DataScope scope,
                                     Class<T> type,
                                     SqlExecutionOptions options) {
        RowMapper<T> mapper = results.rowMapper(type, "form result type must not be null");
        ScopedRead read = scopes.scopedRead(form, where, scope);
        return results.decodeRows(read.form(),
                          executor.query(renderer.selectOrdered(read.form(), read.where(), sorts), options),
                          options).map(mapper::map);
    }

    /**
     * 执行安全字段投影/分组查询并返回紧凑 {@link DynamicRow}。字段范围先于 SQL 渲染应用，
     * 因此不可读字段会在获取连接前失败。
     */
    Flux<DynamicRow> selectProjected(DynamicForm form,
                                            ConditionGroup where,
                                            List<String> projections,
                                            List<String> groups,
                                            List<PageSort> sorts,
                                            DataScope scope) {
        ScopedRead read = scopes.scopedRead(form, where, scope);
        return results.decodeRows(read.form(), executor.query(renderer.selectProjected(read.form(), read.where(),
                                                                                projections, groups, sorts)));
    }

    /** 使用本次显式资源保护执行安全字段投影/分组查询。 */
    Flux<DynamicRow> selectProjected(DynamicForm form,
                                            ConditionGroup where,
                                            List<String> projections,
                                            List<String> groups,
                                            List<PageSort> sorts,
                                            DataScope scope,
                                            SqlExecutionOptions options) {
        ScopedRead read = scopes.scopedRead(form, where, scope);
        return results.decodeRows(read.form(), executor.query(renderer.selectProjected(read.form(), read.where(),
                                                                                projections, groups, sorts),
                                                      options), options);
    }

    /**
     * 查询动态表单数据，直接接收前端结构化条件。
     *
     * @param form  动态表单
     * @param input 前端结构化条件
     * @return 行数据流
     */
}
