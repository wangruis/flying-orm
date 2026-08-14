package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;

/**
 * 负责单行更新和乐观锁更新。
 *
 * <p>具体行为从原客户端原样迁移，SQL、参数、Scope、执行保护和响应式订阅语义不变。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class ReactiveFormWriteOperations extends ReactiveFormOperationSupport {

    ReactiveFormWriteOperations(ReactiveFormOperationSupport runtime) {
        super(runtime);
    }
    Mono<Long> update(DynamicForm form, Map<String, Object> values, ConditionGroup where) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return executor.rowsUpdated(renderer.update(safeForm,
                                                    values,
                                                    scopes.writableActiveWhere(safeForm,
                                                                        values,
                                                                        where,
                                                                        DataScope.none())));
    }

    /**
     * 更新动态表单数据，并显式传入执行保护。
     *
     * @param form    动态表单
     * @param values  字段值
     * @param where   更新条件
     * @param options 执行保护选项
     * @return 影响行数
     */
    Mono<Long> update(DynamicForm form,
                             Map<String, Object> values,
                             ConditionGroup where,
                             SqlExecutionOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return executor.rowsUpdated(renderer.update(safeForm,
                                                    values,
                                                    scopes.writableActiveWhere(safeForm,
                                                                        values,
                                                                        where,
                                                                        DataScope.none())),
                                    options);
    }

    Mono<Long> update(DynamicForm form, Map<String, Object> values, ConditionGroup where, DataScope scope) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return executor.rowsUpdated(renderer.update(safeForm,
                                                    values,
                                                    scopes.writableActiveWhere(safeForm, values, where, scope)));
    }

    Mono<Long> update(DynamicForm form,
                             Map<String, Object> values,
                             ConditionGroup where,
                             DataScope scope,
                             SqlExecutionOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return executor.rowsUpdated(renderer.update(safeForm,
                                                    values,
                                                    scopes.writableActiveWhere(safeForm, values, where, scope)),
                                    options);
    }

    Mono<Long> update(DynamicForm form,
                             Map<String, Object> values,
                             ConditionGroup where,
                             OptimisticLockOptions lock) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        OptimisticLockOptions safeLock = Objects.requireNonNull(lock, "optimistic lock options must not be null");
        SqlRequest request = renderer.update(safeForm,
                                             values,
                                             scopes.writableActiveWhere(safeForm,
                                                                 values,
                                                                 where,
                                                                 DataScope.none()),
                                             safeLock);
        return results.optimisticRowsUpdated(safeForm, safeLock, request);
    }

    Mono<Long> update(DynamicForm form,
                             Map<String, Object> values,
                             ConditionGroup where,
                             OptimisticLockOptions lock,
                             SqlExecutionOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        OptimisticLockOptions safeLock = Objects.requireNonNull(lock, "optimistic lock options must not be null");
        return executor.rowsUpdated(renderer.update(safeForm,
                                                    values,
                                                    scopes.writableActiveWhere(safeForm,
                                                                        values,
                                                                        where,
                                                                        DataScope.none()),
                                                    safeLock),
                                    options)
                       .map(rows -> results.requireOptimisticSuccess(safeForm, safeLock, rows));
    }

    Mono<Long> update(DynamicForm form,
                             Map<String, Object> values,
                             ConditionGroup where,
                             DataScope scope,
                             OptimisticLockOptions lock) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        OptimisticLockOptions safeLock = Objects.requireNonNull(lock, "optimistic lock options must not be null");
        SqlRequest request = renderer.update(safeForm,
                                             values,
                                             scopes.writableActiveWhere(safeForm, values, where, scope),
                                             safeLock);
        return results.optimisticRowsUpdated(safeForm, safeLock, request);
    }

    Mono<Long> update(DynamicForm form,
                             Map<String, Object> values,
                             ConditionGroup where,
                             DataScope scope,
                             OptimisticLockOptions lock,
                             SqlExecutionOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        OptimisticLockOptions safeLock = Objects.requireNonNull(lock, "optimistic lock options must not be null");
        return executor.rowsUpdated(renderer.update(safeForm,
                                                    values,
                                                    scopes.writableActiveWhere(safeForm, values, where, scope),
                                                    safeLock),
                                    options)
                       .map(rows -> results.requireOptimisticSuccess(safeForm, safeLock, rows));
    }

    /**
     * 删除动态表单数据。
     *
     * @param form  动态表单
     * @param where 删除条件
     * @return 影响行数
     */
}
