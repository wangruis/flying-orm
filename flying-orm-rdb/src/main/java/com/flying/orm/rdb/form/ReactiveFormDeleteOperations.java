package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 负责逻辑删除和显式物理删除。
 *
 * <p>具体行为从原客户端原样迁移，SQL、参数、Scope、执行保护和响应式订阅语义不变。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class ReactiveFormDeleteOperations extends ReactiveFormOperationSupport {

    ReactiveFormDeleteOperations(ReactiveFormOperationSupport runtime) {
        super(runtime);
    }
    Mono<Long> delete(DynamicForm form, ConditionGroup where) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        ConditionGroup scopedWhere = scopes.scopedWhere(safeForm, where);
        ConditionGroup activeWhere = FormLogicDeletes.activeWhere(safeForm, scopedWhere);
        return FormLogicDeletes.deleteValues(safeForm)
                               .map(values -> executor.rowsUpdated(renderer.update(safeForm, values, activeWhere)))
                               .orElseGet(() -> executor.rowsUpdated(renderer.delete(safeForm, scopedWhere)));
    }

    /**
     * 删除动态表单数据，并显式传入执行保护。
     *
     * @param form    动态表单
     * @param where   删除条件
     * @param options 执行保护选项
     * @return 影响行数
     */
    Mono<Long> delete(DynamicForm form, ConditionGroup where, SqlExecutionOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        ConditionGroup scopedWhere = scopes.scopedWhere(safeForm, where);
        ConditionGroup activeWhere = FormLogicDeletes.activeWhere(safeForm, scopedWhere);
        return FormLogicDeletes.deleteValues(safeForm)
                               .map(values -> executor.rowsUpdated(renderer.update(safeForm, values, activeWhere),
                                                                    options))
                               .orElseGet(() -> executor.rowsUpdated(renderer.delete(safeForm, scopedWhere), options));
    }

    Mono<Long> delete(DynamicForm form, ConditionGroup where, DataScope scope) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        ConditionGroup scopedWhere = scopes.scopedWhere(safeForm, where, scope);
        ConditionGroup activeWhere = FormLogicDeletes.activeWhere(safeForm, scopedWhere);
        return FormLogicDeletes.deleteValues(safeForm)
                               .map(values -> executor.rowsUpdated(renderer.update(safeForm, values, activeWhere)))
                               .orElseGet(() -> executor.rowsUpdated(renderer.delete(safeForm, scopedWhere)));
    }

    Mono<Long> delete(DynamicForm form,
                             ConditionGroup where,
                             DataScope scope,
                             SqlExecutionOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        ConditionGroup scopedWhere = scopes.scopedWhere(safeForm, where, scope);
        ConditionGroup activeWhere = FormLogicDeletes.activeWhere(safeForm, scopedWhere);
        return FormLogicDeletes.deleteValues(safeForm)
                               .map(values -> executor.rowsUpdated(renderer.update(safeForm, values, activeWhere),
                                                                    options))
                               .orElseGet(() -> executor.rowsUpdated(renderer.delete(safeForm, scopedWhere), options));
    }

    Mono<Long> delete(DynamicForm form, ConditionGroup where, OptimisticLockOptions lock) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        OptimisticLockOptions safeLock = Objects.requireNonNull(lock, "optimistic lock options must not be null");
        ConditionGroup scopedWhere = scopes.scopedWhere(safeForm, where);
        ConditionGroup activeWhere = FormLogicDeletes.activeWhere(safeForm, scopedWhere);
        Mono<Long> deleted = FormLogicDeletes.deleteValues(safeForm)
                                             .map(values -> executor.rowsUpdated(renderer.update(safeForm,
                                                                                                 values,
                                                                                                 activeWhere,
                                                                                                 safeLock)))
                                             .orElseGet(() -> executor.rowsUpdated(renderer.delete(safeForm,
                                                                                                   scopedWhere,
                                                                                                   safeLock)));
        return deleted.map(rows -> results.requireOptimisticSuccess(safeForm, safeLock, rows));
    }

    Mono<Long> delete(DynamicForm form,
                             ConditionGroup where,
                             OptimisticLockOptions lock,
                             SqlExecutionOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        OptimisticLockOptions safeLock = Objects.requireNonNull(lock, "optimistic lock options must not be null");
        ConditionGroup scopedWhere = scopes.scopedWhere(safeForm, where);
        ConditionGroup activeWhere = FormLogicDeletes.activeWhere(safeForm, scopedWhere);
        Mono<Long> deleted = FormLogicDeletes.deleteValues(safeForm)
                                             .map(values -> executor.rowsUpdated(renderer.update(safeForm,
                                                                                                 values,
                                                                                                 activeWhere,
                                                                                                 safeLock),
                                                                                 options))
                                             .orElseGet(() -> executor.rowsUpdated(renderer.delete(safeForm,
                                                                                                   scopedWhere,
                                                                                                   safeLock),
                                                                                   options));
        return deleted
                       .map(rows -> results.requireOptimisticSuccess(safeForm, safeLock, rows));
    }

    Mono<Long> delete(DynamicForm form, ConditionGroup where, DataScope scope, OptimisticLockOptions lock) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        OptimisticLockOptions safeLock = Objects.requireNonNull(lock, "optimistic lock options must not be null");
        ConditionGroup scopedWhere = scopes.scopedWhere(safeForm, where, scope);
        ConditionGroup activeWhere = FormLogicDeletes.activeWhere(safeForm, scopedWhere);
        Mono<Long> deleted = FormLogicDeletes.deleteValues(safeForm)
                                             .map(values -> executor.rowsUpdated(renderer.update(safeForm,
                                                                                                 values,
                                                                                                 activeWhere,
                                                                                                 safeLock)))
                                             .orElseGet(() -> executor.rowsUpdated(renderer.delete(safeForm,
                                                                                                   scopedWhere,
                                                                                                   safeLock)));
        return deleted.map(rows -> results.requireOptimisticSuccess(safeForm, safeLock, rows));
    }

    Mono<Long> delete(DynamicForm form,
                             ConditionGroup where,
                             DataScope scope,
                             OptimisticLockOptions lock,
                             SqlExecutionOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        OptimisticLockOptions safeLock = Objects.requireNonNull(lock, "optimistic lock options must not be null");
        ConditionGroup scopedWhere = scopes.scopedWhere(safeForm, where, scope);
        ConditionGroup activeWhere = FormLogicDeletes.activeWhere(safeForm, scopedWhere);
        Mono<Long> deleted = FormLogicDeletes.deleteValues(safeForm)
                                             .map(values -> executor.rowsUpdated(renderer.update(safeForm,
                                                                                                 values,
                                                                                                 activeWhere,
                                                                                                 safeLock),
                                                                                 options))
                                             .orElseGet(() -> executor.rowsUpdated(renderer.delete(safeForm,
                                                                                                   scopedWhere,
                                                                                                   safeLock),
                                                                                   options));
        return deleted.map(rows -> results.requireOptimisticSuccess(safeForm, safeLock, rows));
    }

    Mono<Long> physicalDelete(DynamicForm form, ConditionGroup where) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return executor.rowsUpdated(renderer.delete(safeForm, scopes.scopedWhere(safeForm, where)));
    }

    Mono<Long> physicalDelete(DynamicForm form, ConditionGroup where, SqlExecutionOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return executor.rowsUpdated(renderer.delete(safeForm, scopes.scopedWhere(safeForm, where)), options);
    }

    Mono<Long> physicalDelete(DynamicForm form, ConditionGroup where, DataScope scope) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return executor.rowsUpdated(renderer.delete(safeForm, scopes.scopedWhere(safeForm, where, scope)));
    }

    Mono<Long> physicalDelete(DynamicForm form,
                                     ConditionGroup where,
                                     DataScope scope,
                                     SqlExecutionOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return executor.rowsUpdated(renderer.delete(safeForm, scopes.scopedWhere(safeForm, where, scope)), options);
    }

    Mono<Long> physicalDelete(DynamicForm form, ConditionGroup where, OptimisticLockOptions lock) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        OptimisticLockOptions safeLock = Objects.requireNonNull(lock, "optimistic lock options must not be null");
        return results.optimisticRowsUpdated(safeForm,
                                     safeLock,
                                     renderer.delete(safeForm, scopes.scopedWhere(safeForm, where), safeLock));
    }

    Mono<Long> physicalDelete(DynamicForm form,
                                     ConditionGroup where,
                                     DataScope scope,
                                     OptimisticLockOptions lock) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        OptimisticLockOptions safeLock = Objects.requireNonNull(lock, "optimistic lock options must not be null");
        return results.optimisticRowsUpdated(safeForm,
                                     safeLock,
                                     renderer.delete(safeForm, scopes.scopedWhere(safeForm, where, scope), safeLock));
    }

    Mono<Long> physicalDelete(DynamicForm form,
                                     ConditionGroup where,
                                     OptimisticLockOptions lock,
                                     SqlExecutionOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        OptimisticLockOptions safeLock = Objects.requireNonNull(lock, "optimistic lock options must not be null");
        return executor.rowsUpdated(renderer.delete(safeForm, scopes.scopedWhere(safeForm, where), safeLock), options)
                       .map(rows -> results.requireOptimisticSuccess(safeForm, safeLock, rows));
    }

    Mono<Long> physicalDelete(DynamicForm form,
                                     ConditionGroup where,
                                     DataScope scope,
                                     OptimisticLockOptions lock,
                                     SqlExecutionOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        OptimisticLockOptions safeLock = Objects.requireNonNull(lock, "optimistic lock options must not be null");
        return executor.rowsUpdated(renderer.delete(safeForm, scopes.scopedWhere(safeForm, where, scope), safeLock), options)
                       .map(rows -> results.requireOptimisticSuccess(safeForm, safeLock, rows));
    }

}
