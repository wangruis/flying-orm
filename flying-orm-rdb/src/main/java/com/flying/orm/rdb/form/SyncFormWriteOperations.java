package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

/**
 * 同步表单写执行边界，把写计划与 JDBC 执行保持在一个小而直接的调用链内。
 *
 * <p>本类只承接 {@link SyncFormOperations} 的写职责，不创建事务，也不改变规划结果。
 * 生成键、影响行数和受保护写入仍由同一个执行器调用返回。</p>
 */
final class SyncFormWriteOperations {

    private final SyncSqlExecutor executor;
    private final FormOperationPlanner planner;
    private final FieldUsePolicy fieldUsePolicy;
    private final QueryShapeLimits queryShapeLimits;
    private final boolean governed;

    SyncFormWriteOperations(SyncSqlExecutor executor,
                            FormOperationPlanner planner,
                            FieldUsePolicy fieldUsePolicy,
                            QueryShapeLimits queryShapeLimits,
                            boolean governed) {
        this.executor = executor;
        this.planner = planner;
        this.fieldUsePolicy = fieldUsePolicy;
        this.queryShapeLimits = queryShapeLimits;
        this.governed = governed;
    }

    long insert(WriteSpec spec) {
        return execute(governed
                ? planner.insertGoverned(spec, fieldUsePolicy, queryShapeLimits).plan()
                : planner.insert(spec));
    }

    SqlWriteResult insertReturningKeys(WriteSpec spec) {
        FormOperationPlanner.PlannedWrite plan = governed
                ? planner.insertGoverned(spec, fieldUsePolicy, queryShapeLimits).plan()
                : planner.insert(spec);
        SqlWriteResult result = plan.protectedWriteRequired()
                ? executor.atomicProtectedWrite(plan.protectedWrite(), plan.options())
                : rowsUpdatedReturningKeys(plan);
        plan.requireSuccess(result.affectedRows());
        return result;
    }

    long update(WriteSpec spec) {
        return execute(governed
                ? planner.updateGoverned(spec, fieldUsePolicy, queryShapeLimits).plan()
                : planner.update(spec));
    }

    long delete(WriteSpec spec) {
        return execute(governed
                ? planner.deleteGoverned(spec, fieldUsePolicy, queryShapeLimits).plan()
                : planner.delete(spec));
    }

    long physicalDelete(WriteSpec spec) {
        return execute(governed
                ? planner.physicalDeleteGoverned(spec, fieldUsePolicy, queryShapeLimits).plan()
                : planner.physicalDelete(spec));
    }

    private SqlWriteResult rowsUpdatedReturningKeys(FormOperationPlanner.PlannedWrite plan) {
        return plan.generatedKeyColumn()
                   .map(column -> executor.rowsUpdatedReturningKeys(plan.request(), plan.options(), column))
                   .orElseGet(() -> executor.rowsUpdatedReturningKeys(plan.request(), plan.options()));
    }

    private long execute(FormOperationPlanner.PlannedWrite plan) {
        long affectedRows = plan.protectedWriteRequired()
                ? executor.atomicProtectedWrite(plan.protectedWrite(), plan.options()).affectedRows()
                : executor.rowsUpdated(plan.request(), plan.options());
        return plan.requireSuccess(affectedRows);
    }
}
