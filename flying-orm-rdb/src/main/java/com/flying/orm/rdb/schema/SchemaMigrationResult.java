package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.execution.SqlExecutionStepResult;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;

import java.util.List;
import java.util.Objects;

/**
 * DDL 计划及其实际执行结果，普通表单与完整关系结构使用同一载体。
 *
 * @param plan        执行前生成的计划
 * @param rowsUpdated DDL 执行器返回的影响数汇总
 * @param steps       按执行顺序记录的逐条 DDL 结果，不代表事务提交或结构回读验证
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public record SchemaMigrationResult(SchemaMigrationPlan plan,
                                    long rowsUpdated,
                                    List<SqlExecutionStepResult> steps) {

    public SchemaMigrationResult {
        plan = Objects.requireNonNull(plan, "schema migration plan must not be null");
        steps = List.copyOf(Objects.requireNonNull(steps, "schema migration step results must not be null"));
        if (rowsUpdated < 0) {
            throw new IllegalArgumentException("schema migration rows updated must not be negative");
        }
    }

    public int executedSqlCount() {
        return plan.executableSqlCount();
    }

    public boolean hasSkippedChanges() {
        return plan.hasSkippedChanges();
    }

    static long addRows(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            throw new RdbException(RdbErrorKind.UNKNOWN,
                    "database execution count exceeds supported range", null, null, overflow);
        }
    }
}
