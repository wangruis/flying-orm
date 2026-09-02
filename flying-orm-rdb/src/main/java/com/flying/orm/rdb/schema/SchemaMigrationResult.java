package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.execution.SqlExecutionStepResult;

import java.util.List;
import java.util.Objects;

/**
 * DDL 迁移执行后的结果。它把执行结果和计划放一起，上层能知道哪些做了、哪些被拦了。
 *
 * @param plan        执行前生成的计划
 * @param rowsUpdated DDL 执行器返回的影响数汇总
 * @param steps       使用同连接执行时记录的逐条 DDL 结果；普通执行路径没有明细时为空
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

    public boolean requiresManualReview() {
        return plan.requiresManualReview();
    }
}
