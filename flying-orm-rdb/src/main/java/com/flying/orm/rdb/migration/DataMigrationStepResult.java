package com.flying.orm.rdb.migration;

import java.util.Objects;

/** 单步正向执行和补偿执行的结果。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record DataMigrationStepResult(String stepId,
                                      long rowsUpdated,
                                      boolean rolledBack,
                                      long rollbackRows,
                                      String rollbackFailure) {

    public DataMigrationStepResult {
        stepId = Objects.requireNonNull(stepId, "data migration step result id must not be null");
        if (rowsUpdated < 0 || rollbackRows < 0) {
            throw new IllegalArgumentException("data migration row counts must not be negative");
        }
    }

    static DataMigrationStepResult completed(String stepId, long rows) {
        return new DataMigrationStepResult(stepId, rows, false, 0, null);
    }

    DataMigrationStepResult rolledBack(long rows) {
        return new DataMigrationStepResult(stepId, rowsUpdated, true, rows, null);
    }

    DataMigrationStepResult rollbackFailed(Throwable failure) {
        Objects.requireNonNull(failure, "data migration rollback failure must not be null");
        // 驱动异常原文可能包含 SQL、连接信息或无界业务值，因此不能写入公开结果。
        return new DataMigrationStepResult(stepId, rowsUpdated, false, 0, "data migration rollback failed");
    }
}
