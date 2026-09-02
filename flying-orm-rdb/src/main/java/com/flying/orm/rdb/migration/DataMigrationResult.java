package com.flying.orm.rdb.migration;

import java.util.List;
import java.util.Objects;

/** 数据迁移执行结果，上层可以据此决定告警、人工恢复或是否继续发布。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record DataMigrationResult(String planId,
                                  DataMigrationStatus status,
                                  List<DataMigrationStepResult> steps) {

    public DataMigrationResult {
        planId = Objects.requireNonNull(planId, "data migration result plan id must not be null");
        status = Objects.requireNonNull(status, "data migration status must not be null");
        steps = List.copyOf(Objects.requireNonNull(steps, "data migration step results must not be null"));
    }
}
