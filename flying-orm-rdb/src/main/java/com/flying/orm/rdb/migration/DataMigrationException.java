package com.flying.orm.rdb.migration;

import java.util.Objects;

/** 正向迁移失败后的异常，始终携带补偿是否成功的完整结果。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class DataMigrationException extends RuntimeException {

    private final DataMigrationResult result;

    DataMigrationException(DataMigrationResult result, Throwable cause) {
        super("data migration failed: status=" + result.status(), cause);
        this.result = Objects.requireNonNull(result, "data migration failure result must not be null");
    }

    public DataMigrationResult result() {
        return result;
    }
}
