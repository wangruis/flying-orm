package com.flying.orm.rdb.migration;

import com.flying.orm.core.sql.render.SqlRequest;

import java.util.Objects;

/** 一步参数化数据迁移，以及失败后用来恢复已提交数据的补偿 SQL。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record DataMigrationStep(String id, SqlRequest forward, SqlRequest rollback) {

    public DataMigrationStep {
        id = Objects.requireNonNull(id, "data migration step id must not be null").trim();
        if (id.isEmpty()) {
            throw new IllegalArgumentException("data migration step id must not be blank");
        }
        forward = Objects.requireNonNull(forward, "data migration forward request must not be null");
        rollback = Objects.requireNonNull(rollback, "data migration rollback request must not be null");
    }
}
