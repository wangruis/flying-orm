package com.flying.orm.rdb.schema;

import java.util.Objects;

/**
 * 实体结构校验或同步被拒绝。异常保留完整报告，启动框架可以直接记录表名、计划 SQL 和被拦下的危险项。
 *
 * @author wangr
 * @version v2.0.0
 */
public final class EntitySchemaSyncException extends IllegalStateException {

    private final EntitySchemaSyncReport report;

    public EntitySchemaSyncException(String message, EntitySchemaSyncReport report) {
        super(message);
        this.report = Objects.requireNonNull(report, "entity schema sync report must not be null");
    }

    public EntitySchemaSyncReport report() {
        return report;
    }
}
