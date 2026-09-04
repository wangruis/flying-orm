package com.flying.orm.rdb.schema;

import java.util.Objects;

/**
 * 在发送任何 DDL 前拒绝不满足当前实体关系同步策略的整批计划。
 *
 * @author wangr
 * @version v3.2
 */
public final class EntityRelationalSchemaSyncException extends IllegalStateException {

    private final transient EntityRelationalSchemaSyncReport report;

    public EntityRelationalSchemaSyncException(String message,
                                               EntityRelationalSchemaSyncReport report) {
        super(message);
        this.report = Objects.requireNonNull(report, "relational schema sync report must not be null");
    }

    public EntityRelationalSchemaSyncReport report() {
        return report;
    }
}
