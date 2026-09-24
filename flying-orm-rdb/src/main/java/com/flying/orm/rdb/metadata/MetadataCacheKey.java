package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.RelationIdentity;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 元数据缓存内部使用的稳定键和值。
 *
 * <p>表名只在这里拆成 schema 和 table，读取、命中和失效就不会各自维护一套解析规则。</p>
 */
record MetadataCacheKey(Kind kind, String formId, String schema, String table) {

    static MetadataCacheKey form(String formId, String schema, String table) {
        String[] tableParts = splitTable(schema, table);
        return new MetadataCacheKey(Kind.FORM,
                                    requireText(formId, "metadata cache form id"),
                                    tableParts[0],
                                    tableParts[1]);
    }

    static MetadataCacheKey table(String schema, String table) {
        String[] tableParts = splitTable(schema, table);
        return new MetadataCacheKey(Kind.TABLE, null, tableParts[0], tableParts[1]);
    }

    /** 关系型入口已经完成分段，不能再按点号猜测。catalog 由连接身份承接，不进入本地缓存键。 */
    static MetadataCacheKey table(RelationIdentity relation) {
        RelationIdentity target = Objects.requireNonNull(
                relation, "metadata cache relation must not be null");
        String schema = target.schema().map(value -> requireText(value, "metadata cache schema"))
                .orElse(null);
        return new MetadataCacheKey(
                Kind.TABLE, null, schema, requireText(target.table(), "metadata cache table"));
    }

    MetadataCacheKey asForm(String formId) {
        return new MetadataCacheKey(
                Kind.FORM, requireText(formId, "metadata cache form id"), schema, table);
    }

    /** 读取键仍区分大小写；失效覆盖数据库可能折叠的别名，不让旧结构留在另一个入口。 */
    boolean matchesInvalidation(String targetSchema, String targetTable) {
        return table.equalsIgnoreCase(targetTable)
                && (targetSchema == null || schema == null || schema.equalsIgnoreCase(targetSchema));
    }

    private static String[] splitTable(String schema, String table) {
        String safeTable = requireText(table, "metadata cache table");
        if (schema != null) {
            return new String[]{requireText(schema, "metadata cache schema"), safeTable};
        }
        int separator = safeTable.indexOf('.');
        if (separator < 0) {
            return new String[]{null, safeTable};
        }
        if (separator == 0
                || separator == safeTable.length() - 1
                || safeTable.indexOf('.', separator + 1) >= 0) {
            throw new IllegalArgumentException("metadata cache table must be table or schema.table");
        }
        return new String[]{requireText(safeTable.substring(0, separator), "metadata cache schema"),
                            requireText(safeTable.substring(separator + 1), "metadata cache table")};
    }

    private static String requireText(String value, String name) {
        String safeValue = Objects.requireNonNull(value, name + " must not be null").trim();
        if (safeValue.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return safeValue;
    }

    enum Kind {
        FORM,
        TABLE
    }
}

/** 缓存的共享 Mono 及其在统一区域中的稳定逻辑权重。 */
record MetadataCachedValue<T>(Mono<T> value, int weight) {
}
