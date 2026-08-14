package com.flying.orm.rdb.metadata;

import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 元数据缓存内部使用的稳定键和值。
 *
 * <p>表名只在这里拆成 schema 和 table，读取、命中和失效就不会各自维护一套解析规则。</p>
 */
record MetadataCacheKey(Kind kind, String formId, String partition, String schema, String table) {

    static MetadataCacheKey form(String formId, String schema, String table) {
        return form(formId, null, schema, table);
    }

    static MetadataCacheKey form(String formId, String partition, String schema, String table) {
        String[] tableParts = splitTable(schema, table);
        return new MetadataCacheKey(Kind.FORM,
                                    requireText(formId, "metadata cache form id"),
                                    optionalText(partition),
                                    tableParts[0],
                                    tableParts[1]);
    }

    static MetadataCacheKey table(String schema, String table) {
        return table(null, schema, table);
    }

    static MetadataCacheKey table(String partition, String schema, String table) {
        String[] tableParts = splitTable(schema, table);
        return new MetadataCacheKey(Kind.TABLE, null, optionalText(partition), tableParts[0], tableParts[1]);
    }

    MetadataCacheKey {
        kind = Objects.requireNonNull(kind, "metadata cache key kind must not be null");
        if (schema != null) {
            schema = requireText(schema, "metadata cache schema");
        }
        table = requireText(table, "metadata cache table");
    }

    MetadataCacheKey withIsolation(String databaseKey, String contextSchema) {
        return new MetadataCacheKey(kind,
                                    formId,
                                    optionalText(databaseKey),
                                    schema == null ? optionalText(contextSchema) : schema,
                                    table);
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

    private static String optionalText(String value) {
        return value == null ? null : requireText(value, "metadata cache partition");
    }

    enum Kind {
        FORM,
        TABLE
    }
}

/** 缓存的共享 Mono 及其在统一区域中的稳定逻辑权重。 */
record MetadataCachedValue<T>(Mono<T> value, int weight) {
}
