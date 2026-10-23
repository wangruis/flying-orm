package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.RelationIdentity;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 元数据缓存内部使用的稳定键和值。
 *
 * <p>缓存直接保存关系身份；相同展示名的不同分段关系不会共用条目。</p>
 */
record MetadataCacheKey(Kind kind, String formId, RelationIdentity relation) {

    String schema() {
        return relation.schema().orElse(null);
    }

    String table() {
        return relation.table();
    }

    static MetadataCacheKey form(String formId, RelationIdentity relation) {
        return new MetadataCacheKey(Kind.FORM, requireText(formId, "metadata cache form id"),
                Objects.requireNonNull(relation, "metadata relation must not be null"));
    }

    /** 已分段的身份不重新解析，也不丢弃 catalog。 */
    static MetadataCacheKey table(RelationIdentity relation) {
        RelationIdentity target = Objects.requireNonNull(
                relation, "metadata cache relation must not be null");
        return new MetadataCacheKey(Kind.TABLE, null, target);
    }

    /** 读取键仍区分大小写；失效覆盖数据库可能折叠的别名，不让旧结构留在另一个入口。 */
    boolean matchesInvalidation(RelationIdentity target) {
        return table().equalsIgnoreCase(target.table())
                && (target.schema().isEmpty() || relation.schema().isEmpty()
                    || target.schema().orElseThrow().equalsIgnoreCase(schema()))
                && (target.catalog().isEmpty() || relation.catalog().isEmpty()
                    || target.catalog().orElseThrow().equalsIgnoreCase(relation.catalog().orElseThrow()));
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
