package com.flying.orm.rdb.internal.mapping;

/** 处理字段名与列名之间常见的下划线、连字符差异。 */
final class EntityMetadataNames {

    private EntityMetadataNames() {
    }

    static boolean matches(String left, String right) {
        return left.replace("_", "")
                   .replace("-", "")
                   .equalsIgnoreCase(right.replace("_", "").replace("-", ""));
    }
}
