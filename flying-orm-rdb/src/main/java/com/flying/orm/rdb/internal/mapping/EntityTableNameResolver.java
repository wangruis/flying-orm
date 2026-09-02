package com.flying.orm.rdb.internal.mapping;

import java.util.Optional;

/** 从 flying-orm 的 @TableName 或命名约定确定物理表名。 */
final class EntityTableNameResolver {

    private EntityTableNameResolver() {
    }

    static String resolve(Class<?> type, EntityNamingStrategy namingStrategy) {
        Optional<com.flying.orm.core.annotation.TableName> flyingTable = FlyingAnnotationReader.tableName(type);
        String defaultName = namingStrategy.tableName(type);
        if (flyingTable.isPresent()) {
            com.flying.orm.core.annotation.TableName definition = flyingTable.orElseThrow();
            String name = Optional.ofNullable(FlyingAnnotationReader.text(definition.value())).orElse(defaultName);
            String schema = FlyingAnnotationReader.text(definition.schema());
            return schema == null ? name : schema + "." + name;
        }
        return defaultName;
    }
}
