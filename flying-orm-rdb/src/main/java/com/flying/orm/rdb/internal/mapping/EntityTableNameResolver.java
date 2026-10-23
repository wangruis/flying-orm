package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.annotation.TableCatalog;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.rdb.mapping.MappingException;

import java.util.Optional;

/** 从表注解或命名约定解析 CRUD 与 Schema 共用的物理关系身份。 */
final class EntityTableNameResolver {

    private EntityTableNameResolver() {
    }

    /** 持久对象的命名输入来自注解原始表名，不随关系展示格式调整。 */
    static String namingSource(Class<?> type, RelationIdentity identity) {
        return FlyingAnnotationReader.tableName(type).map(TableName::value)
                .map(FlyingAnnotationReader::text).orElse(identity.table());
    }

    /**
     * 直接从实体注解构造分段关系身份。
     *
     * <p>无显式命名空间时支持限定名简写；显式声明命名空间后各段按字面值保存。</p>
     */
    static RelationIdentity resolveIdentity(Class<?> type) {
        Optional<TableName> tableDefinition = FlyingAnnotationReader.tableName(type);
        String defaultName = EntityFieldNames.snakeCase(type.getSimpleName());
        String table = tableDefinition.map(TableName::value)
                                      .map(FlyingAnnotationReader::text)
                                      .orElse(null);
        String schema = tableDefinition.map(TableName::schema)
                                       .map(FlyingAnnotationReader::text)
                                       .orElse(null);
        TableCatalog catalogDefinition = type.getAnnotation(TableCatalog.class);
        String catalog = catalogDefinition == null
                ? null
                : catalogDefinition.value();
        String name = table == null ? defaultName : table;
        if (catalogDefinition != null || schema != null || !name.contains(".")) {
            return RelationIdentity.of(catalog, schema, name);
        }
        try {
            return RelationIdentity.parse(name);
        } catch (IllegalArgumentException error) {
            throw new MappingException("invalid qualified table name: " + type.getName(), error);
        }
    }
}
