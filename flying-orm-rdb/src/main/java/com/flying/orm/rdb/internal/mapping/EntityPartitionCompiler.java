package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.annotation.TablePartition;
import com.flying.orm.core.metadata.TablePartitionDefinition;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.mapping.MappingException;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

/**
 * 把实体表分区注解编译为已绑定物理列的关系事实。
 *
 * @author wangr
 * @version v3.3
 */
final class EntityPartitionCompiler {

    private EntityPartitionCompiler() {
    }

    static TablePartitionDefinition compile(
            Class<?> entityType,
            List<Field> excludedFields,
            Map<String, EntityRelationalMetadataCompiler.Property> properties) {
        TablePartition annotation = entityType.getAnnotation(TablePartition.class);
        if (annotation == null) {
            return null;
        }
        String propertyName = EntityRelationalMetadataCompiler.text(annotation.property());
        if (propertyName == null) {
            throw new MappingException("table partition property must not be blank");
        }
        if (excludedFields.stream().anyMatch(field -> field.getName().equals(propertyName))) {
            throw new MappingException("table partition property must be a persistent entity property");
        }
        EntityRelationalMetadataCompiler.Property property = properties.get(propertyName);
        if (property == null) {
            throw new MappingException("table partition property must be a persistent entity property");
        }
        if (FlyingAnnotationReader.encryptedField(property.field()).isPresent()) {
            throw new MappingException("table partition property must not be encrypted");
        }
        DatabaseType type = property.mapping().databaseType();
        if (type.isArray() || switch (type.logicalType()) {
            case DATE, TIMESTAMP, OFFSET_TIMESTAMP -> false;
            default -> true;
        }) {
            throw new MappingException(
                    "table partition property must map to a scalar date or timestamp column");
        }
        return switch (annotation.strategy()) {
            case RANGE -> TablePartitionDefinition.range(property.metadata().columnName());
        };
    }
}
