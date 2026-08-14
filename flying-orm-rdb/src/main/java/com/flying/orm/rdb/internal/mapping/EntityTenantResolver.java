package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.FlyingTenant;
import com.flying.orm.rdb.mapping.MappingException;

import java.lang.reflect.Field;
import java.util.List;

/** 解析实体唯一的租户字段，并把 Java 字段名统一收口为物理列名。 */
final class EntityTenantResolver {

    private EntityTenantResolver() {
    }

    static TenantMapping resolve(Class<?> entityType, List<EntityFieldMetadata> fields) {
        FlyingTenant classTenant = entityType.getAnnotation(FlyingTenant.class);
        TenantMapping mapping = classTenant == null
                ? null
                : new TenantMapping(requireField(classTenant.field(), entityType), classTenant.strategy());
        for (Class<?> persistentType : EntityMetadataHierarchy.persistentTypes(entityType)) {
            for (Field field : persistentType.getDeclaredFields()) {
                FlyingTenant annotation = field.getAnnotation(FlyingTenant.class);
                if (annotation == null) {
                    continue;
                }
                if (mapping != null) {
                    throw new MappingException("entity declares multiple tenant fields: " + entityType.getName());
                }
                mapping = new TenantMapping(field.getName(), annotation.strategy());
            }
        }
        if (mapping == null) {
            return null;
        }
        String requested = mapping.field();
        EntityFieldMetadata field = fields.stream()
                                           .filter(candidate -> EntityMetadataNames.matches(candidate.name(), requested)
                                                   || EntityMetadataNames.matches(candidate.columnName(), requested))
                                           .findFirst()
                                           .orElseThrow(() -> new MappingException("tenant field is not persistent: "
                                                   + entityType.getName() + "." + requested));
        return new TenantMapping(field.columnName(), mapping.strategy());
    }

    private static String requireField(String field, Class<?> entityType) {
        if (field == null || field.isBlank()) {
            throw new MappingException("class-level @FlyingTenant must name a field: " + entityType.getName());
        }
        return field.trim();
    }

    record TenantMapping(String field, TenantStrategy strategy) {
    }
}
