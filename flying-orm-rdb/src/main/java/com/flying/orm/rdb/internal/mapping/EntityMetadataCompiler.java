package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.protection.FieldProtectionRegistry;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.FlyingLogicDelete;
import com.flying.orm.rdb.mapping.MappingException;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 编排一次实体元数据编译。
 * 父类字段固定排在子类字段前，这个顺序同时是实体读取、单行写入和批量参数布局的共同依据。
 */
final class EntityMetadataCompiler {

    private final EntityNamingStrategy namingStrategy;

    EntityMetadataCompiler(EntityNamingStrategy namingStrategy) {
        this.namingStrategy = namingStrategy;
    }

    <T> EntityMetadata<T> compile(Class<T> type) {
        String table = EntityTableNameResolver.resolve(type, namingStrategy);
        CompiledFields compiled = fields(type);
        List<EntityFieldMetadata> fields = compiled.fields();
        if (fields.isEmpty()) {
            throw new MappingException("entity has no persistent field: " + type.getName());
        }
        validateSingleSemanticFields(type, fields);
        EntityTenantResolver.TenantMapping tenant = EntityTenantResolver.resolve(type, fields);
        return EntityMetadata.create(type,
                                     namingStrategy.tableName(type),
                                     table,
                                     fields,
                                     tenant == null ? null : tenant.field(),
                                     tenant == null ? TenantStrategy.NONE : tenant.strategy(),
                                     compiled.protections());
    }

    private CompiledFields fields(Class<?> type) {
        Optional<FlyingLogicDelete> classLogicDelete = Optional.ofNullable(type.getAnnotation(FlyingLogicDelete.class));
        List<EntityFieldMetadata> fields = new ArrayList<>();
        FieldProtectionRegistry.Builder protections = FieldProtectionRegistry.builder();
        for (Class<?> persistentType : EntityMetadataHierarchy.persistentTypes(type)) {
            for (Field field : persistentType.getDeclaredFields()) {
                if (EntityMetadataHierarchy.isPersistentField(field)) {
                    EntityFieldMetadata metadata = EntityFieldMetadataCompiler.compile(
                            type, field, namingStrategy, classLogicDelete);
                    fields.add(metadata);
                    EntityFieldProtectionCompiler.compile(field, metadata, protections);
                }
            }
        }
        return new CompiledFields(List.copyOf(fields), protections.build());
    }

    private static void validateSingleSemanticFields(Class<?> type, List<EntityFieldMetadata> fields) {
        int versions = 0;
        int logicDeletes = 0;
        for (EntityFieldMetadata field : fields) {
            if (field.version() && ++versions > 1) {
                throw new MappingException("entity must not declare multiple version fields: " + type.getName());
            }
            if (field.logicDelete() && ++logicDeletes > 1) {
                throw new MappingException("entity must not declare multiple logic delete fields: " + type.getName());
            }
        }
    }

    private record CompiledFields(List<EntityFieldMetadata> fields,
                                  FieldProtectionRegistry protections) {
    }
}
