package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.protection.FieldProtectionRegistry;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.FlyingLogicDelete;
import com.flying.orm.rdb.mapping.FlyingTenant;
import com.flying.orm.rdb.mapping.MappingException;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
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

    /** 保留旧实体 CRUD 的编译入口和结果语义。 */
    <T> EntityMetadata<T> compile(Class<T> type) {
        EntityCompilation<T> compilation = compile(type, false);
        return compilation.explicitRelationNamespace()
                ? compilation.relationalMetadata(compilation.fieldMetadata())
                : compilation.metadata(compilation.fieldMetadata());
    }

    /**
     * 为严格关系元数据编译保留同一遍扫描得到的 Java 字段和排除字段。
     * 后续编译器可以替换字段结构信息，不必再次反射实体继承层次。
     */
    <T> EntityCompilation<T> compileModel(Class<T> type) {
        return compile(type, true);
    }

    private <T> EntityCompilation<T> compile(Class<T> type, boolean strictRelational) {
        RelationIdentity relationIdentity = strictRelational
                ? EntityTableNameResolver.resolveRelationalIdentity(type, namingStrategy)
                : EntityTableNameResolver.resolveIdentity(type, namingStrategy);
        String legacyTable = EntityTableNameResolver.resolveTable(relationIdentity);
        String formId = namingStrategy.tableName(type);
        Optional<FlyingLogicDelete> classLogicDelete = Optional.ofNullable(type.getAnnotation(FlyingLogicDelete.class));
        List<Field> persistentFields = strictRelational ? new ArrayList<>() : null;
        List<Field> excludedFields = strictRelational ? new ArrayList<>() : null;
        List<EntityFieldMetadata> fieldMetadata = new ArrayList<>();
        FieldProtectionRegistry.Builder protections = FieldProtectionRegistry.builder();

        // 租户声明也在这次字段遍历中采集。先只记住声明，等普通字段语义校验完成后再按旧顺序校验。
        FlyingTenant classTenant = type.getAnnotation(FlyingTenant.class);
        int fieldTenantCount = 0;
        String fieldTenantName = null;
        TenantStrategy fieldTenantStrategy = TenantStrategy.NONE;

        for (Class<?> persistentType : EntityMetadataHierarchy.persistentTypes(type)) {
            for (Field field : persistentType.getDeclaredFields()) {
                FlyingTenant fieldTenant = field.getAnnotation(FlyingTenant.class);
                if (fieldTenant != null) {
                    fieldTenantCount++;
                    if (fieldTenantCount == 1) {
                        fieldTenantName = field.getName();
                        fieldTenantStrategy = fieldTenant.strategy();
                    }
                }

                int modifiers = field.getModifiers();
                if (field.isSynthetic() || Modifier.isStatic(modifiers)) {
                    continue;
                }
                if (!EntityMetadataHierarchy.isPersistentField(field)) {
                    // 到这里的字段只可能是 Java transient 或 @TableField(exist=false)。
                    // 严格编译器需要它们检查结构注解冲突，普通 CRUD 不为此保留额外列表。
                    if (strictRelational) {
                        excludedFields.add(field);
                    }
                    continue;
                }

                EntityFieldMetadata metadata = EntityFieldMetadataCompiler.compile(
                        type, field, namingStrategy, classLogicDelete, strictRelational);
                if (strictRelational) {
                    persistentFields.add(field);
                }
                fieldMetadata.add(metadata);
                EntityFieldProtectionCompiler.compile(field, metadata, protections);
            }
        }

        if (fieldMetadata.isEmpty()) {
            throw new MappingException("entity has no persistent field: " + type.getName());
        }
        validateSingleSemanticFields(type, fieldMetadata);
        TenantDefinition tenant = resolveTenant(
                type, fieldMetadata, classTenant, fieldTenantCount, fieldTenantName, fieldTenantStrategy);
        return new EntityCompilation<>(type,
                                       formId,
                                       legacyTable,
                                       relationIdentity,
                                       EntityTableNameResolver.hasExplicitNamespace(type),
                                       strictRelational ? persistentFields : List.of(),
                                       fieldMetadata,
                                       strictRelational ? excludedFields : List.of(),
                                       tenant == null ? null : tenant.field(),
                                       tenant == null ? TenantStrategy.NONE : tenant.strategy(),
                                       protections.build());
    }

    private static TenantDefinition resolveTenant(Class<?> type,
                                                   List<EntityFieldMetadata> fields,
                                                   FlyingTenant classTenant,
                                                   int fieldTenantCount,
                                                   String fieldTenantName,
                                                   TenantStrategy fieldTenantStrategy) {
        String requestedField;
        TenantStrategy strategy;
        if (classTenant != null) {
            requestedField = requireTenantField(classTenant.field(), type);
            strategy = classTenant.strategy();
            if (fieldTenantCount > 0) {
                throw new MappingException("entity declares multiple tenant fields: " + type.getName());
            }
        } else {
            if (fieldTenantCount == 0) {
                return null;
            }
            if (fieldTenantCount > 1) {
                throw new MappingException("entity declares multiple tenant fields: " + type.getName());
            }
            requestedField = fieldTenantName;
            strategy = fieldTenantStrategy;
        }

        for (EntityFieldMetadata field : fields) {
            if (EntityFieldNames.matches(field.name(), requestedField)
                    || EntityFieldNames.matches(field.columnName(), requestedField)) {
                return new TenantDefinition(field.columnName(), strategy);
            }
        }
        throw new MappingException("tenant field is not persistent: " + type.getName() + "." + requestedField);
    }

    private static String requireTenantField(String field, Class<?> type) {
        if (field == null || field.isBlank()) {
            throw new MappingException("class-level @FlyingTenant must name a field: " + type.getName());
        }
        return field.trim();
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

    private record TenantDefinition(String field, TenantStrategy strategy) {
    }
}

/**
 * 一次实体反射扫描得到的不可变中间态。
 *
 * <p>严格关系编译器可以在不重新扫描实体的前提下，用替换后的字段结构构造兼容的
 * {@link EntityMetadata}。普通 CRUD 编译不会保留 Java 字段列表。</p>
 */
record EntityCompilation<T>(Class<T> type,
                            String formId,
                            String legacyTable,
                            RelationIdentity relationIdentity,
                            boolean explicitRelationNamespace,
                            List<Field> persistentFields,
                            List<EntityFieldMetadata> fieldMetadata,
                            List<Field> excludedFields,
                            String tenantField,
                            TenantStrategy tenantStrategy,
                            FieldProtectionRegistry protections) {

    EntityCompilation {
        persistentFields = List.copyOf(persistentFields);
        fieldMetadata = List.copyOf(fieldMetadata);
        excludedFields = List.copyOf(excludedFields);
    }

    /** 用替换后的字段结构构造与旧 Repository 共用的实体元数据。 */
    EntityMetadata<T> metadata(List<EntityFieldMetadata> fields) {
        return EntityMetadata.create(type,
                                     formId,
                                     legacyTable,
                                     fields,
                                     tenantField,
                                     tenantStrategy,
                                     protections);
    }

    /** 3.2 严格关系入口把已解析的 catalog/schema/table 原样交给 Repository 表单。 */
    EntityMetadata<T> relationalMetadata(List<EntityFieldMetadata> fields) {
        return EntityMetadata.createRelational(type,
                                               formId,
                                               legacyTable,
                                               relationIdentity,
                                               fields,
                                               tenantField,
                                               tenantStrategy,
                                               protections);
    }
}
