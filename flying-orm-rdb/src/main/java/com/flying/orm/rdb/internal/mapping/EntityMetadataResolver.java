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
import java.util.Objects;
import java.util.Optional;

/**
 * 实体元数据的统一解析入口。
 *
 * <p>本类型直接拥有实体继承层次的一次反射扫描、字段语义校验和租户声明解析。注册表决定何时缓存
 * 不可变结果；严格 Schema 编译复用同一份扫描中间态，不再经过第二个同义编译器。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class EntityMetadataResolver {

    private static final EntityNamingStrategy NAMING_STRATEGY = EntityNamingStrategy.SNAKE_CASE;

    private EntityMetadataResolver() {
    }

    /** 注册表缓存未命中时编译一份不可变实体元数据。 */
    public static <T> EntityMetadata<T> createUncached(Class<T> type) {
        EntityCompilation<T> compilation = compile(
                Objects.requireNonNull(type, "entity type must not be null"), false);
        return compilation.explicitRelationNamespace()
                ? compilation.relationalMetadata(compilation.fieldMetadata())
                : compilation.metadata(compilation.fieldMetadata());
    }

    /** 严格关系冷路径复用同一遍实体扫描，并保留 Java 字段及排除字段。 */
    static <T> EntityCompilation<T> compileModel(Class<T> type) {
        return compile(Objects.requireNonNull(type, "entity type must not be null"), true);
    }

    private static <T> EntityCompilation<T> compile(Class<T> type, boolean strictRelational) {
        RelationIdentity relationIdentity = strictRelational
                ? EntityTableNameResolver.resolveRelationalIdentity(type, NAMING_STRATEGY)
                : EntityTableNameResolver.resolveIdentity(type, NAMING_STRATEGY);
        String legacyTable = EntityTableNameResolver.resolveTable(relationIdentity);
        String formId = NAMING_STRATEGY.tableName(type);
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
                        type, field, NAMING_STRATEGY, classLogicDelete, strictRelational);
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
