package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.protection.FieldProtectionRegistry;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.core.annotation.TableLogic;
import com.flying.orm.rdb.mapping.FlyingTenant;
import com.flying.orm.rdb.mapping.MappingException;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

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

    private EntityMetadataResolver() {
    }

    /** 编译一份不可变实体元数据；缓存归注册表管理。 */
    public static <T> EntityMetadata<T> createUncached(Class<T> type) {
        return compileModel(type, EntityTypeMappingRegistry.standard()).metadata();
    }

    /** 一次扫描的字段事实供 CRUD 和显式 Schema 投影共同使用。 */
    public static <T> Compilation<T> compileModel(Class<T> type, EntityTypeMappingRegistry mappings) {
        return compileModel(type, mappings, Map.of(), Map.of());
    }

    static <T> Compilation<T> compileModel(Class<T> type, EntityTypeMappingRegistry mappings,
                                          Map<String, UniqueConstraintDefinition> declaredUniques,
                                          Map<String, IndexDefinition> declaredIndexes) {
        Objects.requireNonNull(type, "entity type must not be null");
        Objects.requireNonNull(mappings, "entity type mappings must not be null");
        RelationIdentity relationIdentity = EntityTableNameResolver.resolveIdentity(type);
        String formId = EntityFieldNames.snakeCase(type.getSimpleName());
        Optional<TableLogic> classLogicDelete = Optional.ofNullable(type.getAnnotation(TableLogic.class));
        Map<String, Property> properties = new LinkedHashMap<>();
        List<Field> excludedFields = new ArrayList<>();
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
                    excludedFields.add(field);
                    continue;
                }

                EntityFieldMetadata metadata = EntityFieldMetadataCompiler.compile(
                        type, field, classLogicDelete, mappings);
                if (properties.putIfAbsent(field.getName(), new Property(field, metadata)) != null) {
                    throw new MappingException("entity declares duplicate persistent property: " + field.getName());
                }
                EntityFieldProtectionCompiler.compile(field, metadata, protections);
            }
        }

        if (properties.isEmpty()) {
            throw new MappingException("entity has no persistent field: " + type.getName());
        }
        Map<String, UniqueConstraintDefinition> uniques = new LinkedHashMap<>(declaredUniques);
        EntityRelationalConstraintCompiler.mergeAnnotatedUniques(type, properties, uniques);
        Map<String, IndexDefinition> indexes = new LinkedHashMap<>(declaredIndexes);
        EntityRelationalConstraintCompiler.mergeAnnotatedIndexes(type, properties, indexes);
        Set<String> uniqueColumns = EntityRelationalConstraintCompiler.singleColumnUniqueColumns(
                uniques.values(), indexes.values());
        if (!uniqueColumns.isEmpty()) {
            properties.replaceAll((name, property) -> {
                EntityFieldMetadata field = property.metadata();
                return uniqueColumns.contains(field.columnName()) && !field.unique()
                        ? new Property(property.field(), withUnique(field)) : property;
            });
        }
        List<EntityFieldMetadata> fieldMetadata = properties.values().stream().map(Property::metadata).toList();
        validateSingleSemanticFields(type, fieldMetadata, classLogicDelete);
        TenantDefinition tenant = resolveTenant(
                type, fieldMetadata, classTenant, fieldTenantCount, fieldTenantName, fieldTenantStrategy);
        String tenantField = tenant == null ? null : tenant.field();
        TenantStrategy tenantStrategy = tenant == null ? TenantStrategy.NONE : tenant.strategy();
        FieldProtectionRegistry fieldProtections = protections.build();
        try {
            EntityMetadata<T> metadata = EntityMetadata.create(type, formId, relationIdentity,
                    EntityTableNameResolver.namingSource(type, relationIdentity),
                    fieldMetadata, tenantField, tenantStrategy, fieldProtections, Map.of());
            return new Compilation<>(metadata, properties, excludedFields, uniques, indexes);
        } catch (IllegalArgumentException | IllegalStateException error) {
            throw new MappingException("entity metadata is invalid: " + type.getName(), error);
        }
    }

    private static EntityFieldMetadata withUnique(EntityFieldMetadata source) {
        return new EntityFieldMetadata(
                source.name(), source.javaType(), source.field().withUnique(true), source.typeMapping(),
                source.version(), source.logicDelete(), source.logicNotDeletedValue(), source.logicDeletedValue(),
                source.idType(), source.enumStorage(), source.enumValueMember(), source.selectable(),
                source.ordered(), source.orderAscending(), source.orderPriority(),
                source.fill(), source.insertStrategy(), source.updateStrategy(), source.insertable(), source.updatable());
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

    private static void validateSingleSemanticFields(Class<?> type, List<EntityFieldMetadata> fields,
                                                      Optional<TableLogic> classLogicDelete) {
        String declared = classLogicDelete.map(TableLogic::field).map(FlyingAnnotationReader::text).orElse(null);
        if (classLogicDelete.isPresent() && (declared == null || fields.stream().noneMatch(
                field -> field.logicDelete() && (EntityFieldNames.matches(field.name(), declared)
                        || EntityFieldNames.matches(field.columnName(), declared))))) {
            throw new MappingException("class-level @TableLogic must name a persistent field: " + type.getName());
        }
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

    /** 同一缓存条目拥有实体模型和 Schema 投影需要的 Java 字段，不另建反射缓存。 */
    public record Compilation<T>(EntityMetadata<T> metadata,
                                 Map<String, Property> properties,
                                 List<Field> excludedFields,
                                 Map<String, UniqueConstraintDefinition> uniques,
                                 Map<String, IndexDefinition> indexes) {
        public Compilation {
            properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
            excludedFields = List.copyOf(excludedFields);
            uniques = Collections.unmodifiableMap(new LinkedHashMap<>(uniques));
            indexes = Collections.unmodifiableMap(new LinkedHashMap<>(indexes));
        }
    }

    /** Java 字段和持久化字段的编译绑定，结构投影直接使用。 */
    public record Property(Field field, EntityFieldMetadata metadata) {
        EntityTypeMappingRegistry.Mapping mapping() {
            return metadata.typeMapping();
        }
    }
}
