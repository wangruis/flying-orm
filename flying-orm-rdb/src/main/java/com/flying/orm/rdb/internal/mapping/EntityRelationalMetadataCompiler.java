package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.join.QueryRelation;
import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationalSchemaDefinition;
import com.flying.orm.core.metadata.TablePartitionDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.protection.ProtectedRelationalSchemaProjector;
import com.flying.orm.rdb.internal.mapping.EntityMetadataResolver.Property;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 把实体注解和程序化声明编译成一份严格、不可变的关系模型。
 *
 * <p>反射扫描由 {@link EntityMetadataResolver} 完成一次；本类只在 Schema/descriptor 冷路径上把扫描结果
 * 投影成完整关系元数据。唯一性等读写事实由模型编译器拥有；完整 DDL 校验只在本路径执行。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class EntityRelationalMetadataCompiler {

    private EntityRelationalMetadataCompiler() {
    }

    /** EntitySchemaDescriptor.Builder 的唯一编译出口。 */
    public static <T> EntitySchemaDescriptor<T> compile(
            Class<T> entityType,
            EntityTypeMappingRegistry typeMappings,
            PrimaryKeyDefinition declaredPrimaryKey,
            Map<String, UniqueConstraintDefinition> declaredUniques,
            Map<String, IndexDefinition> declaredIndexes,
            Map<String, ForeignKeyDefinition> declaredForeignKeys,
            Map<String, CheckConstraintDefinition> declaredChecks,
            Map<String, QueryRelation> relations) {
        Class<T> safeType = Objects.requireNonNull(entityType, "entity type must not be null");
        EntityTypeMappingRegistry safeMappings = Objects.requireNonNull(
                typeMappings, "entity type mappings must not be null");
        return compile(EntityMetadataResolver.compileModel(safeType, safeMappings, declaredUniques, declaredIndexes),
                safeMappings, declaredPrimaryKey, declaredForeignKeys, declaredChecks, relations);
    }

    /** 默认 Schema 路径复用注册表已经编译的字段，不重新解释注解或类型映射。 */
    public static <T> EntitySchemaDescriptor<T> compile(EntityMetadataResolver.Compilation<T> compilation,
                                                        EntityTypeMappingRegistry typeMappings) {
        return compile(compilation, typeMappings, null, Map.of(), Map.of(), Map.of());
    }

    private static <T> EntitySchemaDescriptor<T> compile(
            EntityMetadataResolver.Compilation<T> compilation,
            EntityTypeMappingRegistry typeMappings,
            PrimaryKeyDefinition declaredPrimaryKey,
            Map<String, ForeignKeyDefinition> declaredForeignKeys,
            Map<String, CheckConstraintDefinition> declaredChecks,
            Map<String, QueryRelation> relations) {
        EntityMetadata<T> source = compilation.metadata();
        Class<T> safeType = source.type();
        RelationIdentity identity = source.toDynamicForm().relationIdentity();

        rejectSchemaAnnotationsOnExcludedFields(compilation.excludedFields());
        Map<String, Property> properties = compilation.properties();
        for (Property property : properties.values()) {
            if (property.mapping() == null) {
                throw new MappingException("no strict schema type mapping for entity property "
                        + property.field().getName() + " with Java type " + property.field().getType().getTypeName());
            }
        }
        PrimaryKeyDefinition primaryKey = EntityRelationalConstraintCompiler.primaryKey(
                safeType, identity, properties, declaredPrimaryKey);

        LinkedHashMap<String, ForeignKeyDefinition> foreignKeys =
                new LinkedHashMap<>(declaredForeignKeys);
        EntityRelationalConstraintCompiler.mergeAnnotatedForeignKeys(
                safeType, properties, foreignKeys);
        LinkedHashMap<String, CheckConstraintDefinition> checks =
                new LinkedHashMap<>(declaredChecks);
        EntityRelationalConstraintCompiler.mergeAnnotatedChecks(safeType, properties, checks);

        TablePartitionDefinition partition = EntityPartitionCompiler.compile(
                safeType, compilation.excludedFields(), properties);
        RelationalTableDefinition.Builder table =
                RelationalTableDefinition.builder(identity)
                        .comment(tableComment(safeType));
        for (Property property : properties.values()) {
            table.addColumn(property.metadata().field().column());
        }
        if (primaryKey != null) {
            table.primaryKey(primaryKey);
        }
        if (partition != null) {
            table.partition(partition);
        }
        compilation.uniques().values().forEach(table::addUnique);
        compilation.indexes().values().forEach(table::addIndex);
        foreignKeys.values().forEach(table::addForeignKey);
        checks.values().forEach(table::addCheck);

        try {
            EntityMetadata<T> metadata = relations.isEmpty()
                    ? source : EntityMetadata.create(safeType, source.formId(),
                    identity, source.toDynamicForm().namingSource(), source.fields(), source.tenantField().map(EntityFieldMetadata::columnName).orElse(null),
                    source.tenantStrategy(), source.toDynamicForm().protections(), relations);
            RelationalTableDefinition relationalTable = table.build();
            RelationalSchemaDefinition physicalSchema =
                    ProtectedRelationalSchemaProjector.project(
                            metadata.toDynamicForm(), relationalTable);
            return EntitySchemaDescriptor.create(
                    typeMappings, metadata, physicalSchema);
        } catch (MappingException error) {
            throw error;
        } catch (IllegalArgumentException | IllegalStateException error) {
            throw new MappingException(
                    "entity relational metadata is invalid: " + safeType.getName(), error);
        }
    }

    private static void rejectSchemaAnnotationsOnExcludedFields(List<Field> excludedFields) {
        for (Field field : excludedFields) {
            if (field.isAnnotationPresent(TableColumn.class)
                    || field.isAnnotationPresent(TableId.class)) {
                throw new MappingException(
                        "non-persistent entity property must not declare schema metadata: "
                                + field.getDeclaringClass().getName() + '.' + field.getName());
            }
        }
    }

    private static String tableComment(Class<?> entityType) {
        TableName table = entityType.getAnnotation(TableName.class);
        return table == null ? null : text(table.comment());
    }

    static <T> T build(String owner, java.util.function.Supplier<T> factory) {
        try {
            return factory.get();
        } catch (IllegalArgumentException | IllegalStateException error) {
            throw new MappingException(owner + " definition is invalid", error);
        }
    }

    static String text(String value) {
        if (value == null) {
            return null;
        }
        String text = value.trim();
        return text.isEmpty() ? null : text;
    }

}
