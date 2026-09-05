package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableComment;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationalSchemaDefinition;
import com.flying.orm.core.metadata.TablePartitionDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.protection.ProtectedRelationalSchemaProjector;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 把实体注解和程序化声明编译成一份严格、不可变的关系模型。
 *
 * <p>反射扫描由 {@link EntityMetadataCompiler} 完成一次；本类只在 Schema/descriptor 冷路径上把扫描结果
 * 投影成 CRUD 元数据和完整关系元数据。普通旧实体仍走原来的轻量编译，不承担这里的约束归并和指纹成本。</p>
 *
 * @author wangr
 * @version v3.2
 */
@InternalApi
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
            Map<String, CheckConstraintDefinition> declaredChecks) {
        Class<T> safeType = Objects.requireNonNull(entityType, "entity type must not be null");
        EntityTypeMappingRegistry safeMappings = Objects.requireNonNull(
                typeMappings, "entity type mappings must not be null");
        EntityCompilation<T> compilation = new EntityMetadataCompiler(EntityNamingStrategy.SNAKE_CASE)
                .compileModel(safeType);

        rejectSchemaAnnotationsOnExcludedFields(compilation.excludedFields());
        LinkedHashMap<String, Property> properties = properties(compilation, safeMappings);
        PrimaryKeyDefinition primaryKey = EntityRelationalConstraintCompiler.primaryKey(
                safeType, compilation.relationIdentity(), properties, declaredPrimaryKey);

        LinkedHashMap<String, UniqueConstraintDefinition> uniques =
                new LinkedHashMap<>(declaredUniques);
        EntityRelationalConstraintCompiler.mergeAnnotatedUniques(
                safeType, properties, uniques);
        LinkedHashMap<String, IndexDefinition> indexes = new LinkedHashMap<>(declaredIndexes);
        EntityRelationalConstraintCompiler.mergeAnnotatedIndexes(
                safeType, properties, indexes);
        LinkedHashMap<String, ForeignKeyDefinition> foreignKeys =
                new LinkedHashMap<>(declaredForeignKeys);
        EntityRelationalConstraintCompiler.mergeAnnotatedForeignKeys(
                safeType, properties, foreignKeys);
        LinkedHashMap<String, CheckConstraintDefinition> checks =
                new LinkedHashMap<>(declaredChecks);
        EntityRelationalConstraintCompiler.mergeAnnotatedChecks(safeType, properties, checks);

        Set<String> uniqueIndexColumns =
                EntityRelationalConstraintCompiler.singleColumnUniqueColumns(
                        uniques.values(), indexes.values());
        Set<String> primaryKeyColumns = primaryKey == null
                ? Set.of() : Set.copyOf(primaryKey.columns());
        TablePartitionDefinition partition = EntityPartitionCompiler.compile(
                safeType, compilation.excludedFields(), properties);
        List<EntityFieldMetadata> strictFields = new ArrayList<>(properties.size());
        Map<String, EntityTypeMappingRegistry.Mapping> fieldMappings =
                new LinkedHashMap<>(properties.size());
        RelationalTableDefinition.Builder table =
                RelationalTableDefinition.builder(compilation.relationIdentity())
                        .comment(tableComment(safeType));
        for (Property property : properties.values()) {
            boolean primary = primaryKeyColumns.contains(property.metadata().columnName());
            boolean unique = uniqueIndexColumns.contains(property.metadata().columnName());
            EntityFieldMetadata strictField = strictField(property, primary, unique);
            strictFields.add(strictField);
            fieldMappings.put(strictField.name(), property.mapping());
            table.addColumn(column(property, strictField));
        }
        if (primaryKey != null) {
            table.primaryKey(primaryKey);
        }
        if (partition != null) {
            table.partition(partition);
        }
        uniques.values().forEach(table::addUnique);
        indexes.values().forEach(table::addIndex);
        foreignKeys.values().forEach(table::addForeignKey);
        checks.values().forEach(table::addCheck);

        try {
            EntityMetadata<T> metadata = compilation.relationalMetadata(strictFields);
            RelationalTableDefinition relationalTable = table.build();
            requireSameColumns(metadata.toDynamicForm(), relationalTable);
            RelationalSchemaDefinition physicalSchema =
                    ProtectedRelationalSchemaProjector.project(
                            metadata.toDynamicForm(), relationalTable);
            return EntitySchemaDescriptor.create(
                    safeMappings, metadata, physicalSchema, fieldMappings);
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

    private static <T> LinkedHashMap<String, Property> properties(
            EntityCompilation<T> compilation,
            EntityTypeMappingRegistry mappings) {
        List<Field> javaFields = compilation.persistentFields();
        List<EntityFieldMetadata> metadata = compilation.fieldMetadata();
        if (javaFields.size() != metadata.size()) {
            throw new IllegalStateException("entity field scan and metadata must stay aligned");
        }
        LinkedHashMap<String, Property> properties = new LinkedHashMap<>();
        for (int index = 0; index < javaFields.size(); index++) {
            Field field = javaFields.get(index);
            EntityFieldMetadata fieldMetadata = metadata.get(index);
            TableColumn column = field.getAnnotation(TableColumn.class);
            EntityTypeMappingRegistry.Mapping mapping = resolveMapping(
                    compilation.type(), field, fieldMetadata, column, mappings);
            Property previous = properties.putIfAbsent(
                    field.getName(), new Property(field, fieldMetadata, mapping, column));
            if (previous != null) {
                throw new MappingException(
                        "entity declares duplicate persistent property: " + field.getName());
            }
        }
        return properties;
    }

    private static EntityTypeMappingRegistry.Mapping resolveMapping(
            Class<?> entityType,
            Field field,
            EntityFieldMetadata metadata,
            TableColumn column,
            EntityTypeMappingRegistry mappings) {
        Class<?> mappedType = mappedJavaType(field, metadata);
        try {
            String explicitId = column == null ? null : text(column.databaseTypeId());
            return explicitId == null
                    ? mappings.resolve(mappedType) : mappings.resolve(explicitId, mappedType);
        } catch (MappingException error) {
            throw new MappingException(
                    "no strict schema type mapping for entity property "
                            + entityType.getName() + '.' + field.getName()
                            + " with Java type " + mappedType.getTypeName(), error);
        }
    }

    private static Class<?> mappedJavaType(Field field, EntityFieldMetadata metadata) {
        if (metadata.enumValueMember() == null) {
            return field.getType();
        }
        try {
            return field.getType().getDeclaredField(metadata.enumValueMember()).getType();
        } catch (NoSuchFieldException error) {
            throw new MappingException(
                    "enum value member is missing after entity compilation", error);
        }
    }

    private static EntityFieldMetadata strictField(
            Property property,
            boolean primaryKey,
            boolean unique) {
        EntityFieldMetadata source = property.metadata();
        TableColumn annotation = property.column();
        DatabaseType databaseType = property.mapping().databaseType();
        Integer length = annotation == null
                ? source.length() : optionalPositive(annotation.length());
        Integer precision = annotation == null
                ? source.precision() : optionalNonNegative(annotation.precision());
        Integer scale = annotation == null
                ? source.scale() : optionalNonNegative(annotation.scale());
        Integer temporalPrecision = annotation == null
                ? null : optionalNonNegative(annotation.temporalPrecision());
        if (databaseType.isTemporal()) {
            if (precision != null && temporalPrecision != null) {
                throw new MappingException(
                        "temporal column must not declare both precision and temporalPrecision");
            }
            precision = temporalPrecision == null ? precision : temporalPrecision;
            scale = null;
        } else if (temporalPrecision != null) {
            throw new MappingException("temporalPrecision requires a temporal database type");
        }
        boolean nullable = nullable(annotation, primaryKey, source.nullable());
        ValueGeneration generation = generation(annotation, source);
        Object logicNotDeletedValue = source.logicDelete()
                ? readLogicDeleteLiteral(property, source.logicNotDeletedValue()) : null;
        Object logicDeletedValue = source.logicDelete()
                ? readLogicDeleteLiteral(property, source.logicDeletedValue()) : null;
        return new EntityFieldMetadata(
                source.name(), source.columnName(), databaseType,
                source.primaryKey(), source.version(), source.logicDelete(),
                logicNotDeletedValue, logicDeletedValue,
                length, precision, scale, generation, source.idType(),
                source.enumStorage(), source.enumValueMember(), source.selectable(),
                source.ordered(), source.orderAscending(), source.orderPriority(),
                source.fill(), source.insertStrategy(), source.updateStrategy(),
                source.insertable(), source.updatable(), nullable, source.unique() || unique);
    }

    private static Object readLogicDeleteLiteral(Property property, Object literal) {
        EntityTypeMappingRegistry.Mapping mapping = property.mapping();
        // descriptor 冷路径只还原领域值；真正绑定时再 write 一次，避免双重编码。
        return mapping.codec().read(literal, mapping.javaType());
    }

    private static ColumnDefinition column(Property property, EntityFieldMetadata field) {
        TableColumn annotation = property.column();
        Integer temporalPrecision = annotation == null
                ? null : optionalNonNegative(annotation.temporalPrecision());
        Integer numericPrecision = field.databaseType().isTemporal() ? null : field.precision();
        ColumnDefinition.Builder builder =
                ColumnDefinition.builder(field.columnName(), field.databaseType())
                        .codecId(property.mapping().id())
                        .nullable(field.nullable())
                        .length(field.length())
                        .precision(numericPrecision)
                        .scale(field.scale())
                        .temporalPrecision(temporalPrecision)
                        .generation(field.generation());
        if (annotation != null) {
            builder.defaultValue(defaultValue(annotation.defaultId()))
                    .comment(text(annotation.comment()))
                    .charset(text(annotation.charset()))
                    .collation(text(annotation.collation()));
        }
        return build("column", builder::build);
    }

    private static boolean nullable(
            TableColumn annotation,
            boolean primaryKey,
            boolean inherited) {
        if (annotation == null || annotation.nullable() == TableColumn.Nullability.INFER) {
            return primaryKey ? false : inherited;
        }
        if (primaryKey && annotation.nullable() == TableColumn.Nullability.NULLABLE) {
            throw new MappingException("primary key column must not be nullable");
        }
        return annotation.nullable() == TableColumn.Nullability.NULLABLE;
    }

    private static ValueGeneration generation(
            TableColumn annotation,
            EntityFieldMetadata field) {
        ValueGeneration inherited = field.generation();
        if (annotation == null || annotation.generation() == TableColumn.Generation.INFER) {
            return inherited;
        }
        ValueGeneration declared = annotation.generation() == TableColumn.Generation.IDENTITY
                ? ValueGeneration.identity() : ValueGeneration.none();
        // @TableId 决定主键值来源，列结构只能补充，不能把调用方生成改成数据库生成。
        if (field.primaryKey() && declared.generated() && field.idType() != IdType.AUTO) {
            throw new MappingException(
                    "TableColumn database generation conflicts with the TableId strategy");
        }
        if (inherited.generated() && !inherited.equals(declared)) {
            throw new MappingException(
                    "TableColumn generation conflicts with the existing TableId generation");
        }
        return declared;
    }

    private static ColumnDefault defaultValue(String defaultId) {
        String id = text(defaultId);
        if (id == null || "NONE".equalsIgnoreCase(id)) {
            return ColumnDefault.none();
        }
        return switch (id.toUpperCase(java.util.Locale.ROOT)) {
            case "CURRENT_DATE" -> ColumnDefault.currentDate();
            case "CURRENT_TIME" -> ColumnDefault.currentTime();
            case "CURRENT_TIMESTAMP" -> ColumnDefault.currentTimestamp();
            default -> throw new MappingException("unknown controlled column default id");
        };
    }

    private static void requireSameColumns(
            DynamicForm form,
            RelationalTableDefinition table) {
        List<String> formColumns = form.fields().stream()
                .map(field -> field.name()).toList();
        List<String> relationalColumns = table.columns().stream()
                .map(ColumnDefinition::name).toList();
        if (!formColumns.equals(relationalColumns)) {
            throw new IllegalStateException(
                    "entity CRUD and relational columns must stay aligned");
        }
    }

    private static Integer optionalPositive(int value) {
        if (value == -1) {
            return null;
        }
        if (value < 1) {
            throw new MappingException("column length must be positive or -1");
        }
        return value;
    }

    private static String tableComment(Class<?> entityType) {
        TableComment tableComment = entityType.getAnnotation(TableComment.class);
        return tableComment == null ? null : text(tableComment.value());
    }

    private static Integer optionalNonNegative(int value) {
        if (value == -1) {
            return null;
        }
        if (value < 0) {
            throw new MappingException(
                    "column numeric option must be non-negative or -1");
        }
        return value;
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

    record Property(
            Field field,
            EntityFieldMetadata metadata,
            EntityTypeMappingRegistry.Mapping mapping,
            TableColumn column) {
    }
}
