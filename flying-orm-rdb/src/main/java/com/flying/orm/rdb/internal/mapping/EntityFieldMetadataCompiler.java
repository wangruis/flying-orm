package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.FieldFill;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.KeySequence;
import com.flying.orm.core.annotation.OrderBy;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableLogic;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.mapping.EntityEnumStorage;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.mapping.MappingException;

import java.lang.reflect.Field;
import java.util.Optional;

/**
 * 把单个持久化字段编译成不可变元数据。
 *
 * <p>V2 只读取 flying-orm 自有注解和 Java 命名约定。这样一个字段不会因为类路径上偶然存在另一套
 * 持久化注解而改变 SQL，启动时看到的元数据就是执行时真正使用的元数据。</p>
 */
final class EntityFieldMetadataCompiler {

    private EntityFieldMetadataCompiler() {
    }

    static EntityFieldMetadata compile(Class<?> entityType,
                                       Field field,
                                       Optional<TableLogic> classLogicDelete,
                                       EntityTypeMappingRegistry mappings) {
        Optional<TableField> flyingField = FlyingAnnotationReader.tableField(field);
        Optional<TableId> flyingId = FlyingAnnotationReader.tableId(field);
        String columnName = columnName(field, flyingId, flyingField);
        Optional<TableLogic> logicDelete = logicDelete(field, columnName, classLogicDelete);
        Class<?> javaType = EntityMetadataHierarchy.memberClass(entityType, field.getGenericType());
        EntityFieldTypeResolver.EnumValueDefinition enumValue = EntityFieldTypeResolver.enumValue(javaType);
        EntityEnumStorage enumStorage = EntityFieldTypeResolver.enumStorage(javaType, enumValue);
        Class<?> logicDeleteType = enumValue.valueType() == null ? javaType : enumValue.valueType();
        TableColumn column = field.getAnnotation(TableColumn.class);
        EntityTypeMappingRegistry.Mapping mapping = mapping(field, logicDeleteType, column, mappings);
        boolean primaryKey = flyingId.isPresent();
        ValueGeneration generation = valueGeneration(entityType, flyingId);
        boolean selectable = flyingField.map(TableField::select).orElse(true);
        OrderBy orderBy = field.getAnnotation(OrderBy.class);
        if (orderBy != null && orderBy.sort() < 0) {
            throw new IllegalArgumentException("@OrderBy sort must not be negative: " + field);
        }
        FieldStrategy insertStrategy = flyingField.map(TableField::insertStrategy).orElse(FieldStrategy.DEFAULT);
        // 主键只能参与定位和插入，不能因为实体里带着 id 就被放进 UPDATE SET。
        FieldStrategy updateStrategy = primaryKey
                ? FieldStrategy.NEVER
                : flyingField.map(TableField::updateStrategy).orElse(FieldStrategy.DEFAULT);

        IdType idType = flyingId.map(TableId::type).orElse(IdType.NONE);
        DynamicField dynamicField = column(columnName, column, mapping, primaryKey, generation, idType);
        return new EntityFieldMetadata(field.getName(),
                                       field.getGenericType(),
                                       dynamicField,
                                       mapping,
                                       isVersion(field),
                                       logicDelete.isPresent(),
                                       logicDelete.map(TableLogic::value)
                                                  .map(value -> logicDeleteValue(
                                                          value, logicDeleteType, mapping))
                                                  .orElse(null),
                                       logicDelete.map(TableLogic::delval)
                                                  .map(value -> logicDeleteValue(
                                                          value, logicDeleteType, mapping))
                                                  .orElse(null),
                                       idType,
                                       enumStorage,
                                       enumValue.memberName(),
                                       selectable,
                                       orderBy != null,
                                       orderBy == null || orderBy.asc(),
                                       orderBy == null ? 0 : orderBy.sort(),
                                       flyingField.map(TableField::fill).orElse(FieldFill.DEFAULT),
                                       insertStrategy,
                                       updateStrategy,
                                       insertStrategy != FieldStrategy.NEVER,
                                       updateStrategy != FieldStrategy.NEVER);
    }

    private static Object logicDeleteValue(String literal, Class<?> fieldType,
                                           EntityTypeMappingRegistry.Mapping mapping) {
        return mapping == null ? EntityFieldTypeResolver.typedValue(literal, fieldType)
                : mapping.codec().read(literal, mapping.javaType());
    }

    private static EntityTypeMappingRegistry.Mapping mapping(
            Field field, Class<?> storageType, TableColumn column,
            EntityTypeMappingRegistry mappings) {
        String id = column == null ? null : FlyingAnnotationReader.text(column.databaseTypeId());
        EntityTypeMappingRegistry.Mapping mapping;
        try {
            mapping = id == null ? mappings.find(storageType).orElse(null) : mappings.resolve(id, storageType);
        } catch (MappingException error) {
            throw new MappingException("no entity type mapping for property " + field.getName()
                    + " with Java type " + storageType.getTypeName(), error);
        }
        return mapping;
    }

    private static DynamicField column(String name, TableColumn annotation,
                                       EntityTypeMappingRegistry.Mapping mapping, boolean primaryKey,
                                       ValueGeneration generation, IdType idType) {
        DatabaseType type = mapping == null ? DatabaseType.of("VARCHAR") : mapping.databaseType();
        ColumnDefinition.Builder column = ColumnDefinition.builder(name, type)
                .codecId(mapping == null ? ColumnDefinition.STANDARD_CODEC : mapping.id())
                .nullable(!primaryKey).generation(generation);
        if (annotation != null) {
            applyColumnOptions(column, type, annotation, primaryKey, generation, idType);
        }
        try {
            return new DynamicField(column.build(), primaryKey, false);
        } catch (IllegalArgumentException error) {
            throw new MappingException("entity column definition is invalid: " + name, error);
        }
    }

    private static void applyColumnOptions(ColumnDefinition.Builder column, DatabaseType type,
                                           TableColumn annotation, boolean primaryKey,
                                           ValueGeneration inherited, IdType idType) {
        Integer precision = optionalNonNegative(annotation.precision());
        Integer temporal = optionalNonNegative(annotation.temporalPrecision());
        if (type.logicalType().temporal()) {
            if (precision != null && temporal != null) {
                throw new MappingException("temporal column must not declare both precision and temporalPrecision");
            }
            column.temporalPrecision(temporal == null ? precision : temporal);
        } else {
            if (temporal != null) {
                throw new MappingException("temporalPrecision requires a temporal database type");
            }
            column.precision(precision).scale(optionalNonNegative(annotation.scale()));
        }
        if (annotation.nullable() != TableColumn.Nullability.INFER) {
            boolean nullable = annotation.nullable() == TableColumn.Nullability.NULLABLE;
            if (primaryKey && nullable) {
                throw new MappingException("primary key column must not be nullable");
            }
            column.nullable(nullable);
        }
        if (annotation.generation() != TableColumn.Generation.INFER) {
            ValueGeneration declared = annotation.generation() == TableColumn.Generation.IDENTITY
                    ? ValueGeneration.identity() : ValueGeneration.none();
            if (primaryKey && declared.generated() && idType != IdType.AUTO) {
                throw new MappingException("TableColumn database generation conflicts with the TableId strategy");
            }
            if (inherited.generated() && !inherited.equals(declared)) {
                throw new MappingException("TableColumn generation conflicts with the existing TableId generation");
            }
            column.generation(declared);
        }
        column.length(annotation.length() == -1 ? null : annotation.length())
                .defaultValue(defaultValue(annotation.defaultId()))
                .comment(FlyingAnnotationReader.text(annotation.comment()))
                .charset(FlyingAnnotationReader.text(annotation.charset()))
                .collation(FlyingAnnotationReader.text(annotation.collation()));
    }

    private static ColumnDefault defaultValue(String defaultId) {
        String id = FlyingAnnotationReader.text(defaultId);
        if (id == null || "NONE".equalsIgnoreCase(id)) return ColumnDefault.none();
        return switch (id.toUpperCase(java.util.Locale.ROOT)) {
            case "CURRENT_DATE" -> ColumnDefault.currentDate();
            case "CURRENT_TIME" -> ColumnDefault.currentTime();
            case "CURRENT_TIMESTAMP" -> ColumnDefault.currentTimestamp();
            default -> throw new MappingException("unknown controlled column default id");
        };
    }

    private static Integer optionalNonNegative(int value) {
        if (value == -1) return null;
        if (value < 0) throw new MappingException("column numeric option must be non-negative or -1");
        return value;
    }

    static String columnName(Field field) {
        return columnName(field, FlyingAnnotationReader.tableId(field), FlyingAnnotationReader.tableField(field));
    }

    private static String columnName(Field field,
                                     Optional<TableId> flyingId,
                                     Optional<TableField> flyingField) {
        String explicitIdColumn = flyingId.map(TableId::value).map(FlyingAnnotationReader::text).orElse(null);
        if (explicitIdColumn != null) {
            return explicitIdColumn;
        }
        String explicitFieldColumn = flyingField.map(TableField::value)
                                                .map(FlyingAnnotationReader::text)
                                                .orElse(null);
        if (explicitFieldColumn != null) {
            return explicitFieldColumn;
        }
        return EntityFieldNames.snakeCase(field.getName());
    }

    private static ValueGeneration valueGeneration(Class<?> entityType, Optional<TableId> flyingId) {
        if (flyingId.isEmpty()) {
            return ValueGeneration.none();
        }
        Optional<ValueGeneration> sequence = FlyingAnnotationReader.keySequence(entityType)
                .map(KeySequence::value)
                .map(FlyingAnnotationReader::text)
                .map(ValueGeneration::sequence);
        if (sequence.isPresent()) {
            return sequence.orElseThrow();
        }
        // NONE 按安全的 INPUT 处理；ASSIGN_ID/ASSIGN_UUID 仍由 Repository 的 SQL 前阶段处理。
        return flyingId.orElseThrow().type() == IdType.AUTO
                ? ValueGeneration.identity() : ValueGeneration.none();
    }

    private static boolean isVersion(Field field) {
        return FlyingAnnotationReader.version(field);
    }

    private static Optional<TableLogic> logicDelete(Field field,
                                                            String columnName,
                                                            Optional<TableLogic> classLogicDelete) {
        Optional<TableLogic> tableLogic = FlyingAnnotationReader.tableLogic(field);
        return tableLogic.isPresent() ? tableLogic : classLogicDelete.filter(
                annotation -> EntityFieldNames.matches(annotation.field(), field.getName())
                        || EntityFieldNames.matches(annotation.field(), columnName));
    }
}
