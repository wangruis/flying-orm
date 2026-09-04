package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.FieldFill;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.OrderBy;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableLogic;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.mapping.EntityEnumStorage;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.FlyingLogicDelete;

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
                                       EntityNamingStrategy namingStrategy,
                                       Optional<FlyingLogicDelete> classLogicDelete,
                                       boolean deferLogicDeleteLiteralDecoding) {
        Optional<TableField> flyingField = FlyingAnnotationReader.tableField(field);
        Optional<TableId> flyingId = FlyingAnnotationReader.tableId(field);
        String columnName = columnName(field, namingStrategy, flyingId, flyingField);
        Optional<LogicDeleteValues> logicDelete = logicDelete(field, columnName, classLogicDelete);
        EntityFieldTypeResolver.EnumValueDefinition enumValue = EntityFieldTypeResolver.enumValue(field);
        EntityEnumStorage enumStorage = EntityFieldTypeResolver.enumStorage(field, enumValue);
        boolean primaryKey = isPrimaryKey(field);
        ValueGeneration generation = EntityValueGenerationResolver.resolve(entityType, field);
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

        return new EntityFieldMetadata(field.getName(),
                                       columnName,
                                       DatabaseType.of(EntityFieldTypeResolver.dataType(field, enumStorage, enumValue)),
                                       primaryKey,
                                       isVersion(field),
                                       logicDelete.isPresent(),
                                       logicDelete.map(LogicDeleteValues::notDeletedValue)
                                                  .map(value -> logicDeleteValue(
                                                          value, field.getType(), deferLogicDeleteLiteralDecoding))
                                                  .orElse(null),
                                       logicDelete.map(LogicDeleteValues::deletedValue)
                                                  .map(value -> logicDeleteValue(
                                                          value, field.getType(), deferLogicDeleteLiteralDecoding))
                                                  .orElse(null),
                                       null,
                                       null,
                                       null,
                                       generation,
                                       idType(field, primaryKey, generation),
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
                                       updateStrategy != FieldStrategy.NEVER,
                                       true,
                                       false);
    }

    private static Object logicDeleteValue(String literal,
                                           Class<?> fieldType,
                                           boolean deferLiteralDecoding) {
        // 严格 descriptor 还要先解析 EntityTypeMappingRegistry，原始文本必须留到映射确定后再交给它的 codec。
        // 普通 CRUD 没有这份显式映射，继续沿用原来的内置类型转换，既有实体语义不会改变。
        return deferLiteralDecoding ? literal : EntityFieldTypeResolver.typedValue(literal, fieldType);
    }

    private static String columnName(Field field,
                                     EntityNamingStrategy namingStrategy,
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
        return namingStrategy.columnName(field.getName());
    }

    private static boolean isPrimaryKey(Field field) {
        return FlyingAnnotationReader.tableId(field).isPresent();
    }

    private static IdType idType(Field field, boolean primaryKey, ValueGeneration generation) {
        if (!primaryKey) {
            return IdType.NONE;
        }
        return FlyingAnnotationReader.tableId(field).map(TableId::type).orElse(IdType.NONE);
    }

    private static boolean isVersion(Field field) {
        return FlyingAnnotationReader.version(field);
    }

    private static Optional<LogicDeleteValues> logicDelete(Field field,
                                                            String columnName,
                                                            Optional<FlyingLogicDelete> classLogicDelete) {
        Optional<TableLogic> tableLogic = FlyingAnnotationReader.tableLogic(field);
        if (tableLogic.isPresent()) {
            TableLogic definition = tableLogic.orElseThrow();
            return Optional.of(new LogicDeleteValues(definition.value(), definition.delval()));
        }
        Optional<FlyingLogicDelete> fieldLogicDelete = Optional.ofNullable(field.getAnnotation(FlyingLogicDelete.class));
        if (fieldLogicDelete.isPresent()) {
            FlyingLogicDelete definition = fieldLogicDelete.orElseThrow();
            return Optional.of(new LogicDeleteValues(definition.notDeletedValue(), definition.deletedValue()));
        }
        return classLogicDelete.filter(annotation -> EntityFieldNames.matches(annotation.field(), field.getName())
                        || EntityFieldNames.matches(annotation.field(), columnName))
                               .map(annotation -> new LogicDeleteValues(annotation.notDeletedValue(),
                                                                         annotation.deletedValue()));
    }

    /** 统一成普通值，避免后续元数据代码继续依赖旧的 RDB 注解对象。 */
    private record LogicDeleteValues(String notDeletedValue, String deletedValue) {
    }
}
