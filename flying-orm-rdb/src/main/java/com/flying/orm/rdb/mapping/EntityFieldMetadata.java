package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.FieldFill;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.ValueGeneration;

import java.util.Objects;

/**
 * 实体里的一个字段，对应动态表单里的一个字段。
 *
 * @param name       Java 字段名
 * @param columnName 数据库列名
 * @param dataType   flying-orm 逻辑类型
 * @param primaryKey 是否主键
 * @param version    是否乐观锁版本字段
 * @param logicDelete 是否逻辑删除字段
 * @param logicNotDeletedValue 未删除值
 * @param logicDeletedValue    已删除值
 * @param length     字符串长度
 * @param precision  数字精度
 * @param scale      数字小数位
 * @param generation 数据库生成值方式；普通字段为 NONE
 * @param enumStorage 显式枚举存储方式；非枚举或沿用名字约定时为 NONE
 * @param insertable  insert 时是否允许把这个字段交给数据库
 * @param updatable   update 时是否允许把这个字段放进 SET 列表
 * @param nullable    数据库列是否允许 null；当前先作为元数据和校验依据
 * @param unique      是否声明唯一约束；当前先保留语义，不偷偷改动已有表结构
 * @author wangr
 * @date 2026-07-30
 * @version v1.0
 */
public record EntityFieldMetadata(String name,
                                  String columnName,
                                  String dataType,
                                  boolean primaryKey,
                                  boolean version,
                                  boolean logicDelete,
                                  Object logicNotDeletedValue,
                                  Object logicDeletedValue,
                                  Integer length,
                                  Integer precision,
                                  Integer scale,
                                  ValueGeneration generation,
                                  IdType idType,
                                  EntityEnumStorage enumStorage,
                                  String enumValueMember,
                                  boolean selectable,
                                  boolean ordered,
                                  boolean orderAscending,
                                  int orderPriority,
                                  FieldFill fill,
                                  FieldStrategy insertStrategy,
                                  FieldStrategy updateStrategy,
                                  boolean insertable,
                                  boolean updatable,
                                  boolean nullable,
                                  boolean unique) {

    public EntityFieldMetadata {
        name = requireText(name, "entity field name");
        columnName = requireText(columnName, "entity column name");
        dataType = requireText(dataType, "entity field data type");
        generation = Objects.requireNonNull(generation, "entity field value generation must not be null");
        idType = Objects.requireNonNull(idType, "entity field id type must not be null");
        enumStorage = Objects.requireNonNull(enumStorage, "entity enum storage must not be null");
        fill = Objects.requireNonNull(fill, "entity field fill must not be null");
        insertStrategy = Objects.requireNonNull(insertStrategy, "entity insert strategy must not be null");
        updateStrategy = Objects.requireNonNull(updateStrategy, "entity update strategy must not be null");
        enumValueMember = optionalText(enumValueMember);
        if (!primaryKey && idType != IdType.NONE) {
            throw new IllegalArgumentException("only a primary key field may declare an id type");
        }
    }

    /**
     * 转成动态表单字段。动态表单只关心表结构，乐观锁语义留在实体元数据里。
     *
     * @return 动态表单字段
     */
    public DynamicField toDynamicField() {
        DynamicField field = primaryKey ? DynamicField.primaryKey(columnName, dataType) : DynamicField.of(columnName, dataType);
        if (length != null) {
            field = field.withLength(length);
        }
        if (precision != null) {
            field = field.withPrecision(precision, scale);
        }
        return field.withNullable(nullable)
                    .withUnique(unique)
                    .withGeneration(generation);
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }

    private static String optionalText(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
