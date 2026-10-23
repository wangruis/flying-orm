package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.FieldFill;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.type.DatabaseType;

import java.lang.reflect.Type;
import java.util.Objects;

/**
 * 实体属性及其读写声明；物理列事实由共享的动态字段持有。
 *
 * @param name Java 属性名
 * @param javaType Java 属性类型，保留集合元素等泛型信息
 * @param field 与表单、Schema 共用的字段定义
 * @author wangr
 * @version v1.0
 */
public record EntityFieldMetadata(String name,
                                  Type javaType,
                                  DynamicField field,
                                  EntityTypeMappingRegistry.Mapping typeMapping,
                                  boolean version,
                                  boolean logicDelete,
                                  Object logicNotDeletedValue,
                                  Object logicDeletedValue,
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
                                  boolean updatable) {

    public EntityFieldMetadata {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("entity field name must not be blank");
        }
        name = name.trim();
        javaType = Objects.requireNonNull(javaType, "entity field Java type must not be null");
        field = Objects.requireNonNull(field, "entity dynamic field must not be null");
        idType = Objects.requireNonNull(idType, "entity field id type must not be null");
        enumStorage = Objects.requireNonNull(enumStorage, "entity enum storage must not be null");
        fill = Objects.requireNonNull(fill, "entity field fill must not be null");
        insertStrategy = Objects.requireNonNull(insertStrategy, "entity insert strategy must not be null");
        updateStrategy = Objects.requireNonNull(updateStrategy, "entity update strategy must not be null");
        enumValueMember = enumValueMember == null || enumValueMember.isBlank() ? null : enumValueMember.trim();
        if (!field.primaryKey() && idType != IdType.NONE) {
            throw new IllegalArgumentException("only a primary key field may declare an id type");
        }
    }

    /** 显式业务转换绑定；普通字段沿用执行链的标准 codec。 */
    public EntityTypeMappingRegistry.Mapping customMapping() {
        return typeMapping != null && typeMapping.custom() ? typeMapping : null;
    }

    public String columnName() { return field.name(); }
    public DatabaseType databaseType() { return field.databaseType(); }
    public String dataType() { return field.dataType(); }
    public boolean primaryKey() { return field.primaryKey(); }
    public Integer length() { return field.length(); }
    public Integer precision() { return field.precision(); }
    public Integer scale() { return field.scale(); }
    public ValueGeneration generation() { return field.generation(); }
    public boolean nullable() { return field.nullable(); }
    public boolean unique() { return field.unique(); }
}
