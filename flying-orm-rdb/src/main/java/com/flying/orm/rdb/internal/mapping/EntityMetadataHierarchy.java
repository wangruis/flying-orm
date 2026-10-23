package com.flying.orm.rdb.internal.mapping;


import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * 解析实体继承层次，并筛掉不会持久化的字段。
 *
 * @author wangr
 * @version v3.3
 */
public final class EntityMetadataHierarchy {

    private EntityMetadataHierarchy() {
    }

    /** 在模型或访问计划编译时解析成员实际类型；继承变量只沿其声明层次查找。 */
    public static Class<?> memberClass(Class<?> entityType, Type memberType) {
        if (memberType instanceof Class<?> type) return type;
        if (memberType instanceof ParameterizedType type) return (Class<?>) type.getRawType();
        if (memberType instanceof GenericArrayType array) {
            return memberClass(entityType, array.getGenericComponentType()).arrayType();
        }
        TypeVariable<?> variable = (TypeVariable<?>) memberType;
        if (variable.getGenericDeclaration() instanceof Class<?> owner) {
            for (Class<?> current = entityType; current != owner;) {
                Type parent = genericParent(current, owner);
                if (parent == null) break;
                current = memberClass(entityType, parent);
                if (current == owner && parent instanceof ParameterizedType parameterized) {
                    TypeVariable<?>[] variables = owner.getTypeParameters();
                    for (int index = 0; index < variables.length; index++) {
                        if (variables[index].equals(variable)) {
                            return memberClass(entityType, parameterized.getActualTypeArguments()[index]);
                        }
                    }
                }
            }
        }
        return memberClass(entityType, variable.getBounds()[0]);
    }

    private static Type genericParent(Class<?> type, Class<?> owner) {
        Type parent = type.getGenericSuperclass();
        if (parent != null && owner.isAssignableFrom(memberClass(type, parent))) return parent;
        for (Type candidate : type.getGenericInterfaces()) {
            if (owner.isAssignableFrom(memberClass(type, candidate))) return candidate;
        }
        return null;
    }

    static List<Class<?>> persistentTypes(Class<?> entityType) {
        List<Class<?>> hierarchy = new ArrayList<>();
        hierarchy.add(entityType);
        Class<?> parent = entityType.getSuperclass();
        while (parent != null && parent != Object.class) {
            // V2 不依赖 MappedSuperclass。普通 Java 继承字段遵守同一持久化规则；不入库字段显式用
            // transient 或 @TableField(exist=false) 排除，使用者不必再引入另一套注解体系。
            hierarchy.add(parent);
            parent = parent.getSuperclass();
        }
        Collections.reverse(hierarchy);
        return hierarchy;
    }

    /** 元数据、映射计划和主键回填共用同一套字段排除规则。 */
    public static boolean isPersistentField(Field field) {
        int modifiers = field.getModifiers();
        Optional<com.flying.orm.core.annotation.TableField> flyingField = FlyingAnnotationReader.tableField(field);
        return !field.isSynthetic()
                && !Modifier.isStatic(modifiers)
                && !Modifier.isTransient(modifiers)
                && flyingField.map(com.flying.orm.core.annotation.TableField::exist).orElse(true);
    }

    /** 读写计划共享元数据编译器的持久字段规则，不把被排除的同名继承字段重新纳入。 */
    public static List<Field> persistentFields(Class<?> entityType) {
        List<Field> fields = new ArrayList<>();
        for (Class<?> persistentType : persistentTypes(entityType)) {
            for (Field field : persistentType.getDeclaredFields()) {
                if (isPersistentField(field)) {
                    fields.add(field);
                }
            }
        }
        return fields;
    }

    /** 精确声明的非持久字段优先；没有同名字段时保留 JavaBean 的规范化命名兼容。 */
    public static boolean isPersistentAccessor(Method accessor, String propertyName) {
        for (Class<?> owner = accessor.getDeclaringClass();
                owner != null && owner != Object.class; owner = owner.getSuperclass()) {
            try {
                return isPersistentField(owner.getDeclaredField(propertyName));
            } catch (NoSuchFieldException ignored) {
                // 继续检查继承成员；不根据数据库列别名推断 Java 成员。
            }
        }
        return true;
    }
}
