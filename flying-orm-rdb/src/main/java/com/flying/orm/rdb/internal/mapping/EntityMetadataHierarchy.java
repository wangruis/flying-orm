package com.flying.orm.rdb.internal.mapping;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** 解析实体继承层次，并筛掉不会持久化的字段。 */
final class EntityMetadataHierarchy {

    private EntityMetadataHierarchy() {
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

    static boolean isPersistentField(Field field) {
        int modifiers = field.getModifiers();
        Optional<com.flying.orm.core.annotation.TableField> flyingField = FlyingAnnotationReader.tableField(field);
        return !field.isSynthetic()
                && !Modifier.isStatic(modifiers)
                && !Modifier.isTransient(modifiers)
                && flyingField.map(com.flying.orm.core.annotation.TableField::exist).orElse(true);
    }
}
