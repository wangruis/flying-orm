package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.annotation.EncryptedField;
import com.flying.orm.core.annotation.KeySequence;
import com.flying.orm.core.annotation.MaskedField;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableLogic;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.Version;

import java.lang.reflect.Field;
import java.util.Optional;

/**
 * 集中读取 flying-orm 自己的实体注解。
 *
 * <p>这里使用强类型访问，而不是按类名反射。core 的注解属性一旦调整，编译期会直接报错；
 * V2 生产映射链也不会因为类路径上存在 Jakarta Persistence 而悄悄改变行为。</p>
 */
final class FlyingAnnotationReader {

    private FlyingAnnotationReader() {
    }

    static Optional<TableName> tableName(Class<?> type) {
        return Optional.ofNullable(type.getAnnotation(TableName.class));
    }

    static Optional<TableField> tableField(Field field) {
        return Optional.ofNullable(field.getAnnotation(TableField.class));
    }

    static Optional<TableId> tableId(Field field) {
        return Optional.ofNullable(field.getAnnotation(TableId.class));
    }

    static Optional<EncryptedField> encryptedField(Field field) {
        return Optional.ofNullable(field.getAnnotation(EncryptedField.class));
    }

    static Optional<MaskedField> maskedField(Field field) {
        return Optional.ofNullable(field.getAnnotation(MaskedField.class));
    }

    static boolean version(Field field) {
        return field.isAnnotationPresent(Version.class);
    }

    static Optional<TableLogic> tableLogic(Field field) {
        return Optional.ofNullable(field.getAnnotation(TableLogic.class));
    }

    static Optional<KeySequence> keySequence(Class<?> type) {
        return Optional.ofNullable(type.getAnnotation(KeySequence.class));
    }

    static String text(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
