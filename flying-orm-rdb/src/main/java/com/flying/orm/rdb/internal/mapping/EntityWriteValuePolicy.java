package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.annotation.FieldFill;
import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.rdb.mapping.EntityFieldFiller;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.MappingException;

/** 统一处理实体字段的填充时机和空值写入策略。 */
final class EntityWriteValuePolicy {

    private EntityWriteValuePolicy() {
    }

    static Object fill(Object entity,
                       EntityFieldMetadata field,
                       EntityFieldFiller.Operation operation,
                       Object currentValue,
                       EntityFieldFiller filler) {
        if (operation == null || !fills(field.fill(), operation)) {
            return currentValue;
        }
        try {
            return filler.fill(entity, field, operation, currentValue);
        } catch (RuntimeException error) {
            throw new MappingException("entity field fill failed: " + field.name(), error);
        }
    }

    static boolean accepts(FieldStrategy strategy, Object value) {
        return switch (strategy) {
            case DEFAULT, ALWAYS -> true;
            case NOT_NULL -> value != null;
            // 空白字符串仍是业务值，写入策略不会擅自 trim。
            case NOT_EMPTY -> value != null && (!(value instanceof CharSequence text) || !text.isEmpty());
            case NEVER -> false;
        };
    }

    private static boolean fills(FieldFill fill, EntityFieldFiller.Operation operation) {
        return switch (fill) {
            case DEFAULT -> false;
            case INSERT -> operation == EntityFieldFiller.Operation.INSERT
                    || operation == EntityFieldFiller.Operation.UPSERT;
            case UPDATE -> operation == EntityFieldFiller.Operation.UPDATE
                    || operation == EntityFieldFiller.Operation.UPSERT;
            case INSERT_UPDATE -> true;
        };
    }
}
