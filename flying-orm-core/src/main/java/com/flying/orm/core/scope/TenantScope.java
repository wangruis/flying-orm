package com.flying.orm.core.scope;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.internal.condition.ConditionValueNormalizer;
import com.flying.orm.core.internal.condition.ConditionValuePolicy;

import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.IdentityHashMap;

/**
 * 租户字段范围。上层算出当前租户是谁，flying-orm 只负责把它安全拼进条件里。
 *
 * @param field 租户字段名
 * @param value 租户值
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public record TenantScope(String field, Object value) {

    public TenantScope {
        field = requireText(field, "tenant field");
        value = ConditionValueNormalizer.normalize(ConditionValueShape.SCALAR,
                                                   value,
                                                   ConditionValuePolicy.REJECT_EMPTY)
                                        .value();
        value = copyArray(value);
    }

    public static TenantScope of(String field, Object value) {
        return new TenantScope(field, value);
    }

    @Override
    public Object value() {
        return copyArray(value);
    }

    public ConditionGroup toCondition() {
        return ConditionGroup.and().where(field, "=", value).build();
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.strip();
    }

    private static Object copyArray(Object value) {
        if (value == null || !value.getClass().isArray()) {
            return value;
        }
        IdentityHashMap<Object, Object> copies = new IdentityHashMap<>();
        Object rootCopy = Array.newInstance(value.getClass().getComponentType(), Array.getLength(value));
        copies.put(value, rootCopy);
        ArrayDeque<Object> sources = new ArrayDeque<>();
        ArrayDeque<Object> targets = new ArrayDeque<>();
        sources.addLast(value);
        targets.addLast(rootCopy);
        while (!sources.isEmpty()) {
            Object source = sources.removeFirst();
            Object target = targets.removeFirst();
            int length = Array.getLength(source);
            if (source.getClass().getComponentType().isPrimitive()) {
                System.arraycopy(source, 0, target, 0, length);
                continue;
            }
            for (int index = 0; index < length; index++) {
                Object item = Array.get(source, index);
                if (item == null || !item.getClass().isArray()) {
                    Array.set(target, index, item);
                    continue;
                }
                Object itemCopy = copies.get(item);
                if (itemCopy == null) {
                    itemCopy = Array.newInstance(item.getClass().getComponentType(), Array.getLength(item));
                    copies.put(item, itemCopy);
                    sources.addLast(item);
                    targets.addLast(itemCopy);
                }
                Array.set(target, index, itemCopy);
            }
        }
        return rootCopy;
    }
}
