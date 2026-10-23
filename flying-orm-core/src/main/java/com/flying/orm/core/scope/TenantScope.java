package com.flying.orm.core.scope;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.internal.condition.ConditionValueNormalizer;
import com.flying.orm.core.internal.condition.ConditionValuePolicy;
import com.flying.orm.core.internal.value.BindableValueSnapshots;

import java.util.Objects;

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
                                                   ConditionValuePolicy.REJECT_EMPTY,
                                                   (scalar, index) -> scalar,
                                                   Integer.MAX_VALUE,
                                                   Integer.MAX_VALUE)
                                        .value();
        value = BindableValueSnapshots.logicalValue(value);
    }

    public static TenantScope of(String field, Object value) {
        return new TenantScope(field, value);
    }

    @Override
    public Object value() {
        return BindableValueSnapshots.logicalValue(value);
    }

    /** 包内范围合并读取构造时已经拥有的值，不触发公共访问器的隔离副本。 */
    Object ownedValue() {
        return value;
    }

    /** 按租户值语义比较，文本载体按内容匹配，数组按值匹配；不创建本范围值的快照。 */
    public boolean matches(Object candidate) {
        String expectedText = text(value);
        String candidateText = text(candidate);
        return expectedText != null && candidateText != null
                ? expectedText.equals(candidateText) : Objects.deepEquals(value, candidate);
    }

    private static String text(Object value) {
        return switch (value) {
            case CharSequence text -> text.toString();
            case Character character -> character.toString();
            case char[] characters -> new String(characters);
            case null, default -> null;
        };
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
}
