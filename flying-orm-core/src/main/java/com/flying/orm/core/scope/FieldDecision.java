package com.flying.orm.core.scope;

import com.flying.orm.core.field.FieldIdentity;

import java.util.Objects;

/**
 * 对一个字段、一个用途和一个来源作出的只读批准结果。
 *
 * <p>拒绝项和所有 INTERNAL_* 项的 visibility 必须是 HIDDEN。这样内部租户条件、逻辑删除、
 * 版本列或 keyset tie-breaker 即使获准参与 SQL，也不会反向成为可发布结果。</p>
 *
 * @param field 规范化后的字段键
 * @param use 字段用途
 * @param origin 用途来源
 * @param allowed 是否同时通过 policy 与 FieldScope 边界
 * @param visibility 本次调用可发布的最高显示级别
 * @author wangr
 * @version v3.2
 */
public record FieldDecision(String field,
                            FieldUse use,
                            FieldUseOrigin origin,
                            boolean allowed,
                            FieldVisibility visibility) {

    public FieldDecision {
        field = FieldIdentity.of(field).key();
        use = Objects.requireNonNull(use, "field use must not be null");
        origin = Objects.requireNonNull(origin, "field use origin must not be null");
        visibility = Objects.requireNonNull(visibility, "field visibility must not be null");
        if ((!allowed || origin.internal()) && visibility != FieldVisibility.HIDDEN) {
            throw new IllegalArgumentException("denied or internal field use must remain hidden");
        }
    }

    /** @return true 表示这项要求被拒绝 */
    public boolean denied() {
        return !allowed;
    }
}
