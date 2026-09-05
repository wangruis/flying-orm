package com.flying.orm.core.scope;

import com.flying.orm.core.join.JoinFieldRef;

import java.util.Objects;

/**
 * 对一个来源限定 JOIN 字段作出的不可变用途决定。
 *
 * <p>字段名相同也不能合并：{@link JoinFieldRef} 中的数据源身份是授权事实的一部分，SQL 别名和
 * 结果别名都不参与授权。</p>
 *
 * @param field 来源限定字段
 * @param use 字段用途
 * @param origin 用途来源
 * @param allowed 是否同时通过 policy 与该来源的 FieldScope
 * @param visibility 本次调用可发布的最高显示级别
 * @author wangr
 * @version v3.3
 */
public record JoinFieldDecision(JoinFieldRef field,
                                FieldUse use,
                                FieldUseOrigin origin,
                                boolean allowed,
                                FieldVisibility visibility) {

    public JoinFieldDecision {
        field = Objects.requireNonNull(field, "join field reference must not be null");
        use = Objects.requireNonNull(use, "field use must not be null");
        origin = Objects.requireNonNull(origin, "field use origin must not be null");
        visibility = Objects.requireNonNull(visibility, "field visibility must not be null");
        if ((!allowed || origin.internal()) && visibility != FieldVisibility.HIDDEN) {
            throw new IllegalArgumentException("denied or internal join field use must remain hidden");
        }
    }

    public boolean denied() {
        return !allowed;
    }
}
