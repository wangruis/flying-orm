package com.flying.orm.core.join;

import java.util.Objects;

/**
 * ON 子句中的字段等值关系。
 *
 * @param left 已加入数据源的左字段
 * @param right 当前连接源的右字段
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public record JoinFieldPair(JoinFieldRef left, JoinFieldRef right) {

    /** 拒绝空字段和同一数据源内部比较。 */
    public JoinFieldPair {
        left = Objects.requireNonNull(left, "join left field must not be null");
        right = Objects.requireNonNull(right, "join right field must not be null");
        if (left.source().equals(right.source())) {
            throw new IllegalArgumentException("join fields must belong to different sources");
        }
    }
}
