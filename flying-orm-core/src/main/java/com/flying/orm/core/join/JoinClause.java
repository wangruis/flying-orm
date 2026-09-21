package com.flying.orm.core.join;

import java.util.List;
import java.util.Objects;

/**
 * 单个连接源及其复合等值 ON 条件。
 *
 * @param type 连接类型
 * @param source 本次新加入的数据源
 * @param on 有序字段等值条件
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public record JoinClause(JoinType type, JoinSource source, List<JoinFieldPair> on) {

    /** 创建不可变 ON 快照并拒绝空连接条件。 */
    public JoinClause {
        type = Objects.requireNonNull(type, "join type must not be null");
        source = Objects.requireNonNull(source, "joined source must not be null");
        on = List.copyOf(Objects.requireNonNull(on, "join ON conditions must not be null"));
        if (on.isEmpty()) {
            throw new IllegalArgumentException("join ON conditions must not be empty");
        }
        for (JoinFieldPair pair : on) {
            if (!pair.right().source().equals(source)) {
                throw new IllegalArgumentException("join ON right field must belong to the joined source");
            }
        }
    }
}
