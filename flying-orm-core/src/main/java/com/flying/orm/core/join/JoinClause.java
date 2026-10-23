package com.flying.orm.core.join;

import java.util.Objects;

/**
 * 单个连接源及其结构化 ON 条件。
 *
 * @param type 连接类型
 * @param source 本次新加入的数据源
 * @param condition 完整的字段比较、参数条件与逻辑分组
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public record JoinClause(JoinType type, JoinSource source, JoinCondition condition) {

    /** 创建不可变 ON 快照并拒绝空连接条件。 */
    public JoinClause {
        type = Objects.requireNonNull(type, "join type must not be null");
        source = Objects.requireNonNull(source, "joined source must not be null");
        condition = Objects.requireNonNull(condition, "join ON condition must not be null");
    }
}
