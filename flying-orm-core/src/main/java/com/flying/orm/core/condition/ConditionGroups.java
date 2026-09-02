package com.flying.orm.core.condition;

import java.util.Objects;

/**
 * 条件组合的小工具。
 *
 * <p>安全范围、逻辑删除、业务 where 最后都要用 AND 收窄。这里统一处理 OR 条件，避免
 * {@code tenant_id = ?} 只约束到某个 OR 分支。</p>
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public final class ConditionGroups {

    private ConditionGroups() {
    }

    public static ConditionGroup and(ConditionGroup first, ConditionGroup second) {
        ConditionGroup safeFirst = Objects.requireNonNull(first, "first condition group must not be null");
        ConditionGroup safeSecond = Objects.requireNonNull(second, "second condition group must not be null");
        ConditionGroup.Builder builder = ConditionGroup.and();
        addAsAnd(builder, safeFirst);
        addAsAnd(builder, safeSecond);
        return builder.build();
    }

    public static boolean isEmpty(ConditionGroup group) {
        return Objects.requireNonNull(group, "condition group must not be null").children().isEmpty();
    }

    private static void addAsAnd(ConditionGroup.Builder builder, ConditionGroup group) {
        if (group.children().isEmpty()) {
            return;
        }
        if (group.operator() == LogicalOperator.AND) {
            for (ConditionNode child : group.children()) {
                builder.add(child);
            }
            return;
        }
        builder.add(group);
    }
}
