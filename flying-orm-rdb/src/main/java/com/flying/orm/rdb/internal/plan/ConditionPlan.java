package com.flying.orm.rdb.internal.plan;

import java.util.Objects;

/**
 * 已编译条件结构中可跨请求复用的部分，只保存参数化 SQL 与参数槽数量，不保存任何业务参数值。
 *
 * @param sql 参数化条件 SQL
 * @param parameterCount 参数槽数量
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record ConditionPlan(String sql, int parameterCount) {

    /** 校验结构计划自身一致性。 */
    public ConditionPlan {
        sql = Objects.requireNonNull(sql, "condition plan sql must not be null");
        if (parameterCount < 0) {
            throw new IllegalArgumentException("condition plan parameter count must not be negative");
        }
    }
}
