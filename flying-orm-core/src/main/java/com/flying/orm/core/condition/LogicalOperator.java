package com.flying.orm.core.condition;

/**
 * 条件组逻辑操作符，用于表达同一层条件之间的连接关系。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public enum LogicalOperator {

    /**
     * 所有子条件都需要满足。
     */
    AND,

    /**
     * 任一子条件满足即可。
     */
    OR
}
