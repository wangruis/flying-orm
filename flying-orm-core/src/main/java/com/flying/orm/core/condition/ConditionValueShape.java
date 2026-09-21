package com.flying.orm.core.condition;

/**
 * term 需要的值形状。SQL handler 只处理符合这里约定的值。
 *
 * @author wangr
 * @date 2026-07-31
 * @version v1.0
 */
public enum ConditionValueShape {
    /** 不需要值，例如 is-null 和 is-not-null。 */
    NONE,

    /** 只接收一个普通值，例如 =、>、like。 */
    SCALAR,

    /** 接收集合或数组，例如 in 和 not-in。 */
    COLLECTION,

    /** 接收两个有顺序的边界值，例如 between。 */
    RANGE,

    /**
     * 明确允许单值或集合，例如关系存在条件既可以查一个机构，也可以查一组机构。
     * 这个形状不能作为未知 term 的兜底，只有 term 主动声明后才能使用。
     */
    SCALAR_OR_COLLECTION
}
