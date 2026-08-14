package com.flying.orm.core.condition;

/**
 * term 处理器描述一个可扩展通用条件的稳定标识，后续 SQL 规划阶段会基于该标识解析具体语义。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public interface TermHandler {

    /**
     * 返回 term id，例如 `=`、`like`、`user-in-org`。
     *
     * @return term id
     */
    String id();

    /**
     * 返回 term 接受的值形状。最常见的业务 term 默认按单值处理；集合、区间和无值条件要明确声明。
     */
    default ConditionValueShape shape() {
        return ConditionValueShape.SCALAR;
    }

    /**
     * 创建只携带 id 的简单 term 处理器，适合先注册业务 term 语义边界。
     *
     * @param id term id
     * @return term 处理器
     */
    static TermHandler simple(String id) {
        return new SimpleTermHandler(id);
    }

    /**
     * 创建带明确值形状的 term，适合集合、区间和无值业务条件。
     */
    static TermHandler simple(String id, ConditionValueShape shape) {
        return new SimpleTermHandler(id, shape);
    }
}
