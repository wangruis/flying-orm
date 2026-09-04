package com.flying.orm.core.condition;

import java.util.Objects;
import java.util.Optional;

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
     * 返回可配置查询使用的显式扩展契约。空值保留既有 trusted startup extension 语义。
     */
    default Optional<TermExtensionDescriptor> descriptor() {
        return Optional.empty();
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

    /** 创建带治理描述器的条件 term；稳定 id 直接取自描述器，避免两份名称漂移。 */
    static TermHandler described(TermExtensionDescriptor descriptor, ConditionValueShape shape) {
        return new DescribedTermHandler(
                Objects.requireNonNull(descriptor, "term extension descriptor must not be null"),
                Objects.requireNonNull(shape, "term value shape must not be null"));
    }
}
