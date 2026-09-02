package com.flying.orm.core.condition;

import com.flying.orm.core.internal.Names;

/**
 * 简单 term 处理器仅保存 term id，用于在规划器实现前建立可扩展条件注册能力。
 *
 * @param id    term id
 * @param shape term 接受的值形状
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
record SimpleTermHandler(String id, ConditionValueShape shape) implements TermHandler {

    public SimpleTermHandler(String id) {
        this(id, ConditionValueShape.SCALAR);
    }

    /**
     * 创建简单 term 处理器并校验 id。
     *
     * @param id term id
     */
    public SimpleTermHandler {
        id = Names.key(id, "term id");
        shape = java.util.Objects.requireNonNull(shape, "term value shape must not be null");
    }
}
