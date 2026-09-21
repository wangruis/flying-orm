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

/** 描述器只在显式扩展对象上保存，标准 term 的简单对象布局保持不变。 */
record DescribedTermHandler(TermExtensionDescriptor extension,
                            ConditionValueShape shape) implements TermHandler {

    DescribedTermHandler {
        extension = java.util.Objects.requireNonNull(
                extension, "term extension descriptor must not be null");
        shape = java.util.Objects.requireNonNull(shape, "term value shape must not be null");
    }

    @Override
    public String id() {
        return extension.id();
    }

    @Override
    public java.util.Optional<TermExtensionDescriptor> descriptor() {
        return java.util.Optional.of(extension);
    }
}
