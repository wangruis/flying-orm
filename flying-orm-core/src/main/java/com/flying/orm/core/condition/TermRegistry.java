package com.flying.orm.core.condition;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * term 注册表保存内置和业务自定义条件处理器，使通用条件扩展不依赖硬编码分支。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public final class TermRegistry {

    private static final TermRegistry EMPTY = new TermRegistry(List.of());

    private static final TermRegistry STANDARD = new TermRegistry(List.of(
            TermHandler.simple("is-null", ConditionValueShape.NONE),
            TermHandler.simple("is-not-null", ConditionValueShape.NONE),
            TermHandler.simple("=", ConditionValueShape.SCALAR),
            TermHandler.simple("!=", ConditionValueShape.SCALAR),
            TermHandler.simple("<>", ConditionValueShape.SCALAR),
            TermHandler.simple(">", ConditionValueShape.SCALAR),
            TermHandler.simple(">=", ConditionValueShape.SCALAR),
            TermHandler.simple("<", ConditionValueShape.SCALAR),
            TermHandler.simple("<=", ConditionValueShape.SCALAR),
            TermHandler.simple("like", ConditionValueShape.SCALAR),
            TermHandler.simple("not-like", ConditionValueShape.SCALAR),
            TermHandler.simple("like-ignore-case", ConditionValueShape.SCALAR),
            TermHandler.simple("not-like-ignore-case", ConditionValueShape.SCALAR),
            TermHandler.simple("in", ConditionValueShape.COLLECTION),
            TermHandler.simple("not-in", ConditionValueShape.COLLECTION),
            TermHandler.simple("between", ConditionValueShape.RANGE),
            TermHandler.simple("not-between", ConditionValueShape.RANGE)));

    private final List<TermHandler> handlers;

    private final Map<String, TermHandler> handlersById;

    private TermRegistry(List<TermHandler> handlers) {
        List<TermHandler> copiedHandlers = List.copyOf(handlers);
        Map<String, TermHandler> indexedHandlers = new LinkedHashMap<>(ConditionNames.mapCapacity(copiedHandlers.size()));
        for (TermHandler handler : copiedHandlers) {
            TermHandler safeHandler = Objects.requireNonNull(handler, "term handler must not be null");
            String normalizedId = ConditionNames.normalize(safeHandler.id(), "term id");
            TermHandler previous = indexedHandlers.putIfAbsent(normalizedId, safeHandler);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate term id");
            }
        }
        this.handlers = copiedHandlers;
        this.handlersById = Map.copyOf(indexedHandlers);
    }

    /**
     * 返回空 term 注册表。
     *
     * @return 空注册表
     */
    public static TermRegistry empty() {
        return EMPTY;
    }

    /**
     * 返回内置 term 的不可变值形状注册表。
     */
    public static TermRegistry standard() {
        return STANDARD;
    }

    /**
     * 创建 term 注册表构建器。
     *
     * @return term 注册表构建器
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 返回只读 term 处理器集合。
     *
     * @return 只读 term 处理器集合
     */
    public List<TermHandler> handlers() {
        return handlers;
    }

    /**
     * 按规范化 term id 查找处理器。
     *
     * @param id term id
     * @return 匹配处理器；不存在时返回空
     */
    public Optional<TermHandler> find(String id) {
        return Optional.ofNullable(handlersById.get(ConditionNames.normalize(id, "term id")));
    }

    /**
     * 按规范化 term id 获取处理器，不存在时抛出确定性异常。
     *
     * @param id term id
     * @return 匹配处理器
     * @throws IllegalArgumentException term 不存在时抛出
     */
    public TermHandler handler(String id) {
        return find(id).orElseThrow(() -> new IllegalArgumentException("term does not exist"));
    }

    /**
     * term 注册表构建器，用于发布前收集处理器。
     *
     * @author wangr
     * @date 2026-07-21
     * @version v1.0
     */
    public static final class Builder {

        private final List<TermHandler> handlers = new ArrayList<>();

        private Builder() {
        }

        /**
         * 添加 term 处理器。
         *
         * @param handler term 处理器
         * @return 当前构建器
         */
        public Builder add(TermHandler handler) {
            handlers.add(Objects.requireNonNull(handler, "term handler must not be null"));
            return this;
        }

        /**
         * 构建只读 term 注册表。
         *
         * @return term 注册表
         */
        public TermRegistry build() {
            if (handlers.isEmpty()) {
                return EMPTY;
            }
            for (TermHandler handler : handlers) {
                String id = ConditionNames.normalize(handler.id(), "term id");
                ConditionValueShape shape = Objects.requireNonNull(handler.shape(),
                                                                   "term value shape must not be null");
                STANDARD.find(id).ifPresent(standard -> {
                    if (standard.shape() != shape) {
                        throw new IllegalArgumentException("standard term must use its declared value shape");
                    }
                });
            }
            return new TermRegistry(handlers);
        }
    }
}
