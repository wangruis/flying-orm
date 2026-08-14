package com.flying.orm.core.sql.render;

import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.TermHandler;
import com.flying.orm.core.condition.TermRegistry;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * SQL term 注册表保存 term id 到 SQL handler 的映射，支持业务 term 的可插拔渲染。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public final class SqlTermRegistry {

    private final Map<String, SqlTermHandler> handlersById;

    private final TermRegistry conditionTerms;

    private SqlTermRegistry(List<SqlTermHandler> handlers) {
        Map<String, SqlTermHandler> indexedHandlers = new LinkedHashMap<>(RenderNames.mapCapacity(handlers.size()));
        TermRegistry.Builder conditionTerms = TermRegistry.builder();
        for (SqlTermHandler handler : handlers) {
            SqlTermHandler safeHandler = Objects.requireNonNull(handler, "sql term handler must not be null");
            String normalizedId = RenderNames.normalize(safeHandler.id(), "sql term id");
            ConditionValueShape shape = Objects.requireNonNull(safeHandler.shape(),
                                                               "sql term value shape must not be null");
            SqlTermHandler previous = indexedHandlers.putIfAbsent(normalizedId, safeHandler);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate sql term id");
            }
            // handler 同时声明值形状，条件校验和 SQL 渲染从此共用同一份注册信息。
            conditionTerms.add(TermHandler.simple(safeHandler.id(), shape));
        }
        handlersById = Map.copyOf(indexedHandlers);
        this.conditionTerms = conditionTerms.build();
    }

    /**
     * 创建 SQL term 注册表构建器。
     *
     * @return SQL term 注册表构建器
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 按 term id 查找 SQL handler。
     *
     * @param id term id
     * @return 匹配 handler；不存在时返回空
     */
    public Optional<SqlTermHandler> find(String id) {
        return Optional.ofNullable(handlersById.get(RenderNames.normalize(id, "sql term id")));
    }

    /**
     * 按 term id 获取 SQL handler，不存在时抛出确定性异常。
     *
     * @param id term id
     * @return SQL term handler
     */
    public SqlTermHandler handler(String id) {
        return find(id).orElseThrow(() -> new IllegalArgumentException("sql term does not exist"));
    }

    /**
     * 返回与 SQL handler 完全对应的条件值形状注册表，避免 DSL 和渲染器分别维护 operator 规则。
     */
    TermRegistry conditionTerms() {
        return conditionTerms;
    }

    /**
     * SQL term 注册表构建器。
     *
     * @author wangr
     * @date 2026-07-21
     * @version v1.0
     */
    public static final class Builder {

        private final List<SqlTermHandler> handlers = new ArrayList<>();

        private Builder() {
        }

        /**
         * 添加 SQL term handler。
         *
         * @param handler SQL term handler
         * @return 当前构建器
         */
        public Builder add(SqlTermHandler handler) {
            handlers.add(Objects.requireNonNull(handler, "sql term handler must not be null"));
            return this;
        }

        /**
         * 构建 SQL term 注册表。
         *
         * @return SQL term 注册表
         */
        public SqlTermRegistry build() {
            return new SqlTermRegistry(handlers);
        }
    }
}
