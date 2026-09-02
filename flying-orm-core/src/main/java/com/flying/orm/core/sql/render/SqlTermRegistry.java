package com.flying.orm.core.sql.render;

import com.flying.orm.core.internal.Names;

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
        Map<String, SqlTermHandler> indexedHandlers = new LinkedHashMap<>(Names.mapCapacity(handlers.size()));
        TermRegistry.Builder conditionTerms = TermRegistry.builder();
        for (SqlTermHandler handler : handlers) {
            SqlTermHandler safeHandler = Objects.requireNonNull(handler, "sql term handler must not be null");
            String normalizedId = Names.key(safeHandler.id(), "sql term id");
            ConditionValueShape shape = Objects.requireNonNull(safeHandler.shape(),
                                                               "sql term value shape must not be null");
            SqlTermHandler previous = indexedHandlers.putIfAbsent(normalizedId, safeHandler);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate sql term id");
            }
            // 内置 handler 复用 STANDARD 中的对象身份，执行层据此区分内置语义和同名自定义语义，
            // 不新增公开缓存能力，也不会把自定义 handler 误当成内置快速路径。
            TermHandler standard = TermRegistry.standard().find(normalizedId).orElse(null);
            boolean structuralCacheSafe = safeHandler instanceof SimpleSqlTermHandler simple
                    && simple.structuralCacheSafe()
                    && standard != null
                    && standard.shape() == shape;
            conditionTerms.add(structuralCacheSafe ? standard : TermHandler.simple(safeHandler.id(), shape));
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
        return Optional.ofNullable(handlersById.get(Names.key(id, "sql term id")));
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
