package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.internal.condition.ConditionExecutionView;
import com.flying.orm.core.internal.condition.ConditionExecutionViews;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.cache.BoundedCacheRegion;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.cache.OrmCacheSnapshot;

import java.util.List;
import java.util.Objects;

/** 缓存条件 SQL 结构，并为当前 AST 提取本次请求参数。 */
final class ConditionPlanCache {

    private final BoundedCacheRegion<Key, ConditionPlan> plans;
    private final boolean enabled;

    private ConditionPlanCache(CacheRegionPolicy policy) {
        CacheRegionPolicy safePolicy = Objects.requireNonNull(
                policy, "condition plan cache policy must not be null");
        enabled = safePolicy.enabled();
        plans = BoundedCacheRegion.create(
                safePolicy,
                ConditionPlanCache::weight);
    }

    static ConditionPlanCache create(CacheRegionPolicy policy) {
        return new ConditionPlanCache(policy);
    }

    ConditionStructurePlan condition(String dialect,
                                     ConditionGroup where,
                                     SqlRenderer renderer) {
        String safeDialect = requireText(dialect, "condition cache dialect");
        ConditionGroup safeWhere = Objects.requireNonNull(where, "where condition must not be null");
        SqlRenderer safeRenderer = Objects.requireNonNull(renderer, "condition renderer must not be null");
        ConditionExecutionView view = enabled ? ConditionExecutionViews.of(safeWhere) : null;
        if (view == null || !view.cacheable(safeRenderer.standardConditionTermMask())) {
            SqlFragment rendered = safeRenderer.renderWhere(safeWhere);
            List<Object> parameters = OwnedBindableValues.ownedValues(rendered.parameters());
            ConditionPlan plan = new ConditionPlan(rendered.sql(), parameters.size());
            return new ConditionStructurePlan(plan, parameters, "", false);
        }
        Key key = new Key(safeDialect, safeRenderer, view.shapeDigest());
        FirstRender firstRender = new FirstRender();
        ConditionPlan plan = plans.get(key, ignored -> {
            SqlFragment rendered = safeRenderer.renderWhere(safeWhere);
            List<Object> parameters = OwnedBindableValues.ownedValues(rendered.parameters());
            firstRender.parameters = parameters;
            return new ConditionPlan(rendered.sql(), parameters.size());
        });
        List<Object> parameters = firstRender.parameters;
        return new ConditionStructurePlan(
                plan,
                parameters == null
                        ? ConditionExecutionViews.bindParameters(safeWhere, safeRenderer.valueCodecs())
                        : parameters,
                view.shapeDigest(),
                true);
    }

    void invalidateAll() {
        plans.invalidateAll();
    }

    OrmCacheSnapshot snapshot() {
        return plans.snapshot();
    }

    private static int weight(Key key, ConditionPlan plan) {
        String sql = plan.sql();
        int placeholders = 0;
        for (int index = 0; index < sql.length(); index++) {
            if (sql.charAt(index) == '?') {
                placeholders++;
            }
        }
        return 2 + dividedLength(sql.length(), 32) + placeholders;
    }

    private static int dividedLength(int length, int divisor) {
        return Math.max(1, (length + divisor - 1) / divisor);
    }

    /** Mutable holder used only by the synchronous Caffeine mapping function of this invocation. */
    private static final class FirstRender {
        private List<Object> parameters;
    }

    private static String requireText(String text, String name) {
        String safeText = Objects.requireNonNull(text, name + " must not be null");
        if (safeText.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return safeText;
    }

    private record Key(String dialect, SqlRenderer renderer, String shape) {

        private Key {
            dialect = requireText(dialect, "condition cache dialect");
            renderer = Objects.requireNonNull(renderer, "condition cache renderer must not be null");
            shape = Objects.requireNonNull(shape, "condition shape must not be null");
        }
    }
}
