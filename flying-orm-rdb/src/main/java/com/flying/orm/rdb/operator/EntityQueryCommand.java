package com.flying.orm.rdb.operator;

import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.rdb.form.spec.QuerySpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 实体查询的执行方式无关计划。
 *
 * <p>投影、分组、排序和条件都在执行前归并成 {@link QuerySpec}。这样同步和响应式门面使用同一份
 * 字段验证与参数顺序，也能保证投影查询不会被误映射成字段不完整的实体。</p>
 *
 * @param <T> 实体类型
 */
final class EntityQueryCommand<T> {

    private final EntityCommandState<T> state;
    private final List<PageSort> sorts = new ArrayList<>();
    private final List<String> projections = new ArrayList<>();
    private final List<String> groups = new ArrayList<>();
    private SensitiveDisplayMode displayMode = SensitiveDisplayMode.DECLARED;

    EntityQueryCommand(EntityCommandState<T> state) {
        this.state = Objects.requireNonNull(state, "entity command state must not be null");
    }

    EntityCommandState<T> state() {
        return state;
    }

    void orderByAsc(EntityProperty<T, ?> property) {
        sorts.add(PageSort.asc(state.where().column(property)));
    }

    void orderByDesc(EntityProperty<T, ?> property) {
        sorts.add(PageSort.desc(state.where().column(property)));
    }

    void select(EntityProperty<T, ?>[] properties) {
        addColumns(properties, projections, "entity select requires at least one property");
    }

    void groupBy(EntityProperty<T, ?>[] properties) {
        addColumns(properties, groups, "entity group by requires at least one property");
    }

    void sensitiveDisplay(SensitiveDisplayMode mode) {
        displayMode = Objects.requireNonNull(mode, "sensitive display mode must not be null");
    }

    QuerySpec entitySpec() {
        requireEntityResult();
        return baseSpec();
    }

    QuerySpec projectedSpec() {
        if (projections.isEmpty()) {
            throw new IllegalStateException("executeRows() requires select(entity properties)");
        }
        return baseSpec().withProjection(projections, groups);
    }

    List<PageSort> sorts() {
        return List.copyOf(sorts);
    }

    private QuerySpec baseSpec() {
        QuerySpec spec = QuerySpec.of(state.form(), state.where().build()).withScope(state.scope()).withSorts(sorts);
        return switch (displayMode) {
            case DECLARED -> spec.declaredDisplay();
            case MASKED -> spec.masked();
            case FULL -> spec.showSensitive();
        };
    }

    private void requireEntityResult() {
        if (!projections.isEmpty() || !groups.isEmpty()) {
            throw new IllegalStateException("projected or grouped entity query must use executeRows()");
        }
    }

    private void addColumns(EntityProperty<T, ?>[] properties, List<String> target, String emptyMessage) {
        Objects.requireNonNull(properties, "entity properties must not be null");
        if (properties.length == 0) {
            throw new IllegalArgumentException(emptyMessage);
        }
        for (EntityProperty<T, ?> property : properties) {
            target.add(state.where().column(property));
        }
    }
}
