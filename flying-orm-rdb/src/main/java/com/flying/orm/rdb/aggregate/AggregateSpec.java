package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.rdb.form.spec.QuerySpec;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * 一次聚合查询的不可变规格。
 *
 * <p>基础 where、DataScope、结构化条件和执行保护继续由原 QuerySpec 承载；这里只增加分组、聚合和
 * HAVING 事实。别名在 build 时一次性去重，后续 planner 无需反复扫描。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class AggregateSpec {

    private final QuerySpec query;
    private final List<GroupSelection> groups;
    private final List<AggregateExpression<?>> aggregates;
    private final AggregateHaving having;

    private AggregateSpec(Builder builder) {
        query = builder.query;
        groups = List.copyOf(builder.groups);
        aggregates = List.copyOf(builder.aggregates);
        having = builder.having;
        if (aggregates.isEmpty()) {
            throw new IllegalArgumentException("aggregate spec requires at least one aggregate expression");
        }
        validateAliases(groups, aggregates);
    }

    public static Builder builder(QuerySpec query) {
        return new Builder(query);
    }

    public QuerySpec query() {
        return query;
    }

    public List<GroupSelection> groups() {
        return groups;
    }

    public List<AggregateExpression<?>> aggregates() {
        return aggregates;
    }

    public Optional<AggregateHaving> having() {
        return Optional.ofNullable(having);
    }

    private static void validateAliases(List<GroupSelection> groups,
                                        List<AggregateExpression<?>> aggregates) {
        Set<String> aliases = new HashSet<>();
        for (GroupSelection group : groups) {
            if (!aliases.add(FieldIdentity.of(group.alias()).key())) {
                throw new IllegalArgumentException("aggregate result aliases must be unique");
            }
        }
        for (AggregateExpression<?> aggregate : aggregates) {
            if (!aliases.add(FieldIdentity.of(aggregate.alias()).key())) {
                throw new IllegalArgumentException("aggregate result aliases must be unique");
            }
        }
    }

    /** 只在一次配置调用中使用的构建器。 */
    public static final class Builder {

        private final QuerySpec query;
        private final List<GroupSelection> groups = new ArrayList<>();
        private final List<AggregateExpression<?>> aggregates = new ArrayList<>();
        private AggregateHaving having;

        private Builder(QuerySpec query) {
            this.query = Objects.requireNonNull(query, "aggregate query must not be null");
        }

        public Builder group(GroupSelection group) {
            groups.add(Objects.requireNonNull(group, "aggregate group must not be null"));
            return this;
        }

        public Builder aggregate(AggregateExpression<?> aggregate) {
            aggregates.add(Objects.requireNonNull(
                    aggregate, "aggregate expression must not be null"));
            return this;
        }

        public Builder having(AggregateHaving having) {
            this.having = Objects.requireNonNull(having, "aggregate having must not be null");
            return this;
        }

        public AggregateSpec build() {
            return new AggregateSpec(this);
        }
    }
}
