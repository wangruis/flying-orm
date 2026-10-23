package com.flying.orm.rdb.aggregate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 聚合行的固定列布局。一个结果集只需创建一次并由所有行共享。
 *
 * @author wangr
 * @version v3.2
 */
public final class AggregateRowLayout {

    private final List<GroupSelection> groups;
    private final List<AggregateExpression<?>> aggregates;
    // String 与两种 final record 的身份互不相等，共享一个索引但不混淆查找语义。
    private final Map<Object, Integer> indexes;

    private AggregateRowLayout(List<GroupSelection> groups,
                               List<AggregateExpression<?>> aggregates) {
        this.groups = List.copyOf(Objects.requireNonNull(groups, "aggregate groups must not be null"));
        this.aggregates = List.copyOf(Objects.requireNonNull(
                aggregates, "aggregate expressions must not be null"));
        Map<Object, Integer> positions = new HashMap<>();
        for (int index = 0; index < this.groups.size(); index++) {
            GroupSelection group = this.groups.get(index);
            if (positions.putIfAbsent(group, index) != null) {
                throw new IllegalArgumentException("aggregate group selection must not be duplicated");
            }
            positions.merge(group.alias(), index, (previous, current) -> -1);
        }
        for (int index = 0; index < this.aggregates.size(); index++) {
            AggregateExpression<?> aggregate = this.aggregates.get(index);
            int position = this.groups.size() + index;
            if (positions.putIfAbsent(aggregate, position) != null) {
                throw new IllegalArgumentException("aggregate expression must not be duplicated");
            }
            positions.merge(aggregate.alias(), position, (previous, current) -> -1);
        }
        indexes = positions;
    }

    public static AggregateRowLayout of(List<GroupSelection> groups,
                                        List<AggregateExpression<?>> aggregates) {
        return new AggregateRowLayout(groups, aggregates);
    }

    public List<GroupSelection> groups() {
        return groups;
    }

    public List<AggregateExpression<?>> aggregates() {
        return aggregates;
    }

    public int size() {
        return groups.size() + aggregates.size();
    }

    int indexOf(String alias) {
        Integer index = indexes.get(Objects.requireNonNull(alias, "aggregate result alias must not be null"));
        if (index == null || index < 0) {
            throw new IllegalArgumentException("aggregate result alias must identify exactly one column: " + alias);
        }
        return index;
    }

    int indexOf(GroupSelection group) {
        Integer index = indexes.get(Objects.requireNonNull(group, "aggregate group must not be null"));
        if (index == null) {
            throw new IllegalArgumentException("group selection does not belong to this aggregate row layout");
        }
        return index;
    }

    int indexOf(AggregateExpression<?> aggregate) {
        Integer index = indexes.get(Objects.requireNonNull(
                aggregate, "aggregate expression must not be null"));
        if (index == null) {
            throw new IllegalArgumentException("expression does not belong to this aggregate row layout");
        }
        return index;
    }
}
