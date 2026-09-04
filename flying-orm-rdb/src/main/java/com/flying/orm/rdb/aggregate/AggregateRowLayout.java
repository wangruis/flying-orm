package com.flying.orm.rdb.aggregate;

import java.util.LinkedHashMap;
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
    private final Map<GroupSelection, Integer> groupIndexes;
    private final Map<AggregateExpression<?>, Integer> aggregateIndexes;

    private AggregateRowLayout(List<GroupSelection> groups,
                               List<AggregateExpression<?>> aggregates) {
        this.groups = List.copyOf(Objects.requireNonNull(groups, "aggregate groups must not be null"));
        this.aggregates = List.copyOf(Objects.requireNonNull(
                aggregates, "aggregate expressions must not be null"));
        Map<GroupSelection, Integer> groupMap = new LinkedHashMap<>();
        for (int index = 0; index < this.groups.size(); index++) {
            if (groupMap.putIfAbsent(this.groups.get(index), index) != null) {
                throw new IllegalArgumentException("aggregate group selection must not be duplicated");
            }
        }
        Map<AggregateExpression<?>, Integer> aggregateMap = new LinkedHashMap<>();
        for (int index = 0; index < this.aggregates.size(); index++) {
            if (aggregateMap.putIfAbsent(this.aggregates.get(index), this.groups.size() + index) != null) {
                throw new IllegalArgumentException("aggregate expression must not be duplicated");
            }
        }
        groupIndexes = Map.copyOf(groupMap);
        aggregateIndexes = Map.copyOf(aggregateMap);
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

    int indexOf(GroupSelection group) {
        Integer index = groupIndexes.get(Objects.requireNonNull(group, "aggregate group must not be null"));
        if (index == null) {
            throw new IllegalArgumentException("group selection does not belong to this aggregate row layout");
        }
        return index;
    }

    int indexOf(AggregateExpression<?> aggregate) {
        Integer index = aggregateIndexes.get(Objects.requireNonNull(
                aggregate, "aggregate expression must not be null"));
        if (index == null) {
            throw new IllegalArgumentException("expression does not belong to this aggregate row layout");
        }
        return index;
    }
}
