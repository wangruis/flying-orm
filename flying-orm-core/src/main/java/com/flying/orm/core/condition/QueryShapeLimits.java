package com.flying.orm.core.condition;

/**
 * 单次查询规划允许使用的结构上限。
 *
 * <p>默认不设限；上层可显式配置外部动态查询的结构预算。ORM 在生成 SQL 前完成计费。</p>
 *
 * @author wangr
 * @version v4.0.0
 */
public record QueryShapeLimits(int maxProjectionCount,
                               int maxJoinCount,
                               int maxGroupCount,
                               int maxAggregateCount,
                               int maxHavingCount,
                               int maxSortCount,
                               int maxBindCount,
                               int maxSqlLength) {

    private static final QueryShapeLimits DEFAULTS = new QueryShapeLimits(
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE);

    public QueryShapeLimits {
        requireNonNegative(maxProjectionCount, "query max projection count");
        requireNonNegative(maxJoinCount, "query max join count");
        requireNonNegative(maxGroupCount, "query max group count");
        requireNonNegative(maxAggregateCount, "query max aggregate count");
        requireNonNegative(maxHavingCount, "query max having count");
        requireNonNegative(maxSortCount, "query max sort count");
        requireNonNegative(maxBindCount, "query max bind count");
        requireNonNegative(maxSqlLength, "query max SQL length");
    }

    /** 返回不设限的共享默认值。 */
    public static QueryShapeLimits defaults() {
        return DEFAULTS;
    }

    public QueryShapeLimits withMaxProjectionCount(int value) {
        return copy(value, maxJoinCount, maxGroupCount, maxAggregateCount,
                    maxHavingCount, maxSortCount, maxBindCount, maxSqlLength);
    }

    public QueryShapeLimits withMaxJoinCount(int value) {
        return copy(maxProjectionCount, value, maxGroupCount, maxAggregateCount,
                    maxHavingCount, maxSortCount, maxBindCount, maxSqlLength);
    }

    public QueryShapeLimits withMaxGroupCount(int value) {
        return copy(maxProjectionCount, maxJoinCount, value, maxAggregateCount,
                    maxHavingCount, maxSortCount, maxBindCount, maxSqlLength);
    }

    public QueryShapeLimits withMaxAggregateCount(int value) {
        return copy(maxProjectionCount, maxJoinCount, maxGroupCount, value,
                    maxHavingCount, maxSortCount, maxBindCount, maxSqlLength);
    }

    public QueryShapeLimits withMaxHavingCount(int value) {
        return copy(maxProjectionCount, maxJoinCount, maxGroupCount, maxAggregateCount,
                    value, maxSortCount, maxBindCount, maxSqlLength);
    }

    public QueryShapeLimits withMaxSortCount(int value) {
        return copy(maxProjectionCount, maxJoinCount, maxGroupCount, maxAggregateCount,
                    maxHavingCount, value, maxBindCount, maxSqlLength);
    }

    public QueryShapeLimits withMaxBindCount(int value) {
        return copy(maxProjectionCount, maxJoinCount, maxGroupCount, maxAggregateCount,
                    maxHavingCount, maxSortCount, value, maxSqlLength);
    }

    public QueryShapeLimits withMaxSqlLength(int value) {
        return copy(maxProjectionCount, maxJoinCount, maxGroupCount, maxAggregateCount,
                    maxHavingCount, maxSortCount, maxBindCount, value);
    }

    private QueryShapeLimits copy(int projections,
                                  int joins,
                                  int groups,
                                  int aggregates,
                                  int having,
                                  int sorts,
                                  int binds,
                                  int sqlLength) {
        return new QueryShapeLimits(projections, joins, groups, aggregates,
                                    having, sorts, binds, sqlLength);
    }

    private static void requireNonNegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
    }
}
