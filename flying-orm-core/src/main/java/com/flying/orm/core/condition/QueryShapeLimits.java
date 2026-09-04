package com.flying.orm.core.condition;

/**
 * 单次查询规划允许使用的结构上限。
 *
 * <p>默认值保持不设限，确保原有调用不会因为升级而被收窄。上层只需要针对外部动态查询创建一个
 * 更严格的不可变副本；ORM 在生成 SQL 和获取连接前完成计费。</p>
 *
 * @author wangr
 * @version v3.2.0
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

    /** 返回不会收窄现有查询的共享默认值。 */
    public static QueryShapeLimits existingDefaults() {
        return DEFAULTS;
    }

    /** 简短别名；语义与 {@link #existingDefaults()} 完全相同。 */
    public static QueryShapeLimits defaults() {
        return existingDefaults();
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
