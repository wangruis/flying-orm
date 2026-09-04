package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.QueryShapeLimits;

import java.util.Objects;

/**
 * 一次查询规划的轻量计数器。
 *
 * <p>规划器在原本遍历字段和条件的位置顺手记账，不能为了预算再次扫描查询树。该对象只属于一次
 * 调用，不进入结构缓存，也不在线程间共享。</p>
 */
final class QueryShapeBudget {

    private final QueryShapeLimits limits;
    private int projections;
    private int joins;
    private int groups;
    private int aggregates;
    private int havingNodes;
    private int sorts;
    private int binds;
    private int sqlLength;

    QueryShapeBudget(QueryShapeLimits limits) {
        this.limits = Objects.requireNonNull(limits, "query shape limits must not be null");
    }

    void addProjections(int count) {
        projections = add(projections, count, limits.maxProjectionCount(), "query projection count");
    }

    void addJoins(int count) {
        joins = add(joins, count, limits.maxJoinCount(), "query join count");
    }

    void addGroups(int count) {
        groups = add(groups, count, limits.maxGroupCount(), "query group count");
    }

    void addAggregates(int count) {
        aggregates = add(aggregates, count, limits.maxAggregateCount(), "query aggregate count");
    }

    void addHavingNodes(int count) {
        havingNodes = add(havingNodes, count, limits.maxHavingCount(), "query having count");
    }

    void addSorts(int count) {
        sorts = add(sorts, count, limits.maxSortCount(), "query sort count");
    }

    void addBinds(int count) {
        binds = add(binds, count, limits.maxBindCount(), "query bind count");
    }

    void addSqlLength(int count) {
        sqlLength = add(sqlLength, count, limits.maxSqlLength(), "query SQL length");
    }

    private static int add(int current, int count, int limit, String name) {
        if (count < 0) {
            throw new IllegalArgumentException(name + " increment must not be negative");
        }
        if (count > limit - current) {
            throw new IllegalArgumentException(name + " exceeds configured limit " + limit);
        }
        return current + count;
    }
}
