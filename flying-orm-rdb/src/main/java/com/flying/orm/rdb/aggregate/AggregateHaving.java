package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.condition.ConditionGroup;

import java.util.Objects;

/**
 * 参数化 HAVING 条件树。
 *
 * <p>它复用现有 ConditionGroup，不创建第二套条件 AST。FormAggregatePlanner 只会把 group alias
 * 和已声明 aggregate alias 放进可引用集合，普通字段或未知别名会在 SQL 前失败。</p>
 *
 * @param condition 参数化条件树
 * @author wangr
 * @version v3.2
 */
public record AggregateHaving(ConditionGroup condition) {

    public AggregateHaving {
        condition = Objects.requireNonNull(condition, "aggregate having condition must not be null");
    }

    public static AggregateHaving of(ConditionGroup condition) {
        return new AggregateHaving(condition);
    }
}
