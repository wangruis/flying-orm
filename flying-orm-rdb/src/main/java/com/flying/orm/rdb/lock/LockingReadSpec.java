package com.flying.orm.rdb.lock;

import com.flying.orm.rdb.execution.QueryRoutingIntent;
import com.flying.orm.rdb.form.spec.QuerySpec;

import java.util.Objects;

/**
 * 在不修改既有 QuerySpec 组件的前提下，为一次查询附加受控锁定语义。
 *
 * <p>该值对象不读取事务或连接；上层可以先读取路由意图，选择主数据源并开启外部事务，再把同一
 * 规格交给 ORM。</p>
 *
 * @author wangr
 * @version v3.2
 */
public record LockingReadSpec(QuerySpec query, ReadLock lock) {

    public LockingReadSpec {
        query = Objects.requireNonNull(query, "locking read query must not be null");
        lock = Objects.requireNonNull(lock, "locking read lock must not be null");
    }

    public static LockingReadSpec of(QuerySpec query, ReadLock lock) {
        return new LockingReadSpec(query, lock);
    }

    public QueryRoutingIntent routingIntent() {
        return QueryRoutingIntent.PRIMARY_REQUIRED;
    }
}
