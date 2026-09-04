package com.flying.orm.rdb.lock;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.QueryRoutingIntent;

import java.util.Objects;

/**
 * 纯规划器交给 JDBC/R2DBC 门面的锁定查询结果。
 *
 * @author wangr
 * @version v3.2
 */
public record LockingReadPlan(SqlRequest request, QueryRoutingIntent routingIntent) {

    public LockingReadPlan {
        request = Objects.requireNonNull(request, "locking read SQL request must not be null");
        routingIntent = Objects.requireNonNull(
                routingIntent, "locking read routing intent must not be null");
        if (routingIntent != QueryRoutingIntent.PRIMARY_REQUIRED) {
            throw new IllegalArgumentException("locking read requires primary routing");
        }
    }
}
