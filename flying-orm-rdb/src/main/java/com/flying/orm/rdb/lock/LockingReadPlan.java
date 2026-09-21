package com.flying.orm.rdb.lock;

import com.flying.orm.core.sql.render.SqlRequest;

import java.util.Objects;

/**
 * 纯规划器交给 JDBC/R2DBC 门面的锁定查询结果。
 *
 * @author wangr
 * @version v3.2
 */
public record LockingReadPlan(SqlRequest request) {

    public LockingReadPlan {
        request = Objects.requireNonNull(request, "locking read SQL request must not be null");
    }
}
