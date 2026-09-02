package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlRequest;

import java.util.List;
import java.util.Objects;

/**
 * 在线 DDL 审核结果。这里宁可把语句保守标成可能锁表，也不假设某个数据库版本一定支持 online hint。
 *
 * @param mode 本次要求
 * @param support 当前方言经过确认的在线 DDL 能力
 * @param potentiallyBlocking 改写后仍可能持有表锁或重写表的语句
 * @param requiresNonTransactionalExecution 是否包含不能放进事务块的在线 DDL，例如 PostgreSQL 并发索引
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record OnlineDdlReview(OnlineDdlMode mode,
                              SchemaOnlineDdlSupport support,
                              List<SqlRequest> potentiallyBlocking,
                              boolean requiresNonTransactionalExecution) {

    public OnlineDdlReview(OnlineDdlMode mode, List<SqlRequest> potentiallyBlocking) {
        this(mode, SchemaOnlineDdlSupport.NONE, potentiallyBlocking, false);
    }

    public OnlineDdlReview(OnlineDdlMode mode,
                           SchemaOnlineDdlSupport support,
                           List<SqlRequest> potentiallyBlocking) {
        this(mode, support, potentiallyBlocking, false);
    }

    public OnlineDdlReview {
        mode = Objects.requireNonNull(mode, "online DDL mode must not be null");
        support = Objects.requireNonNull(support, "online DDL support must not be null");
        potentiallyBlocking = List.copyOf(Objects.requireNonNull(
                potentiallyBlocking, "potentially blocking statements must not be null"));
    }

    public boolean requiresExternalOnlineTool() {
        return mode != OnlineDdlMode.ALLOW_BLOCKING && !potentiallyBlocking.isEmpty();
    }

    public boolean executionAllowed() {
        return mode != OnlineDdlMode.REQUIRE_ONLINE || potentiallyBlocking.isEmpty();
    }
}
