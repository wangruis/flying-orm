package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlRequest;

import java.util.List;
import java.util.Objects;

/**
 * 按正向迁移的相反顺序排列的结构回退计划。
 *
 * @param requests 可以自动执行的反向结构 SQL
 * @param gaps 无法靠 DDL 自动恢复的内容
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record SchemaRollbackPlan(List<SqlRequest> requests, List<SchemaRollbackGap> gaps) {

    public SchemaRollbackPlan {
        requests = List.copyOf(Objects.requireNonNull(requests, "rollback requests must not be null"));
        gaps = List.copyOf(Objects.requireNonNull(gaps, "rollback gaps must not be null"));
    }

    public boolean complete() {
        return gaps.isEmpty();
    }
}
