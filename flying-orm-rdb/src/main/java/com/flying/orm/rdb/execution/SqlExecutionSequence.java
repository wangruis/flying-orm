package com.flying.orm.rdb.execution;

import com.flying.orm.core.sql.render.SqlRequest;

import java.util.List;
import java.util.Objects;

/**
 * 必须在同一数据库连接上顺序执行的一组 SQL。setup 通常设置会话级保护，work 是真正业务 SQL，
 * cleanup 负责恢复会话；执行器在完成、失败和取消时都必须处理 cleanup。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record SqlExecutionSequence(List<SqlRequest> setup,
                                   List<SqlRequest> work,
                                   List<SqlRequest> cleanup) {
    public SqlExecutionSequence {
        setup = List.copyOf(Objects.requireNonNull(setup, "sequence setup requests must not be null"));
        work = List.copyOf(Objects.requireNonNull(work, "sequence work requests must not be null"));
        cleanup = List.copyOf(Objects.requireNonNull(cleanup, "sequence cleanup requests must not be null"));
    }
}
