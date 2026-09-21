package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionSequence;
import com.flying.orm.rdb.execution.SqlExecutionSequenceResult;
import reactor.core.publisher.Mono;

/**
 * 能保证一组 SQL 使用同一条 R2DBC 连接的执行器能力。会话级 SET/RESET 只能走这里，
 * 普通 ReactiveSqlExecutor 的多次 rowsUpdated 调用没有同连接保证。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public interface ConnectionScopedReactiveSqlExecutor {
    Mono<SqlExecutionSequenceResult> executeInConnection(SqlExecutionSequence sequence,
                                                         SqlExecutionOptions options);
}
