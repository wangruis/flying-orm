package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.core.internal.error.ThrowableGraph;
import com.flying.orm.rdb.observation.SqlFailureCategory;

import java.util.Objects;

/**
 * 上层可以长期依赖的迁移失败码。数据库驱动异常文字会随驱动版本变化，所以业务判断不要解析 message，
 * 直接使用这里的枚举即可。它只描述迁移这一层真正关心的结果，不复制底层所有数据库错误。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public enum SchemaMigrationFailureCode {
    /** 没有失败。 */
    NONE,
    /** 增量目标不足以生成所需 DDL，调用方须提供完整结构目标。 */
    MANUAL_ACTION_REQUIRED,
    /** 当前执行环境不满足 DDL 语义，例如 MySQL 注释所需的会话模式。 */
    EXECUTOR_CAPABILITY_REQUIRED,
    /** 执行器或资源所有者报告清理失败；不表示 Schema 自行治理会话。 */
    CLEANUP_FAILED,
    /** 建连失败、连接中断或连接已经不可用。 */
    CONNECTION_FAILURE,
    /** 驱动或上层执行上下文报告执行超时。 */
    TIMEOUT,
    /** 数据库检测到死锁并终止本次迁移。 */
    DEADLOCK,
    /** DDL 等待表锁或元数据锁超过数据库允许的时间。 */
    LOCK_TIMEOUT,
    /** SQL 语法、对象名或参数布局不被数据库接受。 */
    BAD_SQL,
    /** 唯一键、外键、非空或检查约束不允许本次结构变化。 */
    CONSTRAINT_VIOLATION,
    /** 调用方主动取消了响应式订阅。 */
    CANCELLED,
    /** 当前异常信息不足，不能可靠归入以上类别。 */
    UNKNOWN;

    static SchemaMigrationFailureCode classify(Throwable error,
                                                SqlFailureCategory category,
                                                SqlExecutionPhase failedPhase) {
        Objects.requireNonNull(category, "SQL failure category must not be null");
        SchemaMigrationRejectedException rejected = findRejected(error);
        if (rejected != null) {
            return rejected.failureCode();
        }
        if (failedPhase == SqlExecutionPhase.CLEANUP) {
            return CLEANUP_FAILED;
        }
        return switch (category) {
            case NONE -> NONE;
            case CONNECTION -> CONNECTION_FAILURE;
            case TIMEOUT -> TIMEOUT;
            case DEADLOCK -> DEADLOCK;
            case LOCK_TIMEOUT -> LOCK_TIMEOUT;
            case BAD_SQL -> BAD_SQL;
            case DUPLICATE_KEY, CONSTRAINT -> CONSTRAINT_VIOLATION;
            case CANCELLED -> CANCELLED;
            case ROW_LIMIT, RESULT_MEMORY_LIMIT, OPTIMISTIC_LOCK, UNKNOWN -> UNKNOWN;
        };
    }

    private static SchemaMigrationRejectedException findRejected(Throwable error) {
        return ThrowableGraph.findCause(error, SchemaMigrationRejectedException.class);
    }
}
