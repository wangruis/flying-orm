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
    /** 计划要求在线 DDL，但当前 SQL 可能阻塞业务。 */
    ONLINE_DDL_REQUIRED,
    /** 计划有回滚缺口，调用方没有提供与当前指纹一致的批准。 */
    APPROVAL_REQUIRED,
    /** 计划包含没有通用安全 SQL 的动作，只能按结构化步骤人工处理。 */
    MANUAL_ACTION_REQUIRED,
    /** 当前执行器不具备迁移选项要求的同连接能力。 */
    EXECUTOR_CAPABILITY_REQUIRED,
    /** 当前 DDL 会隐式提交、需要脱离事务执行，或方言事务能力尚未确认。 */
    DDL_TRANSACTION_NOT_SUPPORTED,
    /** 会话参数恢复失败，连接关闭后才会由连接池决定是否继续复用。 */
    CLEANUP_FAILED,
    /** 建连失败、连接中断或连接已经不可用。 */
    CONNECTION_FAILURE,
    /** 单条 DDL 超过调用方设置的执行时限。 */
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
