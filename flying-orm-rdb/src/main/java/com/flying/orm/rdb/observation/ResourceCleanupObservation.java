package com.flying.orm.rdb.observation;

import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * 数据库业务结果确定前后发生的资源清理故障事实。
 *
 * <p>事件通过 {@link Phase} 标识清理阶段，通过 {@link FailureKind} 给出安全的故障分类，并把原始异常
 * 转换成不含驱动消息、SQL 参数、租户值或连接凭据的 {@link SanitizedCleanupException}。事件只保留
 * 顶层主失败和直接 suppressed 次失败，不递归复制外来异常图。</p>
 *
 * @param operation        发生清理故障的执行类型
 * @param phase            发生故障的资源清理阶段
 * @param outcomeConfirmed 数据库业务结果是否已经确定
 * @param error            已脱敏的主清理异常，直接 suppressed 项表示次清理失败
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record ResourceCleanupObservation(SqlExecutionOperation operation,
                                         Phase phase,
                                         boolean outcomeConfirmed,
                                         Throwable error) {

    /**
     * 创建并脱敏资源清理观测事件。
     *
     * @param operation        发生清理故障的执行类型
     * @param phase            发生故障的资源清理阶段
     * @param outcomeConfirmed 数据库业务结果是否已经确定
     * @param error            原始清理异常，仅在构造期间用于提取安全结构
     */
    public ResourceCleanupObservation {
        operation = Objects.requireNonNull(operation, "cleanup operation must not be null");
        phase = Objects.requireNonNull(phase, "cleanup phase must not be null");
        error = sanitize(Objects.requireNonNull(error, "cleanup error must not be null"));
    }

    /**
     * @return 顶层清理故障的安全分类
     */
    public FailureKind failureKind() {
        return ((SanitizedCleanupException) error).failureKind();
    }

    private static SanitizedCleanupException sanitize(Throwable error) {
        SanitizedCleanupException sanitized = sanitizedFact(error);
        for (Throwable suppressed : error.getSuppressed()) {
            if (suppressed != error) {
                sanitized.addSuppressed(sanitizedFact(suppressed));
            }
        }
        return sanitized;
    }

    private static SanitizedCleanupException sanitizedFact(Throwable error) {
        return new SanitizedCleanupException(
                error instanceof TimeoutException ? FailureKind.TIMEOUT : FailureKind.FAILURE);
    }

    /**
     * 资源清理生命周期阶段。阶段名称只描述框架动作，不包含 SQL、租户或连接标识。
     *
     * @author wangr
     * @date 2026-08-03
     * @version v1.0
     */
    public enum Phase {
        /** 归还或关闭可复用连接。 */
        CONNECTION_CLOSE,
        /** 回滚取消中的活动事务。 */
        TRANSACTION_ROLLBACK,
        /** 清理 DDL setup/work 留下的会话状态。 */
        SESSION_CLEANUP
    }

    /**
     * 不含驱动细节的清理故障分类。
     *
     * @author wangr
     * @date 2026-08-03
     * @version v1.0
     */
    public enum FailureKind {
        /** 清理 Publisher 超过独立上限。 */
        TIMEOUT,
        /** 清理 Publisher 报错，具体驱动消息已删除。 */
        FAILURE
    }

    /**
     * 面向观测系统的安全异常。消息固定且不保留 cause，只通过分类和 suppressed 层级表达诊断事实。
     *
     * @author wangr
     * @date 2026-08-03
     * @version v1.0
     */
    private static final class SanitizedCleanupException extends RuntimeException {

        private final FailureKind failureKind;

        private SanitizedCleanupException(FailureKind failureKind) {
            this(failureKind, failureKind == FailureKind.TIMEOUT
                    ? "resource cleanup timed out"
                    : "resource cleanup failed");
        }

        private SanitizedCleanupException(FailureKind failureKind, String message) {
            super(message);
            this.failureKind = failureKind;
        }

        /**
         * @return 不含驱动细节的安全故障分类
         */
        public FailureKind failureKind() {
            return failureKind;
        }
    }
}
