package com.flying.orm.rdb.repository;

import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;

import java.util.Objects;

/**
 * Repository 批量保留和生命周期回调的失败聚合工具。
 *
 * <p>它只处理一次执行链中已知的主失败与清理失败。普通同步生命周期按 Java 的标准语义保留主失败，
 * 把后续失败作为 suppressed；只有 Reactor 收到真实 JDK 异步包装器时才解包其中的 JVM 致命错误。
 * 工具无共享可变状态，可被同步和响应式路径并发复用。</p>
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
final class RepositoryFailureSupport {

    private RepositoryFailureSupport() {
    }

    /** 在清理完成后选择出站错误；主失败保持不变，清理错误只作为 suppressed 诊断。 */
    static Throwable afterCleanup(Throwable primary, ThrowingRunnable cleanup) {
        Throwable safePrimary = Objects.requireNonNull(primary, "repository primary failure must not be null");
        Objects.requireNonNull(cleanup, "repository cleanup must not be null");
        try {
            cleanup.run();
            return safePrimary;
        } catch (RuntimeException | Error cleanupFailure) {
            return merge(safePrimary, cleanupFailure);
        }
    }

    /** 合并多个独立恢复失败；首个失败保持为主失败，后续失败使用 Java 标准 suppressed 语义。 */
    static Throwable merge(Throwable primary, Throwable secondary) {
        Throwable safePrimary = Objects.requireNonNull(primary, "repository primary failure must not be null");
        Throwable safeSecondary = Objects.requireNonNull(secondary, "repository secondary failure must not be null");
        if (safePrimary != safeSecondary) {
            safePrimary.addSuppressed(safeSecondary);
        }
        return safePrimary;
    }

    /** 仅供真实 Reactor/JDK 异步错误边界恢复受支持包装器中的 VME。 */
    static Throwable preferVirtualMachineError(Throwable failure) {
        Throwable safeFailure = Objects.requireNonNull(failure, "repository failure must not be null");
        VirtualMachineError fatal = findVirtualMachineError(safeFailure);
        return fatal == null ? safeFailure : fatal;
    }

    /** 把当前运行时可传播的失败原样抛出；不允许把 Error 降级成普通异常。 */
    static RuntimeException propagate(Throwable failure) {
        Throwable safeFailure = Objects.requireNonNull(failure, "repository failure must not be null");
        if (safeFailure instanceof Error error) {
            throw error;
        }
        if (safeFailure instanceof RuntimeException error) {
            return error;
        }
        return new IllegalStateException("repository cleanup failed", safeFailure);
    }

    /**
     * 取消没有可用的 onError 通道；仍必须执行恢复和释放。普通恢复失败不能改变取消状态，
     * 直接 {@link Error} 则继续按 JVM 终止语义抛出。
     */
    static void cleanupAfterCancellation(ThrowingRunnable cleanup) {
        try {
            Objects.requireNonNull(cleanup, "repository cleanup must not be null").run();
        } catch (RuntimeException ignored) {
            // 取消信号没有 onError 通道；恢复诊断不能把取消改写成失败。
        }
    }

    @FunctionalInterface
    interface ThrowingRunnable {
        void run();
    }
}
