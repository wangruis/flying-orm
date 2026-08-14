package com.flying.orm.rdb.repository;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;

/**
 * Repository 批量保留和生命周期回调的失败聚合工具。
 *
 * <p>它只处理一次执行链中已知的主失败与清理失败。按对象身份遍历 cause/suppressed 图，
 * 寻找 {@link VirtualMachineError} 并优先保留该致命错误；附加诊断时拒绝形成异常图环。
 * 工具无共享可变状态，可被同步和响应式路径并发复用。</p>
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
final class RepositoryFailureSupport {

    private RepositoryFailureSupport() {
    }

    /** 在清理完成后选择出站错误；清理错误只作为诊断，嵌套 VME 始终优先。 */
    static Throwable afterCleanup(Throwable primary, ThrowingRunnable cleanup) {
        Throwable safePrimary = Objects.requireNonNull(primary, "repository primary failure must not be null");
        Objects.requireNonNull(cleanup, "repository cleanup must not be null");
        try {
            cleanup.run();
            return preferVirtualMachineError(safePrimary);
        } catch (Throwable cleanupFailure) {
            return merge(safePrimary, cleanupFailure);
        }
    }

    /** 合并多个独立恢复失败，保留首个非致命失败或任一嵌套 VME。 */
    static Throwable merge(Throwable primary, Throwable secondary) {
        Throwable safePrimary = Objects.requireNonNull(primary, "repository primary failure must not be null");
        Throwable safeSecondary = Objects.requireNonNull(secondary, "repository secondary failure must not be null");
        VirtualMachineError primaryFatal = virtualMachineError(safePrimary);
        if (primaryFatal != null) {
            addSuppressedIfAcyclic(primaryFatal, safeSecondary);
            return primaryFatal;
        }
        VirtualMachineError secondaryFatal = virtualMachineError(safeSecondary);
        if (secondaryFatal != null) {
            addSuppressedIfAcyclic(secondaryFatal, safePrimary);
            return secondaryFatal;
        }
        addSuppressedIfAcyclic(safePrimary, safeSecondary);
        return safePrimary;
    }

    /** 若失败图包含 VME，则把它提升为出站错误；其他失败保持原对象。 */
    static Throwable preferVirtualMachineError(Throwable failure) {
        Throwable safeFailure = Objects.requireNonNull(failure, "repository failure must not be null");
        VirtualMachineError fatal = virtualMachineError(safeFailure);
        return fatal == null ? safeFailure : fatal;
    }

    /** 把当前运行时可传播的失败原样抛出；不允许把 Error 降级成普通异常。 */
    static RuntimeException propagate(Throwable failure) {
        Throwable safeFailure = Objects.requireNonNull(failure, "repository failure must not be null");
        VirtualMachineError fatal = virtualMachineError(safeFailure);
        if (fatal != null) {
            throw fatal;
        }
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
     * VME 则继续按 JVM 致命错误规则抛出。
     */
    static void cleanupAfterCancellation(ThrowingRunnable cleanup) {
        try {
            Objects.requireNonNull(cleanup, "repository cleanup must not be null").run();
        } catch (Throwable failure) {
            VirtualMachineError fatal = virtualMachineError(failure);
            if (fatal != null) {
                throw fatal;
            }
        }
    }

    /** 在 cause/suppressed 身份图中查找第一个 VME，循环图不会无限遍历。 */
    static VirtualMachineError virtualMachineError(Throwable failure) {
        if (failure == null) {
            return null;
        }
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(failure);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current instanceof VirtualMachineError fatal) {
                return fatal;
            }
            Throwable cause = current.getCause();
            if (cause != null) {
                pending.addLast(cause);
            }
            Collections.addAll(pending, current.getSuppressed());
        }
        return null;
    }

    private static void addSuppressedIfAcyclic(Throwable primary, Throwable secondary) {
        if (primary == secondary || reaches(secondary, primary)) {
            return;
        }
        primary.addSuppressed(secondary);
    }

    private static boolean reaches(Throwable root, Throwable expected) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(root);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current == expected) {
                return true;
            }
            Throwable cause = current.getCause();
            if (cause != null) {
                pending.addLast(cause);
            }
            Collections.addAll(pending, current.getSuppressed());
        }
        return false;
    }

    @FunctionalInterface
    interface ThrowingRunnable {
        void run();
    }
}
