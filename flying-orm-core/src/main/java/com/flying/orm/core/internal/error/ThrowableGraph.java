package com.flying.orm.core.internal.error;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * flying-orm 内部异常组合操作。
 *
 * <p>这里只识别直接异常、反射目标异常和 JDK 异步包装器。不会递归搜索任意 cause/suppressed 图；
 * 资源清理代码在合并主、次失败前分别检查它们；只有确实收到反射或 JDK 异步包装器的边界
 * 才应调用解包方法。确定性的内部转换和普通扩展回调不得借这里搜索异常图。</p>
 *
 * @author wangr
 * @date 2026-08-24
 * @version v3.1
 */
public final class ThrowableGraph {

    private static final int MAX_KNOWN_WRAPPERS = 8;

    private static final int MAX_CAUSE_DEPTH = 32;

    private ThrowableGraph() {
    }

    /** 查找直接或受支持包装器中的 JVM 致命错误。 */
    public static VirtualMachineError findVirtualMachineError(Throwable root) {
        return find(root, VirtualMachineError.class);
    }

    /** 若真实反射/异步边界的直接异常或受支持包装器包含 JVM 致命错误，则原样抛出该对象。 */
    public static void rethrowVirtualMachineError(Throwable root) {
        VirtualMachineError fatal = findVirtualMachineError(root);
        if (fatal != null) {
            throw fatal;
        }
    }

    /**
     * 在直接异常、反射目标异常和有限 JDK 异步包装链中查找指定类型。
     *
     * @param root 异常入口；允许为 {@code null}
     * @param type 目标异常类型
     * @param <T> 目标异常类型
     * @return 第一个匹配节点；未找到时返回 {@code null}
     */
    public static <T extends Throwable> T find(Throwable root, Class<T> type) {
        Objects.requireNonNull(type, "throwable type must not be null");
        Throwable current = root;
        for (int depth = 0; current != null && depth <= MAX_KNOWN_WRAPPERS; depth++) {
            if (type.isInstance(current)) {
                return type.cast(current);
            }
            Throwable next = knownWrapperTarget(current);
            if (next == current) {
                return null;
            }
            current = next;
        }
        return null;
    }

    /**
     * 在有限 cause 链中查找指定异常类型；用于 flying-orm 自己的领域异常分类。
     *
     * @param root cause 链入口；允许为 {@code null}
     * @param type 目标异常类型
     * @param <T> 目标异常类型
     * @return 第一个匹配节点；未找到时返回 {@code null}
     */
    public static <T extends Throwable> T findCause(Throwable root, Class<T> type) {
        Objects.requireNonNull(type, "throwable type must not be null");
        Throwable current = root;
        for (int depth = 0; current != null && depth <= MAX_CAUSE_DEPTH; depth++) {
            if (type.isInstance(current)) {
                return type.cast(current);
            }
            Throwable next = current.getCause();
            if (next == current) {
                return null;
            }
            current = next;
        }
        return null;
    }

    /** 把清理失败保留为 suppressed，但拒绝自环、重复边和受支持包装链中的直接反向边。 */
    public static void addSuppressedIfAcyclic(Throwable primary, Throwable secondary) {
        if (primary == null || secondary == null || primary == secondary
                || primary.getCause() == secondary || secondary.getCause() == primary
                || hasDirectSuppressed(primary, secondary)
                || hasDirectSuppressed(secondary, primary)
                || reaches(primary, secondary) || reaches(secondary, primary)) {
            return;
        }
        primary.addSuppressed(secondary);
    }

    /** 主失败与清理失败中存在 JVM 致命错误时恢复原对象；主失败中的致命错误保持优先。 */
    public static VirtualMachineError promoteVirtualMachineError(Throwable primary, Throwable secondary) {
        VirtualMachineError primaryFatal = findVirtualMachineError(primary);
        if (primaryFatal != null) {
            addSuppressedIfAcyclic(primaryFatal, secondary);
            return primaryFatal;
        }
        VirtualMachineError secondaryFatal = findVirtualMachineError(secondary);
        if (secondaryFatal != null) {
            addSuppressedIfAcyclic(secondaryFatal, primary);
        }
        return secondaryFatal;
    }

    /** 判断直接异常或受支持包装链是否按对象身份包含目标节点。 */
    public static boolean reaches(Throwable root, Throwable target) {
        if (root == null || target == null) {
            return false;
        }
        Throwable current = root;
        for (int depth = 0; current != null && depth <= MAX_KNOWN_WRAPPERS; depth++) {
            if (current == target) {
                return true;
            }
            Throwable next = knownWrapperTarget(current);
            if (next == current) {
                return false;
            }
            current = next;
        }
        return false;
    }

    private static boolean hasDirectSuppressed(Throwable error, Throwable target) {
        for (Throwable suppressed : error.getSuppressed()) {
            if (suppressed == target) {
                return true;
            }
        }
        return false;
    }

    private static Throwable knownWrapperTarget(Throwable error) {
        if (error instanceof InvocationTargetException reflection) {
            return reflection.getTargetException();
        }
        if (error instanceof UndeclaredThrowableException reflection) {
            return reflection.getUndeclaredThrowable();
        }
        if (error instanceof CompletionException || error instanceof ExecutionException) {
            return error.getCause();
        }
        return null;
    }
}
