package com.flying.orm.rdb.jdbc;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * JDBC 清理异常的身份图工具，防止为保留上下文而把 Throwable 图补成环。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
final class JdbcThrowableGraph {

    private JdbcThrowableGraph() {
    }

    /** 仅在两条因果图之间不存在已有可达路径时附加 suppressed 上下文。 */
    static void addSuppressedIfAcyclic(Throwable primary, Throwable secondary) {
        if (primary == null || secondary == null || primary == secondary
                || reaches(primary, secondary) || reaches(secondary, primary)) {
            return;
        }
        primary.addSuppressed(secondary);
    }

    /**
     * 在 cause 与 suppressed 构成的异常图中按对象身份查找 VM 错误。遍历优先保留直接
     * suppressed 的既有优先级，并在外部异常图意外成环时保持有界终止。
     */
    static VirtualMachineError findVirtualMachineError(Throwable error) {
        if (error == null) {
            return null;
        }
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.addFirst(error);
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
                pending.addFirst(cause);
            }
            Throwable[] suppressed = current.getSuppressed();
            for (int index = suppressed.length - 1; index >= 0; index--) {
                pending.addFirst(suppressed[index]);
            }
        }
        return null;
    }

    /**
     * 按对象身份遍历 cause 与 suppressed 边；即使传入的异常图原本已有环，检查也不会递归失控。
     */
    private static boolean reaches(Throwable start, Throwable expected) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(start);
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
}
