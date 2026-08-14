package com.flying.orm.rdb.protection;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * 字段保护扩展边界共享的致命错误识别工具。
 *
 * <p>自定义 codec、规范化器和脱敏策略可能用普通异常包装 JVM 致命错误。这里按对象身份非递归遍历
 * cause/suppressed 图，既恢复深层致命错误语义，也避免循环图造成重复遍历。</p>
 *
 * @author wangr
 * @date 2026-08-11
 * @version v1.0
 */
final class ProtectedFailureSupport {

    private ProtectedFailureSupport() {
    }

    /**
     * 在异常图中按对象身份查找第一个 JVM 致命错误。
     *
     * @param error 扩展抛出的异常
     * @return 找到的致命错误；不存在时返回 {@code null}
     */
    static VirtualMachineError findVirtualMachineError(Throwable error) {
        if (error == null) {
            return null;
        }
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(error);
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
}
