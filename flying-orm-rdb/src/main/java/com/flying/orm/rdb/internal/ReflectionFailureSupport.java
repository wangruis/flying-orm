package com.flying.orm.rdb.internal;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * 保留内部扩展或反射调用隐藏在包装异常中的 JVM 致命错误。
 *
 * <p>实体访问器、构造器和可选驱动适配器会把目标方法抛出的错误包装成
 * {@link java.lang.reflect.InvocationTargetException}，应用扩展也可能使用普通运行时异常包装底层失败。
 * 普通失败仍由各调用点转换成稳定业务异常；
 * {@link VirtualMachineError} 则必须保持原对象出站，不能被误判成可恢复的映射失败。遍历按对象身份去重并使用
 * 显式队列，即使异常图存在环或深链也不会递归失控。</p>
 *
 * @author wangr
 * @date 2026-08-12
 * @version v1.0
 */
@InternalApi
public final class ReflectionFailureSupport {

    private ReflectionFailureSupport() {
    }

    /**
     * 在 cause/suppressed 身份图中发现 JVM 致命错误时原样抛出。
     *
     * @param failure 内部扩展或反射边界捕获的失败
     */
    public static void rethrowVirtualMachineError(Throwable failure) {
        if (failure == null) {
            return;
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
                throw fatal;
            }
            Throwable cause = current.getCause();
            if (cause != null) {
                pending.addFirst(cause);
            }
            for (Throwable suppressed : current.getSuppressed()) {
                if (suppressed instanceof VirtualMachineError fatal) {
                    throw fatal;
                }
                pending.addLast(suppressed);
            }
        }
    }
}
