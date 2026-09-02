package com.flying.orm.rdb.jdbc;

/** JDBC 同步调用边界只处理当前抛出的失败，不递归解释驱动或回调包装链。 */
final class JdbcFailureSupport {

    private JdbcFailureSupport() {
    }

    static VirtualMachineError directVirtualMachineError(Throwable failure) {
        return failure instanceof VirtualMachineError fatal ? fatal : null;
    }

    static void suppress(Throwable primary, Throwable secondary) {
        if (primary != null && secondary != null && primary != secondary) {
            primary.addSuppressed(secondary);
        }
    }
}
