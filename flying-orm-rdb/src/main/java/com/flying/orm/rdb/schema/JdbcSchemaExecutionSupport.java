package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;

/** Schema JDBC 执行的无状态失败与计数工具。 */
final class JdbcSchemaExecutionSupport {

    private JdbcSchemaExecutionSupport() {
    }

    static SchemaMigrationRejectedException rejection(
            SchemaMigrationFailureCode failureCode,
            String planFingerprint,
            String message) {
        return planFingerprint == null
                ? new SchemaMigrationRejectedException(failureCode, message)
                : new SchemaMigrationRejectedException(failureCode, planFingerprint, message);
    }

    static VirtualMachineError directVirtualMachineError(Throwable failure) {
        return failure instanceof VirtualMachineError fatal ? fatal : null;
    }

    static void suppress(Throwable primary, Throwable secondary) {
        if (primary != null && secondary != null && primary != secondary) {
            primary.addSuppressed(secondary);
        }
    }

    static void rethrow(Throwable failure) {
        if (failure instanceof Error fatal) {
            throw fatal;
        }
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        throw new IllegalStateException("schema migration failed with an unsupported checked exception", failure);
    }

    static long addExact(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            throw new RdbException(RdbErrorKind.UNKNOWN,
                                   "database execution count exceeds supported range",
                                   null, null, overflow);
        }
    }
}
