package com.flying.orm.rdb.jdbc;

import static com.flying.orm.rdb.jdbc.JdbcFailureSupport.suppress;

import com.flying.orm.rdb.observation.SqlExecutionOperation;

import java.util.ArrayList;
import java.util.List;

/**
 * 按 ResultSet、Statement、Connection 的顺序关闭 JDBC 资源，并保留所有清理故障。
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
final class JdbcResources {

    private JdbcResources() {
    }

    static void close(SqlExecutionOperation operation,
                      boolean outcomeConfirmed,
                      Throwable operationFailure,
                      JdbcExecutionObservationSupport observations,
                      AutoCloseable... resources) {
        List<Throwable> cleanupFailures = new ArrayList<>();
        JdbcConnectionProvider.JdbcConnectionLease lease = connectionLease(resources);
        for (AutoCloseable resource : resources) {
            if (resource == null || resource == lease) {
                continue;
            }
            try {
                resource.close();
            } catch (Exception | Error error) {
                cleanupFailures.add(error);
            }
        }
        if (lease != null) {
            try {
                lease.close();
            } catch (Exception | Error error) {
                cleanupFailures.add(error);
            }
        }
        VirtualMachineError fatal = selectFatal(operationFailure, cleanupFailures);
        if (fatal != null) {
            suppress(fatal, operationFailure);
            cleanupFailures.forEach(error -> suppress(fatal, error));
            throw fatal;
        }
        if (operationFailure != null) {
            cleanupFailures.forEach(error -> suppress(operationFailure, error));
            return;
        }
        if (!cleanupFailures.isEmpty()) {
            Throwable primaryCleanup = cleanupFailures.getFirst();
            cleanupFailures.stream().skip(1).forEach(error -> suppress(primaryCleanup, error));
            observations.cleanupFailure(operation, outcomeConfirmed, primaryCleanup);
        }
    }

    private static VirtualMachineError selectFatal(Throwable operationFailure, List<Throwable> cleanupFailures) {
        if (operationFailure instanceof VirtualMachineError operationFatal) {
            return operationFatal;
        }
        for (Throwable cleanupFailure : cleanupFailures) {
            if (cleanupFailure instanceof VirtualMachineError candidate) {
                return candidate;
            }
        }
        return null;
    }

    private static JdbcConnectionProvider.JdbcConnectionLease connectionLease(AutoCloseable[] resources) {
        for (AutoCloseable resource : resources) {
            if (resource instanceof JdbcConnectionProvider.JdbcConnectionLease lease) {
                return lease;
            }
        }
        return null;
    }

}
