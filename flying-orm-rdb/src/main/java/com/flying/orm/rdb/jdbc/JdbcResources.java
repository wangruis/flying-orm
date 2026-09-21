package com.flying.orm.rdb.jdbc;

import static com.flying.orm.rdb.jdbc.JdbcFailureSupport.suppress;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/** 关闭语句级资源，再调用上层释放端口；任何清理失败都保留。 */
final class JdbcResources {
    private JdbcResources() {}

    static void close(SqlExecutionOperation operation, boolean completed, Throwable failure,
                      JdbcExecutionObservationSupport observations, AutoCloseable... resources) {
        close(operation, completed, failure, observations, null, null, null, resources);
    }

    static void close(SqlExecutionOperation operation, boolean completed, Throwable failure,
                      JdbcExecutionObservationSupport observations, JdbcConnectionAccess access,
                      Connection connection, SqlRequest request, AutoCloseable... resources) {
        List<Throwable> cleanup = new ArrayList<>();
        for (AutoCloseable resource : resources) {
            if (resource == null) continue;
            try {
                resource.close();
            } catch (Exception | Error error) {
                cleanup.add(error);
                observe(observations, operation, ResourceCleanupObservation.Phase.SESSION_CLEANUP,
                        completed, error, cleanup);
            }
        }
        if (access != null && connection != null) {
            try {
                access.releaseConnection(connection, request);
            } catch (Exception | Error error) {
                cleanup.add(error);
                observe(observations, operation, ResourceCleanupObservation.Phase.CONNECTION_RELEASE,
                        completed, error, cleanup);
            }
        }
        VirtualMachineError fatal = failure instanceof VirtualMachineError vm ? vm : null;
        if (fatal == null) {
            for (Throwable error : cleanup) {
                if (error instanceof VirtualMachineError vm) { fatal = vm; break; }
            }
        }
        if (fatal != null) {
            suppress(fatal, failure);
            for (Throwable error : cleanup) suppress(fatal, error);
            throw fatal;
        }
        Error directError = failure instanceof Error error ? error : null;
        if (directError == null) {
            for (Throwable error : cleanup) {
                if (error instanceof Error candidate) { directError = candidate; break; }
            }
        }
        if (directError != null) {
            suppress(directError, failure);
            for (Throwable error : cleanup) suppress(directError, error);
            throw directError;
        }
        if (failure != null) {
            cleanup.forEach(error -> suppress(failure, error));
        } else if (!cleanup.isEmpty()) {
            Throwable primary = cleanup.getFirst();
            cleanup.stream().skip(1).forEach(error -> suppress(primary, error));
            if (primary instanceof Error error) throw error;
            throw RdbExceptionTranslator.translate(primary);
        }
    }

    private static void observe(JdbcExecutionObservationSupport observations,
                                SqlExecutionOperation operation, ResourceCleanupObservation.Phase phase,
                                boolean completed, Throwable error, List<Throwable> failures) {
        if (observations == null) return;
        try {
            observations.cleanupFailure(operation, phase, completed, error);
        } catch (Error fatal) {
            failures.add(fatal);
        }
    }
}
