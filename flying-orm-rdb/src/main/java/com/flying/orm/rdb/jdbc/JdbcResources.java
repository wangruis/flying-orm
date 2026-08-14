package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import com.flying.orm.rdb.observation.SqlExecutionOperation;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

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
        if (lease != null && JdbcThrowableGraph.findVirtualMachineError(operationFailure) != null) {
            // 驱动执行阶段若已出现 JVM 致命错误，连接会话是否仍可复用无法确认；仅隔离自有租约，
            // 外部事务租约的 discard 是空操作，连接所有权仍由外部事务控制。
            lease.discardAfterUncertainTransaction(operationFailure);
        }
        for (AutoCloseable resource : resources) {
            if (resource == null || resource == lease) {
                continue;
            }
            try {
                resource.close();
            } catch (Exception | Error error) {
                discardConnectionFailure(lease, error);
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
            addSuppressedIfAcyclic(fatal, operationFailure);
            cleanupFailures.forEach(error -> addSuppressedIfAcyclic(fatal, error));
            throw fatal;
        }
        if (operationFailure != null) {
            cleanupFailures.forEach(error -> addSuppressedIfAcyclic(operationFailure, error));
            return;
        }
        if (!cleanupFailures.isEmpty()) {
            Throwable primaryCleanup = cleanupFailures.getFirst();
            cleanupFailures.stream().skip(1).forEach(error -> addSuppressedIfAcyclic(primaryCleanup, error));
            observations.cleanupFailure(operation, outcomeConfirmed, primaryCleanup);
        }
    }

    private static VirtualMachineError selectFatal(Throwable operationFailure, List<Throwable> cleanupFailures) {
        VirtualMachineError operationFatal = JdbcThrowableGraph.findVirtualMachineError(operationFailure);
        if (operationFatal != null) {
            return operationFatal;
        }
        VirtualMachineError selected = null;
        for (Throwable cleanupFailure : cleanupFailures) {
            VirtualMachineError candidate = JdbcThrowableGraph.findVirtualMachineError(cleanupFailure);
            if (candidate != null
                    && (selected == null || reaches(candidate, selected))) {
                // abort VME 会按隔离语义 suppress 触发 discard 的 VME；应选择它作为主异常，避免反向形成环。
                selected = candidate;
            }
        }
        return selected;
    }

    private static void addSuppressedIfAcyclic(Throwable primary, Throwable secondary) {
        if (secondary == null || primary == secondary || reaches(primary, secondary) || reaches(secondary, primary)) {
            return;
        }
        primary.addSuppressed(secondary);
    }

    private static boolean reaches(Throwable root, Throwable target) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(root);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current == target) {
                return true;
            }
            Throwable cause = current.getCause();
            if (cause != null) {
                pending.addLast(cause);
            }
            for (Throwable suppressed : current.getSuppressed()) {
                pending.addLast(suppressed);
            }
        }
        return false;
    }

    private static JdbcConnectionProvider.JdbcConnectionLease connectionLease(AutoCloseable[] resources) {
        for (AutoCloseable resource : resources) {
            if (resource instanceof JdbcConnectionProvider.JdbcConnectionLease lease) {
                return lease;
            }
        }
        return null;
    }

    private static void discardConnectionFailure(JdbcConnectionProvider.JdbcConnectionLease lease,
                                                 Throwable cleanupFailure) {
        if (lease == null) {
            return;
        }
        // Statement/ResultSet.close 的非检查异常没有稳定的 SQLState 可供判定连接是否可复用；保守隔离自有连接，
        // 不能让驱动 RuntimeException/Error 在清理后把未知会话归还连接池。
        if (cleanupFailure instanceof RuntimeException || cleanupFailure instanceof Error) {
            lease.discardAfterUncertainTransaction(cleanupFailure);
            return;
        }
        RuntimeException translated = RdbExceptionTranslator.translate(cleanupFailure);
        if (translated instanceof RdbException rdbError && rdbError.kind() == RdbErrorKind.CONNECTION) {
            lease.discardAfterUncertainTransaction(cleanupFailure);
        }
    }
}
