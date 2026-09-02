package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.execution.ProtectedWriteWork;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/** Executes a bounded, verified JDBC batch for protected side-index tokens. */
final class JdbcProtectedSideIndexDml {

    static final int MAX_TOKEN_BATCH_SIZE = 500;

    private JdbcProtectedSideIndexDml() {
    }

    static void insertTokens(Connection connection,
                             ProtectedWriteWork work,
                             Map<String, Object> owner,
                             ProtectedWriteWork.FieldTokens field,
                             JdbcProtectedWriteDeadline deadline) throws SQLException {
        int tokenCount = field.tokenCount();
        for (int offset = 0; offset < tokenCount; offset += MAX_TOKEN_BATCH_SIZE) {
            int limit = Math.min(tokenCount, offset + MAX_TOKEN_BATCH_SIZE);
            try (PreparedStatement statement = connection.prepareStatement(work.insertSql())) {
                JdbcStatementOptions.apply(statement, deadline.remainingOptions());
                executeExactBatch(statement, work, owner, field, offset, limit);
            }
        }
    }

    static void insertParameterSets(Connection connection,
                                    String sql,
                                    List<List<Object>> parameterSets,
                                    JdbcBatchSupport.BatchDeadline deadline)
            throws SQLException, TimeoutException {
        if (parameterSets.isEmpty()) {
            return;
        }
        if (parameterSets.size() > MAX_TOKEN_BATCH_SIZE) {
            throw new IllegalArgumentException("protected side index token batch exceeds internal limit");
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            applyTimeout(statement, deadline.remaining());
            for (List<Object> parameters : parameterSets) {
                JdbcStatementControl.requireNotInterrupted(statement);
                JdbcStatementBinder.bind(statement, parameters);
                statement.addBatch();
            }
            JdbcStatementControl.requireNotInterrupted(statement);
            requireExactCounts(statement.executeBatch(), parameterSets.size());
        }
        deadline.remaining();
    }

    static void deleteParameterSets(Connection connection,
                                    String sql,
                                    List<List<Object>> parameterSets,
                                    JdbcBatchSupport.BatchDeadline deadline)
            throws SQLException, TimeoutException {
        if (parameterSets.isEmpty()) {
            return;
        }
        if (parameterSets.size() > MAX_TOKEN_BATCH_SIZE
                || parameterSets.stream().mapToInt(List::size).sum()
                > com.flying.orm.rdb.internal.protection.ProtectedReplacementBatchPlan.MAX_PARAMETERS) {
            throw new IllegalArgumentException("protected side index delete batch exceeds internal limit");
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            applyTimeout(statement, deadline.remaining());
            for (List<Object> parameters : parameterSets) {
                JdbcStatementControl.requireNotInterrupted(statement);
                JdbcStatementBinder.bind(statement, parameters);
                statement.addBatch();
            }
            JdbcStatementControl.requireNotInterrupted(statement);
            requireDeleteCounts(statement.executeBatch(), parameterSets.size());
        }
        deadline.remaining();
    }

    static void insertTokens(Connection connection,
                             ProtectedWriteWork work,
                             Map<String, Object> owner,
                             ProtectedWriteWork.FieldTokens field,
                             JdbcBatchSupport.BatchDeadline deadline) throws SQLException, TimeoutException {
        int tokenCount = field.tokenCount();
        for (int offset = 0; offset < tokenCount; offset += MAX_TOKEN_BATCH_SIZE) {
            int limit = Math.min(tokenCount, offset + MAX_TOKEN_BATCH_SIZE);
            try (PreparedStatement statement = connection.prepareStatement(work.insertSql())) {
                applyTimeout(statement, deadline.remaining());
                executeExactBatch(statement, work, owner, field, offset, limit);
            }
            deadline.remaining();
        }
    }

    private static void executeExactBatch(PreparedStatement statement,
                                          ProtectedWriteWork work,
                                          Map<String, Object> owner,
                                          ProtectedWriteWork.FieldTokens field,
                                          int offset,
                                          int limit) throws SQLException {
        for (int index = offset; index < limit; index++) {
            JdbcStatementControl.requireNotInterrupted(statement);
            JdbcStatementBinder.bind(statement, work.sideIndexParameters(owner, field, index));
            statement.addBatch();
        }
        JdbcStatementControl.requireNotInterrupted(statement);
        requireExactCounts(statement.executeBatch(), limit - offset);
    }

    private static void requireExactCounts(int[] affectedRows, int expected) {
        if (affectedRows == null || affectedRows.length != expected) {
            throw new IllegalStateException("protected side index batch must report one count per token");
        }
        for (int affected : affectedRows) {
            // This batch only contains flying-orm's single-row VALUES token INSERT. JDBC defines
            // SUCCESS_NO_INFO as successful execution with an unavailable count; it is accepted
            // only at this narrow boundary and is never generalized to arbitrary business DML.
            if (affected != 1 && affected != Statement.SUCCESS_NO_INFO) {
                throw new IllegalStateException("protected side index insert must affect one row");
            }
        }
    }

    private static void requireDeleteCounts(int[] affectedRows, int expected) {
        if (affectedRows == null || affectedRows.length != expected) {
            throw new IllegalStateException("protected side index delete batch must report one count per binding");
        }
        for (int affected : affectedRows) {
            if (affected < 0 && affected != Statement.SUCCESS_NO_INFO) {
                throw new IllegalStateException("protected side index delete batch failed");
            }
        }
    }

    private static void applyTimeout(PreparedStatement statement, java.time.Duration remaining) throws SQLException {
        if (remaining.isZero()) {
            return;
        }
        long seconds = remaining.getSeconds();
        long rounded = remaining.getNano() == 0 ? seconds : seconds + 1L;
        statement.setQueryTimeout((int) Math.min(Integer.MAX_VALUE, Math.max(1L, rounded)));
    }
}
