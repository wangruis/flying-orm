package com.flying.orm.rdb.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/** Executes a bounded, verified JDBC batch for protected side-index tokens. */
final class JdbcProtectedSideIndexDml {

    static final int MAX_TOKEN_BATCH_SIZE = 500;

    private JdbcProtectedSideIndexDml() {
    }

    static void insertParameterSets(Connection connection,
                                    String sql,
                                    List<List<Object>> parameterSets)
            throws SQLException {
        if (parameterSets.isEmpty()) {
            return;
        }
        if (parameterSets.size() > MAX_TOKEN_BATCH_SIZE) {
            throw new IllegalArgumentException("protected side index token batch exceeds internal limit");
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (List<Object> parameters : parameterSets) {
                JdbcStatementControl.requireNotInterrupted(statement);
                JdbcStatementBinder.bind(statement, parameters);
                statement.addBatch();
            }
            JdbcStatementControl.requireNotInterrupted(statement);
            requireExactCounts(statement.executeBatch(), parameterSets.size());
        }
    }

    static void deleteParameterSets(Connection connection,
                                    String sql,
                                    List<List<Object>> parameterSets)
            throws SQLException {
        if (parameterSets.isEmpty()) {
            return;
        }
        if (parameterSets.size() > MAX_TOKEN_BATCH_SIZE
                || (parameterSets.size() > 1 && parameterSets.stream().mapToLong(List::size).sum()
                > com.flying.orm.rdb.internal.protection.ProtectedReplacementBatchPlan.MAX_PARAMETERS)) {
            throw new IllegalArgumentException("protected side index delete batch exceeds internal limit");
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (List<Object> parameters : parameterSets) {
                JdbcStatementControl.requireNotInterrupted(statement);
                JdbcStatementBinder.bind(statement, parameters);
                statement.addBatch();
            }
            JdbcStatementControl.requireNotInterrupted(statement);
            requireDeleteCounts(statement.executeBatch(), parameterSets.size());
        }
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


}
