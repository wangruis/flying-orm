package com.flying.orm.rdb.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/** Executes a bounded, verified JDBC batch for protected side-index tokens. */
final class JdbcProtectedSideIndexDml {

    private JdbcProtectedSideIndexDml() {
    }

    static void insertParameterSets(Connection connection, String sql, List<List<Object>> parameterSets)
            throws SQLException {
        execute(connection, sql, parameterSets, true);
    }

    static void deleteParameterSets(Connection connection, String sql, List<List<Object>> parameterSets)
            throws SQLException {
        execute(connection, sql, parameterSets, false);
    }

    private static void execute(Connection connection, String sql, List<List<Object>> parameterSets,
                                boolean insert) throws SQLException {
        if (parameterSets.isEmpty()) return;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (List<Object> parameters : parameterSets) {
                JdbcStatementControl.requireNotInterrupted(statement);
                JdbcStatementBinder.bind(statement, parameters);
                statement.addBatch();
            }
            JdbcStatementControl.requireNotInterrupted(statement);
            int[] counts = statement.executeBatch();
            if (counts == null || counts.length != parameterSets.size()) {
                throw new IllegalStateException("protected side index batch must report one count per binding");
            }
            for (int count : counts) {
                // Only ORM-owned single-row token INSERT accepts SUCCESS_NO_INFO as successful insertion.
                if (count != Statement.SUCCESS_NO_INFO && (insert ? count != 1 : count < 0)) {
                    throw new IllegalStateException(insert
                            ? "protected side index insert must affect one row"
                            : "protected side index delete batch failed");
                }
            }
        }
    }
}
