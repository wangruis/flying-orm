package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.execution.SqlRowLimitExceededException;
import com.flying.orm.rdb.observation.SqlStatementType;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.result.JdbcDynamicRowFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * JDBC 结果集的有界行读取协作器。
 *
 * <p>查询结果和生成键都在读取每一行时检查行数、估算内存和大字段限制，避免先物化完整结果后再拒绝。
 * 普通查询额外在每一行前检查中断；生成键读取保持原有 JDBC 行为，不额外改变驱动调用顺序。</p>
 *
 * @author wangr
 * @date 2026-08-08
 * @version v1.0
 */
final class JdbcResultSetReader {

    private JdbcResultSetReader() {
    }

    /**
     * 读取普通查询结果；每行读取前检查取消请求。
     */
    static void readQueryRows(ResultSet resultSet,
                              PreparedStatement statement,
                              SqlStatementType statementType,
                              SqlExecutionOptions options,
                              List<DynamicRow> result) throws SQLException {
        readRows(resultSet,
                 Objects.requireNonNull(statement, "jdbc statement must not be null"),
                 statementType,
                 options,
                 result,
                 0);
    }

    /**
     * 读取终端判定所需的有限行数。终端行数只决定何时停止读取，不替代执行保护里的 maxRows。
     */
    static void readQueryRows(ResultSet resultSet,
                              PreparedStatement statement,
                              SqlStatementType statementType,
                              SqlExecutionOptions options,
                              List<DynamicRow> result,
                              int rowLimit) throws SQLException {
        readRows(resultSet,
                 Objects.requireNonNull(statement, "jdbc statement must not be null"),
                 statementType,
                 options,
                 result,
                 rowLimit);
    }

    /**
     * 读取 JDBC 返回的生成键；保持生成键读取原本不额外检查中断的调用顺序。
     */
    static List<DynamicRow> readGeneratedKeys(ResultSet resultSet,
                                              SqlExecutionOptions options) throws SQLException {
        if (resultSet == null) {
            return List.of();
        }
        List<DynamicRow> result = new ArrayList<>();
        readRows(resultSet, null, SqlStatementType.INSERT, options, result, 0);
        return result;
    }

    private static void readRows(ResultSet resultSet,
                                 PreparedStatement interruptibleStatement,
                                 SqlStatementType statementType,
                                 SqlExecutionOptions options,
                                 List<DynamicRow> result,
                                 int rowLimit) throws SQLException {
        ResultSet safeResultSet = Objects.requireNonNull(resultSet, "jdbc result set must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        List<DynamicRow> safeResult = Objects.requireNonNull(result, "jdbc result must not be null");
        if (rowLimit < 0) {
            throw new IllegalArgumentException("mapped query row limit must not be negative");
        }
        JdbcDynamicRowFactory rows = JdbcDynamicRowFactory.from(safeResultSet, safeOptions);
        long estimatedBytes = 0L;
        long rowIndex = 0L;
        while (rowLimit == 0 || rowIndex < rowLimit) {
            if (interruptibleStatement != null) {
                JdbcStatementControl.requireNotInterrupted(interruptibleStatement);
            }
            if (!safeResultSet.next()) {
                break;
            }
            if (interruptibleStatement != null) {
                JdbcStatementControl.requireNotInterrupted(interruptibleStatement);
            }
            if (safeOptions.maxRows() > 0 && rowIndex >= safeOptions.maxRows()) {
                throw new SqlRowLimitExceededException(statementType, safeOptions.maxRows(), rowIndex);
            }
            DynamicRow row = rows.readCurrentRow();
            if (safeOptions.maxResultBytes() > 0) {
                estimatedBytes = saturatedAdd(estimatedBytes, BatchMemoryBudget.estimateRowBytes(row));
                if (estimatedBytes == Long.MAX_VALUE || estimatedBytes > safeOptions.maxResultBytes()) {
                    throw new SqlResultMemoryLimitExceededException(
                            statementType, safeOptions.maxResultBytes(), estimatedBytes, rowIndex);
                }
            }
            safeResult.add(row);
            rowIndex++;
        }
    }

    private static long saturatedAdd(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }
}
