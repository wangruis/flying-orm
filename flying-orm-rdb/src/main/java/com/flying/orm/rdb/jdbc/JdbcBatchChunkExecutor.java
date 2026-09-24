package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.result.JdbcDynamicRowFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * 在已经选定的 JDBC 连接上执行一个有界输入缓冲。
 *
 * <p>普通写入始终使用 PreparedStatement.addBatch/executeBatch，不拼接参数到 SQL。严格的
 * 乐观锁行数策略仍然使用原生批量，但会拒绝驱动给出的 SUCCESS_NO_INFO，因为那种返回值不能证明
 * 每一行都恰好影响了一行。</p>
 */
final class JdbcBatchChunkExecutor {


    private final JdbcProtectedBatchSideIndex protectedSideIndex = new JdbcProtectedBatchSideIndex();

    void execute(Connection connection, BatchWriteRequest request, long startOffset,
                 List<ProtectedBatchRows.RowView> rows, JdbcBatchEvidenceSupport.Counts evidence)
                 throws SQLException {
        Connection safeConnection = Objects.requireNonNull(connection, "jdbc batch connection must not be null");
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        // rows 是批量读取器刚生成的私有缓冲，执行期间不会再修改；不复制可以省下一份缓冲引用数组。
        List<ProtectedBatchRows.RowView> safeRows = Objects.requireNonNull(
                rows, "jdbc batch rows must not be null");
        if (safeRows.isEmpty()) {
            throw new IllegalArgumentException("jdbc batch chunk must not be empty");
        }
        JdbcProtectedBatchSideIndex.Prepared protectedRows = protectedSideIndex.prepare(
                safeConnection, safeRequest, safeRows, evidence);
        if (safeRequest.generatedKeys().required()) {
            executeReturningGeneratedKeys(safeConnection, safeRequest, startOffset,
                                                 safeRows, protectedRows, evidence);
            return;
        }
        if (hasOwnerRestrictedUpdates(protectedRows)) {
            executeOwnerRestrictedUpdates(safeConnection, safeRequest, startOffset, safeRows, protectedRows, evidence);
            protectedSideIndex.complete(safeConnection, protectedRows);
            return;
        }
        PreparedStatement statement = safeConnection.prepareStatement(safeRequest.sql());
        Throwable executionFailure = null;
        try {
            for (ProtectedBatchRows.RowView rowView : safeRows) {
                JdbcStatementBinder.bindOwned(statement, rowView);
                statement.addBatch();
            }
            JdbcStatementControl.requireNotInterrupted(statement);
            markDatabaseWorkAttempted(evidence);
            JdbcBatchEvidenceSupport.executeBusinessBatch(
                    statement, evidence, startOffset, safeRows.size());
            evidence.requireSuccessful();
        } catch (SQLException | RuntimeException | Error failure) {
            executionFailure = failure;
            throw failure;
        } finally {
            boolean closed = false;
            try {
                statement.close();
                closed = true;
            } catch (SQLException | RuntimeException | Error cleanup) {
                if (executionFailure == null) throw cleanup;
                com.flying.orm.core.internal.error.ThrowableGraph.addSuppressedIfAcyclic(executionFailure, cleanup);
            }
            // SQL 计数与 POST 资格分离：Statement 已关闭，且该行没有待完成的保护辅助工作。
            if (closed) evidence.completedPlainRows(safeRows);
        }
        protectedSideIndex.complete(safeConnection, protectedRows);
    }

    /**
     * 数据库生成主键不能依赖 JDBC 批处理结果的返回顺序。这里仍然只保留当前缓冲，
     * 但把缓冲里的每一行在同一个连接上逐条执行、逐条取键并立即交回调用方。
     * 这样既不会缓存整批主键，也不会把主键错写到后续实体。
     */
    private static void executeReturningGeneratedKeys(Connection connection,
                                                                    BatchWriteRequest request,

                                                                    long startOffset,
                                                                     List<ProtectedBatchRows.RowView> rows,
                                                                     JdbcProtectedBatchSideIndex.Prepared protectedRows,
                                                                     JdbcBatchEvidenceSupport.Counts evidence)
            throws SQLException {
        JdbcProtectedBatchSideIndex.GeneratedTokenBatch generatedTokens =
                JdbcProtectedBatchSideIndex.generatedTokenBatch(
                        connection, protectedRows);
        // 显式告诉驱动只返回主键列。PostgreSQL 等驱动在 RETURN_GENERATED_KEYS 下可能返回整行，
        // 指定列名既少传无关数据，也让 MySQL、PostgreSQL、H2 的回填结果保持同一形状。
        int readyRows = 0;
        PreparedStatement statement = connection.prepareStatement(
                request.sql(), new String[]{request.generatedKeys().columnName()});
        Throwable executionFailure = null;
        try {
            for (int index = 0; index < rows.size(); index++) {
                JdbcStatementBinder.bindOwned(statement, rows.get(index));
                JdbcStatementControl.requireNotInterrupted(statement);
                markDatabaseWorkAttempted(evidence);
                long count = executeUpdate(statement);
                record(evidence, startOffset + index, count);
                JdbcStatementControl.requireNotInterrupted(statement);
                DynamicRow generatedKey = readGeneratedKey(
                        statement.getGeneratedKeys(), request.generatedKeys().columnName(), SqlExecutionOptions.safeDefaults());
                // 生成键立即应用；后续错误不会恢复已经交给调用方的键。
                request.generatedKeys().accept(startOffset + index, generatedKey);
                if (generatedTokens != null) {
                    generatedTokens.add(
                            protectedRows.rows().get(index), count, generatedKey);
                } else if (!protectedRows.rows().isEmpty()) {
                    JdbcProtectedBatchSideIndex.completeGeneratedRow(
                            connection, protectedRows.rows().get(index), count, generatedKey,
                            protectedRows.maxBufferedBytes());
                }
                readyRows++;
            }
        } catch (SQLException | RuntimeException | Error failure) {
            executionFailure = failure;
            throw failure;
        } finally {
            boolean closed = false;
            try {
                statement.close();
                closed = true;
            } catch (SQLException | RuntimeException | Error cleanup) {
                if (executionFailure == null) throw cleanup;
                com.flying.orm.core.internal.error.ThrowableGraph.addSuppressedIfAcyclic(executionFailure, cleanup);
            }
            // 已完成的逐行工作只有在共享 Statement 正常关闭后才具备 POST 资格。
            // 合并 token 尚未 flush 的行仍不完整；关闭异常同样不能发布成功 POST。
            if (closed && executionFailure != null && generatedTokens == null) {
                evidence.completedRows(readyRows);
            }
        }
        evidence.requireSuccessful();
        if (generatedTokens != null) {
            generatedTokens.flush();
        }
    }

    /**
     * 批量主键回填必须在回调前拒绝少键、多键和空键；当前行处理完即关闭结果集，
     * 不把驱动游标带到下一个输入项。
     */
    private static DynamicRow readGeneratedKey(ResultSet resultSet,
                                               String expectedColumn,
                                               SqlExecutionOptions options) throws SQLException {
        if (resultSet == null) {
            throw new SQLException("jdbc driver did not return generated keys", "HY000");
        }
        try (ResultSet keys = resultSet) {
            if (!keys.next()) {
                throw new SQLException("jdbc driver did not return generated key", "HY000");
            }
            DynamicRow row = JdbcDynamicRowFactory.from(keys, options).readCurrentRow();
            requireNonNullGeneratedKey(row, expectedColumn);
            if (keys.next()) {
                throw new SQLException("jdbc driver returned more than one generated key for one batch row", "HY000");
            }
            return row;
        }
    }

    private static void requireNonNullGeneratedKey(DynamicRow row, String expectedColumn) throws SQLException {
        DynamicRow safeRow = Objects.requireNonNull(row, "jdbc generated key row must not be null");
        String expected = normalizeGeneratedKeyColumn(expectedColumn);
        Object value = null;
        int matches = 0;
        for (int index = 0; index < safeRow.columnCount(); index++) {
            if (normalizeGeneratedKeyColumn(safeRow.columnName(index)).equals(expected)) {
                value = safeRow.value(index);
                matches++;
            }
        }
        if (matches == 0 && safeRow.columnCount() == 1) {
            value = safeRow.value(0);
            matches = 1;
        }
        if (matches != 1 || value == null) {
            throw new SQLException("jdbc driver returned an invalid generated key", "HY000");
        }
    }

    private static String normalizeGeneratedKeyColumn(String column) {
        return Objects.requireNonNull(column, "generated key column must not be null")
                      .toLowerCase(Locale.ROOT);
    }

    /**
     * owner 预读与业务更新之间可能发生并发行漂移。受保护 UPDATE 必须按预读主键再次收窄 SQL，
     * 不能只凭最终影响行数相等就认定更新了同一批行。
     */
    private static void executeOwnerRestrictedUpdates(
            Connection connection,
            BatchWriteRequest request,

            long startOffset,
            List<ProtectedBatchRows.RowView> rows,
            JdbcProtectedBatchSideIndex.Prepared protectedRows,
            JdbcBatchEvidenceSupport.Counts evidence) throws SQLException {
        SqlRequest[] updates = new SqlRequest[rows.size()];
        for (int index = 0; index < rows.size(); index++) {
            JdbcProtectedBatchSideIndex.RowState state = protectedRows.rows().get(index);
            ProtectedWriteWork work = state.work();
            if (work == null) {
                ProtectedBatchRows.RowView rowView = rows.get(index);
                updates[index] = new SqlRequest(
                        request.sql(), Arrays.asList(rowView.row()).subList(0, rowView.parameterCount()),
                        request.bindMarkerStyle());
                continue;
            }
            if (work.kind() != ProtectedWriteWork.Kind.UPDATE) {
                throw new IllegalArgumentException("protected batch work kind does not match update request");
            }
            if (!state.owners().isEmpty()) {
                updates[index] = work.writeRequestForOwners(state.owners());
            }
        }

        int index = 0;
        while (index < updates.length) {
            SqlRequest update = updates[index];
            if (update == null) {
                record(evidence, startOffset + index, 0L);
                index++;
                continue;
            }
            int limit = index + 1;
            while (limit < updates.length && sameBatchShape(update, updates[limit])) {
                limit++;
            }
            executeOwnerRestrictedBatch(connection, request, startOffset, updates, index, limit, evidence);
            index = limit;
        }

        evidence.requireSuccessful();
    }

    private static void executeOwnerRestrictedBatch(
            Connection connection,
            BatchWriteRequest request,
            long startOffset,
            SqlRequest[] updates,
            int offset,
            int limit,
            JdbcBatchEvidenceSupport.Counts evidence) throws SQLException {
        SqlRequest first = updates[offset];
        try (PreparedStatement statement = connection.prepareStatement(first.sql())) {
            for (int index = offset; index < limit; index++) {
                JdbcStatementBinder.bind(statement,
                        OwnedBindableValues.ownedValues(updates[index].parameters()));
                statement.addBatch();
            }
            JdbcStatementControl.requireNotInterrupted(statement);
            markDatabaseWorkAttempted(evidence);
            int[] counts = JdbcBatchEvidenceSupport.executeBusinessBatch(
                    statement, evidence, startOffset + offset, limit - offset);
            if (counts == null || counts.length != limit - offset) {
                throw new SQLException("jdbc driver returned incomplete batch update counts", "HY000");
            }
            evidence.requireNoFailures();
        }
    }

    private static boolean sameBatchShape(SqlRequest expected, SqlRequest candidate) {
        return candidate != null
                && expected.sql().equals(candidate.sql())
                && expected.bindMarkerStyle() == candidate.bindMarkerStyle()
                && expected.parameters().size() == candidate.parameters().size();
    }

    private static boolean hasOwnerRestrictedUpdates(JdbcProtectedBatchSideIndex.Prepared prepared) {
        return prepared.rows().stream().map(JdbcProtectedBatchSideIndex.RowState::work)
                .filter(Objects::nonNull)
                .anyMatch(work -> work.kind() == ProtectedWriteWork.Kind.UPDATE);
    }

    private static long executeUpdate(PreparedStatement statement) throws SQLException {
        try {
            return statement.executeLargeUpdate();
        } catch (java.sql.SQLFeatureNotSupportedException | AbstractMethodError unsupported) {
            return statement.executeUpdate();
        }
    }

    static long addExact(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            throw new RdbException(RdbErrorKind.UNKNOWN,
                                   "database execution count exceeds supported range",
                                   null,
                                   null,
                                   overflow);
        }
    }

    private static void record(JdbcBatchEvidenceSupport.Counts evidence, long inputOffset, long count) {
        if (evidence != null) {
            evidence.record(inputOffset, count);
        }
    }

    private static void markDatabaseWorkAttempted(JdbcBatchEvidenceSupport.Counts evidence) {
        if (evidence != null) {
            evidence.markDatabaseWorkAttempted();
        }
    }


}
