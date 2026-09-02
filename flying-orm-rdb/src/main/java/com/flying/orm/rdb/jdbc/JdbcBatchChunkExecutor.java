package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchRowConflict;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.result.DynamicRow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * 在已经选定的 JDBC 连接和事务边界内执行一个批量分片。
 *
 * <p>普通写入始终使用 PreparedStatement.addBatch/executeBatch，不拼接参数到 SQL。严格的
 * 乐观锁行数策略仍然使用原生批量，但会拒绝驱动给出的 SUCCESS_NO_INFO，因为那种返回值不能证明
 * 每一行都恰好影响了一行。</p>
 */
final class JdbcBatchChunkExecutor {

    private final JdbcProtectedBatchSideIndex protectedSideIndex = new JdbcProtectedBatchSideIndex();

    BatchChunkResult execute(Connection connection,
                             BatchWriteRequest request,
                             int chunkIndex,
                             long startOffset,
                             List<ProtectedBatchRows.RowView> rows,
                             JdbcBatchSupport.BatchDeadline deadline) throws SQLException, TimeoutException {
        Connection safeConnection = Objects.requireNonNull(connection, "jdbc batch connection must not be null");
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        // rows 是批量读取器刚生成的私有分片，执行期间不会再修改；不复制可以省下一份分片引用数组。
        List<ProtectedBatchRows.RowView> safeRows = Objects.requireNonNull(
                rows, "jdbc batch rows must not be null");
        if (safeRows.isEmpty()) {
            throw new IllegalArgumentException("jdbc batch chunk must not be empty");
        }
        deadline.remaining();
        JdbcProtectedBatchSideIndex.Prepared protectedRows = protectedSideIndex.prepare(
                safeConnection, safeRequest, safeRows, deadline);
        if (safeRequest.generatedKeys().required()) {
            return executeReturningGeneratedKeys(safeConnection, safeRequest, chunkIndex, startOffset,
                                                 safeRows, deadline, protectedRows);
        }
        if (hasOwnerRestrictedUpdates(protectedRows)) {
            BatchChunkResult result = executeOwnerRestrictedUpdates(
                    safeConnection, safeRequest, chunkIndex, startOffset, safeRows, deadline, protectedRows);
            protectedSideIndex.complete(safeConnection, protectedRows, result, deadline);
            deadline.remaining();
            return result;
        }
        BatchChunkResult result;
        try (PreparedStatement statement = safeConnection.prepareStatement(safeRequest.sql())) {
            applyTimeout(statement, deadline.remaining());
            for (ProtectedBatchRows.RowView rowView : safeRows) {
                JdbcStatementBinder.bindOwned(statement, rowView);
                statement.addBatch();
                deadline.remaining();
            }
            deadline.remaining();
            JdbcStatementControl.requireNotInterrupted(statement);
            int[] counts = statement.executeBatch();
            result = result(safeRequest, chunkIndex, startOffset, safeRows.size(), counts);
        }
        protectedSideIndex.complete(safeConnection, protectedRows, result, deadline);
        deadline.remaining();
        return result;
    }

    /**
     * 数据库生成主键不能依赖 JDBC 批处理结果的返回顺序。这里仍然只保留当前分片，
     * 但把分片里的每一行在同一个连接上逐条执行、逐条取键并立即交回调用方。
     * 这样既不会缓存整批主键，也不会把主键错写到后续实体。
     */
    private static BatchChunkResult executeReturningGeneratedKeys(Connection connection,
                                                                    BatchWriteRequest request,
                                                                    int chunkIndex,
                                                                    long startOffset,
                                                                    List<ProtectedBatchRows.RowView> rows,
                                                                    JdbcBatchSupport.BatchDeadline deadline,
                                                                    JdbcProtectedBatchSideIndex.Prepared protectedRows)
            throws SQLException, TimeoutException {
        long affectedRows = 0L;
        List<BatchRowConflict> conflicts = new ArrayList<>();
        JdbcProtectedBatchSideIndex.GeneratedTokenBatch generatedTokens =
                JdbcProtectedBatchSideIndex.generatedTokenBatch(
                        connection, protectedRows, deadline);
        // 显式告诉驱动只返回主键列。PostgreSQL 等驱动在 RETURN_GENERATED_KEYS 下可能返回整行，
        // 指定列名既少传无关数据，也让 MySQL、PostgreSQL、H2 的回填结果保持同一形状。
        try (PreparedStatement statement = connection.prepareStatement(
                request.sql(), new String[]{request.generatedKeys().columnName()})) {
            for (int index = 0; index < rows.size(); index++) {
                JdbcStatementBinder.bindOwned(statement, rows.get(index));
                // 逐行回填会在一个分片内多次执行 SQL。每次执行前重新算剩余时间，
                // 这样前一行的绑定、驱动调用或回填回调耗时都会计入整批 timeout。
                applyTimeout(statement, deadline.remaining());
                JdbcStatementControl.requireNotInterrupted(statement);
                long count = executeUpdate(statement);
                JdbcStatementControl.requireNotInterrupted(statement);
                DynamicRow generatedKey = JdbcBatchGeneratedKeyReader.readOne(
                        statement.getGeneratedKeys(), request.generatedKeys().columnName(), SqlExecutionOptions.safeDefaults());
                // 回调故意放在下一行 SQL 之前。异常会直接冒泡，让已有事务边界回滚本分片或整批。
                request.generatedKeys().accept(startOffset + index, generatedKey);
                if (generatedTokens != null) {
                    generatedTokens.add(
                            protectedRows.rows().get(index), count, generatedKey);
                } else if (!protectedRows.rows().isEmpty()) {
                    JdbcProtectedBatchSideIndex.completeGeneratedRow(
                            connection, protectedRows.rows().get(index), count, generatedKey, deadline);
                }
                collectRowCount(request, startOffset + index, count, conflicts);
                if (count > 0L) {
                    affectedRows = addExact(affectedRows, count);
                }
                // 回调可能做实体主键回填，不能让它绕开批量任务的统一截止时间。
                deadline.remaining();
            }
        }
        if (generatedTokens != null) {
            generatedTokens.flush();
        }
        deadline.remaining();
        return conflicts.isEmpty()
                ? BatchChunkResult.committed(chunkIndex, startOffset, rows.size(), affectedRows)
                : BatchChunkResult.conflicted(chunkIndex, startOffset, rows.size(), conflicts);
    }

    /**
     * owner 预读与业务更新之间可能发生并发行漂移。受保护 UPDATE 必须按预读主键再次收窄 SQL，
     * 不能只凭最终影响行数相等就认定更新了同一批行。
     */
    private static BatchChunkResult executeOwnerRestrictedUpdates(
            Connection connection,
            BatchWriteRequest request,
            int chunkIndex,
            long startOffset,
            List<ProtectedBatchRows.RowView> rows,
            JdbcBatchSupport.BatchDeadline deadline,
            JdbcProtectedBatchSideIndex.Prepared protectedRows) throws SQLException, TimeoutException {
        long affectedRows = 0L;
        List<BatchRowConflict> conflicts = new ArrayList<>();
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
                collectRowCount(request, startOffset + index, 0L, conflicts);
                index++;
                continue;
            }
            int limit = index + 1;
            while (limit < updates.length && sameBatchShape(update, updates[limit])) {
                limit++;
            }
            affectedRows = addExact(affectedRows, executeOwnerRestrictedBatch(
                    connection, request, startOffset, updates, index, limit, deadline, conflicts));
            index = limit;
        }
        return conflicts.isEmpty()
                ? BatchChunkResult.committed(chunkIndex, startOffset, rows.size(), affectedRows)
                : BatchChunkResult.conflicted(chunkIndex, startOffset, rows.size(), conflicts);
    }

    private static long executeOwnerRestrictedBatch(
            Connection connection,
            BatchWriteRequest request,
            long startOffset,
            SqlRequest[] updates,
            int offset,
            int limit,
            JdbcBatchSupport.BatchDeadline deadline,
            List<BatchRowConflict> conflicts) throws SQLException, TimeoutException {
        SqlRequest first = updates[offset];
        deadline.remaining();
        try (PreparedStatement statement = connection.prepareStatement(first.sql())) {
            applyTimeout(statement, deadline.remaining());
            for (int index = offset; index < limit; index++) {
                JdbcStatementBinder.bind(statement, updates[index].parameters());
                statement.addBatch();
                deadline.remaining();
            }
            applyTimeout(statement, deadline.remaining());
            JdbcStatementControl.requireNotInterrupted(statement);
            int[] counts = statement.executeBatch();
            JdbcStatementControl.requireNotInterrupted(statement);
            deadline.remaining();
            if (counts == null || counts.length != limit - offset) {
                throw new SQLException("jdbc driver returned incomplete batch update counts", "HY000");
            }
            long affectedRows = 0L;
            for (int index = 0; index < counts.length; index++) {
                int count = counts[index];
                if (count == Statement.EXECUTE_FAILED) {
                    throw new SQLException("jdbc driver reported a failed batch item", "HY000");
                }
                collectRowCount(request, startOffset + offset + index, count, conflicts);
                if (count > 0) {
                    affectedRows = addExact(affectedRows, count);
                }
            }
            return affectedRows;
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

    private static BatchChunkResult result(BatchWriteRequest request,
                                           int chunkIndex,
                                           long startOffset,
                                           int inputCount,
                                           int[] counts) throws SQLException {
        if (counts == null || counts.length != inputCount) {
            throw new SQLException("jdbc driver returned incomplete batch update counts", "HY000");
        }
        long affectedRows = 0L;
        List<BatchRowConflict> conflicts = new ArrayList<>();
        for (int index = 0; index < counts.length; index++) {
            int count = counts[index];
            if (count == Statement.EXECUTE_FAILED) {
                throw new SQLException("jdbc driver reported a failed batch item", "HY000");
            }
            collectRowCount(request, startOffset + index, count, conflicts);
            if (count > 0) {
                affectedRows = addExact(affectedRows, count);
            }
        }
        return conflicts.isEmpty()
                ? BatchChunkResult.committed(chunkIndex, startOffset, inputCount, affectedRows)
                : BatchChunkResult.conflicted(chunkIndex, startOffset, inputCount, conflicts);
    }

    private static void collectRowCount(BatchWriteRequest request,
                                        long inputOffset,
                                        long count,
                                        List<BatchRowConflict> conflicts) throws SQLException {
        if (request.rowCountPolicy() != BatchRowCountPolicy.EXACTLY_ONE || count == 1L) {
            return;
        }
        if (count == Statement.SUCCESS_NO_INFO) {
            throw new SQLException("jdbc driver cannot verify exact batch row counts", "0A000");
        }
        conflicts.add(BatchRowConflict.exactlyOne(inputOffset, count));
    }

    private static long addExact(long left, long right) {
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

    private static void applyTimeout(PreparedStatement statement, Duration remaining) throws SQLException {
        if (remaining.isZero()) {
            return;
        }
        long seconds = remaining.getSeconds();
        if (seconds >= Integer.MAX_VALUE) {
            statement.setQueryTimeout(Integer.MAX_VALUE);
            return;
        }
        long roundedSeconds = remaining.getNano() == 0 ? seconds : seconds + 1L;
        statement.setQueryTimeout((int) Math.max(1L, roundedSeconds));
    }

}
