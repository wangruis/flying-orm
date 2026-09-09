package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.internal.protection.ProtectedOwnerBatchPlan;
import com.flying.orm.rdb.internal.protection.ProtectedReplacementBatchPlan;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.result.JdbcDynamicRowFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 在 JDBC 批量分片事务内读取 owner 并维护 CONTAINS 侧索引。
 *
 * <p>owner 查询发生在业务更新前，令牌替换发生在业务分片执行成功后。
 * 任一步失败都交给上层外部事务拥有者或显式 INDEPENDENT 分片事务边界处理，不治理物理连接。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class JdbcProtectedBatchSideIndex {

    Prepared prepare(Connection connection,
                     BatchWriteRequest request,
                     List<ProtectedBatchRows.RowView> rows,
                     JdbcBatchEvidenceSupport.Counts evidence)
            throws SQLException {
        if (rows.stream().noneMatch(row -> row.work() != null)) {
            return new Prepared(List.of(), request.options().maxBufferedBytes());
        }
        List<RowState> states = new ArrayList<>(rows.size());
        for (ProtectedBatchRows.RowView rowView : rows) {
            states.add(new RowState(rowView.work(), List.of()));
        }
        SqlExecutionOptions options = ownerReadOptions(request);
        for (ProtectedOwnerBatchPlan plan : ProtectedOwnerBatchPlan.plans(
                rows, request.options().maxBufferedBytes())) {
            readOwners(connection, plan, states, options, evidence);
        }
        return new Prepared(states, request.options().maxBufferedBytes());
    }

    void complete(Connection connection,
                  Prepared prepared,
                  BatchChunkResult result) throws SQLException {
        if (result.status() != BatchChunkResult.Status.COMMITTED || prepared.rows().isEmpty()) {
            return;
        }
        for (RowState state : prepared.rows()) {
            if (state.work() != null
                    && state.work().kind() == ProtectedWriteWork.Kind.UPDATE
                    && state.owners().isEmpty()) {
                throw new SQLException("protected batch update owner was not found", "02000");
            }
        }
        completeReplacements(connection, prepared.rows(), prepared.maxBufferedBytes());
    }

    static void replaceOwners(Connection connection,
                              ProtectedWriteWork work,
                              List<Map<String, Object>> owners)
            throws SQLException {
        // 普通写入没有批量字节预算；复用已有参数数量和操作数上限，不增加新的输入限制。
        completeReplacements(connection, List.of(new RowState(work, owners)), Long.MAX_VALUE);
    }

    static void insertOwners(Connection connection,
                             ProtectedWriteWork work,
                             List<Map<String, Object>> owners)
            throws SQLException {
        TokenInsertBatch inserts = new TokenInsertBatch(connection, Long.MAX_VALUE);
        for (Map<String, Object> owner : owners) {
            for (ProtectedWriteWork.FieldTokens field : work.fields()) {
                inserts.add(work, owner, field);
            }
        }
        inserts.flush();
    }

    static GeneratedTokenBatch generatedTokenBatch(Connection connection,
                                                    Prepared prepared) {
        if (prepared.rows().isEmpty()) {
            return null;
        }
        for (RowState state : prepared.rows()) {
            if (state.work() != null && state.work().kind() != ProtectedWriteWork.Kind.INSERT) {
                return null;
            }
        }
        return new GeneratedTokenBatch(new TokenInsertBatch(
                connection, prepared.maxBufferedBytes()));
    }

    private static void completeReplacements(Connection connection,
                                              List<RowState> rows,
                                              long maxBufferedBytes)
            throws SQLException {
        for (ProtectedReplacementBatchPlan.Segment segment
                : ProtectedReplacementBatchPlan.segments(rows, maxBufferedBytes)) {
            if (!segment.deleteParameterSets().isEmpty()) {
                JdbcProtectedSideIndexDml.deleteParameterSets(
                        connection, segment.deleteSql(), segment.deleteParameterSets());
            }
            TokenInsertBatch inserts = new TokenInsertBatch(
                    connection, maxBufferedBytes);
            for (ProtectedReplacementBatchPlan.Insertion insertion : segment.insertions()) {
                inserts.add(insertion.work(), insertion.owner(), insertion.field());
            }
            inserts.flush();
        }
    }

    static void completeGeneratedRow(Connection connection,
                                     RowState state,
                                     long affectedRows,
                                     DynamicRow generatedKey)
            throws SQLException {
        if (state.work() != null && affectedRows > 0L) {
            replace(connection, state, new SqlWriteResult(affectedRows, List.of(generatedKey)));
        }
    }

    private static void readOwners(Connection connection,
                                   ProtectedOwnerBatchPlan plan,
                                   List<RowState> states,
                                   SqlExecutionOptions options,
                                   JdbcBatchEvidenceSupport.Counts evidence) throws SQLException {
        boolean[] matched = new boolean[plan.size()];
        try (PreparedStatement statement = connection.prepareStatement(plan.sql())) {
            JdbcStatementBinder.bind(statement, plan.parameters());
            JdbcStatementControl.requireNotInterrupted(statement);
            if (evidence != null) {
                evidence.markDatabaseWorkAttempted();
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                JdbcDynamicRowFactory rowFactory = JdbcDynamicRowFactory.from(resultSet, options);
                while (true) {
                    JdbcStatementControl.requireNotInterrupted(statement);
                    if (!resultSet.next()) {
                        break;
                    }
                    JdbcStatementControl.requireNotInterrupted(statement);
                    ProtectedOwnerBatchPlan.Match match;
                    try {
                        match = plan.decode(rowFactory.readCurrentRow());
                    } catch (IllegalStateException error) {
                        throw new SQLException(error.getMessage(), "21000", error);
                    }
                    if (matched[match.slot()]) {
                        throw new SQLException("protected batch update owner query returned multiple rows", "21000");
                    }
                    matched[match.slot()] = true;
                    states.set(match.rowIndex(), new RowState(
                            states.get(match.rowIndex()).work(), List.of(match.owner())));
                }
            }
        }
    }

    private static SqlExecutionOptions ownerReadOptions(BatchWriteRequest request) {
        long limit = request.options().maxBufferedBytes();
        return ProtectedWriteWork.ownerReadOptions(
                SqlExecutionOptions.safeDefaults()
                        .withMaxResultBytes(limit)
                        .withMaxLargeObjectBytes(limit)
                        .withMaxLargeObjectChars(limit));
    }

    private static void replace(Connection connection,
                                RowState state,
                                SqlWriteResult result)
            throws SQLException {
        ProtectedWriteWork work = state.work();
        List<Map<String, Object>> owners = switch (work.kind()) {
            case INSERT -> List.of(work.resolveInsertOwner(result));
            case UPSERT -> List.of(work.knownOwner());
            case UPDATE -> state.owners();
        };
        if (owners.isEmpty()) {
            throw new SQLException("protected batch update owner was not found", "02000");
        }
        for (Map<String, Object> owner : owners) {
            for (ProtectedWriteWork.FieldTokens field : work.fields()) {
                if (work.kind() != ProtectedWriteWork.Kind.INSERT) {
                    update(connection, work.deleteSql(), work.sideIndexParameters(owner, field, null), false);
                }
                JdbcProtectedSideIndexDml.insertTokens(
                        connection, work, owner, field);
            }
        }
    }

    private static void update(Connection connection,
                               String sql,
                               List<Object> values,
                               boolean requireOneRow)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            JdbcStatementBinder.bind(statement, values);
            JdbcStatementControl.requireNotInterrupted(statement);
            int affectedRows = statement.executeUpdate();
            if (requireOneRow && affectedRows != 1) {
                throw new IllegalStateException("protected side index insert must affect one row");
            }
        }
    }

    record Prepared(List<RowState> rows, long maxBufferedBytes) {
        Prepared(List<RowState> rows) {
            this(rows, com.flying.orm.rdb.batch.BatchWriteOptions.DEFAULT_MAX_BUFFERED_BYTES);
        }

        Prepared {
            rows = List.copyOf(rows);
            if (maxBufferedBytes <= 0L) {
                throw new IllegalArgumentException("protected batch byte limit must be greater than zero");
            }
        }
    }

    record RowState(ProtectedWriteWork work, List<Map<String, Object>> owners)
            implements ProtectedReplacementBatchPlan.Row {
        RowState {
            owners = List.copyOf(owners);
        }
    }

    static final class GeneratedTokenBatch {

        private final TokenInsertBatch inserts;

        private GeneratedTokenBatch(TokenInsertBatch inserts) {
            this.inserts = inserts;
        }

        void add(RowState state,
                 long affectedRows,
                 DynamicRow generatedKey)
                throws SQLException {
            if (state.work() == null || affectedRows <= 0L) {
                return;
            }
            ProtectedWriteWork work = state.work();
            Map<String, Object> owner = work.resolveInsertOwner(
                    new SqlWriteResult(affectedRows, List.of(generatedKey)));
            for (ProtectedWriteWork.FieldTokens field : work.fields()) {
                inserts.add(work, owner, field);
            }
        }

        void flush() throws SQLException {
            inserts.flush();
        }
    }

    private static final class TokenInsertBatch {

        private final Connection connection;
        private final long maxBufferedBytes;
        private final List<List<Object>> parameterSets = new ArrayList<>(
                JdbcProtectedSideIndexDml.MAX_TOKEN_BATCH_SIZE);
        private String sql;
        private int parameterCount;
        private long bufferedBytes;

        private TokenInsertBatch(Connection connection,
                                 long maxBufferedBytes) {
            this.connection = connection;
            this.maxBufferedBytes = maxBufferedBytes;
        }

        private void add(ProtectedWriteWork work,
                         Map<String, Object> owner,
                         ProtectedWriteWork.FieldTokens field)
                throws SQLException {
            if (sql != null && !sql.equals(work.insertSql())) {
                flush();
            }
            sql = work.insertSql();
            for (int index = 0; index < field.tokenCount(); index++) {
                List<Object> parameters = work.sideIndexParameters(owner, field, index);
                long bytes = com.flying.orm.rdb.batch.BatchMemoryBudget.estimateValueBytes(parameters);
                if (!parameterSets.isEmpty() && (parameterSets.size()
                        == JdbcProtectedSideIndexDml.MAX_TOKEN_BATCH_SIZE
                        || parameterCount + parameters.size()
                        > ProtectedReplacementBatchPlan.MAX_PARAMETERS
                        || saturatedAdd(bufferedBytes, bytes) > maxBufferedBytes)) {
                    flush();
                    sql = work.insertSql();
                }
                if (parameters.size() > ProtectedReplacementBatchPlan.MAX_PARAMETERS
                        || bytes > maxBufferedBytes) {
                    throw new IllegalArgumentException(
                            "protected side-index token parameters exceed batch safety limits");
                }
                parameterSets.add(parameters);
                parameterCount += parameters.size();
                bufferedBytes = saturatedAdd(bufferedBytes, bytes);
            }
        }

        private void flush() throws SQLException {
            if (parameterSets.isEmpty()) {
                return;
            }
            JdbcProtectedSideIndexDml.insertParameterSets(connection, sql, parameterSets);
            parameterSets.clear();
            parameterCount = 0;
            bufferedBytes = 0L;
        }

        private static long saturatedAdd(long left, long right) {
            return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
        }
    }
}
