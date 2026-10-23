package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.internal.protection.ProtectedReplacementBatchPlan.Prepared;
import com.flying.orm.rdb.internal.protection.ProtectedReplacementBatchPlan.Row;
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
 * 在同一 JDBC 连接上读取 owner 并维护 CONTAINS 侧索引。
 *
 * <p>owner 查询发生在业务更新前，令牌替换发生在业务缓冲执行成功后。
 * 任一步失败都保留实际 SQL 执行事实并交给上层，不治理事务或物理连接。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class JdbcProtectedBatchSideIndex {

    Prepared prepare(Connection connection,
                     BatchWriteRequest request,
                     List<ProtectedBatchRows.RowView> rows)
            throws SQLException {
        List<Row> states = ProtectedReplacementBatchPlan.prepareRows(rows);
        if (states.isEmpty()) return new Prepared(states, request.options().maxBufferedBytes());
        SqlExecutionOptions options = ownerReadOptions(request);
        for (ProtectedOwnerBatchPlan plan : ProtectedOwnerBatchPlan.plans(
                rows, request.options().maxBufferedBytes())) {
            readOwners(connection, plan, states, options);
        }
        return new Prepared(states, request.options().maxBufferedBytes());
    }

    void complete(Connection connection,
                  Prepared prepared) throws SQLException {
        if (prepared.rows().isEmpty()) {
            return;
        }
        completeReplacements(connection, prepared.rows(), prepared.maxBufferedBytes());
    }

    static void replaceOwners(Connection connection,
                              ProtectedWriteWork work,
                              List<Map<String, Object>> owners)
            throws SQLException {
        // 普通写入没有批量字节预算；复用已有参数数量和操作数上限，不增加新的输入限制。
        completeReplacements(connection, List.of(new Row(work, owners)), Long.MAX_VALUE);
    }

    /** Counts have already passed the executor's completeness and failure checks. */
    void complete(Connection connection, Prepared prepared, int[] updateCounts) throws SQLException {
        List<Row> rows = prepared.rows();
        if (rows.isEmpty()) return;
        List<Row> written = null;
        for (int index = 0; index < rows.size(); index++) {
            if (updateCounts[index] == 0) {
                if (written == null) written = new ArrayList<>(rows.subList(0, index));
            } else if (written != null) {
                written.add(rows.get(index));
            }
        }
        completeReplacements(connection, written == null ? rows : written, prepared.maxBufferedBytes());
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
        for (Row state : prepared.rows()) {
            if (state.work() != null && state.work().kind() != ProtectedWriteWork.Kind.INSERT) {
                return null;
            }
        }
        return new GeneratedTokenBatch(new TokenInsertBatch(
                connection, prepared.maxBufferedBytes()));
    }

    private static void completeReplacements(Connection connection,
                                              List<Row> rows,
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
                                     Row state,
                                     long affectedRows,
                                     DynamicRow generatedKey,
                                     long maxBufferedBytes)
            throws SQLException {
        if (state.work() != null && affectedRows > 0L) {
            completeReplacements(connection,
                    List.of(state.completed(new SqlWriteResult(affectedRows, List.of(generatedKey)))),
                    maxBufferedBytes);
        }
    }

    private static void readOwners(Connection connection,
                                   ProtectedOwnerBatchPlan plan,
                                   List<Row> states,
                                   SqlExecutionOptions options) throws SQLException {
        boolean[] matched = new boolean[plan.size()];
        try (PreparedStatement statement = connection.prepareStatement(plan.sql())) {
            JdbcStatementBinder.bind(statement, plan.parameters());
            JdbcStatementControl.requireNotInterrupted(statement);
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
                    states.set(match.rowIndex(), new Row(
                            states.get(match.rowIndex()).work(), List.of(match.owner())));
                }
            }
        }
    }

    private static SqlExecutionOptions ownerReadOptions(BatchWriteRequest request) {
        long limit = request.options().maxBufferedBytes();
        return new SqlExecutionOptions(0, limit, limit, limit, 0);
    }

    static final class GeneratedTokenBatch {

        private final TokenInsertBatch inserts;

        private GeneratedTokenBatch(TokenInsertBatch inserts) {
            this.inserts = inserts;
        }

        void add(Row state,
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
        private final ProtectedReplacementBatchPlan.TokenBuffer buffer;

        private TokenInsertBatch(Connection connection, long maxBufferedBytes) {
            this.connection = connection;
            this.buffer = new ProtectedReplacementBatchPlan.TokenBuffer(maxBufferedBytes);
        }

        private void add(ProtectedWriteWork work,
                         Map<String, Object> owner,
                         ProtectedWriteWork.FieldTokens field) throws SQLException {
            ProtectedReplacementBatchPlan.Insertion insertion =
                    new ProtectedReplacementBatchPlan.Insertion(work, owner, field);
            for (int index = 0; index < field.tokenCount(); index++) {
                if (!buffer.add(insertion, index)) {
                    flush();
                    buffer.add(insertion, index);
                }
            }
        }

        private void flush() throws SQLException {
            JdbcProtectedSideIndexDml.insertParameterSets(connection, buffer.sql(), buffer.parameters());
            buffer.clear();
        }
    }
}
