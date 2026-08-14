package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.result.JdbcDynamicRowFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 在 JDBC 批量分片事务内读取 owner 并维护 CONTAINS 侧索引。
 *
 * <p>owner 查询发生在业务更新前，令牌替换发生在业务分片确认成功后、事务提交前。任一步失败都会冒泡给既有批量
 * 事务协调器，由它统一回滚、报告 UNKNOWN 和隔离连接。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class JdbcProtectedBatchSideIndex {

    Prepared prepare(Connection connection,
                     BatchWriteRequest request,
                     List<Object[]> rows,
                     JdbcBatchSupport.BatchDeadline deadline) throws SQLException, java.util.concurrent.TimeoutException {
        List<RowState> states = new ArrayList<>(rows.size());
        for (Object[] row : rows) {
            ProtectedWriteWork work = ProtectedBatchRows.work(row, request.parameterCount());
            List<Map<String, Object>> owners = work == null || work.kind() != ProtectedWriteWork.Kind.UPDATE
                    ? List.of() : readOwners(connection, work, deadline.remaining());
            states.add(new RowState(work, owners));
        }
        return new Prepared(states);
    }

    void complete(Connection connection,
                  Prepared prepared,
                  BatchChunkResult result,
                  JdbcBatchSupport.BatchDeadline deadline) throws SQLException, java.util.concurrent.TimeoutException {
        if (result.status() != BatchChunkResult.Status.COMMITTED) {
            return;
        }
        for (RowState state : prepared.rows()) {
            if (state.work() != null) {
                replace(connection, state, new SqlWriteResult(1L, List.of()), deadline.remaining());
            }
        }
    }

    static void completeGeneratedRow(Connection connection,
                                     RowState state,
                                     long affectedRows,
                                     DynamicRow generatedKey,
                                     JdbcBatchSupport.BatchDeadline deadline)
            throws SQLException, java.util.concurrent.TimeoutException {
        if (state.work() != null && affectedRows > 0L) {
            replace(connection, state, new SqlWriteResult(affectedRows, List.of(generatedKey)), deadline.remaining());
        }
    }

    private static List<Map<String, Object>> readOwners(Connection connection,
                                                         ProtectedWriteWork work,
                                                         Duration remaining) throws SQLException {
        SqlRequest query = Objects.requireNonNull(work.ownerQuery(), "protected batch owner query must not be null");
        List<Map<String, Object>> owners = new ArrayList<>(1);
        try (PreparedStatement statement = connection.prepareStatement(query.sql())) {
            applyTimeout(statement, remaining);
            JdbcStatementBinder.bind(statement, query.parameters());
            JdbcStatementControl.requireNotInterrupted(statement);
            try (ResultSet resultSet = statement.executeQuery()) {
                JdbcDynamicRowFactory rowFactory = JdbcDynamicRowFactory.from(
                        resultSet, SqlExecutionOptions.safeDefaults());
                while (true) {
                    JdbcStatementControl.requireNotInterrupted(statement);
                    if (!resultSet.next()) {
                        break;
                    }
                    JdbcStatementControl.requireNotInterrupted(statement);
                    if (!owners.isEmpty()) {
                        throw new SQLException("protected batch update owner query returned multiple rows", "21000");
                    }
                    DynamicRow row = rowFactory.readCurrentRow();
                    Map<String, Object> owner = new LinkedHashMap<>();
                    for (int index = 0; index < work.ownerFields().size(); index++) {
                        owner.put(work.ownerFields().get(index), row.value(index));
                    }
                    owners.add(Map.copyOf(owner));
                }
            }
        }
        return List.copyOf(owners);
    }

    private static void replace(Connection connection,
                                RowState state,
                                SqlWriteResult result,
                                Duration remaining) throws SQLException {
        ProtectedWriteWork work = state.work();
        List<Map<String, Object>> owners = switch (work.kind()) {
            case INSERT -> List.of(resolveInsertOwner(work, result));
            case UPSERT -> List.of(work.knownOwner());
            case UPDATE -> state.owners();
        };
        if (owners.isEmpty()) {
            throw new SQLException("protected batch update owner was not found", "02000");
        }
        for (Map<String, Object> owner : owners) {
            for (ProtectedWriteWork.FieldTokens field : work.fields()) {
                if (work.kind() != ProtectedWriteWork.Kind.INSERT) {
                    update(connection, work.deleteSql(), values(work, owner, field, null), remaining, false);
                }
                for (byte[] token : field.tokens()) {
                    update(connection, work.insertSql(), values(work, owner, field, token), remaining, true);
                }
            }
        }
    }

    private static Map<String, Object> resolveInsertOwner(ProtectedWriteWork work, SqlWriteResult result) {
        Map<String, Object> owner = new LinkedHashMap<>(work.knownOwner());
        List<String> missing = work.ownerFields().stream()
                                   .filter(field -> !owner.containsKey(field) || owner.get(field) == null)
                                   .toList();
        if (missing.isEmpty()) {
            return Map.copyOf(owner);
        }
        if (missing.size() != 1 || result.generatedKeys().size() != 1) {
            throw new IllegalStateException("protected batch insert did not return one complete owner key");
        }
        DynamicRow key = result.generatedKeys().getFirst();
        Object value = key.containsKey(missing.getFirst()) ? key.get(missing.getFirst()) : key.value(0);
        owner.put(missing.getFirst(), Objects.requireNonNull(
                value, "protected batch generated owner key must not be null"));
        return Map.copyOf(owner);
    }

    private static void update(Connection connection,
                               String sql,
                               List<Object> values,
                               Duration remaining,
                               boolean requireOneRow) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            applyTimeout(statement, remaining);
            JdbcStatementBinder.bind(statement, values);
            JdbcStatementControl.requireNotInterrupted(statement);
            int affectedRows = statement.executeUpdate();
            if (requireOneRow && affectedRows != 1) {
                throw new IllegalStateException("protected side index insert must affect one row");
            }
        }
    }

    private static List<Object> values(ProtectedWriteWork work,
                                       Map<String, Object> owner,
                                       ProtectedWriteWork.FieldTokens field,
                                       byte[] token) {
        List<Object> values = new ArrayList<>(work.ownerFields().size() + 2);
        work.ownerFields().forEach(name -> values.add(Objects.requireNonNull(
                owner.get(name), "protected batch owner value must not be null")));
        values.add(field.fieldTag());
        if (token != null) {
            values.add(token);
        }
        return values;
    }

    private static void applyTimeout(PreparedStatement statement, Duration remaining) throws SQLException {
        if (remaining.isZero()) {
            return;
        }
        long seconds = remaining.getSeconds();
        long rounded = remaining.getNano() == 0 ? seconds : seconds + 1L;
        statement.setQueryTimeout((int) Math.min(Integer.MAX_VALUE, Math.max(1L, rounded)));
    }

    record Prepared(List<RowState> rows) {
        Prepared {
            rows = List.copyOf(rows);
        }
    }

    record RowState(ProtectedWriteWork work, List<Map<String, Object>> owners) {
        RowState {
            owners = List.copyOf(owners);
        }
    }
}
