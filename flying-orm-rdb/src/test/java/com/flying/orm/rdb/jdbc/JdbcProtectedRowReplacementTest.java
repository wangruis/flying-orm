package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcProtectedRowReplacementTest {
    @Test void rowReplacementClosesDeleteBeforeExactTokenInsert() throws SQLException {
        List<String> events = new ArrayList<>();
        JdbcProtectedBatchSideIndex.completeGeneratedRow(connection(events, null), state(), 1,
                DynamicRow.copyOf(Map.of("id", 7L)));
        assertEquals(List.of("delete", "delete-close", "insert", "insert-close"), events);
    }

    @Test void failedDeleteCloseStopsTokenInsertAndPreservesFailure() {
        List<String> events = new ArrayList<>();
        SQLException closeFailure = new SQLException("delete close failed");
        SQLException failure = assertThrows(SQLException.class, () ->
                JdbcProtectedBatchSideIndex.completeGeneratedRow(connection(events, closeFailure), state(), 1,
                        DynamicRow.copyOf(Map.of("id", 7L))));
        assertSame(closeFailure, failure);
        assertEquals(List.of("delete", "delete-close"), events);
    }

    private static JdbcProtectedBatchSideIndex.RowState state() {
        var work = new ProtectedWriteWork(ProtectedWriteWork.Kind.UPSERT,
                new SqlRequest("update sample set value_col=? where id=?", List.of("value", 7L)),
                null, List.of("id"), Map.of("id", 7L), "id=?",
                "delete from tokens where id=? and field_tag=?",
                "insert into tokens(id,field_tag,token) values (?,?,?)",
                List.of(new ProtectedWriteWork.FieldTokens("value_col", List.of(new byte[]{1}))));
        return new JdbcProtectedBatchSideIndex.RowState(work, List.of());
    }

    private static Connection connection(List<String> events, SQLException closeFailure) {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class}, (connection, method, arguments) -> {
                    if (!method.getName().equals("prepareStatement")) throw new AssertionError(method.getName());
                    boolean delete = ((String) arguments[0]).startsWith("delete");
                    return Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),
                            new Class<?>[]{PreparedStatement.class}, (statement, call, values) -> switch (call.getName()) {
                                case "setObject", "setNull", "setBinaryStream", "addBatch" -> null;
                                case "executeUpdate" -> { events.add("delete"); yield 0; }
                                case "executeBatch" -> { events.add("insert"); yield new int[]{1}; }
                                case "close" -> {
                                    events.add(delete ? "delete-close" : "insert-close");
                                    if (delete && closeFailure != null) throw closeFailure;
                                    yield null;
                                }
                                default -> throw new AssertionError(call.getName());
                            });
                });
    }
}
