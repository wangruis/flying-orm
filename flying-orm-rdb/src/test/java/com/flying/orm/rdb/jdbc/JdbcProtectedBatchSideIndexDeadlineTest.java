package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 受保护批量侧索引的多条 DML 必须共享同一个绝对截止时间。 */
class JdbcProtectedBatchSideIndexDeadlineTest {

    @Test
    void stopsAfterCumulativeSideIndexWorkExhaustsTheBatchDeadline() {
        AtomicInteger executions = new AtomicInteger();
        Connection connection = connection(executions, Duration.ofMillis(180));
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.UPSERT,
                new SqlRequest("update business_row set value = ?", List.of("value")),
                null,
                List.of("id"),
                Map.of("id", 7L),
                "id = ?",
                "delete from token_index where id = ? and field_tag = ?",
                "insert into token_index(id, field_tag, token) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("phone", List.of(new byte[]{1}))));
        JdbcProtectedBatchSideIndex.Prepared prepared = new JdbcProtectedBatchSideIndex.Prepared(
                List.of(new JdbcProtectedBatchSideIndex.RowState(work, List.of())));
        JdbcBatchSupport.BatchDeadline deadline = JdbcBatchSupport.BatchDeadline.start(Duration.ofMillis(300));

        assertThrows(TimeoutException.class, () -> new JdbcProtectedBatchSideIndex().complete(
                connection, prepared, BatchChunkResult.committed(0, 0, 1, 1L), deadline));
        assertEquals(2, executions.get());
    }

    private static Connection connection(AtomicInteger executions, Duration delay) {
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                JdbcProtectedBatchSideIndexDeadlineTest.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "executeUpdate" -> {
                        executions.incrementAndGet();
                        delay(delay);
                        yield 1;
                    }
                    case "executeBatch" -> {
                        executions.incrementAndGet();
                        delay(delay);
                        yield new int[]{1};
                    }
                    case "close", "setObject", "setQueryTimeout" -> null;
                    case "isClosed" -> false;
                    case "toString" -> "delayed prepared statement";
                    default -> defaultValue(method.getReturnType());
                });
        return (Connection) Proxy.newProxyInstance(
                JdbcProtectedBatchSideIndexDeadlineTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "prepareStatement" -> statement;
                    case "toString" -> "delayed connection";
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static void delay(Duration delay) throws SQLException {
        try {
            Thread.sleep(delay);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new SQLException("test statement interrupted", error);
        }
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == char.class) {
            return '\0';
        }
        return 0;
    }
}
