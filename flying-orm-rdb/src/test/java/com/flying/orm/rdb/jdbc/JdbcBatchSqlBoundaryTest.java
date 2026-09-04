package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcBatchSqlBoundaryTest {

    @Test
    void atomicBatchRejectsMultipleStatementsAtTheRequestBoundary() {
        AtomicBoolean connectionRequested = new AtomicBoolean();
        AtomicBoolean rowsSubscribed = new AtomicBoolean();
        JdbcBatchWriter writer = JdbcBatchWriter.create(dataSource(connectionRequested));

        assertThrows(IllegalArgumentException.class, () -> writer.writeBatch(com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "update alpha set value_col=?; update beta set value_col=?",
                2,
                List.of(Integer.class, Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.defer(() -> {
                    rowsSubscribed.set(true);
                    return Flux.<Object[]>just(new Object[]{1, 2});
                }),
                BatchWriteOptions.atomic(1))));

        assertFalse(connectionRequested.get());
        assertFalse(rowsSubscribed.get());
    }

    @Test
    void independentBatchRejectsMultipleStatementsBeforeReadingTheFirstChunk() {
        AtomicBoolean connectionRequested = new AtomicBoolean();
        AtomicBoolean rowsSubscribed = new AtomicBoolean();
        JdbcBatchWriter writer = JdbcBatchWriter.create(dataSource(connectionRequested));

        assertThrows(IllegalArgumentException.class, () -> writer.writeBatch(com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "update alpha set value_col=?; update beta set value_col=?",
                2,
                List.of(Integer.class, Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.defer(() -> {
                    rowsSubscribed.set(true);
                    return Flux.<Object[]>just(new Object[]{1, 2}, new Object[]{3, 4});
                }),
                BatchWriteOptions.independent(1, 1))));

        assertFalse(connectionRequested.get());
        assertFalse(rowsSubscribed.get());
    }

    private static DataSource dataSource(AtomicBoolean connectionRequested) {
        return (DataSource) Proxy.newProxyInstance(
                JdbcBatchSqlBoundaryTest.class.getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> {
                    if ("getConnection".equals(method.getName())) {
                        connectionRequested.set(true);
                        throw new AssertionError("invalid batch SQL reached connection acquisition");
                    }
                    return defaultValue(method.getReturnType());
                });
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
