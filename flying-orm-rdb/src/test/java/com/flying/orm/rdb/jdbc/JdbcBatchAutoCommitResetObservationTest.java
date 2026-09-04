package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcBatchAutoCommitResetObservationTest {

    @Test
    void doesNotResetAutoCommitBeforeClosingCommittedOwnedAtomicConnection() {
        assertAutoCommitIsNotReset(BatchWriteOptions.atomic(1));
    }

    @Test
    void doesNotResetAutoCommitBeforeClosingCommittedOwnedIndependentConnection() {
        assertAutoCommitIsNotReset(BatchWriteOptions.independent(1, 1));
    }

    @Test
    void doesNotResetAutoCommitBeforeClosingCommittedOwnedProtectedWriteConnection() {
        List<Boolean> autoCommitSettings = new ArrayList<>();
        AtomicInteger closes = new AtomicInteger();
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource(
                protectedWriteConnection(autoCommitSettings, closes)));

        executor.atomicProtectedWrite(protectedInsert(), SqlExecutionOptions.safeDefaults());

        assertEquals(List.of(false), autoCommitSettings);
        assertEquals(1, closes.get());
    }

    private static void assertAutoCommitIsNotReset(BatchWriteOptions options) {
        List<Boolean> autoCommitSettings = new ArrayList<>();
        AtomicInteger closes = new AtomicInteger();
        RecordingObserver observer = new RecordingObserver();
        JdbcBatchWriter writer = JdbcBatchWriter.create(dataSource(batchConnection(autoCommitSettings, closes)))
                                                .withBatchObserver(observer);

        BatchWriteResult result = writer.writeBatch(request(options));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(List.of(false), autoCommitSettings);
        assertEquals(1, closes.get());
        assertTrue(observer.cleanup.isEmpty());
    }

    private static BatchWriteRequest request(BatchWriteOptions options) {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into batch_people(name_col) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{"name-0"}),
                options);
    }

    private static DataSource dataSource(Connection connection) {
        return (DataSource) Proxy.newProxyInstance(
                JdbcBatchAutoCommitResetObservationTest.class.getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> "getConnection".equals(method.getName())
                        ? connection : defaultValue(method.getReturnType()));
    }

    private static Connection batchConnection(List<Boolean> autoCommitSettings, AtomicInteger closes) {
        DatabaseMetaData metadata = (DatabaseMetaData) Proxy.newProxyInstance(
                JdbcBatchAutoCommitResetObservationTest.class.getClassLoader(),
                new Class<?>[]{DatabaseMetaData.class},
                (proxy, method, arguments) -> "getDatabaseProductName".equals(method.getName())
                        ? "H2" : defaultValue(method.getReturnType()));
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                JdbcBatchAutoCommitResetObservationTest.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, arguments) -> "executeBatch".equals(method.getName())
                        ? new int[]{1} : defaultValue(method.getReturnType()));
        return (Connection) Proxy.newProxyInstance(
                JdbcBatchAutoCommitResetObservationTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getMetaData" -> metadata;
                    case "getAutoCommit" -> true;
                    case "prepareStatement" -> statement;
                    case "setAutoCommit" -> autoCommitSettings.add((Boolean) arguments[0]);
                    case "close" -> closes.incrementAndGet();
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static Connection protectedWriteConnection(List<Boolean> autoCommitSettings, AtomicInteger closes) {
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                JdbcBatchAutoCommitResetObservationTest.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "executeLargeUpdate" -> 1L;
                    case "executeUpdate" -> 1;
                    case "executeBatch" -> new int[]{1};
                    default -> defaultValue(method.getReturnType());
                });
        return (Connection) Proxy.newProxyInstance(
                JdbcBatchAutoCommitResetObservationTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getAutoCommit" -> true;
                    case "setAutoCommit" -> autoCommitSettings.add((Boolean) arguments[0]);
                    case "prepareStatement" -> statement;
                    case "close" -> closes.incrementAndGet();
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static ProtectedWriteWork protectedInsert() {
        return new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into business_row(id, value_col) values (?, ?)", List.of(7L, "value")),
                null,
                List.of("id"),
                Map.of("id", 7L),
                "id = ?",
                "delete from token_index where id = ? and field_tag = ?",
                "insert into token_index(id, field_tag, token) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("phone", List.of(new byte[]{1}))));
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

    private static final class RecordingObserver implements BatchExecutionObserver, SqlExecutionObserver {

        private final List<ResourceCleanupObservation> cleanup = new ArrayList<>();

        @Override
        public boolean enabled() {
            return true;
        }

        @Override
        public void onExecution(BatchExecutionObservation observation) {
            // Batch result events are not the subject of this cleanup regression.
        }

        @Override
        public void onExecution(SqlExecutionObservation observation) {
            // Batch writes do not publish per-statement SQL observations here.
        }

        @Override
        public void onResourceCleanup(ResourceCleanupObservation observation) {
            cleanup.add(observation);
        }
    }
}
