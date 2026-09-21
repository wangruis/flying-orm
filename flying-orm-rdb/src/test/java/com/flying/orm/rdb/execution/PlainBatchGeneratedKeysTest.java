package com.flying.orm.rdb.execution;

import com.flying.orm.rdb.dialect.RdbDialect;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchAffectedRows;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.jdbc.JdbcBatchWriter;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import io.r2dbc.h2.H2ConnectionFactoryProvider;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PROTOCOL;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PlainBatchGeneratedKeysTest {

    @Test
    void jdbcReturnsOrderedKeysAcrossBuffersWithoutProtectionState() throws Exception {
        assertSuccessfulKeys(false);
    }

    @Test
    void reactiveReturnsOrderedKeysAcrossBuffersWithoutProtectionState() throws Exception {
        assertSuccessfulKeys(true);
    }

    @Test
    void jdbcCallerRollsBackAllBuffersWhenAGeneratedKeyCallbackFails() throws Exception {
        assertCallbackRollback(false);
    }

    @Test
    void reactiveCallerRollsBackAllBuffersWhenAGeneratedKeyCallbackFails() throws Exception {
        assertCallbackRollback(true);
    }

    private static void assertSuccessfulKeys(boolean reactive) throws Exception {
        try (Database database = new Database()) {
            List<Key> callbacks = new ArrayList<>();

            BatchExecutionEvidence result = database.write(reactive, request(callbacks, null));

            assertEquals(BatchExecutionState.SUCCESS, result.state());
            assertEquals(3, result.inputCount());
            assertEquals(BatchAffectedRows.known(3), result.affectedRows());
            assertEquals(List.of(0L, 1L, 2L), result.successfulOffsets().boxed().toList());
            assertEquals(0, result.failedCount());
            assertEquals(List.of(new Key(0, 1), new Key(1, 2), new Key(2, 3)), callbacks);
            assertEquals(List.of("1:first", "2:second", "3:third"), database.storedRows());
        }
    }

    private static void assertCallbackRollback(boolean reactive) throws Exception {
        try (Database database = new Database()) {
            List<Key> callbacks = new ArrayList<>();
            IllegalStateException callbackFailure = new IllegalStateException("generated key callback failed");

            BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                    () -> database.write(reactive, request(callbacks, callbackFailure)));

            assertSame(callbackFailure, failure.getCause());
            assertEquals(BatchExecutionState.PARTIAL, failure.evidence().state());
            assertEquals(3, failure.evidence().inputCount());
            assertEquals(BatchAffectedRows.known(3), failure.evidence().affectedRows());
            assertEquals(3, failure.evidence().successfulCount());
            assertEquals(List.of(new Key(0, 1), new Key(1, 2), new Key(2, 3)), callbacks);
            assertEquals(List.of(), database.storedRows(), "the earlier successful buffer must also roll back");
        }
    }

    private static BatchWriteRequest request(List<Key> callbacks, RuntimeException callbackFailure) {
        return BatchWriteRequests.request(
                "insert into plain_generated_keys(label) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{"first"}, new Object[]{"second"}, new Object[]{"third"}),
                BatchWriteOptions.of(2),
                BatchRowCountPolicy.EXACTLY_ONE,
                BatchGeneratedKeys.required("id", (offset, row) -> {
                    callbacks.add(new Key(offset, ((Number) row.value(0)).longValue()));
                    if (callbackFailure != null && offset == 2L) {
                        throw callbackFailure;
                    }
                }));
    }

    private record Key(long offset, long value) {
    }

    /** Both drivers access an isolated in-process H2 database, discarded when this keeper closes. */
    private static final class Database implements AutoCloseable {
        private final JdbcDataSource dataSource;
        private final Connection keeper;
        private final ConnectionFactory factory;

        private Database() throws Exception {
            String name = "plain_generated_keys_" + UUID.randomUUID().toString().replace("-", "");
            dataSource = new JdbcDataSource();
            dataSource.setURL("jdbc:h2:mem:" + name + ";DATABASE_TO_LOWER=TRUE");
            dataSource.setUser("sa");
            dataSource.setPassword("");
            keeper = dataSource.getConnection();
            try (Statement statement = keeper.createStatement()) {
                statement.execute("create table plain_generated_keys ("
                        + "id bigint generated always as identity primary key, label varchar(20) not null)");
            }
            factory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                    .option(DRIVER, "h2")
                    .option(PROTOCOL, H2ConnectionFactoryProvider.PROTOCOL_MEM)
                    .option(DATABASE, name)
                    .option(USER, "sa")
                    .option(PASSWORD, "")
                    .option(H2ConnectionFactoryProvider.OPTIONS, "DATABASE_TO_LOWER=TRUE")
                    .build());
        }

        private BatchExecutionEvidence write(boolean useReactive, BatchWriteRequest request) throws Exception {
            return useReactive
                    ? ExternalTransactionTestSupport.reactive(factory, access -> R2dbcSqlExecutor.create(access, RdbDialect.h2())
                            .writeBatch(request))
                    : ExternalTransactionTestSupport.jdbc(dataSource, access -> JdbcBatchWriter.create(access, RdbDialect.h2()).writeBatch(request));
        }

        private List<String> storedRows() throws Exception {
            List<String> values = new ArrayList<>();
            try (Statement statement = keeper.createStatement();
                 ResultSet rows = statement.executeQuery("select id, label from plain_generated_keys order by id")) {
                while (rows.next()) {
                    values.add(rows.getLong(1) + ":" + rows.getString(2));
                }
            }
            return values;
        }

        @Override
        public void close() throws Exception {
            keeper.close();
        }
    }
}
