package com.flying.orm.rdb.metadata;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcFormMetadataReaderTest {

    @Test
    void externalTransactionBypassesFormAndTableCache() {
        AtomicInteger queries = new AtomicInteger();
        JdbcFormMetadataReader reader = cachedReader(new BudgetExecutor(queries, externalTransaction()));

        reader.readForm("public.customers", "public", "customers");
        reader.readForm("public.customers", "public", "customers");
        reader.readTable("public", "customers");
        reader.readTable("public", "customers");

        assertEquals(6, queries.get(), "an external transaction must neither read nor write shared metadata cache entries");
    }

    @Test
    void invalidateTableRemovesBothCachedFormsAndTables() {
        AtomicInteger queries = new AtomicInteger();
        JdbcFormMetadataReader reader = cachedReader(new BudgetExecutor(queries));

        reader.readForm("public.customers", "public", "customers");
        reader.readTable("public", "customers");
        assertEquals(3, queries.get());

        reader.invalidate("public", "customers");
        reader.readForm("public.customers", "public", "customers");
        reader.readTable("public", "customers");

        assertEquals(6, queries.get(), "table invalidation must remove every metadata kind for that table");
    }

    @Test
    void readTableDecidesExternalTransactionParticipationOnce() {
        AtomicInteger transactionProbes = new AtomicInteger();
        SyncSqlExecutor executor = new MetadataExecutor(transactionProbes);
        InformationSchemaFormMetadataReader.Queries queries =
                new InformationSchemaFormMetadataReader.Queries(
                        (schema, table) -> new SqlRequest("select columns", List.of()),
                        null,
                        null,
                        type -> type);
        JdbcFormMetadataReader reader = new JdbcFormMetadataReader(executor, queries);

        reader.readTable("public", "customers");

        assertEquals(1, transactionProbes.get());
    }

    @Test
    void readTableRejectsAnEmptyQualifiedTablePart() {
        AtomicInteger transactionProbes = new AtomicInteger();
        JdbcFormMetadataReader reader = new JdbcFormMetadataReader(
                new MetadataExecutor(transactionProbes),
                new InformationSchemaFormMetadataReader.Queries(
                        (schema, table) -> new SqlRequest("select columns", List.of()),
                        null,
                        null,
                        type -> type));

        assertThrows(IllegalArgumentException.class, () -> reader.readTable("public."));
        assertEquals(0, transactionProbes.get());
    }

    @Test
    void schemaSnapshotReadsCurrentFactsWithoutPollutingTheCrudCache() {
        AtomicInteger queries = new AtomicInteger();
        JdbcFormMetadataReader reader = cachedReader(new BudgetExecutor(queries));

        reader.readTable("public", "customers");
        reader.readTable("public", "customers");
        SchemaSnapshot first = reader.readSnapshot("public", "customers");
        SchemaSnapshot second = reader.readSnapshot("public", "customers");

        assertEquals(6, queries.get(), "each schema audit must execute its own column and index reads");
        assertEquals(SchemaSnapshot.State.PRESENT, first.tableState());
        assertEquals(SchemaSnapshot.State.PRESENT, first.indexes().state());
        assertEquals(SchemaSnapshot.State.UNKNOWN, first.foreignKeys().state());
        assertEquals(first.identity(), second.identity());
    }

    private static JdbcFormMetadataReader cachedReader(SyncSqlExecutor executor) {
        return new JdbcFormMetadataReader(
                executor,
                new InformationSchemaFormMetadataReader.Queries(
                        (schema, table) -> new SqlRequest("select columns", List.of()),
                        (schema, table) -> new SqlRequest("select indexes", List.of()),
                        null,
                        type -> type),
                new CacheRegionPolicy(true, 10, 10, Duration.ofMinutes(1), false),
                MetadataCacheInvalidator.none());
    }

    private static JdbcTransactionContext externalTransaction() {
        Connection connection = (Connection) Proxy.newProxyInstance(
                JdbcFormMetadataReaderTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> {
                    throw new AssertionError("the metadata reader must not use the transaction connection directly");
                });
        return JdbcTransactionContext.external(connection);
    }

    private static final class MetadataExecutor implements SyncSqlExecutor {

        private final AtomicInteger transactionProbes;

        private MetadataExecutor(AtomicInteger transactionProbes) {
            this.transactionProbes = transactionProbes;
        }

        @Override
        public Optional<JdbcTransactionContext> currentTransaction() {
            transactionProbes.incrementAndGet();
            return Optional.empty();
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            LinkedHashMap<String, Object> values = new LinkedHashMap<>();
            values.put("COLUMN_NAME", "id");
            values.put("DATA_TYPE", "BIGINT");
            values.put("PRIMARY_KEY", true);
            return List.of(DynamicRow.copyOf(values));
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class BudgetExecutor implements SyncSqlExecutor {

        private final AtomicInteger queries;
        private final Optional<JdbcTransactionContext> transaction;

        private BudgetExecutor(AtomicInteger queries) {
            this(queries, Optional.empty());
        }

        private BudgetExecutor(AtomicInteger queries, JdbcTransactionContext transaction) {
            this(queries, Optional.of(transaction));
        }

        private BudgetExecutor(AtomicInteger queries, Optional<JdbcTransactionContext> transaction) {
            this.queries = queries;
            this.transaction = transaction;
        }

        @Override
        public Optional<JdbcTransactionContext> currentTransaction() {
            return transaction;
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            queries.incrementAndGet();
            if (request.sql().contains("indexes")) {
                return List.of();
            }
            LinkedHashMap<String, Object> values = new LinkedHashMap<>();
            values.put("COLUMN_NAME", "id");
            values.put("DATA_TYPE", "BIGINT");
            values.put("PRIMARY_KEY", true);
            return List.of(DynamicRow.copyOf(values));
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new UnsupportedOperationException();
        }
    }
}
