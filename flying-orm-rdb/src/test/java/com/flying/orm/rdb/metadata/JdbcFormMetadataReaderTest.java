package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcFormMetadataReaderTest {

    @Test
    void callerCanDisableFormAndTableCaching() {
        AtomicInteger queries = new AtomicInteger();
        JdbcFormMetadataReader reader = reader(new BudgetExecutor(queries), CacheRegionPolicy.disabled());

        reader.readForm("public.customers", "public", "customers");
        reader.readForm("public.customers", "public", "customers");
        reader.readTable("public", "customers");
        reader.readTable("public", "customers");

        assertEquals(6, queries.get(), "disabled caching must neither read nor write shared metadata entries");
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
    void qualifiedInvalidationAlsoRemovesUnqualifiedFormAndTableEntries() {
        AtomicInteger queries = new AtomicInteger();
        JdbcFormMetadataReader reader = cachedReader(new BudgetExecutor(queries));

        reader.readForm("customers", "customers");
        reader.readTable("customers");
        assertEquals(3, queries.get());

        reader.invalidate("public", "customers");
        reader.readForm("customers", "customers");
        reader.readTable("customers");

        assertEquals(6, queries.get(),
                     "qualified DDL must also evict the default-schema alias of the same table");
    }

    @Test
    void readTableNeverQueriesTransactionState() {
        AtomicInteger queries = new AtomicInteger();
        SyncSqlExecutor executor = new MetadataExecutor(queries);
        InformationSchemaFormMetadataReader.Queries metadataQueries =
                new InformationSchemaFormMetadataReader.Queries(
                        (schema, table) -> new SqlRequest("select columns", List.of()),
                        null,
                        null,
                        type -> type);
        JdbcFormMetadataReader reader = new JdbcFormMetadataReader(executor, metadataQueries);

        reader.readTable("public", "customers");

        assertEquals(1, queries.get());
        org.junit.jupiter.api.Assertions.assertFalse(java.util.Arrays.stream(SyncSqlExecutor.class.getMethods())
                .anyMatch(method -> method.getName().toLowerCase().contains("transaction")));
    }

    @Test
    void readTableRejectsAnEmptyQualifiedTablePart() {
        AtomicInteger queries = new AtomicInteger();
        JdbcFormMetadataReader reader = new JdbcFormMetadataReader(
                new MetadataExecutor(queries),
                new InformationSchemaFormMetadataReader.Queries(
                        (schema, table) -> new SqlRequest("select columns", List.of()),
                        null,
                        null,
                        type -> type));

        assertThrows(IllegalArgumentException.class, () -> reader.readTable("public."));
        assertEquals(0, queries.get());
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

    @Test
    void relationalSnapshotDoesNotSplitAnUnqualifiedLiteralDotTable() {
        AtomicReference<String> requestedSchema = new AtomicReference<>();
        AtomicReference<String> requestedTable = new AtomicReference<>();
        JdbcFormMetadataReader reader = new JdbcFormMetadataReader(
                new MetadataExecutor(new AtomicInteger()),
                new InformationSchemaFormMetadataReader.Queries(
                        (schema, table) -> {
                            requestedSchema.set(schema);
                            requestedTable.set(table);
                            return new SqlRequest("select columns", List.of());
                        },
                        null,
                        null,
                        type -> type));
        RelationIdentity target = RelationIdentity.table("accounts.v2");

        SchemaSnapshot snapshot = reader.readSnapshot(target);

        assertNull(requestedSchema.get());
        assertEquals("accounts.v2", requestedTable.get());
        assertEquals(target, snapshot.identity());
    }

    private static JdbcFormMetadataReader cachedReader(SyncSqlExecutor executor) {
        return reader(executor, new CacheRegionPolicy(true, 10, 10, Duration.ofMinutes(1), false));
    }

    private static JdbcFormMetadataReader reader(SyncSqlExecutor executor, CacheRegionPolicy policy) {
        return new JdbcFormMetadataReader(
                executor,
                new InformationSchemaFormMetadataReader.Queries(
                        (schema, table) -> new SqlRequest("select columns", List.of()),
                        (schema, table) -> new SqlRequest("select indexes", List.of()),
                        null,
                        type -> type),
                policy,
                MetadataCacheInvalidator.none());
    }

    private static final class MetadataExecutor implements SyncSqlExecutor {

        private final AtomicInteger queries;

        private MetadataExecutor(AtomicInteger queries) {
            this.queries = queries;
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            queries.incrementAndGet();
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
        private BudgetExecutor(AtomicInteger queries) {
            this.queries = queries;
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
