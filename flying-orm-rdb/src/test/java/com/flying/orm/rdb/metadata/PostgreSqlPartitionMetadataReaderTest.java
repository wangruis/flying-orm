package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.TablePartitionDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgreSqlPartitionMetadataReaderTest {

    @Test
    void jdbcAndR2dbcReadTheSameRangeParentWithoutAnotherCatalogQuery() {
        CountingSyncExecutor syncExecutor = new CountingSyncExecutor(partitionedTable());
        CountingReactiveExecutor reactiveExecutor = new CountingReactiveExecutor(partitionedTable());
        JdbcFormMetadataReader jdbc = new JdbcFormMetadataReader(
                syncExecutor, PostgreSqlReactiveFormMetadataReader.queries());
        ReactiveFormMetadataReader reactive = PostgreSqlReactiveFormMetadataReader.create(reactiveExecutor);

        SchemaSnapshot jdbcSnapshot = jdbc.readSnapshot("jobs", "job_events");
        SchemaSnapshot reactiveSnapshot = reactive.readSnapshot("jobs", "job_events").block();

        TablePartitionDefinition expected = TablePartitionDefinition.range("occurred_at");
        RelationIdentity identity = RelationIdentity.of(null, "jobs", "job_events");
        assertEquals(identity, jdbcSnapshot.identity());
        assertEquals(identity, reactiveSnapshot.identity());
        assertEquals(expected, jdbcSnapshot.partition().value());
        assertEquals(expected, reactiveSnapshot.partition().value());
        assertTrue(jdbcSnapshot.completeTable().isPresent());
        assertTrue(reactiveSnapshot.completeTable().isPresent());
        assertEquals(7, syncExecutor.queries.size());
        assertEquals(7, reactiveExecutor.queries.size());
    }

    @Test
    void ordinaryTopLevelTablePublishesAnExplicitAbsentPartition() {
        SchemaSnapshot snapshot = convert(tableRow(
                "TABLE_REPRESENTABLE", true,
                "TABLE_PARTITIONED", false));

        assertEquals(SchemaSnapshot.State.ABSENT, snapshot.partition().state());
        assertTrue(snapshot.completeTable().isPresent());
    }

    @Test
    void catalogQueryCarriesTheSupportedParentFactAndDoesNotRejectItsChildren() {
        String sql = PostgreSqlReactiveFormMetadataReader.queries()
                .tableQuery().create("jobs", "job_events").sql();

        assertTrue(sql.contains("pg_catalog.pg_partitioned_table"));
        assertTrue(sql.contains("partitioning.partstrat = 'r'"));
        assertTrue(sql.contains("partitioning.partnatts = 1"));
        assertTrue(sql.contains("partitioning.partexprs is null"));
        assertTrue(sql.contains("partitioning.partclass[0]"));
        assertTrue(sql.contains("partitioning.partcollation[0]"));
        assertTrue(sql.contains("pg_catalog.pg_opclass partition_opclass"));
        assertTrue(sql.contains("pg_catalog.pg_am partition_access_method"));
        assertTrue(sql.contains("partition_opclass.opcdefault"));
        assertTrue(sql.contains("partition_access_method.amname = 'btree'"));
        assertTrue(sql.contains("partition_attribute.attisdropped"));
        assertTrue(sql.contains("inheritance.inhrelid = t.oid"));
        assertTrue(sql.contains("t.relkind = 'p'"));
        int supportedParent = sql.indexOf("when t.relkind = 'p' then coalesce(");
        int supportedParentEnd = sql.indexOf("else false", supportedParent);
        assertTrue(supportedParent >= 0 && supportedParentEnd > supportedParent);
        assertFalse(sql.substring(supportedParent, supportedParentEnd).contains("inhparent"));
    }

    @Test
    void childHashListExpressionMultiKeyAndMalformedParentsFailClosed() {
        for (Map<String, Object> table : List.of(
                tableRow("TABLE_REPRESENTABLE", false,
                         "UNSUPPORTED_TABLE_REASON", "partition child"),
                tableRow("TABLE_REPRESENTABLE", false,
                         "UNSUPPORTED_TABLE_REASON", "non-range partition strategy"),
                tableRow("TABLE_REPRESENTABLE", false,
                         "UNSUPPORTED_TABLE_REASON", "partition expression"),
                tableRow("TABLE_REPRESENTABLE", false,
                         "UNSUPPORTED_TABLE_REASON", "multiple partition keys"),
                tableRow("TABLE_REPRESENTABLE", false,
                         "UNSUPPORTED_TABLE_REASON", "non-default partition operator class"),
                tableRow("TABLE_REPRESENTABLE", false,
                         "UNSUPPORTED_TABLE_REASON", "non-default partition collation"),
                tableRow("TABLE_REPRESENTABLE", true,
                         "TABLE_PARTITIONED", true,
                         "PARTITION_STRATEGY", "HASH",
                         "PARTITION_COLUMN", "occurred_at"),
                tableRow("TABLE_REPRESENTABLE", true,
                         "TABLE_PARTITIONED", true,
                         "PARTITION_STRATEGY", "RANGE"))) {
            assertThrows(IllegalStateException.class, () -> convert(table));
        }
    }

    private static SchemaSnapshot convert(Map<String, Object> tableRow) {
        return FormMetadataRowConverter.toCompleteSchemaSnapshot(
                RelationIdentity.of(null, "jobs", "job_events"),
                columnRows(),
                List.of(tableRow),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                value -> value,
                InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL);
    }

    private static Map<String, Object> partitionedTable() {
        return tableRow("TABLE_REPRESENTABLE", true,
                        "TABLE_PARTITIONED", true,
                        "PARTITION_STRATEGY", "RANGE",
                        "PARTITION_COLUMN", "occurred_at");
    }

    private static List<DynamicRow> rows(SqlRequest request, Map<String, Object> tableRow) {
        String sql = request.sql();
        if (sql.contains("information_schema.columns")) {
            return columnRows().stream().map(DynamicRow::copyOf).toList();
        }
        if (sql.contains("obj_description")) {
            return List.of(DynamicRow.copyOf(tableRow));
        }
        return List.of();
    }

    private static List<Map<String, Object>> columnRows() {
        return List.of(column("id", "bigint"), column("occurred_at", "timestamp"));
    }

    private static Map<String, Object> column(String name, String type) {
        return tableRow("COLUMN_NAME", name,
                        "DATA_TYPE", type,
                        "NULLABLE", false,
                        "COLUMN_REPRESENTABLE", true,
                        "IS_IDENTITY", false,
                        "PRIMARY_KEY", false);
    }

    private static Map<String, Object> tableRow(Object... entries) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("TABLE_COMMENT", null);
        for (int index = 0; index < entries.length; index += 2) {
            row.put((String) entries[index], entries[index + 1]);
        }
        return row;
    }

    private static final class CountingSyncExecutor implements SyncSqlExecutor {
        private final Map<String, Object> tableRow;
        private final List<SqlRequest> queries = new ArrayList<>();

        private CountingSyncExecutor(Map<String, Object> tableRow) {
            this.tableRow = tableRow;
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            queries.add(request);
            return rows(request, tableRow);
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(
                SqlRequest request, SqlExecutionOptions options) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class CountingReactiveExecutor implements ReactiveSqlExecutor {
        private final Map<String, Object> tableRow;
        private final List<SqlRequest> queries = new ArrayList<>();

        private CountingReactiveExecutor(Map<String, Object> tableRow) {
            this.tableRow = tableRow;
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            queries.add(request);
            return Flux.fromIterable(rows(request, tableRow));
        }

        @Override
        public reactor.core.publisher.Mono<Long> rowsUpdated(SqlRequest request) {
            return reactor.core.publisher.Mono.error(new UnsupportedOperationException());
        }
    }
}
