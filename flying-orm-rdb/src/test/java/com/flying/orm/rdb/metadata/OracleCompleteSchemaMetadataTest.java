package com.flying.orm.rdb.metadata;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OracleCompleteSchemaMetadataTest {

    @Test
    void declaresAndProjectsEveryCompleteSnapshotFact() {
        InformationSchemaFormMetadataReader.Queries queries = OracleReactiveFormMetadataReader.queries();

        assertTrue(InformationSchemaFormMetadataReader.coverage(queries).isComplete());
        assertEquals(InformationSchemaFormMetadataReader.SnapshotDialect.ORACLE,
                     queries.snapshotDialect());
        assertProjection(queries.tableQuery().create("app", "orders"),
                         "TABLE_COMMENT", "TABLE_REPRESENTABLE");
        assertProjection(queries.columnQuery().create("app", "orders"),
                         "COLUMN_DEFAULT", "GENERATION_START", "GENERATION_INCREMENT",
                         "GENERATION_CACHE", "GENERATION_SEQUENCE_NAME",
                         "COLUMN_COLLATION", "COLUMN_REPRESENTABLE");
        String columnSql = queries.columnQuery().create("app", "orders").sql();
        assertTrue(columnSql.contains("from ALL_TAB_COLS c"));
        assertFalse(columnSql.contains("from ALL_TAB_COLUMNS c"));
        assertTrue(columnSql.contains("c.HIDDEN_COLUMN"));
        assertFalse(columnSql.contains("INVISIBLE_COLUMN"));
        assertFalse(queries.tableQuery().create("app", "orders").sql()
                           .contains("INVISIBLE_COLUMN"));
        assertTrue(columnSql.contains("ALL_IND_COLUMNS hidden_index_column"));
        assertTrue(columnSql.contains("ALL_SEQUENCES column_sequence"));
        assertTrue(columnSql.contains("[[flying-orm:v1:SEQUENCE:"));
        assertTrue(queries.tableQuery().create("app", "orders").sql()
                          .contains("ALL_IND_COLUMNS hidden_index_column"));
        assertTrue(columnSql.contains("then 'BIGINT'"));
        assertTrue(columnSql.contains("then 'INTEGER'"));
        assertTrue(columnSql.contains("then 'BOOLEAN'"));
        assertProjection(queries.primaryKeyQuery().create("app", "orders"),
                         "CONSTRAINT_NAME", "CONSTRAINT_REPRESENTABLE");
        assertProjection(queries.uniqueConstraintQuery().create("app", "orders"),
                         "CONSTRAINT_NAME", "CONSTRAINT_REPRESENTABLE");
        assertProjection(queries.indexQuery().create("app", "orders"),
                         "INDEX_DIRECTION", "INDEX_EXPRESSION", "INDEX_REPRESENTABLE");
        assertTrue(queries.indexQuery().create("app", "orders").sql()
                          .contains("index_expression.COLUMN_EXPRESSION"));
        assertTrue(queries.indexQuery().create("app", "orders").sql()
                          .contains("i.INDEX_TYPE in ('NORMAL', 'FUNCTION-BASED NORMAL')"));
        assertProjection(queries.foreignKeyQuery().create("app", "orders"),
                         "ON_DELETE", "ON_UPDATE", "CONSTRAINT_REPRESENTABLE");
        assertProjection(queries.checkConstraintQuery().create("app", "orders"),
                         "CHECK_EXPRESSION", "CHECK_REPRESENTABLE");
    }

    @Test
    void rejectsStandaloneSequenceDefaultsWhenAnySequenceOptionIsMissing() {
        for (String missing : List.of(
                "GENERATION_START", "GENERATION_INCREMENT", "GENERATION_CACHE")) {
            OracleReactiveFormMetadataReader reader = OracleReactiveFormMetadataReader.create(
                    new StandaloneSequenceExecutor(missing));

            IllegalStateException error = assertThrows(
                    IllegalStateException.class,
                    () -> reader.readSnapshot("app", "orders").block());

            assertTrue(error.getMessage().contains(missing));
        }
    }

    @Test
    void readsAStandaloneSequenceWhenEveryStableCreationOptionIsPresent() {
        OracleReactiveFormMetadataReader reader = OracleReactiveFormMetadataReader.create(
                new StandaloneSequenceExecutor(null));

        var snapshot = reader.readSnapshot("APP", "orders").block();

        assertNotNull(snapshot);
        assertEquals(ValueGeneration.sequence("ORDERS_SEQ", 1, 1, 100),
                     snapshot.columns().value().getFirst().generation());
    }

    @Test
    void rejectsASequenceMarkerThatDoesNotMatchTheColumnDefault() {
        OracleReactiveFormMetadataReader reader = OracleReactiveFormMetadataReader.create(
                new StandaloneSequenceExecutor(null, "APP.OTHER_SEQ"));

        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> reader.readSnapshot("APP", "orders").block());

        assertTrue(error.getMessage().contains("does not match"));
    }

    @Test
    void bindsTableAndOwnerForEveryCompleteQuery() {
        InformationSchemaFormMetadataReader.Queries queries = OracleReactiveFormMetadataReader.queries();
        assertNotNull(queries.tableQuery());
        assertNotNull(queries.primaryKeyQuery());
        assertNotNull(queries.uniqueConstraintQuery());
        assertNotNull(queries.checkConstraintQuery());
        List<InformationSchemaFormMetadataReader.Query> completeQueries = List.of(
                queries.columnQuery(), queries.tableQuery(), queries.primaryKeyQuery(),
                queries.uniqueConstraintQuery(), queries.indexQuery(), queries.foreignKeyQuery(),
                queries.checkConstraintQuery());

        for (InformationSchemaFormMetadataReader.Query query : completeQueries) {
            SqlRequest request = query.create("app", "orders");
            assertEquals(List.of("orders", "orders", "orders", "app", "app", "app"),
                         request.parameters());
        }
    }

    private static void assertProjection(SqlRequest request, String... aliases) {
        for (String alias : aliases) {
            assertTrue(request.sql().contains(alias), alias + " missing from " + request.sql());
        }
    }

    private static final class StandaloneSequenceExecutor implements ReactiveSqlExecutor {

        private final String missingOption;
        private final String sequenceMarker;

        private StandaloneSequenceExecutor(String missingOption) {
            this(missingOption, "APP.ORDERS_SEQ");
        }

        private StandaloneSequenceExecutor(String missingOption, String sequenceMarker) {
            this.missingOption = missingOption;
            this.sequenceMarker = sequenceMarker;
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            if (request.sql().contains("from ALL_TAB_COLS c")) {
                Map<String, Object> column = new LinkedHashMap<>();
                column.put("COLUMN_NAME", "id");
                column.put("RESOLUTION_SCHEMA", "APP");
                column.put("DATA_TYPE", "BIGINT");
                column.put("NUMERIC_PRECISION", 19);
                column.put("NUMERIC_SCALE", 0);
                column.put("NULLABLE", "N");
                column.put("COLUMN_REPRESENTABLE", "true");
                column.put("UNSUPPORTED_COLUMN_REASON", null);
                column.put("COLUMN_DEFAULT", "\"APP\".\"ORDERS_SEQ\".NEXTVAL");
                column.put("GENERATION_EXPRESSION", "\"APP\".\"ORDERS_SEQ\".NEXTVAL");
                column.put("GENERATION_SEQUENCE_NAME", sequenceMarker);
                addOption(column, "GENERATION_START", 1L);
                addOption(column, "GENERATION_INCREMENT", 1L);
                addOption(column, "GENERATION_CACHE", 100);
                column.put("IS_IDENTITY", "false");
                return Flux.just(DynamicRow.copyOf(column));
            }
            if (request.sql().contains("from ALL_TABLES t")) {
                Map<String, Object> table = new LinkedHashMap<>();
                table.put("TABLE_COMMENT", null);
                table.put("TABLE_REPRESENTABLE", "true");
                return Flux.just(DynamicRow.copyOf(table));
            }
            return Flux.empty();
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            return Mono.error(new UnsupportedOperationException());
        }

        private void addOption(Map<String, Object> row, String name, Object value) {
            if (!name.equals(missingOption)) {
                row.put(name, value);
            }
        }
    }
}
