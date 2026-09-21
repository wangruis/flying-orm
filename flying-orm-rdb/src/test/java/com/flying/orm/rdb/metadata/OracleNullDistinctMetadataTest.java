package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.UniqueNullPolicy;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OracleNullDistinctMetadataTest {

    private static final RelationIdentity TABLE = RelationIdentity.of(null, "APP", "accounts");
    private static final String GATE = "\"Email\" IS NOT NULL AND \"region\" IS NOT NULL";
    private static final String FIRST = "CASE WHEN (" + GATE + ") THEN \"Email\" END";
    private static final String SECOND = "case when ((\"Email\" is not null) and (\"region\" is not null))"
            + " then \"region\" else null end";

    @Test
    void jdbcAndReactiveReadersRecoverTheOriginalCompositeKeyWithoutSystemColumns() {
        CatalogExecutor catalog = new CatalogExecutor();
        var jdbc = JdbcFormMetadataReaders.create(catalog, RdbDialect.oracle());
        var reactive = ReactiveFormMetadataReaders.create(catalog.reactive(), RdbDialect.oracle());
        for (SchemaSnapshot snapshot : List.of(jdbc.readSnapshot(TABLE), reactive.readSnapshot(TABLE).block())) {
            var table = snapshot.completeTable().orElseThrow();
            assertEquals(List.of("Email", "region"), table.columns().stream().map(column -> column.name()).toList());
            assertEquals(List.of(new UniqueConstraintDefinition(
                    "uq_accounts", List.of("Email", "region"), UniqueNullPolicy.DISTINCT)),
                    table.uniqueConstraints());
            assertTrue(table.indexes().isEmpty());
        }
    }

    @Test
    void rejectsIncompleteOrDifferentCaseSemanticsAndUnknownSourceColumns() {
        for (String expression : List.of(
                "CASE WHEN \"Email\" IS NOT NULL THEN \"Email\" END",
                "CASE WHEN \"Email\" IS NOT NULL OR \"region\" IS NOT NULL THEN \"Email\" END",
                "CASE WHEN " + GATE + " THEN \"Email\" ELSE 'missing' END",
                "CASE WHEN " + GATE + " THEN \"EMAIL\" END",
                "CASE WHEN \"Email\" IS NOT NULL AND \"other\" IS NOT NULL THEN \"Email\" END",
                "CASE WHEN " + GATE + " THEN LOWER(\"Email\") END")) {
            assertThrows(IllegalStateException.class, () -> snapshot(indexes(expression, SECOND)), expression);
        }
        assertThrows(IllegalStateException.class, () -> snapshot(indexes(FIRST,
                "CASE WHEN " + GATE + " THEN \"Email\" END")));
    }

    @Test
    void rejectsNonUniqueDescendingMixedAndAdditionalUnknownExpressionIndexes() {
        for (Map<String, Object> replacement : List.of(
                changed(indexes(FIRST, SECOND).getFirst(), "UNIQUE_INDEX", false),
                changed(indexes(FIRST, SECOND).getFirst(), "INDEX_DIRECTION", "DESC"),
                changed(indexes(FIRST, SECOND).getFirst(), "INDEX_EXPRESSION", "\"Email\""))) {
            assertThrows(IllegalStateException.class,
                    () -> snapshot(List.of(replacement, indexes(FIRST, SECOND).getLast())));
        }
        List<Map<String, Object>> rows = new ArrayList<>(indexes(FIRST, SECOND));
        rows.add(changed(indexRow("LOWER(\"Email\")", 3), "INDEX_NAME", "ix_other"));
        assertThrows(IllegalStateException.class, () -> snapshot(rows));
    }

    @Test
    void hiddenColumnQueriesRequireSystemGeneratedIndexOwnershipInBothVersionProfiles() {
        for (var queries : List.of(OracleMetadataQueries.queries(), OracleMetadataQueries.queries12c())) {
            String columns = queries.columnQuery().create("APP", "accounts").sql();
            String table = queries.tableQuery().create("APP", "accounts").sql();
            assertTrue(columns.contains("c.USER_GENERATED = 'NO'"));
            assertTrue(table.contains("hidden_column.USER_GENERATED = 'NO'"));
            for (String sql : List.of(columns, table)) {
                assertTrue(sql.contains("ALL_IND_EXPRESSIONS hidden_index_expression"));
                assertTrue(sql.contains("hidden_index_expression.INDEX_NAME = hidden_index_column.INDEX_NAME"));
                assertTrue(sql.contains("hidden_index_expression.COLUMN_POSITION = hidden_index_column.COLUMN_POSITION"));
            }
            assertTrue(queries.indexQuery().create("APP", "accounts").sql()
                    .contains("i.FUNCIDX_STATUS = 'ENABLED'"));
            assertFalse(queries.indexQuery().create("APP", "accounts").sql().contains("regexp_substr"));
        }
    }

    @Test
    void legacyReadersRejectCaseIndexesAtTheRelationalBoundary() {
        CatalogExecutor catalog = new CatalogExecutor();
        IllegalStateException jdbc = assertThrows(IllegalStateException.class,
                () -> JdbcFormMetadataReaders.create(catalog, RdbDialect.oracle()).readTable("APP", "accounts"));
        IllegalStateException reactive = assertThrows(IllegalStateException.class,
                () -> ReactiveFormMetadataReaders.create(catalog.reactive(), RdbDialect.oracle())
                        .readTable("APP", "accounts").block());
        assertTrue(jdbc.getMessage().contains("complete relational metadata"));
        assertTrue(reactive.getMessage().contains("complete relational metadata"));
    }

    private static SchemaSnapshot snapshot(List<Map<String, Object>> indexes) {
        var queries = OracleMetadataQueries.queries();
        return FormMetadataRowConverter.toCompleteSchemaSnapshot(TABLE, columns(),
                List.of(Map.of("TABLE_REPRESENTABLE", true)), List.of(), List.of(), indexes,
                List.of(), List.of(), queries.typeMapper(), queries.snapshotTypeMapper(),
                InformationSchemaFormMetadataReader.SnapshotDialect.ORACLE);
    }

    private static List<Map<String, Object>> columns() {
        return List.of(column("Email"), column("region"));
    }

    private static Map<String, Object> column(String name) {
        return Map.of("COLUMN_NAME", name, "DATA_TYPE", "VARCHAR2", "NULLABLE", "Y",
                "CHARACTER_MAXIMUM_LENGTH", 32, "COLUMN_REPRESENTABLE", true);
    }

    private static List<Map<String, Object>> indexes(String first, String second) {
        return List.of(indexRow(first, 1), indexRow(second, 2));
    }

    private static Map<String, Object> indexRow(String expression, int position) {
        return Map.of("INDEX_NAME", "uq_accounts", "COLUMN_NAME", "SYS_NC0000" + position + "$",
                "INDEX_EXPRESSION", expression, "UNIQUE_INDEX", true,
                "INDEX_REPRESENTABLE", true, "INDEX_DIRECTION", "ASC");
    }

    private static Map<String, Object> changed(Map<String, Object> source, String name, Object value) {
        Map<String, Object> row = new LinkedHashMap<>(source);
        row.put(name, value);
        return row;
    }

    private static final class CatalogExecutor implements SyncSqlExecutor {
        @Override
        public List<DynamicRow> query(SqlRequest request) {
            List<Map<String, Object>> rows;
            if (request.sql().contains("from ALL_TAB_COLS c")) {
                rows = columns();
            } else if (request.sql().contains("from ALL_INDEXES i")) {
                rows = indexes(FIRST, SECOND);
            } else {
                rows = request.sql().contains("from ALL_TABLES t")
                        ? List.of(Map.of("TABLE_REPRESENTABLE", true)) : List.of();
            }
            return rows.stream().map(DynamicRow::copyOf).toList();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new AssertionError("metadata verification must not execute writes");
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new AssertionError("metadata verification must not execute writes");
        }

        private ReactiveSqlExecutor reactive() {
            return new ReactiveSqlExecutor() {
                @Override
                public Flux<DynamicRow> query(SqlRequest request) {
                    return Flux.defer(() -> Flux.fromIterable(CatalogExecutor.this.query(request)));
                }

                @Override
                public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.error(new AssertionError("metadata verification must not execute writes"));
                }
            };
        }
    }
}
