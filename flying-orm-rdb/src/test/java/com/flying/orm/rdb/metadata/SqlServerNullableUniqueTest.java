package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.UniqueNullPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import com.flying.orm.rdb.schema.SchemaDiffer;
import com.flying.orm.rdb.schema.RelationalSchemaSqlRenderer;
import com.flying.orm.rdb.schema.SchemaCompatibilityMode;
import com.flying.orm.rdb.schema.SchemaOperation;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SqlServerNullableUniqueTest {

    private static final RelationIdentity TABLE = RelationIdentity.of(null, "dbo", "accounts");

    @Test
    void createsOnlyNonNullUniqueKeysAndReadsTheSameRelationshipBack() {
        RelationalTableDefinition desired = table(UniqueNullPolicy.DISTINCT);
        var requests = RelationalSchemaSqlRenderer.create(RdbDialect.sqlServer().schema()).render(
                SchemaOperation.of(SchemaOperation.Kind.CREATE_TABLE, TABLE, TABLE.table(), null,
                        desired, SchemaOperation.Compatibility.REQUIRES_REVIEW));
        assertEquals(2, requests.size());
        assertFalse(requests.getFirst().sql().contains("unique"));
        assertEquals("create unique index [uq_email] on [dbo].[accounts] ([email]) where [email] is not null",
                requests.getLast().sql());

        SchemaSnapshot actual = snapshot(List.of(indexRow("([email] IS NOT NULL)", true, "ASC")));
        assertEquals(desired.uniqueConstraints(), actual.completeTable().orElseThrow().uniqueConstraints());
        assertTrue(actual.completeTable().orElseThrow().indexes().isEmpty());
        var reviewed = SchemaDiffer.diff(desired, actual, RdbDialect.sqlServer().capabilities(),
                SchemaCompatibilityMode.EXACT);
        assertTrue(reviewed.operations().isEmpty());
    }

    @Test
    void defaultUniqueKeepsItsOldDdlAndCannotEqualAnExplicitNullDistinctIndex() {
        RelationalTableDefinition desired = table(UniqueNullPolicy.DEFAULT);
        var requests = RelationalSchemaSqlRenderer.create(RdbDialect.sqlServer().schema()).render(
                SchemaOperation.of(SchemaOperation.Kind.CREATE_TABLE, TABLE, TABLE.table(), null,
                        desired, SchemaOperation.Compatibility.REQUIRES_REVIEW));
        assertEquals(1, requests.size());
        assertTrue(requests.getFirst().sql().contains("constraint [uq_email] unique ([email])"));
        var reviewed = SchemaDiffer.diff(desired,
                snapshot(List.of(indexRow("([email] IS NOT NULL)", true, "ASC"))),
                RdbDialect.sqlServer().capabilities(), SchemaCompatibilityMode.EXACT);
        assertFalse(reviewed.operations().isEmpty());
    }

    @Test
    void rejectsOtherFiltersAndUnobservableOrNonUniqueIndexFacts() {
        for (String filter : List.of("[email] IS NULL", "[email] IS NOT NULL OR 1=1",
                "[other] IS NOT NULL", "[email] <> ''", "")) {
            assertThrows(IllegalStateException.class,
                    () -> snapshot(List.of(indexRow(filter, true, "ASC"))), filter);
        }
        assertThrows(IllegalStateException.class,
                () -> snapshot(List.of(indexRow("[email] IS NOT NULL", false, "ASC"))));
        assertThrows(IllegalStateException.class,
                () -> snapshot(List.of(indexRow("[email] IS NOT NULL", true, "DESC"))));
    }

    @Test
    void reportsFilterFactsFromTheBuiltInQueryWithoutRemovingOtherIndexChecks() {
        String sql = SqlServerMetadataQueries.queries().indexQuery().create("dbo", "accounts").sql();
        assertTrue(sql.contains("i.has_filter as INDEX_FILTERED"));
        assertTrue(sql.contains("i.filter_definition as INDEX_FILTER"));
        assertTrue(sql.contains("i.is_disabled = 0"));
        assertTrue(sql.contains("included.is_included_column = 1"));
    }

    @Test
    void legacyMetadataDoesNotMisrepresentAFilteredKeyAsAnUnfilteredIndex() {
        DynamicForm form = DynamicForm.builder("accounts", "accounts")
                .addField(DynamicField.of("email", "VARCHAR")).build();
        assertThrows(IllegalStateException.class, () -> FormMetadataRowConverter.toTableMetadata(
                "accounts", form, List.of(indexRow("[email] IS NOT NULL", true, "ASC")), List.of()));
    }

    @Test
    void jdbcAndReactivePublicReadersAndReviewersPreserveTheSameNullPolicy() {
        SyncSqlExecutor sync = new CatalogExecutor();
        ReactiveSqlExecutor reactive = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.defer(() -> Flux.fromIterable(catalog(request)));
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.error(new AssertionError("read-only verification must not execute DDL"));
            }
        };
        RdbDialect dialect = RdbDialect.sqlServer();
        var jdbcReader = JdbcFormMetadataReaders.create(sync, dialect);
        var reactiveReader = ReactiveFormMetadataReaders.create(reactive, dialect);
        for (SchemaSnapshot observed : List.of(jdbcReader.readSnapshot("dbo", "accounts"),
                reactiveReader.readSnapshot("dbo", "accounts").block())) {
            assertEquals(table(UniqueNullPolicy.DISTINCT).uniqueConstraints(),
                    observed.completeTable().orElseThrow().uniqueConstraints());
        }
        var database = DatabaseDescriptor.of("SQL Server", "16", dialect);
        var desired = table(UniqueNullPolicy.DISTINCT);
        for (var plan : List.of(JdbcSchemaClient.create(sync, dialect).reviewRelational(
                database, desired, jdbcReader, SchemaCompatibilityMode.EXACT),
                ReactiveSchemaClient.create(reactive, dialect).reviewRelational(
                        database, desired, reactiveReader, SchemaCompatibilityMode.EXACT).block())) {
            assertTrue(plan.operations().isEmpty(), () -> plan.operations().toString());
            assertTrue(plan.requests().isEmpty());
        }
    }

    @Test
    void compositePredicateMustCoverEveryKeyColumnAndOnlyThoseColumns() {
        var first = new java.util.LinkedHashMap<>(indexRow(
                "(([email] IS NOT NULL) AND ([region] IS NOT NULL))", true, "ASC"));
        var second = new java.util.LinkedHashMap<>(first);
        second.put("COLUMN_NAME", "region");
        SchemaSnapshot actual = FormMetadataRowConverter.toCompleteSchemaSnapshot(TABLE,
                List.of(Map.of("COLUMN_NAME", "email", "DATA_TYPE", "VARCHAR", "NULLABLE", true),
                        Map.of("COLUMN_NAME", "region", "DATA_TYPE", "VARCHAR", "NULLABLE", true)),
                List.of(Map.of("TABLE_REPRESENTABLE", true)), List.of(), List.of(), List.of(first, second),
                List.of(), List.of(), value -> value,
                InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER);
        assertEquals(List.of("email", "region"), actual.completeTable().orElseThrow()
                .uniqueConstraints().getFirst().columns());
        second.put("INDEX_FILTER", "[email] IS NOT NULL");
        assertThrows(IllegalStateException.class, () -> snapshot(List.of(first, second)));
    }

    @Test
    void unknownDialectStillCannotGuessDistinctSemantics() {
        assertThrows(UnsupportedOperationException.class,
                () -> RelationalSchemaSqlRenderer.create(com.flying.orm.rdb.schema.SchemaDialect.standard()).render(
                        SchemaOperation.of(SchemaOperation.Kind.CREATE_TABLE, TABLE, TABLE.table(), null,
                                table(UniqueNullPolicy.DISTINCT), SchemaOperation.Compatibility.REQUIRES_REVIEW)));
    }

    private static RelationalTableDefinition table(UniqueNullPolicy policy) {
        return RelationalTableDefinition.builder(TABLE)
                .addColumn(ColumnDefinition.builder("email", "VARCHAR").length(32).build())
                .addUnique(new UniqueConstraintDefinition("uq_email", List.of("email"), policy)).build();
    }

    private static Map<String, Object> indexRow(String filter, boolean unique, String direction) {
        return Map.of("INDEX_NAME", "uq_email", "COLUMN_NAME", "email", "UNIQUE_INDEX", unique,
                "INDEX_REPRESENTABLE", true, "INDEX_DIRECTION", direction,
                "INDEX_FILTERED", true, "INDEX_FILTER", filter);
    }

    private static List<DynamicRow> catalog(SqlRequest request) {
        if (request.sql().contains("from INFORMATION_SCHEMA.COLUMNS c")) {
            return List.of(DynamicRow.copyOf(Map.of("COLUMN_NAME", "email", "DATA_TYPE", "nvarchar",
                    "NULLABLE", true, "CHARACTER_MAXIMUM_LENGTH", 32, "COLUMN_REPRESENTABLE", true)));
        }
        if (request.sql().contains("as INDEX_FILTERED")) {
            return List.of(DynamicRow.copyOf(indexRow("([email] IS NOT NULL)", true, "ASC")));
        }
        return request.sql().contains("TABLE_REPRESENTABLE")
                ? List.of(DynamicRow.copyOf(Map.of("TABLE_REPRESENTABLE", true))) : List.of();
    }

    private static final class CatalogExecutor implements SyncSqlExecutor {
        @Override
        public List<DynamicRow> query(SqlRequest request) {
            return catalog(request);
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new AssertionError("read-only verification must not execute DDL");
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new AssertionError("read-only verification must not execute DDL");
        }
    }

    private static SchemaSnapshot snapshot(List<Map<String, Object>> indexes) {
        return FormMetadataRowConverter.toCompleteSchemaSnapshot(TABLE,
                List.of(Map.of("COLUMN_NAME", "email", "DATA_TYPE", "VARCHAR", "NULLABLE", true,
                        "CHARACTER_MAXIMUM_LENGTH", 32)),
                List.of(Map.of("TABLE_REPRESENTABLE", true)), List.of(), List.of(), indexes,
                List.of(), List.of(), value -> value,
                InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER);
    }
}
