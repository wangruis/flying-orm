package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FormMetadataTableProjectionTest {

    @Test
    void ordinaryMetadataKeepsNativeColumnArgumentsAcrossReadShapes() {
        RelationIdentity relation = RelationIdentity.table("items");
        List<Map<String, Object>> rows = List.of(
                Map.of("COLUMN_NAME", "code", "DATA_TYPE", "CHAR", "CHARACTER_MAXIMUM_LENGTH", 12,
                        "PRIMARY_KEY", true, "NULLABLE", true),
                Map.of("COLUMN_NAME", "label", "DATA_TYPE", "NCHAR", "CHARACTER_MAXIMUM_LENGTH", 24),
                Map.of("COLUMN_NAME", "payload", "DATA_TYPE", "VARBINARY", "CHARACTER_MAXIMUM_LENGTH", 64),
                Map.of("COLUMN_NAME", "amount", "DATA_TYPE", "NUMBER",
                        "NUMERIC_PRECISION", 18, "NUMERIC_SCALE", 4));
        DynamicForm form = FormMetadataRowConverter.toDynamicForm("items", relation, rows, value -> value);
        RelationalTableDefinition table = FormMetadataRowConverter.tableBuilder(relation, rows, value -> value).build();
        SchemaSnapshot snapshot = FormMetadataRowConverter.toSchemaSnapshot(
                relation, rows, List.of(), false, List.of(), false, value -> value);

        assertEquals(12, form.field("code").length());
        assertEquals(24, form.field("label").length());
        assertEquals(64, form.field("payload").length());
        assertEquals(18, form.field("amount").precision());
        assertEquals(4, form.field("amount").scale());
        assertEquals(false, form.field("code").nullable());
        assertEquals(List.of("code"), table.primaryKey().orElseThrow().columns());
        assertEquals(form.toTableDefinition().columns(), table.columns());
        assertEquals(table.columns(), snapshot.columns().value());
    }

    @Test
    void explicitSchemaIsPreservedForForeignKeysAcrossBothReadersAndReadShapes() {
        MetadataQueryProfile profile = new MetadataQueryProfile(new MetadataQueryProfile.Queries(
                (schema, table) -> new SqlRequest("columns", List.of()), null,
                (schema, table) -> new SqlRequest("foreignKeys", List.of()), value -> value));
        SyncSqlExecutor executor = new com.flying.orm.rdb.support.ListQueryFixture() {
            public long rowsUpdated(SqlRequest request) {
                throw new AssertionError("no writes expected");
            }
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options,
                                                          String generatedKeyColumn) {
                throw new AssertionError("no writes expected");
            }
            @Override
            public List<DynamicRow> query(SqlRequest request) {
                return request.sql().equals("columns")
                        ? List.of(DynamicRow.copyOf(Map.of("COLUMN_NAME", "parent_id", "DATA_TYPE", "BIGINT",
                                "METADATA_TABLE", "orders")))
                        : List.of(DynamicRow.copyOf(Map.of(
                                "FOREIGN_KEY_NAME", "fk_parent", "COLUMN_NAME", "parent_id",
                                "TABLE_SCHEMA", "app", "METADATA_TABLE", "orders",
                                "REFERENCED_TABLE_SCHEMA", "app", "REFERENCED_TABLE_NAME", "parents",
                                "REFERENCED_COLUMN_NAME", "id")));
            }
        };
        DefaultJdbcFormMetadataReader jdbc = new DefaultJdbcFormMetadataReader(executor, profile,
                com.flying.orm.rdb.cache.CacheRegionPolicy.disabled(), MetadataCacheInvalidator.none());
        InformationSchemaFormMetadataReader reactive = new InformationSchemaFormMetadataReader(
                new ReactiveSqlExecutor() {
                    public Flux<DynamicRow> query(SqlRequest request) {
                        return Flux.fromIterable(executor.query(request));
                    }
                    public Mono<Long> rowsUpdated(SqlRequest request) {
                        return Mono.error(new AssertionError("no writes expected"));
                    }
                }, profile);
        RelationIdentity expected = RelationIdentity.of(null, "app", "parents");
        assertEquals(expected, jdbc.readTable("app", "orders").foreignKeys().getFirst().reference());
        assertEquals(expected, reactive.readTable("app", "orders").block().foreignKeys().getFirst().reference());
        assertEquals(expected, jdbc.readTables("app").getFirst().foreignKeys().getFirst().reference());
        assertEquals(expected, reactive.readTables("app").blockFirst().foreignKeys().getFirst().reference());
        assertEquals(expected, jdbc.readSnapshot("app", "orders").foreignKeys().value().getFirst().reference());
        assertEquals(expected, reactive.readSnapshot("app", "orders").block().foreignKeys().value().getFirst().reference());
    }

    @Test
    void customReadersKeepTheSameIdentityInTableAndPartialSnapshot() {
        DynamicForm form = DynamicForm.builder("items", "items").addField(DynamicField.of("code", "BIGINT")).build();
        JdbcFormMetadataReader jdbc = new JdbcFormMetadataReader() {
            public DynamicForm readForm(String formId, RelationIdentity relation) {
                throw new AssertionError("table facts must not read a form");
            }

            @Override
            public RelationalTableDefinition readTable(RelationIdentity relation) {
                return RelationalTableDefinition.builder(relation).addColumn(form.field("code").column()).build();
            }

        };
        ReactiveFormMetadataReader reactive = new ReactiveFormMetadataReader() {
            public Mono<DynamicForm> readForm(String formId, RelationIdentity relation) {
                throw new AssertionError("table facts must not read a form");
            }

            @Override
            public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
                return Mono.just(RelationalTableDefinition.builder(relation).addColumn(form.field("code").column()).build());
            }

        };
        RelationIdentity shorthand = RelationIdentity.of(null, "audit", "items");
        assertEquals(shorthand, jdbc.readTable("audit.items").identity());
        assertEquals(shorthand, jdbc.readSnapshot("audit.items").identity());
        assertEquals(shorthand, reactive.readTable("audit.items").block().identity());
        assertEquals(shorthand, reactive.readSnapshot("audit.items").block().identity());
        RelationIdentity literal = RelationIdentity.of(null, "audit", "items.v1");
        assertEquals(literal, jdbc.readTable("audit", "items.v1").identity());
        assertEquals(literal, jdbc.readSnapshot("audit", "items.v1").identity());
        assertEquals(literal, reactive.readTable("audit", "items.v1").block().identity());
        assertEquals(literal, reactive.readSnapshot("audit", "items.v1").block().identity());
    }

    @Test
    void tableReadPublishesTheCanonicalDefinitionWithSegmentedIdentity() {
        DefaultJdbcFormMetadataReader reader = new DefaultJdbcFormMetadataReader(new com.flying.orm.rdb.support.ListQueryFixture() {
            public List<DynamicRow> query(SqlRequest request) { return caseDistinctColumns(); }
            public long rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected write"); }
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
                throw new AssertionError("unexpected write");
            }
        }, new MetadataQueryProfile(columnQueries()), com.flying.orm.rdb.cache.CacheRegionPolicy.disabled(), MetadataCacheInvalidator.none());
        Object result = reader.readTable("audit", "items.v1");
        RelationalTableDefinition table = assertInstanceOf(RelationalTableDefinition.class, result);
        assertEquals(RelationIdentity.of(null, "audit", "items.v1"), table.identity());
        assertEquals(List.of("Code", "code"), table.columns().stream().map(column -> column.name()).toList());
    }

    @Test
    void jdbcTableAndLegacySnapshotPreserveCaseDistinctPhysicalColumns() {
        DefaultJdbcFormMetadataReader reader = new DefaultJdbcFormMetadataReader(new com.flying.orm.rdb.support.ListQueryFixture() {
            public List<DynamicRow> query(SqlRequest request) { return caseDistinctColumns(); }
            public long rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected write"); }
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
                throw new AssertionError("unexpected write");
            }
        }, new MetadataQueryProfile(columnQueries()), com.flying.orm.rdb.cache.CacheRegionPolicy.disabled(), MetadataCacheInvalidator.none());
        assertCaseDistinctColumns(reader.readTable("items"));
        assertCaseDistinctColumns(reader.readTableForSchema(RelationIdentity.parse("items")));
        assertEquals(List.of("Code", "code"), reader.readSnapshot("items").columns().value().stream()
                .map(column -> column.name()).toList());
        assertThrows(IllegalArgumentException.class, () -> reader.readForm("items", "items"));
    }

    @Test
    void reactiveTableAndLegacySnapshotPreserveCaseDistinctPhysicalColumns() {
        InformationSchemaFormMetadataReader reader = new InformationSchemaFormMetadataReader(new ReactiveSqlExecutor() {
                    public Flux<DynamicRow> query(SqlRequest request) {
                        return Flux.fromIterable(caseDistinctColumns());
                    }
                    public Mono<Long> rowsUpdated(SqlRequest request) {
                        return Mono.error(new AssertionError("unexpected write"));
                    }
                }, new MetadataQueryProfile(columnQueries()));
        Mono<RelationalTableDefinition> table = reader.readTable("items");
        assertCaseDistinctColumns(table.block());
        assertCaseDistinctColumns(table.block());
        assertEquals(RelationIdentity.of(null, "audit", "items.v1"),
                reader.readTable("audit", "items.v1").block().identity());
        assertEquals(List.of("Code", "code"), reader.readSnapshot("items").block().columns().value().stream()
                .map(column -> column.name()).toList());
        assertThrows(IllegalArgumentException.class, () -> reader.readForm("items", "items").block());
    }

    private static void assertCaseDistinctColumns(RelationalTableDefinition table) {
        assertEquals(List.of("Code", "code"), table.columns().stream().map(column -> column.name()).toList());
        assertEquals("Code", table.column("Code").name());
        assertEquals("code", table.column("code").name());
        assertTrue(table.findColumn("CODE").isEmpty());
    }

    private static MetadataQueryProfile.Queries columnQueries() {
        return new MetadataQueryProfile.Queries(
                (schema, table) -> new SqlRequest("columns", List.of()), null, null, value -> value);
    }

    private static List<DynamicRow> caseDistinctColumns() {
        return List.of(DynamicRow.copyOf(Map.of("COLUMN_NAME", "Code", "DATA_TYPE", "VARCHAR")),
                DynamicRow.copyOf(Map.of("COLUMN_NAME", "code", "DATA_TYPE", "VARCHAR")));
    }

    @Test
    void preservesOrderedColumnFactsAndActualIndexesAndForeignKeysInBothProjections() {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT").withGeneration(ValueGeneration.identity()))
                .addField(DynamicField.of("code", "VARCHAR").withLength(42).withNullable(false)
                        .withComment("keep"))
                .addField(DynamicField.of("amount", "DECIMAL").withPrecision(12, 3))
                .addField(DynamicField.of("created", "TIMESTAMP").withPrecision(0, null))
                .addField(DynamicField.of("sequence_value", "BIGINT")
                        .withGeneration(ValueGeneration.sequence("items_seq")))
                .build();
        var indexes = List.<Map<String, Object>>of(Map.of(
                "INDEX_NAME", "ix_code", "COLUMN_NAME", "code", "UNIQUE_INDEX", false));
        var foreignKeys = List.<Map<String, Object>>of(Map.of(
                "FOREIGN_KEY_NAME", "fk_code", "COLUMN_NAME", "code",
                "REFERENCED_TABLE_NAME", "codes", "REFERENCED_COLUMN_NAME", "code"));
        List<Map<String, Object>> columns = List.of(
                Map.of("COLUMN_NAME", "id", "DATA_TYPE", "BIGINT", "PRIMARY_KEY", true, "IS_IDENTITY", true),
                Map.of("COLUMN_NAME", "code", "DATA_TYPE", "VARCHAR", "CHARACTER_MAXIMUM_LENGTH", 42,
                        "NULLABLE", false, "REMARKS", "keep"),
                Map.of("COLUMN_NAME", "amount", "DATA_TYPE", "DECIMAL", "NUMERIC_PRECISION", 12, "NUMERIC_SCALE", 3),
                Map.of("COLUMN_NAME", "created", "DATA_TYPE", "TIMESTAMP", "TEMPORAL_PRECISION", 0),
                Map.of("COLUMN_NAME", "sequence_value", "DATA_TYPE", "BIGINT", "GENERATION_EXPRESSION", "nextval('items_seq'::regclass)"));
        var ordinary = FormMetadataRowConverter.toTableDefinition(RelationIdentity.table("items"),
                FormMetadataRowConverter.tableBuilder(RelationIdentity.table("items"), columns, value -> value),
                indexes, foreignKeys, List.of(), MetadataQueryProfile.SnapshotDialect.PARTIAL);
        var mysql = FormMetadataRowConverter.toTableDefinition(RelationIdentity.table("items"),
                FormMetadataRowConverter.tableBuilder(RelationIdentity.table("items"), columns, value -> value), indexes, foreignKeys,
                List.of(Map.of("CONSTRAINT_NAME", "uq_code", "COLUMN_NAME", "code")),
                MetadataQueryProfile.SnapshotDialect.MYSQL);

        for (var table : List.of(ordinary, mysql)) {
            assertEquals(form.toTableDefinition().columns(), table.columns());
            assertEquals(List.of("id", "code", "amount", "created", "sequence_value"),
                    table.columns().stream().map(column -> column.name()).toList());
            assertEquals(List.of("code"), table.indexes().getFirst().columns());
            assertEquals("ix_code", table.indexes().getFirst().name());
            assertEquals("fk_code", table.foreignKeys().getFirst().name());
            assertEquals("codes", table.foreignKeys().getFirst().reference().table());
        }
        assertEquals(1, ordinary.indexes().size());
        assertEquals(2, mysql.indexes().size());
        assertEquals("uq_code", mysql.indexes().getLast().name());
        assertTrue(mysql.indexes().getLast().unique());
    }

    @Test
    void emptyDictionaryRemainsMissingTableForFormButAbsentForSnapshot() {
        var failure = assertThrows(IllegalArgumentException.class,
                () -> FormMetadataRowConverter.toDynamicForm("items", RelationIdentity.parse("items"), List.of(), value -> value));
        assertEquals("table metadata not found", failure.getMessage());
        assertEquals("table metadata not found", assertThrows(IllegalArgumentException.class,
                () -> FormMetadataRowConverter.tableBuilder(RelationIdentity.table("items"), List.of(), value -> value)).getMessage());
        assertEquals(SchemaSnapshot.State.ABSENT,
                FormMetadataRowConverter.toSchemaSnapshot(RelationIdentity.table("items"),
                        List.of(), List.of(), false, List.of(), false, value -> value).tableState());
    }

    @Test
    void dictionaryStillPassesThroughFormDuplicateFieldValidation() {
        assertThrows(IllegalArgumentException.class, () -> FormMetadataRowConverter.toDynamicForm(
                "items", RelationIdentity.parse("items"), List.of(
                        Map.of("COLUMN_NAME", "Code", "DATA_TYPE", "VARCHAR"),
                        Map.of("COLUMN_NAME", "code", "DATA_TYPE", "VARCHAR")), value -> value));
    }

    @Test
    void tableStillRejectsExactDuplicateColumnNames() {
        Map<String, Object> column = Map.of("COLUMN_NAME", "Code", "DATA_TYPE", "VARCHAR");
        assertEquals("duplicate column name", assertThrows(IllegalArgumentException.class,
                () -> FormMetadataRowConverter.tableBuilder(RelationIdentity.table("items"), List.of(column, column), value -> value)
                        .build()).getMessage());
    }
}
