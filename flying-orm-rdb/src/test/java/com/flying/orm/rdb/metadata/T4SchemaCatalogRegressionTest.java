package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;
import com.flying.orm.rdb.schema.ReviewedSchemaPlan;
import com.flying.orm.rdb.schema.SchemaCompatibilityMode;
import com.flying.orm.rdb.schema.SchemaMigrationOptions;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class T4SchemaCatalogRegressionTest {

    @Test
    void t401MysqlWideningPreservesDefaultOnBothClients() {
        for (boolean reactive : List.of(false, true)) {
            Catalog catalog = mysql(false);
            DynamicForm target = DynamicForm.builder("items", "app.items")
                    .addField(DynamicField.of("code", "VARCHAR").withLength(32).withNullable(false)).build();

            catalog.migrate(target, List.of(), reactive);

            assertEquals(1, catalog.snapshotQueries);
            assertEquals(1, catalog.ddl.size());
            String sql = catalog.ddl.getFirst().sql().toLowerCase(java.util.Locale.ROOT);
            assertTrue(sql.contains("varchar(32)"), sql);
            assertTrue(sql.contains("default 'pending'"), sql);
            assertTrue(sql.contains("character set `utf8mb4`"), sql);
            assertTrue(sql.contains("collate `utf8mb4_bin`"), sql);
        }
    }

    @Test
    void t402OracleNegativeScaleCannotBecomeAnAbsentPhysicalFact() {
        Catalog catalog = new Catalog(RdbDialect.oracle(), "APP", List.of(row(
                "COLUMN_NAME", "amount", "DATA_TYPE", "NUMBER", "PHYSICAL_DATA_TYPE", "NUMBER",
                "NUMERIC_PRECISION", 8, "NUMERIC_SCALE", -2, "NULLABLE", true)));
        assertThrows(IllegalStateException.class, () -> catalog.jdbcReader.readSnapshot(catalog.table));
        assertThrows(IllegalStateException.class, () -> catalog.reactiveReader.readSnapshot(catalog.table).block());
    }

    @Test
    void t401MysqlNullabilityPreservesTheExistingDefault() {
        for (boolean reactive : List.of(false, true)) {
            Catalog catalog = mysql(false);
            DynamicForm target = DynamicForm.builder("items", "app.items")
                    .addField(DynamicField.of("code", "VARCHAR").withLength(16)).build();
            catalog.migrate(target, List.of(), reactive);
            assertEquals(1, catalog.ddl.size());
            String sql = catalog.ddl.getFirst().sql().toLowerCase(java.util.Locale.ROOT);
            assertTrue(sql.contains("default 'pending'"), sql);
            assertFalse(sql.contains("not null"), sql);
        }
    }

    @Test
    void t401MysqlUnrepresentablePhysicalColumnFailsBeforeDdl() {
        for (boolean reactive : List.of(false, true)) {
            Catalog catalog = new Catalog(RdbDialect.mysql(), "app", List.of(row(
                    "COLUMN_NAME", "code", "DATA_TYPE", "varchar", "PHYSICAL_DATA_TYPE", "varchar",
                    "CHARACTER_MAXIMUM_LENGTH", 16, "NULLABLE", false, "COLUMN_DEFAULT", "pending",
                    "COLUMN_REPRESENTABLE", false, "UNSUPPORTED_COLUMN_REASON", "on-update expression")));
            DynamicForm target = DynamicForm.builder("items", "app.items")
                    .addField(DynamicField.of("code", "VARCHAR").withLength(32).withNullable(false)).build();
            assertThrows(IllegalStateException.class, () -> catalog.migrate(target, List.of(), reactive));
            assertTrue(catalog.ddl.isEmpty());
        }
    }

    @Test
    void t402OracleZeroScaleStillRoundTrips() {
        Catalog catalog = new Catalog(RdbDialect.oracle(), "APP", List.of(row(
                "COLUMN_NAME", "amount", "DATA_TYPE", "NUMBER", "PHYSICAL_DATA_TYPE", "NUMBER",
                "NUMERIC_PRECISION", 8, "NUMERIC_SCALE", 0, "NULLABLE", true)));
        RelationalTableDefinition desired = RelationalTableDefinition.builder(catalog.table)
                .addColumn(ColumnDefinition.builder("amount", "NUMBER").precision(8).scale(0).build()).build();
        for (ReviewedSchemaPlan plan : catalog.reviews(desired)) {
            assertTrue(plan.operations().isEmpty());
        }
    }

    @Test
    void t403MysqlLegacyUniqueIndexRoundTripsWithoutRecreation() {
        for (boolean reactive : List.of(false, true)) {
            Catalog catalog = mysql(true);
            var table = reactive ? catalog.reactiveReader.readTable("app", "items").block()
                    : catalog.jdbcReader.readTable("app", "items");
            assertEquals(List.of("uq_code"), table.indexes().stream().map(IndexMetadata::name).toList());
            assertTrue(table.index("uq_code").unique());
            DynamicForm target = DynamicForm.builder("items", "app.items")
                    .addField(DynamicField.of("code", "VARCHAR").withLength(16).withNullable(false)).build();
            catalog.migrate(target, List.of(IndexMetadata.builder("uq_code").unique().addColumn("code").build()),
                    reactive);
            assertTrue(catalog.ddl.isEmpty(), catalog.ddl.toString());
            SchemaSnapshot snapshot = reactive ? catalog.reactiveReader.readSnapshot(catalog.table).block()
                    : catalog.jdbcReader.readSnapshot(catalog.table);
            assertEquals(1, snapshot.completeTable().orElseThrow().uniqueConstraints().size());
            assertTrue(snapshot.completeTable().orElseThrow().indexes().isEmpty());
        }
    }

    @Test
    void t405SqlServerNamedDefaultNullRoundTripsOnBothReaders() {
        for (String expression : List.of("NULL", "(NULL)", "((null))")) {
            Catalog catalog = new Catalog(RdbDialect.sqlServer(), "dbo", List.of(row(
                    "COLUMN_NAME", "value", "DATA_TYPE", "int", "PHYSICAL_DATA_TYPE", "int",
                    "NULLABLE", true, "COLUMN_DEFAULT", expression, "GENERATION_EXPRESSION", expression,
                    "DEFAULT_CONSTRAINT_NAME", "DF_items_value")));
            SchemaSnapshot jdbc = assertDoesNotThrow(() -> catalog.jdbcReader.readSnapshot(catalog.table));
            SchemaSnapshot reactive = assertDoesNotThrow(() -> catalog.reactiveReader.readSnapshot(catalog.table).block());
            for (SchemaSnapshot snapshot : List.of(jdbc, reactive)) {
                ColumnDefinition column = snapshot.completeTable().orElseThrow().columns().getFirst();
                assertEquals(ColumnDefault.none(), column.defaultValue());
                assertEquals("DF_items_value", column.defaultConstraintName());
            }
            RelationalTableDefinition desired = RelationalTableDefinition.builder(catalog.table)
                    .addColumn(ColumnDefinition.builder("value", "INT")
                            .defaultConstraintName("DF_items_value").build()).build();
            for (ReviewedSchemaPlan plan : catalog.reviews(desired)) {
                assertTrue(plan.operations().isEmpty());
            }
        }
    }

    private static Catalog mysql(boolean unique) {
        Catalog catalog = new Catalog(RdbDialect.mysql(), "app", List.of(row(
                "COLUMN_NAME", "code", "DATA_TYPE", "varchar", "PHYSICAL_DATA_TYPE", "varchar",
                "CHARACTER_MAXIMUM_LENGTH", 16, "NULLABLE", false, "COLUMN_DEFAULT", "pending",
                "COLUMN_CHARSET", "utf8mb4", "COLUMN_COLLATION", "utf8mb4_bin")));
        catalog.unique = unique;
        return catalog;
    }

    @Test
    void t401MysqlOrdinaryReadsAndAddedColumnsDoNotRequestSnapshot() {
        for (boolean reactive : List.of(false, true)) {
            Catalog catalog = mysql(false);
            if (reactive) {
                catalog.reactiveReader.readTable("app", "items").block();
            } else {
                catalog.jdbcReader.readTable("app", "items");
            }
            assertEquals(0, catalog.snapshotQueries);
            DynamicForm target = DynamicForm.builder("items", "app.items")
                    .addField(DynamicField.of("code", "VARCHAR").withLength(16).withNullable(false))
                    .addField(DynamicField.of("extra", "INT")).build();
            catalog.migrate(target, List.of(), reactive);
            assertEquals(0, catalog.snapshotQueries);
            assertEquals(1, catalog.ddl.size());
            assertTrue(catalog.ddl.getFirst().sql().contains("add column"));
        }
    }

    @Test
    void t401MysqlSkippedChangesAndAddedColumnsDoNotRequestSnapshot() {
        List<DynamicField> skippedChanges = List.of(
                DynamicField.of("code", "VARCHAR").withLength(16).withNullable(false),
                DynamicField.of("code", "VARCHAR").withLength(16).withNullable(false)
                        .withComment("not applied"),
                DynamicField.of("code", "VARCHAR").withLength(8),
                DynamicField.of("code", "VARCHAR").withLength(32).withNullable(false));
        for (boolean reactive : List.of(false, true)) {
            for (DynamicField skippedChange : skippedChanges) {
                Catalog catalog = new Catalog(RdbDialect.mysql(), "app", List.of(
                        row("COLUMN_NAME", "code", "DATA_TYPE", "varchar", "PHYSICAL_DATA_TYPE", "varchar",
                                "CHARACTER_MAXIMUM_LENGTH", 16, "NULLABLE", true),
                        row("COLUMN_NAME", "updated_at", "DATA_TYPE", "timestamp",
                                "PHYSICAL_DATA_TYPE", "timestamp", "NULLABLE", true,
                                "COLUMN_DEFAULT", "CURRENT_TIMESTAMP",
                                "COLUMN_REPRESENTABLE", false,
                                "UNSUPPORTED_COLUMN_REASON", "ON UPDATE CURRENT_TIMESTAMP")));
                DynamicForm target = DynamicForm.builder("items", "app.items")
                        .addField(skippedChange)
                        .addField(DynamicField.of("updated_at", "TIMESTAMP"))
                        .addField(DynamicField.of("extra", "INT")).build();

                catalog.migrate(target, List.of(), reactive);

                assertEquals(0, catalog.snapshotQueries);
                assertEquals(1, catalog.ddl.size());
                String sql = catalog.ddl.getFirst().sql().toLowerCase(java.util.Locale.ROOT);
                assertTrue(sql.contains("add column"), sql);
                assertTrue(sql.contains("`extra`"), sql);
                assertFalse(sql.contains("modify column"), sql);
            }
        }
    }

    @Test
    void t401MysqlCommentRewriteRequestsSnapshotAndPreservesDefault() {
        for (boolean reactive : List.of(false, true)) {
            Catalog catalog = mysql(false);
            DynamicForm target = DynamicForm.builder("items", "app.items")
                    .addField(DynamicField.of("code", "VARCHAR").withLength(16).withNullable(false)
                            .withComment("reviewed comment")).build();

            catalog.migrate(target, List.of(), reactive);

            assertEquals(1, catalog.snapshotQueries);
            assertEquals(1, catalog.ddl.size());
            String sql = catalog.ddl.getFirst().sql().toLowerCase(java.util.Locale.ROOT);
            assertTrue(sql.contains("modify column"), sql);
            assertTrue(sql.contains("comment 'reviewed comment'"), sql);
            assertTrue(sql.contains("default 'pending'"), sql);
            assertTrue(sql.contains("character set `utf8mb4`"), sql);
            assertTrue(sql.contains("collate `utf8mb4_bin`"), sql);
        }
    }

    @Test
    void t405MysqlStringNullRemainsALiteral() {
        Catalog catalog = new Catalog(RdbDialect.mysql(), "app", List.of(row(
                "COLUMN_NAME", "code", "DATA_TYPE", "varchar", "PHYSICAL_DATA_TYPE", "varchar",
                "CHARACTER_MAXIMUM_LENGTH", 16, "NULLABLE", true, "COLUMN_DEFAULT", "NULL")));
        for (SchemaSnapshot snapshot : List.of(catalog.jdbcReader.readSnapshot(catalog.table),
                catalog.reactiveReader.readSnapshot(catalog.table).block())) {
            assertEquals("NULL", snapshot.completeTable().orElseThrow().columns().getFirst()
                    .defaultValue().value().orElseThrow());
        }
    }

    private static DynamicRow row(Object... values) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int index = 0; index < values.length; index += 2) {
            row.put((String) values[index], values[index + 1]);
        }
        return DynamicRow.copyOf(row);
    }

    private static final class Catalog {
        private final RdbDialect dialect;
        private final RelationIdentity table;
        private final List<DynamicRow> columns;
        private final List<SqlRequest> ddl = new ArrayList<>();
        private final JdbcFormMetadataReader jdbcReader;
        private final ReactiveFormMetadataReader reactiveReader;
        private final JdbcSchemaClient jdbc;
        private final ReactiveSchemaClient reactive;
        private boolean unique;
        private int snapshotQueries;

        private Catalog(RdbDialect dialect, String schema, List<DynamicRow> columns) {
            this.dialect = dialect;
            this.table = RelationIdentity.of(null, schema, "items");
            this.columns = columns;
            SyncSqlExecutor sync = new SyncSqlExecutor() {
                @Override
                public List<DynamicRow> query(SqlRequest request) {
                    return rows(request);
                }

                @Override
                public long rowsUpdated(SqlRequest request) {
                    ddl.add(request);
                    return 0;
                }

                @Override
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                    throw new AssertionError("schema must not request generated keys");
                }
            };
            ReactiveSqlExecutor async = new ReactiveSqlExecutor() {
                @Override
                public Flux<DynamicRow> query(SqlRequest request) {
                    return Flux.defer(() -> Flux.fromIterable(rows(request)));
                }

                @Override
                public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.fromSupplier(() -> {
                        ddl.add(request);
                        return 0L;
                    });
                }
            };
            jdbcReader = JdbcFormMetadataReaders.create(sync, dialect);
            reactiveReader = ReactiveFormMetadataReaders.create(async, dialect);
            jdbc = JdbcSchemaClient.create(sync, dialect);
            reactive = ReactiveSchemaClient.create(async, dialect);
        }

        private void migrate(DynamicForm target, List<IndexMetadata> indexes, boolean async) {
            if (async) {
                reactive.createOrAlterDetailed(target, indexes, List.of(), reactiveReader,
                        SchemaMigrationOptions.safe()).block();
            } else {
                jdbc.createOrAlterDetailed(target, indexes, List.of(), jdbcReader, SchemaMigrationOptions.safe());
            }
        }

        private List<ReviewedSchemaPlan> reviews(RelationalTableDefinition desired) {
            DatabaseDescriptor database = DatabaseDescriptor.of(dialect.name(), dialect.version(), dialect);
            return List.of(jdbc.reviewRelational(database, desired, jdbcReader, SchemaCompatibilityMode.EXACT),
                    reactive.reviewRelational(database, desired, reactiveReader, SchemaCompatibilityMode.EXACT).block());
        }

        private List<DynamicRow> rows(SqlRequest request) {
            String sql = request.sql();
            if (sql.contains("COLUMN_DEFAULT")) {
                return columns;
            }
            if (sql.contains("TABLE_REPRESENTABLE")) {
                snapshotQueries++;
                return List.of(row("TABLE_REPRESENTABLE", true));
            }
            if (unique && sql.contains("as UNIQUE_INDEX")) {
                // MySQL exposes an ordinary unique index as a UNIQUE table constraint.
                return sql.contains("owned_constraint.CONSTRAINT_TYPE in ('PRIMARY KEY', 'UNIQUE')")
                        ? List.of() : List.of(row("INDEX_NAME", "uq_code", "COLUMN_NAME", "code",
                        "UNIQUE_INDEX", true, "INDEX_DIRECTION", "ASC", "INDEX_REPRESENTABLE", true));
            }
            if (unique && sql.contains("CONSTRAINT_TYPE = 'UNIQUE'")) {
                return List.of(row("CONSTRAINT_NAME", "uq_code", "COLUMN_NAME", "code",
                        "CONSTRAINT_REPRESENTABLE", true, "INDEX_DIRECTION", "ASC"));
            }
            return List.of();
        }
    }
}
