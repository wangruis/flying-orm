package com.flying.orm.rdb.metadata;

import com.flying.orm.core.annotation.EncryptedField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import com.flying.orm.rdb.protection.ProtectedFormLayout;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.EntitySchemaSyncMode;
import com.flying.orm.rdb.schema.EntitySchemaSynchronizer;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;
import com.flying.orm.rdb.schema.SchemaMigrationOptions;
import com.flying.orm.rdb.schema.SchemaMigrationReviewPolicy;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaTableReadContractTest {

    @Test
    void schemaReadPreservesIndexesForeignKeysAndIdentityWithoutWarmingCrudCache() {
        Fixture fixture = new Fixture();
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(com.flying.orm.core.form.DynamicField.primaryKey("id", "BIGINT")
                        .withGeneration(ValueGeneration.identity()))
                .build();
        TableMetadata metadata = FormMetadataRowConverter.toTableMetadata("items", form,
                List.of(Map.of("INDEX_NAME", "ix_id", "COLUMN_NAME", "id", "UNIQUE_INDEX", false)),
                List.of(Map.of("FOREIGN_KEY_NAME", "fk_id", "COLUMN_NAME", "id",
                        "REFERENCED_TABLE_NAME", "owners", "REFERENCED_COLUMN_NAME", "id")));
        fixture.tables.put("items", metadata);

        TableMetadata jdbc = fixture.jdbcReader.readTableForSchema("items");
        Mono<TableMetadata> pending = fixture.reactiveReader.readTableForSchema("items");
        assertEquals(1, fixture.reads);
        assertSame(metadata, pending.block());
        assertSame(metadata, pending.block());
        assertEquals(3, fixture.reads, "each subscription reads fresh metadata");
        for (TableMetadata actual : List.of(jdbc, metadata)) {
            assertEquals(metadata.columns(), actual.columns());
            assertEquals(1, actual.indexes().size());
            assertEquals("ix_id", actual.indexes().getFirst().name());
            assertEquals(List.of("id"), actual.indexes().getFirst().columns());
            assertFalse(actual.indexes().getFirst().unique());
            assertEquals(metadata.foreignKeys(), actual.foreignKeys());
            assertEquals(ValueGeneration.identity(), actual.column("id").generation());
        }
        fixture.jdbcReader.readTable("items");
        fixture.reactiveReader.readTable("items").block();
        assertEquals(5, fixture.reads, "schema reads must not populate CRUD entries");
    }

    @Test
    void protectedSideTablePlanningAndReviewBypassBothCacheLayers() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            DynamicForm form = models.metadata(ProtectedAccount.class).toDynamicForm();
            var side = ProtectedContainsLayout.resolve(form).orElseThrow();
            for (boolean reactive : List.of(false, true)) {
                for (boolean review : List.of(false, true)) {
                    Fixture fixture = new Fixture();
                    fixture.tables.put(form.table(), ProtectedFormLayout.physical(form).toTableMetadata());
                    TableMetadata.Builder currentSide = TableMetadata.builder(side.table().table());
                    side.table().toTableMetadata().columns().forEach(currentSide::addColumn);
                    side.indexes().forEach(currentSide::addIndex);
                    side.foreignKeys().forEach(currentSide::addForeignKey);
                    fixture.tables.put(side.table().table(), currentSide.build());
                    TableMetadata cached = fixture.read(side.table().table(), reactive);
                    fixture.tables.remove(side.table().table());

                    var plan = reactive
                            ? (review ? fixture.reactive.reviewCreateOrAlter(form, List.of(), List.of(),
                                    fixture.reactiveReader, SchemaMigrationOptions.safe(),
                                    SchemaMigrationReviewPolicy.allowBlocking()).block().migration()
                                    : fixture.reactive.planCreateOrAlter(form, List.of(), fixture.reactiveReader).block())
                            : (review ? fixture.jdbc.reviewCreateOrAlter(form, List.of(), List.of(),
                                    fixture.jdbcReader, SchemaMigrationOptions.safe(),
                                    SchemaMigrationReviewPolicy.allowBlocking()).migration()
                                    : fixture.jdbc.planCreateOrAlter(form, List.of(), fixture.jdbcReader));

                    assertTrue(plan.additionalCreatedTables().contains(side.table().table()));
                    assertTrue(plan.requests().stream().anyMatch(request ->
                            request.sql().contains("create table") && request.sql().contains(side.table().table())));
                    assertSame(cached, fixture.read(side.table().table(), reactive));
                    assertEquals(3, fixture.reads, "one warmup, one owner read and one current side-table read");
                }
            }
        }
    }

    @Test
    void entityValidationUsesTheSameFreshPlanningBoundary() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            TableMetadata current = models.metadata(Account.class).toDynamicForm().toTableMetadata();
            for (boolean reactive : List.of(false, true)) {
                Fixture fixture = new Fixture();
                fixture.tables.put(current.name(), TableMetadata.builder(current.name())
                        .addColumn(current.column("id")).build());
                TableMetadata cached = fixture.read(current.name(), reactive);
                fixture.tables.put(current.name(), current);
                EntitySchemaSynchronizer synchronizer = new EntitySchemaSynchronizer(models,
                        fixture.reactive, fixture.reactiveReader, fixture.jdbc, fixture.jdbcReader);
                var result = reactive
                        ? synchronizer.synchronizeReactive(EntitySchemaSyncMode.VALIDATE, Account.class).block()
                        : synchronizer.synchronize(EntitySchemaSyncMode.VALIDATE, Account.class);
                assertFalse(result.hasDifferences());
                assertSame(cached, fixture.read(current.name(), reactive));
                assertEquals(2, fixture.reads);
            }
        }
    }

    @TableName("accounts")
    static class Account { @TableId Long id; String note; }

    @TableName("protected_accounts")
    static class ProtectedAccount {
        @TableId Long id;
        @EncryptedField(search = EncryptedSearchMode.CONTAINS) String note;
    }

    private static final class Fixture implements SyncSqlExecutor {
        private final Map<String, TableMetadata> tables = new LinkedHashMap<>();
        private int reads;
        private final JdbcFormMetadataReader jdbcReader = new JdbcFormMetadataReader(this,
                new InformationSchemaFormMetadataReader.Queries(
                        (schema, table) -> new SqlRequest("columns", List.of(table)),
                        (schema, table) -> new SqlRequest("indexes", List.of(table)),
                        (schema, table) -> new SqlRequest("foreignKeys", List.of(table)), type -> type),
                CacheRegionPolicy.metadataDefaults(), MetadataCacheInvalidator.none());
        private final ReactiveFormMetadataReader reactiveReader = ReactiveFormMetadataReaders.cached(
                ReactiveFormMetadataReaders.cached(new ReactiveFormMetadataReader() {
                    public Mono<TableMetadata> readTable(String table) {
                        reads++;
                        TableMetadata current = tables.get(table);
                        return current == null ? Mono.error(new IllegalArgumentException("table metadata not found"))
                                : Mono.just(current);
                    }
                    public Mono<DynamicForm> readForm(String id, String table) {
                        return Mono.error(new AssertionError("complete table metadata is required"));
                    }
                    public Mono<DynamicForm> readForm(String id, String schema, String table) {
                        return readForm(id, table);
                    }
                }));
        private final JdbcSchemaClient jdbc = JdbcSchemaClient.create(this, RdbDialect.postgresql());
        private final ReactiveSchemaClient reactive = ReactiveSchemaClient.create(new ReactiveSqlExecutor() {
            public Flux<DynamicRow> query(SqlRequest request) { return Flux.error(new AssertionError("use reader")); }
            public Mono<Long> rowsUpdated(SqlRequest request) { return Mono.error(new AssertionError("no DDL")); }
        }, RdbDialect.postgresql());

        private TableMetadata read(String table, boolean useReactive) {
            return useReactive ? reactiveReader.readTable(table).block() : jdbcReader.readTable(table);
        }

        public List<DynamicRow> query(SqlRequest request) {
            TableMetadata table = tables.get((String) request.parameters().getFirst());
            if (request.sql().equals("columns")) {
                reads++;
                return table == null ? List.of() : table.columns().stream().map(column -> {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("COLUMN_NAME", column.name());
                    row.put("DATA_TYPE", column.dataType());
                    row.put("PRIMARY_KEY", column.primaryKey());
                    row.put("NULLABLE", column.nullable());
                    row.put("CHARACTER_MAXIMUM_LENGTH", column.length());
                    row.put("IS_IDENTITY", column.generation().strategy() == ValueGeneration.Strategy.IDENTITY);
                    return DynamicRow.copyOf(row);
                }).toList();
            }
            if (request.sql().equals("indexes")) {
                return table.indexes().stream().map(index -> DynamicRow.copyOf(Map.of(
                        "INDEX_NAME", index.name(), "COLUMN_NAME", index.columns().getFirst(),
                        "UNIQUE_INDEX", index.unique()))).toList();
            }
            return table.foreignKeys().stream().map(key -> DynamicRow.copyOf(Map.of(
                    "FOREIGN_KEY_NAME", key.name(), "COLUMN_NAME", key.columns().getFirst(),
                    "REFERENCED_TABLE_NAME", key.referenceTable(),
                    "REFERENCED_COLUMN_NAME", key.referenceColumns().getFirst()))).toList();
        }

        public long rowsUpdated(SqlRequest request) { throw new AssertionError("no DDL"); }
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new AssertionError("no DDL");
        }
    }
}
