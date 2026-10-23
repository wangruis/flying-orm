package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;
import com.flying.orm.rdb.schema.SchemaMigrationOptions;
import com.flying.orm.rdb.schema.SchemaMigrationPlan;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LegacySchemaCacheBypassTest {

    @TestFactory
    List<DynamicTest> defaultReactiveSnapshotUsesSchemaReadThroughNestedCaches() {
        List<DynamicTest> tests = new ArrayList<>();
        for (int cacheLayers = 0; cacheLayers <= 2; cacheLayers++) {
            for (boolean relationEntry : List.of(false, true)) {
                int layers = cacheLayers;
                tests.add(DynamicTest.dynamicTest("layers=" + layers + "/relation=" + relationEntry, () -> {
                    MetadataSource source = new MetadataSource(form(false));
                    RelationalTableDefinition cached = source.current.toTableDefinition();
                    ReactiveFormMetadataReader reader = new ReactiveFormMetadataReader() {
            @Override
            public Mono<DynamicForm> readForm(String id, RelationIdentity relation) {
                return Mono.just(form(false));
            }

            @Override
            public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
                return Mono.just(cached);
            }

            @Override
            public Mono<RelationalTableDefinition> readTableForSchema(RelationIdentity relation) {
                source.reads++;
                return Mono.just(source.current.toTableDefinition());
            }
                    };
                    for (int i = 0; i < layers; i++) {
                        reader = ReactiveFormMetadataReaders.cached(reader);
                    }
                    assertSame(cached, reader.readTable("accounts").block());
                    Mono<SchemaSnapshot> pending = relationEntry
                            ? reader.readSnapshot(RelationIdentity.table("accounts"))
                            : reader.readSnapshot("accounts");
                    assertEquals(0, source.reads, "snapshot reads must remain cold until subscribed");
                    source.current = form(true);
                    assertEquals(2, pending.block().columns().value().size());
                    source.current = form(false);
                    assertEquals(1, pending.block().columns().value().size());
                    assertEquals(2, source.reads, "each subscription must read current metadata once");
                    assertSame(cached, reader.readTable("accounts").block());
                }));
            }
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> schemaPlanningReadsCurrentColumnsWithoutChangingCrudCache() {
        List<DynamicTest> tests = new ArrayList<>();
        for (RdbDialect dialect : List.of(RdbDialect.mysql(), RdbDialect.postgresql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            for (boolean cachedHasNote : List.of(false, true)) {
                for (boolean reactive : List.of(false, true)) {
                    for (boolean review : List.of(false, true)) {
                        String name = dialect.name() + "/cachedNote=" + cachedHasNote
                                + "/reactive=" + reactive + "/review=" + review;
                        tests.add(DynamicTest.dynamicTest(name,
                                () -> assertCurrentColumns(dialect, cachedHasNote, reactive, review)));
                    }
                }
            }
        }
        return tests;
    }

    private static void assertCurrentColumns(RdbDialect dialect, boolean cachedHasNote,
                                              boolean reactive, boolean review) {
        MetadataSource source = new MetadataSource(form(cachedHasNote));
        SchemaMigrationPlan plan;
        if (reactive) {
            ReactiveFormMetadataReader delegate = new ReactiveFormMetadataReader() {
            @Override
            public Mono<DynamicForm> readForm(String id, RelationIdentity relation) {
                return Mono.fromSupplier(() -> {
                    source.reads++;
                    return source.current;
                });
            }

            @Override
            public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).map(form -> form.toTableDefinition(relation));
            }

            };
            // Nested caches must also be bypassed by schema reads.
            ReactiveFormMetadataReader reader = ReactiveFormMetadataReaders.cached(
                    ReactiveFormMetadataReaders.cached(delegate));
            RelationalTableDefinition cached = reader.readTable("accounts").block();
            source.current = form(!cachedHasNote);
            ReactiveSchemaClient client = ReactiveSchemaClient.create(source.reactiveExecutor(), dialect);
            Mono<SchemaMigrationPlan> pending = review
                    ? client.planCreateOrAlter(form(true), List.of(), List.of(), reader,
                            SchemaMigrationOptions.safe())
                            .map(result -> result)
                    : client.planCreateOrAlter(form(true), List.of(), reader);
            assertEquals(1, source.reads, "schema reads must remain cold until subscribed");
            plan = pending.block();
            assertSame(cached, reader.readTable("accounts").block(), "schema reads must not replace CRUD entries");
        } else {
            JdbcFormMetadataReader reader = new DefaultJdbcFormMetadataReader(source, new MetadataQueryProfile(new MetadataQueryProfile.Queries(
                            (schema, table) -> new SqlRequest("columns", List.of()),
                            null, null, type -> type)), CacheRegionPolicy.metadataDefaults(), MetadataCacheInvalidator.none());
            RelationalTableDefinition cached = reader.readTable("accounts");
            source.current = form(!cachedHasNote);
            JdbcSchemaClient client = JdbcSchemaClient.create(source, dialect);
            plan = review
                    ? client.planCreateOrAlter(form(true), List.of(), List.of(), reader,
                            SchemaMigrationOptions.safe())
                    : client.planCreateOrAlter(form(true), List.of(), reader);
            assertSame(cached, reader.readTable("accounts"), "schema reads must not replace CRUD entries");
        }
        assertEquals(cachedHasNote ? 1 : 0, plan.requests().size(),
                "DDL must describe the current column state rather than the warmed CRUD entry");
        assertEquals(2, source.reads, "each schema operation must read current metadata once");
        if (cachedHasNote) {
            assertTrue(plan.requests().getFirst().sql().contains("note"));
        }
    }

    private static DynamicForm form(boolean note) {
        DynamicForm.Builder form = DynamicForm.builder("accounts", "accounts")
                .addField(DynamicField.primaryKey("id", "INTEGER"));
        if (note) {
            form.addField(DynamicField.of("note", "VARCHAR").withLength(100));
        }
        return form.build();
    }

    private static final class MetadataSource implements com.flying.orm.rdb.support.ListQueryFixture {
        private DynamicForm current;
        private int reads;

        private MetadataSource(DynamicForm current) {
            this.current = current;
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            assertEquals("columns", request.sql());
            reads++;
            return current.fields().stream().map(field -> {
                LinkedHashMap<String, Object> row = new LinkedHashMap<>();
                row.put("COLUMN_NAME", field.name());
                row.put("DATA_TYPE", field.databaseType().canonical());
                row.put("PRIMARY_KEY", field.primaryKey());
                row.put("IS_NULLABLE", field.nullable() ? "YES" : "NO");
                row.put("CHARACTER_MAXIMUM_LENGTH", field.length());
                return DynamicRow.copyOf(row);
            }).toList();
        }

        private ReactiveSqlExecutor reactiveExecutor() {
            return new ReactiveSqlExecutor() {
                @Override
                public Flux<DynamicRow> query(SqlRequest request) {
                    return Flux.error(new AssertionError("custom reader must not query the executor"));
                }

                @Override
                public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.error(new AssertionError("planning must not execute DDL"));
                }
            };
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new AssertionError("planning must not execute DDL");
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
            throw new AssertionError("planning must not execute DDL");
        }
    }
}
