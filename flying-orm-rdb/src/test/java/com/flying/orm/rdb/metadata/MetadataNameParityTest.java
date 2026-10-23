package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MetadataNameParityTest {
    private final List<List<Object>> reads = new ArrayList<>();
    private final MetadataQueryProfile.Queries queries =
            new MetadataQueryProfile.Queries(
                    (schema, table) -> new SqlRequest("select ?, ?", Arrays.asList(schema, table)),
                    null, null, type -> type);
    private final JdbcFormMetadataReader jdbc = new DefaultJdbcFormMetadataReader(new com.flying.orm.rdb.support.ListQueryFixture() {
        public List<DynamicRow> query(SqlRequest request) { return rows(request); }
        public long rowsUpdated(SqlRequest request) { throw new AssertionError(); }
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
            throw new AssertionError();
        }
    }, new MetadataQueryProfile(queries), com.flying.orm.rdb.cache.CacheRegionPolicy.disabled(), MetadataCacheInvalidator.none());
    private final ReactiveFormMetadataReader reactive = new InformationSchemaFormMetadataReader(new ReactiveSqlExecutor() {
        public Flux<DynamicRow> query(SqlRequest request) { return Flux.defer(() -> Flux.fromIterable(rows(request))); }
        public Mono<Long> rowsUpdated(SqlRequest request) { throw new AssertionError(); }
    }, new MetadataQueryProfile(queries));

    @Test
    void nullSchemaAndNormalizedNamesMatchAcrossEverySingleTableRead() {
        for (String schema : Arrays.asList(null, " app ")) {
            String display = schema == null ? "items" : "app.items";
            String normalizedSchema = schema == null ? null : "app";
            assertEquals(display, jdbc.readForm("form", schema, " items ").table());
            assertEquals(display, reactive.readForm("form", schema, " items ").block().table());
            RelationIdentity identity = RelationIdentity.of(null, normalizedSchema, "items");
            assertEquals(identity, jdbc.readTable(schema, " items ").identity());
            assertEquals(identity, reactive.readTable(schema, " items ").block().identity());
            assertEquals(identity, jdbc.readSnapshot(schema, " items ").identity());
            assertEquals(identity, reactive.readSnapshot(schema, " items ").block().identity());
            for (List<Object> read : reads) assertEquals(Arrays.asList(normalizedSchema, "items"), read);
            reads.clear();
        }
    }

    @Test
    void shorthandIsParsedButExplicitSegmentsAreNot() {
        for (String table : List.of(" app.items ", "app . items")) {
            assertEquals("app.items", reactive.readForm("form", table).block().table());
            assertEquals(RelationIdentity.of(null, "app", "items"), reactive.readTable(table).block().identity());
            assertEquals(RelationIdentity.of(null, "app", "items"), reactive.readSnapshot(table).block().identity());
        }
        for (List<Object> read : reads) assertEquals(List.of("app", "items"), read);
        reads.clear();
        RelationIdentity literal = RelationIdentity.table("app.items");
        assertEquals(literal, reactive.readTable(null, "app.items").block().identity());
        assertEquals(literal, jdbc.readSnapshot(null, "app.items").identity());
        for (List<Object> read : reads) assertEquals(Arrays.asList(null, "app.items"), read);
    }

    @Test
    void cacheAliasesCannotRetainADifferentlyParsedTable() {
        ReactiveFormMetadataCache cached = ReactiveFormMetadataReaders.cached(reactive);
        assertEquals("items", cached.readTable(null, "items").block().name());
        assertSame(cached.readTable("items").block(), cached.readTable(null, "items").block());
        assertEquals("app.items", cached.readForm("form", "app . items").block().table());
        assertSame(cached.readForm("form", "app.items").block(), cached.readForm("form", "app", "items").block());
        assertEquals(List.of(Arrays.asList(null, "items"), List.of("app", "items")), reads);
    }

    @Test
    void malformedShorthandNeverQueriesADifferentTable() {
        for (String table : List.of("items.", "app.items.", ".items", "app..items")) {
            assertThrows(IllegalArgumentException.class, () -> reactive.readForm("form", table).block());
            assertThrows(IllegalArgumentException.class, () -> reactive.readTable(table).block());
            assertThrows(IllegalArgumentException.class, () -> reactive.readSnapshot(table).block());
            assertThrows(IllegalArgumentException.class, () -> jdbc.readTable(table));
        }
        assertEquals(List.of(), reads);
    }

    @Test
    void segmentedIdentityPreservesLiteralDots() {
        for (RelationIdentity identity : List.of(RelationIdentity.table("items.part"),
                RelationIdentity.of(null, "app.part", "items.part"))) {
            assertEquals(identity, jdbc.readForm("form", identity).relationIdentity());
            assertEquals(identity, reactive.readForm("form", identity).block().relationIdentity());
            assertEquals(identity, jdbc.readTable(identity).identity());
            assertEquals(identity, reactive.readTable(identity).block().identity());
            assertEquals(identity, jdbc.readSnapshot(identity).identity());
            assertEquals(identity, reactive.readSnapshot(identity).block().identity());
            for (List<Object> read : reads) assertEquals(Arrays.asList(identity.schema().orElse(null), "items.part"), read);
            reads.clear();
        }
    }

    @Test
    void cachedReadersKeepLiteralAndQualifiedTablesSeparate() {
        ReactiveFormMetadataCache cached = ReactiveFormMetadataReaders.cached(reactive);
        RelationIdentity literal = RelationIdentity.table("app.items");
        RelationIdentity qualified = RelationIdentity.parse("app.items");
        assertEquals(literal, cached.readTable(literal).block().identity());
        assertEquals(qualified, cached.readTable(qualified).block().identity());
        assertEquals(literal, cached.readForm("form", literal).block().relationIdentity());
        assertEquals(qualified, cached.readForm("form", qualified).block().relationIdentity());
        assertEquals(4, reads.size());
        cached.readTable(literal).block();
        cached.readTable(qualified).block();
        assertEquals(4, reads.size());
        cached.invalidate(literal);
        cached.readTable(literal).block();
        cached.readTable(qualified).block();
        assertEquals(5, reads.size());
        assertEquals(Arrays.asList(null, "app.items"), reads.getLast());
    }

    @Test
    void customCatalogReadersDoNotShareCachedEntriesAcrossCatalogs() {
        List<RelationIdentity> identities = new ArrayList<>();
        ReactiveFormMetadataReader custom = new ReactiveFormMetadataReader() {
            public Mono<com.flying.orm.core.form.DynamicForm> readForm(String id, RelationIdentity relation) {
                throw new AssertionError("table metadata is independent of forms");
            }
            public Mono<com.flying.orm.core.metadata.RelationalTableDefinition> readTable(RelationIdentity relation) {
                identities.add(relation);
                return Mono.just(com.flying.orm.core.metadata.RelationalTableDefinition.builder(relation).build());
            }
        };
        ReactiveFormMetadataCache cached = ReactiveFormMetadataReaders.cached(custom);
        RelationIdentity first = RelationIdentity.of("first", "app", "items");
        RelationIdentity second = RelationIdentity.of("second", "app", "items");
        Mono<com.flying.orm.core.metadata.RelationalTableDefinition> pending = cached.readTable(first);
        assertEquals(List.of(), identities);
        assertEquals(first, pending.block().identity());
        assertEquals(second, cached.readTable(second).block().identity());
        assertEquals(first, pending.block().identity());
        assertEquals(List.of(first, second), identities);
    }

    @Test
    void explicitFormNamesKeepLiteralSegmentsWithoutReparsingTheDisplayName() {
        RelationIdentity identity = RelationIdentity.of(null, "app.part", "items.part");
        assertEquals(identity, jdbc.readForm("form", "app.part", "items.part").relationIdentity());
        assertEquals(identity, reactive.readForm("form", "app.part", "items.part").block().relationIdentity());
    }

    private List<DynamicRow> rows(SqlRequest request) {
        reads.add(request.parameters());
        return List.of(DynamicRow.copyOf(Map.of("COLUMN_NAME", "id", "DATA_TYPE", "BIGINT")));
    }
}
