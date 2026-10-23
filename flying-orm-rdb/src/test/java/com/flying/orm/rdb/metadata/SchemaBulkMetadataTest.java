package com.flying.orm.rdb.metadata;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import io.r2dbc.spi.ConnectionFactories;
import org.junit.jupiter.api.Test;
import org.h2.jdbcx.JdbcDataSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;

class SchemaBulkMetadataTest {

    @Test
    void mysqlViewMarkerIsNotReportedAsAUserComment() throws Exception {
        String sql = MySqlMetadataQueries.queries().columnQuery().create(null, null).sql();
        java.util.regex.Matcher projection = java.util.regex.Pattern.compile(
                "(?s)c.TABLE_NAME as METADATA_TABLE,\\s*(.*?) as TABLE_COMMENT,").matcher(sql);
        assertTrue(projection.find());
        try (Connection connection = java.sql.DriverManager.getConnection("jdbc:h2:mem:");
             Statement statement = connection.createStatement();
             java.sql.ResultSet rows = statement.executeQuery("select " + projection.group(1)
                     + " from (values (1, 'BASE TABLE', 'VIEW'), (2, 'VIEW', 'VIEW'),"
                     + " (3, 'BASE TABLE', '用户说明'), (4, 'BASE TABLE', ''))"
                     + " table_info(ord, TABLE_TYPE, TABLE_COMMENT) order by ord")) {
            assertTrue(rows.next());
            assertEquals("VIEW", rows.getString(1));
            assertTrue(rows.next());
            org.junit.jupiter.api.Assertions.assertNull(rows.getString(1));
            assertTrue(rows.next());
            assertEquals("用户说明", rows.getString(1));
            assertTrue(rows.next());
            org.junit.jupiter.api.Assertions.assertNull(rows.getString(1));
            assertFalse(rows.next());
        }
    }

    @Test
    void bulkReadKeepsContextCancellationAndDatabaseErrors() {
        for (boolean cancel : List.of(false, true)) {
            AtomicInteger subscribed = new AtomicInteger();
            AtomicInteger cancelled = new AtomicInteger();
            IllegalStateException failure = new IllegalStateException("dictionary unavailable");
            ReactiveFormMetadataReader reader = ReactiveFormMetadataReaders.cached(
                    new InformationSchemaFormMetadataReader(new ReactiveSqlExecutor() {
                        public Flux<DynamicRow> query(SqlRequest request) {
                            return Flux.deferContextual(context -> {
                                assertEquals("caller", context.get("owner"));
                                subscribed.incrementAndGet();
                                return cancel ? Flux.<DynamicRow>never().doOnCancel(cancelled::incrementAndGet)
                                        : Flux.error(failure);
                            });
                        }
                        public Mono<Long> rowsUpdated(SqlRequest request) { throw new AssertionError(); }
                    }, new MetadataQueryProfile(MySqlMetadataQueries.queries())));
            Flux<RelationalTableDefinition> tables = reader.readTables().contextWrite(context -> context.put("owner", "caller"));
            assertEquals(0, subscribed.get());
            if (cancel) {
                reactor.core.Disposable subscription = tables.subscribe();
                subscription.dispose();
                assertEquals(1, cancelled.get());
            } else {
                assertSame(failure, assertThrows(IllegalStateException.class, () -> tables.collectList().block()));
            }
            assertEquals(1, subscribed.get(), "later dictionary queries must not execute after cancellation or error");
        }
    }

    @Test
    void mysqlUniqueAndOrdinaryIndexesStayWithTheirExactTableIdentity() {
        List<Map<String, Object>> columns = List.of(
                Map.of("METADATA_TABLE", "Item", "COLUMN_NAME", "id", "DATA_TYPE", "BIGINT", "TABLE_COMMENT", "Item comment"),
                Map.of("METADATA_TABLE", "item", "COLUMN_NAME", "code", "DATA_TYPE", "VARCHAR", "TABLE_COMMENT", "item comment"));
        List<Map<String, Object>> indexes = List.of(
                Map.of("METADATA_TABLE", "Item", "INDEX_NAME", "ix_same", "COLUMN_NAME", "id"),
                Map.of("METADATA_TABLE", "item", "INDEX_NAME", "ix_same", "COLUMN_NAME", "code"));
        List<Map<String, Object>> unique = List.of(
                Map.of("METADATA_TABLE", "Item", "CONSTRAINT_NAME", "uk_same", "COLUMN_NAME", "id",
                        "CONSTRAINT_REPRESENTABLE", true),
                Map.of("METADATA_TABLE", "item", "CONSTRAINT_NAME", "uk_same", "COLUMN_NAME", "code",
                        "CONSTRAINT_REPRESENTABLE", true));
        List<RelationalTableDefinition> tables = FormMetadataRowConverter.toTables("app", List.of(), columns, indexes,
                List.of(), unique, MySqlMetadataQueries.queries());
        assertEquals(List.of(com.flying.orm.core.metadata.RelationIdentity.of(null, "app", "Item"),
                com.flying.orm.core.metadata.RelationIdentity.of(null, "app", "item")),
                tables.stream().map(RelationalTableDefinition::identity).toList());
        assertEquals(List.of("Item comment", "item comment"), tables.stream().map(RelationalTableDefinition::comment).toList());
        assertEquals("Item comment", com.flying.orm.rdb.schema.SchemaSnapshot.fromPartial(
                tables.getFirst()).tableComment().value());
        assertEquals(List.of("id"), tables.get(0).index("ix_same").columns());
        assertEquals(List.of("code"), tables.get(1).index("ix_same").columns());
        assertEquals(List.of("id"), tables.get(0).index("uk_same").columns());
        assertEquals(List.of("code"), tables.get(1).index("uk_same").columns());
    }

    @Test
    void postgresKeepsZeroColumnTablesInSchemaEnumeration() {
        AtomicInteger calls = new AtomicInteger();
        ReactiveFormMetadataReader reader = new InformationSchemaFormMetadataReader(new ReactiveSqlExecutor() {
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.defer(() -> {
                    calls.incrementAndGet();
                    return request.sql().contains("from information_schema.tables")
                            ? Flux.just(DynamicRow.copyOf(Map.of("TABLE_NAME", "empty_table", "TABLE_COMMENT", "empty comment"))) : Flux.empty();
                });
            }
            public Mono<Long> rowsUpdated(SqlRequest request) { throw new AssertionError(); }
        }, new MetadataQueryProfile(PostgreSqlMetadataQueries.queries()));
        List<RelationalTableDefinition> tables = reader.readTables("app").collectList().block();
        assertEquals(List.of(com.flying.orm.core.metadata.RelationIdentity.of(null, "app", "empty_table")),
                tables.stream().map(RelationalTableDefinition::identity).toList());
        assertEquals(List.of(), tables.getFirst().columns());
        assertEquals("empty comment", tables.getFirst().comment());
        assertEquals(4, calls.get());
    }

    @Test
    void postgresDoesNotInferZeroColumnsFromMissingColumnPrivileges() {
        ReactiveFormMetadataReader reader = new InformationSchemaFormMetadataReader(new ReactiveSqlExecutor() {
            public Flux<DynamicRow> query(SqlRequest request) {
                if (request.sql().contains("information_schema.tables")) {
                    // 表可见但列不可见：只有核对物理列的空表查询才能排除这张表。
                    return request.sql().contains("attisdropped") && request.sql().contains("attnum > 0")
                            ? Flux.empty() : Flux.just(DynamicRow.copyOf(Map.of("TABLE_NAME", "delete_only")));
                }
                return Flux.empty();
            }
            public Mono<Long> rowsUpdated(SqlRequest request) { throw new AssertionError(); }
        }, new MetadataQueryProfile(PostgreSqlMetadataQueries.queries()));
        assertEquals(List.of(), reader.readTables("app").collectList().block());
    }

    @Test
    void postgresEmptyRelationQueryChecksPhysicalColumnsWithinTheRequestedSchema() throws Exception {
        try (Connection connection = java.sql.DriverManager.getConnection("jdbc:h2:mem:");
             Statement statement = connection.createStatement()) {
            statement.execute("create table visible_tables(table_schema varchar, table_name varchar)");
            statement.execute("create table namespaces(oid int, nspname varchar)");
            statement.execute("create table relations(oid int, relnamespace int, relname varchar)");
            statement.execute("create table attributes(attrelid int, attnum int, attisdropped boolean)");
            statement.execute("insert into namespaces values (1, 'app'), (2, 'other')");
            statement.execute("insert into visible_tables values ('app','empty_table'), ('app','delete_only'), ('other','empty_table')");
            statement.execute("insert into relations values (10,1,'empty_table'), (11,1,'delete_only'), (12,2,'empty_table')");
            statement.execute("insert into attributes values (10,-1,false), (10,1,true), (11,1,false)");
            SqlRequest request = PostgreSqlMetadataQueries.emptyTableQuery("app");
            String sql = request.sql().replace("information_schema.tables", "visible_tables")
                    .replace("pg_catalog.obj_description(c.oid, 'pg_class')", "'empty comment'")
                    .replace("pg_catalog.pg_namespace", "namespaces")
                    .replace("pg_catalog.pg_class", "relations")
                    .replace("pg_catalog.pg_attribute", "attributes");
            try (java.sql.PreparedStatement query = connection.prepareStatement(sql)) {
                query.setObject(1, request.parameters().getFirst());
                try (java.sql.ResultSet rows = query.executeQuery()) {
                    assertTrue(rows.next());
                    assertEquals("empty_table", rows.getString(1));
                    assertFalse(rows.next());
                }
            }
        }
        assertEquals(List.of(), PostgreSqlMetadataQueries.emptyTableQuery(null).parameters());
        assertEquals(List.of("app"), PostgreSqlMetadataQueries.emptyTableQuery(" app ").parameters());
    }

    @Test
    void nullSingleTableNeverTurnsIntoWholeSchemaRead() {
        AtomicInteger calls = new AtomicInteger();
        ReactiveFormMetadataReader reader = new InformationSchemaFormMetadataReader(new ReactiveSqlExecutor() {
            public Flux<DynamicRow> query(SqlRequest request) {
                calls.incrementAndGet();
                return Flux.just(DynamicRow.copyOf(Map.of("COLUMN_NAME", "id", "DATA_TYPE", "BIGINT")));
            }
            public Mono<Long> rowsUpdated(SqlRequest request) { throw new AssertionError(); }
        }, new MetadataQueryProfile(H2MetadataQueries.queries()));
        assertThrows(IllegalArgumentException.class, () -> reader.readForm("form", "app", null).block());
        assertThrows(IllegalArgumentException.class, () -> reader.readTable("app", null).block());
        assertEquals(0, calls.get());
    }

    @Test
    void jdbcAndR2dbcReadWholeSchemaWithConstantQueryCountAndNoStaleCache() throws Exception {
        String database = "schema_bulk_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + database);
        source.setUser("sa");
        try (Connection connection = source.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("create table first_table (id bigint primary key, note varchar(32))");
            statement.execute("create index ix_note on first_table(note)");
            statement.execute("create table second_table (id bigint primary key, parent_id bigint, "
                    + "constraint fk_parent foreign key(parent_id) references first_table(id))");
            statement.execute("create view third_view as select note from first_table");
            statement.execute("comment on table first_table is '  用户说明  '");
            statement.execute("comment on table third_view is '视图说明'");
            statement.execute("create schema other_schema");
            statement.execute("create table other_schema.first_table (other_id integer)");
            statement.execute("create schema empty_schema");
            AtomicInteger syncCalls = new AtomicInteger();
            AtomicInteger reactiveCalls = new AtomicInteger();
            JdbcSqlExecutor jdbc = JdbcSqlExecutor.create(ConnectionAccessTestSupport.jdbc(source), RdbDialect.h2());
            R2dbcSqlExecutor r2dbc = R2dbcSqlExecutor.create(ConnectionAccessTestSupport.reactive(
                    ConnectionFactories.get("r2dbc:h2:mem:///" + database + ";USER=sa")),
                    RdbDialect.h2());
            JdbcFormMetadataReader sync = new DefaultJdbcFormMetadataReader(new com.flying.orm.rdb.support.ListQueryFixture() {
                public List<DynamicRow> query(SqlRequest request) {
                    syncCalls.incrementAndGet();
                    return jdbc.query(request);
                }
                public long rowsUpdated(SqlRequest request) { throw new AssertionError(); }
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
                    throw new AssertionError();
                }
            }, new MetadataQueryProfile(H2MetadataQueries.queries()), CacheRegionPolicy.metadataDefaults(), MetadataCacheInvalidator.none());
            ReactiveFormMetadataReader reactive = ReactiveFormMetadataReaders.cached(
                    new InformationSchemaFormMetadataReader(new ReactiveSqlExecutor() {
                        public Flux<DynamicRow> query(SqlRequest request) {
                            return Flux.defer(() -> {
                                reactiveCalls.incrementAndGet();
                                return r2dbc.query(request);
                            });
                        }
                        public Mono<Long> rowsUpdated(SqlRequest request) { throw new AssertionError(); }
                    }, new MetadataQueryProfile(H2MetadataQueries.queries())));
            Flux<RelationalTableDefinition> tables = reactive.readTables();
            assertEquals(0, reactiveCalls.get());
            for (int iteration = 1; iteration <= 2; iteration++) {
                assertTables(sync.readTables());
                assertTables(tables.collectList().block());
                assertEquals(3 * iteration, syncCalls.get());
                assertEquals(3 * iteration, reactiveCalls.get());
            }
            assertEquals(List.of("other_id"), sync.readTables("other_schema").getFirst().columns()
                    .stream().map(column -> column.name()).toList());
            assertEquals(com.flying.orm.core.metadata.RelationIdentity.of(null, "other_schema", "FIRST_TABLE"),
                    reactive.readTables("other_schema").blockFirst().identity());
            assertEquals(List.of(), sync.readTables("empty_schema"));
            assertEquals(List.of(), reactive.readTables("empty_schema").collectList().block());
            statement.execute("drop view third_view");
            assertEquals(2, sync.readTables().size());
            assertEquals(2L, tables.count().block());
            assertEquals("用户说明", sync.readTable("first_table").comment());
            assertEquals("用户说明", reactive.readTable("first_table").block().comment());
        }
    }

    private static void assertTables(List<RelationalTableDefinition> tables) {
        Map<String, RelationalTableDefinition> byName = tables.stream().collect(Collectors.toMap(
                t -> t.name().toLowerCase(java.util.Locale.ROOT), t -> t));
        assertEquals(java.util.Set.of("first_table", "second_table", "third_view"), byName.keySet());
        assertEquals(List.of("id", "note"), byName.get("first_table").columns().stream().map(c -> c.name()).toList());
        assertEquals(List.of("id", "parent_id"), byName.get("second_table").columns().stream().map(c -> c.name()).toList());
        assertEquals(List.of("note"), byName.get("third_view").columns().stream().map(c -> c.name()).toList());
        assertEquals("ix_note", byName.get("first_table").indexes().getFirst().name());
        assertEquals("first_table", byName.get("second_table").foreignKeys().getFirst().reference().table());
        assertEquals(1, byName.get("second_table").primaryKey().orElseThrow().columns().size());
        assertEquals(0, byName.get("third_view").indexes().size());
        assertEquals("用户说明", byName.get("first_table").comment());
        assertEquals("视图说明", byName.get("third_view").comment());
        org.junit.jupiter.api.Assertions.assertNull(byName.get("second_table").comment());
    }

    @Test
    void nativeQueriesReadOneSchemaWithoutTableBindings() {
        for (MetadataQueryProfile.Queries queries : List.of(
                PostgreSqlMetadataQueries.queries(), MySqlMetadataQueries.queries(),
                SqlServerMetadataQueries.queries(), OracleMetadataQueries.queries(),
                OracleMetadataQueries.queries12c(), H2MetadataQueries.queries())) {
            assertTrue(queries.columnQuery().create(null, null).sql().contains(" as TABLE_COMMENT"),
                    queries.snapshotDialect().name());
            for (MetadataQueryProfile.Query query : List.of(
                    queries.columnQuery(), queries.indexQuery(), queries.foreignKeyQuery(),
                    queries.uniqueConstraintQuery())) {
                for (String schema : new String[]{null, "app'; --"}) {
                    SqlRequest request = assertDoesNotThrow(() -> query.create(schema, null),
                            queries.snapshotDialect().name());
                    assertTrue(request.sql().contains(" as METADATA_TABLE"));
                    assertFalse(request.sql().contains("${"));
                    assertFalse(request.sql().contains("app'; --"));
                    assertTrue(request.parameters().stream().allMatch(value -> value.equals(schema)));
                    assertEquals(schema == null ? 0
                            : queries.snapshotDialect() == MetadataQueryProfile.SnapshotDialect.ORACLE
                            || queries.snapshotDialect() == MetadataQueryProfile.SnapshotDialect.H2
                            ? 3 : 1, request.parameters().size());
                }
            }
        }
    }
}
