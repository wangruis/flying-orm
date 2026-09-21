package com.flying.orm.rdb.metadata;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Evaluates the production ownership predicate against catalog facts in H2, not a PostgreSQL instance.
 * PostgreSQL pg_constraint documents conindid for foreign keys as well as owning constraints:
 * https://www.postgresql.org/docs/current/catalog-pg-constraint.html
 */
class PostgreSqlIndexOwnershipQueryTest {

    @TestFactory
    Stream<DynamicTest> referencesDoNotBecomeIndexOwnership() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).map(complete ->
                DynamicTest.dynamicTest((reactive ? "R2DBC" : "JDBC") + " complete=" + complete,
                        () -> verifyOwnership(captureIndexSql(reactive, complete)))));
    }

    private static String captureIndexSql(boolean reactive, boolean complete) {
        CapturedQueries queries = new CapturedQueries();
        if (reactive) {
            var reader = ReactiveFormMetadataReaders.create(new ReactiveCapture(queries), RdbDialect.postgresql());
            if (complete) {
                reader.readSnapshot("public", "accounts").block(Duration.ofSeconds(5));
            } else {
                reader.readTable("public", "accounts").block(Duration.ofSeconds(5));
            }
        } else {
            var reader = JdbcFormMetadataReaders.create(new SyncCapture(queries), RdbDialect.postgresql());
            if (complete) {
                reader.readSnapshot("public", "accounts");
            } else {
                reader.readTable("public", "accounts");
            }
        }
        assertNotNull(queries.indexSql);
        return queries.indexSql;
    }

    private static void verifyOwnership(String sql) throws Exception {
        int start = sql.lastIndexOf("and not exists (");
        int end = sql.indexOf("and n.nspname = ?", start);
        assertTrue(start >= 0 && end > start, "the index query must expose its catalog ownership predicate");
        String predicate = sql.substring(start + "and ".length(), end).trim()
                .replace("pg_catalog.pg_constraint", "constraint_fixture");

        // A schema-neutral fixture executes the original predicate, including its type/identity conditions.
        // FK rows refer to another table's index or the same table's index without owning either index.
        try (var connection = DriverManager.getConnection("jdbc:h2:mem:");
             var statement = connection.createStatement()) {
            statement.execute("create table index_fixture(indexrelid bigint, indrelid bigint)");
            statement.execute("create table constraint_fixture("
                    + "conindid bigint, contype varchar(1), conrelid bigint, confrelid bigint)");
            statement.execute("insert into index_fixture values (10,1),(20,1),(30,1),(40,1),(50,1),(60,1),(70,1)");
            statement.execute("insert into constraint_fixture values "
                    + "(10,'f',1,1),(20,'f',2,1),(30,'u',1,0),(40,'p',1,0),"
                    + "(50,'x',1,0),(70,'f',2,1),(70,'u',1,0)");
            List<Long> visible = new ArrayList<>();
            try (var rows = statement.executeQuery(
                    "select ix.indexrelid from index_fixture ix where " + predicate + " order by ix.indexrelid")) {
                while (rows.next()) {
                    visible.add(rows.getLong(1));
                }
            }
            assertEquals(List.of(10L, 20L, 60L), visible,
                    "self/cross-table FK references must preserve independent indexes; PK/UK/exclusion own theirs");
        }
    }

    private static final class CapturedQueries {
        private String indexSql;

        private List<DynamicRow> rows(SqlRequest request) {
            String sql = request.sql();
            if (sql.contains("select ci.relname as INDEX_NAME")) {
                indexSql = sql;
            }
            if (sql.contains("information_schema.columns")) {
                return List.of(DynamicRow.copyOf(Map.ofEntries(
                        Map.entry("COLUMN_NAME", "id"), Map.entry("DATA_TYPE", "bigint"),
                        Map.entry("LOGICAL_DATA_TYPE", "bigint"), Map.entry("PHYSICAL_DATA_TYPE", "bigint"),
                        Map.entry("PHYSICAL_TYPE_SCHEMA", "pg_catalog"), Map.entry("PHYSICAL_TYPE_NAME", "int8"),
                        Map.entry("PHYSICAL_ARRAY", false), Map.entry("NULLABLE", false),
                        Map.entry("COLUMN_REPRESENTABLE", true), Map.entry("IS_IDENTITY", false),
                        Map.entry("PRIMARY_KEY", false))));
            }
            if (sql.contains("obj_description")) {
                return List.of(DynamicRow.copyOf(Map.of("TABLE_REPRESENTABLE", true,
                        "TABLE_PARTITIONED", false)));
            }
            return List.of();
        }
    }

    private record SyncCapture(CapturedQueries queries) implements SyncSqlExecutor {
        @Override
        public List<DynamicRow> query(SqlRequest request) {
            return queries.rows(request);
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new UnsupportedOperationException();
        }
    }

    private record ReactiveCapture(CapturedQueries queries) implements ReactiveSqlExecutor {
        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return Flux.defer(() -> Flux.fromIterable(queries.rows(request)));
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            return Mono.error(new UnsupportedOperationException());
        }
    }
}
