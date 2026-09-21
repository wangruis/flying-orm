package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.RelationalMetadataFingerprint;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompleteSchemaMetadataReaderParityTest {

    @Test
    void mysqlSeparatesRawDefaultsFromGenerationAndUnsupportedExpressions() {
        String columns = MySqlMetadataQueries.queries().columnQuery().create("app", "orders").sql();
        assertTrue(columns.contains("null as GENERATION_EXPRESSION"));
        assertTrue(columns.contains("lower(c.EXTRA) not like '%default_generated%'"));
        assertTrue(columns.contains("then 'default expression'"));
        assertTrue(columns.contains("lower(c.DATA_TYPE) in ('timestamp', 'datetime')"));
        assertTrue(columns.contains("'^current_timestamp([(][0-6]?[)])?$'"));
        assertTrue(columns.contains("lower(c.DATA_TYPE) = 'date'"));
        assertTrue(columns.contains("'^(current_date|curdate)([(][)])?$'"));
        assertTrue(columns.contains("lower(c.DATA_TYPE) = 'time'"));
        assertTrue(columns.contains("'^(current_time|curtime)([(][0-6]?[)])?$'"));
    }

    @Test
    void mysqlKeepsDroppedDefaultRepresentableWhenGeneratedMarkerLingers() {
        String columns = MySqlMetadataQueries.queries().columnQuery().create("app", "events").sql();

        assertTrue(columns.contains("c.COLUMN_DEFAULT is null"));
    }

    @Test
    void everyBuiltInDialectPublishesCompleteRelationalSnapshotCoverage() {
        for (InformationSchemaFormMetadataReader.Queries queries : List.of(
                PostgreSqlMetadataQueries.queries(),
                MySqlMetadataQueries.queries(),
                H2MetadataQueries.queries(),
                OracleMetadataQueries.queries(),
                SqlServerMetadataQueries.queries())) {
            assertTrue(InformationSchemaFormMetadataReader.coverage(queries).isComplete());
        }
    }

    @Test
    void jdbcAndReactiveReadersExecuteTheSameCompletePostgresqlContract() {
        InformationSchemaFormMetadataReader.Queries queries = PostgreSqlMetadataQueries.queries();
        JdbcFormMetadataReader jdbc = new JdbcFormMetadataReader(new RowsSyncExecutor(), queries);
        ReactiveFormMetadataReader reactive = new InformationSchemaFormMetadataReader(
                new RowsReactiveExecutor(), PostgreSqlMetadataQueries.queries());

        SchemaSnapshot jdbcSnapshot = jdbc.readSnapshot("orders");
        SchemaSnapshot reactiveSnapshot = reactive.readSnapshot("orders").block();

        assertTrue(jdbc.snapshotCoverage().isComplete());
        assertEquals(jdbc.snapshotCoverage(), reactive.snapshotCoverage());
        assertEquals(RelationalMetadataFingerprint.of(jdbcSnapshot.completeTable().orElseThrow()),
                     RelationalMetadataFingerprint.of(reactiveSnapshot.completeTable().orElseThrow()));
        assertTrue(jdbcSnapshot.completeTable().isPresent());
    }

    @Test
    void completeQueriesKeepAllNamesBoundAndProjectEveryRequiredFact() {
        for (InformationSchemaFormMetadataReader.Queries queries : List.of(
                PostgreSqlMetadataQueries.queries(),
                MySqlMetadataQueries.queries())) {
            assertTrue(InformationSchemaFormMetadataReader.coverage(queries).isComplete());
            assertEquals(List.of("orders"), queries.tableQuery().create(null, "orders").parameters());
            assertEquals(List.of("orders", "app"),
                         queries.checkConstraintQuery().create("app", "orders").parameters());
            String columnSql = queries.columnQuery().create("app", "orders").sql();
            assertTrue(columnSql.contains("COLUMN_DEFAULT"));
            assertTrue(columnSql.contains("COLUMN_CHARSET"));
            assertTrue(columnSql.contains("COLUMN_COLLATION"));
            assertTrue(columnSql.contains("COLUMN_REPRESENTABLE"));
            assertTrue(columnSql.contains("UNSUPPORTED_COLUMN_REASON"));
            assertTrue(queries.indexQuery().create("app", "orders").sql().contains("INDEX_DIRECTION"));
            assertTrue(queries.foreignKeyQuery().create("app", "orders").sql().contains("ON_DELETE"));
            assertTrue(queries.foreignKeyQuery().create("app", "orders").sql().contains("ON_UPDATE"));
            assertTrue(queries.tableQuery().create("app", "orders").sql().contains("TABLE_REPRESENTABLE"));
        }

        InformationSchemaFormMetadataReader.Queries postgresql =
                PostgreSqlMetadataQueries.queries();
        assertTrue(postgresql.primaryKeyQuery().create("app", "orders").sql()
                                     .contains("CONSTRAINT_REPRESENTABLE"));
        assertTrue(postgresql.uniqueConstraintQuery().create("app", "orders").sql()
                                     .contains("CONSTRAINT_REPRESENTABLE"));
        assertTrue(postgresql.foreignKeyQuery().create("app", "orders").sql()
                                     .contains("CONSTRAINT_REPRESENTABLE"));
        String postgresqlColumns = postgresql.columnQuery().create("app", "orders").sql();
        assertTrue(postgresqlColumns.contains("GENERATION_START"));
        assertTrue(postgresqlColumns.contains("GENERATION_INCREMENT"));
        assertTrue(postgresqlColumns.contains("GENERATION_CACHE"));
        assertTrue(postgresqlColumns.contains("seqmin"));
        assertTrue(postgresqlColumns.contains("seqmax"));
        assertTrue(postgresqlColumns.contains("seqtypid"));
        String postgresqlPrimaryKey = postgresql.primaryKeyQuery().create("app", "orders").sql();
        String postgresqlUnique = postgresql.uniqueConstraintQuery().create("app", "orders").sql();
        for (String constraintSql : List.of(postgresqlPrimaryKey, postgresqlUnique)) {
            assertTrue(constraintSql.contains("indnkeyatts"));
            assertTrue(constraintSql.contains("opcdefault"));
            assertTrue(constraintSql.contains("indcollation"));
        }

        InformationSchemaFormMetadataReader.Queries mysql = MySqlMetadataQueries.queries();
        assertTrue(mysql.primaryKeyQuery().create("app", "orders").sql()
                              .contains("CONSTRAINT_REPRESENTABLE"));
        String mysqlUnique = mysql.uniqueConstraintQuery().create("app", "orders").sql();
        assertTrue(mysqlUnique.contains("CONSTRAINT_REPRESENTABLE"));
        assertTrue(mysqlUnique.contains("INDEX_DIRECTION"));
        assertTrue(mysqlUnique.contains("s.COLLATION in ('A', 'D')"));
        assertTrue(mysql.foreignKeyQuery().create("app", "orders").sql()
                              .contains("CONSTRAINT_REPRESENTABLE"));
        assertTrue(mysql.indexQuery().create("app", "orders").sql().contains("IS_VISIBLE"));
        assertFalse(mysql.indexQuery().create("app", "orders").sql().contains("'FOREIGN KEY'"));
    }

    @Test
    void jdbcAndReactiveReadersReconstructPostgresqlVarcharCheckCasts() {
        String expression = "((val)::text = ANY "
                + "((ARRAY['a'::character varying, 'b'::character varying])::text[]))";
        RowsSyncExecutor syncExecutor = new RowsSyncExecutor();
        syncExecutor.checkExpression = expression;
        RowsReactiveExecutor reactiveExecutor = new RowsReactiveExecutor();
        reactiveExecutor.checkExpression = expression;
        SchemaSnapshot sync = new JdbcFormMetadataReader(
                syncExecutor, PostgreSqlMetadataQueries.queries()).readSnapshot("orders");
        SchemaSnapshot reactive = new InformationSchemaFormMetadataReader(
                reactiveExecutor, PostgreSqlMetadataQueries.queries()).readSnapshot("orders").block();

        assertEquals(CheckPredicate.in("val", List.of("a", "b")),
                     sync.completeTable().orElseThrow().checks().getFirst().predicate());
        assertEquals(RelationalMetadataFingerprint.of(sync.completeTable().orElseThrow()),
                     RelationalMetadataFingerprint.of(reactive.completeTable().orElseThrow()));
    }

    private static List<DynamicRow> rows(SqlRequest request, String checkExpression) {
        String sql = request.sql();
        if (sql.contains("information_schema.columns")) {
            List<DynamicRow> columns = new java.util.ArrayList<>(List.of(dynamicRow(
                                      "COLUMN_NAME", "id", "DATA_TYPE", "bigint",
                                      "LOGICAL_DATA_TYPE", "bigint",
                                      "PHYSICAL_DATA_TYPE", "bigint",
                                      "PHYSICAL_TYPE_SCHEMA", "pg_catalog",
                                      "PHYSICAL_TYPE_NAME", "int8",
                                      "PHYSICAL_ARRAY", false,
                                      "NULLABLE", false, "IS_IDENTITY", true)));
            if (checkExpression != null) {
                columns.add(dynamicRow("COLUMN_NAME", "val", "DATA_TYPE", "character varying",
                        "LOGICAL_DATA_TYPE", "character varying",
                        "PHYSICAL_DATA_TYPE", "character varying",
                        "PHYSICAL_TYPE_SCHEMA", "pg_catalog", "PHYSICAL_TYPE_NAME", "varchar",
                        "PHYSICAL_ARRAY", false, "NULLABLE", true));
            }
            return columns;
        }
        if (sql.contains("obj_description")) {
            return List.of(dynamicRow("TABLE_COMMENT", "订单"));
        }
        if (sql.contains("con.contype = 'p'")) {
            return List.of(dynamicRow("CONSTRAINT_NAME", "pk_orders", "COLUMN_NAME", "id"));
        }
        if (checkExpression != null && sql.contains("con.contype = 'c'")) {
            return List.of(dynamicRow("CONSTRAINT_NAME", "ck_val",
                    "CHECK_EXPRESSION", checkExpression, "CHECK_REPRESENTABLE", true));
        }
        return List.of();
    }

    private static DynamicRow dynamicRow(Object... values) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int index = 0; index < values.length; index += 2) {
            row.put((String) values[index], values[index + 1]);
        }
        return DynamicRow.copyOf(row);
    }

    private static final class RowsSyncExecutor implements SyncSqlExecutor {
        private String checkExpression;

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            return rows(request, checkExpression);
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

    private static final class RowsReactiveExecutor implements ReactiveSqlExecutor {
        private String checkExpression;

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return Flux.fromIterable(rows(request, checkExpression));
        }

        @Override
        public reactor.core.publisher.Mono<Long> rowsUpdated(SqlRequest request) {
            return reactor.core.publisher.Mono.error(new UnsupportedOperationException());
        }
    }
}
