package com.flying.orm.rdb.metadata;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.schema.SchemaSnapshotFingerprint;
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
        String columns = MySqlReactiveFormMetadataReader.queries().columnQuery().create("app", "orders").sql();
        assertTrue(columns.contains("null as GENERATION_EXPRESSION"));
        assertTrue(columns.contains("lower(c.EXTRA) not like '%default_generated%'"));
        assertTrue(columns.contains("then 'default expression'"));
        assertTrue(columns.contains("lower(c.DATA_TYPE) in ('timestamp', 'datetime')"));
        assertTrue(columns.contains("'^current_timestamp([(][0-6]?[)])?$'"));
        assertTrue(columns.contains("lower(c.DATA_TYPE) = 'date'"));
        assertTrue(columns.contains("'^current_date([(][)])?$'"));
        assertTrue(columns.contains("lower(c.DATA_TYPE) = 'time'"));
        assertTrue(columns.contains("'^current_time([(][0-6]?[)])?$'"));
    }

    @Test
    void everyBuiltInDialectPublishesCompleteRelationalSnapshotCoverage() {
        for (InformationSchemaFormMetadataReader.Queries queries : List.of(
                PostgreSqlReactiveFormMetadataReader.queries(),
                MySqlReactiveFormMetadataReader.queries(),
                H2ReactiveFormMetadataReader.queries(),
                OracleReactiveFormMetadataReader.queries(),
                SqlServerReactiveFormMetadataReader.queries())) {
            assertTrue(InformationSchemaFormMetadataReader.coverage(queries).isComplete());
        }
    }

    @Test
    void jdbcAndReactiveReadersExecuteTheSameCompletePostgresqlContract() {
        InformationSchemaFormMetadataReader.Queries queries = PostgreSqlReactiveFormMetadataReader.queries();
        JdbcFormMetadataReader jdbc = new JdbcFormMetadataReader(new RowsSyncExecutor(), queries);
        ReactiveFormMetadataReader reactive = PostgreSqlReactiveFormMetadataReader.create(new RowsReactiveExecutor());

        SchemaSnapshot jdbcSnapshot = jdbc.readSnapshot("orders");
        SchemaSnapshot reactiveSnapshot = reactive.readSnapshot("orders").block();

        assertTrue(jdbc.snapshotCoverage().isComplete());
        assertEquals(jdbc.snapshotCoverage(), reactive.snapshotCoverage());
        assertEquals(SchemaSnapshotFingerprint.of(jdbcSnapshot),
                     SchemaSnapshotFingerprint.of(reactiveSnapshot));
        assertTrue(jdbcSnapshot.completeTable().isPresent());
    }

    @Test
    void completeQueriesKeepAllNamesBoundAndProjectEveryRequiredFact() {
        for (InformationSchemaFormMetadataReader.Queries queries : List.of(
                PostgreSqlReactiveFormMetadataReader.queries(),
                MySqlReactiveFormMetadataReader.queries())) {
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
                PostgreSqlReactiveFormMetadataReader.queries();
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

        InformationSchemaFormMetadataReader.Queries mysql = MySqlReactiveFormMetadataReader.queries();
        assertTrue(mysql.primaryKeyQuery().create("app", "orders").sql()
                              .contains("CONSTRAINT_REPRESENTABLE"));
        assertTrue(mysql.uniqueConstraintQuery().create("app", "orders").sql()
                              .contains("CONSTRAINT_REPRESENTABLE"));
        assertTrue(mysql.foreignKeyQuery().create("app", "orders").sql()
                              .contains("CONSTRAINT_REPRESENTABLE"));
        assertTrue(mysql.indexQuery().create("app", "orders").sql().contains("IS_VISIBLE"));
        assertFalse(mysql.indexQuery().create("app", "orders").sql().contains("'FOREIGN KEY'"));
    }

    private static List<DynamicRow> rows(SqlRequest request) {
        String sql = request.sql();
        if (sql.contains("information_schema.columns")) {
            return List.of(dynamicRow("COLUMN_NAME", "id", "DATA_TYPE", "bigint",
                                      "NULLABLE", false, "IS_IDENTITY", true));
        }
        if (sql.contains("obj_description")) {
            return List.of(dynamicRow("TABLE_COMMENT", "订单"));
        }
        if (sql.contains("con.contype = 'p'")) {
            return List.of(dynamicRow("CONSTRAINT_NAME", "pk_orders", "COLUMN_NAME", "id"));
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

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            return rows(request);
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

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return Flux.fromIterable(rows(request));
        }

        @Override
        public reactor.core.publisher.Mono<Long> rowsUpdated(SqlRequest request) {
            return reactor.core.publisher.Mono.error(new UnsupportedOperationException());
        }
    }
}
