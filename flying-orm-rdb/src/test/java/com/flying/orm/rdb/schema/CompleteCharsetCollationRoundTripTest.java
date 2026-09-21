package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompleteCharsetCollationRoundTripTest {

    private static final String MYSQL_CHARSET = "utf8mb4";
    private static final String MYSQL_COLLATION = "utf8mb4_0900_ai_ci";
    private static final String SQL_SERVER_COLLATION = "SQL_Latin1_General_CP1_CI_AS";

    @TestFactory
    Stream<DynamicTest> jdbcAndReactiveRoundTrips() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(
                DynamicTest.dynamicTest("charset-only / reactive=" + reactive,
                        () -> mysqlCharsetOnlyConvergesWithDatabaseSelectedCollation(reactive)),
                DynamicTest.dynamicTest("collation-only / reactive=" + reactive,
                        () -> mysqlCollationOnlyConvergesWithEffectiveCharset(reactive)),
                DynamicTest.dynamicTest("MySQL table defaults / reactive=" + reactive,
                        () -> mysqlExplicitTableDefaultsRemainObservableAndConverge(reactive)),
                DynamicTest.dynamicTest("SQL Server database default / reactive=" + reactive,
                        () -> sqlServerExplicitDatabaseDefaultRemainsObservableAndConverges(reactive)),
                DynamicTest.dynamicTest("unspecified / reactive=" + reactive,
                        () -> unspecifiedOptionsDoNotConstrainEffectiveDatabaseValues(reactive)),
                DynamicTest.dynamicTest("mismatch / reactive=" + reactive,
                        () -> explicitMismatchesStillChangeTheColumn(reactive)),
                DynamicTest.dynamicTest("missing actual / reactive=" + reactive,
                        () -> missingActualOptionsCannotSatisfyExplicitDesiredOptions(reactive))));
    }

    private static void mysqlCharsetOnlyConvergesWithDatabaseSelectedCollation(boolean reactive) {
        assertDiff(reactive, RdbDialect.mysql(), MYSQL_CHARSET, MYSQL_COLLATION,
                MYSQL_CHARSET, null, false);
    }

    private static void mysqlCollationOnlyConvergesWithEffectiveCharset(boolean reactive) {
        assertDiff(reactive, RdbDialect.mysql(), MYSQL_CHARSET, MYSQL_COLLATION,
                null, MYSQL_COLLATION, false);
    }

    private static void mysqlExplicitTableDefaultsRemainObservableAndConverge(boolean reactive) {
        List<SqlRequest> requests = assertDiff(reactive, RdbDialect.mysql(), MYSQL_CHARSET, MYSQL_COLLATION,
                MYSQL_CHARSET, MYSQL_COLLATION, false);
        String columns = columnQuery(requests);
        assertTrue(columns.contains("c.CHARACTER_SET_NAME as COLUMN_CHARSET"));
        assertTrue(columns.contains("c.COLLATION_NAME as COLUMN_COLLATION"));
    }

    private static void sqlServerExplicitDatabaseDefaultRemainsObservableAndConverges(boolean reactive) {
        List<SqlRequest> requests = assertDiff(reactive, RdbDialect.sqlServer(), null, SQL_SERVER_COLLATION,
                null, SQL_SERVER_COLLATION, false);
        assertTrue(columnQuery(requests).contains("c.COLLATION_NAME as COLUMN_COLLATION"));
    }

    private static void unspecifiedOptionsDoNotConstrainEffectiveDatabaseValues(boolean reactive) {
        assertDiff(reactive, RdbDialect.mysql(), MYSQL_CHARSET, MYSQL_COLLATION, null, null, false);
        assertDiff(reactive, RdbDialect.sqlServer(), null, SQL_SERVER_COLLATION, null, null, false);
    }

    private static void explicitMismatchesStillChangeTheColumn(boolean reactive) {
        assertDiff(reactive, RdbDialect.mysql(), "latin1", "latin1_swedish_ci", MYSQL_CHARSET, null, true);
        assertDiff(reactive, RdbDialect.mysql(), MYSQL_CHARSET, MYSQL_COLLATION, null, "utf8mb4_bin", true);
        assertDiff(reactive, RdbDialect.sqlServer(), null, SQL_SERVER_COLLATION,
                null, "Latin1_General_100_BIN2", true);
    }

    private static void missingActualOptionsCannotSatisfyExplicitDesiredOptions(boolean reactive) {
        assertDiff(reactive, RdbDialect.mysql(), null, MYSQL_COLLATION, MYSQL_CHARSET, null, true);
        assertDiff(reactive, RdbDialect.mysql(), MYSQL_CHARSET, null, null, MYSQL_COLLATION, true);
        assertDiff(reactive, RdbDialect.sqlServer(), null, null, null, SQL_SERVER_COLLATION, true);
    }

    private static List<SqlRequest> assertDiff(boolean reactive, RdbDialect dialect,
                                               String actualCharset, String actualCollation,
                                               String desiredCharset, String desiredCollation,
                                               boolean change) {
        List<SqlRequest> requests = new ArrayList<>();
        Map<String, Object> column = new LinkedHashMap<>();
        column.put("COLUMN_NAME", "value");
        String type = dialect.schema().generatedValueStyle() == SchemaDialect.GeneratedValueStyle.SQL_SERVER
                ? "nvarchar" : "varchar";
        column.put("DATA_TYPE", type);
        column.put("PHYSICAL_DATA_TYPE", type);
        column.put("CHARACTER_MAXIMUM_LENGTH", 32);
        column.put("NULLABLE", true);
        column.put("COLUMN_REPRESENTABLE", true);
        column.put("COLUMN_CHARSET", actualCharset);
        column.put("COLUMN_COLLATION", actualCollation);
        SchemaSnapshot actual = read(reactive, dialect, column, requests);
        assertTrue(actual.completeTable().isPresent());
        assertEquals(actualCharset, actual.columns().value().getFirst().charset());
        assertEquals(actualCollation, actual.columns().value().getFirst().collation());
        RelationalTableDefinition desired = RelationalTableDefinition.builder(RelationIdentity.table("options"))
                .addColumn(ColumnDefinition.builder("value", "VARCHAR").length(32)
                        .charset(desiredCharset).collation(desiredCollation).build())
                .build();
        SchemaCompatibilityReport report = SchemaDiffer.diff(desired, actual, dialect.capabilities(),
                SchemaCompatibilityMode.EXACT, null, dialect.schema());
        assertEquals(change ? List.of(SchemaOperation.Kind.CHANGE_COLUMN) : List.of(),
                report.operations().stream().map(SchemaOperation::kind).toList());
        return requests;
    }

    private static SchemaSnapshot read(boolean reactive, RdbDialect dialect,
                                       Map<String, Object> column, List<SqlRequest> requests) {
        if (reactive) {
            ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
                @Override
                public Flux<DynamicRow> query(SqlRequest request) {
                    return Flux.fromIterable(rows(request, column, requests));
                }

                @Override
                public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.error(new UnsupportedOperationException());
                }
            };
            return ReactiveFormMetadataReaders.create(executor, dialect).readSnapshot("options").block();
        }
        SyncSqlExecutor executor = new SyncSqlExecutor() {
            @Override
            public List<DynamicRow> query(SqlRequest request) {
                return rows(request, column, requests);
            }

            @Override
            public long rowsUpdated(SqlRequest request) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                throw new UnsupportedOperationException();
            }
        };
        return JdbcFormMetadataReaders.create(executor, dialect).readSnapshot("options");
    }

    private static List<DynamicRow> rows(SqlRequest request, Map<String, Object> column,
                                          List<SqlRequest> requests) {
        requests.add(request);
        if (request.sql().contains("end as DATA_TYPE")) {
            return List.of(DynamicRow.copyOf(column));
        }
        return request.sql().contains("TABLE_COMMENT")
                ? List.of(DynamicRow.copyOf(Map.of("TABLE_REPRESENTABLE", true))) : List.of();
    }

    private static String columnQuery(List<SqlRequest> requests) {
        return requests.stream().map(SqlRequest::sql).filter(sql -> sql.contains("end as DATA_TYPE"))
                .findFirst().orElseThrow();
    }
}
