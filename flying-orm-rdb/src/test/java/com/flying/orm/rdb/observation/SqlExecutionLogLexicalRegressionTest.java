package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.batch.BatchAffectedRows;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.internal.plan.SqlStatementCompiler;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlExecutionLogLexicalRegressionTest {

    @TestFactory
    Stream<DynamicTest> masksLiteralsInAmbiguousPostgresqlRegionsWithoutDialectMetadata() {
        return Stream.of(SqlExecutionBackend.JDBC, SqlExecutionBackend.R2DBC, SqlExecutionBackend.UNKNOWN)
                .flatMap(backend -> sensitiveSql()
                        .map(sql -> DynamicTest.dynamicTest(backend + "/" + sql, () -> {
                            String message = format(backend, sql);
                            assertFalse(message.contains("private-rule"), message);
                        })));
    }

    @TestFactory
    Stream<DynamicTest> batchLogsUseTheSameConservativeLiteralBoundaries() {
        return Stream.of(SqlExecutionBackend.JDBC, SqlExecutionBackend.R2DBC, SqlExecutionBackend.UNKNOWN)
                .flatMap(backend -> sensitiveSql().map(sql -> DynamicTest.dynamicTest(backend + "/" + sql, () -> {
                    var request = new BatchExecutionObservation.BatchWriteRequestView(
                            sql, 0, backend);
                    var observation = new BatchExecutionObservation(
                            BatchExecutionEventType.PROGRESS, request, BatchExecutionState.SUCCESS,
                            1, BatchAffectedRows.known(1), 1, 0, 0, 1, SqlFailureCategory.NONE, null);

                    String message = SqlExecutionLogFormatter.batch(observation,
                            SqlExecutionLogOptions.defaults().withSql(true), SqlExecutionLogSelection.defaults());

                    assertFalse(message.contains("private-rule"), message);
                })));
    }

    @TestFactory
    Stream<DynamicTest> preservesUnambiguousSqlServerBracketIdentifiersAndEscapedClosers() {
        return Stream.of(SqlExecutionBackend.JDBC, SqlExecutionBackend.R2DBC, SqlExecutionBackend.UNKNOWN)
                .map(backend -> DynamicTest.dynamicTest(backend.toString(), () -> {
                    String sql = "select [order], [escaped]]name], [Price$USD] from [dbo].[items]";
                    assertTrue(format(backend, sql).contains(sql));
                }));
    }

    @TestFactory
    Stream<DynamicTest> redactionDoesNotChangeValidPostgresqlExecutionSql() {
        return Stream.of(
                "select '\\' as slash, 'private-rule' as token -- '",
                "select '\\' as slash, 'private-rule' as token")
                .map(sql -> DynamicTest.dynamicTest(sql, () ->
                        assertEquals(sql, SqlStatementCompiler.compile(
                                sql, 0, SqlBindMarkerStyle.NATIVE, "postgresql").sql())));
    }

    @TestFactory
    Stream<DynamicTest> preservesExplicitAndUnambiguousLiteralMasking() {
        return Stream.of(
                "select E'private-rule\\'suffix'",
                "select $$private-rule$$",
                "select q'[private-rule]' from dual",
                "select 'private-rule''suffix'",
                "select 'private-rule\\\\' as token")
                .map(sql -> DynamicTest.dynamicTest(sql, () -> {
                    String message = format(SqlExecutionBackend.JDBC, sql);
                    assertFalse(message.contains("private-rule"), message);
                    assertFalse(message.contains("<invalid SQL>"), message);
                    assertTrue(message.contains("***"), message);
                }));
    }

    private static Stream<String> sensitiveSql() {
        return Stream.of(
                "select '\\' as slash, 'private-rule' as token -- '",
                "select '\\' as slash, 'private-rule' as token",
                "select ARRAY['private-rule']",
                "select ARRAY[ARRAY['private-rule']]",
                "select ARRAY[$$private-rule$$]",
                "select ARRAY[$tag$private-rule$tag$]",
                "select ARRAY[$$prefix]private-rule$$]",
                "select 1 # 2, 'private-rule'",
                "select 1 # 2, $$private-rule$$");
    }

    private static String format(SqlExecutionBackend backend, String sql) {
        SqlExecutionObservation observation = new SqlExecutionObservation(
                SqlExecutionOperation.QUERY, backend, SqlStatementType.SELECT,
                SqlExecutionStatus.SUCCESS, SqlFailureCategory.NONE, sql, 0, 0, 1, 1, null);
        return SqlExecutionLogFormatter.sql(observation,
                SqlExecutionLogOptions.defaults().withSql(true), SqlExecutionLogSelection.defaults(),
                List.of());
    }
}
