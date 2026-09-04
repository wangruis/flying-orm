package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlExecutionLogFormatterTest {

    @Test
    void masksMysqlDoubleQuotedStringLiteralsWhenSqlLoggingIsEnabled() {
        SqlExecutionObservation observation = new SqlExecutionObservation(
                SqlExecutionOperation.QUERY,
                SqlExecutionBackend.JDBC,
                SqlStatementType.SELECT,
                SqlExecutionStatus.SUCCESS,
                SqlFailureCategory.NONE,
                "select \"secret-value\" as credential",
                0,
                0,
                1,
                1,
                null);

        String message = SqlExecutionLogFormatter.sql(
                observation,
                SqlExecutionLogOptions.defaults().withSql(true),
                SqlExecutionLogSelection.defaults(),
                List.of(),
                SqlTransactionSource.AUTO_COMMIT);

        assertFalse(message.contains("secret-value"));
        assertTrue(message.contains("\"***\""));
    }

    @Test
    void keepsSqlLogEntryOnOneLineWhenSqlContainsLineBreaks() {
        SqlExecutionObservation observation = new SqlExecutionObservation(
                SqlExecutionOperation.QUERY,
                SqlExecutionBackend.JDBC,
                SqlStatementType.SELECT,
                SqlExecutionStatus.SUCCESS,
                SqlFailureCategory.NONE,
                "select 1\r\n-- forged-entry",
                0,
                0,
                0,
                1,
                null);

        String message = SqlExecutionLogFormatter.sql(
                observation,
                SqlExecutionLogOptions.defaults().withSql(true),
                SqlExecutionLogSelection.defaults(),
                List.of(),
                SqlTransactionSource.AUTO_COMMIT);

        assertFalse(message.contains("\r"));
        assertFalse(message.contains("\n"));
    }

    @Test
    void appliesExplicitFullMaskBeforeBuiltInArrayAndTypedNullRendering() {
        SqlExecutionObservation observation = new SqlExecutionObservation(
                SqlExecutionOperation.UPDATE,
                SqlExecutionBackend.JDBC,
                SqlStatementType.UPDATE,
                SqlExecutionStatus.SUCCESS,
                SqlFailureCategory.NONE,
                "update account set binary_value=?, password_chars=?, nickname=?",
                3,
                1,
                1,
                1,
                null);
        SqlExecutionLogOptions options = SqlExecutionLogOptions.defaults()
                .withParameters(true)
                .withRedactionRule((parameterIndex, valueType) -> true);

        String message = SqlExecutionLogFormatter.sql(
                observation,
                options,
                SqlExecutionLogSelection.defaults(),
                List.<Object>of(new byte[]{1, 2}, new char[]{'s', 'e', 'c'}, new SqlNullParameter(String.class)),
                SqlTransactionSource.AUTO_COMMIT);

        String parameters = message.substring(message.indexOf("parameters="));
        assertEquals("parameters=[<masked>, <masked>, <masked>]", parameters);
    }

    @Test
    void keepsMaskedParameterTextOnOneLogLine() {
        SqlExecutionObservation observation = new SqlExecutionObservation(
                SqlExecutionOperation.QUERY,
                SqlExecutionBackend.R2DBC,
                SqlStatementType.SELECT,
                SqlExecutionStatus.SUCCESS,
                SqlFailureCategory.NONE,
                "select ?",
                1,
                0,
                1,
                1,
                null);
        String message = SqlExecutionLogFormatter.sql(
                observation,
                SqlExecutionLogOptions.defaults().withParameters(true),
                SqlExecutionLogSelection.defaults(),
                List.of("\rvalue\n", "\u2028", "\u2029", "\u001B"),
                SqlTransactionSource.AUTO_COMMIT);

        assertFalse(message.contains("\r"));
        assertFalse(message.contains("\n"));
        assertFalse(message.contains("\u2028"));
        assertFalse(message.contains("\u2029"));
        assertFalse(message.contains("\u001B"));
    }
}
