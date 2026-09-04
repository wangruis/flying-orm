package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.type.DatabaseType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaDialectLiteralReadbackTest {

    @Test
    void h2NumericCastsDoNotRewriteQuotedCheckValues() {
        String value = "CAST(1 AS DECIMAL) and 'CAST(2 AS NUMERIC)'";
        assertEquals(CheckPredicate.compare("value", CheckPredicate.ComparisonOperator.NOT_EQUAL, value),
                RelationalMetadataValueParser.checkPredicate(
                        "(\"value\" <> 'CAST(1 AS DECIMAL) and ''CAST(2 AS NUMERIC)''')",
                        Map.of("value", DatabaseType.of("VARCHAR")),
                        InformationSchemaFormMetadataReader.SnapshotDialect.H2));
    }

    @Test
    void sqlServerUnicodeDefaultReturnsTheOriginalLiteral() {
        assertEquals(ColumnDefault.literal("中文"), RelationalMetadataValueParser.columnDefault(
                "(N'中文')", DatabaseType.of("VARCHAR"),
                InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER));
    }

    @Test
    void sqlServerChecksDecodeIdentifiersWithoutRewritingStringContents() {
        assertEquals(CheckPredicate.compare("value", CheckPredicate.ComparisonOperator.NOT_EQUAL, "中文[值]"),
                RelationalMetadataValueParser.checkPredicate("([value]<>N'中文[值]')",
                        Map.of("value", DatabaseType.of("VARCHAR")),
                        InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER));
    }

    @Test
    void sqlServerDateDefaultRoundTripsItsCanonicalCast() {
        assertEquals(ColumnDefault.currentDate(), RelationalMetadataValueParser.columnDefault(
                "(CONVERT([date],getdate()))", DatabaseType.of("DATE"),
                InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER));
    }

    @Test
    void sqlServerTimeDefaultRoundTripsItsCanonicalCast() {
        for (String expression : new String[]{"(CONVERT([time],getdate(),0))",
                "(cast(current_timestamp as time))"}) {
            assertEquals(ColumnDefault.currentTime(), RelationalMetadataValueParser.columnDefault(
                    expression, DatabaseType.of("TIME"),
                    InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER));
        }
    }

    @Test
    void mysqlHexCheckAndStoredEscapedCheckReturnTheOriginalString() {
        String value = "C:\\new\\file'中文";
        String hex = java.util.HexFormat.of().formatHex(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        for (String expression : new String[]{"(`value` <> _utf8mb4 X'" + hex + "')",
                "(`value` <> _utf8mb4'C:\\\\new\\\\file\\'中文')"}) {
            assertEquals(CheckPredicate.compare("value", CheckPredicate.ComparisonOperator.NOT_EQUAL, value),
                    RelationalMetadataValueParser.checkPredicate(expression,
                            Map.of("value", DatabaseType.of("VARCHAR")),
                            InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL));
        }
        assertEquals(ColumnDefault.literal(value), RelationalMetadataValueParser.columnDefault(
                value, DatabaseType.of("VARCHAR"),
                InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL));
    }
}
