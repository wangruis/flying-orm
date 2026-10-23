package com.flying.orm.rdb.type;

import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.codec.LargeObjectValueCodec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.time.Duration;
import java.time.Period;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class DatabaseTypesTest {

    private static volatile Class<?> parameterTypeResult;
    private static volatile LogicalType logicalTypeResult;

    @Test
    void dialectIndependentTypesDoNotAllocateNormalizedDialectStrings() {
        java.lang.management.ThreadMXBean management = java.lang.management.ManagementFactory.getThreadMXBean();
        assumeTrue(management instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocations = (com.sun.management.ThreadMXBean) management;
        assumeTrue(allocations.isThreadAllocatedMemorySupported());
        allocations.setThreadAllocatedMemoryEnabled(true);
        DatabaseType[] types = {DatabaseType.of("BIGINT"), DatabaseType.of("DECIMAL"),
                DatabaseType.of("TIMESTAMP"), DatabaseType.of("VARCHAR"), DatabaseType.of("TEXT[]"),
                DatabaseType.of("DATE"), DatabaseType.of("CUSTOM_TYPE")};
        String[] dialects = {" PostgreSQL ", " MySQL ", " OrAcLe ", " SqlServer "};
        resolveTypes(types, dialects, 30_000);
        long thread = Thread.currentThread().threadId();
        long before = allocations.getThreadAllocatedBytes(thread);
        resolveTypes(types, dialects, 30_000);
        double bytesPerCall = (allocations.getThreadAllocatedBytes(thread) - before) / 30_000.0;
        assertTrue(bytesPerCall < 16, "dialect-independent type resolution allocated " + bytesPerCall + " B/call");
    }

    private static void resolveTypes(DatabaseType[] types, String[] dialects, int iterations) {
        for (int index = 0; index < iterations; index++) {
            DatabaseType type = types[index % types.length];
            String dialect = dialects[index % dialects.length];
            parameterTypeResult = DatabaseTypes.parameterType(type, dialect, true);
            logicalTypeResult = DatabaseTypes.effectiveLogicalType(type, dialect);
        }
    }

    @Test
    void deferredDialectNormalizationPreservesNullInputContracts() {
        assertThrows(NullPointerException.class, () -> DatabaseTypes.parameterType(DatabaseType.of("BIGINT"), null, true));
        assertThrows(NullPointerException.class, () -> DatabaseTypes.parameterType(DatabaseType.of("TEXT[]"), null, true));
        assertThrows(NullPointerException.class, () -> DatabaseTypes.effectiveLogicalType(DatabaseType.of("BIGINT"), null));
        assertEquals(LogicalType.OTHER, DatabaseTypes.effectiveLogicalType(DatabaseType.of("TEXT[]"), null));
    }

    @Test
    void dialectNormalizationPreservesWhitespaceCaseAliasesAndUnknownNames() {
        for (String dialect : List.of("h2", "mysql", "postgresql", "oracle", "sqlserver", "sql_server", "generic", "custom")) {
            String canonical = dialect.toUpperCase(Locale.ROOT);
            for (String input : List.of(dialect, canonical, " \t" + dialect + "\n",
                    canonical.substring(0, 1) + dialect.substring(1))) {
                for (String declaration : List.of("BIGINT", "BOOLEAN", "BIT(1)", "TIMESTAMPTZ", "TIME",
                        "UUID", "INTERVAL DAY TO SECOND", "TEXT[]", "CUSTOM_TYPE")) {
                    DatabaseType type = DatabaseType.of(declaration);
                    assertEquals(DatabaseTypes.parameterType(type, canonical, false),
                            DatabaseTypes.parameterType(type, input, false), input + ": " + declaration);
                    assertEquals(DatabaseTypes.effectiveLogicalType(type, canonical),
                            DatabaseTypes.effectiveLogicalType(type, input));
                    assertEquals(DatabaseTypes.logicalDeclaration(declaration, canonical),
                            DatabaseTypes.logicalDeclaration(declaration, input));
                }
                assertEquals(DatabaseTypes.legacyOracleBoolean(canonical, false),
                        DatabaseTypes.legacyOracleBoolean(input, false));
                assertEquals(DatabaseTypes.mysqlOffsetTimestamp(DatabaseType.of("TIMESTAMPTZ"), canonical),
                        DatabaseTypes.mysqlOffsetTimestamp(DatabaseType.of("TIMESTAMPTZ"), input));
            }
        }
        // 保留原 Locale.ROOT 大写转换语义，包括一次映射为多个字符的 Unicode 输入。
        assertEquals(java.util.UUID.class, DatabaseTypes.parameterType(DatabaseType.of("UUID"), "po\uFB06gresql", true));
    }

    @Test
    void doesNotClassifyCustomNamesByKnownTypeSubstrings() {
        assertFalse(DatabaseTypes.supportsScalar(DatabaseType.of("CONTEXT_BOOL"), "postgresql"));
        assertFalse(DatabaseTypes.supportsScalar(DatabaseType.of("DECIMAL_POINT"), "postgresql"));
        assertFalse(DatabaseTypes.supportsScalar(DatabaseType.of("TIMESTAMP_AUDIT"), "postgresql"));
    }

    @Test
    void keepsMySqlBitAndTinyIntSemanticsDistinct() {
        assertEquals(Boolean.class,
                     DatabaseTypes.parameterType(DatabaseType.of("BIT(1)"), "mysql", true));
        assertEquals(Integer.class,
                     DatabaseTypes.parameterType(DatabaseType.of("TINYINT(1)"), "mysql", true));
    }

    @Test
    void malformedKnownPrefixRemainsAnUnknownDriverType() {
        assertEquals(Object.class,
                     DatabaseTypes.parameterType(
                             DatabaseType.of("TIMESTAMPTZ(6) INVALID"), "mysql", true));
    }

    @Test
    void mapsFiveDialectMetadataWithoutSubstringGuessing() {
        assertEquals("TIMESTAMPTZ",
                     DatabaseTypes.logicalDeclaration("timestamp with time zone", "h2"));
        assertEquals("BIGINT UNSIGNED ZEROFILL",
                     DatabaseTypes.logicalDeclaration("bigint(20) zerofill", "mysql"));
        assertEquals("TIMETZ(6)[]",
                     DatabaseTypes.logicalDeclaration("time(6) with time zone[]", "postgresql"));
        assertEquals("ORACLE_DATE", DatabaseTypes.logicalDeclaration("date", "oracle"));
        assertEquals("ROWVERSION", DatabaseTypes.logicalDeclaration("timestamp", "sqlserver"));
    }

    @Test
    void routesExplicitOracleLongRawThroughTheBinaryLobPath() {
        DatabaseType type = DatabaseType.of("LONG RAW");

        assertEquals("BLOB", DatabaseTypes.logicalDeclaration(type.declaration(), "oracle"));
        assertTrue(LargeObjectValueCodec.isBinaryDataType(type));
    }

    @Test
    void routesMySqlBinaryMetadataAliasesThroughTheBinaryReadPath() {
        for (String declaration : List.of("MYSQL_BINARY", "MYSQL_BLOB", "TINYBLOB", "MEDIUMBLOB")) {
            DatabaseType type = DatabaseType.of(declaration);

            assertTrue(type.isBinary(), declaration);
            assertTrue(LargeObjectValueCodec.isBinaryDataType(type), declaration);
        }
    }

    @Test
    void preservesUnknownVendorTypesByTheirCompleteCanonicalName() {
        assertEquals("GEOGRAPHY_POINT",
                     DatabaseTypes.logicalDeclaration("geography_point", "sqlserver"));
    }

    @Test
    void preservesOracleIntervalMetadataAndDriverTypes() {
        assertEquals("INTERVAL DAY TO SECOND",
                     DatabaseTypes.logicalDeclaration("interval day to second", "oracle"));
        assertEquals("INTERVAL YEAR TO MONTH",
                     DatabaseTypes.logicalDeclaration("interval year to month", "oracle"));
        assertEquals(Duration.class,
                     DatabaseTypes.parameterType(DatabaseType.of("INTERVAL DAY TO SECOND"), "oracle", false));
        assertEquals(Period.class,
                     DatabaseTypes.parameterType(DatabaseType.of("INTERVAL YEAR TO MONTH"), "oracle", false));
        assertEquals("INTERVAL DAY(2) TO SECOND(3)",
                     DatabaseTypes.logicalDeclaration("interval day(2) to second(3)", "oracle"));
        assertEquals("INTERVAL YEAR(2) TO MONTH",
                     DatabaseTypes.logicalDeclaration("interval year(2) to month", "oracle"));
        assertEquals(Duration.class, DatabaseTypes.parameterType(
                DatabaseType.of("INTERVAL DAY(2) TO SECOND(3)"), "oracle", false));
        assertEquals(Period.class, DatabaseTypes.parameterType(
                DatabaseType.of("INTERVAL YEAR(2) TO MONTH"), "oracle", false));
    }
}
