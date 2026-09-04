package com.flying.orm.rdb.type;

import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.codec.LargeObjectValueCodec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.time.Duration;
import java.time.Period;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseTypesTest {

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
    }
}
