package com.flying.orm.core.type;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseTypeTest {

    @Test
    void classifiesOnlyCompleteKnownTypeNames() {
        DatabaseType text = DatabaseType.of("VARCHAR(255)");
        DatabaseType custom = DatabaseType.of("CONTEXT_ID");

        assertEquals(LogicalType.TEXT, text.logicalType());
        assertTrue(text.isTextual());
        assertEquals(LogicalType.OTHER, custom.logicalType());
        assertFalse(custom.isTextual());
    }

    @Test
    void parsesArgumentsTimeZoneAndArrayShapeOnce() {
        DatabaseType type = DatabaseType.of(" timestamp ( 6 ) with time zone [ ] [ ] ");

        assertEquals("TIMESTAMP WITH TIME ZONE", type.baseName());
        assertEquals(List.of("6"), type.arguments());
        assertEquals(2, type.arrayDimensions());
        assertEquals(LogicalType.OFFSET_TIMESTAMP, type.logicalType());
        assertTrue(type.isArray());
    }

    @Test
    void keepsMySqlNumericModifiersAsStructuredProperties() {
        DatabaseType type = DatabaseType.of("BIGINT(20) ZEROFILL");

        assertEquals(LogicalType.BIG_INTEGER, type.logicalType());
        assertTrue(type.unsigned());
        assertTrue(type.zerofill());
        assertEquals(List.of("20"), type.arguments());
    }

    @Test
    void recognizesOracleLongRawAsASafeBinaryType() {
        DatabaseType type = DatabaseType.of("LONG RAW");

        assertTrue(type.safeDeclaration());
        assertEquals("LONG RAW", type.baseName());
        assertEquals(LogicalType.BINARY, type.logicalType());
        assertTrue(type.isBinary());
    }

    @Test
    void malformedSuffixCannotBorrowTheMeaningOfAKnownPrefix() {
        DatabaseType type = DatabaseType.of("TIMESTAMPTZ(6) INVALID");

        assertFalse(type.safeDeclaration());
        assertEquals(LogicalType.OTHER, type.logicalType());
    }

    @Test
    void recognizesOnlyExactLargeObjectTypeNames() {
        DatabaseType binary = DatabaseType.of("BINARY LARGE OBJECT");
        DatabaseType text = DatabaseType.of("CHARACTER LARGE OBJECT");
        assertTrue(binary.safeDeclaration());
        assertTrue(text.safeDeclaration());
        assertEquals(LogicalType.BINARY, binary.logicalType());
        assertEquals(LogicalType.TEXT, text.logicalType());
        assertFalse(DatabaseType.of("BINARY LARGE OBJECT INVALID").safeDeclaration());
        assertFalse(DatabaseType.of("CHARACTER LARGE OBJECT; DROP TABLE t").safeDeclaration());
    }

    @Test
    void equalityUsesCanonicalTypeSyntax() {
        assertEquals(DatabaseType.of("VARCHAR ( 64 )"), DatabaseType.of(" varchar(64) "));
    }

    @Test
    void acceptsPostgreSqlIntervalFieldsAndFractionalSecondPrecision() {
        for (String declaration : List.of(
                "INTERVAL", "INTERVAL(3)", "INTERVAL YEAR", "INTERVAL YEAR TO MONTH",
                "INTERVAL YEAR(2) TO MONTH",
                "INTERVAL MONTH", "INTERVAL DAY", "INTERVAL DAY TO HOUR",
                "INTERVAL DAY TO MINUTE", "INTERVAL DAY TO SECOND(3)",
                "INTERVAL DAY(2) TO SECOND", "INTERVAL DAY(2) TO SECOND(3)",
                "INTERVAL HOUR", "INTERVAL HOUR TO MINUTE", "INTERVAL HOUR TO SECOND(4)",
                "INTERVAL MINUTE", "INTERVAL MINUTE TO SECOND(5)", "INTERVAL SECOND(6)[]",
                "pg_catalog.interval day to second(3)[]")) {
            DatabaseType type = DatabaseType.of(declaration);

            assertTrue(type.safeDeclaration(), declaration);
            assertEquals(LogicalType.INTERVAL, type.logicalType(), declaration);
        }
        assertFalse(DatabaseType.of("INTERVAL MONTH TO YEAR").safeDeclaration());
        assertFalse(DatabaseType.of("INTERVAL SECOND TO MINUTE").safeDeclaration());
        assertFalse(DatabaseType.of("INTERVAL YEAR(2) TO DAY").safeDeclaration());
        assertFalse(DatabaseType.of("INTERVAL DAY(2) TO MONTH").safeDeclaration());
    }
}
