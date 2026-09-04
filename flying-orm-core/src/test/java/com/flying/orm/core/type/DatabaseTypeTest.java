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
    void equalityUsesCanonicalTypeSyntax() {
        assertEquals(DatabaseType.of("VARCHAR ( 64 )"), DatabaseType.of(" varchar(64) "));
    }
}
