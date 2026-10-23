package com.flying.orm.core.type;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    void preservesUnsupportedQuotedDeclarationsWithoutAcceptingThemAsSql() {
        for (String declaration : List.of("custom('Active')", "\"CaseSensitive\"", "`CaseSensitive`")) {
            DatabaseType type = DatabaseType.of(declaration);
            assertEquals(declaration, type.declaration());
            assertEquals(declaration, type.canonical());
            assertEquals(declaration, type.comparisonShape());
            assertFalse(type.safeDeclaration());
            assertThrows(IllegalArgumentException.class, () -> type.requireSafe("column type"));
        }
        assertNotEquals(DatabaseType.of("enum('Active')"), DatabaseType.of("enum('active')"));
        assertNotEquals(DatabaseType.of("set('A  B')"), DatabaseType.of("set('A B')"));
        assertNotEquals(DatabaseType.of("enum('A , B')"), DatabaseType.of("enum('A,B')"));
    }

    @Test
    void parsesLiteralMembersWithoutNormalizingTheirContentsOrTreatingThemAsSizes() {
        DatabaseType type = DatabaseType.of(" EnUm ( 'Active', 'It''s (Active)', 'A , B', '' ) ");
        assertTrue(type.safeDeclaration());
        assertEquals("ENUM", type.baseName());
        assertEquals(List.of("'Active'", "'It''s (Active)'", "'A , B'", "''"), type.arguments());
        assertEquals("enum('Active','It''s (Active)','A , B','')", type.canonical());
        assertEquals(type.canonical(), type.comparisonShape());
        assertTrue(type.isTextual());
        assertEquals(DatabaseType.of("SET ( 'Read', 'Write' )"), DatabaseType.of("set('Read','Write')"));
        assertNotEquals(DatabaseType.of("enum('A','B')"), DatabaseType.of("enum('B','A')"));
        assertTrue(DatabaseType.of("enum('); drop table users; --','/*comment*/','?')").safeDeclaration());
    }

    @Test
    void acceptsModeIndependentHexMembersAndRejectsEscapingOrTrailingSql() {
        assertTrue(DatabaseType.of("set(X'415c42', x'00', '')").safeDeclaration());
        assertFalse(DatabaseType.of("enum(X'123')").safeDeclaration());
        for (String declaration : List.of("enum()", "enum(1)", "set('a',)", "enum('a')[]",
                "enum('a') unsigned", "enum('a'); drop table users", "enum('a') /* comment */",
                "enum('a' 'b')", "enum(\"a\")", "enum('a\\' , '); drop table users; --')",
                "enum('a\\b')", "enum('a\u0000b')", "enum('a', X'zz')")) {
            DatabaseType type = DatabaseType.of(declaration);
            assertFalse(type.safeDeclaration(), declaration);
            assertThrows(IllegalArgumentException.class, () -> type.requireSafe("column type"));
        }
    }

    @Test
    void literalParsingDoesNotImposeAMemberLimitOrRecurseThroughQuotedText() {
        String member = "'" + "a''".repeat(10_000) + "z'";
        DatabaseType quoted = DatabaseType.of("enum(" + member + ")");
        assertTrue(quoted.safeDeclaration());
        assertEquals(List.of(member), quoted.arguments());
        String members = java.util.stream.IntStream.range(0, 5_000)
                .mapToObj(index -> "'" + index + "'").collect(java.util.stream.Collectors.joining(","));
        assertEquals(5_000, DatabaseType.of("enum(" + members + ")").arguments().size());
    }

    @Test
    void recognizesBinaryVaryingWithoutAcceptingSqlSuffixes() {
        DatabaseType type = DatabaseType.of("BINARY VARYING(12)");
        assertTrue(type.safeDeclaration());
        assertEquals(LogicalType.BINARY, type.logicalType());
        assertEquals(List.of("12"), type.arguments());
        assertFalse(DatabaseType.of("BINARY VARYING(12); DROP TABLE t").safeDeclaration());
        assertFalse(DatabaseType.of("BINARY VARYING(12) INVALID").safeDeclaration());
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
