package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.type.DatabaseType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class SchemaIntegerLiteralRegressionTest {

    private static final InformationSchemaFormMetadataReader.SnapshotDialect MYSQL =
            InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL;

    @Test
    void mediumIntDefaultRetainsItsFullValue() {
        assertDefault("MEDIUMINT", "40000", 40000);
    }

    @Test
    void unsignedSmallIntDefaultRetainsItsFullValue() {
        assertDefault("SMALLINT UNSIGNED", "40000", 40000);
    }

    @Test
    void unsignedIntDefaultRetainsItsFullValue() {
        assertDefault("INT UNSIGNED", "2147483648", 2147483648L);
    }

    @Test
    void unsignedBigIntDefaultRetainsItsFullValue() {
        assertDefault("BIGINT UNSIGNED", "9223372036854775808",
                new BigInteger("9223372036854775808"));
    }

    @Test
    void mediumIntCheckRetainsItsFullValue() {
        assertCheck("MEDIUMINT", "40000", 40000);
    }

    @Test
    void unsignedSmallIntCheckRetainsItsFullValue() {
        assertCheck("SMALLINT UNSIGNED", "40000", 40000);
    }

    @Test
    void unsignedIntCheckRetainsItsFullValue() {
        assertCheck("INT UNSIGNED", "2147483648", 2147483648L);
    }

    @Test
    void unsignedBigIntCheckRetainsItsFullValue() {
        assertCheck("BIGINT UNSIGNED", "9223372036854775808",
                new BigInteger("9223372036854775808"));
    }

    @Test
    void ordinarySignedDefaultCarriersRemainUnchanged() {
        assertDefault("SMALLINT", "12", (short) 12);
        assertDefault("INT", "12", 12);
        assertDefault("BIGINT", "12", 12L);
    }

    @Test
    void ordinaryDecimalBooleanAndMissingDefaultsRemainUnchanged() {
        assertDefault("DECIMAL(10,2)", "1.25", new BigDecimal("1.25"));
        assertDefault("BOOLEAN", "1", true);
        assertEquals(ColumnDefault.none(), RelationalMetadataValueParser.columnDefault(
                null, DatabaseType.of("INT"), MYSQL));
    }

    private static void assertDefault(String type, String literal, Object expected) {
        ColumnDefault actual = RelationalMetadataValueParser.columnDefault(
                literal, DatabaseType.of(type), MYSQL);
        assertEquals(expected, actual.value().orElseThrow());
    }

    private static void assertCheck(String type, String literal, Object expected) {
        CheckPredicate parsed = RelationalMetadataValueParser.checkPredicate(
                "n >= " + literal, Map.of("n", DatabaseType.of(type)), MYSQL);
        CheckPredicate.Comparison comparison = assertInstanceOf(CheckPredicate.Comparison.class, parsed);
        assertEquals("n", comparison.column());
        assertEquals(CheckPredicate.ComparisonOperator.GREATER_THAN_OR_EQUAL, comparison.operator());
        assertEquals(expected, comparison.value());
    }
}
