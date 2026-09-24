package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.type.DatabaseType;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PostgresqlCheckCastRegressionTest {

    @TestFactory
    Stream<DynamicTest> preservesSamePhysicalFloatingPointCasts() {
        return Stream.of("real", "float4", "double precision", "float8")
                .map(type -> DynamicTest.dynamicTest(type, () -> {
                    assertEquals(ColumnDefault.literal("0.1"), RelationalMetadataValueParser.columnDefault(
                            "'0.1'::" + type, DatabaseType.of(type),
                            InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL));
                    assertEquals(CheckPredicate.compare("val", CheckPredicate.ComparisonOperator.GREATER_THAN,
                            "0.1"), parse("val > '0.1'::" + type, type));
                }));
    }

    @Test
    void doesNotEraseCrossPrecisionFloatingPointRounding() {
        assertThrows(IllegalStateException.class, () -> parse("val > '0.1'::real", "double precision"));
        assertThrows(IllegalStateException.class, () -> parse("val > '0.1'::double precision", "numeric"));
        assertThrows(IllegalStateException.class, () -> parse("val > ((16777216::real)::numeric)", "numeric"));
    }

    @TestFactory
    Stream<DynamicTest> keepsFloatingPointCastChainsAcrossParentheses() {
        return Stream.of("'1.234567'::real::numeric", "(('1.234567'::real)::numeric)",
                        "'1.234567'::real::float8")
                .map(value -> DynamicTest.dynamicTest(value, () -> assertThrows(IllegalStateException.class,
                        () -> parse("val > " + value, "real"))));
    }

    @Test
    void preservesSamePrecisionAliases() {
        assertEquals(CheckPredicate.compare("val", CheckPredicate.ComparisonOperator.GREATER_THAN, "0.1"),
                parse("val > (('0.1'::real)::pg_catalog.float4)", "real"));
    }

    @TestFactory
    Stream<DynamicTest> doesNotEraseLossyNumericOrApplicationCasts() {
        return Stream.of("1.6::integer", "1.6::real", "1.6::app.numeric", "1.6::numeric::integer")
                .map(value -> DynamicTest.dynamicTest(value, () ->
                        assertThrows(IllegalStateException.class, () -> parse("val > " + value, "numeric"))));
    }

    @TestFactory
    Stream<DynamicTest> preservesExactNumericCasts() {
        return Stream.of("2::integer", "2::bigint", "2::numeric", "2::pg_catalog.int4")
                .map(value -> DynamicTest.dynamicTest(value, () -> assertEquals(
                        CheckPredicate.compare("val", CheckPredicate.ComparisonOperator.GREATER_THAN,
                                new java.math.BigDecimal("2")), parse("val > " + value, "numeric"))));
    }

    @Test
    void doesNotIgnoreFixedWidthToVariableWidthDefaultConversion() {
        assertThrows(IllegalStateException.class, () -> RelationalMetadataValueParser.columnDefault(
                "'a '::bpchar", DatabaseType.of("VARCHAR"),
                InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL));
        assertEquals(ColumnDefault.literal("a "), RelationalMetadataValueParser.columnDefault(
                "'a '::text", DatabaseType.of("VARCHAR"),
                InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL));
    }

    @TestFactory
    Stream<DynamicTest> reconstructsTextColumnComparisons() {
        return Stream.of(
                "((val)::text <> 'y'::text)",
                "(val::text <> 'y'::text)",
                "(((val))::pg_catalog.text <> ('y'::character varying)::text)",
                "((val::varchar)::text <> 'y'::varchar)")
                .map(sql -> DynamicTest.dynamicTest(sql, () -> assertEquals(
                        CheckPredicate.compare("val", CheckPredicate.ComparisonOperator.NOT_EQUAL, "y"),
                        parse(sql, "varchar(12)"))));
    }

    @TestFactory
    Stream<DynamicTest> reconstructsArrayAndElementCasts() {
        return Stream.of(
                "((val)::text = ANY ((ARRAY['a'::character varying, 'b'::character varying])::text[]))",
                "((val)::text = ANY (ARRAY[('a'::character varying)::text, ('b'::character varying)::text]))",
                "(val = ANY (ARRAY['a'::text, 'b'::text]))",
                "((val)::pg_catalog.text = ANY (((ARRAY['a', 'b'])::pg_catalog.text[])))")
                .map(sql -> DynamicTest.dynamicTest(sql, () ->
                        assertEquals(CheckPredicate.in("val", List.of("a", "b")), parse(sql, "text"))));
    }

    @Test
    void preservesPredicateGroupingAndNullChecks() {
        assertEquals(CheckPredicate.and(
                CheckPredicate.compare("val", CheckPredicate.ComparisonOperator.NOT_EQUAL, "y"),
                CheckPredicate.or(
                        CheckPredicate.compare("val", CheckPredicate.ComparisonOperator.EQUAL, "a"),
                        CheckPredicate.isNull("val"))),
                parse("((val)::text <> 'y'::text) AND "
                        + "(((val)::text = 'a'::text) OR (val IS NULL))", "varchar"));
    }

    @TestFactory
    Stream<DynamicTest> doesNotEraseValueChangingOrUnknownCasts() {
        return Stream.of(
                "(val::varchar(1) = 'a'::text)",
                "(val::bpchar = 'a'::bpchar)",
                "(val::app.custom_text = 'a'::text)",
                "(val::text = ANY ((ARRAY['a'])::char[]))",
                "(val::text = ANY (ARRAY['ab'::varchar(1)]))",
                "(val::text = ANY (ARRAY['a'::app.custom_text]))",
                "(lower(val) = 'a'::text)",
                "(val::text COLLATE custom_collation = 'a'::text)")
                .map(sql -> DynamicTest.dynamicTest(sql, () ->
                        assertThrows(IllegalStateException.class, () -> parse(sql, "varchar"))));
    }

    @Test
    void fixedWidthCharacterCastsAreNotTextEquivalence() {
        assertThrows(IllegalStateException.class, () -> parse("(val::text = 'a '::text)", "char(8)"));
        assertEquals(CheckPredicate.compare("val", CheckPredicate.ComparisonOperator.EQUAL, "a"),
                     parse("(val = 'a'::bpchar)", "char(8)"));
    }

    private static CheckPredicate parse(String sql, String type) {
        return RelationalMetadataValueParser.checkPredicate(sql, Map.of("val", DatabaseType.of(type)),
                InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL);
    }
}
