package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.CheckPredicate;
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
