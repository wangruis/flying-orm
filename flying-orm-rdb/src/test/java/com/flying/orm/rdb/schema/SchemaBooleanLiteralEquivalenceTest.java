package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaBooleanLiteralEquivalenceTest {

    @Test
    void mysqlBooleanDefaultsMatchObservedTinyIntLiterals() {
        assertTrue(defaultEquivalent(RdbDialect.mysql(), "TINYINT(1)", (short) 1, true));
        assertTrue(defaultEquivalent(RdbDialect.mysql(), "TINYINT(1)", (short) 0, false));
        assertFalse(defaultEquivalent(RdbDialect.mysql(), "TINYINT(1)", (short) 0, true));
        assertFalse(defaultEquivalent(RdbDialect.mysql(), "TINYINT(1)", (short) 2, true));
    }

    @Test
    void oracleBooleanDefaultsMatchObservedNumberLiterals() {
        assertTrue(defaultEquivalent(RdbDialect.oracle(), "NUMBER(1)", BigDecimal.ONE, true));
        assertTrue(defaultEquivalent(RdbDialect.oracle(), "NUMBER(1)", BigDecimal.ZERO, false));
        assertFalse(defaultEquivalent(RdbDialect.oracle(), "NUMBER(1)", BigDecimal.ZERO, true));
    }

    @Test
    void numericBooleanCheckReadbackMatchesOnlyExactZeroAndOne() {
        for (RdbDialect dialect : List.of(RdbDialect.mysql(), RdbDialect.oracle(), RdbDialect.sqlServer())) {
            assertTrue(checkEquivalent(dialect, BigDecimal.ONE, true), dialect.name());
            assertTrue(checkEquivalent(dialect, BigDecimal.ZERO, false), dialect.name());
            assertFalse(checkEquivalent(dialect, BigDecimal.ZERO, true), dialect.name());
            assertFalse(checkEquivalent(dialect, new BigDecimal("1.5"), true), dialect.name());
        }
        assertFalse(checkEquivalent(RdbDialect.postgresql(), 1, true));
        assertFalse(checkEquivalent(RdbDialect.h2(), 1, true));
    }

    private static boolean checkEquivalent(RdbDialect dialect, Object observed, Object desired) {
        return SchemaDefinitionEquality.sameCheck(
                CheckConstraintDefinition.of("ck_enabled", CheckPredicate.compare("enabled",
                        CheckPredicate.ComparisonOperator.EQUAL, observed)),
                CheckConstraintDefinition.of("ck_enabled", CheckPredicate.compare("enabled",
                        CheckPredicate.ComparisonOperator.EQUAL, desired)), dialect.schema());
    }

    private static boolean defaultEquivalent(RdbDialect dialect, String physicalType,
                                              Object observedValue, boolean desiredValue) {
        RelationIdentity identity = RelationIdentity.table("flags");
        RelationalTableDefinition actual = RelationalTableDefinition.builder(identity)
                .addColumn(ColumnDefinition.builder("enabled", physicalType)
                        .defaultValue(ColumnDefault.literal(observedValue)).build()).build();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(identity)
                .addColumn(ColumnDefinition.builder("enabled", "BOOLEAN")
                        .defaultValue(ColumnDefault.literal(desiredValue)).build()).build();
        return SchemaDiffer.diff(desired, SchemaSnapshot.present(actual), dialect.capabilities(),
                SchemaCompatibilityMode.EXACT, dialect.name(), dialect.schema()).exact();
    }
}
