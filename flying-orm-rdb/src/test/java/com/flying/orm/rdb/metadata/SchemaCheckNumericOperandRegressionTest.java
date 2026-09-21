package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.type.DatabaseType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaCheckNumericOperandRegressionTest {

    @Test
    void integerColumnCanBeComparedWithFractionalOperand() {
        assertOperand("n >= 1.5", new BigDecimal("1.5"));
    }

    @Test
    void integerColumnCanBeComparedWithLargerExactOperand() {
        assertOperand("n >= 2147483648", new BigDecimal("2147483648"));
    }

    @Test
    void existingExponentSyntaxRetainsExactIntegralValue() {
        assertOperand("n >= 1e2", 100);
    }

    @Test
    void arbitraryFunctionIsStillRejected() {
        assertThrows(IllegalStateException.class, () -> parse("n >= arbitrary_function()"));
    }

    private static void assertOperand(String expression, Object expected) {
        CheckPredicate.Comparison comparison = assertInstanceOf(
                CheckPredicate.Comparison.class, parse(expression));
        assertEquals(expected, comparison.value());
    }

    private static CheckPredicate parse(String expression) {
        return RelationalMetadataValueParser.checkPredicate(expression,
                Map.of("n", DatabaseType.of("INTEGER")),
                InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL);
    }
}
