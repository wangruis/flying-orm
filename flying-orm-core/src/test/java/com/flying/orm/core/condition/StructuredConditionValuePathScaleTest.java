package com.flying.orm.core.condition;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StructuredConditionValuePathScaleTest {

    @Test
    void deepValueValidationDoesNotRetainAFullPathAtEveryLevel() {
        int depth = 12_000;
        Object value = 7;
        for (int level = 0; level < depth; level++) {
            value = List.of(value);
        }
        StructuredConditionInput input = StructuredConditionInput.term("payload", "raw", value);
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults()
                .allowOperator("raw").withMaxDepth(depth).withMaxNodes(depth);

        assertDoesNotThrow(() -> StructuredConditionCompiler.validateStructure(input, policy));
    }

    @Test
    void mapKeyFailureRetainsItsValuePath() {
        StructuredConditionInput input = StructuredConditionInput.term("p", "x",
                List.of(Map.of("long", 7)));
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults()
                .allowOperator("x").withMaxStringLength(2);

        StructuredConditionException error = assertThrows(StructuredConditionException.class,
                () -> StructuredConditionCompiler.validateStructure(input, policy));

        assertEquals(StructuredConditionErrorCode.VALUE_TOO_LONG, error.code());
        assertEquals("conditions.value[0][0].key", error.path());
    }
}
