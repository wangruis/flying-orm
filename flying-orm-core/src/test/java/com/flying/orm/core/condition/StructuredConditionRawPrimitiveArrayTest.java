package com.flying.orm.core.condition;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Raw-value structure contracts shared by the baseline and the primitive-array fast path. */
class StructuredConditionRawPrimitiveArrayTest {

    private static final int MAX_RAW_REFERENCES = 20_000;

    @Test
    void acceptsEveryPrimitiveArrayKindAtTheRawReferenceLimit() {
        Object[] primitiveArrays = {
                new boolean[MAX_RAW_REFERENCES],
                new byte[MAX_RAW_REFERENCES],
                new char[MAX_RAW_REFERENCES],
                new short[MAX_RAW_REFERENCES],
                new int[MAX_RAW_REFERENCES],
                new long[MAX_RAW_REFERENCES],
                new float[MAX_RAW_REFERENCES],
                new double[MAX_RAW_REFERENCES]
        };

        for (Object primitiveArray : primitiveArrays) {
            assertDoesNotThrow(() -> validateRaw(primitiveArray), primitiveArray.getClass().getTypeName());
        }
    }

    @Test
    void sharedPrimitiveArraysChargeEachOccurrenceAndKeepReferenceFailureOrdering() {
        int[] sharedAtLimit = new int[9_999];
        assertDoesNotThrow(() -> validateRaw(new Object[] {sharedAtLimit, sharedAtLimit}));

        int[] sharedAboveLimit = new int[9_999];
        StructuredConditionException error = assertThrows(
                StructuredConditionException.class,
                () -> validateRaw(new Object[] {sharedAboveLimit, sharedAboveLimit, "too-long"},
                                  rawPolicy().withMaxStringLength(3)));

        assertEquals(StructuredConditionErrorCode.NODE_COUNT_EXCEEDED, error.code());
        assertEquals("conditions.value[1]", error.path());
        assertEquals("structured condition value reference count exceeds limit at conditions.value[1]",
                     error.getMessage());
    }

    @Test
    void primitiveArrayContainerDepthLimitPrecedesItsReferenceCharge() {
        int[] shared = new int[9_999];
        StructuredConditionException error = assertThrows(
                StructuredConditionException.class,
                () -> validateRaw(new Object[] {shared, new Object[] {shared}}, rawPolicy().withMaxDepth(2)));

        assertEquals(StructuredConditionErrorCode.DEPTH_EXCEEDED, error.code());
        assertEquals("conditions.value[1][0]", error.path());
    }

    @Test
    void primitiveArrayContainerNodeLimitPrecedesItsReferenceCharge() {
        int[] shared = new int[9_999];
        StructuredConditionException error = assertThrows(
                StructuredConditionException.class,
                () -> validateRaw(new Object[] {shared, shared, 0}, rawPolicy().withMaxNodes(2)));

        assertEquals(StructuredConditionErrorCode.NODE_COUNT_EXCEEDED, error.code());
        assertEquals("conditions.value[1]", error.path());
        assertEquals("structured condition value node count exceeds limit at conditions.value[1]",
                     error.getMessage());
    }

    @Test
    void primitiveArrayCollectionLimitPrecedesItsReferenceCharge() {
        Object[] values = new Object[22];
        Arrays.fill(values, new int[950]);
        values[21] = new int[1_001];
        StructuredConditionException error = assertThrows(
                StructuredConditionException.class,
                () -> StructuredConditionCompiler.validateStructure(
                        StructuredConditionInput.term("payload", "json-contains", values),
                        StructuredConditionPolicy.defaults().withMaxCollectionSize(1_000)));

        assertEquals(StructuredConditionErrorCode.VALUE_COLLECTION_TOO_LARGE, error.code());
        assertEquals("conditions.value[21]", error.path());
    }

    @Test
    void standardScalarArraysKeepTheirRawTraversalBypass() {
        assertDoesNotThrow(() -> StructuredConditionCompiler.validateStructure(
                StructuredConditionInput.term("payload", "eq", new int[2]),
                StructuredConditionPolicy.defaults().withMaxCollectionSize(1)));
    }

    @Test
    void referenceContainersKeepTheirNestedStringValidation() {
        StructuredConditionPolicy policy = rawPolicy().withMaxStringLength(3);

        assertValueTooLong(new Object[] {new Object[] {"long"}}, policy, "conditions.value[0][0]");
        assertValueTooLong(List.of("long"), policy, "conditions.value[0]");
        assertValueTooLong(Map.of("key", "long"), policy, "conditions.value[0]");
    }

    private static void validateRaw(Object value) {
        validateRaw(value, rawPolicy());
    }

    private static void validateRaw(Object value, StructuredConditionPolicy policy) {
        StructuredConditionCompiler.validateStructure(StructuredConditionInput.term("v", "raw", value), policy);
    }

    private static StructuredConditionPolicy rawPolicy() {
        return StructuredConditionPolicy.defaults().allowOperator("raw");
    }

    private static void assertValueTooLong(Object value, StructuredConditionPolicy policy, String path) {
        StructuredConditionException error = assertThrows(
                StructuredConditionException.class,
                () -> validateRaw(value, policy));

        assertEquals(StructuredConditionErrorCode.VALUE_TOO_LONG, error.code());
        assertEquals(path, error.path());
    }
}
