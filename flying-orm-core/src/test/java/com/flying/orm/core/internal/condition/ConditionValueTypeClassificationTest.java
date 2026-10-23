package com.flying.orm.core.internal.condition;

import com.flying.orm.core.condition.ConditionValueException;
import com.flying.orm.core.condition.ConditionValueShape;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConditionValueTypeClassificationTest {

    @Test
    void rangeBindingDoesNotImposeJavaOrderingOrComparableTypes() {
        for (List<?> endpoints : List.of(List.of("a", "Z"), List.of(10, 2L),
                List.of(new byte[]{2}, new byte[]{1}))) {
            List<?> values = assertInstanceOf(List.class,
                    normalize(ConditionValueShape.RANGE, endpoints, (value, index) -> value).value());
            assertEquals(2, values.size());
            assertSame(endpoints.get(0), values.get(0));
            assertSame(endpoints.get(1), values.get(1));
        }
    }

    @Test
    void publishingAnOwnedNormalizedCollectionAvoidsExtraFullSizeArrays() {
        java.lang.management.ThreadMXBean standardBean = java.lang.management.ManagementFactory.getThreadMXBean();
        assertTrue(standardBean instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocationBean = (com.sun.management.ThreadMXBean) standardBean;
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) allocationBean.setThreadAllocatedMemoryEnabled(true);
        List<Integer> input = java.util.stream.IntStream.range(0, 1000).boxed().toList();
        int iterations = 100;
        for (int index = 0; index < iterations; index++) {
            normalize(ConditionValueShape.COLLECTION, input, (value, position) -> value);
        }
        ConditionValueNormalizer.Result[] results = new ConditionValueNormalizer.Result[iterations];
        long threadId = Thread.currentThread().threadId();
        long before = allocationBean.getThreadAllocatedBytes(threadId);
        for (int index = 0; index < iterations; index++) {
            results[index] = normalize(ConditionValueShape.COLLECTION, input, (value, position) -> value);
        }
        long bytesPerCall = (allocationBean.getThreadAllocatedBytes(threadId) - before) / iterations;
        // Diagnostic only: growing and repeatedly copying a contiguous buffer exceeds this budget.
        assertTrue(bytesPerCall < 10_000, () -> "normalized collection allocated " + bytesPerCall + " bytes");
        assertEquals(input, results[iterations - 1].value());
        List<?> values = assertInstanceOf(List.class, results[iterations - 1].value());
        assertThrows(UnsupportedOperationException.class, values::clear);
    }

    @Test
    void repeatedInputTypesStillConvertEveryValueAndValidateMutableTextAfterConversion() {
        List<Integer> indexes = new java.util.ArrayList<>();
        ConditionValueNormalizer.Result result = ConditionValueNormalizer.normalize(
                ConditionValueShape.COLLECTION, java.util.Arrays.asList(1, 2, null, 3L, 4L, 5),
                ConditionValuePolicy.IGNORE_EMPTY,
                (value, index) -> { indexes.add(index); return " value-" + index + " "; },
                Integer.MAX_VALUE, Integer.MAX_VALUE);
        assertEquals(List.of(" value-0 ", " value-1 ", " value-3 ", " value-4 ", " value-5 "), result.value());
        assertEquals(List.of(0, 1, 3, 4, 5), indexes);

        ConditionValueException error = assertThrows(ConditionValueException.class,
                () -> normalize(ConditionValueShape.COLLECTION,
                        List.of(new StringBuilder("first"), new StringBuilder("second")),
                        (value, index) -> {
                            if (index == 1) ((StringBuilder) value).setLength(0);
                            return value;
                        }));
        assertEquals(ConditionValueException.Error.BLANK_VALUE, error.error());
    }

    @Test
    void preservesEmptyResultsFromConvertersAndTheirOriginalIndexes() {
        List<Integer> indexes = new java.util.ArrayList<>();
        ConditionValueNormalizer.Result result = ConditionValueNormalizer.normalize(
                ConditionValueShape.COLLECTION, List.of(1, 2, 3), ConditionValuePolicy.IGNORE_EMPTY,
                (value, index) -> {
                    indexes.add(index);
                    return index == 0 ? null : index == 1 ? "  " : value;
                }, Integer.MAX_VALUE, Integer.MAX_VALUE);
        assertEquals(List.of(3), result.value());
        assertEquals(List.of(0, 1, 2), indexes);
        for (ConditionValueShape shape : List.of(ConditionValueShape.SCALAR,
                ConditionValueShape.COLLECTION, ConditionValueShape.RANGE)) {
            Object input = shape == ConditionValueShape.SCALAR ? 1 : List.of(1, 2);
            ConditionValueException nullError = assertThrows(ConditionValueException.class,
                    () -> normalize(shape, input, (value, index) -> null));
            assertEquals(ConditionValueException.Error.NULL_VALUE, nullError.error());
            ConditionValueException blankError = assertThrows(ConditionValueException.class,
                    () -> normalize(shape, input, (value, index) -> "  "));
            assertEquals(ConditionValueException.Error.BLANK_VALUE, blankError.error());
        }
    }

    @Test
    void acceptsErrorEnumsAsOrdinaryApplicationValues() {
        Object value = ConditionValueException.Error.NULL_VALUE;
        assertSame(value, normalize(ConditionValueShape.SCALAR, value, (scalar, index) -> scalar).value());
        assertEquals(List.of(value), normalize(ConditionValueShape.COLLECTION,
                List.of(value), (scalar, index) -> scalar).value());
    }

    @Test
    void preservesMixedScalarValuesAndConverterIndexes() {
        List<Object> input = new java.util.ArrayList<>(List.of(17, 29L, " text ", new StringBuilder(" domain "), true));
        List<Integer> indexes = new java.util.ArrayList<>();
        ConditionValueNormalizer.Result result = normalize(ConditionValueShape.COLLECTION, input,
                (value, index) -> { indexes.add(index); return value; });
        input.clear();

        List<?> values = assertInstanceOf(List.class, result.value());
        assertEquals(List.of(17, 29L, " text "), values.subList(0, 3));
        assertEquals(" domain ", assertInstanceOf(StringBuilder.class, values.get(3)).toString());
        assertEquals(true, values.get(4));
        assertEquals(List.of(0, 1, 2, 3, 4), indexes);
        assertThrows(UnsupportedOperationException.class, values::clear);
    }

    @Test
    void classifiesActualSubclassesInsteadOfAssumingEveryNumberIsScalar() {
        ConditionValueException error = assertThrows(ConditionValueException.class,
                () -> normalize(ConditionValueShape.SCALAR, new IterableNumber(), (value, index) -> value));
        assertEquals(ConditionValueException.Error.SHAPE_NOT_ALLOWED, error.error());
    }

    @Test
    void validatesTheConvertedTypeRatherThanReusingInputClassification() {
        for (Object converted : List.of(List.of(1), Map.of("id", 1), new Object[]{1})) {
            ConditionValueException error = assertThrows(ConditionValueException.class,
                    () -> normalize(ConditionValueShape.COLLECTION, List.of(1), (value, index) -> converted));
            assertEquals(ConditionValueException.Error.SHAPE_NOT_ALLOWED, error.error());
        }
        assertEquals(" converted ", normalize(ConditionValueShape.SCALAR, 1,
                (value, index) -> " converted ").value());
    }

    @Test
    void preservesArrayRulesForScalarAndCollectionPositions() {
        Object[] objects = {1};
        byte[] binary = {2};
        assertSame(objects, normalize(ConditionValueShape.SCALAR, objects, (value, index) -> value).value());
        List<?> result = assertInstanceOf(List.class, normalize(ConditionValueShape.COLLECTION,
                List.of(binary), (value, index) -> value).value());
        assertSame(binary, result.getFirst());
        assertThrows(ConditionValueException.class, () -> normalize(ConditionValueShape.COLLECTION,
                java.util.Collections.singletonList(objects), (value, index) -> value));
    }

    private static ConditionValueNormalizer.Result normalize(ConditionValueShape shape, Object value,
            ConditionValueNormalizer.ScalarConverter converter) {
        return ConditionValueNormalizer.normalize(shape, value, ConditionValuePolicy.REJECT_EMPTY,
                converter, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    private static final class IterableNumber extends Number implements Iterable<Integer> {
        @Override public int intValue() { return 1; }
        @Override public long longValue() { return 1; }
        @Override public float floatValue() { return 1; }
        @Override public double doubleValue() { return 1; }
        @Override public Iterator<Integer> iterator() { return List.of(1).iterator(); }
    }
}
