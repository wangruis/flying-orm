package com.flying.orm.core.condition;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.internal.condition.ConditionExecutionViews;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TermConditionDirectArrayExecutionTest {

    @Test
    void directAstStandardMultiValueArraysPublishFlatParameters() {
        assertAll(
                () -> assertParameters("in", new int[]{1, 2}, List.of(1, 2)),
                () -> assertParameters("not-in", new String[]{"A", "B"}, List.of("A", "B")),
                () -> assertParameters("between", new long[]{3L, 4L}, List.of(3L, 4L)),
                () -> assertParameters("not-between", new Integer[]{5, 6}, List.of(5, 6)));
    }

    private static void assertParameters(String operator, Object value, List<?> expected) {
        ConditionGroup group = ConditionGroup.and()
                                             .add(TermCondition.of("sequence", operator, value))
                                             .build();

        assertEquals(expected, ConditionExecutionViews.bindParameters(group, ValueCodecRegistry.standard()));
    }
}
