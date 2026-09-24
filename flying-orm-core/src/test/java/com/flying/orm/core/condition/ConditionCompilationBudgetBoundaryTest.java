package com.flying.orm.core.condition;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConditionCompilationBudgetBoundaryTest {

    @Test
    void rejectsTheNextNodeWhenConfiguredMaximumIntegerBudgetIsExhausted() throws ReflectiveOperationException {
        ConditionCompilationBudget budget = new ConditionCompilationBudget();
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults().withMaxNodes(Integer.MAX_VALUE);
        Field nodes = ConditionCompilationBudget.class.getDeclaredField("nodes");
        nodes.setAccessible(true);
        nodes.setInt(budget, Integer.MAX_VALUE - 1);

        assertDoesNotThrow(() -> budget.checkNode(ConditionCompilationBudget.Path.ROOT, policy));
        assertEquals(Integer.MAX_VALUE, nodes.getInt(budget));
        StructuredConditionException failure = assertThrows(StructuredConditionException.class,
                () -> budget.checkNode(ConditionCompilationBudget.Path.ROOT.child(2), policy));
        assertEquals(StructuredConditionErrorCode.NODE_COUNT_EXCEEDED, failure.code());
        assertEquals("conditions[2]", failure.path());
    }
}
