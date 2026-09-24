package com.flying.orm.core.condition;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StructuredConditionDeepTreeTest {

    private static final int DEPTH = 10_000;

    @Test
    void structureValidationHonorsDeveloperConfiguredDeepTreeBudget() {
        StructuredConditionInput input = deepInput();
        StructuredConditionPolicy policy = deepPolicy();

        assertDoesNotThrow(() -> StructuredConditionCompiler.validateStructure(input, policy));
    }

    @Test
    void compilationHonorsDeveloperConfiguredDeepTreeBudget() {
        DynamicForm form = DynamicForm.builder("samples", "samples")
                .addField(DynamicField.of("id", "INTEGER")).build();
        StructuredConditionInput input = deepInput();
        StructuredConditionPolicy policy = deepPolicy();

        ConditionNode node = assertDoesNotThrow(
                () -> StructuredConditionCompiler.create().compile(form, input, policy));
        for (int depth = 1; depth < DEPTH; depth++) {
            ConditionGroup group = assertInstanceOf(ConditionGroup.class, node);
            assertEquals(LogicalOperator.AND, group.operator());
            assertEquals(1, group.children().size());
            node = group.children().getFirst();
        }
        TermCondition term = assertInstanceOf(TermCondition.class, node);
        assertEquals("id", term.field());
        assertEquals("=", term.operator());
        assertEquals(7, term.value());
    }

    @Test
    void deepBudgetFailuresRetainTheirExactInputPath() {
        DynamicForm form = DynamicForm.builder("samples", "samples")
                .addField(DynamicField.of("id", "INTEGER")).build();
        StructuredConditionInput input = deepInput();
        String expectedPath = "conditions[0]" + ".conditions[0]".repeat(DEPTH - 2);
        for (boolean depthLimit : new boolean[]{true, false}) {
            StructuredConditionPolicy policy = depthLimit
                    ? deepPolicy().withMaxDepth(DEPTH - 1)
                    : deepPolicy().withMaxNodes(DEPTH - 1);
            StructuredConditionErrorCode expected = depthLimit
                    ? StructuredConditionErrorCode.DEPTH_EXCEEDED
                    : StructuredConditionErrorCode.NODE_COUNT_EXCEEDED;
            StructuredConditionException validation = assertThrows(StructuredConditionException.class,
                    () -> StructuredConditionCompiler.validateStructure(input, policy));
            StructuredConditionException compilation = assertThrows(StructuredConditionException.class,
                    () -> StructuredConditionCompiler.create().compile(form, input, policy));
            assertEquals(expected, validation.code());
            assertEquals(expectedPath, validation.path());
            assertEquals(expected, compilation.code());
            assertEquals(expectedPath, compilation.path());
        }
    }

    @Test
    void validationAndCompilationVisitDescendantsBeforeLaterInvalidSiblings() {
        DynamicForm form = DynamicForm.builder("samples", "samples")
                .addField(DynamicField.of("id", "INTEGER")).build();
        StructuredConditionInput input = StructuredConditionInput.and(
                StructuredConditionInput.and(StructuredConditionInput.term("long_field", "eq", 7)),
                null);
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults().withMaxStringLength(3);

        StructuredConditionException validation = assertThrows(StructuredConditionException.class,
                () -> StructuredConditionCompiler.validateStructure(input, policy));
        StructuredConditionException compilation = assertThrows(StructuredConditionException.class,
                () -> StructuredConditionCompiler.create().compile(form, input, policy));
        assertEquals(StructuredConditionErrorCode.FIELD_NOT_ALLOWED, validation.code());
        assertEquals("conditions[0].conditions[0].field", validation.path());
        assertEquals(StructuredConditionErrorCode.FIELD_NOT_ALLOWED, compilation.code());
        assertEquals(validation.path(), compilation.path());
    }

    private static StructuredConditionPolicy deepPolicy() {
        return StructuredConditionPolicy.defaults().withMaxDepth(DEPTH).withMaxNodes(DEPTH);
    }

    private static StructuredConditionInput deepInput() {
        StructuredConditionInput input = StructuredConditionInput.term("id", "eq", 7);
        for (int depth = 1; depth < DEPTH; depth++) {
            input = StructuredConditionInput.and(input);
        }
        return input;
    }
}
