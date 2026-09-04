package com.flying.orm.core.condition;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TermExtensionCapabilityAndBudgetTest {

    @Test
    void validatesCapabilitiesAndDeclaredWorkWithoutExecutingRenderer() {
        TermExtensionDescriptor descriptor = TermExtensionDescriptor.filter(
                "json-path-eq", Set.of("json-functions"), 2, 1);

        assertDoesNotThrow(() -> descriptor.requireUsable(Set.of("json-functions"), 2, 1));
        assertThrows(UnsupportedOperationException.class,
                     () -> descriptor.requireUsable(Set.of(), 2, 1));
        assertThrows(IllegalArgumentException.class,
                     () -> descriptor.requireUsable(Set.of("json-functions"), 3, 1));
        assertThrows(IllegalArgumentException.class,
                     () -> descriptor.requireUsable(Set.of("json-functions"), 2, 0));
    }

    @Test
    void structuredCustomTermConsumesItsDeclaredComplexityInTheExistingNodeBudget() {
        TermExtensionDescriptor descriptor = TermExtensionDescriptor.filter(
                "expensive-filter", Set.of(), 1, 2);
        TermRegistry terms = TermRegistry.builder()
                                         .add(TermHandler.described(
                                                 descriptor, ConditionValueShape.SCALAR))
                                         .build();
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults()
                                                                      .allowOperator("expensive-filter")
                                                                      .withTerms(terms)
                                                                      .withMaxNodes(1);

        StructuredConditionException error = assertThrows(
                StructuredConditionException.class,
                () -> StructuredConditionCompiler.create().compile(
                        form(), StructuredConditionInput.term("id", "expensive-filter", 7L), policy));

        assertEquals(StructuredConditionErrorCode.NODE_COUNT_EXCEEDED, error.code());
    }

    @Test
    void structuredCustomTermWithoutDescriptorFailsClosedBeforeRendering() {
        TermRegistry terms = TermRegistry.builder()
                                         .add(TermHandler.simple("trusted-only"))
                                         .build();
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults()
                                                                      .allowOperator("trusted-only")
                                                                      .withTerms(terms);

        assertThrows(IllegalArgumentException.class,
                     () -> StructuredConditionCompiler.create().compile(
                             form(), StructuredConditionInput.term("id", "trusted-only", 7L), policy));
    }

    private static DynamicForm form() {
        return DynamicForm.builder("accounts", "accounts")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .build();
    }
}
