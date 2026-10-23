package com.flying.orm.core.condition;

import org.junit.jupiter.api.Test;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.field.FieldIdentity;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class StructuredConditionPolicyMappingTest {
    private final DynamicForm form = DynamicForm.builder("samples", "samples")
            .addField(DynamicField.of("state", "VARCHAR")).build();
    private final StructuredConditionCompiler compiler = StructuredConditionCompiler.create();

    @Test
    void scalarMappingsComposeAndSurviveAllPolicyDerivations() {
        List<String> calls = new ArrayList<>();
        StructuredConditionPolicy source = StructuredConditionPolicy.defaults().mapValues((field, operator, value) -> {
            calls.add(field + ":" + operator + ":" + value);
            return "v" + value;
        });
        StructuredConditionPolicy derived = source.mapValues((field, operator, value) -> value + "!")
                .allowOperator("one-of", "in").allowOnlyFields(List.of("property"))
                .denyFields(List.of("secret")).allowFieldOperators("property", List.of("one-of"))
                .withMaxCollectionSize(2).withMaxStringLength(8).withMaxDepth(4).withMaxNodes(5)
                .withTerms(TermRegistry.standard()).withAdditionalTerms(TermRegistry.empty())
                .mapFields(name -> name.equals("property") ? "state" : name);
        ConditionGroup result = compiler.compile(form,
                StructuredConditionInput.term("state", "one-of", new String[]{"A", "D"}), derived);
        assertEquals(List.of("vA!", "vD!"), ((TermCondition) result.children().getFirst()).value());
        assertEquals(List.of("state:in:A", "state:in:D"), calls);
        ConditionGroup original = compiler.compile(form, StructuredConditionInput.term("state", "eq", "A"), source);
        assertEquals("vA", ((TermCondition) original.children().getFirst()).value());
    }

    @Test
    void unauthorizedFieldsOperatorsAndValuelessTermsNeverInvokeMappings() {
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults().mapValues((field, operator, value) -> {
            throw new AssertionError("mapping must not run");
        });
        assertThrows(StructuredConditionException.class, () -> compiler.compile(form,
                StructuredConditionInput.term("state", "eq", "A"), policy.denyFields(List.of("state"))));
        assertThrows(StructuredConditionException.class, () -> compiler.compile(form,
                StructuredConditionInput.term("state", "unknown", "A"), policy));
        compiler.compile(form, StructuredConditionInput.term("state", "is-null", null), policy);
        assertThrows(StructuredConditionException.class, () -> compiler.compile(form,
                StructuredConditionInput.term("state", "eq", "LONG"), policy.withMaxStringLength(2)));
    }

    @Test
    void mappingFailuresRetainTheExactCollectionValuePath() {
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults().mapValues((field, operator, value) -> {
            if (value.equals("D")) throw new IllegalArgumentException("bad state");
            return value;
        });
        StructuredConditionException failure = assertThrows(StructuredConditionException.class,
                () -> compiler.compile(form, StructuredConditionInput.and(
                        StructuredConditionInput.term("state", "in", List.of("A", "D"))), policy));
        assertEquals(StructuredConditionErrorCode.VALUE_CONVERSION_FAILED, failure.code());
        assertEquals("conditions[0].value[1]", failure.path());
        assertEquals("bad state", failure.getCause().getMessage());
    }

    @Test
    void mapsAllFieldRulesWithoutBroadeningAliases() {
        StructuredConditionPolicy source = StructuredConditionPolicy.defaults()
                .allowOnlyFields(List.of("state", "status_code", "name"))
                .denyFields(List.of("name"))
                .allowFieldOperators("state", List.of("eq", "in"))
                .allowFieldOperators("status_code", List.of("eq", "ne"))
                .withMaxDepth(19).withMaxNodes(777);
        StructuredConditionPolicy mapped = source.mapFields(name -> switch (name) {
            case "state" -> "status_code";
            case "name" -> "display_name";
            default -> name;
        });
        assertTrue(mapped.allowsField(FieldIdentity.of("status_code")));
        assertFalse(mapped.allowsField(FieldIdentity.of("state")));
        assertFalse(mapped.allowsField(FieldIdentity.of("display_name")));
        assertTrue(mapped.allowsFieldOperator(FieldIdentity.of("status_code"), "eq"));
        assertFalse(mapped.allowsFieldOperator(FieldIdentity.of("status_code"), "in"));
        assertFalse(mapped.allowsFieldOperator(FieldIdentity.of("status_code"), "ne"));
        assertTrue(source.allowsFieldOperator(FieldIdentity.of("state"), "in"));
        assertEquals(19, mapped.maxDepth());
        assertEquals(777, mapped.maxNodes());
        assertEquals(source.resolveOperator("eq"), mapped.resolveOperator("eq"));
    }

    @Test
    void mappingPreservesExplicitlyEmptyAllowlist() {
        StructuredConditionPolicy none = StructuredConditionPolicy.defaults().allowOnlyFields(List.of())
                .mapFields(name -> name);
        assertFalse(none.allowsField(FieldIdentity.of("anything")));
        assertTrue(StructuredConditionPolicy.defaults().mapFields(name -> name).allowsField(FieldIdentity.of("anything")));
    }
}
