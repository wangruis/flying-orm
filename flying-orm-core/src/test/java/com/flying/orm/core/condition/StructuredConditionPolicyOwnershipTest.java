package com.flying.orm.core.condition;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class StructuredConditionPolicyOwnershipTest {

    @Test
    void numericDerivationsShareAllValidatedCollections() throws ReflectiveOperationException {
        StructuredConditionPolicy source = configuredPolicy();
        StructuredConditionPolicy derived = source.withMaxDepth(12);

        assertShared(source, derived, "operators");
        assertShared(source, derived, "allowedFields");
        assertShared(source, derived, "deniedFields");
        assertShared(source, derived, "fieldOperators");
        assertShared(source, derived, "terms");
    }

    @Test
    void collectionDerivationsReplaceOnlyTheChangedValidatedCollection() throws ReflectiveOperationException {
        StructuredConditionPolicy source = configuredPolicy();
        StructuredConditionPolicy derived = source.allowOperator("custom-filter", "=");

        assertNotSame(field(source, "operators"), field(derived, "operators"));
        assertShared(source, derived, "allowedFields");
        assertShared(source, derived, "deniedFields");
        assertShared(source, derived, "fieldOperators");
        assertShared(source, derived, "terms");
    }

    private static StructuredConditionPolicy configuredPolicy() {
        return StructuredConditionPolicy.defaults()
                                        .allowOnlyFields(List.of("tenant_id", "state"))
                                        .denyFields(List.of("secret"))
                                        .allowFieldOperators("state", List.of("eq", "in"));
    }

    private static void assertShared(StructuredConditionPolicy source,
                                     StructuredConditionPolicy derived,
                                     String name) throws ReflectiveOperationException {
        assertSame(field(source, name), field(derived, name));
    }

    private static Object field(StructuredConditionPolicy policy,
                                String name) throws ReflectiveOperationException {
        Field field = StructuredConditionPolicy.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(policy);
    }
}
