package com.flying.orm.core.metadata;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ControlledCheckPredicateTest {

    @Test
    void snapshotsControlledValuesWithoutAcceptingArbitrarySqlObjects() {
        List<Object> states = new ArrayList<>(List.of("NEW", "DONE"));
        CheckPredicate.In predicate = (CheckPredicate.In) CheckPredicate.in("state", states);
        states.add("CANCELLED");

        assertEquals(List.of("NEW", "DONE"), predicate.values());
        assertThrows(IllegalArgumentException.class,
                     () -> CheckPredicate.compare("payload", CheckPredicate.ComparisonOperator.EQUAL, new Object()));
    }

    @Test
    void exposesOnlyStructuredLogicalComposition() {
        CheckPredicate predicate = CheckPredicate.and(
                CheckPredicate.compare("age", CheckPredicate.ComparisonOperator.GREATER_THAN_OR_EQUAL, 18),
                CheckPredicate.isNotNull("name"));

        assertEquals(2, ((CheckPredicate.Logical) predicate).predicates().size());
    }
}
