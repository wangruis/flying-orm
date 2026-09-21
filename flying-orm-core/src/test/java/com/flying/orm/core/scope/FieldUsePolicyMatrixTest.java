package com.flying.orm.core.scope;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FieldUsePolicyMatrixTest {

    @Test
    void unrestrictedPolicyAndSnapshotAreSharedSingletons() {
        FieldUsePolicy policy = FieldUsePolicy.unrestricted();
        FieldUseRequirements requirements = FieldUseRequirements.builder()
                .require("email", FieldUse.PROJECT, FieldUseOrigin.CALLER)
                .require("email", FieldUse.FULL_VALUE, FieldUseOrigin.CALLER)
                .build();

        assertSame(policy, FieldUsePolicy.unrestricted());
        assertSame(FieldUseSnapshot.unrestricted(),
                   policy.approve(requirements, FieldScope.unrestricted()));
    }

    @Test
    void policyAndFieldScopeCanOnlyNarrowEachOther() {
        FieldUsePolicy policy = FieldUsePolicy.builder()
                .allow("id", FieldUse.PROJECT, FieldUse.FULL_VALUE,
                       FieldUse.FILTER, FieldUse.INSERT)
                .visibility("secret", FieldVisibility.MASKED)
                .allow("secret", FieldUse.FILTER, FieldUse.INSERT)
                .build();
        FieldScope scope = new FieldScope(Set.of("id", "secret"), Set.of("id"));

        assertDecision(policy, scope, "id", FieldUse.PROJECT, true, FieldVisibility.FULL);
        assertDecision(policy, scope, "secret", FieldUse.PROJECT, true, FieldVisibility.MASKED);
        assertDecision(policy, scope, "secret", FieldUse.INSERT, false, FieldVisibility.HIDDEN);
        assertDecision(policy, scope, "id", FieldUse.SORT, false, FieldVisibility.HIDDEN);

        FieldScope narrower = FieldScope.readWrite("id");
        assertDecision(policy, narrower, "secret", FieldUse.FILTER, false, FieldVisibility.HIDDEN);
    }

    @Test
    void requirementsAndApprovalSnapshotAreImmutableCallFacts() {
        FieldUseRequirements.Builder builder = FieldUseRequirements.builder()
                .require("id", FieldUse.PROJECT, FieldUseOrigin.CALLER)
                .require("secret", FieldUse.PROJECT, FieldUseOrigin.CALLER);
        FieldUseRequirements requirements = builder.build();
        builder.require("late", FieldUse.PROJECT, FieldUseOrigin.CALLER);

        FieldUsePolicy policy = FieldUsePolicy.builder()
                .visibility("id", FieldVisibility.FULL)
                .visibility("secret", FieldVisibility.MASKED)
                .build();
        FieldUseSnapshot snapshot = policy.approve(requirements, FieldScope.readable("id"));

        assertEquals(2, requirements.requirements().size());
        assertEquals(2, snapshot.decisions().size());
        assertFalse(snapshot.allowed());
        assertEquals(List.of("secret"), snapshot.deniedDecisions().stream()
                                               .map(FieldDecision::field)
                                               .toList());
        assertThrows(UnsupportedOperationException.class,
                     () -> snapshot.decisions().clear());
    }

    @Test
    void governedSnapshotPrecompilesImmutableCallerVisibility() throws Exception {
        FieldUseSnapshot snapshot = FieldUseSnapshot.of(List.of(
                new FieldDecision("id", FieldUse.PROJECT, FieldUseOrigin.CALLER,
                                  true, FieldVisibility.MASKED),
                new FieldDecision("id", FieldUse.FULL_VALUE, FieldUseOrigin.CALLER,
                                  true, FieldVisibility.FULL),
                new FieldDecision("secret", FieldUse.PROJECT, FieldUseOrigin.CALLER,
                                  true, FieldVisibility.MASKED),
                new FieldDecision("tenant_id", FieldUse.FILTER, FieldUseOrigin.INTERNAL_TENANT,
                                  true, FieldVisibility.HIDDEN),
                new FieldDecision("denied", FieldUse.PROJECT, FieldUseOrigin.CALLER,
                                  false, FieldVisibility.HIDDEN)));

        Field compiled = java.util.Arrays.stream(FieldUseSnapshot.class.getDeclaredFields())
                .filter(field -> field.getName().equals("callerVisibility"))
                .findFirst()
                .orElse(null);
        assertNotNull(compiled, "governed snapshots must precompile caller visibility");
        compiled.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, FieldVisibility> visibility =
                (Map<String, FieldVisibility>) compiled.get(snapshot);

        assertEquals(Map.of("id", FieldVisibility.FULL,
                            "secret", FieldVisibility.MASKED), visibility);
        assertThrows(UnsupportedOperationException.class,
                     () -> visibility.put("late", FieldVisibility.FULL));
        assertEquals(FieldVisibility.FULL, snapshot.visibility("id"));
        assertEquals(FieldVisibility.MASKED, snapshot.visibility("secret"));
        assertEquals(FieldVisibility.HIDDEN, snapshot.visibility("tenant_id"));
        assertEquals(FieldVisibility.HIDDEN, snapshot.visibility("denied"));
    }

    private static void assertDecision(FieldUsePolicy policy,
                                       FieldScope scope,
                                       String field,
                                       FieldUse use,
                                       boolean allowed,
                                       FieldVisibility visibility) {
        FieldDecision decision = policy.decide(field, use, FieldUseOrigin.CALLER, scope);
        assertEquals(allowed, decision.allowed());
        assertEquals(visibility, decision.visibility());
        assertEquals(field, decision.field());
        assertEquals(use, decision.use());
        assertEquals(FieldUseOrigin.CALLER, decision.origin());
    }
}
