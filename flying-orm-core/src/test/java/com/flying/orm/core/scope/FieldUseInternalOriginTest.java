package com.flying.orm.core.scope;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FieldUseInternalOriginTest {

    @Test
    void fieldUsesAndOriginsRemainClosedAndExplicit() {
        assertArrayEquals(new FieldUse[]{
                FieldUse.PROJECT,
                FieldUse.FULL_VALUE,
                FieldUse.MASKED_VALUE,
                FieldUse.FILTER,
                FieldUse.HAVING,
                FieldUse.SORT,
                FieldUse.JOIN,
                FieldUse.GROUP,
                FieldUse.AGGREGATE,
                FieldUse.INSERT,
                FieldUse.UPDATE
        }, FieldUse.values());
        assertArrayEquals(new FieldUseOrigin[]{
                FieldUseOrigin.CALLER,
                FieldUseOrigin.INTERNAL_SCOPE,
                FieldUseOrigin.INTERNAL_TENANT,
                FieldUseOrigin.INTERNAL_LOGIC_DELETE,
                FieldUseOrigin.INTERNAL_VERSION,
                FieldUseOrigin.INTERNAL_TIE_BREAKER
        }, FieldUseOrigin.values());
        assertFalse(FieldUseOrigin.CALLER.internal());
        assertTrue(FieldUseOrigin.INTERNAL_SCOPE.internal());
    }

    @Test
    void internalGrantDoesNotLeakToCallerOrAnotherInternalOrigin() {
        FieldUsePolicy policy = FieldUsePolicy.builder()
                .allow("tenant_id", FieldUseOrigin.INTERNAL_TENANT, FieldUse.FILTER)
                .allow("deleted", FieldUseOrigin.INTERNAL_LOGIC_DELETE,
                       FieldUse.FILTER, FieldUse.UPDATE)
                .allow("id", FieldUseOrigin.INTERNAL_TIE_BREAKER,
                       FieldUse.PROJECT, FieldUse.SORT)
                .build();
        FieldScope callerScope = FieldScope.readWrite("display_name");

        assertTrue(policy.decide("tenant_id", FieldUse.FILTER,
                                 FieldUseOrigin.INTERNAL_TENANT, callerScope).allowed());
        assertFalse(policy.decide("tenant_id", FieldUse.FILTER,
                                  FieldUseOrigin.CALLER, callerScope).allowed());
        assertFalse(policy.decide("tenant_id", FieldUse.FILTER,
                                  FieldUseOrigin.INTERNAL_SCOPE, callerScope).allowed());
        assertTrue(policy.decide("id", FieldUse.SORT,
                                 FieldUseOrigin.INTERNAL_TIE_BREAKER, callerScope).allowed());
        assertTrue(policy.decide("id", FieldUse.PROJECT,
                                 FieldUseOrigin.INTERNAL_TIE_BREAKER, callerScope).visibility()
                         == FieldVisibility.HIDDEN);
    }

    @Test
    void callerGrantDoesNotAuthorizeAnInternalUse() {
        FieldUsePolicy policy = FieldUsePolicy.builder()
                .allow("version", FieldUse.UPDATE)
                .build();

        assertTrue(policy.decide("version", FieldUse.UPDATE,
                                 FieldUseOrigin.CALLER, FieldScope.unrestricted()).allowed());
        assertFalse(policy.decide("version", FieldUse.UPDATE,
                                  FieldUseOrigin.INTERNAL_VERSION,
                                  FieldScope.unrestricted()).allowed());
        assertThrows(IllegalArgumentException.class,
                     () -> FieldUsePolicy.builder().allowInternal(
                             "version", FieldUseOrigin.CALLER, FieldUse.UPDATE));
    }
}
