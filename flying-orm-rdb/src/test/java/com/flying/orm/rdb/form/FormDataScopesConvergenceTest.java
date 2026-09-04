package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.TermCondition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class FormDataScopesConvergenceTest {

    @Test
    void unchangedTreeIsReusedWithoutRebuildingAnyBranch() {
        ConditionGroup nested = ConditionGroup.or()
                                              .where("status", "=", "ACTIVE")
                                              .build();
        ConditionGroup source = ConditionGroup.and()
                                              .where("tenant_id", "=", 7L)
                                              .add(nested)
                                              .build();

        ConditionGroup unwrapped = FormDataScopes.unwrapTrustedValues(source);

        assertSame(source, unwrapped);
        assertSame(nested, unwrapped.children().get(1));
    }

    @Test
    void onlyBranchesContainingTrustedValuesAreRebuilt() {
        ConditionGroup unchanged = ConditionGroup.or()
                                                 .where("status", "=", "ACTIVE")
                                                 .build();
        ConditionGroup changed = ConditionGroup.and()
                                               .add(TermCondition.of(
                                                       "tenant_id", "=",
                                                       new FormDataScopes.TrustedScopeValue(7L)))
                                               .build();
        ConditionGroup source = ConditionGroup.and().add(unchanged).add(changed).build();

        ConditionGroup unwrapped = FormDataScopes.unwrapTrustedValues(source);

        assertNotSame(source, unwrapped);
        assertSame(unchanged, unwrapped.children().get(0));
        assertNotSame(changed, unwrapped.children().get(1));
        TermCondition term = (TermCondition) ((ConditionGroup) unwrapped.children().get(1)).children().getFirst();
        assertEquals(7L, term.value());
        assertFalse(containsTrustedValue(unwrapped));
    }

    private static boolean containsTrustedValue(ConditionGroup group) {
        for (ConditionNode child : group.children()) {
            if (child instanceof ConditionGroup nested && containsTrustedValue(nested)) {
                return true;
            }
            if (child instanceof TermCondition term
                    && term.value() instanceof FormDataScopes.TrustedScopeValue) {
                return true;
            }
        }
        return false;
    }
}
