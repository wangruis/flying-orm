package com.flying.orm.core.condition;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.IdentityHashMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConditionExecutionViewTest {

    @Test
    void conditionGroupPublishesTheSamePrecomputedExecutionView() throws Exception {
        ConditionGroup group = ConditionGroup.and()
                                             .where("tenant_id", "=", 7L)
                                             .or(nested -> nested.where("status", "in", java.util.List.of("A", "B")))
                                             .build();
        Class<?> views = Class.forName(
                "com.flying.orm.core.internal.condition.ConditionExecutionViews");
        Method of = views.getMethod("of", ConditionGroup.class);

        Object first = of.invoke(null, group);
        Object second = of.invoke(null, group);

        assertSame(first, second);
        assertThrows(NoSuchMethodException.class,
                     () -> first.getClass().getMethod("parameterSources"));
    }

    @Test
    void publicExecutionViewApiCannotExposeAndMutateAnAstOwnedValue() throws Exception {
        ConditionGroup group = ConditionGroup.and()
                                             .where("payload", "=", new byte[]{1, 2, 3})
                                             .build();
        TermCondition term = (TermCondition) group.children().getFirst();
        Class<?> views = Class.forName(
                "com.flying.orm.core.internal.condition.ConditionExecutionViews");
        Method rawAccessor = Arrays.stream(views.getMethods())
                                   .filter(method -> method.getParameterCount() == 2
                                           && method.getParameterTypes()[0] == ConditionGroup.class
                                           && method.getParameterTypes()[1] == TermCondition.class
                                           && method.getReturnType() == Object.class)
                                   .findFirst()
                                   .orElse(null);

        if (rawAccessor != null) {
            byte[] exposed = (byte[]) rawAccessor.invoke(null, group, term);
            exposed[0] = 9;
        }

        assertNull(rawAccessor, "public execution-view API must not expose raw AST-owned values");
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) term.value());
    }

    @Test
    void conditionExecutionViewDoesNotAllocateATermIdentityIndexPerGroup() throws Exception {
        Class<?> view = Class.forName(
                "com.flying.orm.core.internal.condition.ConditionExecutionView");

        assertFalse(Arrays.stream(view.getDeclaredFields())
                          .anyMatch(field -> IdentityHashMap.class.isAssignableFrom(field.getType())),
                    "each condition group must not retain a separate term identity map");
    }
}
