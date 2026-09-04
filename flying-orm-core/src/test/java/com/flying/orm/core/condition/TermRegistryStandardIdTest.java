package com.flying.orm.core.condition;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TermRegistryStandardIdTest {

    @Test
    void rejectsCustomHandlerThatReusesStandardEqualsIdWithTheSameShape() {
        assertThrows(IllegalArgumentException.class,
                     () -> TermRegistry.builder()
                                       .add(TermHandler.simple("=", ConditionValueShape.SCALAR))
                                       .build());
    }

    @Test
    void reservesStandardIdsWithoutCaseSensitivity() {
        assertThrows(IllegalArgumentException.class,
                     () -> TermRegistry.builder()
                                       .add(TermHandler.simple("LIKE", ConditionValueShape.SCALAR))
                                       .build());
    }

    @Test
    void builderValidatesEachOwnedHandlerOnlyOnce() {
        int[] reads = new int[2];
        TermHandler handler = new TermHandler() {
            @Override
            public String id() {
                reads[0]++;
                return "custom-filter";
            }

            @Override
            public ConditionValueShape shape() {
                reads[1]++;
                return ConditionValueShape.SCALAR;
            }
        };

        TermRegistry registry = TermRegistry.builder().add(handler).build();

        assertEquals(1, reads[0]);
        assertEquals(1, reads[1]);
        assertEquals(handler, registry.handler("custom-filter"));
    }

    @Test
    void publishesOneImmutableHandlerSnapshot() {
        TermHandler handler = TermHandler.simple("custom-filter", ConditionValueShape.SCALAR);
        TermRegistry registry = TermRegistry.builder().add(handler).build();

        List<TermHandler> publishedHandlers = registry.handlers();

        assertSame(publishedHandlers, registry.handlers());
        assertThrows(UnsupportedOperationException.class,
                     () -> publishedHandlers.add(TermHandler.simple("other", ConditionValueShape.SCALAR)));
        assertEquals(List.of(handler), registry.handlers());
    }
}
