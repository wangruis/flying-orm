package com.flying.orm.core.codec;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EnumConstantCodecTest {

    @Test
    void writesConstantSpecificEnumBodiesUsingTheDeclaredName() {
        assertEquals("ACTIVE", ValueCodecRegistry.standard().write(State.ACTIVE));
        assertEquals(State.ACTIVE, ValueCodecRegistry.standard().read("ACTIVE", State.class));
    }

    private enum State {
        ACTIVE {
            @Override
            public String toString() {
                return "display-active";
            }
        }
    }
}
