package com.flying.orm.core.condition;

import com.flying.orm.core.codec.ValueCodecRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

class StandardRegistryFastPathTest {

    @Test
    void standardAndEmptyRegistriesRemainSharedDescriptorFreeInstances() {
        assertSame(TermRegistry.standard(), TermRegistry.standard());
        assertSame(TermRegistry.empty(), TermRegistry.empty());
        assertFalse(TermRegistry.standard().hasDescriptors());
        assertFalse(TermRegistry.empty().hasDescriptors());

        assertSame(ValueCodecRegistry.standard(), ValueCodecRegistry.standard());
        assertFalse(ValueCodecRegistry.standard().hasDescriptors());
    }
}
