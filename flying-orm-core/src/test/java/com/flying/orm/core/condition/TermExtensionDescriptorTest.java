package com.flying.orm.core.condition;

import com.flying.orm.core.scope.FieldUse;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TermExtensionDescriptorTest {

    @Test
    void freezesStableFilterContractAndFingerprint() {
        TermExtensionDescriptor first = TermExtensionDescriptor.filter(
                "custom-search", Set.of("native-json", "json-functions"), 2, 3);
        TermExtensionDescriptor reordered = TermExtensionDescriptor.filter(
                " CUSTOM-SEARCH ", Set.of("json-functions", "native-json"), 2, 3);

        assertEquals("custom-search", first.id());
        assertEquals(FieldUse.FILTER, first.fieldUse());
        assertEquals(Set.of("json-functions", "native-json"), first.requiredCapabilities());
        assertEquals(first, reordered);
        assertEquals(first.fingerprint(), reordered.fingerprint());
    }

    @Test
    void rejectsInvalidBudgetsBeforeRegistration() {
        assertThrows(IllegalArgumentException.class,
                     () -> TermExtensionDescriptor.filter("custom", Set.of(), -1, 1));
        assertThrows(IllegalArgumentException.class,
                     () -> TermExtensionDescriptor.filter("custom", Set.of(), 1, 0));
    }
}
