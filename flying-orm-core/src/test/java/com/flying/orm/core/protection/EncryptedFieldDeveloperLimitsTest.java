package com.flying.orm.core.protection;

import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EncryptedFieldDeveloperLimitsTest {

    @Test
    void acceptsDeveloperNormalizedLengthAboveFormerCeiling() {
        assertEquals(Integer.MAX_VALUE, EncryptedFieldDefinition.builder()
                .maxNormalizedLength(Integer.MAX_VALUE).build().maxNormalizedLength());
    }

    @Test
    void acceptsMoreThanThirtyTwoDeclaredSuffixLengths() {
        int[] lengths = IntStream.rangeClosed(1, 40).toArray();
        EncryptedFieldDefinition definition = EncryptedFieldDefinition.builder()
                .searchModes(EncryptedSearchMode.SUFFIX)
                .suffixLengths(lengths).maxNormalizedLength(40).build();

        assertEquals(40, definition.suffixLengths().size());
    }

    @Test
    void stillRequiresPositiveAndInternallyConsistentDeveloperLimits() {
        assertThrows(IllegalArgumentException.class,
                () -> EncryptedFieldDefinition.builder().maxNormalizedLength(0).build());
        assertThrows(IllegalArgumentException.class, () -> EncryptedFieldDefinition.builder()
                .searchModes(EncryptedSearchMode.SUFFIX).suffixLengths(41)
                .maxNormalizedLength(40).build());
    }
}
