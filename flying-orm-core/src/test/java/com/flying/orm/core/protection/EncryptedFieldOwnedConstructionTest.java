package com.flying.orm.core.protection;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EncryptedFieldOwnedConstructionTest {

    @Test
    void publicConstructionSnapshotsAndNormalizesMutableCollections() {
        Set<EncryptedSearchMode> modes = EnumSet.of(EncryptedSearchMode.SUFFIX);
        List<Integer> lengths = new ArrayList<>(List.of(6, 4, 6));
        EncryptedFieldDefinition definition = new EncryptedFieldDefinition(
                modes, "identity", lengths, 64, 3);
        modes.clear();
        lengths.clear();

        assertEquals(Set.of(EncryptedSearchMode.SUFFIX), definition.searchModes());
        assertEquals(List.of(4, 6), definition.suffixLengths());
        assertThrows(UnsupportedOperationException.class, () -> definition.searchModes().clear());
        assertThrows(UnsupportedOperationException.class, () -> definition.suffixLengths().add(8));
    }
}
