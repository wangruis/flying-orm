package com.flying.orm.rdb.protection;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ProtectedColumnNamesTest {

    @Test
    void keepsNamesStableBoundedAndPurposeSpecific() {
        String first = ProtectedColumnNames.exact("customers", "email");

        assertEquals(first, ProtectedColumnNames.exact("customers", "email"));
        assertEquals(24, first.length());
        assertNotEquals(first, ProtectedColumnNames.containsFieldTag("customers", "email"));
    }

    @Test
    void preservesAlreadyPersistedProtectedColumnNames() {
        assertEquals("__fop_e_3e331ccea67d9589", ProtectedColumnNames.exact("customers", "email"));
    }

    @Test
    void preservesInputBoundariesEvenWhenNamesContainOldSeparators() {
        String separatorInForm = ProtectedColumnNames.exact("a\0b", "c");
        String separatorInField = ProtectedColumnNames.exact("a", "b\0c");

        assertNotEquals(separatorInForm, separatorInField);
    }
}
