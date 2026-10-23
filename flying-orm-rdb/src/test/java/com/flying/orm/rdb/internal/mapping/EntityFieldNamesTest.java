package com.flying.orm.rdb.internal.mapping;

import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntityFieldNamesTest {

    @Test
    void reusesAlreadyCanonicalNames() {
        for (String text : new String[]{"id", "name", "status", "score", "user123", "a.b"}) {
            String name = new String(text.toCharArray());
            assertSame(name, EntityFieldNames.key(name));
            if (!name.contains(".")) {
                assertSame(name, EntityFieldNames.resultKey(name));
            }
        }
    }

    @Test
    void preservesMemberAndResultLabelRules() {
        assertEquals("user name", EntityFieldNames.key(" User Name "));
        assertEquals("username", EntityFieldNames.resultKey("table.\"User_Name\""));
        assertEquals("username", EntityFieldNames.resultKey("table.[User-Name]"));
        assertEquals("username", EntityFieldNames.resultKey("table.`User Name`"));
        assertEquals("user.", EntityFieldNames.resultKey("User."));
        assertEquals("", EntityFieldNames.key("_-"));
        assertEquals("", EntityFieldNames.resultKey("\"\""));
        assertEquals("i\u0307\u03bf\u03c2", EntityFieldNames.key("\u0130Ο_Σ"));
    }

    @Test
    void matchesPreviousAlgorithmForGeneratedAndUnicodeNames() {
        String alphabet = "aAzZ09_- .`[]\"\t\u0000éİΣσς中\ud801\udc00";
        Random random = new Random(4112);
        for (int sample = 0; sample < 10_000; sample++) {
            StringBuilder value = new StringBuilder("x");
            int length = random.nextInt(48);
            for (int index = 0; index < length; index++) {
                value.append(alphabet.charAt(random.nextInt(alphabet.length())));
            }
            String name = value.toString();
            assertEquals(previous(name, false), EntityFieldNames.key(name), name);
            assertEquals(previous(name, true), EntityFieldNames.resultKey(name), name);
        }
    }

    @Test
    void preservesInvalidInputErrors() {
        for (String value : new String[]{null, "", " ", "\t\n", "\u2003"}) {
            assertEquals("entity field name must not be blank",
                    assertThrows(IllegalArgumentException.class, () -> EntityFieldNames.key(value)).getMessage());
            assertEquals("result column label must not be blank",
                    assertThrows(IllegalArgumentException.class, () -> EntityFieldNames.resultKey(value)).getMessage());
        }
    }

    private static String previous(String value, boolean result) {
        String text = value.trim();
        if (result) {
            int qualifier = text.lastIndexOf('.');
            if (qualifier >= 0 && qualifier < text.length() - 1) {
                text = text.substring(qualifier + 1);
            }
            if (text.length() >= 2 && ((text.startsWith("\"") && text.endsWith("\""))
                    || (text.startsWith("`") && text.endsWith("`"))
                    || (text.startsWith("[") && text.endsWith("]")))) {
                text = text.substring(1, text.length() - 1);
            }
        }
        StringBuilder key = new StringBuilder(text.length());
        boolean nonAscii = false;
        for (int index = 0; index < text.length(); index++) {
            char current = text.charAt(index);
            if (current != '_' && current != '-' && (!result || current != ' ')) {
                nonAscii |= current > 0x7f;
                key.append(current >= 'A' && current <= 'Z' ? (char) (current + ('a' - 'A')) : current);
            }
        }
        String normalized = key.toString();
        return nonAscii ? normalized.toLowerCase(Locale.ROOT) : normalized;
    }
}
