package com.flying.orm.core.sql.render;

import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.Random;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlIdentifiersTest {

    private static final Pattern PART = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    @Test
    void preservesIdentifierAndProjectionGrammarIncludingDiagnostics() {
        String[] examples = {null, "", " ", "a", "_", "A_09", "a.b.c", " a.b ", "*", "a.*",
                "a.b.*", "*.*", "a.*.*", ".*", ".", "a.", ".a", "a..b", "1a", "a.1b", "a.*.b", "a*", "a .b",
                "a;drop", "a--b", "a/*b*/", "\"a\"", "`a`", "[a]", "sum(a)", "a as b",
                "中文", "é", "a\u0000b", "\u0000a\u0000", "\u0000", "\u2003", "\u2003a",
                "a\n.b", "a\r", "a\u2028b", "a\ud800b", "a\udc00b"};
        for (String value : examples) {
            assertEquivalent(value);
        }
        for (int character = 0; character < 256; character++) {
            assertEquivalent((char) character + "a");
            assertEquivalent("a" + (char) character + "b");
            assertEquivalent("a." + (char) character + "b");
        }
        Random random = new Random(4102);
        String alphabet = "azAZ_09..* ;-'\"/()\n\t\u0000\u2003é中文\ud800\udc00";
        for (int sample = 0; sample < 10_000; sample++) {
            StringBuilder value = new StringBuilder();
            int size = random.nextInt(32);
            for (int index = 0; index < size; index++) {
                value.append(alphabet.charAt(random.nextInt(alphabet.length())));
            }
            assertEquivalent(value.toString());
        }
    }

    @Test
    void validatingOrdinaryIdentifiersDoesNotAllocateMatchersOrSplitArrays() {
        com.sun.management.ThreadMXBean bean =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!bean.isThreadAllocatedMemoryEnabled()) {
            bean.setThreadAllocatedMemoryEnabled(true);
        }
        String[] names = {"user_id", "app.users", "_tenant9", "db.app.order_items"};
        int checksum = 0;
        for (int index = 0; index < 50_000; index++) {
            checksum += SqlIdentifiers.requireIdentifier(names[index & 3]).length();
        }
        long thread = Thread.currentThread().threadId();
        long before = bean.getThreadAllocatedBytes(thread);
        for (int index = 0; index < 20_000; index++) {
            checksum += SqlIdentifiers.requireIdentifier(names[index & 3]).length();
        }
        long allocated = bean.getThreadAllocatedBytes(thread) - before;
        assertTrue(checksum > 0);
        assertTrue(allocated < 20_000L * 64,
                () -> "identifier validation still allocates per-call parsing objects: " + allocated);
    }

    private static void assertEquivalent(String value) {
        assertEquals(outcome(value, false, true), outcome(value, false, false), "identifier: " + value);
        assertEquals(outcome(value, true, true), outcome(value, true, false), "projection: " + value);
    }

    private static String outcome(String value, boolean projection, boolean baseline) {
        try {
            String result = baseline ? baseline(value, projection)
                    : projection ? SqlIdentifiers.requireProjection(value)
                    : SqlIdentifiers.requireIdentifier(value);
            return "accepted:" + result;
        } catch (IllegalArgumentException failure) {
            return "rejected:" + failure.getMessage();
        }
    }

    private static String baseline(String value, boolean projection) {
        String category = projection ? "sql projection" : "sql identifier";
        if (!projection) {
            return baselinePlain(value, category);
        }
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(category + " must not be blank");
        }
        String text = value.trim();
        if (projection && text.equals("*")) {
            return text;
        }
        if (projection && text.endsWith(".*")) {
            return baselinePlain(text.substring(0, text.length() - 2), category) + ".*";
        }
        return baselinePlain(text, category);
    }

    private static String baselinePlain(String value, String category) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(category + " must not be blank");
        }
        String text = value.trim();
        for (String part : text.split("\\.", -1)) {
            if (!PART.matcher(part).matches()) {
                throw new IllegalArgumentException(category + " must be a plain identifier");
            }
        }
        return text;
    }
}
