package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlIdentifiers;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class SchemaIdentifierQuotingTest {
    private static volatile String sink;

    @Test
    void plainNamesAllocateNoMoreThanDirectQuoting() {
        java.lang.management.ThreadMXBean management = ManagementFactory.getThreadMXBean();
        assumeTrue(management instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocation = (com.sun.management.ThreadMXBean) management;
        assumeTrue(allocation.isThreadAllocatedMemorySupported());
        allocation.setThreadAllocatedMemoryEnabled(true);
        SchemaDialectTypeSupport dialect = dialect("\"", "\"");
        Function<String, String> direct = text -> "\"" + text + "\"";
        Function<String, String> actual = dialect::identifier;
        quoteRepeatedly(direct);
        quoteRepeatedly(actual);
        long directBytes = allocated(allocation, direct);
        long actualBytes = allocated(allocation, actual);
        assertTrue(actualBytes <= directBytes,
                () -> "identifier allocated " + actualBytes + " bytes versus direct quoting " + directBytes);
    }

    @Test
    void preservesSingleAndQualifiedNamesAcrossQuoteStyles() {
        for (String[] quotes : new String[][]{{null, null}, {"\"", "\""}, {"`", "`"}, {"[", "]"}}) {
            SchemaDialectTypeSupport dialect = dialect(quotes[0], quotes[1]);
            for (String name : List.of("id", "UserName", "_value0", " schema.Table ", "cat.schema.table", "a.b.c.d.e")) {
                assertEquals(previousIdentifier(name, quotes[0], quotes[1]), dialect.identifier(name));
            }
            for (int index = 0; index < 1000; index++) {
                String name = "catalog" + index + ".schema_" + index + ".Table" + index;
                assertEquals(previousIdentifier(name, quotes[0], quotes[1]), dialect.identifier(name));
            }
        }
    }

    @Test
    void doesNotBypassIdentifierValidation() {
        for (String[] quotes : new String[][]{{null, null}, {"\"", "\""}, {"`", "`"}, {"[", "]"}}) {
            SchemaDialectTypeSupport dialect = dialect(quotes[0], quotes[1]);
            for (String name : new String[]{null, "", " ", ".a", "a.", "a..b", "a.1b", "a b", "a;drop", "a--b",
                    "a/*b*/", "\"a\"", "a.*", "a\\b", "a\nb"}) {
                IllegalArgumentException expected = assertThrows(IllegalArgumentException.class,
                        () -> previousIdentifier(name, quotes[0], quotes[1]));
                IllegalArgumentException actual = assertThrows(IllegalArgumentException.class,
                        () -> dialect.identifier(name));
                assertEquals(expected.getMessage(), actual.getMessage());
            }
        }
    }

    private static long allocated(com.sun.management.ThreadMXBean allocation, Function<String, String> quoting) {
        long thread = Thread.currentThread().threadId();
        long before = allocation.getThreadAllocatedBytes(thread);
        quoteRepeatedly(quoting);
        return allocation.getThreadAllocatedBytes(thread) - before;
    }

    private static void quoteRepeatedly(Function<String, String> quoting) {
        for (int index = 0; index < 100_000; index++) sink = quoting.apply((index & 1) == 0 ? "status" : "total_score");
    }

    private static SchemaDialectTypeSupport dialect(String open, String close) {
        return new SchemaDialectTypeSupport(open, close, Map.of(), Map.of(), SchemaDialect.GeneratedValueStyle.NONE);
    }

    private static String previousIdentifier(String value, String open, String close) {
        String text = SqlIdentifiers.requireIdentifier(value);
        if (open == null) return text;
        StringJoiner quoted = new StringJoiner(".");
        for (String part : text.split("\\.")) quoted.add(open + part + close);
        return quoted.toString();
    }
}
