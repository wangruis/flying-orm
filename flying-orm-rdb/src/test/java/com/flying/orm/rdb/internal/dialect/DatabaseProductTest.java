package com.flying.orm.rdb.internal.dialect;

import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseProductTest {

    private static volatile DatabaseProduct detected;

    @Test
    void resolvesConfiguredAliasesWithoutGuessingUnknownNames() {
        assertEquals(DatabaseProduct.POSTGRESQL, DatabaseProduct.fromName("postgres"));
        assertEquals(DatabaseProduct.SQL_SERVER, DatabaseProduct.fromName("sql-server"));
        assertEquals(DatabaseProduct.UNKNOWN, DatabaseProduct.fromName("not-mysql"));
    }

    @Test
    void detectsStandardDriverAndMetadataProductNames() {
        assertEquals(DatabaseProduct.MYSQL, DatabaseProduct.detect("MariaDB ConnectionFactory"));
        assertEquals(DatabaseProduct.POSTGRESQL, DatabaseProduct.detect("R2DBC PostgreSQL"));
        assertEquals(DatabaseProduct.ORACLE, DatabaseProduct.detect("Oracle Database"));
        assertEquals(DatabaseProduct.SQL_SERVER, DatabaseProduct.detect("Microsoft SQL Server"));
        assertEquals(DatabaseProduct.UNKNOWN, DatabaseProduct.detect(null));
    }

    @Test
    void preservesCharacterNormalizationAtEveryUtf16Value() {
        for (int value = Character.MIN_VALUE; value <= Character.MAX_VALUE; value++) {
            char character = (char) value;
            String input = "post" + character + "gres";
            String normalized = Character.isLetterOrDigit(character)
                    ? "post" + Character.toLowerCase(character) + "gres" : "postgres";
            DatabaseProduct expected = normalized.equals("postgres")
                    ? DatabaseProduct.POSTGRESQL : DatabaseProduct.UNKNOWN;
            assertEquals(expected, DatabaseProduct.fromName(input));
            assertEquals(expected, DatabaseProduct.detect(input));
        }
        assertEquals(DatabaseProduct.POSTGRESQL, DatabaseProduct.fromName(" P_o-S.t G r E s Q L "));
        assertEquals(DatabaseProduct.UNKNOWN, DatabaseProduct.fromName("\uD83D\uDE00"));
        assertEquals(DatabaseProduct.H2, DatabaseProduct.fromName("H2 Database Engine"));
    }

    @Test
    void doesNotAllocateCopiesOfCanonicalProductNames() {
        com.sun.management.ThreadMXBean allocation =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        allocation.setThreadAllocatedMemoryEnabled(true);
        String[] names = {"postgresql", "mysql", "oracle", "sqlserver", "h2"};
        int iterations = 100_000;
        for (int index = 0; index < iterations; index++) {
            detected = DatabaseProduct.detect(names[index % names.length]);
        }
        long thread = Thread.currentThread().threadId();
        long before = allocation.getThreadAllocatedBytes(thread);
        for (int index = 0; index < iterations; index++) {
            detected = DatabaseProduct.detect(names[index % names.length]);
        }
        long bytes = allocation.getThreadAllocatedBytes(thread) - before;
        assertTrue(bytes < iterations * 16L, "canonical product detection allocated " + bytes + " bytes");
    }
}
