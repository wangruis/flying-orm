package com.flying.orm.rdb.dialect;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RdbDialectNameSelectionTest {

    @Test
    void selectsAllSupportedDialectsFromExplicitConfigurationNames() {
        for (String name : List.of("h2", "mysql", "postgresql", "oracle", "sqlserver")) {
            assertEquals(name, RdbDialectResolver.tryResolveName(name).orElseThrow().name());
        }
    }

    @Test
    void requiresARecognizedExplicitName() {
        assertTrue(RdbDialectResolver.tryResolveName(null).isEmpty());
        assertTrue(RdbDialectResolver.tryResolveName("").isEmpty());
        assertTrue(RdbDialectResolver.tryResolveName("unrecognized").isEmpty());
    }

    @Test
    void keepsOracleDefaultAndExplicitVersionSelection() {
        assertEquals("19c", RdbDialectResolver.tryResolveName("oracle").orElseThrow().version());
        RdbDialect oracle12c = RdbDialect.oracle(OracleVersion.V12C);
        assertEquals("oracle", oracle12c.name());
        assertEquals("12c", oracle12c.version());
        assertEquals(30, oracle12c.maxIdentifierLength());
    }
}
