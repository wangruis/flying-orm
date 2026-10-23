package com.flying.orm.rdb.dialect;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FiveDialectCapabilityMatrixTest {

    @Test
    void exposesTheExistingFiveDialectCapabilityIdMatrixThroughStableCapabilityIds() {
        Map<RdbDialect, Set<DialectCapabilityId>> expected = Map.of(
                RdbDialect.h2(), Set.of(DialectCapabilityId.IDENTITY_COLUMNS,
                                        DialectCapabilityId.SEQUENCES,
                                        DialectCapabilityId.MERGE_UPSERT),
                RdbDialect.mysql(), Set.of(DialectCapabilityId.JSON_FUNCTIONS,
                                           DialectCapabilityId.IDENTITY_COLUMNS,
                                           DialectCapabilityId.MYSQL_RELATIONAL_METADATA),
                RdbDialect.postgresql(), Set.of(DialectCapabilityId.JSON_FUNCTIONS,
                                                DialectCapabilityId.NATIVE_JSON,
                                                DialectCapabilityId.NATIVE_BOOLEAN,
                                                DialectCapabilityId.LARGE_OBJECTS,
                                                DialectCapabilityId.IDENTITY_COLUMNS,
                                                DialectCapabilityId.SEQUENCES,
                                                DialectCapabilityId.POSTGRESQL_VECTOR,
                                                DialectCapabilityId.FULL_OUTER_JOIN),
                RdbDialect.oracle(), Set.of(DialectCapabilityId.OFFSET_FETCH_PAGINATION,
                                           DialectCapabilityId.MERGE_UPSERT,
                                           DialectCapabilityId.IDENTITY_COLUMNS,
                                           DialectCapabilityId.SEQUENCES,
                                           DialectCapabilityId.JSON_FUNCTIONS,
                                           DialectCapabilityId.LARGE_OBJECTS,
                                           DialectCapabilityId.FULL_OUTER_JOIN),
                RdbDialect.sqlServer(), Set.of(DialectCapabilityId.OFFSET_FETCH_PAGINATION,
                                              DialectCapabilityId.MERGE_UPSERT,
                                              DialectCapabilityId.IDENTITY_COLUMNS,
                                              DialectCapabilityId.SEQUENCES,
                                              DialectCapabilityId.JSON_FUNCTIONS,
                                              DialectCapabilityId.LARGE_OBJECTS,
                                              DialectCapabilityId.FULL_OUTER_JOIN));

        for (Map.Entry<RdbDialect, Set<DialectCapabilityId>> entry : expected.entrySet()) {
            assertEquals(entry.getValue(), entry.getKey().capabilities().ids());
            for (DialectCapabilityId feature : expected.values().stream().flatMap(Set::stream).distinct().toList()) {
                boolean supported = entry.getValue().contains(feature);
                assertEquals(supported, entry.getKey().supports(feature));
                assertEquals(supported,
                             entry.getKey().capabilities().supports(feature));
            }
        }
    }

    @Test
    void exposesKnownIdentifierLimitsAndKeepsVersionSensitiveCapabilitiesClosed() {
        assertEquals(256, RdbDialect.h2().maxIdentifierLength());
        assertEquals(64, RdbDialect.mysql().maxIdentifierLength());
        assertEquals(63, RdbDialect.postgresql().maxIdentifierLength());
        assertEquals(30, RdbDialect.oracle(OracleVersion.V12C).maxIdentifierLength());
        assertEquals(128, RdbDialect.oracle(OracleVersion.V19C).maxIdentifierLength());
        assertEquals(128, RdbDialect.sqlServer(SqlServerVersion.V2012).maxIdentifierLength());

        assertFalse(RdbDialect.oracle(OracleVersion.V19C)
                              .capabilities().supports(DialectCapabilityId.NATIVE_JSON));
        assertTrue(RdbDialect.oracle(OracleVersion.V21C)
                             .capabilities().supports(DialectCapabilityId.NATIVE_JSON));
        assertFalse(RdbDialect.oracle(OracleVersion.V21C)
                              .capabilities().supports(DialectCapabilityId.NATIVE_BOOLEAN));
        assertTrue(RdbDialect.oracle(OracleVersion.V23AI)
                             .capabilities().supports(DialectCapabilityId.NATIVE_BOOLEAN));
        assertFalse(RdbDialect.sqlServer(SqlServerVersion.V2012)
                              .capabilities().supports(DialectCapabilityId.JSON_FUNCTIONS));
        assertTrue(RdbDialect.sqlServer(SqlServerVersion.V2016)
                             .capabilities().supports(DialectCapabilityId.JSON_FUNCTIONS));
    }

    @Test
    void customFactoriesOnlyAdvertiseDeclaredCapabilities() {
        RdbDialect template = RdbDialect.postgresql();
        RdbDialect legacy = RdbDialect.of("custom",
                                           template.schema(),
                                           template.pagination(),
                                           template.upsert(),
                                           template.json());

        assertTrue(legacy.capabilities().ids().isEmpty());
        assertEquals(0, legacy.maxIdentifierLength());
        assertThrows(NullPointerException.class,
                     () -> RdbDialect.of("custom",
                                         template.schema(),
                                         template.pagination(),
                                         template.upsert(),
                                         null));

        DialectCapabilities capabilities = DialectCapabilities.of(
                DialectCapabilityId.NATIVE_JSON,
                DialectCapabilityId.IDENTITY_COLUMNS);
        RdbDialect explicit = RdbDialect.of("custom",
                                                            template.schema(),
                                                            template.pagination(),
                                                            template.upsert(),
                                                            template.json(),
                                                            "2.0",
                                                            capabilities,
                                                            42);

        assertSame(capabilities, explicit.capabilities());
        assertEquals("2.0", explicit.version());
        assertEquals(42, explicit.maxIdentifierLength());
        assertTrue(explicit.supports(DialectCapabilityId.NATIVE_JSON));
        assertFalse(explicit.supports(DialectCapabilityId.SEQUENCES));
        assertThrows(UnsupportedOperationException.class,
                     () -> explicit.capabilities().ids().add(DialectCapabilityId.SEQUENCES));
    }
}
