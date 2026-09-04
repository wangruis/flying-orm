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
    void exposesTheExistingFiveDialectFeatureMatrixThroughStableCapabilityIds() {
        Map<RdbDialect, Set<DialectFeature>> expected = Map.of(
                RdbDialect.h2(), Set.of(DialectFeature.IDENTITY_COLUMNS,
                                        DialectFeature.SEQUENCES,
                                        DialectFeature.MERGE_UPSERT),
                RdbDialect.mysql(), Set.of(DialectFeature.IDENTITY_COLUMNS,
                                           DialectFeature.MYSQL_RELATIONAL_METADATA),
                RdbDialect.postgresql(), Set.of(DialectFeature.JSON_FUNCTIONS,
                                                DialectFeature.NATIVE_JSON,
                                                DialectFeature.NATIVE_BOOLEAN,
                                                DialectFeature.LARGE_OBJECTS,
                                                DialectFeature.IDENTITY_COLUMNS,
                                                DialectFeature.SEQUENCES,
                                                DialectFeature.POSTGRESQL_VECTOR),
                RdbDialect.oracle(), Set.of(DialectFeature.OFFSET_FETCH_PAGINATION,
                                           DialectFeature.MERGE_UPSERT,
                                           DialectFeature.IDENTITY_COLUMNS,
                                           DialectFeature.SEQUENCES,
                                           DialectFeature.JSON_FUNCTIONS,
                                           DialectFeature.LARGE_OBJECTS),
                RdbDialect.sqlServer(), Set.of(DialectFeature.OFFSET_FETCH_PAGINATION,
                                              DialectFeature.MERGE_UPSERT,
                                              DialectFeature.IDENTITY_COLUMNS,
                                              DialectFeature.SEQUENCES,
                                              DialectFeature.JSON_FUNCTIONS,
                                              DialectFeature.LARGE_OBJECTS));

        for (Map.Entry<RdbDialect, Set<DialectFeature>> entry : expected.entrySet()) {
            for (DialectFeature feature : DialectFeature.values()) {
                boolean supported = entry.getValue().contains(feature);
                assertEquals(supported, entry.getKey().supports(feature));
                assertEquals(supported,
                             entry.getKey().capabilities().supports(DialectCapabilityId.from(feature)));
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
    void legacyFactoriesFailClosedAndTheExplicitFactoryCarriesDeclaredFacts() {
        RdbDialect template = RdbDialect.postgresql();
        RdbDialect legacy = RdbDialect.of("custom",
                                           template.schema(),
                                           template.pagination(),
                                           template.upsert(),
                                           template.json());

        assertTrue(legacy.capabilities().ids().isEmpty());
        assertEquals(0, legacy.maxIdentifierLength());
        // This call must remain source-compatible and unambiguous even though null is rejected at runtime.
        assertThrows(NullPointerException.class,
                     () -> RdbDialect.of("custom",
                                         template.schema(),
                                         template.pagination(),
                                         template.upsert(),
                                         null));

        DialectCapabilities capabilities = DialectCapabilities.of(
                DialectCapabilityId.NATIVE_JSON,
                DialectCapabilityId.IDENTITY_COLUMNS);
        RdbDialect explicit = RdbDialect.ofWithCapabilities("custom",
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
        assertTrue(explicit.supports(DialectFeature.NATIVE_JSON));
        assertFalse(explicit.supports(DialectFeature.SEQUENCES));
        assertThrows(UnsupportedOperationException.class,
                     () -> explicit.capabilities().ids().add(DialectCapabilityId.SEQUENCES));
    }
}
