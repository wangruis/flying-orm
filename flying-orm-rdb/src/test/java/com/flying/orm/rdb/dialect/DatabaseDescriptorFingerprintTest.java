package com.flying.orm.rdb.dialect;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DatabaseDescriptorFingerprintTest {

    @Test
    void fingerprintsCapabilitiesAndDescriptorsIndependentlyOfDeclarationOrder() {
        DialectCapabilities firstCapabilities = DialectCapabilities.of(
                DialectCapabilityId.NATIVE_JSON,
                DialectCapabilityId.SEQUENCES);
        DialectCapabilities secondCapabilities = DialectCapabilities.of(
                DialectCapabilityId.SEQUENCES,
                DialectCapabilityId.NATIVE_JSON);

        assertEquals(firstCapabilities, secondCapabilities);
        assertEquals(firstCapabilities.fingerprint(), secondCapabilities.fingerprint());

        DatabaseDescriptor first = DatabaseDescriptor.of(
                "PostgreSQL", "16.4", "postgresql", firstCapabilities);
        DatabaseDescriptor second = DatabaseDescriptor.of(
                "PostgreSQL", "16.4", "postgresql", secondCapabilities);

        assertEquals(first, second);
        assertEquals(first.fingerprint(), second.fingerprint());
        assertEquals(firstCapabilities.fingerprint(), first.capabilityFingerprint());
        assertEquals(64, first.fingerprint().length());
    }

    @Test
    void everyStoredIdentityFactParticipatesInTheDescriptorFingerprint() {
        DialectCapabilities baseCapabilities = DialectCapabilities.of(DialectCapabilityId.SEQUENCES);
        DatabaseDescriptor base = DatabaseDescriptor.of(
                "PostgreSQL", "16.4", "postgresql", baseCapabilities);

        Set<String> fingerprints = Set.of(
                base.fingerprint(),
                DatabaseDescriptor.of("PostgreSQL-compatible", "16.4", "postgresql", baseCapabilities)
                                  .fingerprint(),
                DatabaseDescriptor.of("PostgreSQL", "17.0", "postgresql", baseCapabilities).fingerprint(),
                DatabaseDescriptor.of("PostgreSQL", "16.4", "custom", baseCapabilities).fingerprint(),
                DatabaseDescriptor.of("PostgreSQL", "16.4", "postgresql",
                                      DialectCapabilities.of(DialectCapabilityId.IDENTITY_COLUMNS))
                                  .fingerprint());

        assertEquals(5, fingerprints.size());
    }

    @Test
    void derivesDialectIdentityAndCapabilityFingerprintWithoutConnectionDetails() {
        RdbDialect dialect = RdbDialect.postgresql();
        DatabaseDescriptor descriptor = DatabaseDescriptor.of("PostgreSQL", "16.4", dialect);

        assertEquals("PostgreSQL", descriptor.product());
        assertEquals("16.4", descriptor.version());
        assertEquals(dialect.name(), descriptor.dialectId());
        assertEquals(dialect.capabilities().fingerprint(), descriptor.capabilityFingerprint());
        assertNotEquals(descriptor.capabilityFingerprint(), descriptor.fingerprint());
        assertThrows(IllegalArgumentException.class,
                     () -> DatabaseDescriptor.of("PostgreSQL", " ", dialect));
    }
}
