package com.flying.orm.rdb.dialect;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RdbDialectGeneratedValueFeatureTest {

    @Test
    void reportsGeneratedValueFeaturesThatBuiltInDialectsActuallyRender() {
        assertTrue(RdbDialect.h2().supports(DialectCapabilityId.IDENTITY_COLUMNS));
        assertTrue(RdbDialect.h2().supports(DialectCapabilityId.SEQUENCES));
        assertTrue(RdbDialect.mysql().supports(DialectCapabilityId.IDENTITY_COLUMNS));
        assertFalse(RdbDialect.mysql().supports(DialectCapabilityId.SEQUENCES));
        assertTrue(RdbDialect.postgresql().supports(DialectCapabilityId.IDENTITY_COLUMNS));
        assertTrue(RdbDialect.postgresql().supports(DialectCapabilityId.SEQUENCES));
    }
}
