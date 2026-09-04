package com.flying.orm.rdb.dialect;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RdbDialectGeneratedValueFeatureTest {

    @Test
    void reportsGeneratedValueFeaturesThatBuiltInDialectsActuallyRender() {
        assertTrue(RdbDialect.h2().supports(DialectFeature.IDENTITY_COLUMNS));
        assertTrue(RdbDialect.h2().supports(DialectFeature.SEQUENCES));
        assertTrue(RdbDialect.mysql().supports(DialectFeature.IDENTITY_COLUMNS));
        assertFalse(RdbDialect.mysql().supports(DialectFeature.SEQUENCES));
        assertTrue(RdbDialect.postgresql().supports(DialectFeature.IDENTITY_COLUMNS));
        assertTrue(RdbDialect.postgresql().supports(DialectFeature.SEQUENCES));
    }
}
