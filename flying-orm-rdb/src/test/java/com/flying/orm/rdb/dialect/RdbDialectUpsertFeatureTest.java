package com.flying.orm.rdb.dialect;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class RdbDialectUpsertFeatureTest {

    @Test
    void h2ReportsItsMergeUpsertCapability() {
        assertTrue(RdbDialect.h2().supports(DialectFeature.MERGE_UPSERT));
    }
}
