package com.flying.orm.rdb.metadata;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetadataCacheMetricsBridgeTest {

    @Test
    void exportsStableMetricNamesWithoutDependingOnMonitoringFramework() {
        MetadataCacheSnapshot.Region forms = new MetadataCacheSnapshot.Region(2, 5, 1, 4, 0, 10, 0, 0);
        MetadataCacheSnapshot.Region tables = new MetadataCacheSnapshot.Region(3, 7, 2, 6, 1, 20, 0, 0);
        Map<String, Number> metrics = new LinkedHashMap<>();

        MetadataCacheMetricsBridge.export(new MetadataCacheSnapshot(forms, tables), metrics::put);

        assertEquals(5L, metrics.get("flying.orm.metadata.cache.entries"));
        assertEquals(12L, metrics.get("flying.orm.metadata.cache.hits"));
        assertEquals(3L, metrics.get("flying.orm.metadata.cache.misses"));
        assertEquals(30L, metrics.get("flying.orm.metadata.cache.load.nanos"));
    }

    @Test
    void combinedStatisticsSaturateInsteadOfOverflowing() {
        MetadataCacheSnapshot.Region maximum = new MetadataCacheSnapshot.Region(
                Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE,
                Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
        MetadataCacheSnapshot.Region one = new MetadataCacheSnapshot.Region(1, 1, 1, 1, 1, 1, 1, 1);

        MetadataCacheSnapshot.Region combined = new MetadataCacheSnapshot(maximum, one).combined();

        assertEquals(Long.MAX_VALUE, combined.entries());
        assertEquals(Long.MAX_VALUE, combined.requestCount());
        assertEquals(Long.MAX_VALUE, combined.loadCount());
        assertEquals(0.5D, maximum.hitRate());
    }
}
