package com.flying.orm.rdb.metadata;

import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * 把框架无关的缓存快照展开成稳定指标名。
 *
 * <p>上层可以把 sink 接到 Micrometer、OpenTelemetry、Prometheus 或自己的监控 SDK，flying-orm
 * 不因此依赖任何监控框架。指标是当前 reader 生命周期内的累计值，采集周期和 rate 计算由上层决定。</p>
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class MetadataCacheMetricsBridge {

    private static final String PREFIX = "flying.orm.metadata.cache.";

    private MetadataCacheMetricsBridge() {
    }

    public static void export(MetadataCacheSnapshot snapshot, BiConsumer<String, Number> sink) {
        MetadataCacheSnapshot safeSnapshot = Objects.requireNonNull(snapshot,
                                                                     "metadata cache snapshot must not be null");
        BiConsumer<String, Number> safeSink = Objects.requireNonNull(sink, "metadata metrics sink must not be null");
        exportRegion("forms", safeSnapshot.forms(), safeSink);
        exportRegion("tables", safeSnapshot.tables(), safeSink);
        exportRegion(null, safeSnapshot.combined(), safeSink);
    }

    private static void exportRegion(String region,
                                     MetadataCacheSnapshot.Region metrics,
                                     BiConsumer<String, Number> sink) {
        String suffix = region == null ? "" : "." + region;
        sink.accept(PREFIX + "entries" + suffix, metrics.entries());
        sink.accept(PREFIX + "hits" + suffix, metrics.hitCount());
        sink.accept(PREFIX + "misses" + suffix, metrics.missCount());
        sink.accept(PREFIX + "load.success" + suffix, metrics.loadSuccessCount());
        sink.accept(PREFIX + "load.failure" + suffix, metrics.loadFailureCount());
        sink.accept(PREFIX + "load.nanos" + suffix, metrics.totalLoadTimeNanos());
        sink.accept(PREFIX + "evictions" + suffix, metrics.evictionCount());
        sink.accept(PREFIX + "hit.ratio" + suffix, metrics.hitRate());
    }
}
