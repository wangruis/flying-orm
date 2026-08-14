package com.flying.orm.benchmark;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataCache;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 元数据缓存基准只测缓存本身，不连真实数据库。
 * 这样结果更干净：如果这里慢，问题就在缓存链路；真实库字典表压测后面单独做。
 *
 * @author wangr
 * @date 2026-07-30
 * @version v1.0
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class MetadataCacheBenchmark {

    @Param({"128"})
    public int tableCount;

    private ReactiveFormMetadataCache cache;

    private AtomicInteger cursor;

    @Setup
    public void setUp() {
        cache = ReactiveFormMetadataReaders.cached(new CountingMetadataReader(),
                                                   new CacheRegionPolicy(true,
                                                                         tableCount * 32L,
                                                                         64,
                                                                         Duration.ofMinutes(5),
                                                                         false),
                                                   MetadataCacheInvalidator.none());
        cursor = new AtomicInteger();
    }

    @Benchmark
    public DynamicForm hotFormRead() {
        return cache.readForm("users", "Users").block();
    }

    @Benchmark
    public DynamicForm manyTableRead() {
        int index = Math.floorMod(cursor.getAndIncrement(), tableCount);
        return cache.readForm("form_" + index, "table_" + index).block();
    }

    @Benchmark
    public DynamicForm readWithInvalidation() {
        int index = Math.floorMod(cursor.getAndIncrement(), tableCount);
        String table = "table_" + index;
        if (index % 32 == 0) {
            cache.invalidate(table);
        }
        return cache.readForm("form_" + index, table).block();
    }

    private static final class CountingMetadataReader implements ReactiveFormMetadataReader {

        private final AtomicInteger formReads = new AtomicInteger();

        private final AtomicInteger tableReads = new AtomicInteger();

        @Override
        public Mono<DynamicForm> readForm(String formId, String table) {
            return Mono.fromSupplier(() -> form(formId, table));
        }

        @Override
        public Mono<DynamicForm> readForm(String formId, String schema, String table) {
            return Mono.fromSupplier(() -> form(formId, schema + "." + table));
        }

        @Override
        public Mono<TableMetadata> readTable(String schema, String table) {
            return Mono.fromSupplier(() -> TableMetadata.builder(schema + "." + table)
                                                        .addColumn(ColumnMetadata.primaryKey(
                                                                "id_" + tableReads.incrementAndGet(),
                                                                "BIGINT"))
                                                        .build());
        }

        private DynamicForm form(String formId, String table) {
            return DynamicForm.builder(formId, table)
                              .addField(DynamicField.primaryKey("id_" + formReads.incrementAndGet(), "BIGINT"))
                              .build();
        }
    }
}
