package com.flying.orm.benchmark;

import com.flying.orm.rdb.result.DynamicRow;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 比较动态表单常见的 8 列结果。构造基准能看每行对象分配差异，读取和遍历基准则确认紧凑布局没有
 * 用明显的 CPU 退化换内存。正式报告应同时开启 JMH GC profiler 查看 alloc.rate.norm。
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class DynamicRowBenchmark {

    private Map<String, Object> source;
    private DynamicRow dynamicRow;
    private LinkedHashMap<String, Object> linkedRow;

    @Setup(Level.Trial)
    public void setUp() {
        source = Map.of("id", 1001L,
                        "name", "Alice",
                        "status", "ACTIVE",
                        "tenant_id", "t-1",
                        "org_id", "o-1",
                        "created_at", 1_700_000_000L,
                        "updated_at", 1_700_000_100L,
                        "version", 3L);
        dynamicRow = DynamicRow.copyOf(source);
        linkedRow = new LinkedHashMap<>(source);
    }

    @Benchmark
    public DynamicRow constructDynamicRow() {
        return DynamicRow.copyOf(source);
    }

    @Benchmark
    public LinkedHashMap<String, Object> constructLinkedHashMap() {
        return new LinkedHashMap<>(source);
    }

    @Benchmark
    public Object readDynamicRowByName() {
        return dynamicRow.get("updated_at");
    }

    @Benchmark
    public Object readLinkedHashMapByName() {
        return linkedRow.get("updated_at");
    }

    @Benchmark
    public int traverseDynamicRow() {
        int hash = 1;
        for (int index = 0; index < dynamicRow.columnCount(); index++) {
            hash = 31 * hash + dynamicRow.columnName(index).hashCode();
            hash = 31 * hash + dynamicRow.value(index).hashCode();
        }
        return hash;
    }

    @Benchmark
    public int traverseLinkedHashMap() {
        int hash = 1;
        for (Map.Entry<String, Object> entry : linkedRow.entrySet()) {
            hash = 31 * hash + entry.getKey().hashCode();
            hash = 31 * hash + entry.getValue().hashCode();
        }
        return hash;
    }
}
