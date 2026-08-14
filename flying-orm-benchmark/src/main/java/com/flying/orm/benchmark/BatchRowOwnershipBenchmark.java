package com.flying.orm.benchmark;

import com.flying.orm.rdb.execution.BatchRowSnapshotter;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * 比较普通标量批量行在强化所有权边界前后的单行快照成本。
 *
 * <p>baseline 复现原热路径的一次外层数组复制和一次内存估算；hardened 走实际生产快照后再完成分片估算。
 * 两个方法共享相同输入和 JMH 配置，可用于判断安全修复是否让不含可变载荷的常规批量无条件变慢。</p>
 *
 * @author wangr
 * @date 2026-08-13
 * @version v1.0
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class BatchRowOwnershipBenchmark {

    private static final long MAX_BUFFERED_BYTES = 1024L * 1024L;

    private Object[] scalarRow;
    private Object[] binaryRow;

    /** 准备普通标量行和需要复制的二进制行。 */
    @Setup(Level.Trial)
    public void setUp() {
        scalarRow = new Object[]{1L, "user-1", 18, new BigDecimal("10.25"), true,
                LocalDateTime.of(2026, 8, 13, 12, 0), "tenant-1", null};
        binaryRow = new Object[]{1L, new byte[1024], "tenant-1"};
    }

    /** 复现强化所有权前普通标量行的一次容器快照与预算估算。 */
    @Benchmark
    public void baselineScalarOwnership(Blackhole blackhole) {
        Object[] snapshot = Arrays.copyOf(scalarRow, scalarRow.length);
        blackhole.consume(snapshot);
        blackhole.consume(ProtectedBatchRows.estimateRowBytes(snapshot, scalarRow.length));
    }

    /** 测当前生产路径对普通标量行的所有权快照与后续分片估算。 */
    @Benchmark
    public void hardenedScalarOwnership(Blackhole blackhole) {
        BatchRowSnapshotter.Snapshot snapshot = BatchRowSnapshotter.snapshotAndEstimate(
                scalarRow, scalarRow.length, MAX_BUFFERED_BYTES, "benchmark bytes");
        blackhole.consume(snapshot.row());
        blackhole.consume(snapshot.estimatedBytes());
    }

    /** 测二进制载荷必须复制时的受控成本，不把它与零复制旧路径混为普通回归。 */
    @Benchmark
    public void hardenedBinaryOwnership(Blackhole blackhole) {
        BatchRowSnapshotter.Snapshot snapshot = BatchRowSnapshotter.snapshotAndEstimate(
                binaryRow, binaryRow.length, MAX_BUFFERED_BYTES, "benchmark bytes");
        blackhole.consume(snapshot.row());
        blackhole.consume(snapshot.estimatedBytes());
    }
}
