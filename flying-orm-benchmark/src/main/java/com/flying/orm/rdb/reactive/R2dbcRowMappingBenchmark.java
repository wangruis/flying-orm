package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.result.DynamicRowFactory;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 比较不含 Blob/Clob 的普通 R2DBC 行在引入 LOB 生命周期保护前后的映射成本。
 *
 * @author wangr
 * @date 2026-08-13
 * @version v1.0
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class R2dbcRowMappingBenchmark {

    private Row row;
    private RowMetadata metadata;
    private SqlExecutionOptions options;
    private R2dbcLargeObjectScope largeObjects;
    private DynamicRowFactory baselineMapper;
    private R2dbcLargeObjectRows.Mapper hardenedMapper;

    /** 准备固定八列标量行，排除数据库和驱动 I/O。 */
    @Setup(Level.Trial)
    public void setUp() {
        Object[] values = {1L, "user-1", 18, "10.25", true, "2026-08-13T12:00:00", "tenant-1", null};
        List<? extends ColumnMetadata> columns = java.util.stream.IntStream.range(0, values.length)
                .mapToObj(index -> column("c" + index))
                .toList();
        metadata = metadata(columns);
        row = row(metadata, values);
        options = SqlExecutionOptions.unlimited();
        largeObjects = new R2dbcLargeObjectScope();
        baselineMapper = DynamicRowFactory.from(metadata);
        hardenedMapper = R2dbcLargeObjectRows.mapper(metadata, options, largeObjects);
    }

    /** 复现没有 LOB 生命周期包装时的同步行映射。 */
    @Benchmark
    public DynamicRow baselineNoLobMapping() {
        return baselineMapper.read(row);
    }

    /** 测当前生产路径在普通无 LOB 行上的映射成本。 */
    @Benchmark
    public DynamicRow hardenedNoLobMapping() {
        return (DynamicRow) hardenedMapper.mapValue(row);
    }

    private static ColumnMetadata column(String name) {
        return new ColumnMetadata() {
            @Override
            public R2dbcType getType() {
                return R2dbcType.VARCHAR;
            }

            @Override
            public String getName() {
                return name;
            }
        };
    }

    private static RowMetadata metadata(List<? extends ColumnMetadata> columns) {
        return new RowMetadata() {
            @Override
            public ColumnMetadata getColumnMetadata(int index) {
                return columns.get(index);
            }

            @Override
            public ColumnMetadata getColumnMetadata(String name) {
                return columns.stream().filter(column -> column.getName().equals(name)).findFirst().orElseThrow();
            }

            @Override
            public List<? extends ColumnMetadata> getColumnMetadatas() {
                return columns;
            }
        };
    }

    private static Row row(RowMetadata metadata, Object[] values) {
        return new Row() {
            @Override
            public RowMetadata getMetadata() {
                return metadata;
            }

            @Override
            public <T> T get(int index, Class<T> type) {
                return type.cast(values[index]);
            }

            @Override
            public <T> T get(String name, Class<T> type) {
                throw new UnsupportedOperationException("indexed row access expected");
            }
        };
    }
}
