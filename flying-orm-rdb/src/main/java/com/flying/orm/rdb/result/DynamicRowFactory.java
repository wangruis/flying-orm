package com.flying.orm.rdb.result;

import com.flying.orm.rdb.internal.InternalApi;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import java.util.Objects;

/**
 * 把同一个 R2DBC {@code Result} 里的驱动行压缩成 {@link DynamicRow}。
 *
 * <p>工厂创建时只读取一次列元数据，之后每行按列下标读取值。这样一批结果只保存一份列名和索引，
 * 每行只分配一个 {@code Object[]} 和一个很薄的 DynamicRow 外壳。工厂本身只含不可变状态，
 * 即使某个驱动并发调用行映射函数也可以安全复用。</p>
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
@InternalApi
public final class DynamicRowFactory {

    private final RowLayout layout;

    private DynamicRowFactory(RowLayout layout) {
        this.layout = layout;
    }

    /**
     * 从当前结果集的列标签创建工厂。重复列标签会在第一行发布前直接报错，避免按列名读取时悄悄覆盖值。
     */
    public static DynamicRowFactory from(RowMetadata metadata) {
        RowMetadata safeMetadata = Objects.requireNonNull(metadata, "row metadata must not be null");
        return new DynamicRowFactory(RowLayout.from(safeMetadata));
    }

    /**
     * 读取一行。值数组在这里新建并立即交给只读 DynamicRow，发布后没有任何代码再持有可写引用。
     */
    public DynamicRow read(Row row) {
        Row safeRow = Objects.requireNonNull(row, "row must not be null");
        Object[] values = new Object[layout.size()];
        for (int index = 0; index < values.length; index++) {
            values[index] = safeRow.get(index);
        }
        return DynamicRow.owned(layout, values);
    }
}
