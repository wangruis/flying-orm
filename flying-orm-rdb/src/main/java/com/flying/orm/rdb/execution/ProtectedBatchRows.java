package com.flying.orm.rdb.execution;

import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.internal.InternalApi;

import java.util.Arrays;
import java.util.Objects;

/**
 * 在批量参数行尾携带受保护侧索引工作描述，并确保内部元数据不会绑定给数据库驱动。
 *
 * <p>普通批量行仍必须与 parameterCount 完全相等。只有 flying-orm 自己生成、且最后一个元素为
 * {@link ProtectedWriteWork} 才允许多一个内部槽位；JDBC/R2DBC 执行器会在绑定前剥离它。
 * 侧索引工作在同一借用连接上维护 CONTAINS 令牌，多语句一致性由上层决定和保证。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
@InternalApi
public final class ProtectedBatchRows {

    private ProtectedBatchRows() {
    }

    /** 把参数数组和不可变工作描述组合成内部批量行。 */
    public static Object[] extend(Object[] parameters, ProtectedWriteWork work) {
        Object[] safeParameters = Objects.requireNonNull(
                parameters, "protected batch parameters must not be null");
        ProtectedWriteWork safeWork = Objects.requireNonNull(
                work, "protected batch write work must not be null");
        if (OwnedBindableValues.ownedValues(safeWork.writeRequest().parameters()).size()
                != safeParameters.length) {
            throw new IllegalArgumentException("protected batch parameter count must match write work");
        }
        Object[] extended = Arrays.copyOf(safeParameters, safeParameters.length + 1);
        extended[safeParameters.length] = safeWork;
        return extended;
    }

    /** 返回只包含可绑定值的数组；普通行不额外复制。 */
    public static Object[] parameters(Object[] row, int parameterCount) {
        Object[] safeRow = decode(row, parameterCount).row();
        return safeRow.length == parameterCount ? safeRow : Arrays.copyOf(safeRow, parameterCount);
    }

    /** 返回行尾工作描述；普通批量行返回 null。 */
    public static ProtectedWriteWork work(Object[] row, int parameterCount) {
        return decode(row, parameterCount).work();
    }

    /** 按参数、owner、查询和令牌内容估算扩展行预算，避免内部元数据绕过批量内存上限。 */
    public static long estimateRowBytes(Object[] row, int parameterCount) {
        return estimateRowBytes(decode(row, parameterCount));
    }

    /** 一次完成行形状、工作元数据解码，供 JDBC/R2DBC 内核复用。 */
    @InternalApi
    public static RowView decode(Object[] row, int parameterCount) {
        if (row == null || parameterCount < 0) {
            throw invalidShape();
        }
        if (row.length == parameterCount) {
            return new RowView(row, parameterCount, null, -1L);
        }
        if (row.length != parameterCount + 1) {
            throw invalidShape();
        }
        Object metadata = row[parameterCount];
        if (metadata instanceof ProtectedWriteWork work) {
            return new RowView(row, parameterCount, work, -1L);
        }
        throw invalidShape();
    }

    /** 使用已经校验的行视图计算一次预算，不重新解释尾槽元数据。 */
    @InternalApi
    public static long estimateRowBytes(RowView view) {
        RowView safeView = Objects.requireNonNull(view, "protected batch row view must not be null");
        Object[] safeRow = safeView.row();
        int parameterCount = safeView.parameterCount();
        long total = BatchMemoryBudget.estimateRowBytes(safeRow, parameterCount);
        ProtectedWriteWork work = safeView.work();
        if (work == null) {
            return total;
        }
        total = add(total, BatchMemoryBudget.estimateValueBytes(work.knownOwnerInternal()));
        total = add(total, BatchMemoryBudget.estimateValueBytes(
                OwnedBindableValues.ownedValues(work.writeRequest().parameters())));
        if (work.ownerQuery() != null) {
            total = add(total, BatchMemoryBudget.estimateValueBytes(
                    OwnedBindableValues.ownedValues(work.ownerQuery().parameters())));
        }
        for (ProtectedWriteWork.FieldTokens field : work.fields()) {
            total = add(total, BatchMemoryBudget.estimateValueBytes(field.fieldTag()));
            total = add(total, field.estimatedTokenBytes());
        }
        return total;
    }

    private static IllegalArgumentException invalidShape() {
        return new IllegalArgumentException("batch row parameter count does not match request parameter count");
    }

    static long add(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }

    static long multiply(long left, long right) {
        return left != 0L && right > Long.MAX_VALUE / left ? Long.MAX_VALUE : left * right;
    }

    /** 已校验行的只读内部视图；数组所有权仍属于当前批量执行。 */
    @InternalApi
    public static final class RowView {

        private final Object[] row;
        private final int parameterCount;
        private final ProtectedWriteWork work;
        private final long estimatedBytes;

        private RowView(Object[] row,
                        int parameterCount,
                        ProtectedWriteWork work,
                        long estimatedBytes) {
            this.row = row;
            this.parameterCount = parameterCount;
            this.work = work;
            this.estimatedBytes = estimatedBytes;
        }

        public Object[] row() {
            return row;
        }

        public int parameterCount() {
            return parameterCount;
        }

        public ProtectedWriteWork work() {
            return work;
        }

        public long estimatedBytes() {
            if (estimatedBytes < 0L) {
                throw new IllegalStateException("protected batch row view has no byte estimate");
            }
            return estimatedBytes;
        }

        RowView withEstimatedBytes(long bytes) {
            return new RowView(row, parameterCount, work, bytes);
        }
    }
}
