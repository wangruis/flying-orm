package com.flying.orm.rdb.execution;

import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.internal.InternalApi;

import java.util.Arrays;
import java.util.Objects;
import java.util.List;

/**
 * 解码批量行的可选 SQL 布局和受保护侧索引工作，内部元数据不会绑定给数据库驱动。
 *
 * <p>普通参数行必须与 parameterCount 完全相等。ORM 生成的行可在参数后携带 {@link ProtectedWriteWork}，
 * 布局变化时再附加 {@link Layout}；仅声明支持布局切换的执行器接收后者。两类元数据均在绑定前剥离。
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

    /** 校验自有行；仅在配置有限字节预算时估算，JDBC/R2DBC 复用同一视图。 */
    public static RowView admit(Object[] row, int parameterCount, long maxBufferedBytes, String limitName) {
        Object[] safeRow = Objects.requireNonNull(row, "batch row must not be null");
        if (maxBufferedBytes <= 0L) {
            throw new IllegalArgumentException("batch snapshot byte limit must be positive");
        }
        Objects.requireNonNull(limitName, "batch snapshot limit name must not be null");
        RowView view = decode(safeRow, parameterCount);
        if (maxBufferedBytes == Long.MAX_VALUE) return view;
        long estimatedBytes = estimateRowBytes(view);
        if (estimatedBytes == Long.MAX_VALUE || estimatedBytes > maxBufferedBytes) {
            throw new BatchMemoryLimitExceededException(limitName, maxBufferedBytes, estimatedBytes);
        }
        return new RowView(safeRow, view.parameterCount(), view.work(), estimatedBytes, view.layout());
    }

    /** 在批量准入时解码行形状和工作元数据。 */
    private static RowView decode(Object[] row, int parameterCount) {
        if (row == null || parameterCount < 0) {
            throw invalidShape();
        }
        Layout layout = row.length > 0 && row[row.length - 1] instanceof Layout changed ? changed : null;
        int length = row.length;
        if (layout != null) {
            parameterCount = layout.statement().parameterCount();
            length--;
        }
        if (length == parameterCount) {
            return new RowView(row, parameterCount, null, -1L, layout);
        }
        if (length != parameterCount + 1) {
            throw invalidShape();
        }
        Object metadata = row[parameterCount];
        if (metadata instanceof ProtectedWriteWork work) {
            return new RowView(row, parameterCount, work, -1L, layout);
        }
        throw invalidShape();
    }

    /** 使用已经校验的行视图计算一次预算，不重新解释尾槽元数据。 */
    private static long estimateRowBytes(RowView view) {
        RowView safeView = Objects.requireNonNull(view, "protected batch row view must not be null");
        Object[] safeRow = safeView.row();
        int parameterCount = safeView.parameterCount();
        long total = BatchMemoryBudget.estimateRowBytes(safeRow, parameterCount);
        if (view.layout() != null) total = add(total, 48L + 2L * view.layout().statement().sql().length());
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

    /** 同一输入窗口按相邻 SQL 布局切分，不重排行，也不提前读取下一窗口。 */
    public static int windowEnd(List<RowView> rows, int from) {
        Layout layout = rows.get(from).layout();
        int end = from + 1;
        while (end < rows.size() && Objects.equals(layout, rows.get(end).layout())) end++;
        return end;
    }

    /** 仅为非首行布局附加 SQL 结构；普通同形批量的参数数组保持不变。 */
    public record Layout(SqlStatementPlan statement, List<Class<?>> parameterTypes, boolean generatedKeys) {
        public Layout(SqlStatementPlan statement, List<Class<?>> parameterTypes) { this(statement, parameterTypes, true); }
        public Layout {
            Objects.requireNonNull(statement, "batch row statement must not be null");
            parameterTypes = List.copyOf(parameterTypes);
            if (statement.parameterCount() != parameterTypes.size()) throw invalidShape();
        }

        public Object[] attach(Object[] row) {
            Object[] extended = Arrays.copyOf(row, row.length + 1);
            extended[row.length] = this;
            return extended;
        }
    }

    /** 已校验行的只读内部视图；数组所有权仍属于当前批量执行。 */
    public static final class RowView {

        private final Object[] row;
        private final int parameterCount;
        private final ProtectedWriteWork work;
        private final long estimatedBytes;
        private final Layout layout;

        private RowView(Object[] row,
                        int parameterCount,
                        ProtectedWriteWork work,
                        long estimatedBytes, Layout layout) {
            this.row = row;
            this.parameterCount = parameterCount;
            this.work = work;
            this.estimatedBytes = estimatedBytes;
            this.layout = layout;
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

        public Layout layout() { return layout; }

        public BatchWriteRequest request(BatchWriteRequest original) {
            return layout == null ? original : new BatchWriteRequest(layout.statement(), layout.parameterTypes(),
                    original.rows(), original.options(), original.rowCountPolicy(), layout.generatedKeys()
                            ? original.generatedKeys() : com.flying.orm.rdb.batch.BatchGeneratedKeys.none());
        }

        public long estimatedBytes() {
            return estimatedBytes < 0L ? estimateRowBytes(this) : estimatedBytes;
        }
    }
}
