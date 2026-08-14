package com.flying.orm.rdb.execution;

import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.MutableValueSnapshots;

import java.util.Arrays;
import java.util.Objects;

/**
 * 在批量参数行尾携带受保护侧索引工作描述或稳定回执身份，并确保内部元数据不会绑定给数据库驱动。
 *
 * <p>普通批量行仍必须与 parameterCount 完全相等。只有 flying-orm 自己生成、且最后一个元素为
 * {@link ProtectedWriteWork} 或本类型创建的元数据行才允许多一个内部槽位；JDBC/R2DBC 执行器会在绑定前剥离它。
 * 侧索引工作在同一连接事务内维护 CONTAINS 令牌；回执身份则替代随机密文参与摘要，不进入 SQL。</p>
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
        return extend(parameters, work, null);
    }

    /**
     * 把可绑定参数、可选侧索引工作和可选回执参数身份组合成内部批量行。
     *
     * <p>receiptParameters 必须与真实参数位置完全一致；只有随机密文位置应替换为稳定 HMAC 身份。</p>
     */
    public static Object[] extend(Object[] parameters,
                                  ProtectedWriteWork work,
                                  Object[] receiptParameters) {
        Object[] safeParameters = Objects.requireNonNull(
                parameters, "protected batch parameters must not be null");
        if (work == null && receiptParameters == null) {
            throw new IllegalArgumentException("protected batch metadata must not be empty");
        }
        if (work != null && work.writeRequest().parameters().size() != safeParameters.length) {
            throw new IllegalArgumentException("protected batch parameter count must match write work");
        }
        if (receiptParameters != null && receiptParameters.length != safeParameters.length) {
            throw new IllegalArgumentException("protected batch receipt parameter count must match parameters");
        }
        Object[] extended = Arrays.copyOf(safeParameters, safeParameters.length + 1);
        extended[safeParameters.length] = receiptParameters == null
                ? work : new Metadata(work, receiptParameters);
        return extended;
    }

    /** 返回只包含可绑定值的数组；普通行不额外复制。 */
    public static Object[] parameters(Object[] row, int parameterCount) {
        Object[] safeRow = requireShape(row, parameterCount);
        return safeRow.length == parameterCount ? safeRow : Arrays.copyOf(safeRow, parameterCount);
    }

    /** 返回行尾工作描述；普通批量行返回 null。 */
    public static ProtectedWriteWork work(Object[] row, int parameterCount) {
        Object[] safeRow = requireShape(row, parameterCount);
        if (safeRow.length == parameterCount) {
            return null;
        }
        Object metadata = safeRow[parameterCount];
        return metadata instanceof ProtectedWriteWork work ? work : ((Metadata) metadata).work();
    }

    /**
     * 返回回执摘要使用的稳定参数行；没有专用身份时返回 null，调用方应复用真实参数行。
     */
    public static Object[] receiptParameters(Object[] row, int parameterCount) {
        Object[] safeRow = requireShape(row, parameterCount);
        if (safeRow.length == parameterCount || safeRow[parameterCount] instanceof ProtectedWriteWork) {
            return null;
        }
        return ((Metadata) safeRow[parameterCount]).receiptParameters();
    }

    /** 按参数、owner、查询和令牌内容估算扩展行预算，避免内部元数据绕过批量内存上限。 */
    public static long estimateRowBytes(Object[] row, int parameterCount) {
        long total = BatchMemoryBudget.estimateRowBytes(parameters(row, parameterCount));
        ProtectedWriteWork work = work(row, parameterCount);
        if (work == null) {
            return addReceiptBytes(total, row, parameterCount);
        }
        total = add(total, BatchMemoryBudget.estimateValueBytes(work.knownOwnerInternal()));
        total = add(total, BatchMemoryBudget.estimateValueBytes(work.writeRequest().parameters()));
        if (work.ownerQuery() != null) {
            total = add(total, BatchMemoryBudget.estimateValueBytes(work.ownerQuery().parameters()));
        }
        for (ProtectedWriteWork.FieldTokens field : work.fields()) {
            total = add(total, BatchMemoryBudget.estimateValueBytes(field.fieldTag()));
            total = add(total, BatchMemoryBudget.estimateValueBytes(field.tokensInternal()));
        }
        return addReceiptBytes(total, row, parameterCount);
    }

    private static long addReceiptBytes(long total, Object[] row, int parameterCount) {
        Object[] safeRow = requireShape(row, parameterCount);
        if (safeRow.length == parameterCount
                || safeRow[parameterCount] instanceof ProtectedWriteWork) {
            return total;
        }
        Object[] receipt = ((Metadata) safeRow[parameterCount]).rawReceiptParameters();
        for (int index = 0; index < receipt.length; index++) {
            if (receipt[index] != safeRow[index]) {
                total = add(total, BatchMemoryBudget.estimateValueBytes(receipt[index]));
            }
        }
        return total;
    }

    private static Object[] requireShape(Object[] row, int parameterCount) {
        if (row == null || parameterCount < 0) {
            throw invalidShape();
        }
        if (row.length == parameterCount) {
            return row;
        }
        if (row.length == parameterCount + 1
                && (row[parameterCount] instanceof ProtectedWriteWork
                    || row[parameterCount] instanceof Metadata)) {
            return row;
        }
        throw invalidShape();
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

    /** 复制参数容器以及其中直接数组值的完整数组图，避免稳定二进制身份在冷执行前被改写。 */
    private static Object[] snapshotReceiptParameters(Object[] parameters) {
        Object[] snapshot = parameters.clone();
        for (int index = 0; index < snapshot.length; index++) {
            snapshot[index] = MutableValueSnapshots.arrayGraph(snapshot[index]);
        }
        return snapshot;
    }

    /** 内部尾槽元数据；owned 工厂只接收已经由同一身份图快照器冻结的值。 */
    static final class Metadata {

        private final ProtectedWriteWork work;
        private final Object[] receiptParameters;

        Metadata(ProtectedWriteWork work, Object[] receiptParameters) {
            this(work, receiptParameters, true);
        }

        private Metadata(ProtectedWriteWork work, Object[] receiptParameters, boolean snapshot) {
            this.work = work;
            Object[] safeParameters = Objects.requireNonNull(
                    receiptParameters, "protected batch receipt parameters must not be null");
            this.receiptParameters = snapshot ? snapshotReceiptParameters(safeParameters) : safeParameters;
        }

        static Metadata owned(ProtectedWriteWork work, Object[] receiptParameters) {
            return new Metadata(work, receiptParameters, false);
        }

        ProtectedWriteWork work() {
            return work;
        }

        Object[] rawReceiptParameters() {
            return receiptParameters;
        }

        Object[] receiptParameters() {
            return BatchRowSnapshotter.snapshot(receiptParameters);
        }
    }
}
