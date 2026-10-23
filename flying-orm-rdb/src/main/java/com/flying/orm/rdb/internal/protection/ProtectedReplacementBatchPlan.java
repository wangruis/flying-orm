package com.flying.orm.rdb.internal.protection;

import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.SqlWriteResult;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * Plans bounded delete-then-insert segments without reordering repeated owner-field replacements.
 *
 * @author wangr
 * @version v1.0
 */
public final class ProtectedReplacementBatchPlan {

    private static final int MAX_OPERATIONS = 500;
    /** Preferred combined binding count; a wider individual replacement occupies its own segment. */
    public static final int MAX_PARAMETERS = 2_000;
    private static final long OPERATION_OVERHEAD_BYTES = 48L;

    private ProtectedReplacementBatchPlan() {
    }

    public static Iterable<Segment> segments(List<? extends Row> rows, long maxBufferedBytes) {
        List<? extends Row> safeRows = Objects.requireNonNull(
                rows, "protected replacement rows must not be null");
        if (maxBufferedBytes <= 0L) {
            throw new IllegalArgumentException("protected replacement byte limit must be greater than zero");
        }
        return () -> new SegmentIterator(safeRows, maxBufferedBytes);
    }

    /** 共享 JDBC/R2DBC 的已准备行状态，不携带连接或执行行为。 */
    public record Row(ProtectedWriteWork work, List<Map<String, Object>> owners) {
        public Row {
            owners = List.copyOf(owners);
        }

        /** 固定真实写入返回的 owner，随后与普通行共用替换规划。 */
        public Row completed(SqlWriteResult result) {
            List<Map<String, Object>> resolved = replacementOwners(result);
            if (resolved.isEmpty()) {
                throw new IllegalStateException("protected batch update owner was not found");
            }
            return new Row(work, resolved);
        }

        /** result 为空时使用已知或已解析的 INSERT 主键；生成键路径传入真实结果。 */
        public List<Map<String, Object>> replacementOwners(SqlWriteResult result) {
            return switch (work.kind()) {
                case INSERT -> owners.isEmpty() ? List.of(work.resolveInsertOwner(
                        result == null ? new SqlWriteResult(1L, List.of()) : result)) : owners;
                case UPSERT -> List.of(work.knownOwner());
                case UPDATE -> owners;
            };
        }
    }

    public record Prepared(List<Row> rows, long maxBufferedBytes) {
        public Prepared {
            rows = List.copyOf(rows);
            if (maxBufferedBytes <= 0L) {
                throw new IllegalArgumentException("protected batch byte limit must be greater than zero");
            }
        }

        public boolean hasOwnerRestrictedUpdates() {
            return rows.stream().map(Row::work).filter(Objects::nonNull)
                    .anyMatch(work -> work.kind() == ProtectedWriteWork.Kind.UPDATE);
        }
    }

    /** 全部普通行无需侧索引状态；混合窗口保持与输入一一对应的行号。 */
    public static List<Row> prepareRows(List<ProtectedBatchRows.RowView> rows) {
        if (rows.stream().noneMatch(row -> row.work() != null)) return List.of();
        List<Row> states = new ArrayList<>(rows.size());
        for (ProtectedBatchRows.RowView row : rows) states.add(new Row(row.work(), List.of()));
        return states;
    }

    public record Insertion(ProtectedWriteWork work,
                            Map<String, Object> owner,
                            ProtectedWriteWork.FieldTokens field) {
    }

    /** 两个执行端共用的纯令牌缓冲；false 表示先执行当前缓冲，再加入同一令牌。 */
    public static final class TokenBuffer {
        private final long maxBufferedBytes;
        private final List<List<Object>> parameters = new ArrayList<>();
        private String sql;
        private long parameterCount;
        private long bufferedBytes;

        public TokenBuffer(long maxBufferedBytes) {
            this.maxBufferedBytes = maxBufferedBytes;
        }

        public boolean add(Insertion insertion, int tokenIndex) {
            ProtectedWriteWork work = insertion.work();
            int width = work.ownerFields().size() + 2;
            if (!parameters.isEmpty() && (!work.insertSql().equals(sql)
                    || parameters.size() >= MAX_OPERATIONS || parameterCount + width > MAX_PARAMETERS)) {
                return false;
            }
            List<Object> view = new AbstractList<>() {
                @Override
                public Object get(int index) {
                    Objects.checkIndex(index, size());
                    return index < width - 2 ? Objects.requireNonNull(
                            insertion.owner().get(work.ownerFields().get(index)),
                            "protected write owner value must not be null")
                            : index == width - 2 ? insertion.field().fieldTag()
                            : insertion.field().ownedToken(tokenIndex);
                }

                @Override
                public int size() {
                    return width;
                }
            };
            long bytes = BatchMemoryBudget.estimateValueBytes(view);
            if (bytes == Long.MAX_VALUE || bytes > maxBufferedBytes) {
                throw new IllegalArgumentException("protected side-index token parameters exceed batch safety limits");
            }
            if (!parameters.isEmpty() && bytes > maxBufferedBytes - bufferedBytes) {
                return false;
            }
            parameters.add(work.sideIndexParameters(insertion.owner(), insertion.field(), tokenIndex));
            sql = work.insertSql();
            parameterCount += width;
            bufferedBytes += bytes;
            return true;
        }

        public String sql() {
            return sql;
        }

        /** 内部消费端复用自有缓冲；JDBC执行返回后清空，R2DBC移交后不再修改。 */
        public List<List<Object>> parameters() {
            return parameters;
        }

        public void clear() {
            parameters.clear();
            parameterCount = 0;
            bufferedBytes = 0;
            sql = null;
        }
    }

    public record Segment(String deleteSql,
                          List<List<Object>> deleteParameterSets,
                          List<Insertion> insertions) {
        public Segment {
            deleteParameterSets = List.copyOf(deleteParameterSets);
            insertions = List.copyOf(insertions);
        }
    }

    private static final class SegmentIterator implements Iterator<Segment> {

        private final OperationIterator operations;
        private final long maxBufferedBytes;
        private Operation pending;
        private Segment next;

        private SegmentIterator(List<? extends Row> rows, long maxBufferedBytes) {
            this.operations = new OperationIterator(rows);
            this.maxBufferedBytes = maxBufferedBytes;
        }

        @Override
        public boolean hasNext() {
            if (next == null) {
                next = buildNext();
            }
            return next != null;
        }

        @Override
        public Segment next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Segment current = next;
            next = null;
            return current;
        }

        private Segment buildNext() {
            Operation first = takeOperation();
            if (first == null) {
                return null;
            }
            boolean replacement = first.deleteParameters() != null;
            String deleteSql = replacement ? first.insertion().work().deleteSql() : null;
            String insertSql = first.insertion().work().insertSql();
            List<List<Object>> deletes = new ArrayList<>();
            List<Insertion> insertions = new ArrayList<>();
            Set<OwnerFieldKey> keys = new HashSet<>();
            int parameterCount = 0;
            long bytes = 0L;
            Operation operation = first;
            while (operation != null) {
                Insertion insertion = operation.insertion();
                boolean nextReplacement = operation.deleteParameters() != null;
                int addedParameters = nextReplacement ? operation.deleteParameters().size() : 0;
                long addedBytes = saturatingAdd(
                        OPERATION_OVERHEAD_BYTES,
                        nextReplacement
                                ? BatchMemoryBudget.estimateValueBytes(operation.deleteParameters()) : 0L);
                boolean incompatible = replacement != nextReplacement
                        || !insertSql.equals(insertion.work().insertSql())
                        || replacement && !deleteSql.equals(insertion.work().deleteSql());
                boolean exceeds = !insertions.isEmpty() && (incompatible
                        || insertions.size() >= MAX_OPERATIONS
                        || (long) parameterCount + addedParameters > MAX_PARAMETERS
                        || saturatingAdd(bytes, addedBytes) > maxBufferedBytes
                        || replacement && keys.contains(operation.key()));
                if (exceeds) {
                    pending = operation;
                    break;
                }
                if (insertions.isEmpty() && saturatingAdd(bytes, addedBytes) > maxBufferedBytes) {
                    throw new IllegalArgumentException(
                            "protected side-index replacement exceeds batch safety limits");
                }
                insertions.add(insertion);
                if (replacement) {
                    deletes.add(operation.deleteParameters());
                    keys.add(operation.key());
                }
                parameterCount += addedParameters;
                bytes = saturatingAdd(bytes, addedBytes);
                operation = takeOperation();
            }
            return new Segment(deleteSql, deletes, insertions);
        }

        private Operation takeOperation() {
            if (pending != null) {
                Operation current = pending;
                pending = null;
                return current;
            }
            return operations.hasNext() ? operations.next() : null;
        }
    }

    private static final class OperationIterator implements Iterator<Operation> {

        private final List<? extends Row> rows;
        private int rowIndex;
        private int ownerIndex;
        private int fieldIndex;
        private List<Map<String, Object>> currentOwners = List.of();
        private ProtectedWriteWork currentWork;
        private Operation next;

        private OperationIterator(List<? extends Row> rows) {
            this.rows = rows;
        }

        @Override
        public boolean hasNext() {
            if (next == null) {
                next = findNext();
            }
            return next != null;
        }

        @Override
        public Operation next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Operation current = next;
            next = null;
            return current;
        }

        private Operation findNext() {
            while (true) {
                if (currentWork != null && ownerIndex < currentOwners.size()) {
                    if (fieldIndex < currentWork.fields().size()) {
                        Map<String, Object> owner = currentOwners.get(ownerIndex);
                        ProtectedWriteWork.FieldTokens field = currentWork.fields().get(fieldIndex++);
                        List<Object> deleteParameters = currentWork.kind() == ProtectedWriteWork.Kind.INSERT
                                ? null : currentWork.sideIndexParameters(owner, field, (byte[]) null);
                        return new Operation(
                                new Insertion(currentWork, owner, field),
                                deleteParameters,
                                new OwnerFieldKey(deleteParameters));
                    }
                    ownerIndex++;
                    fieldIndex = 0;
                    continue;
                }
                if (rowIndex >= rows.size()) {
                    return null;
                }
                Row row = rows.get(rowIndex++);
                currentWork = row.work();
                ownerIndex = 0;
                fieldIndex = 0;
                if (currentWork == null) {
                    continue;
                }
                currentOwners = row.replacementOwners(null);
            }
        }

    }

    private record Operation(Insertion insertion,
                             List<Object> deleteParameters,
                             OwnerFieldKey key) {
    }

    private static final class OwnerFieldKey {

        private final Object[] values;
        private final int hashCode;

        private OwnerFieldKey(List<Object> values) {
            this.values = values == null ? new Object[0] : values.toArray();
            for (int index = 0; index < this.values.length; index++) {
                // 仅规范分段身份；同值数值载体不能绕过重复 owner 检测，实际绑定保持原样。
                Object value = this.values[index];
                if (value instanceof BigDecimal decimal) {
                    this.values[index] = decimal.stripTrailingZeros();
                } else if (value instanceof BigInteger integer) {
                    this.values[index] = new BigDecimal(integer).stripTrailingZeros();
                } else if (value instanceof Byte || value instanceof Short
                        || value instanceof Integer || value instanceof Long) {
                    this.values[index] = BigDecimal.valueOf(((Number) value).longValue()).stripTrailingZeros();
                } else if (value instanceof Float || value instanceof Double) {
                    double floating = ((Number) value).doubleValue();
                    this.values[index] = Double.isFinite(floating)
                            ? BigDecimal.valueOf(floating).stripTrailingZeros() : floating;
                }
            }
            this.hashCode = Arrays.deepHashCode(this.values);
        }

        @Override
        public boolean equals(Object candidate) {
            return candidate instanceof OwnerFieldKey other
                    && Arrays.deepEquals(values, other.values);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }

    private static long saturatingAdd(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }
}
