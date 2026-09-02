package com.flying.orm.rdb.internal.protection;

import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.internal.InternalApi;

import java.util.ArrayList;
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
@InternalApi
public final class ProtectedReplacementBatchPlan {

    public static final int MAX_OPERATIONS = 500;
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

    public interface Row {
        ProtectedWriteWork work();

        List<Map<String, Object>> owners();
    }

    public record Insertion(ProtectedWriteWork work,
                            Map<String, Object> owner,
                            ProtectedWriteWork.FieldTokens field) {
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
                        || parameterCount + addedParameters > MAX_PARAMETERS
                        || saturatingAdd(bytes, addedBytes) > maxBufferedBytes
                        || replacement && keys.contains(operation.key()));
                if (exceeds) {
                    pending = operation;
                    break;
                }
                if (insertions.isEmpty() && (addedParameters > MAX_PARAMETERS
                        || saturatingAdd(bytes, addedBytes) > maxBufferedBytes)) {
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
                currentOwners = owners(row, currentWork);
                if (currentOwners.isEmpty()) {
                    throw new IllegalStateException("protected batch update owner was not found");
                }
            }
        }

        private static List<Map<String, Object>> owners(Row row, ProtectedWriteWork work) {
            return switch (work.kind()) {
                case INSERT -> List.of(work.resolveInsertOwner(new SqlWriteResult(1L, List.of())));
                case UPSERT -> List.of(work.knownOwner());
                case UPDATE -> row.owners();
            };
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
