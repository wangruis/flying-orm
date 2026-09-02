package com.flying.orm.rdb.internal.protection;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.plan.SqlExecutionStatements;
import com.flying.orm.rdb.result.DynamicRow;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Builds portable, bounded owner pre-read statements for one protected batch chunk.
 *
 * @author wangr
 * @version v1.0
 */
@InternalApi
public final class ProtectedOwnerBatchPlan {

    public static final int MAX_ROWS = 500;
    public static final int MAX_PARAMETERS = 2_000;
    private static final String SLOT_COLUMN = "flying_orm_owner_slot";
    private static final long PLAN_OVERHEAD_BYTES = 64L;

    private final String sql;
    private final List<Object> parameters;
    private final List<Integer> rowIndexes;
    private final List<ProtectedWriteWork> works;

    private ProtectedOwnerBatchPlan(String sql,
                                    List<Object> parameters,
                                    List<Integer> rowIndexes,
                                    List<ProtectedWriteWork> works) {
        this.sql = sql;
        this.parameters = Collections.unmodifiableList(new ArrayList<>(parameters));
        this.rowIndexes = List.copyOf(rowIndexes);
        this.works = List.copyOf(works);
    }

    public static Iterable<ProtectedOwnerBatchPlan> plans(
            List<ProtectedBatchRows.RowView> rows, long maxBufferedBytes) {
        List<ProtectedBatchRows.RowView> safeRows = Objects.requireNonNull(
                rows, "protected batch rows must not be null");
        if (maxBufferedBytes <= 0L) {
            throw new IllegalArgumentException("protected owner batch byte limit must be greater than zero");
        }
        return () -> new PlanIterator(safeRows, maxBufferedBytes);
    }

    public String sql() {
        return sql;
    }

    public List<Object> parameters() {
        return parameters;
    }

    public int size() {
        return rowIndexes.size();
    }

    public Match decode(DynamicRow row) {
        DynamicRow safeRow = Objects.requireNonNull(row, "protected owner row must not be null");
        if (safeRow.columnCount() < 2) {
            throw new IllegalStateException("protected owner batch result is missing its row slot");
        }
        Object rawSlot = safeRow.value(safeRow.columnCount() - 1);
        if (!(rawSlot instanceof Number number)) {
            throw new IllegalStateException("protected owner batch result has an invalid row slot");
        }
        long longSlot = exactLong(number);
        if (longSlot < 0L || longSlot >= size()) {
            throw new IllegalStateException("protected owner batch result has an invalid row slot");
        }
        int slot = (int) longSlot;
        ProtectedWriteWork work = works.get(slot);
        if (safeRow.columnCount() != work.ownerFields().size() + 1) {
            throw new IllegalStateException("protected owner batch result column count does not match owner fields");
        }
        return new Match(rowIndexes.get(slot), work.ownerFrom(safeRow), slot);
    }

    public record Match(int rowIndex, java.util.Map<String, Object> owner, int slot) {
    }

    private static long exactLong(Number number) {
        try {
            return new BigDecimal(number.toString()).longValueExact();
        } catch (ArithmeticException | NumberFormatException exception) {
            throw new IllegalStateException(
                    "protected owner batch result has an invalid row slot", exception);
        }
    }

    private static final class PlanIterator implements Iterator<ProtectedOwnerBatchPlan> {

        private final List<ProtectedBatchRows.RowView> rows;
        private final long maxBufferedBytes;
        private int cursor;
        private ProtectedOwnerBatchPlan next;

        private PlanIterator(List<ProtectedBatchRows.RowView> rows, long maxBufferedBytes) {
            this.rows = rows;
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
        public ProtectedOwnerBatchPlan next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            ProtectedOwnerBatchPlan current = next;
            next = null;
            return current;
        }

        private ProtectedOwnerBatchPlan buildNext() {
            while (cursor < rows.size() && !isUpdate(rows.get(cursor).work())) {
                cursor++;
            }
            if (cursor >= rows.size()) {
                return null;
            }
            StringBuilder sql = new StringBuilder();
            List<Object> parameters = new ArrayList<>();
            List<Integer> indexes = new ArrayList<>();
            List<ProtectedWriteWork> works = new ArrayList<>();
            int ownerColumnCount = -1;
            String slotColumn = null;
            String queryShape = null;
            String transportShape = null;
            int queryParameterCount = -1;
            long estimatedBytes = PLAN_OVERHEAD_BYTES;
            while (cursor < rows.size()) {
                ProtectedWriteWork work = rows.get(cursor).work();
                if (!isUpdate(work)) {
                    cursor++;
                    continue;
                }
                SqlRequest query = Objects.requireNonNull(
                        work.ownerQuery(), "protected batch owner query must not be null");
                if (query.bindMarkerStyle() != SqlBindMarkerStyle.CANONICAL) {
                    throw new IllegalArgumentException("protected owner query must use canonical bind markers");
                }
                if (ownerColumnCount >= 0 && ownerColumnCount != work.ownerFields().size()) {
                    break;
                }
                if (slotColumn != null && containsIgnoreCase(work.ownerFields(), slotColumn)) {
                    break;
                }
                if (slotColumn == null) {
                    slotColumn = availableSlotColumn(work.ownerFields());
                }
                if (queryShape != null && (!queryShape.equals(query.sql())
                        || queryParameterCount != query.parameters().size())) {
                    break;
                }
                String querySql = transportShape == null
                        ? SqlExecutionStatements.canonical(query, "") : transportShape;
                String branch = "select flying_owner_" + works.size()
                        + ".*, cast(? as integer) as " + slotColumn
                        + " from (" + querySql + ") flying_owner_" + works.size();
                int addedParameters = 1 + query.parameters().size();
                long branchBytes = saturatingAdd(
                        BatchMemoryBudget.estimateValueBytes(branch),
                        BatchMemoryBudget.estimateValueBytes(query.parameters()));
                boolean exceeds = !works.isEmpty() && (works.size() >= MAX_ROWS
                        || parameters.size() + addedParameters > MAX_PARAMETERS
                        || saturatingAdd(estimatedBytes, branchBytes) > maxBufferedBytes);
                if (exceeds) {
                    break;
                }
                if (works.isEmpty() && (addedParameters > MAX_PARAMETERS
                        || saturatingAdd(estimatedBytes, branchBytes) > maxBufferedBytes)) {
                    throw new IllegalArgumentException("protected owner query exceeds batch safety limits");
                }
                if (!works.isEmpty()) {
                    sql.append(" union all ");
                }
                int slot = works.size();
                sql.append(branch);
                parameters.add(slot);
                parameters.addAll(query.parameters());
                indexes.add(cursor);
                works.add(work);
                ownerColumnCount = work.ownerFields().size();
                queryShape = query.sql();
                transportShape = querySql;
                queryParameterCount = query.parameters().size();
                estimatedBytes = saturatingAdd(estimatedBytes, branchBytes);
                cursor++;
            }
            return new ProtectedOwnerBatchPlan(sql.toString(), parameters, indexes, works);
        }

        private static boolean isUpdate(ProtectedWriteWork work) {
            return work != null && work.kind() == ProtectedWriteWork.Kind.UPDATE;
        }

        private static String availableSlotColumn(List<String> ownerFields) {
            String candidate = SLOT_COLUMN;
            while (containsIgnoreCase(ownerFields, candidate)) {
                candidate += "_";
            }
            return candidate;
        }

        private static boolean containsIgnoreCase(List<String> values, String candidate) {
            return values.stream().anyMatch(value -> value.equalsIgnoreCase(candidate));
        }

        private static long saturatingAdd(long left, long right) {
            return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
        }
    }
}
