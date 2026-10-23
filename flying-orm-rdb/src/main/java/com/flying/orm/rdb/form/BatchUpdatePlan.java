package com.flying.orm.rdb.form;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Objects;

/**
 * 保存一种更新布局的 SQL、参数类型、行数契约和 bind marker 风格；相邻同形行复用计划。
 *
 * <p>普通更新使用 {@link BatchRowCountPolicy#ANY}；乐观锁更新使用
 * {@link BatchRowCountPolicy#EXACTLY_ONE}，逐行报告不符合预期的影响数。</p>
 */
record BatchUpdatePlan(SqlStatementPlan statement,
                       List<Class<?>> parameterTypes,
                       BatchRowCountPolicy rowCountPolicy) {

    BatchUpdatePlan {
        statement = Objects.requireNonNull(statement, "batch update statement must not be null");
        parameterTypes = List.copyOf(Objects.requireNonNull(parameterTypes,
                                                            "batch update parameter types must not be null"));
        rowCountPolicy = Objects.requireNonNull(rowCountPolicy, "batch update row count policy must not be null");
    }

    Object[] parameters(SqlRequest request, long rowIndex, OptimisticLockOptions lock) {
        if ((lock == null) != (rowCountPolicy == BatchRowCountPolicy.ANY)) {
            throw new IllegalArgumentException("batch update row [" + rowIndex + "] has a different optimistic lock contract");
        }
        SqlRequest safeRequest = Objects.requireNonNull(request, "batch update request must not be null");
        // 条件节点数量变化也会改变 SQL，占位符即使总数碰巧相同也不能混入同一批。
        if (!matches(safeRequest)) {
            throw new IllegalArgumentException("batch update row [" + rowIndex + "] has a different SQL shape");
        }
        if (safeRequest.statement().parameterCount() != parameterTypes.size()) {
            throw new IllegalArgumentException("batch update row [" + rowIndex + "] has a different parameter count");
        }
        return OwnedBindableValues.ownedValues(safeRequest.parameters()).toArray();
    }

    BatchWriteRequest request(Publisher<Object[]> rows, BatchWriteOptions options) {
        return new BatchWriteRequest(statement, parameterTypes, rows, options,
                rowCountPolicy, BatchGeneratedKeys.none());
    }

    private boolean matches(SqlRequest request) {
        return statement.sql().equals(request.sql()) && statement.bindMarkerStyle() == request.bindMarkerStyle();
    }

    /** 每次执行独占，只保留首个与当前计划；后续行的锁契约仍须一致。 */
    static final class Cursor {
        private final BatchUpdatePlan first;
        private final boolean supportsLayouts;
        private BatchUpdatePlan current;
        private ProtectedBatchRows.Layout layout;

        Cursor(BatchUpdatePlan first, boolean supportsLayouts) {
            this.first = first;
            this.current = first;
            this.supportsLayouts = supportsLayouts;
        }

        Object[] parameters(FormDataSqlRenderer renderer, FormScopeSupport.PreparedBatchUpdate update, long index) {
            if (!current.matches(update.request())) {
                if (!supportsLayouts) throw new UnsupportedOperationException("batch executor does not support mixed row layouts");
                if (first.matches(update.request())) current = first;
                else current = renderer.batchUpdatePlan(update.form(), update.values(),
                        update.lock(), update.request());
                if (current.rowCountPolicy() != first.rowCountPolicy()) {
                    throw new IllegalArgumentException("batch update row [" + index + "] has a different optimistic lock contract");
                }
                layout = current == first ? null : new ProtectedBatchRows.Layout(current.statement(), current.parameterTypes());
            }
            return current.parameters(update.request(), index, update.lock());
        }

        Object[] attach(Object[] parameters) { return layout == null ? parameters : layout.attach(parameters); }
    }
}
