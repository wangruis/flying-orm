package com.flying.orm.rdb.form;

import com.flying.orm.core.internal.value.BindableValueSnapshots;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.codec.SqlTypedValue;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.internal.mapping.RepositoryUpsertValues;
import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * BatchInsertPlan 保存一次批量插入里可以复用的 SQL 和字段布局。
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
record BatchInsertPlan(SqlStatementPlan statement,
                       BatchColumnLayout columnLayout,
                       Object[] firstParameters,
                       List<Class<?>> parameterTypes,
                       List<Object> predicateParameters,
                       int predicateParameterIndex) {

    BatchInsertPlan {
        statement = Objects.requireNonNull(statement, "batch insert statement must not be null");
        columnLayout = Objects.requireNonNull(columnLayout, "batch column layout must not be null");
        firstParameters = Objects.requireNonNull(
                firstParameters, "batch first parameters must not be null");
        parameterTypes = List.copyOf(Objects.requireNonNull(parameterTypes,
                                                            "batch insert parameter types must not be null"));
        // 首行拥有原参数；计划保留独立模板，不能被任意一行的执行器消费所改变。
        predicateParameters = predicateParameters.isEmpty() ? List.of()
                : predicateParameters.stream().map(BatchInsertPlan::snapshotPredicateValue).toList();
        if (columnLayout.parameterCount() + predicateParameters.size() != parameterTypes.size()
                || parameterTypes.size() != firstParameters.length
                || predicateParameterIndex < 0 || predicateParameterIndex > columnLayout.parameterCount()) {
            throw new IllegalArgumentException("batch insert parameter type count must match layout size");
        }
    }

    Object[] parameters(Map<String, Object> row, long rowIndex) {
        if (predicateParameters.isEmpty()) {
            return columnLayout.parameters(row, rowIndex);
        }
        // 每行只分配最终参数数组。MySQL 把 Scope 放在 UPDATE-only 参数前，其余方言直接追加。
        Object[] parameters = new Object[parameterTypes.size()];
        columnLayout.writeParameters(row, rowIndex, parameters);
        int tail = columnLayout.parameterCount() - predicateParameterIndex;
        if (tail > 0) {
            System.arraycopy(parameters, predicateParameterIndex, parameters,
                             predicateParameterIndex + predicateParameters.size(), tail);
        }
        for (int index = 0; index < predicateParameters.size(); index++) {
            parameters[predicateParameterIndex + index] = snapshotPredicateValue(predicateParameters.get(index));
        }
        return parameters;
    }

    private static Object snapshotPredicateValue(Object value) {
        if (value instanceof SqlTypedValue typed) {
            Object payload = BindableValueSnapshots.immutableValue(typed.value());
            return payload == typed.value() ? typed : new SqlTypedValue(typed.kind(), payload);
        }
        return BindableValueSnapshots.immutableValue(value);
    }

    String sql() {
        return statement.sql();
    }

    /** 需要列与绑定位置的单条保护写入复用同一计划，不再次编码主键。 */
    SqlRequest singleRequest() {
        for (int index = 0; index < firstParameters.length; index++) {
            if (firstParameters[index] == null) firstParameters[index] =
                    new SqlNullParameter(parameterTypes.get(index));
        }
        return new SqlRequest(statement, OwnedBindableValues.publishOwned(Arrays.asList(firstParameters)));
    }

    /** 主键 upsert 中缺失数据库生成键的行直接 INSERT，其余行仍使用原生冲突更新。 */
    static boolean generatedInsert(BatchSpec spec, Map<String, Object> row) {
        return spec.generatedKeys().required() && !row.containsKey(spec.generatedKeys().columnName());
    }

    SqlBindMarkerStyle bindMarkerStyle() {
        return statement.bindMarkerStyle();
    }

    BatchWriteRequest request(Publisher<Object[]> rows, BatchWriteOptions options) {
        return request(rows, options, BatchGeneratedKeys.none());
    }

    BatchWriteRequest request(Publisher<Object[]> rows, BatchWriteOptions options,
                              BatchGeneratedKeys generatedKeys) {
        return new BatchWriteRequest(statement, parameterTypes, rows, options, BatchRowCountPolicy.ANY,
                Objects.requireNonNull(generatedKeys, "batch generated keys must not be null"));
    }

    /** 每次批量独占，仅保留首个 SQL 和当前字段布局，不缓存输入历史或重排行。 */
    static final class Cursor {
        private final BatchInsertPlan first;
        private final boolean supportsLayouts;
        private Map<String, Object> source;
        private BatchInsertPlan current;
        private ProtectedBatchRows.Layout layout;
        private ProtectedBatchRows.Layout withoutGeneratedKeys;

        Cursor(Map<String, Object> source, BatchInsertPlan first, boolean supportsLayouts) {
            this.source = source;
            this.first = first;
            this.current = first;
            this.supportsLayouts = supportsLayouts;
        }

        boolean matches(Map<String, Object> row) {
            if (current.columnLayout().hasExpressions() || !RepositoryUpsertValues.sameLayout(source, row)) return false;
            for (Object value : row.values()) {
                if (value instanceof ValueExpression) return false;
            }
            return true;
        }

        void select(Map<String, Object> row, BatchInsertPlan plan) {
            current = plan;
            source = row;
            withoutGeneratedKeys = null;
            layout = current.sql().equals(first.sql()) && current.bindMarkerStyle() == first.bindMarkerStyle()
                    && current.parameterTypes().equals(first.parameterTypes()) ? null
                    : new ProtectedBatchRows.Layout(current.statement(), current.parameterTypes());
        }

        BatchInsertPlan plan() { return current; }

        Object[] attach(Object[] parameters) {
            return attach(parameters, true);
        }

        Object[] attach(Object[] parameters, boolean generatedKeys) {
            ProtectedBatchRows.Layout selected = layout;
            if (!generatedKeys) {
                if (withoutGeneratedKeys == null) withoutGeneratedKeys =
                        new ProtectedBatchRows.Layout(current.statement(), current.parameterTypes(), false);
                selected = withoutGeneratedKeys;
            }
            if (selected == null) return parameters;
            if (!supportsLayouts) throw new UnsupportedOperationException("batch executor does not support mixed row layouts");
            return selected.attach(parameters);
        }
    }
}
