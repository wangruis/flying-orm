package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import org.reactivestreams.Publisher;

import java.util.List;
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
                       List<DynamicField> layout,
                       BatchColumnLayout columnLayout,
                       Object[] firstParameters,
                       List<Class<?>> parameterTypes,
                       List<Object> scopeParameters,
                       int scopeParameterIndex) {

    BatchInsertPlan {
        statement = Objects.requireNonNull(statement, "batch insert statement must not be null");
        layout = List.copyOf(Objects.requireNonNull(layout, "batch insert layout must not be null"));
        columnLayout = Objects.requireNonNull(columnLayout, "batch column layout must not be null");
        firstParameters = Objects.requireNonNull(
                firstParameters, "batch first parameters must not be null");
        parameterTypes = List.copyOf(Objects.requireNonNull(parameterTypes,
                                                            "batch insert parameter types must not be null"));
        scopeParameters = scopeParameters.isEmpty() ? List.of()
                : java.util.Collections.unmodifiableList(new java.util.ArrayList<>(scopeParameters));
        if (layout.size() + scopeParameters.size() != parameterTypes.size()
                || parameterTypes.size() != firstParameters.length
                || scopeParameterIndex < 0 || scopeParameterIndex > layout.size()) {
            throw new IllegalArgumentException("batch insert parameter type count must match layout size");
        }
    }

    Object[] parameters(Map<String, Object> row, long rowIndex) {
        if (scopeParameters.isEmpty()) {
            return columnLayout.parameters(row, rowIndex);
        }
        // 每行只分配最终参数数组。MySQL 把 Scope 放在 UPDATE-only 参数前，其余方言直接追加。
        Object[] parameters = new Object[parameterTypes.size()];
        columnLayout.writeParameters(row, rowIndex, parameters);
        int tail = layout.size() - scopeParameterIndex;
        if (tail > 0) {
            System.arraycopy(parameters, scopeParameterIndex, parameters,
                             scopeParameterIndex + scopeParameters.size(), tail);
        }
        for (int index = 0; index < scopeParameters.size(); index++) {
            parameters[scopeParameterIndex + index] = scopeParameters.get(index);
        }
        return parameters;
    }

    String sql() {
        return statement.sql();
    }

    SqlBindMarkerStyle bindMarkerStyle() {
        return statement.bindMarkerStyle();
    }

    BatchWriteRequest request(Publisher<Object[]> rows, BatchWriteOptions options) {
        return request(rows, options, BatchWriteCompletion.noop());
    }

    BatchWriteRequest request(Publisher<Object[]> rows,
                              BatchWriteOptions options,
                              BatchWriteCompletion completion) {
        return request(rows, options, BatchGeneratedKeys.none(), completion);
    }

    /**
     * 把实体 Repository 提供的生成键协作原样放进共享批量请求。SQL 和参数布局仍只编译一次，
     * 普通 Map 批量传入 none 时不会改变现有驱动批处理路径。
     */
    BatchWriteRequest request(Publisher<Object[]> rows,
                              BatchWriteOptions options,
                              BatchGeneratedKeys generatedKeys,
                              BatchWriteCompletion completion) {
        return new BatchWriteRequest(statement,
                                     parameterTypes,
                                     rows,
                                     options,
                                     BatchRowCountPolicy.ANY,
                                     Objects.requireNonNull(generatedKeys, "batch generated keys must not be null"),
                                     completion);
    }

}
