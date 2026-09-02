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
                       List<Class<?>> parameterTypes) {

    BatchInsertPlan {
        statement = Objects.requireNonNull(statement, "batch insert statement must not be null");
        layout = List.copyOf(Objects.requireNonNull(layout, "batch insert layout must not be null"));
        columnLayout = Objects.requireNonNull(columnLayout, "batch column layout must not be null");
        firstParameters = Objects.requireNonNull(
                firstParameters, "batch first parameters must not be null");
        parameterTypes = List.copyOf(Objects.requireNonNull(parameterTypes,
                                                            "batch insert parameter types must not be null"));
        if (layout.size() != parameterTypes.size() || layout.size() != firstParameters.length) {
            throw new IllegalArgumentException("batch insert parameter type count must match layout size");
        }
    }

    Object[] parameters(Map<String, Object> row, long rowIndex) {
        return columnLayout.parameters(row, rowIndex);
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
