package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
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
record BatchInsertPlan(String sql,
                       List<DynamicField> layout,
                       BatchColumnLayout columnLayout,
                       List<Class<?>> parameterTypes,
                       SqlBindMarkerStyle bindMarkerStyle) {

    BatchInsertPlan {
        sql = requireText(sql, "batch insert sql");
        layout = List.copyOf(Objects.requireNonNull(layout, "batch insert layout must not be null"));
        columnLayout = Objects.requireNonNull(columnLayout, "batch column layout must not be null");
        parameterTypes = List.copyOf(Objects.requireNonNull(parameterTypes,
                                                            "batch insert parameter types must not be null"));
        if (layout.size() != parameterTypes.size()) {
            throw new IllegalArgumentException("batch insert parameter type count must match layout size");
        }
        bindMarkerStyle = Objects.requireNonNull(bindMarkerStyle, "batch insert bind marker style must not be null");
    }

    Object[] parameters(Map<String, Object> row, long rowIndex) {
        return columnLayout.parameters(row, rowIndex);
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
        return new BatchWriteRequest(sql,
                                     layout.size(),
                                     parameterTypes,
                                     bindMarkerStyle,
                                     rows,
                                     options,
                                     BatchRowCountPolicy.ANY,
                                     Objects.requireNonNull(generatedKeys, "batch generated keys must not be null"),
                                     completion);
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }
}
