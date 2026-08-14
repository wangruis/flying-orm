package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.dialect.UpsertDialect;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * 动态表单批量写入 SQL 的内部实现。
 *
 * <p>首行确定列布局和 SQL，后续行只按该布局产生参数数组。这样即使输入 Map 的迭代顺序不同，
 * 也不会把值绑定到错误的列；所有字段校验和 codec 转换仍委托给共享 support。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class FormBatchSqlRenderer {

    private final FormSqlRenderSupport support;
    private final UpsertDialect upsertDialect;

    FormBatchSqlRenderer(FormSqlRenderSupport support, UpsertDialect upsertDialect) {
        this.support = Objects.requireNonNull(support, "form SQL render support must not be null");
        this.upsertDialect = Objects.requireNonNull(upsertDialect, "upsert dialect must not be null");
    }

    BatchWriteRequest insertBatch(DynamicForm form, List<Map<String, Object>> rows, BatchWriteOptions options) {
        return batchRequest(form, rows, options, false);
    }

    BatchWriteRequest upsertBatch(DynamicForm form, List<Map<String, Object>> rows, BatchWriteOptions options) {
        return batchRequest(form, rows, options, true);
    }

    BatchInsertPlan insertPlan(DynamicForm form, Map<String, Object> firstRow) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        List<FormSqlRenderSupport.FieldValue> firstValues = support.writeFields(
                safeForm,
                Objects.requireNonNull(firstRow, "batch insert first row must not be null"),
                0L);
        List<DynamicField> layout = firstValues.stream().map(FormSqlRenderSupport.FieldValue::field).toList();
        StringJoiner placeholders = new StringJoiner(", ");
        layout.forEach(field -> placeholders.add(support.valueExpression(field)));
        String sql = "insert into " + support.identifier(safeForm.table()) + " (" + support.columns(layout)
                + ") values (" + placeholders + ")";
        return plan(safeForm, layout, sql);
    }

    BatchInsertPlan upsertPlan(DynamicForm form, Map<String, Object> firstRow) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        List<FormSqlRenderSupport.FieldValue> firstValues = support.writeFields(
                safeForm,
                Objects.requireNonNull(firstRow, "batch upsert first row must not be null"),
                0L);
        List<DynamicField> layout = firstValues.stream().map(FormSqlRenderSupport.FieldValue::field).toList();
        List<String> columns = layout.stream().map(field -> support.identifier(field.name())).toList();
        List<String> conflictColumns = layout.stream()
                                             .filter(DynamicField::primaryKey)
                                             .map(field -> support.identifier(field.name()))
                                             .toList();
        if (conflictColumns.isEmpty()) {
            throw new IllegalArgumentException("batch upsert requires primary key fields in submitted values");
        }
        List<String> updateColumns = layout.stream()
                                           .filter(field -> !field.primaryKey())
                                           .map(field -> support.identifier(field.name()))
                                           .toList();
        String sql = upsertDialect.render(support.identifier(safeForm.table()),
                                          columns,
                                          conflictColumns,
                                          updateColumns,
                                          layout.stream().map(support::valueExpression).toList());
        return plan(safeForm, layout, sql);
    }

    private BatchWriteRequest batchRequest(DynamicForm form,
                                           List<Map<String, Object>> rows,
                                           BatchWriteOptions options,
                                           boolean upsert) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        String operation = upsert ? "upsert" : "insert";
        List<Map<String, Object>> safeRows = Objects.requireNonNull(rows,
                                                                      "batch " + operation + " rows must not be null");
        BatchWriteOptions safeOptions = Objects.requireNonNull(options, "batch write options must not be null");
        if (safeRows.isEmpty()) {
            throw new IllegalArgumentException("batch " + operation + " rows must not be empty");
        }
        Map<String, Object> firstRow = Objects.requireNonNull(safeRows.getFirst(),
                                                                "batch " + operation + " row must not be null");
        BatchInsertPlan plan = upsert ? upsertPlan(safeForm, firstRow) : insertPlan(safeForm, firstRow);
        List<Object[]> parameterRows = new ArrayList<>(safeRows.size());
        for (int i = 0; i < safeRows.size(); i++) {
            parameterRows.add(plan.parameters(safeRows.get(i), i));
        }
        return plan.request(Flux.fromIterable(parameterRows), safeOptions);
    }

    private BatchInsertPlan plan(DynamicForm form, List<DynamicField> layout, String sql) {
        return new BatchInsertPlan(sql,
                                   layout,
                                   BatchColumnLayout.of(form,
                                                        layout,
                                                        support.dialectName,
                                                        support.nativeBoolean,
                                                        support.valueCodecs),
                                   layout.stream().map(support::parameterType).toList(),
                                   SqlBindMarkerStyle.CANONICAL);
    }
}
