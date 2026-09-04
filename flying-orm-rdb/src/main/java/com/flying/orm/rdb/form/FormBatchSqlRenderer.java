package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.dialect.UpsertDialect;
import com.flying.orm.rdb.internal.dialect.StagedUpsertDialect;
import com.flying.orm.rdb.internal.mapping.RepositoryUpsertValues;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

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
        String sql = "insert into " + support.identifier(safeForm) + " (" + support.columns(layout)
                + ") values (" + placeholders + ")";
        return plan(safeForm, layout, firstValues, sql);
    }

    BatchInsertPlan upsertPlan(DynamicForm form, Map<String, Object> firstRow) {
        return upsertPlan(form, firstRow, form, firstRow, firstRow);
    }

    BatchInsertPlan upsertPlan(DynamicForm logicalForm,
                               Map<String, Object> preparedLogicalValues,
                               DynamicForm physicalForm,
                               Map<String, Object> firstRow,
                               Map<String, Object> sourceRow) {
        DynamicForm safeForm = Objects.requireNonNull(physicalForm, "dynamic form must not be null");
        List<FormSqlRenderSupport.FieldValue> firstValues = support.writeFields(
                safeForm,
                Objects.requireNonNull(firstRow, "batch upsert first row must not be null"),
                0L);
        List<DynamicField> layout = firstValues.stream().map(FormSqlRenderSupport.FieldValue::field).toList();
        RepositoryUpsertValues stagedValues = sourceRow instanceof RepositoryUpsertValues values ? values : null;
        UpsertFieldPlan fields = UpsertFieldPlan.create(
                logicalForm, preparedLogicalValues, safeForm, layout, stagedValues);
        requireCompletePrimaryKey(safeForm, fields.insertFields());
        List<String> insertColumns = identifiers(fields.insertFields());
        List<String> conflictColumns = identifiers(fields.conflictFields());
        List<String> updateColumns = identifiers(fields.updateFields());
        List<String> parameterColumns = identifiers(fields.parameterFields());
        List<String> valueExpressions = fields.parameterFields().stream()
                                               .map(support::valueExpression)
                                               .toList();
        String sql = renderUpsert(insertColumns,
                                  conflictColumns,
                                  updateColumns,
                                  parameterColumns,
                                  valueExpressions,
                                  support.identifier(safeForm));
        return plan(safeForm, fields.parameterFields(), firstValues, sql);
    }

    private String renderUpsert(List<String> insertColumns,
                                List<String> conflictColumns,
                                List<String> updateColumns,
                                List<String> parameterColumns,
                                List<String> valueExpressions,
                                String table) {
        if (upsertDialect instanceof StagedUpsertDialect staged) {
            return staged.renderStaged(table,
                                       insertColumns,
                                       conflictColumns,
                                       updateColumns,
                                       parameterColumns,
                                       valueExpressions);
        }
        if (!insertColumns.equals(parameterColumns)) {
            throw new IllegalArgumentException(
                    "custom upsert dialect does not support update-only columns");
        }
        return upsertDialect.render(
                table, insertColumns, conflictColumns, updateColumns, valueExpressions);
    }

    private List<String> identifiers(List<DynamicField> fields) {
        return fields.stream().map(DynamicField::name).map(support::identifier).toList();
    }

    private static List<DynamicField> requireCompletePrimaryKey(DynamicForm form, List<DynamicField> layout) {
        List<DynamicField> primaryKeys = form.fields().stream()
                                             .filter(DynamicField::primaryKey)
                                             .toList();
        if (primaryKeys.isEmpty()) {
            throw new IllegalArgumentException("batch upsert requires primary key fields in submitted values");
        }
        Set<String> requiredPrimaryKeys = primaryKeys.stream()
                                                     .map(DynamicField::normalizedName)
                                                     .collect(Collectors.toUnmodifiableSet());
        Set<String> submittedPrimaryKeys = layout.stream()
                                                 .filter(DynamicField::primaryKey)
                                                 .map(DynamicField::normalizedName)
                                                 .collect(Collectors.toUnmodifiableSet());
        if (!submittedPrimaryKeys.equals(requiredPrimaryKeys)) {
            throw new IllegalArgumentException("batch upsert requires all primary key fields in submitted values");
        }
        return primaryKeys;
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
        Iterator<Map<String, Object>> rowIterator = safeRows.iterator();
        Map<String, Object> firstRow = Objects.requireNonNull(rowIterator.next(),
                                                                "batch " + operation + " row must not be null");
        BatchInsertPlan plan = upsert ? upsertPlan(safeForm, firstRow) : insertPlan(safeForm, firstRow);
        List<Object[]> parameterRows = new ArrayList<>(safeRows.size());
        parameterRows.add(plan.firstParameters());
        long rowIndex = 1L;
        while (rowIterator.hasNext()) {
            parameterRows.add(plan.parameters(rowIterator.next(), rowIndex++));
        }
        return plan.request(Flux.fromIterable(parameterRows), safeOptions);
    }

    private BatchInsertPlan plan(DynamicForm form,
                                 List<DynamicField> layout,
                                 List<FormSqlRenderSupport.FieldValue> firstValues,
                                 String sql) {
        Map<String, Object> valuesByField = new java.util.HashMap<>(Math.max(16, firstValues.size() * 2));
        firstValues.forEach(value -> valuesByField.put(
                value.field().normalizedName(), value.value()));
        Object[] firstParameters = layout.stream()
                                         .map(field -> valuesByField.get(field.normalizedName()))
                                         .toArray();
        return new BatchInsertPlan(support.compiledStatement(
                                           sql,
                                           layout.size(),
                                           SqlBindMarkerStyle.CANONICAL),
                                   layout,
                                   BatchColumnLayout.of(form,
                                                        layout,
                                                        support),
                                   firstParameters,
                                   layout.stream().map(support::parameterType).toList());
    }
}
