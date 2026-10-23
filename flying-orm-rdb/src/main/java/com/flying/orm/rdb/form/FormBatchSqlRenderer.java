package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionGroups;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.dialect.UpsertDialect;
import com.flying.orm.rdb.internal.dialect.StagedUpsertDialect;
import com.flying.orm.rdb.internal.mapping.RepositoryUpsertValues;
import com.flying.orm.rdb.internal.plan.SqlStatementCompiler;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 动态表单批量写入 SQL 的内部实现。
 *
 * <p>首行确定列布局和 SQL，后续行沿用绑定位置；表达式复用共享编译器。即使输入 Map 的迭代顺序不同，
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
        FormSqlRenderSupport.WriteFields firstValues = support.writeFields(
                safeForm,
                Objects.requireNonNull(firstRow, "batch insert first row must not be null"),
                0L);
        List<DynamicField> layout = firstValues.fields();
        SqlStatementPlan statement = FormWriteSqlRenderer.insertStatement(support, safeForm, firstValues);
        return plan(safeForm, layout, firstValues, statement, List.of(), layout.size());
    }

    BatchInsertPlan upsertPlan(DynamicForm form, Map<String, Object> firstRow) {
        return upsertPlan(form, firstRow, form, firstRow, firstRow, null, List.of(), null);
    }

    BatchInsertPlan insertIgnoringConflictsPlan(DynamicForm form, Map<String, Object> firstRow,
                                                List<String> conflictNames) {
        FormSqlRenderSupport.WriteFields firstValues = support.writeFields(form, firstRow, 0L);
        List<DynamicField> layout = firstValues.fields();
        List<DynamicField> keys = requireCompleteConflictKey(conflictNames.isEmpty()
                ? form.fields().stream().filter(DynamicField::primaryKey).toList()
                : conflictNames.stream().map(form::field).toList(), layout, conflictNames.isEmpty());
        String sql = upsertDialect.renderIgnoreConflicts(support.identifier(form), identifiers(layout),
                identifiers(keys), firstValues.expressions(support));
        SqlStatementPlan statement = SqlStatementCompiler.ignoreConflicts(sql, firstValues.parameters().length, support.dialectName,
                upsertDialect instanceof StagedUpsertDialect staged && staged.supportsBatchRowIndex());
        return plan(form, layout, firstValues, statement, List.of(), layout.size());
    }

    BatchInsertPlan upsertPlan(DynamicForm logicalForm, Map<String, Object> preparedLogicalValues,
                               DynamicForm physicalForm, Map<String, Object> firstRow,
                               Map<String, Object> sourceRow, ConditionGroup targetScope, List<String> conflictNames,
                               ConditionGroup updateCondition) {
        DynamicForm safeForm = Objects.requireNonNull(physicalForm, "dynamic form must not be null");
        FormSqlRenderSupport.WriteFields firstValues = support.writeFields(
                safeForm,
                Objects.requireNonNull(firstRow, "batch upsert first row must not be null"),
                0L);
        List<DynamicField> layout = firstValues.fields();
        RepositoryUpsertValues stagedValues = sourceRow instanceof RepositoryUpsertValues values ? values : null;
        boolean scopedTarget = targetScope != null && !ConditionGroups.isEmpty(targetScope);
        boolean conditionalUpdate = updateCondition != null && !ConditionGroups.isEmpty(updateCondition);
        UpsertFieldPlan fields = UpsertFieldPlan.create(
                logicalForm, preparedLogicalValues, safeForm, layout, stagedValues, scopedTarget, conflictNames);
        requireCompleteConflictKey(fields.conflictFields(), fields.insertFields(), conflictNames.isEmpty());
        List<String> insertColumns = identifiers(fields.insertFields());
        List<String> conflictColumns = identifiers(fields.conflictFields());
        List<String> updateColumns = identifiers(fields.updateFields());
        List<String> parameterColumns = fields.parameterFields() == fields.insertFields()
                ? insertColumns : identifiers(fields.parameterFields());
        firstValues = firstValues.select(fields.parameterFields());
        List<String> valueExpressions = firstValues.expressions(support);
        if ((scopedTarget || conditionalUpdate) && !updateColumns.isEmpty()) {
            if (!(upsertDialect instanceof StagedUpsertDialect staged)) {
                throw new UnsupportedOperationException("custom upsert dialect does not support target row predicates");
            }
            String table = support.identifier(safeForm);
            String qualifier = staged.scopeTargetQualifier(table);
            java.util.function.UnaryOperator<String> field = name -> qualifier + "." + support.identifier(name);
            FormSqlRenderSupport.ConditionSql scope = scopedTarget ? support.condition(
                    safeForm, targetScope, field, field, name -> qualifier, null) : null;
            FormSqlRenderSupport.ConditionSql condition = conditionalUpdate ? support.condition(
                    safeForm, updateCondition, field, field, name -> qualifier, null) : null;
            String sql = conditionalUpdate
                    ? staged.renderConditional(table, insertColumns, conflictColumns, updateColumns,
                            parameterColumns, valueExpressions, scope == null ? null : scope.sql(), condition.sql())
                    : staged.renderScoped(table, insertColumns, conflictColumns, updateColumns,
                            parameterColumns, valueExpressions, scope.sql());
            List<Object> parameters = scope == null ? condition.parameters() : scope.parameters();
            if (scope != null && condition != null) {
                parameters = new ArrayList<>(parameters);
                parameters.addAll(condition.parameters());
            }
            return plan(safeForm, fields.parameterFields(), firstValues, sql, parameters,
                        staged.scopeParameterIndex(insertColumns.size(), parameterColumns.size()));
        }
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

    private static List<DynamicField> requireCompleteConflictKey(List<DynamicField> keys,
                                                                List<DynamicField> layout, boolean primary) {
        String kind = primary ? "primary key" : "conflict key";
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("batch upsert requires " + kind + " fields in submitted values");
        }
        boolean complete = primary ? layout.stream().filter(DynamicField::primaryKey).count() == keys.size()
                : layout.containsAll(keys);
        if (!complete) {
            throw new IllegalArgumentException("batch upsert requires all " + kind + " fields in submitted values");
        }
        return keys;
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
        if (!rowIterator.hasNext()) {
            return plan.request(Mono.just(plan.firstParameters()), safeOptions);
        }
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
                                 FormSqlRenderSupport.WriteFields firstValues,
                                 String sql) {
        return plan(form, layout, firstValues, sql, List.of(), layout.size());
    }

    private BatchInsertPlan plan(DynamicForm form,
                                 List<DynamicField> layout,
                                 FormSqlRenderSupport.WriteFields firstValues,
                                 String sql,
                                 List<Object> predicateParameters,
                                 int predicateParameterIndex) {
        firstValues = firstValues.select(layout);
        int parameterCount = firstValues.parameters().length + predicateParameters.size();
        SqlStatementPlan statement = upsertDialect instanceof StagedUpsertDialect staged && staged.supportsBatchRowIndex()
                ? SqlStatementCompiler.upsert(sql, parameterCount, support.dialectName)
                : support.compiledStatement(sql, parameterCount, SqlBindMarkerStyle.CANONICAL);
        return plan(form, layout, firstValues, statement, predicateParameters, predicateParameterIndex);
    }

    private BatchInsertPlan plan(DynamicForm form,
                                 List<DynamicField> layout,
                                 FormSqlRenderSupport.WriteFields firstValues,
                                 SqlStatementPlan statement,
                                 List<Object> predicateParameters,
                                 int predicateParameterIndex) {
        firstValues = firstValues.select(layout);
        BatchColumnLayout columnLayout = BatchColumnLayout.of(form, firstValues, support);
        predicateParameterIndex = firstValues.parameterIndex(predicateParameterIndex);
        Object[] parameters = firstValues.parameters();
        List<Class<?>> parameterTypes = firstValues.parameterTypes(support);
        if (!predicateParameters.isEmpty()) {
            Object[] expanded = new Object[parameters.length + predicateParameters.size()];
            System.arraycopy(parameters, 0, expanded, 0, predicateParameterIndex);
            System.arraycopy(parameters, predicateParameterIndex, expanded,
                    predicateParameterIndex + predicateParameters.size(), parameters.length - predicateParameterIndex);
            parameters = expanded;
            List<Class<?>> predicateTypes = new ArrayList<>(predicateParameters.size());
            for (int index = 0; index < predicateParameters.size(); index++) {
                Object value = predicateParameters.get(index);
                parameters[predicateParameterIndex + index] = value;
                predicateTypes.add(FormSqlRenderSupport.boundParameterType(value));
            }
            parameterTypes.addAll(predicateParameterIndex, predicateTypes);
        }
        return new BatchInsertPlan(statement,
                                   columnLayout,
                                   parameters,
                                   parameterTypes,
                                   predicateParameters,
                                   predicateParameterIndex);
    }
}
