package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.execution.SqlExecutionOptions;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * 对 CONTAINS 候选执行明文复核、最终脱敏和投影裁剪；容量由查询执行选项控制。
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class ProtectedContainsResultSupport {

    private final FormDataSqlRenderer renderer;

    ProtectedContainsResultSupport(FormDataSqlRenderer renderer) {
        this.renderer = Objects.requireNonNull(renderer, "form data sql renderer must not be null");
    }

    List<DynamicRow> finish(DynamicForm form,
                            ProtectedFieldRuntime.PreparedContainsQuery query,
                            List<DynamicRow> decryptedRows,
                            List<String> outputFields,
                            SensitiveDisplayMode displayMode,
                            SqlExecutionOptions options) {
        List<DynamicRow> verified = new ArrayList<>(decryptedRows.size());
        DynamicRow projectionTemplate = null;
        Map<Integer, Object> replacements = null;
        UnaryOperator<DynamicRow> display = renderer.protection().displayOperation(form, displayMode);
        FormResultDecoder.DecodedResultBudget budget = new FormResultDecoder.DecodedResultBudget(options.maxResultBytes());
        for (DynamicRow row : decryptedRows) {
            if (renderer.protection().matchesContains(form, query, row)) {
                DynamicRow displayed = budget.accept(display.apply(row));
                if (sameLayout(displayed, outputFields)) {
                    verified.add(displayed);
                    continue;
                }
                if (projectionTemplate == null) {
                    projectionTemplate = projectionTemplate(outputFields);
                    replacements = new LinkedHashMap<>(projectionTemplate.columnCount());
                    for (int index = 0; index < projectionTemplate.columnCount(); index++) {
                        replacements.put(index, null);
                    }
                }
                verified.add(project(displayed, projectionTemplate, replacements));
            }
        }
        return List.copyOf(verified);
    }

    private static boolean sameLayout(DynamicRow row, List<String> outputFields) {
        boolean sameLayout = outputFields.size() == row.columnCount();
        for (int index = 0; sameLayout && index < outputFields.size(); index++) {
            sameLayout = outputFields.get(index).equals(row.columnName(index));
        }
        return sameLayout;
    }

    private static DynamicRow projectionTemplate(List<String> outputFields) {
        Map<String, Object> columns = new LinkedHashMap<>(outputFields.size());
        for (String field : outputFields) {
            columns.put(field, null);
        }
        // Only the layout is reused; the template never retains a candidate's sensitive values.
        return DynamicRow.copyOf(columns);
    }

    private static DynamicRow project(DynamicRow row,
                                      DynamicRow template,
                                      Map<Integer, Object> replacements) {
        for (Map.Entry<Integer, Object> replacement : replacements.entrySet()) {
            String field = template.columnName(replacement.getKey());
            if (!row.containsKey(field)) {
                throw new IllegalStateException("protected contains result is missing a projected field");
            }
            replacement.setValue(row.get(field));
        }
        // withValues copies immediately, so later replacements cannot change an emitted row.
        return template.withValues(replacements);
    }

}
