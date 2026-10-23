package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import reactor.core.publisher.Flux;

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

    /** 逐行复核并发布最终投影；每次订阅独立持有投影和预算状态，不收集整次查询。 */
    Flux<DynamicRow> finish(DynamicForm form,
                                  ProtectedFieldRuntime.PreparedContainsQuery query,
                                  Flux<DynamicRow> decryptedRows,
                                  List<String> outputFields,
                                  SensitiveDisplayMode displayMode,
                                  SqlExecutionOptions options) {
        return Flux.defer(() -> decryptedRows
                .mapNotNull(rowFinisher(form, query, outputFields, displayMode, options)));
    }

    /** 单次查询的逐行复核、展示和投影；未匹配的候选返回 null，不发布给调用方。 */
    UnaryOperator<DynamicRow> rowFinisher(DynamicForm form,
                                         ProtectedFieldRuntime.PreparedContainsQuery query,
                                         List<String> outputFields, SensitiveDisplayMode displayMode,
                                         SqlExecutionOptions options) {
        UnaryOperator<DynamicRow> display = renderer.protection().displayOperation(form, displayMode);
        FormResultDecoder.DecodedResultBudget budget = new FormResultDecoder.DecodedResultBudget(options.maxResultBytes());
        return new UnaryOperator<>() {
            private DynamicRow projectionTemplate;
            private Map<Integer, Object> replacements;

            @Override
            public DynamicRow apply(DynamicRow row) {
                if (!renderer.protection().matchesContains(form, query, row)) return null;
                DynamicRow displayed = budget.accept(display.apply(row));
                if (sameLayout(displayed, outputFields)) {
                    return displayed;
                }
                if (projectionTemplate == null) {
                    projectionTemplate = projectionTemplate(outputFields);
                    replacements = new LinkedHashMap<>(projectionTemplate.columnCount());
                    for (int index = 0; index < projectionTemplate.columnCount(); index++) {
                        replacements.put(index, null);
                    }
                }
                return project(displayed, projectionTemplate, replacements);
            }
        };
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
