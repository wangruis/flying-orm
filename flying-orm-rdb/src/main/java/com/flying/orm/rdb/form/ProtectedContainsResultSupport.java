package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 对有界 CONTAINS 候选执行明文复核、最终脱敏和投影裁剪。
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class ProtectedContainsResultSupport {

    static final int DEFAULT_CANDIDATE_LIMIT = 1000;

    private final FormDataSqlRenderer renderer;

    ProtectedContainsResultSupport(FormDataSqlRenderer renderer) {
        this.renderer = Objects.requireNonNull(renderer, "form data sql renderer must not be null");
    }

    List<DynamicRow> finish(DynamicForm form,
                            ProtectedFieldRuntime.PreparedContainsQuery query,
                            List<DynamicRow> decryptedRows,
                            List<String> outputFields,
                            SensitiveDisplayMode displayMode,
                            int candidateLimit) {
        List<DynamicRow> safeRows = List.copyOf(Objects.requireNonNull(
                decryptedRows, "protected contains candidate rows must not be null"));
        if (safeRows.size() > candidateLimit) {
            throw new ProtectedSearchCandidateLimitExceededException(candidateLimit, safeRows.size());
        }
        List<DynamicRow> verified = new ArrayList<>(safeRows.size());
        for (DynamicRow row : safeRows) {
            if (renderer.protection().matchesContains(form, query, row)) {
                DynamicRow displayed = renderer.protection().mask(form, row, displayMode);
                verified.add(project(displayed, outputFields));
            }
        }
        return List.copyOf(verified);
    }

    private static DynamicRow project(DynamicRow row, List<String> outputFields) {
        List<String> fields = List.copyOf(Objects.requireNonNull(
                outputFields, "protected contains output fields must not be null"));
        boolean sameLayout = fields.size() == row.columnCount();
        for (int index = 0; sameLayout && index < fields.size(); index++) {
            sameLayout = fields.get(index).equals(row.columnName(index));
        }
        if (sameLayout) {
            return row;
        }
        Map<String, Object> values = new LinkedHashMap<>(fields.size());
        for (String field : fields) {
            if (!row.containsKey(field)) {
                throw new IllegalStateException("protected contains result is missing a projected field");
            }
            values.put(field, row.get(field));
        }
        return DynamicRow.copyOf(values);
    }
}
