package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinProjection;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.scope.ScopeErrorCode;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** 在结果发布边界把不可变字段用途快照落实为 full、masked 或 hidden。 */
final class FieldVisibilityPublisher {

    private FieldVisibilityPublisher() {
    }

    static DynamicRow publish(FormDataSqlRenderer renderer,
                              DynamicForm form,
                              DynamicRow row,
                              FieldUseSnapshot snapshot) {
        DynamicRow safeRow = Objects.requireNonNull(row, "governed result row must not be null");
        FieldUseSnapshot safeSnapshot = Objects.requireNonNull(
                snapshot, "field use snapshot must not be null");
        if (safeSnapshot.isUnrestricted()) {
            return safeRow;
        }
        Map<String, Object> visible = null;
        for (int index = 0; index < safeRow.columnCount(); index++) {
            String field = safeRow.columnName(index);
            FieldVisibility visibility = safeSnapshot.visibility(field);
            if (visibility == FieldVisibility.FULL) {
                if (visible != null) {
                    visible.put(field, safeRow.value(index));
                }
                continue;
            }
            if (visible == null) {
                visible = new LinkedHashMap<>();
                for (int previous = 0; previous < index; previous++) {
                    visible.put(safeRow.columnName(previous), safeRow.value(previous));
                }
            }
            if (visibility == FieldVisibility.HIDDEN) {
                continue;
            }
            MaskedFieldDefinition definition = requireMaskDefinition(form, field);
            Object value = Objects.requireNonNull(renderer, "form data sql renderer must not be null")
                    .protection().maskValue(safeRow.value(index), definition, SensitiveDisplayMode.MASKED);
            visible.put(field, value);
        }
        return visible == null ? safeRow : DynamicRow.copyOf(visible);
    }

    static DynamicRow publishJoin(FormDataSqlRenderer renderer,
                                  JoinQuerySpec spec,
                                  DynamicRow row,
                                  FieldUseSnapshot snapshot) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        DynamicRow safeRow = Objects.requireNonNull(row, "governed join result row must not be null");
        FieldUseSnapshot safeSnapshot = Objects.requireNonNull(
                snapshot, "field use snapshot must not be null");
        if (safeSnapshot.isUnrestricted()) {
            return safeRow;
        }
        Map<String, Object> visible = null;
        for (int index = 0; index < safeSpec.projections().size(); index++) {
            JoinProjection projection = safeSpec.projections().get(index);
            String sourceField = projection.field().field();
            String alias = projection.alias();
            FieldVisibility visibility = safeSnapshot.joinVisibility(projection.field());
            if (visibility == FieldVisibility.FULL) {
                if (visible != null) {
                    visible.put(alias, safeRow.get(alias));
                }
                continue;
            }
            if (visible == null) {
                visible = new LinkedHashMap<>();
                for (int previous = 0; previous < index; previous++) {
                    String previousAlias = safeSpec.projections().get(previous).alias();
                    visible.put(previousAlias, safeRow.get(previousAlias));
                }
            }
            if (visibility == FieldVisibility.HIDDEN) {
                continue;
            }
            Object value = safeRow.get(alias);
            DynamicForm sourceForm = projection.field().source().form();
            MaskedFieldDefinition definition = requireMaskDefinition(sourceForm, sourceField);
            value = Objects.requireNonNull(renderer, "form data sql renderer must not be null")
                    .protection().maskValue(value, definition, SensitiveDisplayMode.MASKED);
            visible.put(alias, value);
        }
        return visible == null ? safeRow : DynamicRow.copyOf(visible);
    }

    private static MaskedFieldDefinition requireMaskDefinition(DynamicForm form, String field) {
        DynamicForm safeForm = Objects.requireNonNull(form, "governed result form must not be null");
        return safeForm.protections().masked(field).orElseThrow(() -> new ScopeAccessException(
                ScopeErrorCode.FIELD_NOT_READABLE, safeForm.id(), field,
                "field [" + field + "] is MASKED but has no masking definition"));
    }
}
