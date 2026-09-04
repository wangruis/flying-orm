package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinProjection;
import com.flying.orm.core.join.JoinQuerySpec;
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
        boolean allFull = true;
        boolean needsMasking = false;
        for (int index = 0; index < safeRow.columnCount(); index++) {
            String field = safeRow.columnName(index);
            FieldVisibility visibility = safeSnapshot.visibility(field);
            if (visibility != FieldVisibility.FULL) {
                allFull = false;
            }
            if (visibility == FieldVisibility.MASKED) {
                requireMaskDefinition(form, field);
                needsMasking = true;
            }
        }
        if (allFull) {
            return safeRow;
        }
        DynamicRow masked = needsMasking
                ? Objects.requireNonNull(renderer, "form data sql renderer must not be null")
                         .protection().mask(form, safeRow,
                                           com.flying.orm.core.protection.SensitiveDisplayMode.MASKED)
                : safeRow;
        Map<String, Object> visible = new LinkedHashMap<>();
        for (int index = 0; index < safeRow.columnCount(); index++) {
            String field = safeRow.columnName(index);
            FieldVisibility visibility = safeSnapshot.visibility(field);
            if (visibility == FieldVisibility.HIDDEN) {
                continue;
            }
            visible.put(field, visibility == FieldVisibility.MASKED
                    ? masked.value(index) : safeRow.value(index));
        }
        return DynamicRow.copyOf(visible);
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
        boolean allFull = true;
        for (JoinProjection projection : safeSpec.projections()) {
            if (safeSnapshot.visibility(projection.field().field()) != FieldVisibility.FULL) {
                allFull = false;
                break;
            }
        }
        if (allFull) {
            return safeRow;
        }
        Map<String, Object> visible = new LinkedHashMap<>();
        for (JoinProjection projection : safeSpec.projections()) {
            String sourceField = projection.field().field();
            String alias = projection.alias();
            FieldVisibility visibility = safeSnapshot.visibility(sourceField);
            if (visibility == FieldVisibility.HIDDEN) {
                continue;
            }
            Object value = safeRow.get(alias);
            if (visibility == FieldVisibility.MASKED) {
                DynamicForm sourceForm = projection.field().source().form();
                requireMaskDefinition(sourceForm, sourceField);
                Map<String, Object> sourceValue = new LinkedHashMap<>(1);
                sourceValue.put(sourceField, value);
                value = Objects.requireNonNull(renderer, "form data sql renderer must not be null")
                        .protection()
                        .mask(sourceForm, DynamicRow.copyOf(sourceValue),
                              com.flying.orm.core.protection.SensitiveDisplayMode.MASKED)
                        .get(sourceField);
            }
            visible.put(alias, value);
        }
        return DynamicRow.copyOf(visible);
    }

    private static void requireMaskDefinition(DynamicForm form, String field) {
        DynamicForm safeForm = Objects.requireNonNull(form, "governed result form must not be null");
        if (safeForm.protections().masked(field).isPresent()) {
            return;
        }
        throw new ScopeAccessException(
                ScopeErrorCode.FIELD_NOT_READABLE,
                safeForm.id(),
                field,
                "field [" + field + "] is MASKED but has no masking definition");
    }
}
