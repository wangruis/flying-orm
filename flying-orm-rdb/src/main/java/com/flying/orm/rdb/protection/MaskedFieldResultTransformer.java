package com.flying.orm.rdb.protection;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** 只替换结果中显式声明 masked 的列，并继续复用 DynamicRow 列布局。 */
final class MaskedFieldResultTransformer {

    private final MaskingPolicyRegistry policies;

    MaskedFieldResultTransformer(MaskingPolicyRegistry policies) {
        this.policies = Objects.requireNonNull(policies, "masking policy registry must not be null");
    }

    DynamicRow transform(DynamicForm form, DynamicRow row, SensitiveDisplayMode requestedMode) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        DynamicRow safeRow = Objects.requireNonNull(row, "dynamic row must not be null");
        SensitiveDisplayMode safeMode = Objects.requireNonNull(
                requestedMode, "sensitive display mode must not be null");
        if (safeForm.protections().maskedFields().isEmpty()) {
            return safeRow;
        }
        List<MaskedColumn> columns = safeRow.mappingBinding(
                safeForm.protections().maskedFields(), () -> bind(safeForm, safeRow));
        Map<Integer, Object> replacements = null;
        for (MaskedColumn column : columns) {
            Object value = safeRow.value(column.index());
            Object transformed = transformValue(value, column.definition(), safeMode);
            if (transformed != value) {
                if (replacements == null) {
                    replacements = new HashMap<>();
                }
                replacements.put(column.index(), transformed);
            }
        }
        return replacements == null ? safeRow : safeRow.withValues(replacements);
    }

    private static List<MaskedColumn> bind(DynamicForm form, DynamicRow row) {
        List<MaskedColumn> columns = new ArrayList<>(form.protections().maskedFields().size());
        for (int index = 0; index < row.columnCount(); index++) {
            MaskedFieldDefinition definition = form.protections().masked(row.columnName(index)).orElse(null);
            if (definition != null) {
                columns.add(new MaskedColumn(index, definition));
            }
        }
        return List.copyOf(columns);
    }

    Object transformValue(Object value,
                          MaskedFieldDefinition definition,
                          SensitiveDisplayMode requestedMode) {
        if (definition == null || value == null
                || display(requestedMode, definition) != SensitiveDisplayMode.MASKED) {
            return value;
        }
        if (!(value instanceof String text)) {
            throw new IllegalArgumentException("masked field result must be text");
        }
        return policies.mask(text, definition);
    }

    private static SensitiveDisplayMode display(SensitiveDisplayMode requested,
                                                MaskedFieldDefinition definition) {
        return requested == SensitiveDisplayMode.DECLARED ? definition.display() : requested;
    }

    private record MaskedColumn(int index, MaskedFieldDefinition definition) {
    }
}
