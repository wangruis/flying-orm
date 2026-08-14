package com.flying.orm.rdb.protection;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.HashMap;
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
        Map<Integer, Object> replacements = new HashMap<>();
        for (int index = 0; index < safeRow.columnCount(); index++) {
            MaskedFieldDefinition definition = safeForm.protections()
                                                        .masked(safeRow.columnName(index))
                                                        .orElse(null);
            Object value = safeRow.value(index);
            if (definition != null && value != null && display(safeMode, definition) == SensitiveDisplayMode.MASKED) {
                if (!(value instanceof String text)) {
                    throw new IllegalArgumentException("masked field result must be text");
                }
                replacements.put(index, policies.mask(text, definition));
            }
        }
        return safeRow.withValues(replacements);
    }

    private static SensitiveDisplayMode display(SensitiveDisplayMode requested,
                                                MaskedFieldDefinition definition) {
        return requested == SensitiveDisplayMode.DECLARED ? definition.display() : requested;
    }
}
