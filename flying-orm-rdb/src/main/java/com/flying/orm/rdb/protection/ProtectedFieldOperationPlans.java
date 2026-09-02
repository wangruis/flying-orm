package com.flying.orm.rdb.protection;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime.ContainsFieldTokens;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime.PreparedWrite;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Owns the operation-scoped field-protection plans used by {@link ProtectedFieldRuntime}.
 * Public runtime types remain thin API wrappers while this package-private helper holds
 * the row-independent write and result execution details.
 */
final class ProtectedFieldOperationPlans {

    private static final String MISSING_KEYS = "protected field key ring is not configured";

    private final ProtectedWriteTransformer writes;
    private final ProtectedResultTransformer results;
    private final MaskedFieldResultTransformer masking;

    ProtectedFieldOperationPlans(ProtectedWriteTransformer writes,
                                 ProtectedResultTransformer results,
                                 MaskedFieldResultTransformer masking) {
        this.writes = writes;
        this.results = results;
        this.masking = masking;
    }

    WritePlan write(DynamicForm form,
                    DynamicForm physicalForm,
                    DataScope scope,
                    ValueCodecRegistry codecs) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        DynamicForm safePhysicalForm = Objects.requireNonNull(
                physicalForm, "physical form must not be null");
        if (safeForm.protections().encryptedFields().isEmpty()) {
            return new WritePlan(safePhysicalForm, null);
        }
        requireKeys(writes);
        return new WritePlan(
                safePhysicalForm, writes.plan(safeForm, safePhysicalForm, scope, codecs));
    }

    List<ContainsFieldTokens> containsTokens(DynamicForm form,
                                             Map<String, Object> values,
                                             DataScope scope,
                                             ValueCodecRegistry codecs) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        boolean containsSearch = safeForm.protections().encryptedFields().values().stream()
                .anyMatch(definition -> definition.searchModes().contains(EncryptedSearchMode.CONTAINS));
        if (!containsSearch) {
            return List.of();
        }
        return write(safeForm, ProtectedFormLayout.physical(safeForm), scope, codecs)
                .containsTokens(values);
    }

    Map<String, byte[]> receiptIdentities(DynamicForm form,
                                          Map<String, Object> values,
                                          DataScope scope,
                                          ValueCodecRegistry codecs) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        if (safeForm.protections().encryptedFields().isEmpty()) {
            return Map.of();
        }
        return write(safeForm, ProtectedFormLayout.physical(safeForm), scope, codecs)
                .receiptIdentities(values);
    }

    ResultPlan result(DynamicForm form,
                      DataScope scope,
                      SensitiveDisplayMode displayMode,
                      ValueCodecRegistry codecs) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        SensitiveDisplayMode safeDisplayMode = Objects.requireNonNull(
                displayMode, "sensitive display mode must not be null");
        if (safeForm.protections().isEmpty()) {
            return new ResultPlan(null, null, null, safeDisplayMode);
        }
        if (!safeForm.protections().encryptedFields().isEmpty()) {
            requireKeys(results);
            return new ResultPlan(
                    results.plan(safeForm, scope, safeDisplayMode, codecs), null, null, safeDisplayMode);
        }
        return new ResultPlan(null, masking, safeForm, safeDisplayMode);
    }

    static List<byte[]> copyTokens(List<byte[]> values) {
        List<byte[]> copy = new ArrayList<>(Objects.requireNonNull(
                values, "protected contains tokens must not be null").size());
        values.forEach(value -> copy.add(Objects.requireNonNull(
                value, "protected contains token must not be null").clone()));
        return List.copyOf(copy);
    }

    private static void requireKeys(Object transformer) {
        if (transformer == null) {
            throw new IllegalStateException(MISSING_KEYS);
        }
    }

    static final class WritePlan {

        private final DynamicForm physicalForm;
        private final ProtectedWriteTransformer.Plan encrypted;

        private WritePlan(DynamicForm physicalForm, ProtectedWriteTransformer.Plan encrypted) {
            this.physicalForm = physicalForm;
            this.encrypted = encrypted;
        }

        PreparedWrite prepare(Map<String, Object> values) {
            Map<String, Object> safeValues = Objects.requireNonNull(
                    values, "dynamic form values must not be null");
            return encrypted == null
                    ? new PreparedWrite(physicalForm, safeValues) : encrypted.prepare(safeValues);
        }

        List<ContainsFieldTokens> containsTokens(Map<String, Object> values) {
            return encrypted == null ? List.of() : encrypted.containsTokens(values);
        }

        Map<String, byte[]> receiptIdentities(Map<String, Object> values) {
            return encrypted == null ? Map.of() : encrypted.receiptIdentities(values);
        }
    }

    static final class ResultPlan {

        private final ProtectedResultTransformer.ResultPlan encrypted;
        private final MaskedFieldResultTransformer masking;
        private final DynamicForm form;
        private final SensitiveDisplayMode displayMode;

        private ResultPlan(ProtectedResultTransformer.ResultPlan encrypted,
                           MaskedFieldResultTransformer masking,
                           DynamicForm form,
                           SensitiveDisplayMode displayMode) {
            this.encrypted = encrypted;
            this.masking = masking;
            this.form = form;
            this.displayMode = displayMode;
        }

        DynamicRow transform(DynamicRow row) {
            DynamicRow safeRow = Objects.requireNonNull(row, "dynamic row must not be null");
            if (encrypted != null) {
                return encrypted.transform(safeRow);
            }
            return masking == null ? safeRow : masking.transform(form, safeRow, displayMode);
        }
    }
}
