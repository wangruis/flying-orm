package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.rdb.internal.mapping.RepositoryUpsertValues;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Repository 阶段字段到最终批量参数布局的不可变内部计划。 */
record UpsertFieldPlan(List<DynamicField> insertFields,
                       List<DynamicField> conflictFields,
                       List<DynamicField> updateFields,
                       List<DynamicField> parameterFields) {

    UpsertFieldPlan {
        insertFields = List.copyOf(insertFields);
        conflictFields = List.copyOf(conflictFields);
        updateFields = List.copyOf(updateFields);
        parameterFields = List.copyOf(parameterFields);
    }

    static UpsertFieldPlan create(DynamicForm logicalForm,
                                  Map<String, Object> preparedLogicalValues,
                                  DynamicForm physicalForm,
                                  List<DynamicField> availableFields,
                                  RepositoryUpsertValues stagedValues) {
        DynamicForm logical = Objects.requireNonNull(logicalForm, "logical form must not be null");
        requireTenantConflictIdentity(logical);
        List<DynamicField> available = List.copyOf(availableFields);
        List<DynamicField> conflicts = physicalForm.fields().stream()
                                                        .filter(DynamicField::primaryKey)
                                                        .toList();
        if (stagedValues == null) {
            return new UpsertFieldPlan(
                    available,
                    conflicts,
                    available.stream().filter(field -> !field.primaryKey()).toList(),
                    available);
        }

        Set<String> staged = normalized(logical, stagedValues);
        Set<String> inserts = new LinkedHashSet<>(normalized(logical, stagedValues.insertValues()));
        Set<String> updates = new LinkedHashSet<>(normalized(logical, stagedValues.updateValues()));
        Set<String> preparedNames = normalized(logical, preparedLogicalValues);
        for (String prepared : preparedNames) {
            if (!staged.contains(prepared)) {
                inserts.add(prepared);
            }
        }
        logical.tenant().ifPresent(tenant -> {
            String tenantName = logical.field(tenant.fieldName()).normalizedName();
            if (preparedNames.contains(tenantName)) {
                inserts.add(tenantName);
            }
            updates.remove(tenantName);
        });
        requireContainsStageConsistency(logical, inserts, updates);
        Map<String, String> physicalOwners = physicalOwners(logical, physicalForm);
        List<DynamicField> insertFields = new ArrayList<>();
        List<DynamicField> updateFields = new ArrayList<>();
        for (DynamicField field : available) {
            String owner = physicalOwners.get(field.normalizedName());
            if (owner == null) {
                throw new IllegalArgumentException("batch upsert physical field has no logical owner");
            }
            if (inserts.contains(owner)) {
                insertFields.add(field);
            }
            if (!field.primaryKey() && updates.contains(owner)) {
                updateFields.add(field);
            }
        }

        List<DynamicField> parameters = new ArrayList<>(insertFields);
        Set<String> parameterNames = new LinkedHashSet<>();
        insertFields.forEach(field -> parameterNames.add(field.normalizedName()));
        for (DynamicField field : updateFields) {
            if (parameterNames.add(field.normalizedName())) {
                parameters.add(field);
            }
        }
        return new UpsertFieldPlan(insertFields, conflicts, updateFields, parameters);
    }

    private static void requireTenantConflictIdentity(DynamicForm form) {
        form.tenant().ifPresent(tenant -> {
            if (!form.field(tenant.fieldName()).primaryKey()) {
                throw new IllegalArgumentException(
                        "batch upsert tenant field must be part of the primary key");
            }
        });
    }

    private static void requireContainsStageConsistency(DynamicForm form,
                                                        Set<String> inserts,
                                                        Set<String> updates) {
        for (DynamicField field : form.fields()) {
            EncryptedFieldDefinition definition = form.protections().encrypted(field.name()).orElse(null);
            if (definition == null || !definition.searchModes().contains(EncryptedSearchMode.CONTAINS)) {
                continue;
            }
            String name = field.normalizedName();
            if (inserts.contains(name) != updates.contains(name)) {
                throw new IllegalArgumentException(
                        "repository batch upsert requires matching INSERT and UPDATE strategies "
                                + "for CONTAINS protected field [" + field.name() + "]");
            }
        }
    }

    private static Map<String, String> physicalOwners(DynamicForm logicalForm, DynamicForm physicalForm) {
        DynamicForm logical = Objects.requireNonNull(logicalForm, "logical form must not be null");
        DynamicForm physical = Objects.requireNonNull(physicalForm, "physical form must not be null");
        List<DynamicField> physicalFields = physical.fields();
        Map<String, String> owners = new LinkedHashMap<>();
        int physicalIndex = 0;
        for (DynamicField logicalField : logical.fields()) {
            EncryptedFieldDefinition definition = logical.protections()
                                                         .encrypted(logicalField.name())
                                                         .orElse(null);
            int groupSize = 1;
            if (definition != null) {
                if (definition.searchModes().contains(EncryptedSearchMode.EXACT)) {
                    groupSize++;
                }
                groupSize += definition.suffixLengths().size();
            }
            for (int offset = 0; offset < groupSize; offset++) {
                if (physicalIndex >= physicalFields.size()) {
                    throw layoutMismatch();
                }
                DynamicField physicalField = physicalFields.get(physicalIndex++);
                owners.put(physicalField.normalizedName(), logicalField.normalizedName());
            }
        }
        if (physicalIndex != physicalFields.size()) {
            throw layoutMismatch();
        }
        return Map.copyOf(owners);
    }

    private static IllegalArgumentException layoutMismatch() {
        return new IllegalArgumentException("protected physical form does not match logical field layout");
    }

    private static Set<String> normalized(DynamicForm form, Map<String, Object> values) {
        Set<String> names = new LinkedHashSet<>();
        values.keySet().forEach(name -> names.add(form.field(name).normalizedName()));
        return Set.copyOf(names);
    }
}
