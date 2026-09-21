package com.flying.orm.rdb.form;

import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.internal.mapping.RepositoryUpsertValues;
import com.flying.orm.rdb.protection.ProtectedFormLayout;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UpsertProtectedStageLayoutTest {

    @Test
    void keepsUpdateOnlyEncryptedPhysicalColumnsOutOfInsertStage() {
        DynamicForm logicalForm = DynamicForm.builder("protected_stage", "protected_stage")
                                             .addField(DynamicField.primaryKey("id", "BIGINT"))
                                             .addField(DynamicField.of("secret", "VARCHAR"))
                                             .encrypted("secret", EncryptedFieldDefinition.builder().build())
                                             .build();
        DynamicForm physicalForm = ProtectedFormLayout.physical(logicalForm);
        RepositoryUpsertValues values = EntityValues.createUncached(ProtectedStageEntity.class)
                                                    .repositoryUpsertValues(
                                                            new ProtectedStageEntity(7L, "rotate me"));

        UpsertFieldPlan plan = UpsertFieldPlan.create(
                logicalForm, values, physicalForm, physicalForm.fields(), values);

        List<String> encryptedColumns = physicalForm.fields().stream()
                                                    .filter(field -> !field.primaryKey())
                                                    .map(DynamicField::name)
                                                    .toList();
        assertEquals(List.of("id"), names(plan.insertFields()));
        assertEquals(encryptedColumns, names(plan.updateFields()));
    }

    @Test
    void keepsPreparedAutoTenantInInsertStageOnly() {
        DynamicForm form = DynamicForm.builder("tenant_stage", "tenant_stage")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("secret", "VARCHAR"))
                                      .addField(DynamicField.primaryKey("tenant_id", "VARCHAR"))
                                      .tenant("tenant_id", TenantStrategy.AUTO)
                                      .build();
        RepositoryUpsertValues values = EntityValues.createUncached(ProtectedStageEntity.class)
                                                    .repositoryUpsertValues(
                                                            new ProtectedStageEntity(7L, "rotate me"));
        Map<String, Object> prepared = new LinkedHashMap<>(values);
        prepared.put("tenant_id", "tenant-a");

        UpsertFieldPlan plan = UpsertFieldPlan.create(
                form, prepared, form, form.fields(), values);

        assertEquals(List.of("id", "tenant_id"), names(plan.insertFields()));
        assertEquals(List.of("secret"), names(plan.updateFields()));
    }

    @Test
    void rejectsDifferentRepositoryStagesForContainsSideIndex() {
        DynamicForm logicalForm = DynamicForm.builder("contains_stage", "contains_stage")
                                             .addField(DynamicField.primaryKey("id", "BIGINT"))
                                             .addField(DynamicField.of("secret", "VARCHAR"))
                                             .encrypted("secret", EncryptedFieldDefinition.builder()
                                                                                           .searchModes(
                                                                                                   EncryptedSearchMode.CONTAINS)
                                                                                           .build())
                                             .build();
        DynamicForm physicalForm = ProtectedFormLayout.physical(logicalForm);
        RepositoryUpsertValues values = EntityValues.createUncached(ProtectedStageEntity.class)
                                                    .repositoryUpsertValues(
                                                            new ProtectedStageEntity(7L, "rotate me"));

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> UpsertFieldPlan.create(
                        logicalForm, values, physicalForm, physicalForm.fields(), values));

        assertTrue(error.getMessage().contains("CONTAINS protected field"));
    }

    private static List<String> names(List<DynamicField> fields) {
        return fields.stream().map(DynamicField::name).toList();
    }

    @TableName("protected_stage")
    private static final class ProtectedStageEntity {

        @TableId(type = IdType.INPUT)
        private final Long id;

        @TableField(insertStrategy = FieldStrategy.NEVER, updateStrategy = FieldStrategy.ALWAYS)
        private final String secret;

        private ProtectedStageEntity(Long id, String secret) {
            this.id = id;
            this.secret = secret;
        }

        public Long getId() {
            return id;
        }

        public String getSecret() {
            return secret;
        }
    }
}
