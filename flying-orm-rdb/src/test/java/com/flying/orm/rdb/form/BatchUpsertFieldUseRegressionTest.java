package com.flying.orm.rdb.form;

import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.internal.mapping.RepositoryUpsertValues;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BatchUpsertFieldUseRegressionTest {

    private static final DynamicForm FORM = DynamicForm.builder("approval", "approval_rows")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("name", "VARCHAR"))
            .addField(DynamicField.of("created", "VARCHAR"))
            .addField(DynamicField.of("changed", "VARCHAR"))
            .build();

    @Test
    void primaryKeyOnlyNoOpRequiresInsertButNotUpdate() {
        Map<String, Object> row = Map.of("id", 1L);
        UpsertFieldPlan plan = UpsertFieldPlan.create(
                FORM, row, FORM, List.of(FORM.field("id")), null);
        assertEquals(List.of(), plan.updateFields());
        assertDoesNotThrow(() -> approve(row, true, FieldUsePolicy.builder()
                .allow("id", FieldUse.INSERT).build()));
    }

    @Test
    void ordinaryUpsertDoesNotRequireUpdatingConflictKey() {
        assertDoesNotThrow(() -> approve(Map.of("id", 1L, "name", "new"), true,
                FieldUsePolicy.builder().allow("id", FieldUse.INSERT)
                        .allow("name", FieldUse.INSERT, FieldUse.UPDATE).build()));
    }

    @Test
    void ordinaryUpsertStillRequiresUpdatingActualSetColumn() {
        assertThrows(ScopeAccessException.class,
                () -> approve(Map.of("id", 1L, "name", "new"), true,
                        FieldUsePolicy.builder().allow("id", FieldUse.INSERT, FieldUse.UPDATE)
                                .allow("name", FieldUse.INSERT).build()));
    }

    @Test
    void ordinaryInsertDoesNotAcquireUpdateRequirements() {
        assertDoesNotThrow(() -> approve(Map.of("id", 1L, "name", "new"), false,
                FieldUsePolicy.builder().allow("id", FieldUse.INSERT)
                        .allow("name", FieldUse.INSERT).build()));
    }

    @Test
    void repositoryStagesUseTheirActualIndependentFieldPermissions() {
        RepositoryUpsertValues row = staged();
        UpsertFieldPlan plan = UpsertFieldPlan.create(FORM, row, FORM, FORM.fields(), row);
        assertEquals(List.of("id", "name", "created"),
                plan.insertFields().stream().map(DynamicField::name).toList());
        assertEquals(List.of("name", "changed"),
                plan.updateFields().stream().map(DynamicField::name).toList());
        assertDoesNotThrow(() -> approve(row, true, FieldUsePolicy.builder()
                .allow("id", FieldUse.INSERT)
                .allow("name", FieldUse.INSERT, FieldUse.UPDATE)
                .allow("created", FieldUse.INSERT)
                .allow("changed", FieldUse.UPDATE).build()));
    }

    @Test
    void repositoryInsertOnlyFieldStillRequiresInsertPermission() {
        assertThrows(ScopeAccessException.class, () -> approve(staged(), true,
                FieldUsePolicy.builder().allow("id", FieldUse.INSERT, FieldUse.UPDATE)
                        .allow("name", FieldUse.INSERT, FieldUse.UPDATE)
                        .allow("created", FieldUse.UPDATE)
                        .allow("changed", FieldUse.INSERT, FieldUse.UPDATE).build()));
    }

    @Test
    void repositoryUpdateOnlyFieldStillRequiresUpdatePermission() {
        assertThrows(ScopeAccessException.class, () -> approve(staged(), true,
                FieldUsePolicy.builder().allow("id", FieldUse.INSERT, FieldUse.UPDATE)
                        .allow("name", FieldUse.INSERT, FieldUse.UPDATE)
                        .allow("created", FieldUse.INSERT, FieldUse.UPDATE)
                        .allow("changed", FieldUse.INSERT).build()));
    }

    private static void approve(Map<String, Object> row, boolean upsert, FieldUsePolicy policy) {
        FieldUseGuard.approveBatchInsert(FORM, row, DataScope.none(), upsert, policy);
    }

    private static RepositoryUpsertValues staged() {
        return EntityValues.createUncached(StagedEntity.class)
                .repositoryUpsertValues(new StagedEntity(1L, "new", "initial", "modified"));
    }

    @TableName("approval_rows")
    record StagedEntity(@TableId Long id, String name,
                        @TableField(updateStrategy = FieldStrategy.NEVER) String created,
                        @TableField(insertStrategy = FieldStrategy.NEVER) String changed) {
    }
}
