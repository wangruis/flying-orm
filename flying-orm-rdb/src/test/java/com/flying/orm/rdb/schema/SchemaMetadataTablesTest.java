package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.flying.orm.rdb.schema.SchemaMigrationSupport.metadataTables;
import static org.junit.jupiter.api.Assertions.*;

class SchemaMetadataTablesTest {
    @Test
    void ordinaryTablesKeepDeclarationOrderExactNamesAndAnImmutableResult() {
        DynamicForm form = DynamicForm.builder("accounts", "accounts")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        assertEquals(List.of(form.relationIdentity()), metadataTables(form));
        List<RelationIdentity> result = metadataTables(form,
                List.of("audit", "accounts", "Accounts", "audit").stream().map(RelationIdentity::table).toList());
        assertEquals(List.of("accounts", "audit", "Accounts").stream().map(RelationIdentity::table).toList(), result);
        assertThrows(UnsupportedOperationException.class, result::clear);
    }

    @Test
    void planAndDirectFormIncludeContainsTableExactlyOnce() {
        DynamicForm form = DynamicForm.builder("accounts", "accounts")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("note", "VARCHAR"))
                .encrypted("note", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
        RelationIdentity side = ProtectedContainsLayout.resolve(form).orElseThrow().table().relationIdentity();
        assertEquals(List.of(form.relationIdentity(), side), metadataTables(form));
        SchemaMigrationPlan plan = new SchemaMigrationPlan(form.toTableDefinition(), true,
                List.of(), List.of(), List.of(side, RelationIdentity.table("audit"), form.relationIdentity(), side));
        assertEquals(List.of(form.relationIdentity(), side, RelationIdentity.table("audit")),
                SchemaMigrationSupport.metadataRelations(plan));
    }

    @Test
    void containsCreationPreservesBothTablesOperationsAndSql() {
        DynamicForm form = DynamicForm.builder("accounts", "accounts")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("note", "VARCHAR"))
                .encrypted("note", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(com.flying.orm.rdb.dialect.RdbDialect.postgresql())
                .migrationPlanner().createTablePlan(form, List.of(), List.of());
        assertEquals(2, plan.operations().size());
        assertTrue(plan.operations().stream().allMatch(operation -> operation.kind() == SchemaOperation.Kind.CREATE_TABLE));
        assertEquals(metadataTables(form), plan.operations().stream()
                .map(SchemaOperation::relation).toList());
        assertEquals(plan.requests(), plan.steps().stream().flatMap(step -> step.requests().stream()).toList());
    }
}
