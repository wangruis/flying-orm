package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaSkippedPrimaryKeyChangeTest {

    private final FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.h2());

    @Test
    void skippedPrimaryKeyReplacementDoesNotRecreateTheRetainedColumn() {
        DynamicForm current = form(DynamicField.primaryKey("old_id", "BIGINT"));
        DynamicForm target = form(DynamicField.primaryKey("new_id", "BIGINT"));

        SchemaMigrationPlan reviewed = review(current, target, SchemaMigrationOptions.safe());

        assertTrue(reviewed.requests().isEmpty());
        assertEquals(List.of(SkippedSchemaChange.Kind.CHANGE_PRIMARY_KEY),
                reviewed.skippedChanges().stream().map(SkippedSchemaChange::kind).toList());
    }

    @Test
    void partialCompositeKeyDeclarationPreservesUnmentionedKey() {
        DynamicForm current = form(DynamicField.primaryKey("tenant_id", "BIGINT"),
                                   DynamicField.primaryKey("id", "BIGINT"));
        DynamicForm target = form(DynamicField.primaryKey("id", "BIGINT"),
                                  DynamicField.of("note", "VARCHAR"));

        SchemaMigrationPlan reviewed = review(current, target, SchemaMigrationOptions.safe());

        assertEquals(List.of("alter table records add column note VARCHAR"),
                reviewed.requests().stream().map(SqlRequest::sql).toList());
        assertEquals(List.of(SkippedSchemaChange.Kind.DROP_COLUMN),
                reviewed.skippedChanges().stream().map(SkippedSchemaChange::kind).toList());
        assertEquals("tenant_id", reviewed.skippedChanges().getFirst().name());
        assertEquals(reviewed.requests(), reviewed.requestsForExecution());
    }

    @Test
    void dropsOnlyTheNonKeyColumnWhileLeavingTheSkippedKeyAlone() {
        DynamicForm current = form(DynamicField.primaryKey("old_id", "BIGINT"),
                                   DynamicField.of("note", "VARCHAR"));
        DynamicForm target = form(DynamicField.primaryKey("new_id", "BIGINT"));

        SchemaMigrationPlan reviewed = review(current, target,
                                                       SchemaMigrationOptions.safe().allowDropColumn());

        assertEquals(List.of("alter table records drop column note"),
                reviewed.requests().stream().map(SqlRequest::sql).toList());
    }

    private SchemaMigrationPlan review(DynamicForm current, DynamicForm target,
                                                SchemaMigrationOptions options) {
        RelationalTableDefinition metadata = current.toTableDefinition();
        SchemaMigrationPlan migration = renderer.migrateSafelyPlan(metadata, target, List.of(), options);
        return renderer.renderPlan(metadata, null, migration, SchemaMigrationOptions.safe());
    }

    private static DynamicForm form(DynamicField... fields) {
        DynamicForm.Builder builder = DynamicForm.builder("records", "records");
        for (DynamicField field : fields) {
            builder.addField(field);
        }
        return builder.build();
    }
}
