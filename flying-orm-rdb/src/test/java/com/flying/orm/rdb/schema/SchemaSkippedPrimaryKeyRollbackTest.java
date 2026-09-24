package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaSkippedPrimaryKeyRollbackTest {

    private final FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.h2());

    @Test
    void skippedPrimaryKeyReplacementDoesNotRecreateTheRetainedColumn() {
        DynamicForm current = form(DynamicField.primaryKey("old_id", "BIGINT"));
        DynamicForm target = form(DynamicField.primaryKey("new_id", "BIGINT"));

        ReviewedSchemaMigrationPlan reviewed = review(current, target, SchemaMigrationOptions.safe());

        assertTrue(reviewed.migration().requests().isEmpty());
        assertTrue(reviewed.rollback().requests().isEmpty());
        assertEquals(List.of(SchemaRollbackGap.Kind.PRIMARY_KEY_REQUIRES_REVIEW),
                     reviewed.rollback().gaps().stream().map(SchemaRollbackGap::kind).toList());
    }

    @Test
    void skippedCompositeKeyRemovalDoesNotHideAnAppliedColumnAddition() {
        DynamicForm current = form(DynamicField.primaryKey("tenant_id", "BIGINT"),
                                   DynamicField.primaryKey("id", "BIGINT"));
        DynamicForm target = form(DynamicField.primaryKey("id", "BIGINT"),
                                  DynamicField.of("note", "VARCHAR"));

        ReviewedSchemaMigrationPlan reviewed = review(current, target, SchemaMigrationOptions.safe());

        assertEquals(1, reviewed.migration().requests().size());
        assertEquals(List.of("alter table records drop column note"),
                     reviewed.rollback().requests().stream().map(SqlRequest::sql).toList());
        assertFalse(reviewed.rollback().gaps().stream()
                            .anyMatch(gap -> gap.kind() == SchemaRollbackGap.Kind.DATA_CANNOT_BE_RESTORED));
    }

    @Test
    void restoresAnActuallyDroppedNonKeyColumnWhileLeavingTheSkippedKeyAlone() {
        DynamicForm current = form(DynamicField.primaryKey("old_id", "BIGINT"),
                                   DynamicField.of("note", "VARCHAR"));
        DynamicForm target = form(DynamicField.primaryKey("new_id", "BIGINT"));

        ReviewedSchemaMigrationPlan reviewed = review(current, target,
                                                       SchemaMigrationOptions.safe().allowDropColumn());

        assertEquals(1, reviewed.migration().requests().size());
        assertEquals(1, reviewed.rollback().requests().size());
        assertTrue(reviewed.rollback().requests().getFirst().sql().contains("note"));
        assertFalse(reviewed.rollback().requests().getFirst().sql().contains("old_id"));
        assertEquals(1, reviewed.rollback().gaps().stream()
                                .filter(gap -> gap.kind() == SchemaRollbackGap.Kind.DATA_CANNOT_BE_RESTORED).count());
    }

    private ReviewedSchemaMigrationPlan review(DynamicForm current, DynamicForm target,
                                                SchemaMigrationOptions options) {
        TableMetadata metadata = current.toTableMetadata();
        SchemaMigrationPlan migration = renderer.migrateSafelyPlan(metadata, target, List.of(), options);
        return SchemaMigrationReviewer.create(renderer).review(
                metadata, migration, SchemaMigrationReviewPolicy.allowBlocking());
    }

    private static DynamicForm form(DynamicField... fields) {
        DynamicForm.Builder builder = DynamicForm.builder("records", "records");
        for (DynamicField field : fields) {
            builder.addField(field);
        }
        return builder.build();
    }
}
