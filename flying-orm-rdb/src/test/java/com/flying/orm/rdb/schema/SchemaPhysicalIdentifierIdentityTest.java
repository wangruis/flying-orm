package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** PostgreSQL quoted 列的仅大小写改名必须显式表达，不能被兼容查找吞掉。 */
class SchemaPhysicalIdentifierIdentityTest {

    @Test
    void acceptsExplicitCaseOnlyRenameAndRendersIt() {
        DynamicForm target = targetForm();
        TableMetadata current = currentMetadata();

        SchemaMigrationPlan plan = renderer().migrateSafelyPlan(
                current,
                target,
                List.of(),
                SchemaMigrationOptions.safe().renameColumn("customerId", "CustomerId"));

        assertEquals(List.of("alter table \"customers\" rename column \"customerId\" to \"CustomerId\""),
                     plan.requests().stream().map(SqlRequest::sql).toList());
        assertTrue(plan.skippedChanges().isEmpty());
    }

    @Test
    void keepsExactPhysicalNamesWhenBuildingRenameRollback() {
        SchemaMigrationPlan plan = renderer().migrateSafelyPlan(
                currentMetadata(),
                targetForm(),
                List.of(),
                SchemaMigrationOptions.safe().renameColumn("customerId", "CustomerId"));

        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(renderer())
                .review(currentMetadata(), plan,
                        SchemaMigrationReviewPolicy.allowBlocking()
                                                    .withColumnRenames(Map.of("customerId", "CustomerId")));

        assertEquals(List.of("alter table \"customers\" rename column \"CustomerId\" to \"customerId\""),
                     reviewed.rollback().requests().stream().map(SqlRequest::sql).toList());
    }

    @Test
    void reportsCaseOnlyPhysicalDifferenceWhenRenameIsNotDeclared() {
        SchemaMigrationPlan plan = renderer().migrateSafelyPlan(currentMetadata(), targetForm(), List.of());

        assertTrue(plan.requests().isEmpty());
        assertEquals(1, plan.skippedChanges().size());
        assertEquals(SkippedSchemaChange.Kind.CHANGE_COLUMN, plan.skippedChanges().getFirst().kind());
        assertEquals("CustomerId", plan.skippedChanges().getFirst().name());
    }

    @Test
    void renamesCaseDistinctPhysicalSourcesIndependently() {
        TableMetadata current = TableMetadata.builder("customers")
                                             .addColumn(ColumnMetadata.of("customerId", "BIGINT"))
                                             .addColumn(ColumnMetadata.of("CustomerId", "BIGINT"))
                                             .build();
        DynamicForm target = DynamicForm.builder("target", "customers")
                                        .addField(DynamicField.of("lowerCustomerId", "BIGINT"))
                                        .addField(DynamicField.of("upperCustomerId", "BIGINT"))
                                        .build();
        SchemaMigrationOptions options = SchemaMigrationOptions.safe()
                                                                 .renameColumn("customerId", "lowerCustomerId")
                                                                 .renameColumn("CustomerId", "upperCustomerId");

        SchemaMigrationPlan plan = renderer().migrateSafelyPlan(current, target, List.of(), options);

        assertEquals(List.of(
                "alter table \"customers\" rename column \"customerId\" to \"lowerCustomerId\"",
                "alter table \"customers\" rename column \"CustomerId\" to \"upperCustomerId\""),
                     plan.requests().stream().map(SqlRequest::sql).toList());
        assertTrue(plan.skippedChanges().isEmpty());
    }

    @Test
    void reportsUnrepresentedCaseDistinctPhysicalColumn() {
        SchemaMigrationPlan plan = renderer().migrateSafelyPlan(
                caseDistinctCurrentMetadata(), targetForm(), List.of());

        assertTrue(plan.requests().isEmpty());
        assertEquals(List.of(SkippedSchemaChange.Kind.DROP_COLUMN),
                     plan.skippedChanges().stream().map(SkippedSchemaChange::kind).toList());
        assertEquals("customerId", plan.skippedChanges().getFirst().name());
    }

    @Test
    void restoresDroppedCaseDistinctPhysicalColumnInRollback() {
        TableMetadata current = caseDistinctCurrentMetadata();
        SchemaMigrationPlan plan = renderer().migrateSafelyPlan(
                current,
                targetForm(),
                List.of(),
                SchemaMigrationOptions.safe().allowDropColumn());

        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(renderer())
                .review(current, plan, SchemaMigrationReviewPolicy.allowBlocking());

        assertEquals(List.of("alter table \"customers\" drop column \"customerId\""),
                     plan.requests().stream().map(SqlRequest::sql).toList());
        assertTrue(reviewed.rollback().requests().stream()
                           .map(SqlRequest::sql)
                           .anyMatch(sql -> sql.startsWith(
                                   "alter table \"customers\" add column \"customerId\" ")));
    }

    @Test
    void retainsMatchedCaseDistinctIndexAsOnePhysicalObject() {
        IndexMetadata currentIndex = IndexMetadata.builder("IX_customer")
                                                  .addColumn("customerId")
                                                  .build();
        IndexMetadata targetIndex = IndexMetadata.builder("ix_CUSTOMER")
                                                 .addColumn("customerId")
                                                 .build();
        TableMetadata current = TableMetadata.builder("customers")
                                              .addColumn(ColumnMetadata.of("customerId", "BIGINT"))
                                              .addIndex(currentIndex)
                                              .build();

        SchemaMigrationPlan plan = renderer().migrateSafelyPlan(
                current, sameCaseTargetForm(), List.of(targetIndex));

        assertTrue(plan.requests().isEmpty());
        assertEquals(List.of(SkippedSchemaChange.Kind.CHANGE_INDEX),
                     plan.skippedChanges().stream().map(SkippedSchemaChange::kind).toList());
    }

    @Test
    void treatsCaseDistinctTargetIndexesAsSeparatePhysicalObjects() {
        IndexMetadata currentIndex = IndexMetadata.builder("ix_customer")
                                                  .addColumn("customerId")
                                                  .build();
        IndexMetadata additionalIndex = IndexMetadata.builder("IX_CUSTOMER")
                                                     .addColumn("customerId")
                                                     .build();
        TableMetadata current = TableMetadata.builder("customers")
                                              .addColumn(ColumnMetadata.of("customerId", "BIGINT"))
                                              .addIndex(currentIndex)
                                              .build();

        SchemaMigrationPlan plan = renderer().migrateSafelyPlan(
                current, sameCaseTargetForm(), List.of(currentIndex, additionalIndex));
        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(renderer())
                .review(current, plan, SchemaMigrationReviewPolicy.allowBlocking());

        assertEquals(1, plan.requests().size());
        assertTrue(plan.requests().getFirst().sql().contains("\"IX_CUSTOMER\""));
        assertTrue(plan.skippedChanges().isEmpty());
        assertEquals(1, reviewed.rollback().requests().size());
        assertTrue(reviewed.rollback().requests().getFirst().sql().contains("\"IX_CUSTOMER\""));
    }

    @Test
    void retainsMatchedCaseDistinctForeignKeyAsOnePhysicalObject() {
        ForeignKeyMetadata currentForeignKey = new ForeignKeyMetadata(
                "fk_customer", List.of("customerId"), "accounts", List.of("id"));
        ForeignKeyMetadata targetForeignKey = new ForeignKeyMetadata(
                "FK_CUSTOMER", List.of("customerId"), "accounts", List.of("id"));
        TableMetadata current = TableMetadata.builder("customers")
                                              .addColumn(ColumnMetadata.of("customerId", "BIGINT"))
                                              .addForeignKey(currentForeignKey)
                                              .build();

        SchemaMigrationPlan plan = renderer().migrateSafelyPlan(
                current, sameCaseTargetForm(), List.of(), List.of(targetForeignKey), SchemaMigrationOptions.safe());

        assertTrue(plan.requests().isEmpty());
        assertEquals(List.of(SkippedSchemaChange.Kind.CHANGE_FOREIGN_KEY),
                     plan.skippedChanges().stream().map(SkippedSchemaChange::kind).toList());
    }

    @Test
    void treatsCaseDistinctTargetForeignKeysAsSeparatePhysicalObjects() {
        ForeignKeyMetadata currentForeignKey = new ForeignKeyMetadata(
                "fk_customer", List.of("customerId"), "accounts", List.of("id"));
        ForeignKeyMetadata additionalForeignKey = new ForeignKeyMetadata(
                "FK_CUSTOMER", List.of("customerId"), "accounts", List.of("id"));
        TableMetadata current = TableMetadata.builder("customers")
                                              .addColumn(ColumnMetadata.of("customerId", "BIGINT"))
                                              .addForeignKey(currentForeignKey)
                                              .build();

        SchemaMigrationPlan plan = renderer().migrateSafelyPlan(
                current,
                sameCaseTargetForm(),
                List.of(),
                List.of(currentForeignKey, additionalForeignKey),
                SchemaMigrationOptions.safe());

        assertEquals(List.of(SkippedSchemaChange.Kind.ADD_FOREIGN_KEY),
                     plan.skippedChanges().stream().map(SkippedSchemaChange::kind).toList());
        assertEquals("FK_CUSTOMER", plan.skippedChanges().getFirst().name());
    }

    private static FormSchemaSqlRenderer renderer() {
        return FormSchemaSqlRenderer.create(RdbDialect.postgresql());
    }

    private static TableMetadata currentMetadata() {
        return TableMetadata.builder("customers")
                            .addColumn(ColumnMetadata.of("customerId", "BIGINT"))
                            .build();
    }

    private static TableMetadata caseDistinctCurrentMetadata() {
        return TableMetadata.builder("customers")
                            .addColumn(ColumnMetadata.of("customerId", "BIGINT"))
                            .addColumn(ColumnMetadata.of("CustomerId", "BIGINT"))
                            .build();
    }

    private static DynamicForm targetForm() {
        return DynamicForm.builder("target", "customers")
                          .addField(DynamicField.of("CustomerId", "BIGINT"))
                          .build();
    }

    private static DynamicForm sameCaseTargetForm() {
        return DynamicForm.builder("target", "customers")
                          .addField(DynamicField.of("customerId", "BIGINT"))
                          .build();
    }
}
