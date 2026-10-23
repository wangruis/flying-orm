package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationIdentity;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.ReferentialAction;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
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
    void detectsForeignKeyActionAndReferenceNamespaceChanges() {
        ForeignKeyDefinition currentKey = foreignKey(RelationIdentity.table("accounts"),
                ReferentialAction.NO_ACTION, ReferentialAction.NO_ACTION);
        RelationalTableDefinition current = RelationalTableDefinition.builder("customers")
                .addColumn(DynamicField.of("customerId", "BIGINT").column()).addForeignKey(currentKey).build();
        List<String> references = List.of("references \"auth\".\"accounts\" (\"id\")",
                "references \"accounts\" (\"id\") on delete cascade",
                "references \"accounts\" (\"id\") on update cascade");
        int index = 0;
        for (ForeignKeyDefinition target : List.of(
                foreignKey(RelationIdentity.of(null, "auth", "accounts"), ReferentialAction.NO_ACTION, ReferentialAction.NO_ACTION),
                foreignKey(RelationIdentity.table("accounts"), ReferentialAction.CASCADE, ReferentialAction.NO_ACTION),
                foreignKey(RelationIdentity.table("accounts"), ReferentialAction.NO_ACTION, ReferentialAction.CASCADE))) {
            SchemaMigrationPlan plan = renderer().migrateSafelyPlan(current, sameCaseTargetForm(),
                    List.of(), List.of(target), SchemaMigrationOptions.safe());
            assertEquals(List.of("alter table \"customers\" drop constraint \"fk_customer\", "
                    + "add constraint \"fk_customer\" foreign key (\"customerId\") "
                            + references.get(index++)), plan.sqlTexts());
            assertTrue(plan.skippedChanges().isEmpty());
        }
    }

    private static ForeignKeyDefinition foreignKey(RelationIdentity reference,
                                                    ReferentialAction onDelete, ReferentialAction onUpdate) {
        return ForeignKeyDefinition.builder("fk_customer").addColumn("customerId")
                .reference(reference).addReferenceColumn("id").onDelete(onDelete).onUpdate(onUpdate).build();
    }

    @Test
    void acceptsExplicitCaseOnlyRenameAndRendersIt() {
        DynamicForm target = targetForm();
        RelationalTableDefinition current = currentMetadata();

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
    void doesNotRebuildAnIndexThatOnlyFollowsAColumnRename() {
        IndexDefinition currentIndex = IndexDefinition.builder("ix_customer")
                                                  .addColumn("customerId")
                                                  .build();
        IndexDefinition targetIndex = IndexDefinition.builder("ix_customer")
                                                 .addColumn("CustomerId")
                                                 .build();
        RelationalTableDefinition current = RelationalTableDefinition.builder("customers")
                                             .addColumn(DynamicField.of("customerId", "BIGINT").column())
                                             .addIndex(currentIndex)
                                             .build();
        SchemaMigrationOptions options = SchemaMigrationOptions.safe()
                                                                 .renameColumn("customerId", "CustomerId");
        SchemaMigrationPlan plan = renderer().migrateSafelyPlan(
                current, targetForm(), List.of(targetIndex), options);

        SchemaMigrationPlan reviewed = renderer().renderPlan(current, null, plan, SchemaMigrationOptions.safe());

        assertEquals(List.of("alter table \"customers\" rename column \"customerId\" to \"CustomerId\""),
                     plan.requests().stream().map(SqlRequest::sql).toList());
    }

    @Test
    void rebuildsAChangedIndexAfterRenamingItsColumn() {
        IndexDefinition currentIndex = IndexDefinition.builder("ix_customer")
                                                  .addColumn("customerId")
                                                  .build();
        IndexDefinition targetIndex = IndexDefinition.builder("ix_customer")
                                                 .unique()
                                                 .addColumn("CustomerId")
                                                 .build();
        RelationalTableDefinition current = RelationalTableDefinition.builder("customers")
                                             .addColumn(DynamicField.of("customerId", "BIGINT").column())
                                             .addIndex(currentIndex)
                                             .build();
        SchemaMigrationOptions options = SchemaMigrationOptions.safe()
                                                                 .allowRebuildIndex()
                                                                 .renameColumn("customerId", "CustomerId");
        SchemaMigrationPlan plan = renderer().migrateSafelyPlan(
                current, targetForm(), List.of(targetIndex), options);

        SchemaMigrationPlan reviewed = renderer().renderPlan(current, null, plan, SchemaMigrationOptions.safe());

        List<String> sql = plan.sqlTexts();
        assertEquals(3, sql.size());
        assertEquals("alter table \"customers\" rename column \"customerId\" to \"CustomerId\"", sql.get(0));
        assertEquals("drop index \"ix_customer\"", sql.get(1));
        assertEquals("create unique index \"ix_customer\" on \"customers\" (\"CustomerId\" asc)", sql.get(2));
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
        RelationalTableDefinition current = RelationalTableDefinition.builder("customers")
                                             .addColumn(DynamicField.of("customerId", "BIGINT").column())
                                             .addColumn(DynamicField.of("CustomerId", "BIGINT").column())
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
    void dropsOnlyTheRequestedCaseDistinctPhysicalColumn() {
        RelationalTableDefinition current = caseDistinctCurrentMetadata();
        SchemaMigrationPlan plan = renderer().migrateSafelyPlan(
                current,
                targetForm(),
                List.of(),
                SchemaMigrationOptions.safe().allowDropColumn());

        SchemaMigrationPlan reviewed = renderer().renderPlan(current, null, plan, SchemaMigrationOptions.safe());

        assertEquals(List.of("alter table \"customers\" drop column \"customerId\""),
                     plan.requests().stream().map(SqlRequest::sql).toList());
    }

    @Test
    void retainsMatchedCaseDistinctIndexAsOnePhysicalObject() {
        IndexDefinition currentIndex = IndexDefinition.builder("IX_customer")
                                                  .addColumn("customerId")
                                                  .build();
        IndexDefinition targetIndex = IndexDefinition.builder("ix_CUSTOMER")
                                                 .addColumn("customerId")
                                                 .build();
        RelationalTableDefinition current = RelationalTableDefinition.builder("customers")
                                              .addColumn(DynamicField.of("customerId", "BIGINT").column())
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
        IndexDefinition currentIndex = IndexDefinition.builder("ix_customer")
                                                  .addColumn("customerId")
                                                  .build();
        IndexDefinition additionalIndex = IndexDefinition.builder("IX_CUSTOMER")
                                                     .addColumn("customerId")
                                                     .build();
        RelationalTableDefinition current = RelationalTableDefinition.builder("customers")
                                              .addColumn(DynamicField.of("customerId", "BIGINT").column())
                                              .addIndex(currentIndex)
                                              .build();

        SchemaMigrationPlan plan = renderer().migrateSafelyPlan(
                current, sameCaseTargetForm(), List.of(currentIndex, additionalIndex));
        SchemaMigrationPlan reviewed = renderer().renderPlan(current, null, plan, SchemaMigrationOptions.safe());

        assertEquals(1, plan.requests().size());
        assertTrue(plan.requests().getFirst().sql().contains("\"IX_CUSTOMER\""));
        assertTrue(plan.skippedChanges().isEmpty());
    }

    @Test
    void retainsMatchedCaseDistinctForeignKeyAsOnePhysicalObject() {
        ForeignKeyDefinition currentForeignKey = ForeignKeyDefinition.builder("fk_customer").addColumn("customerId")
                .reference(RelationIdentity.table("accounts")).addReferenceColumn("id").build();
        ForeignKeyDefinition targetForeignKey = ForeignKeyDefinition.builder("FK_CUSTOMER").addColumn("customerId")
                .reference(RelationIdentity.table("accounts")).addReferenceColumn("id").build();
        RelationalTableDefinition current = RelationalTableDefinition.builder("customers")
                                              .addColumn(DynamicField.of("customerId", "BIGINT").column())
                                              .addForeignKey(currentForeignKey)
                                              .build();

        SchemaMigrationPlan plan = renderer().migrateSafelyPlan(
                current, sameCaseTargetForm(), List.of(), List.of(targetForeignKey), SchemaMigrationOptions.safe());

        assertEquals(List.of("alter table \"customers\" drop constraint \"fk_customer\", "
                + "add constraint \"FK_CUSTOMER\" foreign key (\"customerId\")"
                        + " references \"accounts\" (\"id\")"), plan.sqlTexts());
        assertTrue(plan.skippedChanges().isEmpty());
    }

    @Test
    void treatsCaseDistinctTargetForeignKeysAsSeparatePhysicalObjects() {
        ForeignKeyDefinition currentForeignKey = ForeignKeyDefinition.builder("fk_customer").addColumn("customerId")
                .reference(RelationIdentity.table("accounts")).addReferenceColumn("id").build();
        ForeignKeyDefinition additionalForeignKey = ForeignKeyDefinition.builder("FK_CUSTOMER").addColumn("customerId")
                .reference(RelationIdentity.table("accounts")).addReferenceColumn("id").build();
        RelationalTableDefinition current = RelationalTableDefinition.builder("customers")
                                              .addColumn(DynamicField.of("customerId", "BIGINT").column())
                                              .addForeignKey(currentForeignKey)
                                              .build();

        SchemaMigrationPlan plan = renderer().migrateSafelyPlan(
                current,
                sameCaseTargetForm(),
                List.of(),
                List.of(currentForeignKey, additionalForeignKey),
                SchemaMigrationOptions.safe());

        assertEquals(List.of("alter table \"customers\" add constraint \"FK_CUSTOMER\" foreign key (\"customerId\")"
                + " references \"accounts\" (\"id\")"), plan.sqlTexts());
        assertTrue(plan.skippedChanges().isEmpty());
    }

    private static FormSchemaSqlRenderer renderer() {
        return FormSchemaSqlRenderer.create(RdbDialect.postgresql());
    }

    private static RelationalTableDefinition currentMetadata() {
        return RelationalTableDefinition.builder("customers")
                            .addColumn(DynamicField.of("customerId", "BIGINT").column())
                            .build();
    }

    private static RelationalTableDefinition caseDistinctCurrentMetadata() {
        return RelationalTableDefinition.builder("customers")
                            .addColumn(DynamicField.of("customerId", "BIGINT").column())
                            .addColumn(DynamicField.of("CustomerId", "BIGINT").column())
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
