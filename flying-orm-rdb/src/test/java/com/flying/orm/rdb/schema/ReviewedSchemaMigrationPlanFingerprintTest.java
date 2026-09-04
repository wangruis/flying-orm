package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 审核指纹必须无歧义地绑定迁移计划中的每一个结构化条目。 */
class ReviewedSchemaMigrationPlanFingerprintTest {

    @Test
    void distinguishesOneMultilineStatementFromTwoStatements() {
        DynamicForm target = DynamicForm.builder("orders", "orders")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();
        ReviewedSchemaMigrationPlan multiline = reviewed(target, List.of(
                new SqlRequest("select 1\nforward:select 2", List.of())));
        ReviewedSchemaMigrationPlan separate = reviewed(target, List.of(
                new SqlRequest("select 1", List.of()),
                new SqlRequest("select 2", List.of())));

        assertNotEquals(multiline.fingerprint(), separate.fingerprint());
    }

    @Test
    void rejectsBoundParametersThatCannotBeCoveredByAStableDdlApproval() {
        DynamicForm target = DynamicForm.builder("orders", "orders")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();

        assertThrows(IllegalArgumentException.class, () -> reviewed(target, List.of(
                new SqlRequest("alter table orders add label varchar(20) default ?", List.of("new")))));
    }

    @Test
    void distinguishesStructuredDetailValuesWithTheSameStringRendering() {
        DynamicForm target = DynamicForm.builder("orders", "orders")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();
        SkippedSchemaChange oneValue = skipped(List.of("a, b"));
        SkippedSchemaChange twoValues = skipped(List.of("a", "b"));

        assertNotEquals(reviewed(target, List.of(), List.of(oneValue)).fingerprint(),
                        reviewed(target, List.of(), List.of(twoValues)).fingerprint());
    }

    @Test
    void rejectsArbitraryRecordDetailsInsteadOfReflectingOverApplicationObjects() {
        DynamicForm target = DynamicForm.builder("orders", "orders")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();
        SkippedSchemaChange unsupported = new SkippedSchemaChange(
                SkippedSchemaChange.Kind.CHANGE_COLUMN,
                "status",
                "review required",
                Map.of("custom", new ApplicationDetail("secret")),
                List.of());

        ReviewedSchemaMigrationPlan plan = reviewed(target, List.of(), List.of(unsupported));

        assertThrows(IllegalArgumentException.class, plan::fingerprint);
    }

    @Test
    void encodesTheForeignKeyDetailShapePublishedByTheSchemaPlanner() {
        DynamicForm target = DynamicForm.builder("orders", "orders")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();
        ForeignKeyMetadata foreignKey = new ForeignKeyMetadata(
                "fk_order_customer", List.of("customer_id"), "customer", List.of("id"));
        SkippedSchemaChange skipped = new SkippedSchemaChange(
                SkippedSchemaChange.Kind.ADD_FOREIGN_KEY,
                foreignKey.name(),
                "foreign key changes are planned only",
                Map.of("target", foreignKey),
                List.of("review the foreign key"));

        String first = reviewed(target, List.of(), List.of(skipped)).fingerprint();
        String second = reviewed(target, List.of(), List.of(skipped)).fingerprint();

        assertEquals(first, second);
    }

    @Test
    void keepsApprovalBoundToTheExactVersionedFingerprint() {
        DynamicForm target = DynamicForm.builder("orders", "orders")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();
        ReviewedSchemaMigrationPlan approved = reviewedWithGap(
                target, List.of(new SqlRequest("alter table orders add label varchar(20)", List.of())));
        SchemaMigrationApproval approval = SchemaMigrationApproval.approve(approved, "change ticket 42");
        ReviewedSchemaMigrationPlan changed = reviewedWithGap(
                target, List.of(new SqlRequest("alter table orders add note varchar(20)", List.of())));

        assertEquals(approved.migration().requests(), approved.requestsForExecution(approval));
        SchemaMigrationRejectedException rejection = assertThrows(
                SchemaMigrationRejectedException.class,
                () -> changed.requestsForExecution(approval));
        assertEquals(SchemaMigrationFailureCode.APPROVAL_REQUIRED, rejection.failureCode());
        assertEquals(changed.fingerprint(), rejection.planFingerprint());
    }

    private static ReviewedSchemaMigrationPlan reviewed(DynamicForm target, List<SqlRequest> requests) {
        return reviewed(target, requests, List.of());
    }

    private static ReviewedSchemaMigrationPlan reviewed(DynamicForm target,
                                                         List<SqlRequest> requests,
                                                         List<SkippedSchemaChange> skippedChanges) {
        SchemaMigrationPlan migration = new SchemaMigrationPlan(
                target, List.of(), List.of(), true, requests, skippedChanges);
        return new ReviewedSchemaMigrationPlan(
                migration,
                new SchemaRollbackPlan(List.of(), List.of()),
                new OnlineDdlReview(OnlineDdlMode.ALLOW_BLOCKING, List.of()));
    }

    private static ReviewedSchemaMigrationPlan reviewedWithGap(DynamicForm target,
                                                                List<SqlRequest> requests) {
        SchemaMigrationPlan migration = new SchemaMigrationPlan(
                target, List.of(), List.of(), true, requests, List.of());
        SchemaRollbackGap gap = new SchemaRollbackGap(
                SchemaRollbackGap.Kind.DATA_CANNOT_BE_RESTORED,
                target.table(),
                "test rollback gap");
        return new ReviewedSchemaMigrationPlan(
                migration,
                new SchemaRollbackPlan(List.of(), List.of(gap)),
                new OnlineDdlReview(OnlineDdlMode.ALLOW_BLOCKING, List.of()));
    }

    private static SkippedSchemaChange skipped(List<String> values) {
        return new SkippedSchemaChange(SkippedSchemaChange.Kind.CHANGE_COLUMN,
                                       "status",
                                       "review required",
                                       Map.of("values", values),
                                       List.of());
    }

    private record ApplicationDetail(String value) {
    }
}
