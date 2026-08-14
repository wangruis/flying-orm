package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证回滚计划不会掩盖数据缺口，在线强制模式也不会放行可能锁表的 DDL。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
class SchemaMigrationReviewerTest {

    /** 已存在但受本次 DDL 影响的辅助表只参与缓存失效，复审时不能被当成新表生成删除回滚。 */
    @Test
    void doesNotDropExistingAffectedAuxiliaryTableWhenReviewedAgain() {
        TableMetadata current = TableMetadata.builder("users")
                                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                                             .build();
        DynamicForm target = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();
        SchemaMigrationPlan combined = new SchemaMigrationPlan(
                target, List.of(), List.of(), true,
                List.of(new SqlRequest(
                        "alter table \"users_email_search\" add column \"token\" VARCHAR(64)",
                        List.of())),
                List.of(), List.of("users_email_search"));

        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()))
                .review(current, combined, SchemaMigrationReviewPolicy.preferOnline());

        assertTrue(reviewed.rollback().requests().stream()
                           .noneMatch(request -> request.sql().contains("drop table \"users_email_search\"")));
    }

    @Test
    void buildsReverseStructureStepsAndReportsIrrecoverableDroppedData() {
        TableMetadata current = TableMetadata.builder("users")
                                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                                             .addColumn(ColumnMetadata.of("name", "VARCHAR").withLength(64))
                                             .addColumn(ColumnMetadata.of("legacy", "VARCHAR"))
                                             .build();
        DynamicForm target = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("name", "TEXT"))
                                        .addField(DynamicField.of("email", "VARCHAR"))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());
        SchemaMigrationPlan plan = renderer.migrateSafelyPlan(
                current,
                target,
                List.of(),
                SchemaMigrationOptions.safe().allowColumnChange().allowDropColumn());

        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()))
                .review(current, plan, SchemaMigrationReviewPolicy.preferOnline());

        assertEquals(List.of("alter table \"users\" add column \"legacy\" VARCHAR(255)",
                             "alter table \"users\" alter column \"name\" type VARCHAR(64)",
                             "alter table \"users\" drop column \"email\""),
                     reviewed.rollback().requests().stream().map(request -> request.sql()).toList());
        assertFalse(reviewed.rollback().complete());
        assertEquals(SchemaRollbackGap.Kind.DATA_CANNOT_BE_RESTORED,
                     reviewed.rollback().gaps().getFirst().kind());
        assertTrue(reviewed.onlineDdl().requiresExternalOnlineTool());
        assertEquals(SchemaMigrationRiskLevel.CRITICAL, reviewed.riskLevel());
        assertTrue(reviewed.requiresExplicitApproval());
        assertThrows(IllegalStateException.class, reviewed::requestsForExecution);
        assertThrows(IllegalStateException.class,
                     () -> reviewed.requestsForExecution(new SchemaMigrationApproval("outdated-plan", "旧计划已审批")));
        assertEquals(plan.requests(),
                     reviewed.requestsForExecution(SchemaMigrationApproval.approve(reviewed, "备份已经确认")));
    }

    @Test
    void onlineRequiredModeRefusesPotentiallyBlockingStatements() {
        TableMetadata current = TableMetadata.builder("users")
                                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                                             .build();
        DynamicForm target = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("email", "VARCHAR"))
                                        .build();
        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                                                        .migrateSafelyPlan(current, target, List.of());
        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()))
                .review(current, plan, SchemaMigrationReviewPolicy.requireOnline());

        assertThrows(IllegalStateException.class, reviewed::requestsForExecution);
    }

    @Test
    void postgresqlOnlineRequiredModeUsesConcurrentIndexCreation() {
        TableMetadata current = TableMetadata.builder("users")
                                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                                             .addColumn(ColumnMetadata.of("email", "VARCHAR"))
                                             .build();
        DynamicForm target = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("email", "VARCHAR"))
                                        .build();
        IndexMetadata index = IndexMetadata.builder("idx_users_email").addColumn("email").build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());
        SchemaMigrationPlan plan = renderer.migrateSafelyPlan(current, target, List.of(index));

        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(renderer)
                .review(current, plan, SchemaMigrationReviewPolicy.requireOnline());

        assertTrue(reviewed.onlineDdl().executionAllowed());
        assertTrue(reviewed.onlineDdl().requiresNonTransactionalExecution());
        assertEquals(SchemaOnlineDdlSupport.CONCURRENT_INDEX, reviewed.onlineDdl().support());
        assertEquals(List.of("create index concurrently \"idx_users_email\" on \"users\" (\"email\")"),
                     reviewed.requestsForExecution().stream().map(request -> request.sql()).toList());
    }

    /**
     * 自动唯一索引名因全方言长度边界收短时，审核计划必须把唯一的旧名当作已保留对象，
     * 正向和回滚都不能为从未创建的新名生成索引 SQL。
     */
    @Test
    void retainsLegacyAutomaticUniqueIndexWithoutForwardOrRollbackSql() {
        DynamicForm target = automaticUniqueTarget();
        IndexMetadata targetIndex = target.toTableMetadata().indexes().getFirst();
        TableMetadata current = currentWithIndexes(
                target,
                IndexMetadata.builder("uk_customer_registry_external_reference_legacy")
                             .unique()
                             .addColumn("external_reference")
                             .build());
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());

        SchemaMigrationPlan plan = renderer.migrateSafelyPlan(current, target, List.of(targetIndex));
        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(renderer)
                                                                   .review(current,
                                                                           plan,
                                                                           SchemaMigrationReviewPolicy.allowBlocking());

        assertTrue(plan.requests().isEmpty());
        assertTrue(reviewed.rollback().requests().isEmpty());
    }

    /** 多个旧名形状相同时无法证明复用关系，审核器仍要保留严格名称匹配的新增与对应回滚。 */
    @Test
    void keepsAmbiguousLegacyAutomaticUniqueIndexesStrictInReviewedPlan() {
        DynamicForm target = automaticUniqueTarget();
        IndexMetadata targetIndex = target.toTableMetadata().indexes().getFirst();
        TableMetadata current = currentWithIndexes(
                target,
                IndexMetadata.builder("uk_legacy_first").unique().addColumn("external_reference").build(),
                IndexMetadata.builder("uk_legacy_second").unique().addColumn("external_reference").build());
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());

        SchemaMigrationPlan plan = renderer.migrateSafelyPlan(current, target, List.of(targetIndex));
        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(renderer)
                                                                   .review(current,
                                                                           plan,
                                                                           SchemaMigrationReviewPolicy.allowBlocking());

        assertEquals(1, plan.requests().size());
        assertEquals(List.of("drop index \"" + targetIndex.name() + "\""),
                     reviewed.rollback().requests().stream().map(request -> request.sql()).toList());
    }

    /** 显式唯一索引即使和旧索引同列，也不能被自动索引兼容规则吸收。 */
    @Test
    void keepsExplicitUniqueIndexNamesStrictInReviewedPlan() {
        DynamicForm target = DynamicForm.builder("customerRegistry", "CustomerRegistry")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("external_reference", "VARCHAR"))
                                        .build();
        IndexMetadata explicit = IndexMetadata.builder("idx_customer_reference")
                                               .unique()
                                               .addColumn("external_reference")
                                               .build();
        TableMetadata current = currentWithIndexes(
                target,
                IndexMetadata.builder("uk_legacy_reference").unique().addColumn("external_reference").build());
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());

        SchemaMigrationPlan plan = renderer.migrateSafelyPlan(current, target, List.of(explicit));
        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(renderer)
                                                                   .review(current,
                                                                           plan,
                                                                           SchemaMigrationReviewPolicy.allowBlocking());

        assertEquals(1, plan.requests().size());
        assertEquals(List.of("drop index \"idx_customer_reference\""),
                     reviewed.rollback().requests().stream().map(request -> request.sql()).toList());
    }

    /**
     * 主键顺序变化没有通用安全 SQL，但仍是一个必须绑定旧键、新键和人工步骤的危险计划。
     * 即使最终可执行 SQL 为空，也不能让空批准或另一份计划的批准指纹蒙混过关。
     */
    @Test
    void primaryKeyChangesRemainManualAndRequireTheExactPlanFingerprint() {
        TableMetadata current = TableMetadata.builder("users")
                                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                                             .addColumn(ColumnMetadata.primaryKey("tenant_id", "BIGINT"))
                                             .build();
        DynamicForm reordered = DynamicForm.builder("users", "users")
                                           .addField(DynamicField.primaryKey("tenant_id", "BIGINT"))
                                           .addField(DynamicField.primaryKey("id", "BIGINT"))
                                           .build();
        DynamicForm reduced = DynamicForm.builder("users", "users")
                                         .addField(DynamicField.primaryKey("id", "BIGINT"))
                                         .addField(DynamicField.of("tenant_id", "BIGINT"))
                                         .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());
        SchemaMigrationOptions options = SchemaMigrationOptions.safe().allowPrimaryKeyChange();
        ReviewedSchemaMigrationPlan reorderedReview = SchemaMigrationReviewer.create(renderer).review(
                current,
                renderer.migrateSafelyPlan(current, reordered, List.of(), options),
                SchemaMigrationReviewPolicy.allowBlocking());
        ReviewedSchemaMigrationPlan reducedReview = SchemaMigrationReviewer.create(renderer).review(
                current,
                renderer.migrateSafelyPlan(current, reduced, List.of(), options),
                SchemaMigrationReviewPolicy.allowBlocking());

        assertTrue(reorderedReview.migration().requests().isEmpty());
        assertTrue(reorderedReview.requiresExplicitApproval());
        assertEquals(SchemaMigrationRiskLevel.CRITICAL, reorderedReview.riskLevel());
        assertNotEquals(reorderedReview.fingerprint(), reducedReview.fingerprint());
        SchemaMigrationRejectedException rejected = assertThrows(
                SchemaMigrationRejectedException.class,
                () -> reorderedReview.requestsForExecution(
                        SchemaMigrationApproval.approve(reorderedReview, "主键脚本将由维护窗口人工执行")));
        assertEquals(SchemaMigrationFailureCode.MANUAL_ACTION_REQUIRED, rejected.failureCode());
        assertEquals(reorderedReview.fingerprint(), rejected.planFingerprint());
    }

    /** 放宽 nullable 可以安全前进，但回滚前必须处理迁移后写入的 null，因此审核结果要明确报告缺口。 */
    @Test
    void nullableRelaxationHasAnExactRollbackStatementAndDataGap() {
        TableMetadata current = TableMetadata.builder("users")
                                             .addColumn(ColumnMetadata.of("name", "VARCHAR")
                                                                      .withLength(64)
                                                                      .withNullable(false))
                                             .build();
        DynamicForm target = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.of("name", "VARCHAR").withLength(64))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.mysql());
        SchemaMigrationPlan plan = renderer.migrateSafelyPlan(current, target, List.of());

        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(renderer).review(
                current, plan, SchemaMigrationReviewPolicy.allowBlocking());

        assertEquals(List.of("alter table `users` modify column `name` VARCHAR(64) not null"),
                     reviewed.rollback().requests().stream().map(request -> request.sql()).toList());
        assertEquals(SchemaRollbackGap.Kind.DATA_CANNOT_BE_RESTORED,
                     reviewed.rollback().gaps().getFirst().kind());
        assertTrue(reviewed.requiresExplicitApproval());
    }

    /** MySQL 类型回滚必须重放原完整列定义，不能在恢复长度时丢失 NOT NULL 或内联注释。 */
    @Test
    void mysqlTypeRollbackReplaysTheCompleteOriginalColumnDefinition() {
        TableMetadata current = TableMetadata.builder("users")
                                             .addColumn(ColumnMetadata.of("name", "VARCHAR")
                                                                      .withLength(64)
                                                                      .withNullable(false)
                                                                      .withComment("original name"))
                                             .build();
        DynamicForm target = DynamicForm.builder("users", "users")
                                        .addField(DynamicField.of("name", "VARCHAR")
                                                              .withLength(128)
                                                              .withNullable(false)
                                                              .withComment("original name"))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.mysql());
        SchemaMigrationPlan plan = renderer.migrateSafelyPlan(current, target, List.of());

        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(renderer).review(
                current, plan, SchemaMigrationReviewPolicy.allowBlocking());

        assertEquals(List.of("alter table `users` modify column `name` VARCHAR(64) not null"
                                     + " comment 'original name'"),
                     reviewed.rollback().requests().stream().map(request -> request.sql()).toList());
    }

    private static DynamicForm automaticUniqueTarget() {
        return DynamicForm.builder("customerRegistry", "CustomerRegistry")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("external_reference", "VARCHAR").withUnique(true))
                          .build();
    }

    private static TableMetadata currentWithIndexes(DynamicForm target, IndexMetadata... indexes) {
        TableMetadata.Builder builder = TableMetadata.builder(target.table());
        target.toTableMetadata().columns().forEach(builder::addColumn);
        for (IndexMetadata index : indexes) {
            builder.addIndex(index);
        }
        return builder.build();
    }
}
