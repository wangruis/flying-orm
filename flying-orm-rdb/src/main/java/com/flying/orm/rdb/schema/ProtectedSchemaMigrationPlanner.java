package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * 把受保护字段的辅助表作为独立迁移段接入主表计划。
 *
 * <p>辅助表必须单独读取元数据，才能区分首次启用与重复迁移；审核时也必须生成自己的回滚段，避免只回滚主表而遗留
 * CONTAINS 令牌表。本类型只在 Schema 包内协作，不扩展公开迁移 API。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class ProtectedSchemaMigrationPlanner {

    private final SchemaMigrationPlanner planner;
    private final FormSchemaSqlRenderer renderer;
    private final SchemaTableSqlRenderer tables;

    ProtectedSchemaMigrationPlanner(SchemaMigrationPlanner planner, FormSchemaSqlRenderer renderer) {
        this.planner = Objects.requireNonNull(planner, "schema migration planner must not be null");
        this.renderer = Objects.requireNonNull(renderer, "form schema SQL renderer must not be null");
        this.tables = renderer.tableRenderer();
    }

    SchemaMigrationPlan appendCreatePlan(DynamicForm logical, SchemaMigrationPlan primary) {
        return ProtectedContainsLayout.resolve(logical)
                                      .map(layout -> combine(primary, createContainsPlan(layout)))
                                      .orElse(primary);
    }

    Mono<SchemaMigrationPlan> planExistingReactive(DynamicForm logical,
                                                   SchemaMigrationPlan primary,
                                                   ReactiveFormMetadataReader reader,
                                                   SchemaMigrationOptions options) {
        return ProtectedContainsLayout.resolve(logical)
                                      .map(layout -> readReactivePlan(layout, reader, options)
                                              .map(side -> combine(primary, side)))
                                      .orElseGet(() -> Mono.just(primary));
    }

    SchemaMigrationPlan planExistingJdbc(DynamicForm logical,
                                         SchemaMigrationPlan primary,
                                         Function<String, TableMetadata> lookup,
                                         SchemaMigrationOptions options) {
        return ProtectedContainsLayout.resolve(logical)
                                      .map(layout -> combine(primary, readJdbcPlan(layout, lookup, options)))
                                      .orElse(primary);
    }

    Mono<ReviewedSchemaMigrationPlan> reviewExistingReactive(DynamicForm logical,
                                                              TableMetadata primaryCurrent,
                                                              SchemaMigrationPlan primary,
                                                              ReactiveFormMetadataReader reader,
                                                              SchemaMigrationOptions options,
                                                              SchemaMigrationReviewPolicy policy) {
        ReviewedSchemaMigrationPlan primaryReview = reviewer().review(primaryCurrent, primary, policy);
        return ProtectedContainsLayout.resolve(logical)
                                      .map(layout -> readReactiveReviewed(layout, reader, options, policy)
                                              .map(side -> combine(primaryReview, side)))
                                      .orElseGet(() -> Mono.just(primaryReview));
    }

    ReviewedSchemaMigrationPlan reviewExistingJdbc(DynamicForm logical,
                                                    TableMetadata primaryCurrent,
                                                    SchemaMigrationPlan primary,
                                                    Function<String, TableMetadata> lookup,
                                                    SchemaMigrationOptions options,
                                                    SchemaMigrationReviewPolicy policy) {
        ReviewedSchemaMigrationPlan primaryReview = reviewer().review(primaryCurrent, primary, policy);
        return ProtectedContainsLayout.resolve(logical)
                                      .map(layout -> combine(primaryReview,
                                              readJdbcReviewed(layout, lookup, options, policy)))
                                      .orElse(primaryReview);
    }

    ReviewedSchemaMigrationPlan reviewCreated(DynamicForm logical,
                                               SchemaMigrationPlan primary,
                                               SchemaMigrationReviewPolicy policy) {
        ReviewedSchemaMigrationPlan primaryReview = reviewer().review(
                primary.target().toTableMetadata(), primary, policy);
        return ProtectedContainsLayout.resolve(logical)
                                      .map(layout -> combine(primaryReview, reviewer().review(
                                              layout.table().toTableMetadata(),
                                              createContainsPlan(layout),
                                              policy)))
                                      .orElse(primaryReview);
    }

    private Mono<SchemaMigrationPlan> readReactivePlan(ProtectedContainsLayout layout,
                                                       ReactiveFormMetadataReader reader,
                                                       SchemaMigrationOptions options) {
        return reader.readTable(layout.table().table())
                     .map(current -> migrateContainsPlan(layout, current, options))
                     .onErrorResume(ProtectedSchemaMigrationPlanner::isTableNotFound,
                                    failure -> Mono.just(createContainsPlan(layout)));
    }

    private SchemaMigrationPlan readJdbcPlan(ProtectedContainsLayout layout,
                                             Function<String, TableMetadata> lookup,
                                             SchemaMigrationOptions options) {
        try {
            return migrateContainsPlan(layout, lookup.apply(layout.table().table()), options);
        } catch (IllegalArgumentException failure) {
            if (!isTableNotFound(failure)) {
                throw failure;
            }
            return createContainsPlan(layout);
        }
    }

    private Mono<ReviewedSchemaMigrationPlan> readReactiveReviewed(ProtectedContainsLayout layout,
                                                                   ReactiveFormMetadataReader reader,
                                                                   SchemaMigrationOptions options,
                                                                   SchemaMigrationReviewPolicy policy) {
        return reader.readTable(layout.table().table())
                     .map(current -> reviewer().review(current, migrateContainsPlan(layout, current, options), policy))
                     .onErrorResume(ProtectedSchemaMigrationPlanner::isTableNotFound,
                                    failure -> Mono.just(reviewer().review(
                                            layout.table().toTableMetadata(), createContainsPlan(layout), policy)));
    }

    private ReviewedSchemaMigrationPlan readJdbcReviewed(ProtectedContainsLayout layout,
                                                          Function<String, TableMetadata> lookup,
                                                          SchemaMigrationOptions options,
                                                          SchemaMigrationReviewPolicy policy) {
        try {
            TableMetadata current = lookup.apply(layout.table().table());
            return reviewer().review(current, migrateContainsPlan(layout, current, options), policy);
        } catch (IllegalArgumentException failure) {
            if (!isTableNotFound(failure)) {
                throw failure;
            }
            return reviewer().review(layout.table().toTableMetadata(), createContainsPlan(layout), policy);
        }
    }

    private SchemaMigrationPlan migrateContainsPlan(ProtectedContainsLayout layout,
                                                     TableMetadata current,
                                                     SchemaMigrationOptions options) {
        return planner.migrateSafelyPlan(
                current, layout.table(), layout.indexes(), layout.foreignKeys(), options);
    }

    private SchemaMigrationPlan createContainsPlan(ProtectedContainsLayout layout) {
        List<SqlRequest> requests = new ArrayList<>(tables.createTable(layout.table()));
        requests.addAll(tables.createIndexes(layout.table().table(), layout.indexes()));
        layout.foreignKeys().forEach(foreignKey -> requests.add(
                tables.createCascadeForeignKey(layout.table().table(), foreignKey)));
        return new SchemaMigrationPlan(layout.table(), layout.indexes(), layout.foreignKeys(),
                                       false, requests, List.of());
    }

    private SchemaMigrationReviewer reviewer() {
        return SchemaMigrationReviewer.create(renderer);
    }

    private static SchemaMigrationPlan combine(SchemaMigrationPlan primary, SchemaMigrationPlan side) {
        List<SqlRequest> requests = new ArrayList<>(primary.requests());
        requests.addAll(side.requests());
        List<SkippedSchemaChange> skipped = new ArrayList<>(primary.skippedChanges());
        skipped.addAll(side.skippedChanges());
        List<String> additionalTables = new ArrayList<>(primary.additionalCreatedTables());
        additionalTables.addAll(side.additionalCreatedTables());
        if (!side.tableExists()) {
            additionalTables.add(side.target().table());
        }
        return new SchemaMigrationPlan(primary.target(), primary.targetIndexes(), primary.targetForeignKeys(),
                                       primary.tableExists(), requests, skipped, additionalTables);
    }

    private static ReviewedSchemaMigrationPlan combine(ReviewedSchemaMigrationPlan primary,
                                                       ReviewedSchemaMigrationPlan side) {
        SchemaMigrationPlan migration = combine(primary.migration(), side.migration());
        if (side.migration().hasExecutableSql()
                && !migration.additionalCreatedTables().contains(side.migration().target().table())) {
            List<String> affectedTables = new ArrayList<>(migration.additionalCreatedTables());
            affectedTables.add(side.migration().target().table());
            migration = new SchemaMigrationPlan(
                    migration.target(),
                    migration.targetIndexes(),
                    migration.targetForeignKeys(),
                    migration.tableExists(),
                    migration.requests(),
                    migration.skippedChanges(),
                    affectedTables);
        }
        List<SqlRequest> rollback = new ArrayList<>(side.rollback().requests());
        rollback.addAll(primary.rollback().requests());
        List<SchemaRollbackGap> gaps = new ArrayList<>(side.rollback().gaps());
        gaps.addAll(primary.rollback().gaps());
        List<SqlRequest> blocking = new ArrayList<>(primary.onlineDdl().potentiallyBlocking());
        blocking.addAll(side.onlineDdl().potentiallyBlocking());
        OnlineDdlReview online = new OnlineDdlReview(
                primary.onlineDdl().mode(),
                primary.onlineDdl().support(),
                blocking,
                primary.onlineDdl().requiresNonTransactionalExecution()
                        || side.onlineDdl().requiresNonTransactionalExecution());
        return new ReviewedSchemaMigrationPlan(
                migration, new SchemaRollbackPlan(rollback, gaps), online);
    }

    private static boolean isTableNotFound(Throwable error) {
        if (!(error instanceof IllegalArgumentException) || error.getMessage() == null) {
            return false;
        }
        String message = error.getMessage();
        return message.equals("table metadata not found") || message.startsWith("table metadata not found:");
    }
}
