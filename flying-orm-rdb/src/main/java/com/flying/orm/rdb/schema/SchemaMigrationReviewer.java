package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 给结构迁移补上反向计划和在线 DDL 审核。
 *
 * <p>它使用当前元数据和目标表单做结构化比较，不从 SQL 文本反猜字段差异。SQL 文本只用于保守判断一条 DDL
 * 是否可能锁表；这个判断不会改写 SQL，只决定是否允许直接执行。</p>
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public final class SchemaMigrationReviewer {

    private final FormSchemaSqlRenderer renderer;
    private final SchemaRollbackSqlRenderer rollbackRenderer;

    private SchemaMigrationReviewer(FormSchemaSqlRenderer renderer) {
        this.renderer = Objects.requireNonNull(renderer, "form schema SQL renderer must not be null");
        this.rollbackRenderer = new SchemaRollbackSqlRenderer(renderer.dialect(), renderer.tableRenderer());
    }

    /**
     * 复用已经装配好的结构 SQL 渲染器。客户端通过这个入口审核，正向计划和回滚计划一定使用同一套方言规则。
     *
     * @param renderer 客户端正在使用的结构 SQL 渲染器
     * @return 迁移审核器
     */
    public static SchemaMigrationReviewer create(FormSchemaSqlRenderer renderer) {
        return new SchemaMigrationReviewer(renderer);
    }

    public ReviewedSchemaMigrationPlan review(TableMetadata current,
                                              SchemaMigrationPlan migration,
                                              SchemaMigrationReviewPolicy policy) {
        TableMetadata safeCurrent = Objects.requireNonNull(current, "current table metadata must not be null");
        SchemaMigrationPlan safeMigration = Objects.requireNonNull(migration, "migration plan must not be null");
        SchemaMigrationReviewPolicy safePolicy = Objects.requireNonNull(policy,
                                                                        "migration review policy must not be null");
        SchemaRollbackPlan baseRollback = safeMigration.tableExists()
                ? rollbackExisting(safeCurrent, safeMigration, safePolicy.columnRenames())
                : rollbackCreated(safeMigration);
        List<SqlRequest> rollbackRequests = new ArrayList<>();
        List<String> additionalTables = new ArrayList<>(safeMigration.additionalCreatedTables());
        Collections.reverse(additionalTables);
        additionalTables.stream()
                        .filter(table -> createdByMigration(safeMigration, table))
                        .forEach(table -> rollbackRequests.add(renderer.rollbackDropTable(table)));
        rollbackRequests.addAll(baseRollback.requests());
        SchemaRollbackPlan rollback = new SchemaRollbackPlan(rollbackRequests, baseRollback.gaps());
        List<SqlRequest> executionRequests = safePolicy.onlineDdlMode() == OnlineDdlMode.ALLOW_BLOCKING
                ? safeMigration.requests()
                : safeMigration.requests().stream().map(renderer::preferOnline).toList();
        SchemaMigrationPlan executionMigration = new SchemaMigrationPlan(
                safeMigration.target(),
                safeMigration.targetIndexes(),
                safeMigration.targetForeignKeys(),
                safeMigration.tableExists(),
                executionRequests,
                safeMigration.skippedChanges(),
                safeMigration.additionalCreatedTables());
        List<SqlRequest> blocking = executionRequests.stream()
                                                 .filter(SchemaMigrationReviewer::potentiallyBlocking)
                                                 .toList();
        boolean nonTransactional = executionRequests.stream()
                                                    .map(SqlRequest::sql)
                                                    .map(String::stripLeading)
                                                    .map(sql -> sql.toLowerCase(java.util.Locale.ROOT))
                                                    .anyMatch(sql -> sql.startsWith("create index concurrently ")
                                                            || sql.startsWith("create unique index concurrently "));
        return new ReviewedSchemaMigrationPlan(executionMigration,
                                               rollback,
                                               new OnlineDdlReview(safePolicy.onlineDdlMode(),
                                                                   renderer.onlineDdlSupport(),
                                                                   blocking,
                                                                   nonTransactional));
    }

    private boolean createdByMigration(SchemaMigrationPlan migration, String table) {
        String prefix = "create table " + renderer.tableRenderer().identifier(table) + " ";
        return migration.requests().stream()
                        .map(SqlRequest::sql)
                        .map(String::stripLeading)
                        .anyMatch(sql -> sql.regionMatches(true, 0, prefix, 0, prefix.length()));
    }

    private SchemaRollbackPlan rollbackCreated(SchemaMigrationPlan migration) {
        List<SqlRequest> requests = new ArrayList<>();
        requests.add(renderer.rollbackDropTable(migration.target().table()));
        requests.addAll(renderer.rollbackDropSequences(migration.target()));
        return new SchemaRollbackPlan(requests, List.of());
    }

    private SchemaRollbackPlan rollbackExisting(TableMetadata current,
                                                SchemaMigrationPlan migration,
                                                Map<String, String> renames) {
        List<SqlRequest> rollback = new ArrayList<>();
        List<SchemaRollbackGap> gaps = new ArrayList<>();
        String table = migration.target().table();

        // 正向最后做索引，因此回滚先处理索引，避免列回退时仍被新索引引用。
        rollbackIndexes(current, migration, table, rollback, gaps);

        List<ColumnMetadata> reversedCurrent = new ArrayList<>(current.columns());
        Collections.reverse(reversedCurrent);
        for (ColumnMetadata source : reversedCurrent) {
            String targetName = renameTarget(renames, source.name());
            DynamicField target = migration.target().findField(targetName == null ? source.name() : targetName)
                                                   .orElse(null);
            if (target == null) {
                if (!skipped(migration, SkippedSchemaChange.Kind.DROP_COLUMN, source.name())) {
                    rollback.add(renderer.rollbackAddColumn(table, source));
                    gaps.add(new SchemaRollbackGap(SchemaRollbackGap.Kind.DATA_CANNOT_BE_RESTORED,
                                                   source.name(),
                                                   "the column structure can be recreated, but dropped row values need a backup"));
                    if (source.primaryKey()) {
                        gaps.add(new SchemaRollbackGap(SchemaRollbackGap.Kind.PRIMARY_KEY_REQUIRES_REVIEW,
                                                       source.name(),
                                                       "restoring a dropped primary key needs constraint and reference review"));
                    }
                }
            } else if (!sameShape(source, target)
                    && !skipped(migration, SkippedSchemaChange.Kind.CHANGE_COLUMN, target.name())) {
                if (source.primaryKey() != target.primaryKey()) {
                    gaps.add(new SchemaRollbackGap(SchemaRollbackGap.Kind.PRIMARY_KEY_REQUIRES_REVIEW,
                                                   source.name(),
                                                   "primary key rollback needs constraint and foreign key review"));
                } else if (source.nullable() != target.nullable()) {
                    rollback.add(rollbackRenderer.rollbackColumnNullability(table, source));
                    if (!source.nullable() && target.nullable()) {
                        gaps.add(new SchemaRollbackGap(
                                SchemaRollbackGap.Kind.DATA_CANNOT_BE_RESTORED,
                                source.name(),
                                "rows written as null after relaxing the constraint must be repaired before rollback"));
                    }
                } else {
                    rollback.add(renderer.rollbackColumnType(table, source));
                }
            }
        }

        // 只调整复合主键顺序时，各列的 primaryKey 标志没有变化，也必须保留审批和人工处理边界。
        boolean primaryKeyChangeSkipped = migration.skippedChanges().stream()
                                                   .anyMatch(change -> change.kind()
                                                           == SkippedSchemaChange.Kind.CHANGE_PRIMARY_KEY);
        boolean primaryKeyGapPresent = gaps.stream().anyMatch(gap -> gap.kind()
                == SchemaRollbackGap.Kind.PRIMARY_KEY_REQUIRES_REVIEW);
        if (primaryKeyChangeSkipped && !primaryKeyGapPresent) {
            gaps.add(new SchemaRollbackGap(SchemaRollbackGap.Kind.PRIMARY_KEY_REQUIRES_REVIEW,
                                           table,
                                           "primary key changes require a checked manual script and dependency review"));
        }

        List<DynamicField> reversedTarget = new ArrayList<>(migration.target().fields());
        Collections.reverse(reversedTarget);
        for (DynamicField target : reversedTarget) {
            String sourceName = renameSource(renames, target.name());
            boolean primaryKeySkipped = target.primaryKey()
                    && migration.skippedChanges().stream()
                                .anyMatch(change -> change.kind() == SkippedSchemaChange.Kind.CHANGE_PRIMARY_KEY);
            if (!primaryKeySkipped
                    && current.findColumn(sourceName == null ? target.name() : sourceName).isEmpty()) {
                rollback.add(renderer.rollbackDropColumn(table, target.name()));
            }
        }

        List<Map.Entry<String, String>> reversedRenames = new ArrayList<>(renames.entrySet());
        Collections.reverse(reversedRenames);
        reversedRenames.forEach(rename -> rollback.add(renderer.rollbackRenameColumn(
                table, rename.getValue(), rename.getKey())));
        if (!migration.targetForeignKeys().isEmpty()) {
            gaps.add(new SchemaRollbackGap(SchemaRollbackGap.Kind.FOREIGN_KEY_REQUIRES_REVIEW,
                                           table,
                                           "foreign key rollback needs existing data and referenced-table validation"));
        }
        return new SchemaRollbackPlan(rollback, gaps);
    }

    private void rollbackIndexes(TableMetadata current,
                                 SchemaMigrationPlan migration,
                                 String table,
                                 List<SqlRequest> rollback,
                                 List<SchemaRollbackGap> gaps) {
        List<IndexMetadata> target = migration.targetIndexes();
        List<IndexMetadata> generatedIndexes = migration.target().toTableMetadata().indexes();
        Set<String> targetIndexNames = new HashSet<>();
        target.forEach(index -> targetIndexNames.add(index.normalizedName()));
        Set<String> consumedLegacyIndexNames = new HashSet<>();
        for (IndexMetadata index : target) {
            IndexMetadata source = current.findIndex(index.name()).orElse(null);
            if (source == null) {
                IndexMetadata legacy = SchemaMigrationSupport.findLegacyGeneratedUniqueIndex(
                        current.indexes(),
                        generatedIndexes,
                        index,
                        targetIndexNames,
                        consumedLegacyIndexNames);
                if (legacy != null) {
                    consumedLegacyIndexNames.add(legacy.normalizedName());
                    continue;
                }
                rollback.add(renderer.rollbackDropIndex(table, index));
            } else if (!sameIndex(source, index)
                    && !skipped(migration, SkippedSchemaChange.Kind.CHANGE_INDEX, index.name())) {
                rollback.add(renderer.rollbackDropIndex(table, index));
                rollback.add(renderer.rollbackCreateIndex(table, source));
            }
        }
        for (IndexMetadata index : current.indexes()) {
            boolean retained = consumedLegacyIndexNames.contains(index.normalizedName())
                    || target.stream().anyMatch(candidate -> candidate.normalizedName()
                                                                  .equals(index.normalizedName()));
            if (!retained) {
                if (!skipped(migration, SkippedSchemaChange.Kind.DROP_INDEX, index.name())) {
                    rollback.add(renderer.rollbackCreateIndex(table, index));
                    gaps.add(new SchemaRollbackGap(SchemaRollbackGap.Kind.INDEX_REQUIRES_REVIEW,
                                                   index.name(),
                                                   "recreating a large index may need an online index tool or maintenance window"));
                }
            }
        }
    }

    private static boolean sameIndex(IndexMetadata source, IndexMetadata target) {
        return source.unique() == target.unique() && source.columns().equals(target.columns());
    }

    private static boolean skipped(SchemaMigrationPlan migration,
                                   SkippedSchemaChange.Kind kind,
                                   String name) {
        return migration.skippedChanges().stream()
                        .anyMatch(change -> change.kind() == kind && change.name().equalsIgnoreCase(name));
    }

    private boolean sameShape(ColumnMetadata source, DynamicField target) {
        return ProtectedSchemaTarget.sameProtectedStorage(source, target)
                || SchemaMigrationSupport.sameColumnShape(source, target, renderer.tableRenderer());
    }

    private static boolean potentiallyBlocking(SqlRequest request) {
        String sql = request.sql().stripLeading().toLowerCase(java.util.Locale.ROOT);
        if (sql.startsWith("create index concurrently ")
                || sql.startsWith("create unique index concurrently ")) {
            return false;
        }
        return sql.startsWith("alter table ")
                || sql.startsWith("drop table ")
                || sql.startsWith("create index ")
                || sql.startsWith("create unique index ")
                || sql.startsWith("drop index ");
    }

    private static String renameTarget(Map<String, String> renames, String source) {
        return renames.entrySet().stream()
                      .filter(entry -> entry.getKey().equalsIgnoreCase(source))
                      .map(Map.Entry::getValue)
                      .findFirst()
                      .orElse(null);
    }

    private static String renameSource(Map<String, String> renames, String target) {
        return renames.entrySet().stream()
                      .filter(entry -> entry.getValue().equalsIgnoreCase(target))
                      .map(Map.Entry::getKey)
                      .findFirst()
                      .orElse(null);
    }
}
