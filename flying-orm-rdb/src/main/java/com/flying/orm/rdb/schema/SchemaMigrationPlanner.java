package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicFormChangeSet;
import com.flying.orm.core.form.FieldChange;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 负责把新旧结构比较结果编排成迁移计划。
 *
 * <p>它只做迁移顺序和风险分流；字段、索引、标识符的具体 SQL 由 {@link SchemaTableSqlRenderer} 负责，
 * 外键报告由 {@link SchemaForeignKeyPlanner} 负责。这样安全迁移的行为集中在一个地方，公开渲染器只需协调入口。</p>
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
final class SchemaMigrationPlanner {

    private final SchemaDialect dialect;
    private final SchemaTableSqlRenderer tables;
    private final ProtectedSchemaMigrationPlanner protectedSchemas;
    private FormSchemaSqlRenderer renderer;

    /**
     * 供 ReactiveSchemaClient 使用的包内构造器。结构渲染器是公开门面，计划器只借它读取同一套方言和
     * 回滚规则，避免客户端自己再装配一份可能不一致的 renderer。
     */
    SchemaMigrationPlanner(FormSchemaSqlRenderer renderer) {
        FormSchemaSqlRenderer safeRenderer = Objects.requireNonNull(renderer,
                                                                     "form schema SQL renderer must not be null");
        this.dialect = safeRenderer.dialect();
        this.tables = safeRenderer.tableRenderer();
        this.renderer = safeRenderer;
        this.protectedSchemas = new ProtectedSchemaMigrationPlanner(this, safeRenderer);
    }

    Mono<SchemaMigrationPlan> plan(DynamicForm form,
                                   List<IndexMetadata> indexes,
                                   List<ForeignKeyMetadata> foreignKeys,
                                   ReactiveFormMetadataReader metadataReader,
                                   SchemaMigrationOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "target dynamic form must not be null");
        ReactiveFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "reactive form metadata reader must not be null");
        SchemaMigrationOptions safeOptions = Objects.requireNonNull(options,
                                                                     "schema migration options must not be null");
        return safeReader.readTable(safeForm.table())
                         .flatMap(current -> protectedSchemas.planExistingReactive(
                                 safeForm,
                                 migrateSafelyPlan(current, safeForm, indexes, foreignKeys, safeOptions),
                                 safeReader,
                                 safeOptions))
                         .onErrorResume(SchemaMigrationPlanner::isTableNotFound,
                                        failure -> Mono.just(createTablePlan(
                                                safeForm, indexes, foreignKeys)));
    }

    Mono<ReviewedSchemaMigrationPlan> review(DynamicForm form,
                                             List<IndexMetadata> indexes,
                                             List<ForeignKeyMetadata> foreignKeys,
                                             ReactiveFormMetadataReader metadataReader,
                                             SchemaMigrationOptions migrationOptions,
                                             SchemaMigrationReviewPolicy reviewPolicy) {
        DynamicForm safeForm = Objects.requireNonNull(form, "target dynamic form must not be null");
        ReactiveFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "reactive form metadata reader must not be null");
        SchemaMigrationOptions safeOptions = Objects.requireNonNull(migrationOptions,
                                                                     "schema migration options must not be null");
        SchemaMigrationReviewPolicy safePolicy = Objects.requireNonNull(reviewPolicy,
                                                                         "migration review policy must not be null");
        return safeReader.readTable(safeForm.table())
                         .flatMap(current -> protectedSchemas.reviewExistingReactive(
                                 safeForm,
                                 current,
                                 migrateSafelyPlan(current, safeForm, indexes, foreignKeys, safeOptions),
                                 safeReader,
                                 safeOptions,
                                 safePolicy))
                         .onErrorResume(SchemaMigrationPlanner::isTableNotFound,
                                        failure -> Mono.just(protectedSchemas.reviewCreated(
                                                safeForm,
                                                createPrimaryTablePlan(safeForm, indexes, foreignKeys),
                                                safePolicy)));
    }

    SchemaMigrationPlan createTablePlan(DynamicForm target,
                                        List<IndexMetadata> targetIndexes,
                                        List<ForeignKeyMetadata> targetForeignKeys) {
        return protectedSchemas.appendCreatePlan(
                target, createPrimaryTablePlan(target, targetIndexes, targetForeignKeys));
    }

    SchemaMigrationPlan createPrimaryTablePlan(DynamicForm target,
                                                List<IndexMetadata> targetIndexes,
                                                List<ForeignKeyMetadata> targetForeignKeys) {
        ProtectedSchemaTarget resolved = ProtectedSchemaTarget.resolve(
                target, targetIndexes, targetForeignKeys);
        DynamicForm safeTarget = resolved.form();
        List<IndexMetadata> safeIndexes = resolved.indexes();
        List<ForeignKeyMetadata> safeForeignKeys = resolved.foreignKeys();
        List<SqlRequest> requests = new ArrayList<>(tables.createTable(safeTarget));
        requests.addAll(tables.createIndexes(safeTarget.table(), safeIndexes));
        List<SkippedSchemaChange> skipped = safeForeignKeys.stream()
                                                           .map(SchemaForeignKeyPlanner::addForeignKeyChange)
                                                           .toList();
        return new SchemaMigrationPlan(safeTarget, safeIndexes, safeForeignKeys, false, requests, skipped);
    }

    List<SqlRequest> migrate(DynamicFormChangeSet changeSet) {
        DynamicFormChangeSet changes = Objects.requireNonNull(changeSet, "dynamic form change set must not be null");
        if (!changes.source().protections().isEmpty() || !changes.target().protections().isEmpty()) {
            throw new IllegalArgumentException("protected fields require a reviewed schema migration plan");
        }
        String rawTable = changes.target().table();
        String table = tables.identifier(rawTable);
        List<SqlRequest> requests = new ArrayList<>();
        for (DynamicField field : changes.addedFields()) {
            tables.addSequenceCreate(requests, field);
            requests.add(new SqlRequest(dialect.addColumnSql(rawTable, tables.columnDefinition(field)), List.of()));
            tables.addColumnComment(requests, changes.target().table(), field);
        }
        addUniqueIndexesForAddedFields(requests, changes);
        for (FieldChange change : changes.changedFields()) {
            validateDirectFieldChange(change);
            DynamicField target = change.target();
            boolean shapeChanged = storageShapeChanged(change.source(), target);
            boolean commentChanged = !Objects.equals(change.source().comment(), target.comment());
            if (commentChanged && dialect.inlineColumnComment() && !dialect.rewritesFullColumnDefinition()) {
                throw new IllegalArgumentException("inline column comment changes require a reviewed migration plan");
            }
            if (shapeChanged || commentChanged && dialect.rewritesFullColumnDefinition()) {
                requests.add(new SqlRequest(dialect.alterColumnTypeSql(rawTable,
                                                                        target.name(),
                                                                        tables.dataType(target),
                                                                        tables.columnDefinition(target)), List.of()));
            }
            if (commentChanged && !dialect.inlineColumnComment()) {
                tables.addColumnCommentChange(requests,
                                              changes.target().table(),
                                              target.name(),
                                              change.source().comment(),
                                              target.comment());
            }
        }
        for (DynamicField field : changes.removedFields()) {
            requests.add(new SqlRequest("alter table " + table + " drop column " + tables.identifier(field.name()),
                                        List.of()));
        }
        return List.copyOf(requests);
    }

    /** 新增唯一字段必须同时兑现 DynamicForm 自动发布的唯一索引，不能让迁移成功后约束静默缺失。 */
    private void addUniqueIndexesForAddedFields(List<SqlRequest> requests, DynamicFormChangeSet changes) {
        Set<String> addedUniqueFields = changes.addedFields().stream()
                                                .filter(DynamicField::unique)
                                                .map(DynamicField::normalizedName)
                                                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        if (addedUniqueFields.isEmpty()) {
            return;
        }
        List<IndexMetadata> indexes = changes.target().toTableMetadata().indexes().stream()
                                             .filter(IndexMetadata::unique)
                                             .filter(index -> index.columns().size() == 1)
                                             .filter(index -> changes.target().findField(index.columns().getFirst())
                                                                     .map(DynamicField::normalizedName)
                                                                     .filter(addedUniqueFields::contains)
                                                                     .isPresent())
                                             .toList();
        requests.addAll(tables.createIndexes(changes.target().table(), indexes));
    }

    private static void validateDirectFieldChange(FieldChange change) {
        DynamicField source = change.source();
        DynamicField target = change.target();
        if (source.primaryKey() != target.primaryKey()
                || source.nullable() != target.nullable()
                || source.unique() != target.unique()
                || !source.generation().equals(target.generation())) {
            throw new IllegalArgumentException("column constraint changes require a reviewed migration plan");
        }
    }

    private static boolean storageShapeChanged(DynamicField source, DynamicField target) {
        return !source.dataType().equalsIgnoreCase(target.dataType())
                || !Objects.equals(source.length(), target.length())
                || !Objects.equals(source.precision(), target.precision())
                || !Objects.equals(source.scale(), target.scale());
    }

    SchemaMigrationPlan migrateSafelyPlan(TableMetadata current,
                                          DynamicForm target,
                                          List<IndexMetadata> targetIndexes,
                                          List<ForeignKeyMetadata> targetForeignKeys,
                                          SchemaMigrationOptions options) {
        TableMetadata safeCurrent = Objects.requireNonNull(current, "current table metadata must not be null");
        ProtectedSchemaTarget.validateExistingStorage(safeCurrent, target);
        ProtectedSchemaTarget resolved = ProtectedSchemaTarget.resolve(
                target, targetIndexes, targetForeignKeys);
        DynamicForm safeTarget = resolved.form();
        List<IndexMetadata> safeIndexes = resolved.indexes();
        List<ForeignKeyMetadata> safeForeignKeys = resolved.foreignKeys();
        SchemaMigrationOptions safeOptions = Objects.requireNonNull(options,
                                                                     "schema migration options must not be null");
        String rawTable = safeTarget.table();
        String table = tables.identifier(rawTable);
        List<SqlRequest> requests = new ArrayList<>();
        List<SkippedSchemaChange> skipped = new ArrayList<>();

        // 改名必须先校验并加入计划，后面的字段、主键和索引比较才按新名字判断。
        SchemaMigrationSupport.validateColumnRenames(safeCurrent, safeTarget, safeOptions.columnRenames());
        safeOptions.columnRenames().forEach((oldName, newName) -> requests.add(new SqlRequest(
                dialect.renameColumnSql(safeTarget.table(), oldName, newName), List.of())));
        List<String> currentPrimaryKeys = SchemaMigrationSupport.renamedNames(
                SchemaMigrationSupport.currentPrimaryKeys(safeCurrent), safeOptions.columnRenames());
        List<String> targetPrimaryKeys = SchemaMigrationSupport.targetPrimaryKeys(safeTarget);
        boolean primaryKeyChanged = !SchemaMigrationSupport.sameNamesIgnoreCase(currentPrimaryKeys, targetPrimaryKeys);
        if (primaryKeyChanged) {
            skipped.add(SchemaMigrationSupport.primaryKeyChange(safeTarget.table(),
                                                                currentPrimaryKeys,
                                                                targetPrimaryKeys,
                                                                safeOptions));
        }

        // 先补目标字段，再处理当前结构里多出来的字段，生成的顺序可以直接拿去审核。
        for (DynamicField field : safeTarget.fields()) {
            String currentName = SchemaMigrationSupport.renameSourceForTarget(safeOptions.columnRenames(), field.name());
            ColumnMetadata column = safeCurrent.findColumn(currentName == null ? field.name() : currentName)
                                               .orElse(null);
            if (column != null) {
                SchemaMigrationSupport.addShapeChange(requests,
                                                       skipped,
                                                       rawTable,
                                                       column,
                                                       field,
                                                       safeOptions,
                                                       primaryKeyChanged,
                                                       dialect,
                                                       tables);
                tables.addMissingComment(requests, safeTarget.table(), column, field);
            } else if (!primaryKeyChanged || !field.primaryKey()) {
                tables.addSequenceCreate(requests, field);
                requests.add(new SqlRequest(dialect.addColumnSql(rawTable, tables.columnDefinition(field)), List.of()));
                tables.addColumnComment(requests, safeTarget.table(), field);
            }
        }
        addRemovedColumns(requests, skipped, safeCurrent, safeTarget, table, safeOptions, primaryKeyChanged);
        addIndexes(requests, skipped, safeCurrent, safeTarget, safeIndexes, safeOptions, rawTable);
        SchemaForeignKeyPlanner.addChanges(skipped, safeCurrent, safeForeignKeys, safeOptions.columnRenames());
        return new SchemaMigrationPlan(safeTarget, safeIndexes, safeForeignKeys, true, requests, skipped);
    }

    private void addRemovedColumns(List<SqlRequest> requests,
                                   List<SkippedSchemaChange> skipped,
                                   TableMetadata current,
                                   DynamicForm target,
                                   String table,
                                   SchemaMigrationOptions options,
                                   boolean primaryKeyChanged) {
        for (ColumnMetadata column : current.columns()) {
            if (target.findField(column.name()).isEmpty()
                    && SchemaMigrationSupport.renameTargetForSource(options.columnRenames(), column.name()) == null) {
                if (primaryKeyChanged && column.primaryKey()) {
                    continue;
                }
                if (options.dropColumnAllowed()) {
                    requests.add(new SqlRequest("alter table " + table + " drop column " + tables.identifier(column.name()),
                                                List.of()));
                } else {
                    skipped.add(new SkippedSchemaChange(SkippedSchemaChange.Kind.DROP_COLUMN,
                                                        column.name(),
                                                        "SAFE mode does not drop existing columns"));
                }
            }
        }
    }

    private void addIndexes(List<SqlRequest> requests,
                            List<SkippedSchemaChange> skipped,
                            TableMetadata current,
                            DynamicForm target,
                            List<IndexMetadata> indexes,
                            SchemaMigrationOptions options,
                            String table) {
        List<IndexMetadata> generatedIndexes = target.toTableMetadata().indexes();
        Set<String> targetIndexNames = new HashSet<>();
        indexes.forEach(index -> targetIndexNames.add(index.normalizedName()));
        Set<String> consumedLegacyIndexNames = new HashSet<>();
        for (IndexMetadata index : indexes) {
            IndexMetadata currentIndex = current.findIndex(index.name()).orElse(null);
            if (currentIndex != null) {
                addIndexChange(requests, skipped, target.table(), table, currentIndex, index, options);
                continue;
            }
            IndexMetadata legacyIndex = SchemaMigrationSupport.findLegacyGeneratedUniqueIndex(
                    current.indexes(),
                    generatedIndexes,
                    index,
                    targetIndexNames,
                    consumedLegacyIndexNames);
            if (legacyIndex != null) {
                consumedLegacyIndexNames.add(legacyIndex.normalizedName());
            } else {
                requests.add(tables.createIndex(table, index));
            }
        }
        for (IndexMetadata index : current.indexes()) {
            boolean present = consumedLegacyIndexNames.contains(index.normalizedName())
                    || indexes.stream()
                              .anyMatch(targetIndex -> targetIndex.normalizedName()
                                                                 .equals(index.normalizedName()));
            if (!present) {
                if (options.dropIndexAllowed()) {
                    requests.add(tables.dropIndex(target.table(), index));
                } else {
                    skipped.add(new SkippedSchemaChange(SkippedSchemaChange.Kind.DROP_INDEX,
                                                        index.name(),
                                                        "SAFE mode does not drop existing indexes"));
                }
            }
        }
    }

    private void addIndexChange(List<SqlRequest> requests,
                                List<SkippedSchemaChange> skipped,
                                String rawTable,
                                String table,
                                IndexMetadata current,
                                IndexMetadata target,
                                SchemaMigrationOptions options) {
        // 唯一性或列顺序变化需要重建索引；安全模式默认只报告，不自动删除现有索引。
        if (current.unique() != target.unique()
                || !SchemaMigrationSupport.sameIndexColumns(current, target, options.columnRenames())) {
            if (options.rebuildIndexAllowed()) {
                requests.add(tables.dropIndex(rawTable, current));
                requests.add(tables.createIndex(table, target));
            } else {
                skipped.add(new SkippedSchemaChange(SkippedSchemaChange.Kind.CHANGE_INDEX,
                                                    target.name(),
                                                    "SAFE mode does not rebuild existing indexes"));
            }
        }
    }

    /** 元数据读取器用稳定异常前缀表示目标表还不存在，此时计划应自然退化为建表。 */
    private static boolean isTableNotFound(Throwable error) {
        if (!(error instanceof IllegalArgumentException) || error.getMessage() == null) {
            return false;
        }
        String message = error.getMessage();
        return message.equals("table metadata not found") || message.startsWith("table metadata not found:");
    }
}
