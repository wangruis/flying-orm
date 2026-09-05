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
import com.flying.orm.rdb.dialect.DialectCapabilities;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 负责迁移顺序和风险分流；字段、索引、标识符 SQL 由 {@link SchemaTableSqlRenderer} 负责，
 * 外键报告由 {@link SchemaForeignKeyPlanner} 负责。
 */
final class SchemaMigrationPlanner {

    private final SchemaDialect dialect;
    private final SchemaTableSqlRenderer tables;
    private final SchemaIndexPlanner indexChanges;
    private final ProtectedSchemaMigrationPlanner protectedSchemas;

    /** 复用公开渲染器的方言和回滚规则，避免客户端重复装配。 */
    SchemaMigrationPlanner(FormSchemaSqlRenderer renderer) {
        FormSchemaSqlRenderer safeRenderer = Objects.requireNonNull(renderer,
                                                                     "form schema SQL renderer must not be null");
        this.dialect = safeRenderer.dialect();
        this.tables = safeRenderer.tableRenderer();
        this.indexChanges = new SchemaIndexPlanner(this.tables);
        this.protectedSchemas = new ProtectedSchemaMigrationPlanner(this, safeRenderer);
    }

    /** 只保留由当前结构事实和方言能力共同证明安全的增量操作。 */
    List<SchemaOperation> safeIncrementalPlan(List<SchemaOperation> operations,
                                              SchemaSnapshot actual,
                                              DialectCapabilities capabilities) {
        return SchemaMigrationSupport.requireSafeIncremental(operations, actual, capabilities);
    }

    Mono<SchemaMigrationPlan> plan(DynamicForm form,
                                   List<IndexMetadata> indexes,
                                   List<ForeignKeyMetadata> foreignKeys,
                                   ReactiveFormMetadataReader metadataReader,
                                   SchemaMigrationOptions options) {
        DynamicForm safeForm = SchemaMigrationSupport.requireLegacyRelation(form);
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
        DynamicForm safeForm = SchemaMigrationSupport.requireLegacyRelation(form);
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
        // 变更集已保证 source 和 target 指向同一物理关系，这里只校验一次目标身份。
        SchemaMigrationSupport.requireLegacyRelation(changes.target());
        if (!changes.source().protections().isEmpty() || !changes.target().protections().isEmpty()) {
            throw new IllegalArgumentException("protected fields require a reviewed schema migration plan");
        }
        String rawTable = changes.target().table();
        String table = tables.identifier(rawTable);
        List<SqlRequest> requests = new ArrayList<>();
        tables.addSequenceCreates(requests, changes.addedFields(), changes.source().fields(), true);
        for (DynamicField field : changes.addedFields()) {
            requests.add(new SqlRequest(dialect.addColumnSql(rawTable, tables.columnDefinition(field)), List.of()));
            tables.addColumnComment(requests, changes.target().table(), field);
        }
        addUniqueIndexesForAddedFields(requests, changes);
        for (FieldChange change : changes.changedFields()) {
            validateDirectFieldChange(change);
            DynamicField target = change.target();
            boolean shapeChanged = storageShapeChanged(change.source(), target);
            String sourceStorageComment = tables.storageComment(change.source());
            String targetStorageComment = tables.storageComment(target);
            boolean commentChanged = !Objects.equals(sourceStorageComment, targetStorageComment);
            java.util.Optional<String> separateCommentChange = commentChanged
                    ? dialect.columnCommentChangeSql(rawTable,
                                                     target.name(),
                                                     sourceStorageComment,
                                                     targetStorageComment)
                    : java.util.Optional.empty();
            boolean commentInFullDefinition = dialect.inlineColumnComment()
                    && dialect.rewritesFullColumnDefinition();
            if (commentChanged
                    && !commentInFullDefinition
                    && separateCommentChange.isEmpty()) {
                throw new IllegalArgumentException("the configured dialect cannot alter the column comment safely");
            }
            if (shapeChanged || commentChanged && commentInFullDefinition) {
                requests.add(new SqlRequest(dialect.alterColumnTypeSql(rawTable,
                                                                        target.name(),
                                                                        tables.dataType(target),
                                                                        tables.columnDefinition(target)), List.of()));
            }
            if (commentChanged && !commentInFullDefinition) {
                requests.add(new SqlRequest(separateCommentChange.orElseThrow(), List.of()));
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
        return !source.databaseType().equals(target.databaseType())
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
        ProtectedSchemaTarget resolved = ProtectedSchemaTarget.resolve(
                target, targetIndexes, targetForeignKeys);
        ProtectedSchemaTarget.validateExistingStorage(safeCurrent, target);
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
        validateCommentChanges(safeCurrent, safeTarget, safeOptions.columnRenames());
        safeOptions.columnRenames().forEach((oldName, newName) -> requests.add(new SqlRequest(
                dialect.renameColumnSql(safeTarget.table(), oldName, newName), List.of())));
        List<String> currentPrimaryKeys = SchemaMigrationSupport.renamedNames(
                SchemaMigrationSupport.currentPrimaryKeys(safeCurrent), safeOptions.columnRenames());
        List<String> targetPrimaryKeys = SchemaMigrationSupport.targetPrimaryKeys(safeTarget);
        boolean primaryKeyChanged = !SchemaMigrationSupport.sameNames(currentPrimaryKeys, targetPrimaryKeys);
        if (primaryKeyChanged) {
            skipped.add(SchemaMigrationSupport.primaryKeyChange(safeTarget.table(),
                                                                currentPrimaryKeys,
                                                                targetPrimaryKeys,
                                                                safeOptions));
        }

        List<DynamicField> addedFields = new ArrayList<>();
        List<DynamicField> existingFields = new ArrayList<>();
        for (DynamicField field : safeTarget.fields()) {
            String currentName = SchemaMigrationSupport.renameSourceForTarget(
                    safeOptions.columnRenames(), field.name());
            String lookupName = currentName == null ? field.name() : currentName;
            boolean missing = safeCurrent.findColumn(lookupName).isEmpty();
            if (!missing) {
                existingFields.add(field);
            } else if (!primaryKeyChanged || !field.primaryKey()) {
                addedFields.add(field);
            }
        }
        List<DynamicField> sequenceOccupants = new ArrayList<>(existingFields);
        safeCurrent.columns().stream()
                   .map(SchemaMigrationSupport::toDynamicField)
                   .forEach(sequenceOccupants::add);
        // 数据字典列默认值只能证明物理 sequence 身份，不能证明起点、步长和缓存参数。
        tables.addSequenceCreates(requests, addedFields, sequenceOccupants, false);

        // 先补目标字段，再处理当前结构里多出来的字段，生成的顺序可以直接拿去审核。
        Set<String> matchedCurrentColumnNames = new HashSet<>();
        for (DynamicField field : safeTarget.fields()) {
            String currentName = SchemaMigrationSupport.renameSourceForTarget(safeOptions.columnRenames(), field.name());
            String lookupName = currentName == null ? field.name() : currentName;
            ColumnMetadata column = safeCurrent.findColumn(lookupName).orElse(null);
            if (column != null) {
                matchedCurrentColumnNames.add(column.name());
                if (currentName == null && !column.name().equals(field.name())) {
                    skipped.add(new SkippedSchemaChange(
                            SkippedSchemaChange.Kind.CHANGE_COLUMN,
                            field.name(),
                            "physical column name differs; declare an explicit column rename",
                            Map.of("currentColumn", column.name(), "targetColumn", field.name()),
                            List.of("declare renameColumn with the exact physical source and target names",
                                    "review quoted identifier behavior for the selected database")));
                    continue;
                }
                boolean storageApplied = new SchemaColumnShapeChange(
                        requests,
                        skipped,
                        new SchemaColumnShapeChange.Input(rawTable,
                                                         column,
                                                         field,
                                                         safeOptions,
                                                         primaryKeyChanged),
                        dialect,
                        tables).apply();
                if (storageApplied) {
                    tables.addMissingComment(requests, safeTarget.table(), column, field);
                }
            } else if (!primaryKeyChanged || !field.primaryKey()) {
                requests.add(new SqlRequest(dialect.addColumnSql(rawTable, tables.columnDefinition(field)), List.of()));
                tables.addColumnComment(requests, safeTarget.table(), field);
            }
        }
        addRemovedColumns(requests,
                          skipped,
                          safeCurrent,
                          matchedCurrentColumnNames,
                          table,
                          safeOptions,
                          primaryKeyChanged);
        indexChanges.addChanges(requests, skipped, safeCurrent, safeTarget, safeIndexes, safeOptions);
        SchemaForeignKeyPlanner.addChanges(skipped, safeCurrent, safeForeignKeys, safeOptions.columnRenames());
        return new SchemaMigrationPlan(safeTarget, safeIndexes, safeForeignKeys, true, requests, skipped);
    }

    private void validateCommentChanges(TableMetadata current,
                                        DynamicForm target,
                                        Map<String, String> renames) {
        for (DynamicField field : target.fields()) {
            String sourceName = SchemaMigrationSupport.renameSourceForTarget(renames, field.name());
            ColumnMetadata column = current.findColumn(sourceName == null ? field.name() : sourceName)
                                           .orElse(null);
            if (column == null) {
                tables.validateNewColumnComment(target.table(), field);
            } else {
                tables.validateCommentChange(target.table(), column, field);
            }
        }
    }

    private void addRemovedColumns(List<SqlRequest> requests,
                                   List<SkippedSchemaChange> skipped,
                                   TableMetadata current,
                                   Set<String> matchedCurrentColumnNames,
                                   String table,
                                   SchemaMigrationOptions options,
                                   boolean primaryKeyChanged) {
        for (ColumnMetadata column : current.columns()) {
            if (!matchedCurrentColumnNames.contains(column.name())
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

    /** 元数据读取器用稳定异常前缀表示目标表还不存在，此时计划应自然退化为建表。 */
    private static boolean isTableNotFound(Throwable error) {
        if (!(error instanceof IllegalArgumentException) || error.getMessage() == null) {
            return false;
        }
        String message = error.getMessage();
        return message.equals("table metadata not found") || message.startsWith("table metadata not found:");
    }
}
