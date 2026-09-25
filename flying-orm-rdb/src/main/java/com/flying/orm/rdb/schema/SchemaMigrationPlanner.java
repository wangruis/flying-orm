package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicFormChangeSet;
import com.flying.orm.core.form.FieldChange;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
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
        List<IndexMetadata> safeIndexes = List.copyOf(Objects.requireNonNull(
                indexes, "target indexes must not be null"));
        List<ForeignKeyMetadata> safeForeignKeys = List.copyOf(Objects.requireNonNull(
                foreignKeys, "target foreign keys must not be null"));
        ReactiveFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "reactive form metadata reader must not be null");
        SchemaMigrationOptions safeOptions = Objects.requireNonNull(options,
                                                                     "schema migration options must not be null");
        return safeReader.readTableForSchema(safeForm.table())
                         .flatMap(current -> migrateSafelyPlanReactive(
                                 current, safeForm, safeIndexes, safeForeignKeys, safeOptions, safeReader)
                                 .flatMap(primary -> protectedSchemas.planExistingReactive(
                                         safeForm, primary, safeReader, safeOptions)))
                         .onErrorResume(SchemaMigrationPlanner::isTableNotFound,
                                        failure -> Mono.just(createTablePlan(
                                                safeForm, safeIndexes, safeForeignKeys)));
    }

    Mono<ReviewedSchemaMigrationPlan> review(DynamicForm form,
                                             List<IndexMetadata> indexes,
                                             List<ForeignKeyMetadata> foreignKeys,
                                             ReactiveFormMetadataReader metadataReader,
                                             SchemaMigrationOptions migrationOptions,
                                             SchemaMigrationReviewPolicy reviewPolicy) {
        DynamicForm safeForm = SchemaMigrationSupport.requireLegacyRelation(form);
        List<IndexMetadata> safeIndexes = List.copyOf(Objects.requireNonNull(
                indexes, "target indexes must not be null"));
        List<ForeignKeyMetadata> safeForeignKeys = List.copyOf(Objects.requireNonNull(
                foreignKeys, "target foreign keys must not be null"));
        ReactiveFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "reactive form metadata reader must not be null");
        SchemaMigrationOptions safeOptions = Objects.requireNonNull(migrationOptions,
                                                                     "schema migration options must not be null");
        SchemaMigrationReviewPolicy safePolicy = Objects.requireNonNull(reviewPolicy,
                                                                         "migration review policy must not be null");
        return safeReader.readTableForSchema(safeForm.table())
                         .flatMap(current -> reviewSnapshot(current, safeForm, safeOptions, safeReader)
                                 .map(java.util.Optional::of)
                                 .defaultIfEmpty(java.util.Optional.empty())
                                 .flatMap(snapshot -> protectedSchemas.reviewExistingReactive(
                                         safeForm, current,
                                         migrateSafelyPlan(current, safeForm, safeIndexes, safeForeignKeys,
                                                 safeOptions, snapshot.orElse(null)),
                                         safeReader, safeOptions, safePolicy, snapshot.orElse(null))))
                         .onErrorResume(SchemaMigrationPlanner::isTableNotFound,
                                        failure -> Mono.just(protectedSchemas.reviewCreated(
                                                safeForm,
                                                createPrimaryTablePlan(safeForm, safeIndexes, safeForeignKeys),
                                                safePolicy)));
    }

    SchemaMigrationPlan planJdbc(DynamicForm form,
                                   List<IndexMetadata> indexes,
                                   List<ForeignKeyMetadata> foreignKeys,
                                   JdbcFormMetadataReader metadataReader,
                                   SchemaMigrationOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "target dynamic form must not be null");
        JdbcFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "jdbc form metadata reader must not be null");
        SchemaMigrationOptions safeOptions = Objects.requireNonNull(
                options, "schema migration options must not be null");
        SchemaMigrationSupport.requireLegacyRelation(safeForm);
        try {
            TableMetadata current = safeReader.readTableForSchema(safeForm.table());
            SchemaMigrationPlan primary = migrateSafelyPlan(
                    current, safeForm, indexes, foreignKeys, safeOptions,
                    physicalSnapshotJdbc(current, safeForm, safeOptions, safeReader));
            return protectedSchemas.planExistingJdbc(safeForm, primary, safeReader, safeOptions);
        } catch (IllegalArgumentException failure) {
            if (!isTableNotFound(failure)) {
                throw failure;
            }
            return createTablePlan(safeForm, indexes, foreignKeys);
        }
    }

    ReviewedSchemaMigrationPlan reviewJdbc(DynamicForm form,
                                             List<IndexMetadata> indexes,
                                             List<ForeignKeyMetadata> foreignKeys,
                                             JdbcFormMetadataReader metadataReader,
                                             SchemaMigrationOptions migrationOptions,
                                             SchemaMigrationReviewPolicy reviewPolicy) {
        DynamicForm safeForm = SchemaMigrationSupport.requireLegacyRelation(form);
        JdbcFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "jdbc form metadata reader must not be null");
        SchemaMigrationOptions safeOptions = Objects.requireNonNull(
                migrationOptions, "schema migration options must not be null");
        SchemaMigrationReviewPolicy safePolicy = Objects.requireNonNull(
                reviewPolicy, "migration review policy must not be null");
        try {
            TableMetadata current = safeReader.readTableForSchema(safeForm.table());
            SchemaSnapshot snapshot = reviewSnapshotJdbc(current, safeForm, safeOptions, safeReader);
            SchemaMigrationPlan primary = migrateSafelyPlan(
                    current, safeForm, indexes, foreignKeys, safeOptions, snapshot);
            return protectedSchemas.reviewExistingJdbc(
                    safeForm, current, primary, safeReader, safeOptions, safePolicy, snapshot);
        } catch (IllegalArgumentException failure) {
            if (!isTableNotFound(failure)) {
                throw failure;
            }
            return protectedSchemas.reviewCreated(
                    safeForm, createPrimaryTablePlan(safeForm, indexes, foreignKeys), safePolicy);
        }
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
        return migrate(changeSet, null);
    }

    List<SqlRequest> migrate(DynamicFormChangeSet changeSet, SchemaSnapshot snapshot) {
        DynamicFormChangeSet changes = Objects.requireNonNull(changeSet, "dynamic form change set must not be null");
        RelationalTableDefinition physical = SchemaTableSqlRenderer.physicalColumns(snapshot);
        // 变更集已保证 source 和 target 指向同一物理关系，这里只校验一次目标身份。
        SchemaMigrationSupport.requireLegacyRelation(changes.target());
        if (!changes.source().protections().isEmpty() || !changes.target().protections().isEmpty()) {
            throw new IllegalArgumentException("protected fields require a reviewed schema migration plan");
        }
        String rawTable = changes.target().table();
        List<SqlRequest> requests = new ArrayList<>();
        tables.addSequenceCreates(requests, changes.addedFields(), changes.source().fields(), true);
        for (DynamicField field : changes.addedFields()) {
            requests.add(new SqlRequest(dialect.addColumnSql(rawTable, tables.addedColumnDefinition(rawTable, field)), List.of()));
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
                requests.add(new SqlRequest(tables.alterColumnType(rawTable, target,
                        physical == null ? null : physical.column(change.source().name())), List.of()));
            }
            if (commentChanged && !commentInFullDefinition) {
                requests.add(new SqlRequest(separateCommentChange.orElseThrow(), List.of()));
            }
        }
        for (DynamicField field : changes.removedFields()) {
            requests.add(tables.dropColumn(rawTable, field.name(),
                    physical == null ? null : physical.column(field.name())));
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
        return migrateSafelyPlan(current, target, targetIndexes, targetForeignKeys, options, null);
    }

    Mono<SchemaMigrationPlan> migrateSafelyPlanReactive(TableMetadata current,
                                                       DynamicForm target,
                                                       List<IndexMetadata> indexes,
                                                       List<ForeignKeyMetadata> foreignKeys,
                                                       SchemaMigrationOptions options,
                                                       ReactiveFormMetadataReader reader) {
        return physicalSnapshot(current, target, options, reader)
                .map(snapshot -> migrateSafelyPlan(current, target, indexes, foreignKeys, options, snapshot))
                .switchIfEmpty(Mono.fromSupplier(() ->
                        migrateSafelyPlan(current, target, indexes, foreignKeys, options)));
    }

    Mono<SchemaSnapshot> physicalSnapshot(TableMetadata current, DynamicForm target,
                                          SchemaMigrationOptions options, ReactiveFormMetadataReader reader) {
        return needsPhysicalSnapshot(current, target, options)
                ? requiredPhysicalSnapshot(reader, target.table())
                : Mono.empty();
    }

    private Mono<SchemaSnapshot> requiredPhysicalSnapshot(ReactiveFormMetadataReader reader, String table) {
        return reader.readSnapshot(table).switchIfEmpty(Mono.error(new IllegalStateException(
                "rewriting an existing column requires a physical schema snapshot")));
    }

    SchemaSnapshot physicalSnapshotJdbc(TableMetadata current, DynamicForm target,
                                         SchemaMigrationOptions options, JdbcFormMetadataReader reader) {
        return needsPhysicalSnapshot(current, target, options) ? reader.readSnapshot(target.table()) : null;
    }

    private boolean needsPhysicalSnapshot(TableMetadata current, DynamicForm target,
                                           SchemaMigrationOptions options) {
        if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.SQL_SERVER
                && options.dropColumnAllowed()
                && current.columns().stream().anyMatch(column -> !column.primaryKey()
                        && target.findField(column.name()).isEmpty()
                        && SchemaMigrationSupport.renameTargetForSource(
                                options.columnRenames(), column.name()) == null)) {
            // SQL Server 删除列前还需删除绑定的 DEFAULT；该名称必须在规划时从字典冻结。
            return true;
        }
        if (!dialect.rewritesFullColumnDefinition()
                && dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.SQL_SERVER
                && dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.ORACLE) {
            return false;
        }
        for (DynamicField field : target.fields()) {
            String source = SchemaMigrationSupport.renameSourceForTarget(options.columnRenames(), field.name());
            ColumnMetadata column = current.findColumn(source == null ? field.name() : source).orElse(null);
            if (column == null
                    || source == null && !column.name().equals(field.name())
                    || column.primaryKey() != field.primaryKey()) {
                continue;
            }
            if (!dialect.rewritesFullColumnDefinition() && !tables.requiresPhysicalCollation(field)
                    && !tables.requiresPhysicalCollation(SchemaMigrationSupport.toDynamicField(column))
                    && !tables.requiresPhysicalLengthUnit(field)
                    && !tables.requiresPhysicalLengthUnit(SchemaMigrationSupport.toDynamicField(column))) {
                continue;
            }
            // Only fetch physical facts for changes SchemaColumnShapeChange will execute.
            boolean temporalChanged = SchemaMigrationSupport.logicalTemporalTypeChanged(
                    column.databaseType(), field.databaseType());
            if (column.nullable() != field.nullable()) {
                if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.ORACLE) continue;
                if (!temporalChanged
                        && SchemaMigrationSupport.sameStorageShape(column, field, tables)
                        && (field.nullable() || options.columnChangeAllowed())) {
                    return true;
                }
                continue;
            }
            if (!ProtectedSchemaTarget.sameProtectedStorage(column, field)) {
                boolean shapeChanged = !SchemaMigrationSupport.sameColumnShape(column, field, tables);
                if (shapeChanged || temporalChanged) {
                    boolean storageApplied = shapeChanged && !temporalChanged
                            && SchemaMigrationSupport.safeWidening(column, field, tables)
                            || options.columnChangeAllowed() && SchemaGeneratedValueComparison.same(
                                    column, field, tables.generatedValueStyle());
                    if (!storageApplied) {
                        continue;
                    }
                    if (shapeChanged) {
                        return true;
                    }
                }
            }
            if (dialect.rewritesFullColumnDefinition()
                    && !Objects.equals(tables.storageComment(column), tables.storageComment(field))) {
                return true;
            }
        }
        return false;
    }

    Mono<SchemaSnapshot> reviewSnapshot(TableMetadata current, DynamicForm target,
                                        SchemaMigrationOptions options, ReactiveFormMetadataReader reader) {
        return needsReviewSnapshot(current, target, options)
                ? requiredPhysicalSnapshot(reader, target.table()) : Mono.empty();
    }

    SchemaSnapshot reviewSnapshotJdbc(TableMetadata current, DynamicForm target,
                                      SchemaMigrationOptions options, JdbcFormMetadataReader reader) {
        return needsReviewSnapshot(current, target, options) ? reader.readSnapshot(target.table()) : null;
    }

    private boolean needsReviewSnapshot(TableMetadata current, DynamicForm target, SchemaMigrationOptions options) {
        if (needsPhysicalSnapshot(current, target, options)) return true;
        if (!options.dropColumnAllowed()) {
            return false;
        }
        // 除 SQL Server 正向删除 DEFAULT 的需要外，只有审核为删列回退读取字典。
        return current.columns().stream().anyMatch(column -> !column.primaryKey()
                && target.findField(column.name()).isEmpty()
                && SchemaMigrationSupport.renameTargetForSource(options.columnRenames(), column.name()) == null);
    }

    SchemaMigrationPlan migrateSafelyPlan(TableMetadata current,
                                          DynamicForm target,
                                          List<IndexMetadata> targetIndexes,
                                          List<ForeignKeyMetadata> targetForeignKeys,
                                          SchemaMigrationOptions options,
                                          SchemaSnapshot snapshot) {
        RelationalTableDefinition physical = SchemaTableSqlRenderer.physicalColumns(snapshot);
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
                                                         primaryKeyChanged,
                                                         physical == null ? null : physical.column(lookupName)),
                        tables).apply();
                if (storageApplied) {
                    tables.addMissingComment(requests, safeTarget.table(), column, field,
                            physical == null ? null : physical.column(lookupName));
                }
            } else if (!primaryKeyChanged || !field.primaryKey()) {
                requests.add(new SqlRequest(dialect.addColumnSql(rawTable, tables.addedColumnDefinition(rawTable, field)), List.of()));
                tables.addColumnComment(requests, safeTarget.table(), field);
            }
        }
        Set<String> removedColumns = safeOptions.dropColumnAllowed()
                ? removedColumnNames(safeCurrent, matchedCurrentColumnNames,
                                     safeOptions, primaryKeyChanged)
                : Set.of();
        SchemaIndexPlanner.DependencyDrops dependencyDrops = indexChanges.addDependentDrops(
                requests, safeCurrent, safeTarget, safeIndexes, safeOptions, removedColumns);
        addRemovedColumns(requests,
                          skipped,
                          safeCurrent,
                          matchedCurrentColumnNames,
                          rawTable,
                          safeOptions,
                          primaryKeyChanged,
                          dependencyDrops.blockedColumns(),
                          physical);
        indexChanges.addChanges(requests, skipped, safeCurrent, safeTarget, safeIndexes, safeOptions,
                                dependencyDrops.droppedIndexes());
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
                                   boolean primaryKeyChanged,
                                   Set<String> blockedColumns,
                                   RelationalTableDefinition physical) {
        for (ColumnMetadata column : current.columns()) {
            if (!matchedCurrentColumnNames.contains(column.name())
                    && SchemaMigrationSupport.renameTargetForSource(options.columnRenames(), column.name()) == null) {
                if (primaryKeyChanged && column.primaryKey()) {
                    continue;
                }
                if (blockedColumns.contains(column.name())) {
                    skipped.add(new SkippedSchemaChange(
                            SkippedSchemaChange.Kind.DROP_COLUMN,
                            column.name(),
                            "dependent index removal or rebuild is not approved"));
                } else if (options.dropColumnAllowed()) {
                    requests.add(tables.dropColumn(table, column.name(),
                            physical == null ? null : physical.column(column.name())));
                } else {
                    skipped.add(new SkippedSchemaChange(SkippedSchemaChange.Kind.DROP_COLUMN,
                                                        column.name(),
                                                        "SAFE mode does not drop existing columns"));
                }
            }
        }
    }

    private static Set<String> removedColumnNames(TableMetadata current,
                                                   Set<String> matchedCurrentColumnNames,
                                                   SchemaMigrationOptions options,
                                                   boolean primaryKeyChanged) {
        Set<String> removed = new HashSet<>();
        for (ColumnMetadata column : current.columns()) {
            if (!matchedCurrentColumnNames.contains(column.name())
                    && SchemaMigrationSupport.renameTargetForSource(
                            options.columnRenames(), column.name()) == null
                    && (!primaryKeyChanged || !column.primaryKey())) {
                removed.add(column.name());
            }
        }
        return Set.copyOf(removed);
    }

    /** 元数据读取器用稳定异常前缀表示目标表还不存在，此时计划应自然退化为建表。 */
    static boolean isTableNotFound(Throwable error) {
        if (!(error instanceof IllegalArgumentException) || error.getMessage() == null) {
            return false;
        }
        String message = error.getMessage();
        return message.equals("table metadata not found") || message.startsWith("table metadata not found:");
    }
}
