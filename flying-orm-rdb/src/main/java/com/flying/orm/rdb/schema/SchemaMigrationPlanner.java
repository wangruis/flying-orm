package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicFormChangeSet;
import com.flying.orm.core.form.FieldChange;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * 负责迁移顺序和显式变更选项；字段、索引、标识符 SQL 由 {@link RelationalSchemaSqlRenderer} 负责，
 * 外键操作由 {@link SchemaForeignKeyPlanner} 负责。
 */
final class SchemaMigrationPlanner {

    private final SchemaDialect dialect;
    private final FormSchemaSqlRenderer renderer;
    private final RelationalSchemaSqlRenderer tables;
    private final ProtectedSchemaMigrationPlanner protectedSchemas;

    /** 复用公开渲染器的方言和物理结构规则，避免客户端重复装配。 */
    SchemaMigrationPlanner(FormSchemaSqlRenderer renderer) {
        FormSchemaSqlRenderer safeRenderer = Objects.requireNonNull(renderer,
                                                                     "form schema SQL renderer must not be null");
        this.dialect = safeRenderer.dialect();
        this.renderer = safeRenderer;
        this.tables = safeRenderer.tableRenderer();
        this.protectedSchemas = new ProtectedSchemaMigrationPlanner(this);
    }

    Mono<SchemaMigrationPlan> plan(DynamicForm form,
                                   List<IndexDefinition> indexes,
                                   List<ForeignKeyDefinition> foreignKeys,
                                   ReactiveFormMetadataReader metadataReader,
                                   SchemaMigrationOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "target dynamic form must not be null");
        List<IndexDefinition> safeIndexes = List.copyOf(Objects.requireNonNull(
                indexes, "target indexes must not be null"));
        List<ForeignKeyDefinition> safeForeignKeys = List.copyOf(Objects.requireNonNull(
                foreignKeys, "target foreign keys must not be null"));
        ReactiveFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "reactive form metadata reader must not be null");
        SchemaMigrationOptions safeOptions = Objects.requireNonNull(options,
                                                                     "schema migration options must not be null");
        return Mono.defer(() -> safeReader.readTableForSchema(safeForm.relationIdentity()))
                         .map(Optional::of)
                         .onErrorResume(SchemaMigrationPlanner::isTableNotFound,
                                        failure -> Mono.just(Optional.empty()))
                         .flatMap(current -> current.map(table -> migrateSafelyPlanReactive(
                                 table, safeForm, safeIndexes, safeForeignKeys, safeOptions, safeReader)
                                 .flatMap(primary -> protectedSchemas.planExistingReactive(
                                         safeForm, primary, safeReader, safeOptions)))
                                 .orElseGet(() -> Mono.fromSupplier(() -> createTablePlan(
                                         safeForm, safeIndexes, safeForeignKeys, safeOptions))));
    }

    SchemaMigrationPlan planJdbc(DynamicForm form,
                                   List<IndexDefinition> indexes,
                                   List<ForeignKeyDefinition> foreignKeys,
                                   JdbcFormMetadataReader metadataReader,
                                   SchemaMigrationOptions options) {
        DynamicForm safeForm = Objects.requireNonNull(form, "target dynamic form must not be null");
        JdbcFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "jdbc form metadata reader must not be null");
        SchemaMigrationOptions safeOptions = Objects.requireNonNull(
                options, "schema migration options must not be null");
        RelationalTableDefinition current;
        try {
            current = safeReader.readTableForSchema(safeForm.relationIdentity());
        } catch (IllegalArgumentException failure) {
            if (!isTableNotFound(failure)) {
                throw failure;
            }
            return createTablePlan(safeForm, indexes, foreignKeys, safeOptions);
        }
        MigrationChanges changes = prepareChanges(current, safeForm, indexes, foreignKeys, safeOptions);
        SchemaSnapshot snapshot = needsPhysicalSnapshot(changes.operations())
                ? safeReader.readSnapshot(safeForm.relationIdentity()) : null;
        SchemaMigrationPlan primary = renderer.renderPlan(current, snapshot,
                renderChanges(current, changes, snapshot), safeOptions);
        return protectedSchemas.planExistingJdbc(safeForm, primary, safeReader, safeOptions);
    }

    SchemaMigrationPlan createTablePlan(DynamicForm target,
                                        List<IndexDefinition> targetIndexes,
                                        List<ForeignKeyDefinition> targetForeignKeys) {
        return createTablePlan(target, targetIndexes, targetForeignKeys, SchemaMigrationOptions.safe());
    }

    SchemaMigrationPlan createTablePlan(DynamicForm target,
                                                List<IndexDefinition> targetIndexes,
                                                List<ForeignKeyDefinition> targetForeignKeys,
                                                SchemaMigrationOptions options) {
        return protectedSchemas.appendCreatePlan(target,
                renderer.renderPlan(null, null, createPrimaryTablePlan(target, targetIndexes, targetForeignKeys), options),
                options);
    }

    SchemaMigrationPlan createPrimaryTablePlan(DynamicForm target,
                                                List<IndexDefinition> targetIndexes,
                                                List<ForeignKeyDefinition> targetForeignKeys) {
        ProtectedSchemaTarget resolved = ProtectedSchemaTarget.resolve(
                target, targetIndexes, targetForeignKeys);
        DynamicForm safeTarget = resolved.form();
        List<IndexDefinition> safeIndexes = resolved.indexes();
        List<ForeignKeyDefinition> safeForeignKeys = resolved.foreignKeys();
        RelationalTableDefinition definition = SchemaMigrationSupport.tableDefinition(safeTarget, safeIndexes, safeForeignKeys);
        SchemaOperation operation = SchemaOperation.of(SchemaOperation.Kind.CREATE_TABLE, definition.identity(),
                definition.identity().table(), null, definition, SchemaOperation.Compatibility.SAFE_INCREMENTAL);
        return SchemaMigrationPlan.planned(definition, false,
                List.of(new SchemaPlanStep(operation, tables.render(operation))), List.of());
    }

    List<SqlRequest> migrate(DynamicFormChangeSet changeSet) {
        return migrate(changeSet, null);
    }

    List<SqlRequest> migrate(DynamicFormChangeSet changeSet, SchemaSnapshot snapshot) {
        DynamicFormChangeSet changes = Objects.requireNonNull(changeSet, "dynamic form change set must not be null");
        if (!changes.source().protections().isEmpty() || !changes.target().protections().isEmpty()) {
            throw new IllegalArgumentException("protected fields require a reviewed schema migration plan");
        }
        RelationIdentity relation = changes.target().relationIdentity();
        RelationalTableDefinition source = changes.source().toTableDefinition(relation);
        RelationalTableDefinition target = changes.target().toTableDefinition(relation);
        List<SchemaOperation> operations = new ArrayList<>();
        Map<String, SchemaSequenceSqlRenderer.Definition> sequences = SchemaSequenceSqlRenderer.definitions(dialect,
                source.columns(), tables::dataType, true);
        changes.changedFields().forEach(this::validateDirectFieldChange);
        SchemaDiffer.diffColumns(relation, target.columns(), source.columns(), operations,
                dialect, false, Map.of(), null);
        SchemaDiffer.diffIndexes(relation, target.indexes(), SchemaSnapshot.Observed.present(source.indexes()),
                operations, dialect);
        List<SchemaPlanStep> steps = planOperations(operations, source,
                changes.target(), snapshot, true, sequences);
        SchemaMigrationPlan builtin = SchemaMigrationPlan.planned(target, true, steps, List.of());
        return renderer.renderPlan(source, snapshot, builtin).requestsForExecution();
    }

    private void validateDirectFieldChange(FieldChange change) {
        DynamicField source = change.source();
        DynamicField target = change.target();
        if (source.primaryKey() != target.primaryKey()
                || !Objects.equals(source.generation().sequenceName(), target.generation().sequenceName())
                || !SchemaGeneratedValueComparison.sameOptions(
                        source.generation(), target.generation(), dialect.generatedValueStyle())) {
            throw new IllegalArgumentException("column constraint changes require a reviewed migration plan");
        }
    }

    SchemaMigrationPlan migrateSafelyPlan(RelationalTableDefinition current,
                                          DynamicForm target,
                                          List<IndexDefinition> targetIndexes,
                                          List<ForeignKeyDefinition> targetForeignKeys,
                                          SchemaMigrationOptions options) {
        return migrateSafelyPlan(current, target, targetIndexes, targetForeignKeys, options, null);
    }

    private Mono<SchemaMigrationPlan> migrateSafelyPlanReactive(RelationalTableDefinition current,
                                                       DynamicForm target,
                                                       List<IndexDefinition> indexes,
                                                       List<ForeignKeyDefinition> foreignKeys,
                                                       SchemaMigrationOptions options,
                                                       ReactiveFormMetadataReader reader) {
        MigrationChanges changes = prepareChanges(current, target, indexes, foreignKeys, options);
        if (!needsPhysicalSnapshot(changes.operations())) {
            return Mono.fromSupplier(() -> renderer.renderPlan(current, null,
                    renderChanges(current, changes, null), options));
        }
        return reader.readSnapshot(target.relationIdentity())
                .switchIfEmpty(Mono.error(new IllegalStateException(
                        "rewriting an existing column requires a physical schema snapshot")))
                .map(snapshot -> renderer.renderPlan(current, snapshot,
                        renderChanges(current, changes, snapshot), options));
    }

    /** 只为已选中的实际列操作补读物理事实，不重新比较表单和迁移选项。 */
    private boolean needsPhysicalSnapshot(List<SchemaOperation> operations) {
        boolean sqlServer = dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.SQL_SERVER;
        boolean oracle = dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.ORACLE;
        boolean fullColumn = dialect.rewritesFullColumnDefinition();
        if (!fullColumn && !sqlServer && !oracle) return false;
        for (SchemaOperation operation : operations) {
            // SQL Server 删除列前还需删除绑定的 DEFAULT；名称必须从字典读取。
            if (sqlServer && operation.kind() == SchemaOperation.Kind.DROP_COLUMN) return true;
            if (operation.kind() != SchemaOperation.Kind.CHANGE_COLUMN) continue;
            ColumnDefinition column = (ColumnDefinition) operation.actual();
            ColumnDefinition target = (ColumnDefinition) operation.desired();
            if (sqlServer && target.defaultValue().kind() != com.flying.orm.core.metadata.ColumnDefault.Kind.NONE
                    && (!SchemaDefinitionEquality.sameDefault(column.defaultValue(), target.defaultValue(), dialect)
                        || target.defaultConstraintName() != null
                            && !target.defaultConstraintName().equals(column.defaultConstraintName()))) {
                return true;
            }
            if (!fullColumn && !tables.requiresPhysicalCollation(target)
                    && !tables.requiresPhysicalCollation(column)
                    && !tables.requiresPhysicalLengthUnit(target)
                    && !tables.requiresPhysicalLengthUnit(column)) {
                continue;
            }
            boolean storageChanged = !tables.sameDataType(tables.metadataDataType(column), tables.dataType(target))
                    || !SchemaGeneratedValueComparison.same(column.generation(), target.generation(),
                                                            dialect.generatedValueStyle());
            if (column.nullable() != target.nullable()) {
                if (storageChanged || !oracle
                        || SchemaMigrationSupport.logicalTemporalTypeChanged(
                                column.databaseType(), target.databaseType())) return true;
                continue;
            }
            if (storageChanged || fullColumn
                    && !Objects.equals(tables.storageComment(column), tables.storageComment(target))) return true;
        }
        return false;
    }

    SchemaMigrationPlan migrateSafelyPlan(RelationalTableDefinition current,
                                          DynamicForm target,
                                          List<IndexDefinition> targetIndexes,
                                          List<ForeignKeyDefinition> targetForeignKeys,
                                          SchemaMigrationOptions options,
                                          SchemaSnapshot snapshot) {
        return renderChanges(current, prepareChanges(current, target, targetIndexes, targetForeignKeys, options), snapshot);
    }

    private MigrationChanges prepareChanges(RelationalTableDefinition current, DynamicForm target,
                                             List<IndexDefinition> targetIndexes,
                                             List<ForeignKeyDefinition> targetForeignKeys,
                                             SchemaMigrationOptions options) {
        RelationalTableDefinition safeCurrent = Objects.requireNonNull(current, "current table metadata must not be null");
        ProtectedSchemaTarget resolved = ProtectedSchemaTarget.resolve(
                target, targetIndexes, targetForeignKeys);
        resolved.validateExistingStorage(safeCurrent, target);
        DynamicForm safeTarget = resolved.form();
        List<IndexDefinition> safeIndexes = resolved.indexes();
        List<ForeignKeyDefinition> safeForeignKeys = resolved.foreignKeys();
        SchemaMigrationOptions safeOptions = Objects.requireNonNull(options,
                                                                     "schema migration options must not be null");
        RelationIdentity relation = safeTarget.relationIdentity();
        List<SchemaOperation> operations = new ArrayList<>();
        List<SkippedSchemaChange> skipped = new ArrayList<>();

        // 改名必须先校验并加入计划，后面的字段、主键和索引比较才按新名字判断。
        SchemaMigrationSupport.validateColumnRenames(safeCurrent, safeTarget, safeOptions.columnRenames());
        SchemaForeignKeyPlanner.Changes foreignKeyChanges = SchemaForeignKeyPlanner.planChanges(
                relation, safeCurrent, safeForeignKeys, safeOptions.columnRenames());
        operations.addAll(foreignKeyChanges.operations());
        safeOptions.columnRenames().forEach((oldName, newName) -> {
            ColumnDefinition source = safeCurrent.column(oldName);
            operations.add(SchemaOperation.of(SchemaOperation.Kind.RENAME_COLUMN, relation, newName,
                    source, source.toBuilder(com.flying.orm.core.field.FieldIdentity.of(newName)).build(),
                    SchemaOperation.Compatibility.REQUIRES_REVIEW));
        });
        List<String> physicalPrimaryKeys = SchemaMigrationSupport.currentPrimaryKeys(safeCurrent);
        List<String> currentPrimaryKeys = SchemaMigrationSupport.renamedNames(
                physicalPrimaryKeys, safeOptions.columnRenames());
        List<String> targetPrimaryKeys = SchemaMigrationSupport.targetPrimaryKeys(safeTarget);
        // 增量声明中未提及的主键列保留；只有显式删列选项才把遗漏列视为删除意图。
        List<String> comparedPrimaryKeys = safeOptions.dropColumnAllowed() ? currentPrimaryKeys
                : currentPrimaryKeys.stream().filter(name -> safeTarget.findField(name).isPresent()).toList();
        boolean primaryKeyChanged = !SchemaMigrationSupport.sameNames(comparedPrimaryKeys, targetPrimaryKeys);
        if (primaryKeyChanged) {
            skipped.add(new SkippedSchemaChange(SkippedSchemaChange.Kind.CHANGE_PRIMARY_KEY,
                    safeTarget.table(), "primary key changes require a complete table target",
                    Map.of("oldPrimaryKeys", currentPrimaryKeys, "newPrimaryKeys", targetPrimaryKeys)));
        }

        List<ColumnDefinition> sequenceOccupants = new ArrayList<>(safeCurrent.columns());
        // 先生成结构操作；列、索引和外键的执行顺序由共用依赖规则确定。
        Set<String> matchedCurrentColumnNames = new HashSet<>();
        for (DynamicField field : safeTarget.fields()) {
            String currentName = SchemaMigrationSupport.renameSourceForTarget(safeOptions.columnRenames(), field.name());
            String lookupName = currentName == null ? field.name() : currentName;
            ColumnDefinition column = safeCurrent.findColumn(lookupName).orElse(null);
            if (column != null) {
                sequenceOccupants.add(field.column());
                matchedCurrentColumnNames.add(column.name());
                if (currentName == null && !column.name().equals(field.name())) {
                    skipped.add(new SkippedSchemaChange(
                            SkippedSchemaChange.Kind.CHANGE_COLUMN,
                            field.name(),
                            "physical column name differs; declare an explicit column rename",
                            Map.of("currentColumn", column.name(), "targetColumn", field.name())));
                    continue;
                }
                if (physicalPrimaryKeys.contains(column.name()) != field.primaryKey()) {
                    if (!primaryKeyChanged) {
                        skipped.add(new SkippedSchemaChange(SkippedSchemaChange.Kind.CHANGE_PRIMARY_KEY,
                                field.name(), "primary key changes require a complete table target"));
                    }
                    continue;
                }
                String skipReason = SchemaMigrationSupport.columnChangeSkipReason(column, field, safeOptions, tables);
                if (skipReason != null) {
                    skipped.add(new SkippedSchemaChange(SkippedSchemaChange.Kind.CHANGE_COLUMN, field.name(), skipReason));
                    continue;
                }
                ColumnDefinition desired = field.column();
                if (ProtectedSchemaTarget.sameProtectedStorage(column, field)) {
                    desired = column.toBuilder(desired.identity()).comment(desired.comment())
                            .defaultValue(desired.defaultValue()).build();
                }
                operations.add(SchemaOperation.of(SchemaOperation.Kind.CHANGE_COLUMN, relation,
                        desired.name(), column, desired, SchemaOperation.Compatibility.REQUIRES_REVIEW));
            } else if (!primaryKeyChanged || !field.primaryKey()) {
                operations.add(SchemaOperation.of(SchemaOperation.Kind.ADD_COLUMN, relation,
                        field.name(), null, field.column(), SchemaOperation.Compatibility.SAFE_INCREMENTAL));
            }
        }
        List<ColumnDefinition> unmatchedColumns = safeCurrent.columns().stream()
                .filter(column -> !matchedCurrentColumnNames.contains(column.name())
                        && SchemaMigrationSupport.renameTargetForSource(safeOptions.columnRenames(), column.name()) == null
                        && (!primaryKeyChanged || !physicalPrimaryKeys.contains(column.name()))).toList();
        Set<String> removedColumns = safeOptions.dropColumnAllowed()
                ? unmatchedColumns.stream().map(ColumnDefinition::name).collect(java.util.stream.Collectors.toSet())
                : Set.of();
        SchemaIndexPlanner.Changes indexChanges = SchemaIndexPlanner.planChanges(
                skipped, safeCurrent, safeTarget, safeIndexes, safeOptions, removedColumns,
                foreignKeyChanges.retained(), dialect);
        Set<String> blockedColumns = indexChanges.blockedColumns();
        for (ForeignKeyDefinition foreignKey : foreignKeyChanges.retained()) {
            blockedColumns.addAll(foreignKey.columns());
            if (foreignKey.reference().equals(relation)) {
                blockedColumns.addAll(foreignKey.referenceColumns());
            }
        }
        operations.addAll(indexChanges.operations());
        for (ColumnDefinition column : unmatchedColumns) {
            if (blockedColumns.contains(column.name())) {
                skipped.add(new SkippedSchemaChange(SkippedSchemaChange.Kind.DROP_COLUMN, column.name(),
                        "column is still referenced by a retained index or foreign key"));
            } else if (safeOptions.dropColumnAllowed()) {
                operations.add(SchemaOperation.of(SchemaOperation.Kind.DROP_COLUMN, relation,
                        column.name(), column, null, SchemaOperation.Compatibility.REQUIRES_REVIEW));
            } else {
                skipped.add(new SkippedSchemaChange(SkippedSchemaChange.Kind.DROP_COLUMN, column.name(),
                        "SAFE mode does not drop existing columns"));
            }
        }
        // 数据字典默认值只证明序列身份；完整声明才校验已有序列参数。
        Map<String, SchemaSequenceSqlRenderer.Definition> sequences = SchemaSequenceSqlRenderer.definitions(dialect, sequenceOccupants, tables::dataType, false);
        return new MigrationChanges(safeTarget,
                SchemaMigrationSupport.tableDefinition(safeTarget, safeIndexes, safeForeignKeys, safeCurrent),
                operations, skipped, sequences);
    }

    /** 同一次规划的操作供元数据需求判断和最终 SQL 渲染共同使用。 */
    private record MigrationChanges(DynamicForm form, RelationalTableDefinition target,
                                    List<SchemaOperation> operations, List<SkippedSchemaChange> skipped,
                                    Map<String, SchemaSequenceSqlRenderer.Definition> sequences) {
    }

    private SchemaMigrationPlan renderChanges(RelationalTableDefinition current,
                                               MigrationChanges changes, SchemaSnapshot snapshot) {
        return SchemaMigrationPlan.planned(changes.target(), true,
                planOperations(changes.operations(), current, changes.form(), snapshot, false, changes.sequences()),
                changes.skipped());
    }

    private List<SchemaPlanStep> planOperations(List<SchemaOperation> operations,
                                              RelationalTableDefinition current, DynamicForm target,
                                              SchemaSnapshot snapshot, boolean declaredSource,
                                              Map<String, SchemaSequenceSqlRenderer.Definition> sequences) {
        RelationalTableDefinition physical = SchemaMigrationSupport.physicalColumns(snapshot);
        operations.replaceAll(operation -> operation.kind() == SchemaOperation.Kind.CHANGE_COLUMN
                ? tables.prepareColumnChange(operation, snapshot, declaredSource) : operation);
        operations.removeIf(Objects::isNull);
        return SchemaOperationPlanner.plan(operations, current, dialect,
                operation -> tables.changesType(current.column(((ColumnDefinition) operation.actual()).name()),
                        (ColumnDefinition) operation.desired(), snapshot, declaredSource),
                operation -> switch (operation.kind()) {
                case ADD_COLUMN -> tables.addColumn(target.relationIdentity(),
                        target.findField(operation.objectName()).orElseThrow(), sequences);
                case CHANGE_COLUMN -> {
                    ColumnDefinition source = current.column(((ColumnDefinition) operation.actual()).name());
                    yield tables.changeColumn(operation, tables.actualType(source, snapshot, declaredSource),
                            source.databaseType());
                }
                case DROP_COLUMN -> tables.render(physical == null ? operation : SchemaOperation.of(
                        operation.kind(), operation.relation(), operation.objectName(),
                        physical.column(operation.objectName()), null, operation.compatibility()));
                default -> tables.render(operation);
            });
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
