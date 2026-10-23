package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


/**
 * 把一次关系 diff 和精确 SQL冻结成可复核计划。
 *
 * <p>只使用调用方已经读取的快照，期间不会再次访问数据库。方言渲染错误直接传播；
 * 快照缺失或依赖未解析时规划失败，不发布部分可执行计划。</p>
 *
 * @author wangr
 * @version v3.2
 */
final class RelationalSchemaPlanReviewer {

    private final RdbDialect dialect;
    private final RelationalSchemaSqlRenderer renderer;
    private final RelationalObjectNameGenerator names;

    private RelationalSchemaPlanReviewer(RdbDialect dialect) {
        this.dialect = Objects.requireNonNull(dialect, "RDB dialect must not be null");
        renderer = RelationalSchemaSqlRenderer.create(dialect.schema());
        names = RelationalObjectNameGenerator.forDialect(dialect);
    }

    public static RelationalSchemaPlanReviewer create(RdbDialect dialect) {
        return new RelationalSchemaPlanReviewer(dialect);
    }

    SchemaMigrationPlan planIncremental(DatabaseDescriptor database,
                                       RelationalTableDefinition declared,
                                       SchemaSnapshot actual,
                                       SchemaSnapshotCoverage coverage) {
        return review(database, SchemaDiffer.incrementalTarget(declared, actual, dialect.schema()),
                actual, coverage, SchemaCompatibilityMode.SAFE_INCREMENTAL, null,
                declared.columns().stream().map(column ->
                        SchemaDefinitionEquality.canonicalName(column.name(), dialect.schema()))
                        .collect(java.util.stream.Collectors.toSet()));
    }

    /** 使用显式 reader coverage；不完整时规划失败，不生成可执行计划。 */
    public SchemaMigrationPlan review(DatabaseDescriptor database,
                                     RelationalTableDefinition desired,
                                     SchemaSnapshot actual,
                                     SchemaSnapshotCoverage coverage,
                                     SchemaCompatibilityMode mode) {
        return review(database, desired, actual, coverage, mode, null);
    }

    SchemaMigrationPlan reviewAbsent(DatabaseDescriptor database,
                                    RelationIdentity relation,
                                    SchemaSnapshot actual,
                                    SchemaSnapshotCoverage coverage) {
        DatabaseDescriptor safeDatabase = Objects.requireNonNull(
                database, "database descriptor must not be null");
        RelationIdentity safeRelation = Objects.requireNonNull(
                relation, "absent relational target identity must not be null");
        SchemaSnapshot safeActual = Objects.requireNonNull(
                actual, "actual schema snapshot must not be null");
        SchemaSnapshotCoverage safeCoverage = Objects.requireNonNull(
                coverage, "schema snapshot coverage must not be null");
        requireMatchingDialect(safeDatabase);
        boolean knownAbsent = safeCoverage.observes(SchemaSnapshotCoverage.Fact.TABLE_EXISTENCE)
                && safeActual.tableState() == SchemaSnapshot.State.ABSENT;
        boolean completePresent = safeCoverage.isComplete()
                && safeActual.tableState() == SchemaSnapshot.State.PRESENT;
        if (completePresent) {
            requireValidRelation(safeRelation);
        }

        SchemaMigrationPlan.Builder plan = SchemaMigrationPlan.builder(safeDatabase)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredAbsent(safeRelation)
                .snapshotCoverage(safeCoverage)
                .tableExists(safeActual.tableState() == SchemaSnapshot.State.PRESENT);
        if (!knownAbsent && !completePresent) {
            throw new IllegalStateException("schema planning requires complete snapshot coverage");
        }
        List<SchemaOperation> operations = SchemaDiffer.diffAbsent(
                safeRelation,
                safeActual,
                safeDatabase.capabilities(),
                SchemaCompatibilityMode.EXACT,
                dialect.schema()).operations();
        return reviewOperations(
                plan, operations, safeActual,
                renderer.sequenceDefinitions(List.of(safeActual)));
    }

    Map<String, SchemaSequenceSqlRenderer.Definition> observedSequences(List<SchemaSnapshot> snapshots) {
        return renderer.sequenceDefinitions(snapshots);
    }

    SchemaMigrationPlan review(DatabaseDescriptor database,
                              RelationalTableDefinition desired,
                              SchemaSnapshot actual,
                              SchemaSnapshotCoverage coverage,
                              SchemaCompatibilityMode mode,
                              Map<String, SchemaSequenceSqlRenderer.Definition> sharedSequences) {
        return review(database, desired, actual, coverage, mode, sharedSequences, null);
    }

    private SchemaMigrationPlan review(DatabaseDescriptor database, RelationalTableDefinition desired,
                                       SchemaSnapshot actual, SchemaSnapshotCoverage coverage,
                                       SchemaCompatibilityMode mode,
                                       Map<String, SchemaSequenceSqlRenderer.Definition> sharedSequences,
                                       Set<String> declaredColumns) {
        DatabaseDescriptor safeDatabase = Objects.requireNonNull(
                database, "database descriptor must not be null");
        RelationalTableDefinition safeDesired = Objects.requireNonNull(
                desired, "desired relational table must not be null");
        SchemaSnapshot safeActual = Objects.requireNonNull(
                actual, "actual schema snapshot must not be null");
        SchemaSnapshotCoverage safeCoverage = Objects.requireNonNull(
                coverage, "schema snapshot coverage must not be null");
        SchemaCompatibilityMode safeMode = Objects.requireNonNull(
                mode, "schema compatibility mode must not be null");
        requireMatchingDialect(safeDatabase);
        if (safeCoverage.isComplete()) {
            requireValidIdentifiers(safeDesired);
        }

        SchemaMigrationPlan.Builder plan = SchemaMigrationPlan.builder(safeDatabase)
                .compatibilityMode(safeMode)
                .desiredTable(safeDesired)
                .snapshotCoverage(safeCoverage)
                .tableExists(safeActual.tableState() == SchemaSnapshot.State.PRESENT);
        if (!safeCoverage.isComplete()) {
            throw new IllegalStateException("schema planning requires complete snapshot coverage");
        }
        RelationalTableDdlValidator.validate(safeDesired, dialect.schema());
        Map<String, SchemaSequenceSqlRenderer.Definition> sequences = sharedSequences == null
                ? renderer.sequenceDefinitions(List.of(safeActual))
                : sharedSequences;
        List<SchemaOperation> operations = SchemaDiffer.diff(
                safeDesired, safeActual, safeDatabase.capabilities(), safeMode,
                safeDatabase.dialectId(), dialect.schema(), declaredColumns).operations();
        return reviewOperations(plan, operations, safeActual, sequences);
    }

    private SchemaMigrationPlan reviewOperations(SchemaMigrationPlan.Builder plan,
                                                List<SchemaOperation> operations,
                                                SchemaSnapshot actual,
                                                Map<String, SchemaSequenceSqlRenderer.Definition> sequences) {
        SchemaOperationPlanner.plan(operations, actual.completeTable().orElse(null), dialect.schema(),
                operation -> !SchemaDefinitionEquality.sameColumnType(
                        (ColumnDefinition) operation.actual(), (ColumnDefinition) operation.desired(),
                        dialect.schema(), actual.physicalColumnTypes()),
                operation -> renderer.render(operation, sequences, actual.physicalColumnTypes(),
                        operation.actual() instanceof ColumnDefinition column
                                ? actual.observedLogicalTypes().get(column.name()) : null))
                .forEach(plan::addStep);
        return plan.build();
    }

    SchemaDialect schemaDialect() {
        return dialect.schema();
    }


    private void requireMatchingDialect(DatabaseDescriptor database) {
        if (!database.dialectId().equals(dialect.name())) {
            throw new IllegalArgumentException("database descriptor and schema renderer dialect must match");
        }
        if (!database.capabilityFingerprint().equals(dialect.capabilities().fingerprint())) {
            throw new IllegalArgumentException("database descriptor and schema renderer capabilities must match");
        }
    }

    private void requireValidIdentifiers(RelationalTableDefinition table) {
        requireValidRelation(table.identity());
        table.columns().forEach(column -> {
            names.column(column.name());
            if (column.generation().strategy() == ValueGeneration.Strategy.SEQUENCE) {
                names.qualifiedObject(column.generation().sequenceName());
            }
        });
        table.primaryKey().ifPresent(primaryKey -> names.object(primaryKey.name()));
        table.uniqueConstraints().forEach(unique -> names.object(unique.name()));
        table.indexes().forEach(index -> names.object(index.name()));
        table.checks().forEach(check -> names.object(check.name()));
        table.foreignKeys().forEach(foreignKey -> {
            names.object(foreignKey.name());
            requireValidRelation(foreignKey.reference());
        });
    }

    private void requireValidRelation(RelationIdentity relation) {
        relation.catalog().ifPresent(names::object);
        relation.schema().ifPresent(names::object);
        names.table(relation.table());
    }

}
