package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationalMetadataFingerprint;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 把一次关系 diff、精确 SQL 和执行前置条件冻结成可复核计划。
 *
 * <p>审阅只使用调用方已经读取的快照，期间不会再次访问数据库。不能由当前方言可靠渲染的 operation
 * 仍保留在计划中，但没有 SQL，执行器因此不会把缺失能力误报成成功。</p>
 *
 * @author wangr
 * @version v3.2
 */
final class RelationalSchemaPlanReviewer {

    private enum MigrationStage {
        EARLY_ADD,
        REMOVE_DISRUPTIVE_DEPENDENCY,
        ALTER_COLUMNS,
        RESTORE_DEPENDENCY,
        LATE_DROP
    }

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

    /** 使用显式 reader coverage；不完整时冻结人工步骤且不生成任何可执行 SQL。 */
    public ReviewedSchemaPlan review(DatabaseDescriptor database,
                                     RelationalTableDefinition desired,
                                     SchemaSnapshot actual,
                                     SchemaSnapshotCoverage coverage,
                                     SchemaCompatibilityMode mode) {
        return review(database, desired, actual, coverage, mode, null);
    }

    ReviewedSchemaPlan reviewAbsent(DatabaseDescriptor database,
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

        String actualFingerprint = SchemaSnapshotFingerprint.of(safeActual);
        List<SchemaPlanPrecondition> preconditions = preconditions(
                safeDatabase, actualFingerprint, safeCoverage);
        ReviewedSchemaPlan.Builder plan = ReviewedSchemaPlan.builder(safeDatabase)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredAbsent(safeRelation)
                .comparisonDialect(dialect.schema())
                .desiredFingerprint(SchemaSnapshotFingerprint.of(SchemaSnapshot.absent(safeRelation)))
                .actualFingerprint(actualFingerprint)
                .snapshotCoverage(safeCoverage);
        if (!knownAbsent && !completePresent) {
            return addCoverageManualStep(
                    plan, safeRelation, safeActual, safeDatabase, preconditions);
        }
        List<SchemaOperation> operations = SchemaDiffer.diffAbsent(
                safeRelation,
                safeActual,
                safeDatabase.capabilities(),
                SchemaCompatibilityMode.EXACT,
                dialect.schema()).operations();
        return reviewOperations(
                plan, operations, null, safeActual, safeDatabase, preconditions,
                renderer.sequenceDefinitions(List.of(safeActual)));
    }

    Map<String, SchemaSequenceSqlRenderer.Definition> observedSequences(List<SchemaSnapshot> snapshots) {
        return renderer.sequenceDefinitions(snapshots);
    }

    ReviewedSchemaPlan review(DatabaseDescriptor database,
                              RelationalTableDefinition desired,
                              SchemaSnapshot actual,
                              SchemaSnapshotCoverage coverage,
                              SchemaCompatibilityMode mode,
                              Map<String, SchemaSequenceSqlRenderer.Definition> sharedSequences) {
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

        String actualFingerprint = SchemaSnapshotFingerprint.of(safeActual);
        List<SchemaPlanPrecondition> preconditions = preconditions(
                safeDatabase, actualFingerprint, safeCoverage);
        ReviewedSchemaPlan.Builder plan = ReviewedSchemaPlan.builder(safeDatabase)
                .compatibilityMode(safeMode)
                .desiredTable(safeDesired)
                .comparisonDialect(dialect.schema())
                .desiredFingerprint(RelationalMetadataFingerprint.of(safeDesired))
                .actualFingerprint(actualFingerprint)
                .snapshotCoverage(safeCoverage);
        if (!safeCoverage.isComplete()) {
            return addCoverageManualStep(
                    plan, safeDesired.identity(), safeActual, safeDatabase, preconditions);
        }
        try {
            RelationalTableDdlValidator.validate(safeDesired, dialect.schema());
        } catch (UnsupportedOperationException unsupported) {
            SchemaOperation manual = SchemaOperation.of(
                    SchemaOperation.Kind.VERIFY_MANUALLY,
                    safeDesired.identity(),
                    "table-partition",
                    null,
                    null,
                    SchemaOperation.Compatibility.REQUIRES_REVIEW);
            return plan.addStep(SchemaPlanStep.manual(
                    0,
                    manual,
                    SchemaRiskClassifier.classify(
                            manual, safeActual, safeDatabase.capabilities()),
                    preconditions)).build();
        }
        Map<String, SchemaSequenceSqlRenderer.Definition> sequences = sharedSequences == null
                ? renderer.sequenceDefinitions(List.of(safeActual))
                : sharedSequences;
        List<SchemaOperation> operations = SchemaDiffer.diff(
                safeDesired, safeActual, safeDatabase.capabilities(), safeMode,
                safeDatabase.dialectId(), dialect.schema()).operations();
        return reviewOperations(
                plan, operations, safeDesired, safeActual, safeDatabase, preconditions, sequences);
    }

    private ReviewedSchemaPlan reviewOperations(ReviewedSchemaPlan.Builder plan,
                                                List<SchemaOperation> operations,
                                                RelationalTableDefinition desired,
                                                SchemaSnapshot actual,
                                                DatabaseDescriptor database,
                                                List<SchemaPlanPrecondition> preconditions,
                                                Map<String, SchemaSequenceSqlRenderer.Definition> sequences) {
        Set<String> changedColumns = changedColumns(operations);
        Set<String> disruptiveColumns = disruptiveColumns(operations, actual.physicalColumnTypes());
        List<SelfReference> desiredSelfReferences = selfReferences(desired);
        List<SelfReference> actualSelfReferences = actual.completeTable()
                .map(RelationalSchemaPlanReviewer::selfReferences)
                .orElseGet(List::of);
        Set<String> earlyForeignKeyDrops = candidateDependencyDrops(
                operations, actualSelfReferences, dialect.schema());
        List<SchemaOperation> ordered = operations.stream()
                .sorted(Comparator
                        .comparing((SchemaOperation operation) -> migrationStage(
                                operation, changedColumns, disruptiveColumns,
                                earlyForeignKeyDrops))
                        .thenComparingInt(operation -> operationOrder(operation.kind()))
                        // SchemaDiffer already ordered new columns by their desired physical position.
                        .thenComparing(operation -> operation.kind() == SchemaOperation.Kind.ADD_COLUMN
                                ? "" : operation.objectName()))
                .toList();
        Set<String> dependencyReadyColumns = new HashSet<>();
        int order = 0;
        for (SchemaOperation operation : ordered) {
            SchemaMigrationRiskLevel risk = SchemaRiskClassifier.classify(
                    operation, actual, database.capabilities());
            if (requiresManualAction(
                    operation, operations, changedColumns, disruptiveColumns, dependencyReadyColumns,
                    desiredSelfReferences, actualSelfReferences, earlyForeignKeyDrops)) {
                plan.addStep(SchemaPlanStep.manual(order++, operation, risk, preconditions));
                continue;
            }
            try {
                for (SqlRequest request : renderer.render(operation, sequences, actual.physicalColumnTypes(),
                        operation.actual() instanceof ColumnDefinition column
                                ? actual.observedLogicalTypes().get(column.name()) : null)) {
                    plan.addStep(SchemaPlanStep.executable(
                            order++, operation, request, risk, preconditions));
                }
                if (operation.kind() == SchemaOperation.Kind.CHANGE_COLUMN
                        && (disruptiveColumns.contains(operation.objectName())
                            || ((ColumnDefinition) operation.actual()).nullable()
                                && !((ColumnDefinition) operation.desired()).nullable())) {
                    // 单一安全扩宽或 NOT NULL 收紧已排在前面；后续依赖步骤复用 renderer 的证明。
                    dependencyReadyColumns.add(operation.objectName());
                }
            } catch (UnsupportedOperationException unsupported) {
                plan.addStep(SchemaPlanStep.manual(order++, operation, risk, preconditions));
            }
        }
        return plan.build();
    }

    private ReviewedSchemaPlan addCoverageManualStep(ReviewedSchemaPlan.Builder plan,
                                                     RelationIdentity relation,
                                                     SchemaSnapshot actual,
                                                     DatabaseDescriptor database,
                                                     List<SchemaPlanPrecondition> preconditions) {
        SchemaOperation manual = SchemaOperation.of(
                SchemaOperation.Kind.VERIFY_MANUALLY,
                relation,
                "snapshot-coverage",
                null,
                null,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);
        return plan.addStep(SchemaPlanStep.manual(
                0,
                manual,
                SchemaRiskClassifier.classify(manual, actual, database.capabilities()),
                preconditions)).build();
    }

    private static List<SchemaPlanPrecondition> preconditions(DatabaseDescriptor database,
                                                              String actualFingerprint,
                                                              SchemaSnapshotCoverage coverage) {
        return List.of(
                SchemaPlanPrecondition.database(database.fingerprint()),
                SchemaPlanPrecondition.capabilities(database.capabilityFingerprint()),
                SchemaPlanPrecondition.actualSnapshot(actualFingerprint),
                SchemaPlanPrecondition.snapshotCoverage(coverage.fingerprint()));
    }

    private Set<String> disruptiveColumns(List<SchemaOperation> operations, boolean physicalActualTypes) {
        Set<String> columns = new HashSet<>();
        for (SchemaOperation operation : operations) {
            if (operation.kind() == SchemaOperation.Kind.DROP_COLUMN) {
                columns.add(operation.objectName());
            } else if (operation.kind() == SchemaOperation.Kind.CHANGE_COLUMN) {
                ColumnDefinition actual = (ColumnDefinition) operation.actual();
                ColumnDefinition desired = (ColumnDefinition) operation.desired();
                if (!SchemaDefinitionEquality.sameColumnType(
                        actual, desired, dialect.schema(), physicalActualTypes)) {
                    columns.add(operation.objectName());
                }
            }
        }
        return columns;
    }

    SchemaDialect schemaDialect() {
        return dialect.schema();
    }

    private static Set<String> changedColumns(List<SchemaOperation> operations) {
        Set<String> columns = new HashSet<>();
        for (SchemaOperation operation : operations) {
            if (operation.kind() == SchemaOperation.Kind.ADD_COLUMN
                    || operation.kind() == SchemaOperation.Kind.CHANGE_COLUMN
                    || operation.kind() == SchemaOperation.Kind.DROP_COLUMN) {
                columns.add(operation.objectName());
            }
        }
        return columns;
    }

    private boolean requiresManualAction(SchemaOperation operation,
                                         List<SchemaOperation> operations,
                                         Set<String> changedColumns,
                                         Set<String> disruptiveColumns,
                                         Set<String> dependencyReadyColumns,
                                         List<SelfReference> desiredSelfReferences,
                                         List<SelfReference> actualSelfReferences,
                                         Set<String> earlyForeignKeyDrops) {
        if (isChangedDependency(operation.kind()) && !changedColumns.isEmpty()) {
            boolean compatible = switch (operation.kind()) {
                case CHANGE_PRIMARY_KEY, CHANGE_UNIQUE, CHANGE_INDEX -> true;
                default -> false;
            };
            for (String column : referencedColumns(operation)) {
                if (changedColumns.contains(column) && (!compatible || !dependencyReadyColumns.contains(column))) {
                    return true;
                }
            }
        }
        if (operation.kind() == SchemaOperation.Kind.CHANGE_COLUMN
                && disruptiveColumns.contains(operation.objectName())
                && desiredSelfReferences.stream().anyMatch(reference ->
                        reference.dependsOn(operation.objectName()))) {
            return true;
        }
        List<String> removedCandidate = removedCandidateKey(operation);
        if (removedCandidate != null && actualSelfReferences.stream()
                .filter(reference -> SchemaDefinitionEquality.sameCandidateKeyColumns(
                        reference.targetColumns(), removedCandidate, dialect.schema()))
                .anyMatch(reference -> !earlyForeignKeyDrops.contains(reference.name()))) {
            return true;
        }
        return (isAddDependency(operation.kind()) || isDropDependency(operation.kind()))
                && hasDisruptiveReplacement(operation, operations, disruptiveColumns);
    }

    private static List<SelfReference> selfReferences(RelationalTableDefinition desired) {
        if (desired == null) {
            return List.of();
        }
        return desired.foreignKeys().stream()
                .filter(foreignKey -> foreignKey.reference().equals(desired.identity()))
                .map(foreignKey -> new SelfReference(
                        foreignKey.name(), foreignKey.columns(), foreignKey.referenceColumns()))
                .toList();
    }

    private static Set<String> candidateDependencyDrops(
            List<SchemaOperation> operations,
            List<SelfReference> actualSelfReferences,
            SchemaDialect schemaDialect) {
        List<List<String>> changedCandidates = operations.stream()
                .map(RelationalSchemaPlanReviewer::removedCandidateKey)
                .filter(Objects::nonNull)
                .toList();
        if (changedCandidates.isEmpty()) {
            return Set.of();
        }
        Set<String> drops = new HashSet<>();
        for (SchemaOperation operation : operations) {
            if (operation.kind() != SchemaOperation.Kind.DROP_FOREIGN_KEY) {
                continue;
            }
            actualSelfReferences.stream()
                    .filter(reference -> reference.name().equals(operation.objectName()))
                    .filter(reference -> changedCandidates.stream().anyMatch(candidate ->
                            SchemaDefinitionEquality.sameCandidateKeyColumns(
                                    reference.targetColumns(), candidate, schemaDialect)))
                    .map(SelfReference::name)
                    .forEach(drops::add);
        }
        return Set.copyOf(drops);
    }

    private static List<String> removedCandidateKey(SchemaOperation operation) {
        return switch (operation.kind()) {
            case CHANGE_PRIMARY_KEY, DROP_PRIMARY_KEY ->
                    ((PrimaryKeyDefinition) operation.actual()).columns();
            case CHANGE_UNIQUE, DROP_UNIQUE ->
                    ((UniqueConstraintDefinition) operation.actual()).columns();
            case CHANGE_INDEX, DROP_INDEX -> {
                IndexDefinition index = (IndexDefinition) operation.actual();
                yield index.unique()
                        ? index.keys().stream().map(key -> key.column()).toList()
                        : null;
            }
            default -> null;
        };
    }

    private static boolean hasDisruptiveReplacement(SchemaOperation operation,
                                                    List<SchemaOperation> operations,
                                                    Set<String> disruptiveColumns) {
        if (disruptiveColumns.isEmpty()) {
            return false;
        }
        int family = dependencyFamily(operation.kind());
        boolean addition = isAddDependency(operation.kind());
        Set<String> ownColumns = referencedColumns(operation);
        return operations.stream().anyMatch(candidate -> {
            if (candidate == operation
                    || dependencyFamily(candidate.kind()) != family
                    || addition == isAddDependency(candidate.kind())) {
                return false;
            }
            Set<String> candidateColumns = referencedColumns(candidate);
            return ownColumns.stream().anyMatch(column ->
                    disruptiveColumns.contains(column) && candidateColumns.contains(column));
        });
    }

    private static boolean references(SchemaOperation operation, Set<String> columns) {
        return !columns.isEmpty() && referencedColumns(operation).stream().anyMatch(columns::contains);
    }

    private static Set<String> referencedColumns(SchemaOperation operation) {
        Set<String> columns = new HashSet<>();
        addReferencedColumns(operation.actual(), operation.relation(), columns);
        addReferencedColumns(operation.desired(), operation.relation(), columns);
        return columns;
    }

    private static void addReferencedColumns(Object definition,
                                             RelationIdentity relation,
                                             Set<String> columns) {
        switch (definition) {
            case PrimaryKeyDefinition value -> columns.addAll(value.columns());
            case UniqueConstraintDefinition value -> columns.addAll(value.columns());
            case IndexDefinition value -> value.keys().forEach(key -> columns.add(key.column()));
            case CheckConstraintDefinition value -> addReferencedColumns(value.predicate(), columns);
            case ForeignKeyDefinition value -> {
                columns.addAll(value.columns());
                if (value.reference().equals(relation)) {
                    columns.addAll(value.referenceColumns());
                }
            }
            case null, default -> {
            }
        }
    }

    private static void addReferencedColumns(CheckPredicate predicate, Set<String> columns) {
        switch (predicate) {
            case CheckPredicate.Comparison value -> columns.add(value.column());
            case CheckPredicate.Range value -> columns.add(value.column());
            case CheckPredicate.In value -> columns.add(value.column());
            case CheckPredicate.NullCheck value -> columns.add(value.column());
            case CheckPredicate.Logical value -> value.predicates()
                    .forEach(child -> addReferencedColumns(child, columns));
            case CheckPredicate.Negation value -> addReferencedColumns(value.predicate(), columns);
        }
    }

    private static MigrationStage migrationStage(SchemaOperation operation,
                                                  Set<String> changedColumns,
                                                  Set<String> disruptiveColumns,
                                                  Set<String> earlyForeignKeyDrops) {
        if (isAddDependency(operation.kind())) {
            return references(operation, changedColumns)
                    ? MigrationStage.RESTORE_DEPENDENCY : MigrationStage.EARLY_ADD;
        }
        if (isDropDependency(operation.kind())) {
            return references(operation, disruptiveColumns)
                    || operation.kind() == SchemaOperation.Kind.DROP_FOREIGN_KEY
                    && earlyForeignKeyDrops.contains(operation.objectName())
                    ? MigrationStage.REMOVE_DISRUPTIVE_DEPENDENCY : MigrationStage.LATE_DROP;
        }
        return isChangedDependency(operation.kind())
                ? MigrationStage.RESTORE_DEPENDENCY : MigrationStage.ALTER_COLUMNS;
    }

    private static boolean isAddDependency(SchemaOperation.Kind kind) {
        return switch (kind) {
            case ADD_PRIMARY_KEY, ADD_UNIQUE, ADD_INDEX, ADD_CHECK, ADD_FOREIGN_KEY -> true;
            default -> false;
        };
    }

    private static boolean isDropDependency(SchemaOperation.Kind kind) {
        return switch (kind) {
            case DROP_PRIMARY_KEY, DROP_UNIQUE, DROP_INDEX, DROP_CHECK, DROP_FOREIGN_KEY -> true;
            default -> false;
        };
    }

    private static boolean isChangedDependency(SchemaOperation.Kind kind) {
        return switch (kind) {
            case CHANGE_PRIMARY_KEY, CHANGE_UNIQUE, CHANGE_INDEX, CHANGE_CHECK, CHANGE_FOREIGN_KEY -> true;
            default -> false;
        };
    }

    private static int dependencyFamily(SchemaOperation.Kind kind) {
        return switch (kind) {
            case ADD_PRIMARY_KEY, DROP_PRIMARY_KEY -> 1;
            case ADD_UNIQUE, DROP_UNIQUE -> 2;
            case ADD_INDEX, DROP_INDEX -> 3;
            case ADD_CHECK, DROP_CHECK -> 4;
            case ADD_FOREIGN_KEY, DROP_FOREIGN_KEY -> 5;
            default -> 0;
        };
    }

    private static int operationOrder(SchemaOperation.Kind kind) {
        return switch (kind) {
            case CREATE_TABLE -> 0;
            case ADD_COLUMN -> 1;
            case CHANGE_COLUMN -> 2;
            case DROP_COLUMN -> 3;
            case DROP_TABLE -> 4;
            case ADD_PRIMARY_KEY, CHANGE_PRIMARY_KEY -> 10;
            case ADD_UNIQUE, CHANGE_UNIQUE -> 11;
            case ADD_INDEX, CHANGE_INDEX -> 12;
            case ADD_CHECK, CHANGE_CHECK -> 13;
            case ADD_FOREIGN_KEY, CHANGE_FOREIGN_KEY -> 14;
            case DROP_FOREIGN_KEY -> 20;
            case DROP_CHECK -> 21;
            case DROP_INDEX -> 22;
            case DROP_UNIQUE -> 23;
            case DROP_PRIMARY_KEY -> 24;
            case VERIFY_MANUALLY -> 30;
        };
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

    private record SelfReference(String name, List<String> columns, List<String> targetColumns) {

        private SelfReference {
            name = Objects.requireNonNull(name, "self-reference name must not be null");
            columns = List.copyOf(columns);
            targetColumns = List.copyOf(targetColumns);
        }

        private boolean dependsOn(String column) {
            return columns.contains(column) || targetColumns.contains(column);
        }
    }
}
