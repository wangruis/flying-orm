package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalMetadataFingerprint;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 把一次关系 diff、精确 SQL 和执行前置条件冻结成可复核计划。
 *
 * <p>审阅只使用调用方已经读取的快照，期间不会再次访问数据库。不能由当前方言可靠渲染的 operation
 * 仍保留在计划中，但没有 SQL，执行器因此不会把缺失能力误报成成功。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class RelationalSchemaPlanReviewer {

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

    Map<String, String> observedSequences(List<SchemaSnapshot> snapshots) {
        return renderer.sequenceDefinitions(snapshots);
    }

    ReviewedSchemaPlan review(DatabaseDescriptor database,
                              RelationalTableDefinition desired,
                              SchemaSnapshot actual,
                              SchemaSnapshotCoverage coverage,
                              SchemaCompatibilityMode mode,
                              Map<String, String> sharedSequences) {
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
        List<SchemaPlanPrecondition> preconditions = List.of(
                SchemaPlanPrecondition.database(safeDatabase.fingerprint()),
                SchemaPlanPrecondition.capabilities(safeDatabase.capabilityFingerprint()),
                SchemaPlanPrecondition.actualSnapshot(actualFingerprint),
                SchemaPlanPrecondition.snapshotCoverage(safeCoverage.fingerprint()));
        ReviewedSchemaPlan.Builder plan = ReviewedSchemaPlan.builder(safeDatabase)
                .compatibilityMode(safeMode)
                .desiredTable(safeDesired)
                .comparisonDialect(dialect.schema())
                .desiredFingerprint(RelationalMetadataFingerprint.of(safeDesired))
                .actualFingerprint(actualFingerprint)
                .snapshotCoverage(safeCoverage);
        if (!safeCoverage.isComplete()) {
            SchemaOperation manual = SchemaOperation.of(
                    SchemaOperation.Kind.VERIFY_MANUALLY,
                    safeDesired.identity(),
                    "snapshot-coverage",
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
        int order = 0;
        Map<String, String> sequences = sharedSequences == null
                ? renderer.sequenceDefinitions(List.of(safeActual))
                : sharedSequences;
        for (SchemaOperation operation : SchemaDiffer.diff(
                safeDesired, safeActual, safeDatabase.capabilities(), safeMode,
                safeDatabase.dialectId(), dialect.schema()).operations()) {
            SchemaMigrationRiskLevel risk = SchemaRiskClassifier.classify(
                    operation, safeActual, safeDatabase.capabilities());
            try {
                List<SqlRequest> requests = renderer.render(operation, sequences);
                for (SqlRequest request : requests) {
                    plan.addStep(SchemaPlanStep.executable(
                            order++, operation, request, risk, preconditions));
                }
            } catch (UnsupportedOperationException unsupported) {
                plan.addStep(SchemaPlanStep.manual(order++, operation, risk, preconditions));
            }
        }
        return plan.build();
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
                names.object(column.generation().sequenceName());
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
