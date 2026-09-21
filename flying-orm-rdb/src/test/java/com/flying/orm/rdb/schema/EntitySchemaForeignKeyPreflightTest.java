package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalMetadataFingerprint;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntitySchemaForeignKeyPreflightTest {

    private static final RelationIdentity PARENT = RelationIdentity.table("parents");
    private static final RelationIdentity CHILD = RelationIdentity.table("children");
    private static final ColumnDefinition ID = column("id", "BIGINT");
    private static final ColumnDefinition CODE = column("code", "VARCHAR");
    private static final ColumnDefinition NOTE = column("note", "VARCHAR");
    private static final PrimaryKeyDefinition PRIMARY_ID =
            PrimaryKeyDefinition.of("pk_parents", "id");
    private static final PrimaryKeyDefinition PRIMARY_CODE =
            PrimaryKeyDefinition.of("pk_parents", "code");
    private static final UniqueConstraintDefinition UNIQUE_ID =
            UniqueConstraintDefinition.of("uq_parents_id", "id");
    private static final UniqueConstraintDefinition UNIQUE_CODE =
            UniqueConstraintDefinition.of("uq_parents_id", "code");
    private static final IndexDefinition UNIQUE_INDEX_ID = index("ux_parents_id", "id", true);
    private static final IndexDefinition UNIQUE_INDEX_CODE = index("ux_parents_id", "code", true);
    private static final RelationalTableDefinition DESIRED_PARENT =
            RelationalTableDefinition.builder(PARENT)
                    .addColumn(ID)
                    .addColumn(CODE)
                    .addColumn(NOTE)
                    .primaryKey(PRIMARY_CODE)
                    .build();
    private static final RelationalTableDefinition ACTUAL_PARENT =
            RelationalTableDefinition.builder(PARENT)
                    .addColumn(ID)
                    .addColumn(CODE)
                    .primaryKey(PRIMARY_ID)
                    .addUnique(UNIQUE_ID)
                    .build();
    private static final RelationalTableDefinition ACTUAL_CHILD = childReferencing(PARENT);

    @Test
    void logicalVarcharCommentChangeDoesNotInvalidateItsReferencedCandidateKey() {
        ColumnDefinition actualId = column("id", "VARCHAR");
        ColumnDefinition desiredId = ColumnDefinition.builder("id", "VARCHAR")
                .nullable(false).comment("identifier").build();
        RelationalTableDefinition actualParent = RelationalTableDefinition.builder(PARENT)
                .addColumn(actualId).primaryKey(PRIMARY_ID).build();

        assertDoesNotThrow(() -> rejectReferencedCandidateKeyChanges(
                EntitySchemaSyncMode.FULL_UPDATE,
                List.of(SchemaSnapshot.present(actualParent), SchemaSnapshot.present(ACTUAL_CHILD)),
                List.of(plan(change(SchemaOperation.Kind.CHANGE_COLUMN, actualId, desiredId)))));
    }

    @Test
    void rejectsCrossTableForeignKeysBeforeChangingTheirCandidateKeyOrColumn() {
        for (SchemaOperation operation : blockingOperations()) {
            assertThrows(EntityRelationalSchemaSyncException.class, () ->
                    rejectReferencedCandidateKeyChanges(
                            EntitySchemaSyncMode.FULL_UPDATE,
                            List.of(SchemaSnapshot.present(ACTUAL_PARENT),
                                    SchemaSnapshot.present(ACTUAL_CHILD)),
                            List.of(plan(operation))));
        }
    }

    @Test
    void matchesCrossTableReferencesToPermutedCompleteCandidateKeyColumnsOnly() {
        SchemaSnapshot child = SchemaSnapshot.present(childReferencingPermutedCompositeKey());
        PrimaryKeyDefinition primary = PrimaryKeyDefinition.of(
                "pk_parents_composite", "id", "code");
        UniqueConstraintDefinition unique = UniqueConstraintDefinition.of(
                "uq_parents_composite", "id", "code");
        IndexDefinition uniqueIndex = index(
                "ux_parents_composite", true, "id", "code");
        List<SchemaOperation> blocking = List.of(
                change(SchemaOperation.Kind.CHANGE_PRIMARY_KEY, primary,
                        PrimaryKeyDefinition.of("pk_parents_composite", "id", "note")),
                drop(SchemaOperation.Kind.DROP_PRIMARY_KEY, primary),
                change(SchemaOperation.Kind.CHANGE_UNIQUE, unique,
                        UniqueConstraintDefinition.of("uq_parents_composite", "id", "note")),
                drop(SchemaOperation.Kind.DROP_UNIQUE, unique),
                change(SchemaOperation.Kind.CHANGE_INDEX, uniqueIndex,
                        index("ux_parents_composite", true, "id", "note")),
                drop(SchemaOperation.Kind.DROP_INDEX, uniqueIndex));

        for (SchemaOperation operation : blocking) {
            assertThrows(EntityRelationalSchemaSyncException.class, () ->
                    rejectReferencedCandidateKeyChanges(
                            EntitySchemaSyncMode.FULL_UPDATE,
                            List.of(SchemaSnapshot.present(ACTUAL_PARENT), child),
                            List.of(plan(operation))));
        }

        assertDoesNotThrow(() -> rejectReferencedCandidateKeyChanges(
                EntitySchemaSyncMode.FULL_UPDATE,
                List.of(SchemaSnapshot.present(ACTUAL_PARENT), child),
                List.of(plan(drop(SchemaOperation.Kind.DROP_PRIMARY_KEY,
                        PrimaryKeyDefinition.of("pk_parents_subset", "id"))))));
        assertDoesNotThrow(() -> rejectReferencedCandidateKeyChanges(
                EntitySchemaSyncMode.FULL_UPDATE,
                List.of(SchemaSnapshot.present(ACTUAL_PARENT), child),
                List.of(plan(drop(SchemaOperation.Kind.DROP_PRIMARY_KEY,
                        PrimaryKeyDefinition.of(
                                "pk_parents_superset", "id", "code", "note"))))));
    }

    @Test
    void doesNotInventDependenciesForAnotherIdentityOrUnreferencedColumn() {
        SchemaSnapshot caseDifferentChild = SchemaSnapshot.present(
                childReferencing(RelationIdentity.table("Parents")));
        SchemaOperation codeChange = SchemaOperation.of(
                SchemaOperation.Kind.CHANGE_COLUMN,
                PARENT,
                "code",
                CODE,
                column("code", "TEXT"),
                SchemaOperation.Compatibility.REQUIRES_REVIEW);
        ColumnDefinition idMetadata = ColumnDefinition.builder("id", "BIGINT")
                .nullable(false)
                .defaultValue(ColumnDefault.literal(0L))
                .comment("identifier")
                .build();
        SchemaOperation idMetadataChange = change(
                SchemaOperation.Kind.CHANGE_COLUMN, ID, idMetadata);
        SchemaOperation ordinaryIndexDrop = drop(
                SchemaOperation.Kind.DROP_INDEX, index("ix_parents_id", "id", false));

        assertDoesNotThrow(() -> rejectReferencedCandidateKeyChanges(
                EntitySchemaSyncMode.FULL_UPDATE,
                List.of(SchemaSnapshot.present(ACTUAL_PARENT), caseDifferentChild),
                List.of(plan(codeChange))));
        assertDoesNotThrow(() -> rejectReferencedCandidateKeyChanges(
                EntitySchemaSyncMode.FULL_UPDATE,
                List.of(SchemaSnapshot.present(ACTUAL_PARENT),
                        SchemaSnapshot.present(ACTUAL_CHILD)),
                List.of(plan(idMetadataChange))));
        assertDoesNotThrow(() -> rejectReferencedCandidateKeyChanges(
                EntitySchemaSyncMode.FULL_UPDATE,
                List.of(SchemaSnapshot.present(ACTUAL_PARENT),
                        SchemaSnapshot.present(ACTUAL_CHILD)),
                List.of(plan(ordinaryIndexDrop))));
        assertDoesNotThrow(() -> rejectReferencedCandidateKeyChanges(
                EntitySchemaSyncMode.VALIDATE,
                List.of(SchemaSnapshot.present(ACTUAL_PARENT),
                        SchemaSnapshot.present(ACTUAL_CHILD)),
                List.of(plan(blockingOperations().getFirst()))));
    }

    private static void rejectReferencedCandidateKeyChanges(
            EntitySchemaSyncMode mode,
            List<SchemaSnapshot> snapshots,
            List<ReviewedSchemaPlan> plans) {
        EntitySchemaSyncSupport.rejectReferencedCandidateKeyChanges(
                mode, snapshots, plans, RdbDialect.postgresql().schema());
    }

    private static List<SchemaOperation> blockingOperations() {
        return List.of(
                change(SchemaOperation.Kind.CHANGE_PRIMARY_KEY, PRIMARY_ID, PRIMARY_CODE),
                drop(SchemaOperation.Kind.DROP_PRIMARY_KEY, PRIMARY_ID),
                change(SchemaOperation.Kind.CHANGE_UNIQUE, UNIQUE_ID, UNIQUE_CODE),
                drop(SchemaOperation.Kind.DROP_UNIQUE, UNIQUE_ID),
                change(SchemaOperation.Kind.CHANGE_INDEX, UNIQUE_INDEX_ID, UNIQUE_INDEX_CODE),
                drop(SchemaOperation.Kind.DROP_INDEX, UNIQUE_INDEX_ID),
                change(SchemaOperation.Kind.CHANGE_COLUMN, ID, column("id", "VARCHAR")),
                drop(SchemaOperation.Kind.DROP_COLUMN, ID));
    }

    private static ReviewedSchemaPlan plan(SchemaOperation operation) {
        return ReviewedSchemaPlan.builder(DatabaseDescriptor.of(
                        "PostgreSQL", "17", RdbDialect.postgresql()))
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredTable(DESIRED_PARENT)
                .desiredFingerprint(RelationalMetadataFingerprint.of(DESIRED_PARENT))
                .actualFingerprint("actual")
                .addStep(SchemaPlanStep.executable(
                        0,
                        operation,
                        new SqlRequest("reviewed ddl", List.of()),
                        SchemaMigrationRiskLevel.CRITICAL,
                        List.of()))
                .build();
    }

    private static SchemaOperation change(SchemaOperation.Kind kind,
                                          Object actual,
                                          Object desired) {
        return SchemaOperation.of(
                kind,
                PARENT,
                objectName(actual),
                actual,
                desired,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);
    }

    private static SchemaOperation drop(SchemaOperation.Kind kind, Object actual) {
        return SchemaOperation.of(
                kind,
                PARENT,
                objectName(actual),
                actual,
                null,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);
    }

    private static String objectName(Object definition) {
        if (definition instanceof PrimaryKeyDefinition primaryKey) {
            return primaryKey.name();
        }
        if (definition instanceof UniqueConstraintDefinition unique) {
            return unique.name();
        }
        if (definition instanceof IndexDefinition index) {
            return index.name();
        }
        return ((ColumnDefinition) definition).name();
    }

    private static RelationalTableDefinition childReferencing(RelationIdentity parent) {
        ForeignKeyDefinition foreignKey = ForeignKeyDefinition.builder("fk_children_parent")
                .addColumn("parent_id")
                .reference(parent)
                .addReferenceColumn("id")
                .build();
        return RelationalTableDefinition.builder(CHILD)
                .addColumn(column("child_id", "BIGINT"))
                .addColumn(column("parent_id", "BIGINT"))
                .primaryKey(PrimaryKeyDefinition.of("pk_children", "child_id"))
                .addForeignKey(foreignKey)
                .build();
    }

    private static RelationalTableDefinition childReferencingPermutedCompositeKey() {
        ForeignKeyDefinition foreignKey = ForeignKeyDefinition.builder("fk_children_parent_composite")
                .addColumn("parent_code")
                .addColumn("parent_id")
                .reference(PARENT)
                .addReferenceColumn("code")
                .addReferenceColumn("id")
                .build();
        return RelationalTableDefinition.builder(CHILD)
                .addColumn(column("child_id", "BIGINT"))
                .addColumn(column("parent_id", "BIGINT"))
                .addColumn(column("parent_code", "VARCHAR"))
                .primaryKey(PrimaryKeyDefinition.of("pk_children", "child_id"))
                .addForeignKey(foreignKey)
                .build();
    }

    private static ColumnDefinition column(String name, String type) {
        return ColumnDefinition.builder(name, type).nullable(false).build();
    }

    private static IndexDefinition index(String name, String column, boolean unique) {
        return index(name, unique, column);
    }

    private static IndexDefinition index(String name, boolean unique, String... columns) {
        IndexDefinition.Builder builder = IndexDefinition.builder(name).unique(unique);
        for (String column : columns) {
            builder.addKey(IndexKeyPart.asc(column));
        }
        return builder.build();
    }
}
