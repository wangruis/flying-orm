package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.ReferentialAction;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalMetadataFingerprint;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReviewedSchemaPlanDialectExecutionGateTest {

    private static final RelationIdentity ACCOUNTS = RelationIdentity.table("accounts");
    private static final RelationIdentity GROUPS = RelationIdentity.table("groups");
    private static final ColumnDefinition ID =
            ColumnDefinition.builder("id", "BIGINT").nullable(false).build();
    private static final ColumnDefinition CODE =
            ColumnDefinition.builder("code", "VARCHAR").length(32).build();
    private static final RelationalTableDefinition TABLE = RelationalTableDefinition.builder(ACCOUNTS)
            .addColumn(ID)
            .addColumn(CODE)
            .build();

    @Test
    void builtInPlansAdmitSafeEvolutionKindsButKeepConstraintReplacementClosed() {
        for (RdbDialect dialect : List.of(
                RdbDialect.h2(),
                RdbDialect.mysql(),
                RdbDialect.oracle(),
                RdbDialect.sqlServer())) {
            DatabaseDescriptor database = DatabaseDescriptor.of(dialect.name(), "test", dialect);
            for (SchemaOperation operation : evolutionOperations()) {
                boolean unsupported = switch (operation.kind()) {
                    case VERIFY_MANUALLY -> true;
                    case CHANGE_FOREIGN_KEY -> false;
                    case CHANGE_CHECK -> !dialect.name().equals("mysql");
                    default -> false;
                };
                if (unsupported) {
                    assertThrows(IllegalArgumentException.class, () -> executablePresentPlan(database, operation),
                            () -> dialect.name() + " accepted " + operation.kind());
                } else {
                    assertDoesNotThrow(() -> executablePresentPlan(database, operation),
                            () -> dialect.name() + " rejected " + operation.kind());
                }
            }

            assertDoesNotThrow(() -> executablePresentPlan(database, dropIndex()));
        }
    }

    @Test
    void unknownDialectsDoNotInheritBuiltInEvolutionSupport() {
        DatabaseDescriptor database = DatabaseDescriptor.of("custom", "test", "custom", DialectCapabilities.empty());
        for (SchemaOperation operation : evolutionOperations()) {
            assertThrows(IllegalArgumentException.class, () -> executablePresentPlan(database, operation));
        }
    }

    @Test
    void manualVerificationCanNeverCarryExecutableSql() {
        DatabaseDescriptor postgres = DatabaseDescriptor.of(
                "PostgreSQL", "17", RdbDialect.postgresql());
        SchemaOperation manual = evolutionOperations().getLast();

        assertThrows(IllegalArgumentException.class, () -> executablePresentPlan(postgres, manual));
    }

    private static List<SchemaOperation> evolutionOperations() {
        PrimaryKeyDefinition primaryId = PrimaryKeyDefinition.of("pk_accounts", "id");
        PrimaryKeyDefinition primaryCode = PrimaryKeyDefinition.of("pk_accounts", "code");
        UniqueConstraintDefinition uniqueId = UniqueConstraintDefinition.of("uq_accounts", "id");
        UniqueConstraintDefinition uniqueCode = UniqueConstraintDefinition.of("uq_accounts", "code");
        IndexDefinition indexId = index("ix_accounts", "id");
        IndexDefinition indexCode = index("ix_accounts", "code");
        CheckConstraintDefinition positive = CheckConstraintDefinition.of(
                "ck_accounts", CheckPredicate.compare(
                        "id", CheckPredicate.ComparisonOperator.GREATER_THAN, 0));
        CheckConstraintDefinition nonNegative = CheckConstraintDefinition.of(
                "ck_accounts", CheckPredicate.compare(
                        "id", CheckPredicate.ComparisonOperator.GREATER_THAN_OR_EQUAL, 0));
        ForeignKeyDefinition restrict = foreignKey(ReferentialAction.RESTRICT);
        ForeignKeyDefinition cascade = foreignKey(ReferentialAction.CASCADE);

        List<SchemaOperation> operations = new ArrayList<>();
        operations.add(change(SchemaOperation.Kind.CHANGE_COLUMN, ID.name(), ID, CODE));
        operations.add(change(
                SchemaOperation.Kind.CHANGE_PRIMARY_KEY,
                primaryId.name(), primaryId, primaryCode));
        operations.add(change(
                SchemaOperation.Kind.CHANGE_UNIQUE,
                uniqueId.name(), uniqueId, uniqueCode));
        operations.add(change(
                SchemaOperation.Kind.CHANGE_INDEX,
                indexId.name(), indexId, indexCode));
        operations.add(change(
                SchemaOperation.Kind.CHANGE_CHECK,
                positive.name(), positive, nonNegative));
        operations.add(change(
                SchemaOperation.Kind.CHANGE_FOREIGN_KEY,
                restrict.name(), restrict, cascade));
        operations.add(drop(SchemaOperation.Kind.DROP_FOREIGN_KEY, restrict.name(), restrict));
        operations.add(drop(SchemaOperation.Kind.DROP_CHECK, positive.name(), positive));
        operations.add(drop(SchemaOperation.Kind.DROP_UNIQUE, uniqueId.name(), uniqueId));
        operations.add(drop(SchemaOperation.Kind.DROP_PRIMARY_KEY, primaryId.name(), primaryId));
        operations.add(drop(SchemaOperation.Kind.DROP_COLUMN, CODE.name(), CODE));
        operations.add(SchemaOperation.of(
                SchemaOperation.Kind.VERIFY_MANUALLY,
                ACCOUNTS,
                "manual",
                null,
                null,
                SchemaOperation.Compatibility.REQUIRES_REVIEW));
        return List.copyOf(operations);
    }

    private static ReviewedSchemaPlan executablePresentPlan(DatabaseDescriptor database,
                                                            SchemaOperation operation) {
        return ReviewedSchemaPlan.builder(database)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredTable(TABLE)
                .desiredFingerprint(RelationalMetadataFingerprint.of(TABLE))
                .actualFingerprint("actual")
                .addStep(SchemaPlanStep.executable(
                        0,
                        operation,
                        new SqlRequest("reviewed ddl", List.of()),
                        SchemaMigrationRiskLevel.CRITICAL,
                        List.of()))
                .build();
    }

    private static SchemaOperation dropIndex() {
        IndexDefinition index = index("ix_accounts", "code");
        return drop(SchemaOperation.Kind.DROP_INDEX, index.name(), index);
    }

    private static IndexDefinition index(String name, String column) {
        return IndexDefinition.builder(name).addKey(IndexKeyPart.asc(column)).build();
    }

    private static ForeignKeyDefinition foreignKey(ReferentialAction action) {
        return ForeignKeyDefinition.builder("fk_accounts_group")
                .addColumn("id")
                .reference(GROUPS)
                .addReferenceColumn("id")
                .onDelete(action)
                .build();
    }

    private static SchemaOperation change(SchemaOperation.Kind kind,
                                          String name,
                                          Object actual,
                                          Object desired) {
        return SchemaOperation.of(
                kind,
                ACCOUNTS,
                name,
                actual,
                desired,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);
    }

    private static SchemaOperation drop(SchemaOperation.Kind kind,
                                        String name,
                                        Object actual) {
        return SchemaOperation.of(
                kind,
                ACCOUNTS,
                name,
                actual,
                null,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);
    }
}
