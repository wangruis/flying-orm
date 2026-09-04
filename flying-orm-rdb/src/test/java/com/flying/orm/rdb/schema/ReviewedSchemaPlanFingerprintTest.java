package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ReviewedSchemaPlanFingerprintTest {

    @Test
    void bindsDatabaseSnapshotsOrderRiskPreconditionsAndExactSql() {
        DatabaseDescriptor database = DatabaseDescriptor.of(
                "PostgreSQL", "17.2", "postgresql", DialectCapabilities.empty());
        SchemaOperation firstOperation = manual("users", "confirm-users");
        SchemaOperation secondOperation = manual("orders", "confirm-orders");

        ReviewedSchemaPlan first = ReviewedSchemaPlan.builder(database)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredFingerprint("desired-v1")
                .actualFingerprint("actual-v1")
                .addStep(SchemaPlanStep.executable(
                        0, firstOperation, new SqlRequest("create table users(id bigint)", List.of()),
                        SchemaMigrationRiskLevel.LOW,
                        List.of(SchemaPlanPrecondition.actualSnapshot("actual-v1"))))
                .addStep(SchemaPlanStep.manual(
                        1, secondOperation, SchemaMigrationRiskLevel.HIGH,
                        List.of(SchemaPlanPrecondition.capabilities(database.capabilityFingerprint()))))
                .build();
        ReviewedSchemaPlan same = ReviewedSchemaPlan.builder(database)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredFingerprint("desired-v1")
                .actualFingerprint("actual-v1")
                .addStep(SchemaPlanStep.executable(
                        0, firstOperation, new SqlRequest("create table users(id bigint)", List.of()),
                        SchemaMigrationRiskLevel.LOW,
                        List.of(SchemaPlanPrecondition.actualSnapshot("actual-v1"))))
                .addStep(SchemaPlanStep.manual(
                        1, secondOperation, SchemaMigrationRiskLevel.HIGH,
                        List.of(SchemaPlanPrecondition.capabilities(database.capabilityFingerprint()))))
                .build();

        assertEquals(first.fingerprint(), same.fingerprint());
        assertEquals("create table users(id bigint)",
                     first.steps().getFirst().request().orElseThrow().sql());

        ReviewedSchemaPlan changedSql = ReviewedSchemaPlan.builder(database)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredFingerprint("desired-v1")
                .actualFingerprint("actual-v1")
                .addStep(SchemaPlanStep.executable(
                        0, firstOperation, new SqlRequest("create table users(id integer)", List.of()),
                        SchemaMigrationRiskLevel.LOW,
                        List.of(SchemaPlanPrecondition.actualSnapshot("actual-v1"))))
                .addStep(SchemaPlanStep.manual(
                        1, secondOperation, SchemaMigrationRiskLevel.HIGH,
                        List.of(SchemaPlanPrecondition.capabilities(database.capabilityFingerprint()))))
                .build();

        assertNotEquals(first.fingerprint(), changedSql.fingerprint());
    }

    @Test
    void rejectsDuplicateOrOutOfOrderStepNumbers() {
        DatabaseDescriptor database = DatabaseDescriptor.of(
                "H2", "2.3", "h2", DialectCapabilities.empty());
        SchemaPlanStep step = SchemaPlanStep.manual(
                1, manual("sample", "manual"), SchemaMigrationRiskLevel.MEDIUM, List.of());

        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> ReviewedSchemaPlan.builder(database)
                        .compatibilityMode(SchemaCompatibilityMode.EXACT)
                        .desiredFingerprint("desired")
                        .actualFingerprint("actual")
                        .addStep(step)
                        .build());
    }

    private static SchemaOperation manual(String table, String name) {
        return SchemaOperation.of(
                SchemaOperation.Kind.VERIFY_MANUALLY,
                RelationIdentity.table(table),
                name,
                null,
                null,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);
    }
}
