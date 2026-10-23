package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationalSchemaReviewedPlanTest {

    @Test
    void freezesDiffAndSqlInOnePlan() {
        RdbDialect dialect = RdbDialect.h2();
        DatabaseDescriptor database = DatabaseDescriptor.of("H2", "2.3", dialect);
        RelationalTableDefinition desired = RelationalTableDefinition
                .builder(RelationIdentity.table("accounts"))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "id"))
                .build();
        SchemaSnapshot actual = SchemaSnapshot.absent(desired.identity());
        RelationalSchemaPlanReviewer reviewer = RelationalSchemaPlanReviewer.create(dialect);

        SchemaMigrationPlan first = reviewer.review(
                database, desired, actual, SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.SAFE_INCREMENTAL);
        SchemaMigrationPlan second = reviewer.review(
                database, desired, actual, SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.SAFE_INCREMENTAL);

        assertSame(desired, first.desiredTable().orElseThrow());
        assertEquals(first.requests(), second.requests());
        assertFalse(first.requests().isEmpty());
    }

    @Test
    void rendersUnqualifiedSqlServerCommentsAsReviewedExecutableSql() {
        RdbDialect dialect = RdbDialect.sqlServer();
        DatabaseDescriptor database = DatabaseDescriptor.of("SQL Server", "16", dialect);
        RelationalTableDefinition desired = RelationalTableDefinition
                .builder(RelationIdentity.table("accounts"))
                .comment("账户表")
                .addColumn(ColumnDefinition.builder("id", "BIGINT")
                        .nullable(false)
                        .comment("主键")
                        .build())
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "id"))
                .build();

        SchemaMigrationPlan plan = RelationalSchemaPlanReviewer.create(dialect).review(
                database,
                desired,
                SchemaSnapshot.absent(desired.identity()),
                SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.SAFE_INCREMENTAL);

        assertTrue(plan.requests().stream()
                .anyMatch(request -> request.sql().startsWith("exec sp_executesql ")));
    }

    @Test
    void rejectsInvalidMysqlIdentityLayoutDuringPlanning() {
        RdbDialect dialect = RdbDialect.mysql();
        DatabaseDescriptor database = DatabaseDescriptor.of("MySQL", "8.4", dialect);
        RelationalTableDefinition desired = RelationalTableDefinition
                .builder(RelationIdentity.table("accounts"))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("sequence_no", "BIGINT")
                        .nullable(false)
                        .generation(ValueGeneration.identity())
                        .build())
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "id"))
                .build();

        assertThrows(UnsupportedOperationException.class, () -> RelationalSchemaPlanReviewer.create(dialect).review(
                database,
                desired,
                SchemaSnapshot.absent(desired.identity()),
                SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.SAFE_INCREMENTAL));
    }

    @Test
    void treatsMysqlPrimaryAsThePhysicalNameOfAnAnnotatedPrimaryKey() {
        RdbDialect dialect = RdbDialect.mysql();
        DatabaseDescriptor database = DatabaseDescriptor.of("MySQL", "8.4", dialect);
        RelationIdentity identity = RelationIdentity.table("accounts");
        ColumnDefinition id = ColumnDefinition.builder("id", "BIGINT").nullable(false).build();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(identity)
                .addColumn(id)
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "id"))
                .build();
        RelationalTableDefinition observed = RelationalTableDefinition.builder(identity)
                .addColumn(id)
                .primaryKey(PrimaryKeyDefinition.of("PRIMARY", "id"))
                .build();

        SchemaMigrationPlan plan = RelationalSchemaPlanReviewer.create(dialect).review(
                database, desired, SchemaSnapshot.present(observed),
                SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);

        assertTrue(plan.steps().isEmpty());
    }

    @Test
    void rejectsOverlongExplicitObjectNamesBeforeBuildingAnExecutablePlan() {
        RdbDialect dialect = RdbDialect.oracle(com.flying.orm.rdb.dialect.OracleVersion.V12C);
        DatabaseDescriptor database = DatabaseDescriptor.of("Oracle", "12c", dialect);
        RelationalTableDefinition desired = RelationalTableDefinition
                .builder(RelationIdentity.table("accounts"))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts_name_that_exceeds_oracle_12c_limit", "id"))
                .build();

        assertThrows(IllegalArgumentException.class, () ->
                RelationalSchemaPlanReviewer.create(dialect).review(
                        database,
                        desired,
                        SchemaSnapshot.absent(desired.identity()),
                        SchemaSnapshotCoverage.complete(),
                        SchemaCompatibilityMode.SAFE_INCREMENTAL));
    }

    @Test
    void acceptsQualifiedSequenceWhenEveryIdentifierPartFitsTheDialectLimit() {
        RdbDialect dialect = RdbDialect.postgresql();
        DatabaseDescriptor database = DatabaseDescriptor.of("PostgreSQL", "16", dialect);
        String schema = "schema_" + "s".repeat(50);
        String sequence = "sequence_" + "q".repeat(50);
        RelationalTableDefinition desired = RelationalTableDefinition
                .builder(RelationIdentity.of(null, schema, "events"))
                .addColumn(ColumnDefinition.builder("id", "BIGINT")
                        .nullable(false)
                        .generation(ValueGeneration.sequence(schema + "." + sequence))
                        .build())
                .build();

        SchemaMigrationPlan plan = RelationalSchemaPlanReviewer.create(dialect).review(
                database,
                desired,
                SchemaSnapshot.absent(desired.identity()),
                SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.SAFE_INCREMENTAL);

        assertFalse(plan.requests().isEmpty());
    }

}
