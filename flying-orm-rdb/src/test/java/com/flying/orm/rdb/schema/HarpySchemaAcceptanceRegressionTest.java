package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HarpySchemaAcceptanceRegressionTest {

    private static final RelationIdentity TABLE = RelationIdentity.table("options");
    private static final String COLLATION = "SQL_Latin1_General_CP1_CI_AS";

    @Test
    void h2CatalogAliasesParticipateInSafeWideningInBothDirections() {
        for (String actualType : List.of("VARCHAR", "CHARACTER VARYING")) {
            for (String desiredType : List.of("VARCHAR", "CHARACTER VARYING")) {
                ReviewedSchemaPlan plan = review(RdbDialect.h2(), column(actualType, 32), column(desiredType, 64));
                assertFalse(plan.requiresManualAction(), actualType + " -> " + desiredType);
                assertEquals(1, plan.requests().size());
                assertTrue(plan.requests().getFirst().sql().contains("(64)"));
            }
        }
    }

    @Test
    void h2AliasNormalizationDoesNotAuthorizeNarrowingOrMixedChanges() {
        assertTrue(review(RdbDialect.h2(), column("CHARACTER VARYING", 64),
                column("VARCHAR", 32)).requiresManualAction());
        assertTrue(review(RdbDialect.h2(), column("CHARACTER VARYING", 32),
                ColumnDefinition.builder("code", "VARCHAR").length(64).nullable(true).build())
                .requiresManualAction());
        assertTrue(review(RdbDialect.h2(), column("CHARACTER VARYING", 32),
                column("VARCHAR", 32)).steps().isEmpty());
    }

    @Test
    void sqlServerWideningAndNullabilityPreserveUnquotedCollationNames() {
        ColumnDefinition actual = ColumnDefinition.builder("code", "NVARCHAR").length(32)
                .nullable(false).collation(COLLATION).build();
        for (boolean widen : List.of(true, false)) {
            ColumnDefinition desired = ColumnDefinition.builder("code", "NVARCHAR")
                    .length(widen ? 64 : 32).nullable(!widen).build();
            ReviewedSchemaPlan plan = review(RdbDialect.sqlServer(), actual, desired);
            assertFalse(plan.requiresManualAction());
            assertEquals(List.of("alter table [options] alter column [code] NVARCHAR("
                    + (widen ? "64" : "32") + ") collate " + COLLATION + (widen ? " not null" : " null")),
                    plan.requests().stream().map(request -> request.sql()).toList());
        }
    }

    @Test
    void sqlServerCreateAndAlterUseTheSameSafeCollationToken() {
        var renderer = RelationalSchemaSqlRenderer.create(RdbDialect.sqlServer().schema());
        assertEquals("[code] NVARCHAR(32) collate " + COLLATION + " not null",
                renderer.columnDefinition(ColumnDefinition.builder("code", "NVARCHAR")
                        .length(32).nullable(false).collation(COLLATION).build()));
        for (String unsafe : List.of("dbo.invalid", "name;drop table options", "[quoted]", "name --")) {
            assertThrows(IllegalArgumentException.class, () -> renderer.columnDefinition(
                    ColumnDefinition.builder("code", "NVARCHAR").length(32).collation(unsafe).build()));
        }
    }

    private static ColumnDefinition column(String type, int length) {
        return ColumnDefinition.builder("code", type).length(length).nullable(false).build();
    }

    private static ReviewedSchemaPlan review(RdbDialect dialect, ColumnDefinition actual, ColumnDefinition desired) {
        return RelationalSchemaPlanReviewer.create(dialect).review(
                DatabaseDescriptor.of(dialect.name(), "test", dialect),
                RelationalTableDefinition.builder(TABLE).addColumn(desired).build(),
                SchemaSnapshot.present(RelationalTableDefinition.builder(TABLE).addColumn(actual).build()),
                SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
    }
}
