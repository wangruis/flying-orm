package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.UniqueNullPolicy;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlServerUniqueNullPolicyEvolutionTest {

    private static final RelationIdentity TABLE = RelationIdentity.of(null, "app", "accounts");

    @Test
    void dropsDistinctUniquenessAsItsPhysicalFilteredIndex() {
        SchemaMigrationPlan plan = review(table(key(UniqueNullPolicy.DISTINCT, "code")), table(null));
        assertEquals(List.of("drop index [uq_accounts] on [app].[accounts]"), sql(plan));
    }

    @Test
    void replacesADistinctUniqueIndexWithOneDropExistingStatement() {
        SchemaMigrationPlan plan = review(table(key(UniqueNullPolicy.DISTINCT, "code")),
                table(key(UniqueNullPolicy.DISTINCT, "code", "tenant")));
        assertEquals(List.of("create unique index [uq_accounts] on [app].[accounts] ([code], [tenant])"
                + " where [code] is not null and [tenant] is not null with (drop_existing = on)"), sql(plan));
    }

    @Test
    void changesNullPolicyByReplacingTheDeclaredConstraintOrIndex() {
        for (UniqueNullPolicy target : UniqueNullPolicy.values()) {
            UniqueNullPolicy source = target == UniqueNullPolicy.DEFAULT
                    ? UniqueNullPolicy.DISTINCT : UniqueNullPolicy.DEFAULT;
            RelationalTableDefinition actual = table(key(source, "code"));
            RelationalTableDefinition desired = table(key(target, "code"));
            SchemaMigrationPlan plan = review(actual, desired);
            assertEquals(2, plan.requests().size());
            assertEquals(sql(plan), sql(review(actual, desired)));
            assertEquals(plan.requests(), review(actual, desired).requests());
            assertTrue(plan.operations().stream().allMatch(operation -> operation.kind() == SchemaOperation.Kind.CHANGE_UNIQUE));

            List<String> sql = sql(plan);
            assertEquals(source == UniqueNullPolicy.DISTINCT
                    ? "drop index [uq_accounts] on [app].[accounts]"
                    : "alter table [app].[accounts] drop constraint [uq_accounts]", sql.getFirst());
            assertEquals(target == UniqueNullPolicy.DISTINCT
                    ? "create unique index [uq_accounts] on [app].[accounts] ([code]) where [code] is not null"
                    : "alter table [app].[accounts] add constraint [uq_accounts] unique ([code])", sql.getLast());
        }
    }

    private static UniqueConstraintDefinition key(UniqueNullPolicy policy, String... columns) {
        return new UniqueConstraintDefinition("uq_accounts", List.of(columns), policy);
    }

    private static RelationalTableDefinition table(UniqueConstraintDefinition unique) {
        RelationalTableDefinition.Builder table = RelationalTableDefinition.builder(TABLE)
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("code", "VARCHAR").length(32).build())
                .addColumn(ColumnDefinition.builder("tenant", "VARCHAR").length(32).build());
        return (unique == null ? table : table.addUnique(unique)).build();
    }

    private static SchemaMigrationPlan review(RelationalTableDefinition actual, RelationalTableDefinition desired) {
        RdbDialect dialect = RdbDialect.sqlServer();
        return RelationalSchemaPlanReviewer.create(dialect).review(DatabaseDescriptor.of("SQL Server", "2022", dialect),
                desired, SchemaSnapshot.present(actual), SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
    }

    private static List<String> sql(SchemaMigrationPlan plan) {
        return plan.requests().stream().map(request -> request.sql()).toList();
    }
}
