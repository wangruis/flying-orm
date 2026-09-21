package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalMetadataFingerprint;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.UniqueNullPolicy;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class BuiltInDistinctUniquenessTest {

    private static final RelationIdentity TABLE = RelationIdentity.table("accounts");
    private static final UniqueConstraintDefinition DISTINCT = new UniqueConstraintDefinition(
            "uq_accounts", List.of("a", "b"), UniqueNullPolicy.DISTINCT);

    @Test
    void allBuiltInDialectsCompileCreateAddAndDropForTheSameLogicalPolicy() {
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(), RdbDialect.h2(),
                RdbDialect.sqlServer(), RdbDialect.oracle())) {
            var renderer = RelationalSchemaSqlRenderer.create(dialect.schema());
            assertDoesNotThrow(() -> renderer.render(operation(SchemaOperation.Kind.CREATE_TABLE,
                    "accounts", null, table(DISTINCT))), dialect.name());
            String add = assertDoesNotThrow(() -> renderer.render(operation(SchemaOperation.Kind.ADD_UNIQUE,
                    DISTINCT.name(), null, DISTINCT))).getFirst().sql();
            String drop = assertDoesNotThrow(() -> renderer.render(operation(SchemaOperation.Kind.DROP_UNIQUE,
                    DISTINCT.name(), DISTINCT, null))).getFirst().sql();
            switch (dialect.name()) {
                case "oracle" -> {
                    assertEquals("create unique index \"uq_accounts\" on \"accounts\" ("
                            + "case when \"a\" is not null and \"b\" is not null then \"a\" end, "
                            + "case when \"a\" is not null and \"b\" is not null then \"b\" end)", add);
                    assertEquals("drop index \"uq_accounts\"", drop);
                }
                case "h2" -> assertTrue(add.contains("unique nulls distinct (a, b)"), add);
                case "mysql", "postgresql" -> assertTrue(add.startsWith("alter table "), add);
                case "sqlserver" -> assertTrue(add.contains("where [a] is not null and [b] is not null"), add);
                default -> fail(dialect.name());
            }
        }
    }

    @Test
    void nativeEquivalentUniquesCompareWithoutFalsifyingObservedFactsOrFingerprints() {
        var desired = table(DISTINCT);
        var actual = table(new UniqueConstraintDefinition(DISTINCT.name(), DISTINCT.columns()));
        assertNotEquals(RelationalMetadataFingerprint.of(actual), RelationalMetadataFingerprint.of(desired));
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(), RdbDialect.h2())) {
            for (SchemaCompatibilityMode mode : SchemaCompatibilityMode.values()) {
                var plan = RelationalSchemaPlanReviewer.create(dialect).review(
                        DatabaseDescriptor.of(dialect.name(), "1", dialect), desired,
                        SchemaSnapshot.present(actual), SchemaSnapshotCoverage.complete(), mode);
                assertTrue(plan.steps().isEmpty(), dialect.name() + ": " + plan.operations());
            }
        }
        assertFalse(SchemaDefinitionEquality.sameUnique(actual.uniqueConstraints().getFirst(), DISTINCT, null));
        assertFalse(SchemaDefinitionEquality.sameUnique(actual.uniqueConstraints().getFirst(), DISTINCT,
                RdbDialect.sqlServer().schema()));
        assertFalse(SchemaDefinitionEquality.sameUnique(actual.uniqueConstraints().getFirst(), DISTINCT,
                RdbDialect.oracle().schema()));
    }

    @Test
    void h2ExplicitDistinctSurvivesCompatibilityModeDefaults() throws Exception {
        for (String mode : List.of("Regular", "Oracle", "MSSQLServer")) {
            var sql = RelationalSchemaSqlRenderer.create(RdbDialect.h2().schema())
                    .render(operation(SchemaOperation.Kind.CREATE_TABLE, "accounts", null, table(DISTINCT)));
            try (var connection = DriverManager.getConnection("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=" + mode);
                    var statement = connection.createStatement()) {
                for (var request : sql) {
                    statement.execute(request.sql());
                }
                statement.execute("insert into accounts values (null, null), (null, null),"
                        + " (1, null), (1, null), (null, 1), (null, 1), (1, 1)");
                SQLException duplicate = assertThrows(SQLException.class,
                        () -> statement.execute("insert into accounts values (1, 1)"));
                assertEquals("23505", duplicate.getSQLState());
            }
        }
    }

    private static SchemaOperation operation(SchemaOperation.Kind kind, String name, Object actual, Object desired) {
        return SchemaOperation.of(kind, TABLE, name, actual, desired,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);
    }

    private static RelationalTableDefinition table(UniqueConstraintDefinition key) {
        return RelationalTableDefinition.builder(TABLE)
                .addColumn(ColumnDefinition.builder("a", "INTEGER").build())
                .addColumn(ColumnDefinition.builder("b", "INTEGER").build()).addUnique(key).build();
    }
}
