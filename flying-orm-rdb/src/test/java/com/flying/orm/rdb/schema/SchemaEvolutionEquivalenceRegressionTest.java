package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SchemaEvolutionEquivalenceRegressionTest {
    private static final RelationIdentity TABLE = RelationIdentity.of(null, null, "t");
    private static final RdbDialect DIALECT = RdbDialect.postgresql();

    @Test void equivalentNumericDefaultsDoNotBlockNullableChange() {
        var actual = numeric(false, new BigDecimal("1.00"));
        var desired = numeric(true, 1);
        var plan = review(actual, desired);
        assertFalse(plan.requiresManualAction(), plan.operations().toString());
        assertEquals(1, plan.requests().size());
        assertTrue(plan.requests().getFirst().sql().toLowerCase().contains("drop not null"));
    }

    @Test void unspecifiedSequenceCacheDoesNotBecomeAGenerationChange() {
        var plan = review(sequence(false, 20), sequence(true, 0));
        assertFalse(plan.requiresManualAction(), plan.operations().toString());
        assertEquals(1, plan.requests().size());
        assertTrue(plan.requests().getFirst().sql().toLowerCase().contains("drop not null"));
    }

    @Test void actualDefaultAndNullableChangesRemainManual() {
        var plan = review(numeric(false, 1), numeric(true, 2));
        assertTrue(plan.requiresManualAction());
        assertTrue(plan.requests().isEmpty());
    }

    @Test void explicitSequenceCacheChangeRemainsManual() {
        var plan = review(sequence(false, 20), sequence(true, 30));
        assertTrue(plan.requiresManualAction());
        assertTrue(plan.requests().isEmpty());
    }

    @Test void genuineDefaultOnlyChangeStillRenders() {
        var plan = review(numeric(true, 1), numeric(true, 2));
        assertFalse(plan.requiresManualAction());
        assertEquals(1, plan.requests().size());
        assertTrue(plan.requests().getFirst().sql().toLowerCase().contains("set default 2"));
    }

    private static ColumnDefinition numeric(boolean nullable, Number value) {
        return ColumnDefinition.builder("n", "DECIMAL").precision(10).scale(2)
                .nullable(nullable).defaultValue(ColumnDefault.literal(value)).build();
    }

    private static ColumnDefinition sequence(boolean nullable, int cache) {
        return ColumnDefinition.builder("n", "BIGINT").nullable(nullable)
                .generation(ValueGeneration.sequence("seq_t_n", 1, 1, cache)).build();
    }

    private static ReviewedSchemaPlan review(ColumnDefinition actual, ColumnDefinition desired) {
        var snapshot = SchemaSnapshot.builder(TABLE).tablePresent().tableCommentAbsent().primaryKeyAbsent()
                .uniqueConstraints(List.of()).indexes(List.of()).foreignKeys(List.of()).checks(List.of())
                .partitionAbsent().physicalColumns(List.of(actual), Map.of("n", actual.databaseType()))
                .build();
        return RelationalSchemaPlanReviewer.create(DIALECT).review(
                DatabaseDescriptor.of(DIALECT.name(), "18", DIALECT),
                RelationalTableDefinition.builder(TABLE).addColumn(desired).build(), snapshot,
                SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
    }
}
