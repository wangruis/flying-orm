package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RelationalSchemaSharedSequenceTest {

    private static final RelationIdentity TABLE = RelationIdentity.table("events");
    private static final RdbDialect DIALECT = RdbDialect.postgresql();

    @Test
    void createsOneSequenceForMultipleColumnsInANewTable() {
        RelationalTableDefinition desired = RelationalTableDefinition.builder(TABLE)
                .addColumn(generated("first_value")).addColumn(generated("second_value")).build();

        assertEquals(1, sequenceCreates(review(desired, SchemaSnapshot.absent(TABLE))));
    }

    @Test
    void createsOneSequenceForMultipleAddedColumns() {
        ColumnDefinition id = ColumnDefinition.builder("id", "BIGINT").build();
        RelationalTableDefinition existing = RelationalTableDefinition.builder(TABLE).addColumn(id).build();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(TABLE).addColumn(id)
                .addColumn(generated("first_value")).addColumn(generated("second_value")).build();

        assertEquals(1, sequenceCreates(review(desired, SchemaSnapshot.present(existing))));
    }

    @Test
    void reusesSequenceObservedOnAnExistingColumn() {
        ColumnDefinition first = generated("first_value");
        RelationalTableDefinition existing = RelationalTableDefinition.builder(TABLE).addColumn(first).build();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(TABLE).addColumn(first)
                .addColumn(generated("second_value")).build();

        assertEquals(0, sequenceCreates(review(desired, SchemaSnapshot.present(existing))));
    }

    private static ColumnDefinition generated(String name) {
        return ColumnDefinition.builder(name, "BIGINT").generation(ValueGeneration.sequence("shared_seq")).build();
    }

    @Test
    void directRendererCreatesSharedSequenceOnceWithoutRetainingState() {
        RelationalTableDefinition desired = RelationalTableDefinition.builder(TABLE)
                .addColumn(generated("first_value")).addColumn(generated("second_value")).build();
        SchemaOperation operation = SchemaOperation.of(SchemaOperation.Kind.CREATE_TABLE, TABLE, "events",
                null, desired, SchemaOperation.Compatibility.SAFE_INCREMENTAL);
        RelationalSchemaSqlRenderer renderer = RelationalSchemaSqlRenderer.create(DIALECT.schema());

        for (int attempt = 0; attempt < 2; attempt++) {
            assertEquals(1, renderer.render(operation).stream()
                    .filter(request -> request.sql().startsWith("create sequence ")).count());
        }
    }

    @Test
    void rejectsDifferentOptionsForTheSameSequenceInOneTableOrExistingSnapshot() {
        ColumnDefinition conflicting = ColumnDefinition.builder("second_value", "BIGINT")
                .generation(ValueGeneration.sequence("shared_seq", 10, 1, 100)).build();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(TABLE)
                .addColumn(generated("first_value")).addColumn(conflicting).build();
        SchemaOperation operation = SchemaOperation.of(SchemaOperation.Kind.CREATE_TABLE, TABLE, "events",
                null, desired, SchemaOperation.Compatibility.SAFE_INCREMENTAL);
        assertThrows(IllegalArgumentException.class,
                () -> RelationalSchemaSqlRenderer.create(DIALECT.schema()).render(operation));
        assertThrows(IllegalArgumentException.class,
                () -> review(desired, SchemaSnapshot.absent(TABLE)));
        RelationalTableDefinition existing = RelationalTableDefinition.builder(TABLE)
                .addColumn(generated("first_value")).build();
        assertThrows(IllegalArgumentException.class,
                () -> review(desired, SchemaSnapshot.present(existing)));
    }

    private static ReviewedSchemaPlan review(RelationalTableDefinition desired, SchemaSnapshot actual) {
        ReviewedSchemaPlan plan = RelationalSchemaPlanReviewer.create(DIALECT).review(
                DatabaseDescriptor.of("PostgreSQL", "17", DIALECT), desired, actual,
                SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
        assertFalse(plan.requiresManualAction());
        return plan;
    }

    private static long sequenceCreates(ReviewedSchemaPlan plan) {
        return plan.steps().stream().flatMap(step -> step.request().stream())
                .filter(request -> request.sql().startsWith("create sequence ")).count();
    }
}
