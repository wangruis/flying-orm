package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaLargeReviewedPlanExecutionTest {

    @Test
    void executesOneThousandReviewedAddColumnsInOrderWithSynchronousDriverResults() {
        int additions = 1_000;
        RdbDialect dialect = RdbDialect.postgresql();
        RelationIdentity identity = RelationIdentity.table("wide_form");
        ColumnDefinition id = ColumnDefinition.builder("id", "BIGINT").nullable(false).build();
        RelationalTableDefinition before = RelationalTableDefinition.builder(identity).addColumn(id).build();
        RelationalTableDefinition.Builder desiredBuilder = RelationalTableDefinition.builder(identity).addColumn(id);
        for (int column = 0; column < additions; column++) {
            desiredBuilder.addColumn(ColumnDefinition.builder("field_" + column, "INTEGER").build());
        }
        RelationalTableDefinition desired = desiredBuilder.build();
        AtomicReference<SchemaSnapshot> current = new AtomicReference<>(SchemaSnapshot.present(before));
        AtomicInteger writes = new AtomicInteger();
        AtomicInteger reads = new AtomicInteger();
        SchemaExecutionFixtures.ReactiveExecutor executor = new SchemaExecutionFixtures.ReactiveExecutor(
                request -> {
                    if (writes.incrementAndGet() == additions) {
                        current.set(SchemaSnapshot.present(desired));
                    }
                    return Mono.just(0L);
                });
        ReactiveFormMetadataReader reader = new ReactiveFormMetadataReader() {
            @Override
            public SchemaSnapshotCoverage snapshotCoverage() {
                return SchemaSnapshotCoverage.complete();
            }

            @Override
            public Mono<SchemaSnapshot> readSnapshot(RelationIdentity relation) {
                return Mono.fromSupplier(() -> {
                    reads.incrementAndGet();
                    return current.get();
                });
            }

            @Override
            public Mono<DynamicForm> readForm(String formId, RelationIdentity relation) {
                return Mono.error(new UnsupportedOperationException());
            }

            @Override
            public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).map(form -> form.toTableDefinition(relation));
            }

        };
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, dialect);
        SchemaMigrationPlan plan = client.reviewRelational(
                DatabaseDescriptor.of("PostgreSQL", "17", dialect), desired, reader,
                SchemaCompatibilityMode.EXACT).block();

        assertEquals(additions, plan.steps().size());
        assertEquals(additions, plan.requests().size());
        assertTrue(plan.steps().stream().allMatch(
                step -> step.operation().kind() == SchemaOperation.Kind.ADD_COLUMN));
        Mono<SchemaMigrationResult> execution = client.execute(plan, reader);
        assertTrue(executor.requests.isEmpty());

        SchemaMigrationResult report = execution.block();

        assertNotNull(report);
        assertEquals(plan.requests(), executor.requests);
        assertEquals(additions, writes.get());
        assertEquals(1, reads.get());
        assertEquals(additions, report.steps().size());
        for (int index = 0; index < additions; index++) {
            com.flying.orm.rdb.execution.SqlExecutionStepResult step = report.steps().get(index);
            assertEquals(index, step.stepIndex());
            assertEquals(plan.requests().get(index), step.request());
            assertEquals(0L, step.rowsUpdated());
        }
    }
}
