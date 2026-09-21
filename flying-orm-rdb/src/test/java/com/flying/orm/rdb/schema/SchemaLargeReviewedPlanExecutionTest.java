package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        VerifiedSchemaPlanFixtures.ReactiveExecutor executor = new VerifiedSchemaPlanFixtures.ReactiveExecutor(
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
            public Mono<SchemaSnapshot> readSnapshot(String table) {
                return Mono.fromSupplier(() -> {
                    reads.incrementAndGet();
                    return current.get();
                });
            }

            @Override
            public Mono<DynamicForm> readForm(String formId, String table) {
                return Mono.error(new UnsupportedOperationException());
            }

            @Override
            public Mono<DynamicForm> readForm(String formId, String schema, String table) {
                return Mono.error(new UnsupportedOperationException());
            }
        };
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, dialect);
        ReviewedSchemaPlan plan = client.reviewRelational(
                DatabaseDescriptor.of("PostgreSQL", "17", dialect), desired, reader,
                SchemaCompatibilityMode.EXACT).block();

        assertFalse(plan.requiresManualAction());
        assertEquals(additions, plan.steps().size());
        assertEquals(additions, plan.requests().size());
        assertTrue(plan.steps().stream().allMatch(
                step -> step.operation().kind() == SchemaOperation.Kind.ADD_COLUMN));
        Mono<SchemaExecutionReport> execution = client.executeReviewed(plan, reader);
        assertTrue(executor.requests.isEmpty());

        SchemaExecutionReport report = execution.block();

        assertEquals(SchemaExecutionStatus.SUCCESS, report.status());
        assertEquals(plan.requests(), executor.requests);
        assertEquals(additions, writes.get());
        assertEquals(3, reads.get());
        assertEquals(additions, report.steps().size());
        for (int index = 0; index < additions; index++) {
            SchemaExecutionReport.StepResult step = report.steps().get(index);
            assertEquals(plan.steps().get(index).order(), step.order());
            assertEquals(SchemaExecutionStatus.SUCCESS, step.status());
            assertTrue(step.sqlSent());
            assertEquals(0L, step.rowsUpdated().orElseThrow());
        }
    }
}
