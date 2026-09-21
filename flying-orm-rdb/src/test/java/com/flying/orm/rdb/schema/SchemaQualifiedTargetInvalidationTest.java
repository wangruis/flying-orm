package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.cache.SchemaCacheInvalidationCoordinator;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaQualifiedTargetInvalidationTest {

    private static final RelationIdentity TARGET =
            RelationIdentity.of(null, "tenant.v1", "accounts.v2");
    private static final RelationalTableDefinition PRESENT = RelationalTableDefinition
            .builder(TARGET)
            .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
            .build();

    @Test
    void reactiveExecutionKeepsSchemaAndTableAsSeparateInvalidationSegments() {
        AtomicReference<SchemaSnapshot> current =
                new AtomicReference<>(SchemaSnapshot.present(PRESENT));
        SegmentedReader reader = new SegmentedReader(current, TARGET);
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.error(new AssertionError("metadata must use the supplied reader"));
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                current.set(SchemaSnapshot.absent(TARGET));
                return Mono.just(0L);
            }
        };
        ReactiveSchemaClient client = ReactiveSchemaClient.create(
                executor, RdbDialect.postgresql());
        ReviewedSchemaPlan plan = absentPlan(TARGET, PRESENT);

        SchemaExecutionReport report = client.executeReviewed(
                plan, reader, SchemaMigrationApproval.approve(plan, "test")).block();

        assertTrue(report.successful());
        assertEquals(List.of(TARGET, TARGET), reader.readTargets);
        assertEquals(List.of(TARGET), reader.invalidatedTargets);
        assertTrue(reader.segmented.isEmpty());
        assertTrue(reader.flat.isEmpty());
    }

    @Test
    void unqualifiedLiteralDotStaysOneRelationSegmentThroughReadAndInvalidation() {
        RelationIdentity target = RelationIdentity.table("accounts.v2");
        RelationalTableDefinition present = RelationalTableDefinition.builder(target)
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .build();
        AtomicReference<SchemaSnapshot> current =
                new AtomicReference<>(SchemaSnapshot.present(present));
        SegmentedReader reader = new SegmentedReader(current, target);
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.error(new AssertionError("metadata must use the supplied reader"));
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                current.set(SchemaSnapshot.absent(target));
                return Mono.just(0L);
            }
        };
        ReactiveSchemaClient client = ReactiveSchemaClient.create(
                executor, RdbDialect.postgresql());
        ReviewedSchemaPlan plan = absentPlan(target, present);

        SchemaExecutionReport report = client.executeReviewed(
                plan, reader, SchemaMigrationApproval.approve(plan, "test")).block();

        assertTrue(report.successful());
        assertEquals(List.of(target, target), reader.readTargets);
        assertEquals(List.of(target), reader.invalidatedTargets);
        assertTrue(reader.segmented.isEmpty());
        assertTrue(reader.flat.isEmpty());
    }

    @Test
    void unqualifiedIdentityAndLegacyConsumerKeepTheirOneSegmentContract() {
        List<String> invalidated = new ArrayList<>();
        SchemaCacheInvalidationCoordinator coordinator =
                SchemaCacheInvalidationCoordinator.from(ignored -> { })
                        .with(new Object(), invalidated::add);

        coordinator.invalidate(RelationIdentity.table("accounts.v2"));

        assertEquals(List.of("accounts.v2"), invalidated);
    }

    private static ReviewedSchemaPlan absentPlan(RelationIdentity target,
                                                 RelationalTableDefinition present) {
        SchemaSnapshot actual = SchemaSnapshot.present(present);
        String actualFingerprint = SchemaSnapshotFingerprint.of(actual);
        SchemaOperation operation = SchemaOperation.of(
                SchemaOperation.Kind.DROP_TABLE,
                target,
                target.table(),
                present,
                null,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);
        return ReviewedSchemaPlan.builder(DatabaseDescriptor.of(
                        "PostgreSQL", "17", RdbDialect.postgresql()))
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredAbsent(target)
                .desiredFingerprint(SchemaSnapshotFingerprint.of(SchemaSnapshot.absent(target)))
                .actualFingerprint(actualFingerprint)
                .addStep(SchemaPlanStep.executable(
                        0,
                        operation,
                        new SqlRequest("drop table target", List.of()),
                        SchemaMigrationRiskLevel.CRITICAL,
                        List.of(SchemaPlanPrecondition.actualSnapshot(actualFingerprint))))
                .build();
    }

    private static final class SegmentedReader
            implements ReactiveFormMetadataReader, MetadataCacheInvalidator {

        private final AtomicReference<SchemaSnapshot> current;
        private final RelationIdentity expected;
        private final List<String> flat = new ArrayList<>();
        private final List<List<String>> segmented = new ArrayList<>();
        private final List<RelationIdentity> readTargets = new ArrayList<>();
        private final List<RelationIdentity> invalidatedTargets = new ArrayList<>();

        private SegmentedReader(AtomicReference<SchemaSnapshot> current,
                                RelationIdentity expected) {
            this.current = current;
            this.expected = expected;
        }

        @Override
        public SchemaSnapshotCoverage snapshotCoverage() {
            return SchemaSnapshotCoverage.complete();
        }

        @Override
        public Mono<SchemaSnapshot> readSnapshot(String schema, String table) {
            return Mono.error(new AssertionError("segmented string metadata read must not execute"));
        }

        @Override
        public Mono<SchemaSnapshot> readSnapshot(RelationIdentity relation) {
            assertEquals(expected, relation);
            readTargets.add(relation);
            return Mono.just(current.get());
        }

        @Override
        public Mono<DynamicForm> readForm(String formId, String table) {
            return Mono.error(new AssertionError("unqualified metadata read must not execute"));
        }

        @Override
        public Mono<DynamicForm> readForm(String formId, String schema, String table) {
            return Mono.error(new AssertionError("form metadata read must not execute"));
        }

        @Override
        public Mono<TableMetadata> readTable(String table) {
            return Mono.error(new AssertionError("table metadata read must not execute"));
        }

        @Override
        public void invalidate(String table) {
            flat.add(table);
        }

        @Override
        public void invalidate(String schema, String table) {
            segmented.add(List.of(schema, table));
        }

        @Override
        public void invalidate(RelationIdentity relation) {
            assertEquals(expected, relation);
            invalidatedTargets.add(relation);
        }

        @Override
        public void invalidateAll() {
            throw new AssertionError("targeted DDL must not invalidate all metadata");
        }
    }
}
