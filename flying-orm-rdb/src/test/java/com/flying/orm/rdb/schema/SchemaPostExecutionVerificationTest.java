package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaPostExecutionVerificationTest {

    @TestFactory
    Stream<DynamicTest> safeIncrementalMustRejectAnUnfulfilledAddition() {
        return List.of(false, true).stream().flatMap(reactive -> List.of(false, true).stream()
                .map(create -> DynamicTest.dynamicTest("reactive=" + reactive + ", create=" + create,
                        () -> assertUnfulfilledAddition(reactive, create))));
    }

    private static void assertUnfulfilledAddition(boolean reactive, boolean createTable) {
        RelationalTableDefinition desired = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        SchemaSnapshot before = createTable ? SchemaSnapshot.absent(desired.identity())
                : SchemaSnapshot.present(VerifiedSchemaPlanFixtures.table(VerifiedSchemaPlanFixtures.ID));
        ReviewedSchemaPlan plan = review(before, desired, SchemaCompatibilityMode.SAFE_INCREMENTAL);

        SchemaExecutionReport report = execute(reactive, plan, before, before);

        assertEquals(SchemaExecutionStatus.VERIFICATION_FAILED, report.status());
        assertFalse(report.successful());
        assertEquals(SchemaExecutionStatus.SUCCESS, report.steps().getFirst().status());
        assertFalse(report.verification().orElseThrow().compatible());
        assertEquals(createTable ? SchemaOperation.Kind.CREATE_TABLE : SchemaOperation.Kind.ADD_COLUMN,
                report.verification().orElseThrow().operations().getFirst().kind());
    }

    @TestFactory
    Stream<DynamicTest> safeIncrementalAcceptsActualConvergence() {
        return List.of(false, true).stream().flatMap(reactive -> List.of(false, true).stream()
                .map(create -> DynamicTest.dynamicTest("reactive=" + reactive + ", create=" + create,
                        () -> assertActualConvergence(reactive, create))));
    }

    private static void assertActualConvergence(boolean reactive, boolean createTable) {
        RelationalTableDefinition desired = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        SchemaSnapshot before = createTable ? SchemaSnapshot.absent(desired.identity())
                : SchemaSnapshot.present(VerifiedSchemaPlanFixtures.table(VerifiedSchemaPlanFixtures.ID));
        ReviewedSchemaPlan plan = review(before, desired, SchemaCompatibilityMode.SAFE_INCREMENTAL);

        SchemaExecutionReport report = execute(reactive, plan, before, SchemaSnapshot.present(desired));

        assertTrue(report.successful());
        assertTrue(report.verification().orElseThrow().exact());
    }

    @TestFactory
    Stream<DynamicTest> postconditionPreservesOnlyPermittedExtras() {
        return List.of(false, true).stream().flatMap(reactive -> Stream.of(SchemaCompatibilityMode.values())
                .map(mode -> DynamicTest.dynamicTest("reactive=" + reactive + ", mode=" + mode,
                        () -> assertPermittedExtras(reactive, mode))));
    }

    private static void assertPermittedExtras(boolean reactive, SchemaCompatibilityMode mode) {
        SchemaSnapshot before = SchemaSnapshot.present(
                VerifiedSchemaPlanFixtures.table(VerifiedSchemaPlanFixtures.ID));
        RelationalTableDefinition desired = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        SchemaSnapshot after = SchemaSnapshot.present(VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE, VerifiedSchemaPlanFixtures.TAG));
        ReviewedSchemaPlan plan = review(before, desired, mode);

        SchemaExecutionReport report = execute(reactive, plan, before, after);

        assertEquals(mode != SchemaCompatibilityMode.EXACT, report.successful());
        assertEquals(mode == SchemaCompatibilityMode.EXACT ? SchemaExecutionStatus.VERIFICATION_FAILED
                : SchemaExecutionStatus.SUCCESS, report.status());
        assertEquals(SchemaOperation.Compatibility.COMPATIBLE_EXTRA,
                report.verification().orElseThrow().operations().getFirst().compatibility());
    }

    private static ReviewedSchemaPlan review(SchemaSnapshot before, RelationalTableDefinition desired,
                                             SchemaCompatibilityMode mode) {
        RdbDialect dialect = RdbDialect.h2();
        return RelationalSchemaPlanReviewer.create(dialect).review(
                DatabaseDescriptor.of("H2", "2.4", dialect), desired, before,
                SchemaSnapshotCoverage.complete(), mode);
    }

    private static SchemaExecutionReport execute(boolean reactive, ReviewedSchemaPlan plan,
                                                 SchemaSnapshot before, SchemaSnapshot after) {
        AtomicInteger reads = new AtomicInteger();
        Supplier<SchemaSnapshot> snapshots = () -> reads.getAndIncrement() == 0 ? before : after;
        SchemaExecutionReport report = reactive
                ? VerifiedSchemaPlanExecutor.executeReactive(plan,
                        new VerifiedSchemaPlanFixtures.ReactiveExecutor(request -> Mono.just(0L)),
                        () -> Mono.fromSupplier(snapshots), SchemaSnapshotCoverage::complete,
                        () -> { }, SqlExecutionOptions.safeDefaults()).block()
                : VerifiedSchemaPlanExecutor.executeJdbc(plan,
                        new VerifiedSchemaPlanFixtures.SyncExecutor(request -> 0L),
                        snapshots, SchemaSnapshotCoverage::complete,
                        () -> { }, SqlExecutionOptions.safeDefaults());
        assertEquals(2, reads.get());
        return report;
    }

    @Test
    void rowsUpdatedCannotMasqueradeAsSchemaConvergence() {
        RelationalTableDefinition before = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID);
        RelationalTableDefinition desired = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        SchemaSnapshot unchanged = SchemaSnapshot.present(before);
        ReviewedSchemaPlan plan = VerifiedSchemaPlanFixtures.plan(
                unchanged, desired,
                new SqlRequest("alter table accounts add column note varchar(80)", List.of()));
        AtomicInteger reads = new AtomicInteger();
        VerifiedSchemaPlanFixtures.SyncExecutor executor =
                new VerifiedSchemaPlanFixtures.SyncExecutor(request -> 1L);

        SchemaExecutionReport report = VerifiedSchemaPlanExecutor.executeJdbc(
                plan,
                executor,
                () -> {
                    reads.incrementAndGet();
                    return unchanged;
                },
                SchemaSnapshotCoverage::complete,
                () -> {
                },
                SqlExecutionOptions.safeDefaults());

        assertEquals(SchemaExecutionStatus.VERIFICATION_FAILED, report.status());
        assertFalse(report.successful());
        assertEquals(2, reads.get());
        assertEquals(SchemaExecutionStatus.SUCCESS, report.steps().getFirst().status());
        assertTrue(report.verification().isPresent());
        assertFalse(report.verification().orElseThrow().compatible());
    }
}
