package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicFormChangeSet;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import static org.junit.jupiter.api.Assertions.*;

class SchemaSequenceCacheReuseTest {

    private static final RelationIdentity TABLE = RelationIdentity.table("events");

    @TestFactory
    Stream<DynamicTest> reusesObservedCacheWhenDesiredCacheIsUnspecified() {
        return dialects().flatMap(dialect -> Stream.of(0, 20).map(cache ->
                DynamicTest.dynamicTest(dialect.name() + " relational cache=" + cache, () -> {
                    ColumnDefinition existing = column("first_value", 20, 1);
                    RelationalTableDefinition desired = RelationalTableDefinition.builder(TABLE)
                            .addColumn(column("first_value", cache, 1))
                            .addColumn(column("second_value", cache, 1)).build();
                    ReviewedSchemaPlan plan = assertDoesNotThrow(() -> review(dialect, existing, desired));
                    assertFalse(plan.requiresManualAction(), plan.operations().toString());
                    assertEquals(1, plan.requests().stream()
                            .filter(request -> request.sql().startsWith("alter table ")).count());
                    assertTrue(plan.requests().stream()
                            .noneMatch(request -> request.sql().startsWith("create sequence ")));
                    assertTrue(plan.requests().getFirst().sql().startsWith("alter table "));
                    assertTrue(plan.requests().getFirst().sql().contains("second_value"));
                })));
    }

    @TestFactory
    Stream<DynamicTest> directFormMigrationReusesCacheWithoutRecreatingSequence() {
        return dialects().flatMap(dialect -> Stream.of(0, 20).map(cache ->
                DynamicTest.dynamicTest(dialect.name() + " form cache=" + cache, () -> {
                    DynamicField existing = field("first_value", 20, 1);
                    DynamicField added = field("second_value", cache, 1);
                    DynamicForm source = DynamicForm.builder("events", "events").addField(existing).build();
                    DynamicForm target = DynamicForm.builder("events", "events")
                            .addField(existing).addField(added).build();
                    var changes = new DynamicFormChangeSet(source, target, List.of(added), List.of(), List.of());
                    var requests = assertDoesNotThrow(() -> FormSchemaSqlRenderer.create(dialect).migrate(changes));
                    assertEquals(1, requests.stream()
                            .filter(request -> request.sql().startsWith("alter table ")).count());
                    assertTrue(requests.stream()
                            .noneMatch(request -> request.sql().startsWith("create sequence ")));
                    assertTrue(requests.getFirst().sql().startsWith("alter table "));
                })));
    }

    @TestFactory
    Stream<DynamicTest> explicitCacheAndIncrementConflictsRemainRejected() {
        return dialects().flatMap(dialect -> Stream.of(true, false).map(cacheConflict ->
                DynamicTest.dynamicTest(dialect.name() + " conflict=" + cacheConflict, () -> {
                    int cache = cacheConflict ? 30 : 0;
                    long increment = cacheConflict ? 1 : 2;
                    ColumnDefinition existing = column("first_value", 20, 1);
                    RelationalTableDefinition desired = RelationalTableDefinition.builder(TABLE)
                            .addColumn(existing).addColumn(column("second_value", cache, increment)).build();
                    assertThrows(IllegalArgumentException.class, () -> review(dialect, existing, desired));

                    DynamicField old = field("first_value", 20, 1);
                    DynamicField added = field("second_value", cache, increment);
                    DynamicForm source = DynamicForm.builder("events", "events").addField(old).build();
                    DynamicForm target = DynamicForm.builder("events", "events").addField(old).addField(added).build();
                    var changes = new DynamicFormChangeSet(source, target, List.of(added), List.of(), List.of());
                    assertThrows(IllegalArgumentException.class,
                            () -> FormSchemaSqlRenderer.create(dialect).migrate(changes));
                })));
    }

    @TestFactory
    Stream<DynamicTest> newTableReusesSequenceObservedInAnotherTable() {
        return dialects().map(dialect -> DynamicTest.dynamicTest(dialect.name() + " shared snapshot", () -> {
            var reviewer = RelationalSchemaPlanReviewer.create(dialect);
            SchemaSnapshot existing = SchemaSnapshot.present(RelationalTableDefinition.builder(TABLE)
                    .addColumn(column("first_value", 20, 1)).build());
            var sequences = reviewer.observedSequences(List.of(existing));
            RelationIdentity addedTable = RelationIdentity.table("new_events");
            var desired = RelationalTableDefinition.builder(addedTable)
                    .addColumn(column("second_value", 0, 1)).build();
            ReviewedSchemaPlan plan = reviewer.review(DatabaseDescriptor.of(dialect.name(), "test", dialect),
                    desired, SchemaSnapshot.absent(addedTable), SchemaSnapshotCoverage.complete(),
                    SchemaCompatibilityMode.EXACT, sequences);
            assertFalse(plan.requiresManualAction());
            assertEquals(1, plan.requests().stream()
                    .filter(request -> request.sql().startsWith("create table ")).count());
            assertTrue(plan.requests().stream().noneMatch(request -> request.sql().startsWith("create sequence ")));
            assertEquals(20, sequences.values().iterator().next().generation().cacheSize());
        }));
    }

    @TestFactory
    Stream<DynamicTest> mixedCacheDeclarationsReuseExistingSequenceInEitherOrder() {
        return dialects().flatMap(dialect -> Stream.of(false, true).map(reverse ->
                DynamicTest.dynamicTest(dialect.name() + " mixed existing reverse=" + reverse, () -> {
                    DynamicField existing = field("first_value", 20, 1);
                    List<DynamicField> added = reverse
                            ? List.of(field("third_value", 20, 1), field("second_value", 0, 1))
                            : List.of(field("second_value", 0, 1), field("third_value", 20, 1));
                    DynamicForm source = DynamicForm.builder("events", "events").addField(existing).build();
                    DynamicForm target = DynamicForm.builder("events", "events").addField(existing)
                            .addField(added.get(0)).addField(added.get(1)).build();
                    var changes = new DynamicFormChangeSet(source, target, added, List.of(), List.of());
                    var requests = assertDoesNotThrow(() -> FormSchemaSqlRenderer.create(dialect).migrate(changes));
                    assertEquals(2, requests.stream()
                            .filter(request -> request.sql().startsWith("alter table ")).count());
                    assertTrue(requests.stream()
                            .noneMatch(request -> request.sql().startsWith("create sequence ")));
                })));
    }

    @TestFactory
    Stream<DynamicTest> conflictingNewSequenceDeclarationsStillFailInEitherOrder() {
        return dialects().flatMap(dialect -> Stream.of(false, true).map(reverse ->
                DynamicTest.dynamicTest(dialect.name() + " mixed new reverse=" + reverse, () -> {
                    List<DynamicField> added = reverse
                            ? List.of(field("first_value", 20, 1), field("second_value", 0, 1))
                            : List.of(field("second_value", 0, 1), field("first_value", 20, 1));
                    DynamicField ordinary = DynamicField.of("id", "BIGINT");
                    DynamicForm source = DynamicForm.builder("events", "events").addField(ordinary).build();
                    DynamicForm target = DynamicForm.builder("events", "events").addField(ordinary)
                            .addField(added.get(0)).addField(added.get(1)).build();
                    var changes = new DynamicFormChangeSet(source, target, added, List.of(), List.of());
                    assertThrows(IllegalArgumentException.class,
                            () -> FormSchemaSqlRenderer.create(dialect).migrate(changes));
                })));
    }

    @Test
    void sqlServerSequenceTypeConflictIsNotHiddenByUnspecifiedCache() {
        ColumnDefinition existing = column("first_value", 20, 1);
        var desired = RelationalTableDefinition.builder(TABLE).addColumn(existing)
                .addColumn(ColumnDefinition.builder("second_value", "INTEGER")
                        .generation(ValueGeneration.sequence("shared_seq", 1, 1, 0)).build()).build();
        assertThrows(IllegalArgumentException.class, () -> review(RdbDialect.sqlServer(), existing, desired));
    }

    @Test
    void mysqlStillRejectsNamedSequences() {
        var desired = RelationalTableDefinition.builder(TABLE).addColumn(column("first_value", 0, 1)).build();
        var operation = SchemaOperation.of(SchemaOperation.Kind.CREATE_TABLE, TABLE, "events", null, desired,
                SchemaOperation.Compatibility.SAFE_INCREMENTAL);
        assertThrows(IllegalArgumentException.class,
                () -> RelationalSchemaSqlRenderer.create(RdbDialect.mysql().schema()).render(operation));
    }

    private static ReviewedSchemaPlan review(RdbDialect dialect, ColumnDefinition existing,
                                             RelationalTableDefinition desired) {
        SchemaSnapshot actual = SchemaSnapshot.present(
                RelationalTableDefinition.builder(TABLE).addColumn(existing).build());
        return RelationalSchemaPlanReviewer.create(dialect).review(
                DatabaseDescriptor.of(dialect.name(), "test", dialect), desired, actual,
                SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
    }

    private static ColumnDefinition column(String name, int cache, long increment) {
        return ColumnDefinition.builder(name, "BIGINT")
                .generation(ValueGeneration.sequence("shared_seq", 1, increment, cache)).build();
    }

    private static DynamicField field(String name, int cache, long increment) {
        return DynamicField.of(name, "BIGINT")
                .withGeneration(ValueGeneration.sequence("shared_seq", 1, increment, cache));
    }

    private static Stream<RdbDialect> dialects() {
        return Stream.of(RdbDialect.postgresql(), RdbDialect.oracle(), RdbDialect.sqlServer(), RdbDialect.h2());
    }
}
