package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Mono;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaDropMetadataReadTest {

    @TestFactory
    Stream<DynamicTest> skippedColumnDropsDoNotReadPhysicalMetadata() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest(
                reactive ? "reactive retained index" : "jdbc retained index", () -> {
                    RelationalTableDefinition current = RelationalTableDefinition.builder("items")
                            .addColumn(ColumnDefinition.builder("id", "INTEGER").build())
                            .addColumn(ColumnDefinition.builder("code", "INTEGER").build())
                            .addIndex(IndexDefinition.builder("idx_code").addColumn("code").build()).build();
                    DynamicForm target = DynamicForm.builder("items", "items")
                            .addField(DynamicField.of("id", "INTEGER"))
                            .addField(DynamicField.of("note", "INTEGER")).build();
                    SchemaMigrationPlanner planner = FormSchemaSqlRenderer.create(RdbDialect.sqlServer())
                            .migrationPlanner();
                    SchemaMigrationOptions options = SchemaMigrationOptions.safe().allowDropColumn();
                    AtomicInteger reads = new AtomicInteger();
                    SchemaMigrationPlan plan;
                    if (reactive) {
                        ReactiveFormMetadataReader reader = new ReactiveFormMetadataReader() {
                            public Mono<DynamicForm> readForm(String id, RelationIdentity relation) {
                                throw new AssertionError("planning must use table metadata");
                            }

                            public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
                                return Mono.just(current);
                            }

                            public Mono<SchemaSnapshot> readSnapshot(RelationIdentity relation) {
                                reads.incrementAndGet();
                                return Mono.error(new AssertionError("skipped drops need no physical snapshot"));
                            }
                        };
                        Mono<SchemaMigrationPlan> pending = planner.plan(target, List.of(), List.of(), reader, options);
                        assertEquals(0, reads.get(), "planning must stay cold");
                        plan = pending.block();
                        assertEquals(plan.requests().stream().map(request -> request.sql()).toList(),
                                pending.block().requests().stream().map(request -> request.sql()).toList());
                    } else {
                        JdbcFormMetadataReader reader = new JdbcFormMetadataReader() {
                            public DynamicForm readForm(String id, RelationIdentity relation) {
                                throw new AssertionError("planning must use table metadata");
                            }

                            public RelationalTableDefinition readTable(RelationIdentity relation) {
                                return current;
                            }

                            public SchemaSnapshot readSnapshot(RelationIdentity relation) {
                                reads.incrementAndGet();
                                throw new AssertionError("skipped drops need no physical snapshot");
                            }
                        };
                        plan = planner.planJdbc(target, List.of(), List.of(), reader, options);
                    }
                    assertEquals(0, reads.get());
                    assertEquals(List.of("alter table [items] add [note] INT null"),
                            plan.requests().stream().map(request -> request.sql()).toList());
                    assertTrue(plan.skippedChanges().stream().anyMatch(change ->
                            change.kind() == SkippedSchemaChange.Kind.DROP_COLUMN));
                }));
    }

    @TestFactory
    Stream<DynamicTest> dropReadsPhysicalMetadataOnlyWhenForwardSqlNeedsIt() {
        return Stream.of(RdbDialect.postgresql(), RdbDialect.mysql(), RdbDialect.h2(),
                        RdbDialect.oracle(), RdbDialect.sqlServer())
                .map(dialect -> DynamicTest.dynamicTest(dialect.name(), () -> {
                    SchemaMigrationPlanner planner = FormSchemaSqlRenderer.create(dialect).migrationPlanner();
                    DynamicForm source = DynamicForm.builder("items", "items")
                            .addField(DynamicField.of("id", "INTEGER"))
                            .addField(DynamicField.of("code", "INTEGER")).build();
                    DynamicForm target = DynamicForm.builder("items", "items")
                            .addField(DynamicField.of("id", "INTEGER")).build();
                    SchemaSnapshot snapshot = SchemaSnapshot.builder(RelationIdentity.table("items"))
                            .tablePresent().physicalColumns(List.of(
                                    ColumnDefinition.builder("id", "INTEGER").build(),
                                    ColumnDefinition.builder("code", "INTEGER")
                                            .defaultValue(ColumnDefault.literal(7))
                                            .defaultConstraintName("sqlserver".equals(dialect.name())
                                                    ? "df_items_code" : null).build())).build();
                    AtomicInteger reads = new AtomicInteger();
                    ReactiveFormMetadataReader reader = reader(source, snapshot, reads);
                    SchemaMigrationOptions options = SchemaMigrationOptions.safe().allowDropColumn();

                    planner.plan(target, List.of(), List.of(), reader, options).block();
                    int forwardReads = "sqlserver".equals(dialect.name()) ? 1 : 0;
                    assertEquals(forwardReads, reads.get(),
                            "SQL Server forward drops must observe their default constraint names");
                    SchemaMigrationPlan reviewed = planner.plan(target, List.of(), List.of(), reader,
                            options).block();
                    assertEquals(2 * forwardReads, reads.get());
                    if (forwardReads != 0) {
                        assertEquals(List.of("alter table [items] drop constraint [df_items_code], column [code]"),
                                reviewed.requestsForExecution().stream().map(request -> request.sql()).toList());
                    } else {
                        assertTrue(reviewed.requestsForExecution().stream()
                                .anyMatch(request -> request.sql().contains("drop column")));
                    }
                    if (forwardReads != 0) {
                        assertThrows(IllegalStateException.class, () -> planner.plan(target, List.of(), List.of(),
                                reader(source, null, reads), options).block());
                    }
                }));
    }

    private static ReactiveFormMetadataReader reader(DynamicForm source, SchemaSnapshot snapshot,
                                                     AtomicInteger reads) {
        return new ReactiveFormMetadataReader() {
            public Mono<DynamicForm> readForm(String id, RelationIdentity relation) {
                return Mono.just(source);
            }

            @Override
            public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).map(form -> form.toTableDefinition(relation));
            }

            public Mono<SchemaSnapshot> readSnapshot(RelationIdentity relation) {
                reads.incrementAndGet();
                return Mono.justOrEmpty(snapshot);
            }
        };
    }

}
