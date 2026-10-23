package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class FormSchemaDdlRendererTest {
    @Test
    void ownerAndContainsTableAreRenderedSeparatelyWithUnchangedPhysicalNames() {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR").withUnique(true))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.EXACT, EncryptedSearchMode.CONTAINS).build()).build();
        FormSchemaSqlRenderer builtin = FormSchemaSqlRenderer.create(RdbDialect.h2());
        List<SchemaMigrationPlan> calls = new ArrayList<>();
        FormSchemaSqlRenderer custom = FormSchemaSqlRenderer.create(RdbDialect.h2().schema()
                .withFormRenderer((current, snapshot, plan) -> {
                    assertNull(current);
                    assertNull(snapshot);
                    assertFalse(plan.tableExists());
                    calls.add(plan);
                    return marked(plan.requests());
                }));
        List<SqlRequest> expected = marked(builtin.createTable(form));
        assertEquals(expected, custom.createTable(form));
        assertEquals(2, calls.size());
        assertEquals("items", calls.getFirst().target().name());
        List<IndexDefinition> indexes = builtin.createTablePlan(form, List.of()).target().indexes();
        assertEquals(indexes.stream().map(IndexDefinition::name).toList(),
                calls.getFirst().target().indexes().stream().map(IndexDefinition::name).toList());
        assertEquals(indexes.stream().map(IndexDefinition::columns).toList(),
                calls.getFirst().target().indexes().stream().map(IndexDefinition::columns).toList());
        assertFalse(calls.getLast().target().foreignKeys().isEmpty());
        calls.clear();
        assertEquals(expected, custom.createTablePlan(form, List.of()).requests());
        assertEquals(2, calls.size());
    }

    @Test
    void emptyPlanDoesNotCallRendererAndEmptyReplacementCannotEraseDdl() {
        DynamicForm form = form("old");
        AtomicInteger calls = new AtomicInteger();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(SchemaDialect.builder()
                .formRenderer((current, snapshot, plan) -> { calls.incrementAndGet(); return List.of(); }).build());
        assertTrue(renderer.migrateSafelyPlan(form.toTableDefinition(), form, List.of()).requests().isEmpty());
        assertEquals(0, calls.get());
        assertThrows(IllegalArgumentException.class, () -> renderer.createTable(form));
    }

    @Test
    void reviewRendersTheForwardCommentChangeOnce() {
        DynamicForm source = form("old");
        DynamicForm target = form("new");
        AtomicInteger forwards = new AtomicInteger();
        SchemaDialect dialect = RdbDialect.postgresql().schema().withFormRenderer(new FormSchemaDdlRenderer() {
            @Override
            public List<SqlRequest> render(RelationalTableDefinition current, SchemaSnapshot snapshot, SchemaMigrationPlan plan) {
                forwards.incrementAndGet();
                return marked(plan.requests());
            }

        });
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(dialect);
        SchemaMigrationPlan plan = renderer.migrationPlanner().migrateSafelyPlan(
                source.toTableDefinition(), target, List.of(), List.of(), SchemaMigrationOptions.safe());
        assertEquals(0, forwards.get(), "review must first receive the native plan");
        SchemaMigrationPlan reviewed = renderer.renderPlan(source.toTableDefinition(), null, plan, SchemaMigrationOptions.safe());
        assertEquals(1, forwards.get());
        assertTrue(reviewed.requests().stream().allMatch(sql -> sql.sql().startsWith("/* custom */ ")));
    }

    @Test
    void directMigrationPreservesSnapshotAndFreezesExtensionOutput() {
        DynamicForm source = form("old");
        DynamicForm target = form("new");
        SchemaSnapshot snapshot = SchemaSnapshot.present(source.toTableDefinition());
        List<SqlRequest> returned = new ArrayList<>();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql().schema()
                .withFormRenderer((current, actual, plan) -> {
                    assertSame(snapshot, actual);
                    assertEquals(source.table(), current.name());
                    assertTrue(plan.tableExists());
                    returned.addAll(marked(plan.requests()));
                    return returned;
                }));
        List<SqlRequest> requests = renderer.migrate(source.diffTo(target), snapshot);
        assertEquals(1, requests.size());
        returned.clear();
        assertEquals(1, requests.size());
        assertThrows(UnsupportedOperationException.class, requests::clear);
    }

    @Test
    void extensionReceivesConcurrentIndexSqlWithoutImposingAnOnlinePolicy() {
        DynamicForm source = form("old");
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql().schema()
                .withFormRenderer((current, snapshot, plan) -> marked(plan.requests())));
        SchemaMigrationPlan index = renderer.migrationPlanner().migrateSafelyPlan(source.toTableDefinition(), source,
                List.of(IndexDefinition.builder("ix_title").addColumn("title").build()),
                List.of(), SchemaMigrationOptions.safe());
        SchemaMigrationPlan online = renderer.renderPlan(source.toTableDefinition(), null, index, SchemaMigrationOptions.safe().preferConcurrentIndexes());
        assertTrue(online.requests().getFirst().sql().contains("create index concurrently"));
        DynamicForm expanded = DynamicForm.builder("items", "items")
                .addField(source.field("id")).addField(source.field("title"))
                .addField(DynamicField.of("extra", "VARCHAR")).build();
        SchemaMigrationPlan blocking = renderer.migrationPlanner().migrateSafelyPlan(source.toTableDefinition(),
                expanded, List.of(), List.of(), SchemaMigrationOptions.safe());
        SchemaMigrationPlan refused = renderer.renderPlan(source.toTableDefinition(), null, blocking, SchemaMigrationOptions.safe().preferConcurrentIndexes());
        assertEquals(refused.requests(), refused.requestsForExecution());
        assertTrue(refused.requests().getFirst().sql().startsWith("/* custom */ alter table"));
    }

    @Test
    void dialectCopiesRetainBothIndependentExtensions() {
        FormSchemaDdlRenderer form = (current, snapshot, plan) -> marked(plan.requests());
        SchemaOperationRenderer operation = (step, defaults) -> defaults.get();
        SchemaDialect builtin = RdbDialect.postgresql().schema();
        SchemaDialect first = builtin.withFormRenderer(form).withOperationRenderer(operation);
        SchemaDialect second = builtin.withOperationRenderer(operation).withFormRenderer(form);
        assertSame(form, first.formRenderer());
        assertSame(operation, first.operationRenderer());
        assertSame(form, second.formRenderer());
        assertSame(operation, second.operationRenderer());
        assertNull(builtin.formRenderer());
        assertNull(builtin.operationRenderer());
    }

    private static DynamicForm form(String comment) {
        return DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("title", "VARCHAR").withComment(comment)).build();
    }

    private static List<SqlRequest> marked(List<SqlRequest> requests) {
        return requests.stream().map(request -> new SqlRequest("/* custom */ " + request.sql(), request.parameters())).toList();
    }
}
