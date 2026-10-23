package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaTableCommentPlanningTest {

    @Test
    void unchangedCommentDoesNotRequireDialectSupportOrPhysicalMetadata() {
        List<SqlRequest> requests = changes(SchemaDialect.standard(), target("before"), null);
        assertTrue(requests.isEmpty());
    }

    @Test
    void independentCommentSqlKeepsEscapingAndRemovalWithoutPhysicalMetadata() {
        SchemaDialect dialect = SchemaDialect.builder().quoteIdentifiers('"').commentOnColumn().build();
        var requests = new ArrayList<SqlRequest>();
        requests.addAll(changes(dialect, target("it's new"), null));
        requests.addAll(changes(dialect, target(null), null));
        assertEquals(List.of("comment on column \"items\".\"code\" is 'it''s new'",
                        "comment on column \"items\".\"code\" is null"),
                requests.stream().map(SqlRequest::sql).toList());
        assertTrue(requests.stream().allMatch(request -> request.parameters().isEmpty()));
    }

    @Test
    void unsupportedCommentFailsAtTheSharedRenderingBoundary() {
        var render = assertThrows(UnsupportedOperationException.class,
                () -> changes(SchemaDialect.standard(), target("after"), null));
        assertTrue(render.getMessage().contains("CHANGE_COLUMN"));
    }

    @Test
    void inlineCommentAndTypeChangesShareOnePhysicalRewrite() {
        SchemaDialect dialect = RdbDialect.mysql().schema();
        var failure = assertThrows(IllegalStateException.class,
                () -> changes(dialect, target("after"), null));
        assertEquals("rewriting an existing column requires complete physical column metadata",
                failure.getMessage());
        SchemaSnapshot snapshot = SchemaSnapshot.present(RelationalTableDefinition.builder("items")
                .addColumn(current()).build());
        List<SqlRequest> requests = changes(dialect, target("after").withLength(32), snapshot);
        assertEquals(1, requests.size());
        assertTrue(requests.getFirst().sql().contains("VARCHAR(32)"));
        assertTrue(requests.getFirst().sql().contains("comment 'after'"));
    }

    @Test
    void plannerDoesNotReturnAPartialPlanWhenCommentRenderingFails() {
        var renderer = FormSchemaSqlRenderer.create(SchemaDialect.builder()
                .generatedValues(SchemaDialect.GeneratedValueStyle.POSTGRESQL).build());
        var current = RelationalTableDefinition.builder("items").addColumn(current()).build();
        var target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("generated", "BIGINT")
                        .withGeneration(ValueGeneration.sequence("items_seq")))
                .addField(target("after")).build();
        var failure = assertThrows(UnsupportedOperationException.class,
                () -> renderer.migrateSafelyPlan(current, target, List.of()));
        assertTrue(failure.getMessage().contains("CHANGE_COLUMN"));
    }

    private static ColumnDefinition current() {
        return ColumnDefinition.builder("code", "VARCHAR").length(16).comment("before").build();
    }

    private static List<SqlRequest> changes(SchemaDialect dialect, DynamicField target, SchemaSnapshot snapshot) {
        return new SchemaMigrationPlanner(FormSchemaSqlRenderer.create(dialect)).migrateSafelyPlan(
                RelationalTableDefinition.builder("items").addColumn(current()).build(),
                DynamicForm.builder("items", "items").addField(target).build(), List.of(), List.of(),
                SchemaMigrationOptions.safe(), snapshot).requests();
    }

    private static DynamicField target(String comment) {
        return DynamicField.of("code", "VARCHAR").withLength(16).withComment(comment);
    }
}
