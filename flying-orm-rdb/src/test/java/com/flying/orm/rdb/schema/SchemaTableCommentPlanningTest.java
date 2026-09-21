package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.TableMetadata;
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
        var renderer = new SchemaTableSqlRenderer(SchemaDialect.standard());
        var requests = new ArrayList<SqlRequest>();
        renderer.addMissingComment(requests, "items", current(), target("before"), null);
        assertTrue(requests.isEmpty());
    }

    @Test
    void independentCommentSqlKeepsEscapingAndRemovalWithoutPhysicalMetadata() {
        var renderer = new SchemaTableSqlRenderer(SchemaDialect.builder().quoteIdentifiers('"')
                .commentOnColumn().build());
        var requests = new ArrayList<SqlRequest>();
        renderer.addMissingComment(requests, "items", current(), target("it's new"), null);
        renderer.addMissingComment(requests, "items", current(), target(null), null);
        assertEquals(List.of("comment on column \"items\".\"code\" is 'it''s new'",
                        "comment on column \"items\".\"code\" is null"),
                requests.stream().map(SqlRequest::sql).toList());
        assertTrue(requests.stream().allMatch(request -> request.parameters().isEmpty()));
    }

    @Test
    void unsupportedCommentFailsWithTheSameErrorInPreflightAndRendering() {
        var renderer = new SchemaTableSqlRenderer(SchemaDialect.standard());
        var requests = new ArrayList<SqlRequest>();
        var preflight = assertThrows(IllegalArgumentException.class,
                () -> renderer.validateCommentChange("items", current(), target("after")));
        var render = assertThrows(IllegalArgumentException.class,
                () -> renderer.addMissingComment(requests, "items", current(), target("after"), null));
        assertEquals("the configured dialect cannot alter the column comment safely", preflight.getMessage());
        assertEquals(preflight.getMessage(), render.getMessage());
        assertTrue(requests.isEmpty());
    }

    @Test
    void inlineCommentPreservesPhysicalRequirementAndSkipsSeparateSqlWhenShapeChanges() {
        var renderer = new SchemaTableSqlRenderer(RdbDialect.mysql().schema());
        var requests = new ArrayList<SqlRequest>();
        var failure = assertThrows(IllegalStateException.class,
                () -> renderer.addMissingComment(requests, "items", current(), target("after"), null));
        assertEquals("rewriting an existing column requires complete physical column metadata",
                failure.getMessage());
        renderer.addMissingComment(requests, "items", current(), target("after").withLength(32), null);
        assertTrue(requests.isEmpty());
        renderer.addMissingComment(requests, "items", current(), target("after"),
                ColumnDefinition.builder("code", "VARCHAR").length(16).comment("before").build());
        assertEquals(1, requests.size());
        assertTrue(requests.getFirst().sql().contains("comment 'after'"));
    }

    @Test
    void plannerPreflightsAllCommentsBeforeRenderingEarlierAddedSequence() {
        var renderer = FormSchemaSqlRenderer.create(SchemaDialect.standard());
        var current = TableMetadata.builder("items").addColumn(current()).build();
        var target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("generated", "BIGINT")
                        .withGeneration(ValueGeneration.sequence("items_seq")))
                .addField(target("after")).build();
        var failure = assertThrows(IllegalArgumentException.class,
                () -> renderer.migrateSafelyPlan(current, target, List.of()));
        assertEquals("the configured dialect cannot alter the column comment safely", failure.getMessage());
    }

    private static ColumnMetadata current() {
        return ColumnMetadata.of("code", "VARCHAR").withLength(16).withComment("before");
    }

    private static DynamicField target(String comment) {
        return DynamicField.of("code", "VARCHAR").withLength(16).withComment(comment);
    }
}
