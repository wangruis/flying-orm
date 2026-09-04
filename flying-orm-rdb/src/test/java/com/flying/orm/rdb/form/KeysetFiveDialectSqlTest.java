package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPosition;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class KeysetFiveDialectSqlTest {

    @Test
    void subsequentPagePreservesDeclaredColumnSpellingInEveryPredicate() {
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.of("Score", "INTEGER"))
                .addField(DynamicField.primaryKey("Id", "BIGINT")).build();
        QuerySpec query = QuerySpec.of(form, ConditionGroup.and().build());
        KeysetPageQuery page = KeysetPageQuery.after(
                20, CursorPosition.of(List.of(5, 7L)), KeysetSort.asc("score", NullOrder.LAST));

        for (RdbDialect dialect : List.of(
                RdbDialect.h2(), RdbDialect.mysql(), RdbDialect.postgresql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormOperationPlanner planner = planner(dialect);
            String sql = planner.keysetPage(query, page).request().sql();
            SqlRenderer identifiers = planner.renderer.conditionRenderer();
            String score = identifiers.identifier("Score");
            String id = identifiers.identifier("Id");
            assertTrue(sql.contains(score + " > ?"), dialect.name() + ": " + sql);
            assertTrue(sql.contains(score + " = ?"), dialect.name() + ": " + sql);
            assertTrue(sql.contains(id + " > ?"), dialect.name() + ": " + sql);
            assertFalse(sql.contains(identifiers.identifier("score") + " > ?"), sql);
            assertFalse(sql.contains(identifiers.identifier("id") + " > ?"), sql);
        }
    }

    @Test
    void rendersTheSamePortableNullableShapeForAllBuiltInDialects() {
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.of("payload", "VARCHAR"))
                .addField(DynamicField.of("score", "INTEGER"))
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .build();
        QuerySpec query = QuerySpec.of(form, ConditionGroup.and().build())
                                   .withProjection(List.of("payload"), List.of());
        KeysetPageQuery page = KeysetPageQuery.after(
                20,
                CursorPosition.of(Arrays.asList(null, 7L)),
                KeysetSort.asc("score", NullOrder.LAST));

        for (RdbDialect dialect : List.of(
                RdbDialect.h2(), RdbDialect.mysql(), RdbDialect.postgresql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormOperationPlanner.PlannedKeysetPage plan = planner(dialect).keysetPage(query, page);
            String sql = plan.request().sql().toLowerCase(Locale.ROOT);

            assertTrue(sql.contains("case when"), dialect.name());
            assertTrue(sql.contains("is null then 1 else 0 end asc"), dialect.name());
            assertTrue(sql.contains("__fo_ks_0"), dialect.name());
            assertTrue(sql.contains("__fo_ks_1"), dialect.name());
            assertTrue(sql.contains("is null") && sql.contains("id"), dialect.name());
            assertTrue(plan.request().parameters().contains(7L), dialect.name());
        }
    }

    @Test
    void cachedSqlShapeSeparatesNullAndNonNullCursorPositions() {
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.of("score", "INTEGER"))
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .build();
        QuerySpec query = QuerySpec.of(form, ConditionGroup.and().build());
        FormOperationPlanner planner = planner(RdbDialect.h2());

        String nonNullSql = planner.keysetPage(
                query,
                KeysetPageQuery.after(
                        20,
                        CursorPosition.of(List.of(5, 7L)),
                        KeysetSort.asc("score", NullOrder.LAST)))
                .request().sql();
        String nullSql = planner.keysetPage(
                query,
                KeysetPageQuery.after(
                        20,
                        CursorPosition.of(Arrays.asList(null, 7L)),
                        KeysetSort.asc("score", NullOrder.LAST)))
                .request().sql();

        assertNotEquals(nonNullSql, nullSql);
        assertTrue(nullSql.toUpperCase(Locale.ROOT).contains(" IS NULL AND "));
    }

    private static FormOperationPlanner planner(RdbDialect dialect) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), dialect);
        return new FormOperationPlanner(
                renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.safeDefaults());
    }
}
