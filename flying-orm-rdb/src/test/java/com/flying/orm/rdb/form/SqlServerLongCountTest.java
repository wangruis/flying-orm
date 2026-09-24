package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlServerLongCountTest {

    @Test
    void pageCountUsesDatabaseLongAggregateOnlyForSqlServer() {
        DynamicForm form = form();
        for (RdbDialect dialect : dialects()) {
            FormDataSqlRenderer renderer = renderer(dialect);
            FormOperationPlanner planner = new FormOperationPlanner(renderer,
                    new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                    SqlExecutionOptions.safeDefaults());
            FormOperationPlanner.PlannedPage page = planner.page(QuerySpec.of(form, ConditionGroup.and().build()),
                    PageQuery.of(1, 10, PageSort.asc("id")));

            assertTrue(page.countRequest().sql().startsWith("select " + countFunction(dialect) + "(*) as total"),
                    page.countRequest().sql());
        }
        assertEquals(2_147_483_648L, CountResultReader.read(DynamicRow.copyOf(Map.of("total", 2_147_483_648L))));
    }

    @Test
    void joinPageCountUsesDatabaseLongAggregateOnlyForSqlServer() {
        DynamicForm form = form();
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
        JoinSource left = builder.root();
        JoinSource right = builder.join(JoinType.INNER, form, left, "id", "id");
        JoinQuerySpec spec = builder.select(left, "id").select(right, "id")
                .orderBy(left, "id", PageSort.Direction.ASC).build();
        for (RdbDialect dialect : dialects()) {
            FormDataSqlRenderer renderer = renderer(dialect);
            JoinQueryPlanner planner = new JoinQueryPlanner(renderer,
                    new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                    SqlExecutionOptions.safeDefaults());
            JoinQueryPlanner.PlannedJoinPage page = planner.page(spec, PageQuery.of(1, 10), null);

            assertTrue(page.countRequest().sql().startsWith("select " + countFunction(dialect) + "(*) as total"),
                    page.countRequest().sql());
        }
    }

    private static String countFunction(RdbDialect dialect) {
        return "sqlserver".equals(dialect.name()) ? "count_big" : "count";
    }

    private static List<RdbDialect> dialects() {
        return List.of(RdbDialect.sqlServer(), RdbDialect.h2(), RdbDialect.mysql(),
                RdbDialect.postgresql(), RdbDialect.oracle());
    }

    private static FormDataSqlRenderer renderer(RdbDialect dialect) {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), dialect);
    }

    private static DynamicForm form() {
        return DynamicForm.builder("orders", "orders")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
    }
}
