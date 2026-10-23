package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import com.flying.orm.rdb.dialect.DialectCapabilityId;
import com.flying.orm.rdb.dialect.OracleVersion;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.dialect.SqlServerVersion;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FullJoinPlanTest {
    private static DynamicForm table(String name) {
        return DynamicForm.builder(name, name).addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("tenant_id", "BIGINT")).build();
    }

    private static JoinQuerySpec relation() {
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(table("left_rows"));
        JoinSource right = builder.join(JoinType.FULL, table("right_rows"), builder.root(), "id", "id");
        return builder.scope(builder.root(), DataScope.tenant("tenant_id", 7L))
                .scope(right, DataScope.tenant("tenant_id", 9L))
                .selectAs(builder.root(), "id", "left_id").selectAs(right, "id", "right_id")
                .orderBy(builder.root(), "id", PageSort.Direction.ASC).build();
    }

    private static FormDataSqlRenderer renderer(RdbDialect dialect) {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), dialect);
    }

    private static JoinQueryPlanner planner(RdbDialect dialect) {
        FormDataSqlRenderer renderer = renderer(dialect);
        return new JoinQueryPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited());
    }

    @Test
    void supportedDialectsKeepBothSourceScopesInsideRelationsForCountAndPage() {
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.oracle(), RdbDialect.sqlServer(),
                RdbDialect.oracle(OracleVersion.V12C), RdbDialect.sqlServer(SqlServerVersion.V2012))) {
            assertTrue(dialect.capabilities().supports(DialectCapabilityId.FULL_OUTER_JOIN));
            FormReadPlanner.PlannedQuery page = planner(dialect).page(relation(), PageQuery.of(2, 10), null);
            String count = page.countRequest().sql();
            String data = page.request().sql();
            assertTrue(count.contains("full outer join (select * from"), count);
            assertTrue(data.contains(count.substring(count.indexOf(" from "))), data);
            assertEquals(2, count.split(" where ", -1).length - 1, count);
            assertFalse(count.substring(count.lastIndexOf(" on ")).contains(" where "), count);
            assertEquals(List.of(7L, 9L), page.countRequest().parameters());
            assertEquals(List.of(7L, 9L), page.request().parameters().subList(0, 2));
            assertEquals(4, page.request().parameters().size());
            assertFalse(count.contains("union"), count);
        }
    }

    @Test
    void unsupportedAndUndeclaredDialectsRejectFullJoinInsteadOfRewritingIt() {
        RdbDialect base = RdbDialect.postgresql();
        RdbDialect undeclared = RdbDialect.of("custom", base.schema(), base.pagination(), base.upsert());
        for (RdbDialect dialect : List.of(RdbDialect.mysql(), RdbDialect.h2(), undeclared)) {
            assertFalse(dialect.capabilities().supports(DialectCapabilityId.FULL_OUTER_JOIN));
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> planner(dialect).page(relation(), PageQuery.of(1, 10), null));
            assertTrue(error.getMessage().contains("FULL OUTER JOIN"), error.getMessage());
            FormAggregatePlanner aggregate = new FormAggregatePlanner(renderer(dialect),
                new FormScopeSupport(renderer(dialect), StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults());
            assertThrows(IllegalArgumentException.class, () -> aggregate.plan(
                    AggregateSpec.builder(relation()).aggregate(AggregateExpression.countRows("rows")).build()));
        }
    }

    @Test
    void explicitCustomCapabilityUsesTheExistingSqlAndAggregatePaths() {
        RdbDialect base = RdbDialect.postgresql();
        RdbDialect dialect = RdbDialect.of("custom", base.schema(), base.pagination(), base.upsert(),
                base.json(), "application-selected", DialectCapabilities.of(DialectCapabilityId.FULL_OUTER_JOIN), 0);
        FormAggregatePlanner planner = new FormAggregatePlanner(renderer(dialect),
                new FormScopeSupport(renderer(dialect), StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults());
        AggregateSpec report = AggregateSpec.builder(relation()).aggregate(AggregateExpression.countRows("rows")).build();
        FormAggregatePlanner.Plan plan = planner.plan(report);
        assertTrue(plan.request().sql().contains("full outer join"), plan.request().sql());
        assertEquals(List.of(7L, 9L), plan.request().parameters());
    }
}
