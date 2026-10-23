package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.TermExtensionDescriptor;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUseRequirements;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class JoinRelationPlanningTest {
    @Test
    void sourcePreparationHasNoProjectionOrSortResponsibility() {
        Fixture fixture = fixture();
        FieldUseRequirements.Builder uses = FieldUseRequirements.builder();
        JoinQueryPlanner.PreparedSources sources = fixture.planner.prepareSources(fixture.spec, uses,
                new QueryShapeBudget(QueryShapeLimits.defaults()));
        assertEquals(fixture.spec.sources().size(), sources.physicalForms().size());
        assertEquals(fixture.spec.sources().size(), sources.scopes().size());
        List<FieldUseRequirements.JoinRequirement> requirements = uses.build().joinRequirements();
        assertTrue(requirements.stream().anyMatch(use -> use.use() == FieldUse.JOIN));
        assertTrue(requirements.stream().anyMatch(use -> use.use() == FieldUse.FILTER));
        assertFalse(requirements.stream().anyMatch(use -> use.use() == FieldUse.PROJECT || use.use() == FieldUse.SORT));
        assertEquals(0, fixture.renders.get(), "source preparation must not render SQL");
    }

    @Test
    void pageRendersTheSharedRelationOnlyOnce() {
        Fixture fixture = fixture();
        FormReadPlanner.PlannedQuery page = fixture.planner.page(fixture.spec, PageQuery.of(2, 10), null);
        assertEquals(1, fixture.renders.get(), "count and data must share the already rendered business predicate");
        assertPage(page);
    }

    @Test
    void governedPageRendersTheSharedRelationOnlyOnce() {
        Fixture fixture = fixture();
        FormReadPlanner.PlannedQuery page = fixture.planner.pageGoverned(fixture.spec, PageQuery.of(2, 10), null,
                FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults()).plan();
        assertEquals(1, fixture.renders.get());
        assertPage(page);
    }

    private static void assertPage(FormReadPlanner.PlannedQuery page) {
        String relation = " from \"users\" \"t0\" left outer join (select * from \"orders\" where \"status\" = ?) \"t1\""
                + " on \"t0\".\"id\" = \"t1\".\"user_id\" where ((\"t0\".\"name\" = ?))";
        assertEquals("select count(*) as total" + relation, page.countRequest().sql());
        assertEquals("select \"t0\".\"id\" as \"user_id\"" + relation
                + " order by \"t0\".\"id\" asc limit ? offset ?", page.request().sql());
        assertEquals(List.of("paid", "Ada"), page.countRequest().parameters());
        assertEquals(List.of("paid", "Ada", 10, 10L), page.request().parameters());
    }

    private static Fixture fixture() {
        AtomicInteger renders = new AtomicInteger();
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms()
                .addTerm(SqlTermHandler.of(TermExtensionDescriptor.filter("observed-eq", Set.of(), Integer.MAX_VALUE, 1),
                        ConditionValueShape.SCALAR, (term, context) -> {
                            renders.incrementAndGet();
                            return new SqlFragment(context.identifier(term.field()) + " = ?", List.of(term.value()));
                        })).build();
        DynamicForm users = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR")).build();
        DynamicForm orders = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.of("user_id", "BIGINT"))
                .addField(DynamicField.of("status", "VARCHAR")).build();
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(users);
        JoinSource order = builder.join(JoinType.LEFT, orders, builder.root(), "id", "user_id");
        builder.scope(order, DataScope.where(ConditionGroup.and().where("status", "=", "paid").build()));
        JoinQuerySpec spec = builder.selectAs(builder.root(), "id", "user_id")
                .where(builder.root(), ConditionGroup.and(conditions.terms()).where("name", "observed-eq", "Ada").build())
                .orderBy(builder.root(), "id", PageSort.Direction.ASC).build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql());
        JoinQueryPlanner planner = new JoinQueryPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited());
        return new Fixture(planner, spec, renders);
    }

    private record Fixture(JoinQueryPlanner planner, JoinQuerySpec spec, AtomicInteger renders) { }
}
