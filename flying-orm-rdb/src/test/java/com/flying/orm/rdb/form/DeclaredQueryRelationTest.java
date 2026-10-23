package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinFieldRef;
import com.flying.orm.core.join.QueryRelation;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.array.ArrayStructuredConditions;
import com.flying.orm.rdb.array.ArrayTermHandlers;
import com.flying.orm.rdb.json.JsonStructuredConditions;
import com.flying.orm.rdb.json.JsonTermHandlers;
import com.flying.orm.rdb.vector.VectorStructuredConditions;
import com.flying.orm.rdb.vector.VectorTermHandlers;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class DeclaredQueryRelationTest {
    private static final DynamicForm USERS = table("users", "id", "departmentId", "tenantId");
    private static final DynamicForm DEPARTMENTS = table("departments", "id", "tenantId", "name");

    @Test
    void composedExtensionsShareOneRelatedModelAndRenderItsQualifiedFields() {
        DynamicForm department = DynamicForm.builder("departments", "departments")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("payload", "JSON"))
                .addField(DynamicField.of("tags", "INTEGER[]"))
                .addField(DynamicField.of("embedding", "VECTOR").withLength(2)).build();
        AtomicInteger resolutions = new AtomicInteger();
        JoinQuerySpec.Builder query = JoinQuerySpec.builder(USERS).relation("department", QueryRelation.of(() -> {
            resolutions.incrementAndGet();
            return department;
        }, "departmentId", "id"));
        String value = "x' OR 1=1 --";
        StructuredConditionInput input = StructuredConditionInput.or(
                StructuredConditionInput.term("id", "eq", "7"),
                StructuredConditionInput.and(
                        StructuredConditionInput.term("department.payload", JsonStructuredConditions.JSON_PATH_EQUALS,
                                Map.of("path", "status", "value", value)),
                        StructuredConditionInput.term("department.tags", ArrayStructuredConditions.ANY_EQUALS, "2"),
                        StructuredConditionInput.term("department.embedding", VectorStructuredConditions.L2_LESS_THAN,
                                Map.of("vector", List.of(1, 2), "distance", 0.5))));
        StructuredConditionResolver resolver = StructuredConditionResolver.composite(
                JsonStructuredConditions.standard(), ArrayStructuredConditions.postgresql(),
                VectorStructuredConditions.postgresql());
        ConditionGroup compiled = resolver.compileFields(USERS, path -> {
            JoinFieldRef field = query.field(path);
            return field.source().form().field(field.field());
        }, input, StructuredConditionPolicy.defaults());
        JoinQuerySpec spec = query.selectAs("id", "id").where(compiled)
                .orderBy("id", com.flying.orm.core.page.PageSort.Direction.ASC).build();
        assertEquals(1, resolutions.get());
        assertEquals(1, spec.joins().size());
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms()
                .addTermPackage(JsonTermHandlers.postgresql()).addTermPackage(ArrayTermHandlers.postgresql())
                .addTermPackage(VectorTermHandlers.postgresql()).build(), RdbDialect.postgresql());
        FormReadPlanner.PlannedQuery planned = new JoinQueryPlanner(renderer,
                new FormScopeSupport(renderer, resolver, DataScope.none()), SqlExecutionOptions.unlimited())
                .page(spec, com.flying.orm.core.page.PageQuery.of(1, 5), null);
        String sql = planned.countRequest().sql();
        for (String field : List.of("payload", "tags", "embedding")) {
            assertTrue(sql.contains("\"t1\".\"" + field + "\""), sql);
        }
        assertTrue(sql.contains(" or "), sql);
        assertFalse(sql.contains(value), sql);
        assertEquals(7L, planned.countRequest().parameters().getFirst());
        assertArrayEquals(planned.countRequest().parameters().toArray(),
                planned.request().parameters().subList(0, planned.countRequest().parameters().size()).toArray());
        assertEquals(1, resolutions.get());
    }

    @Test
    void structuredRelationFieldsShareTypeBindingAndPagedSqlWithOrdinaryConditions() {
        JoinQuerySpec.Builder query = JoinQuerySpec.builder(USERS)
                .relation("department", QueryRelation.of(DEPARTMENTS, "departmentId", "id"));
        com.flying.orm.core.condition.StructuredConditionInput input =
                com.flying.orm.core.condition.StructuredConditionInput.or(
                        com.flying.orm.core.condition.StructuredConditionInput.term("id", "eq", "1"),
                        com.flying.orm.core.condition.StructuredConditionInput.and(
                                com.flying.orm.core.condition.StructuredConditionInput.term("department.id", "gt", "2"),
                                com.flying.orm.core.condition.StructuredConditionInput.term("department.name", "eq", "x' OR 1=1 --")));
        ConditionGroup compiled = StructuredConditionResolver.defaults().compileFields(USERS, path -> {
            var field = query.field(path);
            return field.source().form().field(field.field());
        }, input, com.flying.orm.core.condition.StructuredConditionPolicy.defaults());
        JoinQuerySpec structured = query.selectAs("id", "id").where(compiled)
                .orderBy("department.id", com.flying.orm.core.page.PageSort.Direction.ASC).build();
        JoinQuerySpec direct = JoinQuerySpec.builder(USERS)
                .relation("department", QueryRelation.of(DEPARTMENTS, "departmentId", "id"))
                .selectAs("id", "id")
                .where(ConditionGroup.or().where("id", "=", 1L)
                        .and(group -> group.where("department.id", ">", 2L)
                                .where("department.name", "=", "x' OR 1=1 --")).build())
                .orderBy("department.id", com.flying.orm.core.page.PageSort.Direction.ASC).build();
        FormReadPlanner.PlannedQuery actual = planner().page(structured, com.flying.orm.core.page.PageQuery.of(1, 5), null);
        FormReadPlanner.PlannedQuery expected = planner().page(direct, com.flying.orm.core.page.PageQuery.of(1, 5), null);
        assertEquals(expected.request(), actual.request());
        assertEquals(expected.countRequest(), actual.countRequest());
        assertEquals(List.of(1L, 2L, "x' OR 1=1 --"), actual.countRequest().parameters());
        assertFalse(actual.request().sql().contains("x' OR 1=1 --"));
    }

    @Test
    void qualifiedFilterUsesItsOwnSourceFieldScope() {
        JoinQuerySpec.Builder query = JoinQuerySpec.builder(USERS)
                .relation("department", QueryRelation.of(DEPARTMENTS, "departmentId", "id"));
        query.scope(query.related("department"), DataScope.none()
                .withFields(com.flying.orm.core.scope.FieldScope.readable("id", "tenantId")));
        JoinQuerySpec spec = query.select("id")
                .where(ConditionGroup.or().where("id", "=", 1L).where("department.name", "=", "hidden").build())
                .build();
        assertThrows(IllegalArgumentException.class, () -> planner().plan(spec, null));
    }

    @Test
    void mixedSourceConditionsKeepOrGroupingAndShareThePagedRelation() {
        JoinQuerySpec.Builder query = JoinQuerySpec.builder(USERS)
                .relation("department", QueryRelation.of(DEPARTMENTS, "departmentId", "id")
                        .where(ConditionGroup.and().where("tenantId", "=", 7L).build()));
        JoinQuerySpec spec = query.selectAs("id", "id")
                .where(ConditionGroup.or().where("id", "=", 1L)
                        .and(group -> group.where("department.name", "=", "x' OR 1=1 --")
                                .where("department.tenantId", "=", 9L)).build())
                .orderBy("department.name", com.flying.orm.core.page.PageSort.Direction.ASC)
                .build();
        FormReadPlanner.PlannedQuery page = planner().page(spec, com.flying.orm.core.page.PageQuery.of(1, 5), null);
        String count = page.countRequest().sql();
        assertTrue(count.contains("(\"t0\".\"id\" = ?) or ((\"t1\".\"name\" = ?) and (\"t1\".\"tenantId\" = ?))"), count);
        assertFalse(count.contains("x' OR 1=1 --"), count);
        assertEquals(List.of(7L, 1L, "x' OR 1=1 --", 9L), page.countRequest().parameters());
        assertEquals(page.countRequest().parameters(), page.request().parameters().subList(0, 4));
        assertTrue(page.request().sql().contains("order by \"t1\".\"name\" asc"));
        assertEquals(1, spec.joins().size());
    }

    @Test
    void modelOwnsReusableRelationsWithoutChangingPhysicalSchemaOrUnrelatedQueries() {
        AtomicInteger resolutions = new AtomicInteger();
        QueryRelation department = QueryRelation.of(() -> {
            resolutions.incrementAndGet();
            return DEPARTMENTS;
        }, "departmentId", "id").where(ConditionGroup.and().where("tenantId", "=", 7L).build());
        DynamicForm.Builder model = DynamicForm.builder("users", "users");
        USERS.fields().forEach(model::addField);
        DynamicForm users = model.relation("department", department).build();
        model.relation("another", QueryRelation.of(DEPARTMENTS, "departmentId", "id"));
        assertEquals(1, users.relations().size(), "published model must not share builder mutations");
        assertSame(department, users.relations().get("department"));
        assertThrows(UnsupportedOperationException.class, () -> users.relations().clear());
        assertEquals(USERS.structureFingerprint(), users.structureFingerprint());
        assertTrue(users.toTableDefinition().foreignKeys().isEmpty(), "query relations are not physical foreign keys");
        JoinQuerySpec plain = JoinQuerySpec.builder(users).select("id").build();
        assertTrue(plain.joins().isEmpty());
        assertEquals(0, resolutions.get());
        for (int query = 1; query <= 2; query++) {
            JoinQuerySpec related = JoinQuerySpec.builder(users)
                    .selectAs("department.name", "departmentName")
                    .selectAs("department.id", "departmentId").build();
            assertEquals(query, resolutions.get());
            assertEquals(1, related.joins().size());
            assertEquals(List.of(7L), planner().plan(related, null).request().parameters());
        }
    }

    @Test
    void queryLocalRelationOverrideDoesNotChangeModelOrOtherQueries() {
        DynamicForm.Builder model = DynamicForm.builder("users", "users");
        USERS.fields().forEach(model::addField);
        DynamicForm users = model.relation("department", QueryRelation.of(DEPARTMENTS, "departmentId", "id")
                .where(ConditionGroup.and().where("tenantId", "=", 7L).build())).build();
        JoinQuerySpec overridden = JoinQuerySpec.builder(users)
                .relation("department", QueryRelation.of(DEPARTMENTS, "departmentId", "id")
                        .where(ConditionGroup.and().where("tenantId", "=", 8L).build()))
                .selectAs("department.name", "name").build();
        JoinQuerySpec ordinary = JoinQuerySpec.builder(users).selectAs("department.name", "name").build();
        assertEquals(List.of(8L), planner().plan(overridden, null).request().parameters());
        assertEquals(List.of(7L), planner().plan(ordinary, null).request().parameters());
        JoinQuerySpec.Builder resolved = JoinQuerySpec.builder(users).selectAs("department.name", "name");
        assertThrows(IllegalStateException.class,
                () -> resolved.relation("department", QueryRelation.of(DEPARTMENTS, "tenantId", "tenantId")),
                "a relation already used by projections cannot silently keep the old JOIN after replacement");
        assertEquals(List.of(7L), planner().plan(resolved.build(), null).request().parameters());
    }

    @Test
    void relationColumnsJoinLazilyOnceAndKeepFixedConditionsInOn() {
        AtomicInteger resolutions = new AtomicInteger();
        QueryRelation department = QueryRelation.of(() -> {
            resolutions.incrementAndGet();
            return DEPARTMENTS;
        }, "departmentId", "id").where(ConditionGroup.and().where("tenantId", "=", 7L).build());
        JoinQuerySpec.Builder query = JoinQuerySpec.builder(USERS).relation("department", department);
        assertEquals(0, resolutions.get());
        query.selectAs("department.name", "departmentName").selectAs("department.id", "departmentId");
        JoinQuerySpec spec = query.build();
        assertEquals(1, resolutions.get());
        assertEquals(1, spec.joins().size());
        String sql = planner().plan(spec, null).request().sql();
        assertTrue(sql.contains("left outer join"), sql);
        assertTrue(sql.contains("\"departmentId\" = "), sql);
        assertTrue(sql.substring(sql.indexOf(" on ")).contains("\"tenantId\" = ?"), sql);
        assertEquals(List.of(7L), planner().plan(spec, null).request().parameters());
    }

    @Test
    void oneDeclarationProvidesCompositeExistsKeysAndKeepsInputOrGrouped() {
        QueryRelation department = QueryRelation.of(DEPARTMENTS, "departmentId", "id")
                .andKey("tenantId", "tenantId")
                .where(ConditionGroup.and().where("id", ">", 0L).build());
        JoinQuerySpec spec = department.query(ConditionGroup.or()
                .where("name", "=", "a").where("name", "=", "b").build());
        assertEquals(List.of("departmentId", "tenantId"), spec.projections().stream().map(p -> p.alias()).toList());
        assertEquals(List.of(0L, "a", "b"), planner().plan(spec, null).request().parameters());
        String sql = planner().plan(spec, null).request().sql();
        assertTrue(sql.contains(" and ("), sql);
        assertTrue(sql.contains(" or "), sql);
    }

    @Test
    void middleRelationsAndSelfRelationsReuseTheSameJoinGraph() {
        DynamicForm membership = table("memberships", "userId", "departmentId");
        QueryRelation departments = QueryRelation.of(membership, "id", "userId")
                .then(QueryRelation.of(DEPARTMENTS, "departmentId", "id"));
        JoinQuerySpec query = JoinQuerySpec.builder(USERS).relation("departments", departments)
                .selectAs("departments.name", "name").build();
        assertEquals(2, query.joins().size());
        assertSame(DEPARTMENTS, query.projections().getFirst().field().source().form());
        JoinQuerySpec exists = departments.query(ConditionGroup.and().where("name", "=", "team").build());
        assertEquals(1, exists.joins().size());
        assertEquals("id", exists.projections().getFirst().alias());
        assertEquals(List.of("team"), planner().plan(exists, null).request().parameters());
        String existsSql = planner().plan(departments.query(ConditionGroup.and().build()), null).request().sql();
        assertTrue(existsSql.contains("inner join"), "EXISTS must not match a dangling middle row: " + existsSql);
        JoinQuerySpec self = JoinQuerySpec.builder(USERS)
                .relation("parent", QueryRelation.of(USERS, "departmentId", "id"))
                .selectAs("parent.id", "parentId").build();
        assertEquals(2, self.sources().size());
        assertNotEquals(self.root(), self.projections().getFirst().field().source());
    }

    private static DynamicForm table(String table, String... names) {
        DynamicForm.Builder form = DynamicForm.builder(table, table);
        for (String name : names) form.addField(DynamicField.of(name, name.equals("name") ? "VARCHAR" : "BIGINT"));
        return form.build();
    }

    @Test
    void declaredRelationUsesTheExistingExistsRendererAndBindsUntrustedValues() {
        QueryRelation department = QueryRelation.of(DEPARTMENTS, "departmentId", "id")
                .andKey("tenantId", "tenantId").where(ConditionGroup.and().where("id", ">", 0L).build());
        FormDataSqlRenderer inner = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms()
                .addTerm(inner.relationExists("in-department", value -> department.query(
                        ConditionGroup.or().where("name", "=", value).where("name", "=", "b").build()))).build();
        FormDataSqlRenderer outer = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql());
        String input = "x' OR 1=1 --";
        com.flying.orm.core.sql.render.SqlRequest request = outer.select(USERS,
                conditions.conditions().where("departmentId", "in-department", input).build());
        assertEquals(List.of(0L, input, "b"), request.parameters());
        assertTrue(request.sql().contains("exists ("), request.sql());
        assertTrue(request.sql().contains("\"users\".\"departmentId\""), request.sql());
        assertTrue(request.sql().contains("\"users\".\"tenantId\""), request.sql());
        assertFalse(request.sql().contains(input));
    }

    private static JoinQueryPlanner planner() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        return new JoinQueryPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited());
    }
}
