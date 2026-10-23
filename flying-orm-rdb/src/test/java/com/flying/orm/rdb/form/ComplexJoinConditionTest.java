package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.TermExtensionDescriptor;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinCondition;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.GroupSelection;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ComplexJoinConditionTest {
    private static final DynamicForm USERS = DynamicForm.builder("users", "users")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("minimum", "INTEGER")).build();
    private static final DynamicForm ORDERS = DynamicForm.builder("orders", "orders")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("user_id", "BIGINT"))
            .addField(DynamicField.of("status", "VARCHAR"))
            .addField(DynamicField.of("quantity", "INTEGER")).build();

    @Test
    void equalityShortcutsAndStructuredConditionsUseTheSamePlanWithoutMutatingSnapshots() {
        JoinQuerySpec.Builder shortcut = JoinQuerySpec.builder(USERS);
        JoinSource order = shortcut.join(JoinType.LEFT, ORDERS, shortcut.root(), "id", "user_id");
        shortcut.select(shortcut.root(), "id");
        JoinQuerySpec snapshot = shortcut.build();
        shortcut.andOn(order, shortcut.root(), "minimum", "quantity");
        shortcut.andOn(order, JoinCondition.where(order,
                ConditionGroup.and().where("status", "=", "paid").build()));

        JoinQuerySpec.Builder structured = JoinQuerySpec.builder(USERS);
        JoinSource joined = structured.join(JoinType.LEFT, ORDERS, source -> JoinCondition.and(
                JoinCondition.compare(structured.root().field("id"), "=", source.field("user_id")),
                JoinCondition.compare(structured.root().field("minimum"), "=", source.field("quantity"))));
        structured.andOn(joined, JoinCondition.where(joined,
                ConditionGroup.and().where("status", "=", "paid").build()));
        structured.select(structured.root(), "id");

        JoinQueryPlanner planner = planner(SqlRenderer.builder().addDefaultTerms().build());
        SqlRequest simplified = planner.plan(shortcut.build(), null).request();
        SqlRequest explicit = planner.plan(structured.build(), null).request();
        assertEquals(explicit.sql(), simplified.sql());
        assertEquals(List.of("paid"), simplified.parameters());
        assertEquals(explicit.parameters(), simplified.parameters());
        SqlRequest original = planner.plan(snapshot, null).request();
        assertFalse(original.sql().contains("\"quantity\""));
        assertTrue(original.parameters().isEmpty());
    }

    @Test
    void complexOnPreservesUnmatchedRowsAndNullComparisonSemantics() throws Exception {
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(USERS);
        JoinSource order = builder.join(JoinType.LEFT, ORDERS, joined -> JoinCondition.and(
                JoinCondition.compare(builder.root().field("id"), "=", joined.field("user_id")),
                JoinCondition.compare(joined.field("quantity"), ">=", builder.root().field("minimum")),
                JoinCondition.where(joined, ConditionGroup.and().where("status", "=", "paid").build())));
        JoinQuerySpec spec = builder.selectAs(builder.root(), "id", "uid").selectAs(order, "id", "oid")
                .orderBy(builder.root(), "id", PageSort.Direction.ASC).build();
        SqlRequest request = planner(SqlRenderer.builder().addDefaultTerms().build()).plan(spec, null).request();
        assertEquals(List.of("paid"), request.parameters());
        assertTrue(request.sql().contains("\"t1\".\"quantity\" >= \"t0\".\"minimum\""));
        try (Connection connection = DriverManager.getConnection(
                "jdbc:h2:mem:complex_on;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE");
             Statement statement = connection.createStatement()) {
            statement.execute("create table users(id bigint primary key, minimum integer)");
            statement.execute("create table orders(id bigint primary key, user_id bigint, status varchar(20), quantity integer)");
            statement.execute("insert into users values(1,5),(2,3),(3,1),(4,null)");
            statement.execute("insert into orders values(10,1,'paid',2),(11,1,'paid',8),(12,2,'pending',10),(13,4,'paid',10)");
            List<String> rows = new ArrayList<>();
            try (PreparedStatement query = connection.prepareStatement(request.sql())) {
                query.setString(1, "paid");
                try (ResultSet result = query.executeQuery()) {
                    while (result.next()) rows.add(result.getObject("uid") + ":" + result.getObject("oid"));
                }
            }
            assertEquals(List.of("1:11", "2:null", "3:null", "4:null"), rows);
        }
    }

    @Test
    void nestedPredicateAndEqualityKeysKeepPrecedenceAndSnapshotValues() {
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(USERS);
        JoinSource order = builder.join(JoinType.LEFT, ORDERS, builder.root(), "id", "user_id");
        List<String> statuses = new ArrayList<>(List.of("paid", "pending"));
        builder.andOn(order, JoinCondition.or(
                JoinCondition.compare(order.field("quantity"), "<", builder.root().field("minimum")),
                JoinCondition.where(order, ConditionGroup.and().where("status", "in", statuses).build())));
        statuses.clear();
        SqlRequest request = planner(SqlRenderer.builder().addDefaultTerms().build()).plan(
                builder.select(builder.root(), "id").build(), null).request();
        assertTrue(request.sql().contains("on (\"t0\".\"id\" = \"t1\".\"user_id\" and ("), request.sql());
        assertTrue(request.sql().contains(" or "), request.sql());
        assertEquals(List.of("paid", "pending"), request.parameters());
    }

    @Test
    void allComparisonOperatorsAreStructuredAndInvalidSqlIsRejected() {
        for (String operator : List.of("=", "<>", ">", ">=", "<", "<=")) {
            JoinQuerySpec.Builder builder = JoinQuerySpec.builder(USERS);
            builder.join(JoinType.INNER, ORDERS,
                    joined -> JoinCondition.compare(builder.root().field("minimum"), operator, joined.field("quantity")));
            SqlRequest request = planner(SqlRenderer.builder().addDefaultTerms().build()).plan(builder.select(builder.root(), "id").build(), null).request();
            assertTrue(request.sql().contains("\"minimum\" " + operator + " \"t1\".\"quantity\""));
            assertTrue(request.parameters().isEmpty());
        }
        JoinSource root = JoinQuerySpec.builder(USERS).root();
        assertThrows(IllegalArgumentException.class, () -> JoinCondition.compare(root.field("id"), "= 1 or 1=", root.field("id")));
    }

    @Test
    void foreignAndFutureSourcesCannotEnterOnAndFailedJoinDoesNotPublishSource() {
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(USERS);
        JoinSource foreign = new JoinSource(0, ORDERS);
        assertThrows(IllegalArgumentException.class, () -> builder.join(JoinType.LEFT, ORDERS,
                joined -> JoinCondition.compare(foreign.field("id"), "=", joined.field("user_id"))));
        JoinSource joined = builder.join(JoinType.LEFT, ORDERS,
                source -> JoinCondition.compare(builder.root().field("id"), "=", source.field("user_id")));
        assertEquals(1, joined.ordinal());
        assertThrows(IllegalArgumentException.class, () -> builder.andOn(joined,
                JoinCondition.compare(new JoinSource(2, ORDERS).field("id"), "=", joined.field("id"))));
    }

    @Test
    void joinDescriptorsRemainDistinctFromFilterAndPageRendersOnOnce() {
        AtomicInteger renders = new AtomicInteger();
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms().addTerm(SqlTermHandler.of(
                TermExtensionDescriptor.join("on-status", Set.of(), 1, 1), ConditionValueShape.SCALAR,
                (term, context) -> {
                    renders.incrementAndGet();
                    return new SqlFragment(context.identifier(term.field()) + " = ?", List.of(term.value()));
                })).build();
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(USERS);
        JoinSource order = builder.join(JoinType.LEFT, ORDERS, builder.root(), "id", "user_id");
        builder.andOn(order, JoinCondition.where(order,
                ConditionGroup.and(conditions.terms()).where("status", "on-status", "paid").build()));
        builder.scope(builder.root(), DataScope.where(ConditionGroup.and().where("id", ">", 0L).build()));
        builder.scope(order, DataScope.where(ConditionGroup.and().where("quantity", ">", 0).build()));
        JoinQuerySpec spec = builder.select(builder.root(), "id")
                .where(builder.root(), ConditionGroup.and().where("minimum", ">", 1).build())
                .orderBy(builder.root(), "id", PageSort.Direction.ASC).build();
        FormReadPlanner.PlannedQuery page = planner(conditions).pageGoverned(spec, PageQuery.of(1, 5), null,
                FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults()).plan();
        assertEquals(1, renders.get());
        assertEquals(List.of(0L, 0, "paid", 1), page.countRequest().parameters());
        assertEquals(List.of(0L, 0, "paid", 1, 5, 0L), page.request().parameters());
        assertTrue(page.request().sql().contains("left outer join (select * from"));
        assertEquals(FieldUse.JOIN, conditions.terms().find("on-status").orElseThrow().descriptor().orElseThrow().fieldUse());
    }

    private static JoinQueryPlanner planner(SqlRenderer conditions) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql());
        return new JoinQueryPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited());
    }

    @Test
    void onUsesCannotBypassFieldScopeOrJoinPolicy() {
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(USERS);
        JoinSource order = builder.join(JoinType.LEFT, ORDERS, joined ->
                JoinCondition.compare(builder.root().field("minimum"), "<", joined.field("quantity")));
        JoinQuerySpec spec = builder.select(builder.root(), "id").build();
        JoinQueryPlanner planner = planner(SqlRenderer.builder().addDefaultTerms().build());
        FieldUsePolicy denied = FieldUsePolicy.builder().joinVisibility(builder.root().field("id"), FieldVisibility.FULL)
                .allowJoin(order.field("quantity"), FieldUse.JOIN).build();
        assertThrows(ScopeAccessException.class, () -> planner.planGoverned(spec, null, denied, QueryShapeLimits.defaults()));
        builder.scope(builder.root(), DataScope.none().withFields(FieldScope.readable("id")));
        assertThrows(IllegalArgumentException.class, () -> planner.plan(builder.build(), null));
    }

    @Test
    void filterExtensionDoesNotAcquireJoinPermission() {
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().addTerm(SqlTermHandler.of(
                TermExtensionDescriptor.filter("filter-only", Set.of(), 1, 1), ConditionValueShape.SCALAR,
                (term, context) -> new SqlFragment(context.identifier(term.field()) + " = ?", List.of(term.value())))).build();
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(USERS);
        builder.join(JoinType.LEFT, ORDERS, joined -> JoinCondition.where(joined,
                ConditionGroup.and(renderer.terms()).where("status", "filter-only", "paid").build()));
        JoinQuerySpec spec = builder.select(builder.root(), "id").build();
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> planner(renderer)
                .planGoverned(spec, null, FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults()));
        assertTrue(failure.getMessage().contains("not allowed for JOIN"));
    }

    @Test
    void protectedOnValuesUseTheExistingPhysicalPreparation() {
        DynamicForm accounts = DynamicForm.builder("accounts", "accounts")
                .addField(DynamicField.of("user_id", "BIGINT"))
                .addField(DynamicField.of("phone", "VARCHAR"))
                .encrypted("phone", EncryptedFieldDefinition.builder().build()).build();
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(USERS);
        JoinSource account = builder.join(JoinType.LEFT, accounts, joined -> JoinCondition.and(
                JoinCondition.compare(builder.root().field("id"), "=", joined.field("user_id")),
                JoinCondition.where(joined, ConditionGroup.and().add(ProtectedConditions.exact("phone", "123456")).build())));
        JoinQuerySpec spec = builder.select(builder.root(), "id").build();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(),
                    RdbDialect.postgresql()).withProtectedFields(runtime);
            JoinQueryPlanner planner = new JoinQueryPlanner(renderer,
                    new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()), SqlExecutionOptions.unlimited());
            SqlRequest request = planner.planGoverned(spec, null, FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults()).plan().request();
            assertFalse(request.sql().contains(ProtectedConditions.EXACT));
            assertFalse(request.sql().contains("123456"));
            assertFalse(request.parameters().contains("123456"));
            assertInstanceOf(byte[].class, request.parameters().getFirst());
            builder.andOn(account, JoinCondition.compare(account.field("phone"), "=", account.field("phone")));
            assertThrows(IllegalArgumentException.class, () -> planner.plan(builder.build(), null));
        }
    }

    @Test
    void aggregatePageKeepsTheSameOnAndBindOrderBeforeHaving() {
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms().build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql());
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(USERS);
        JoinSource joined = builder.join(JoinType.LEFT, ORDERS, source -> JoinCondition.and(
                JoinCondition.compare(builder.root().field("id"), "=", source.field("user_id")),
                JoinCondition.where(source, ConditionGroup.and().where("status", "=", "paid").build())));
        AggregateSpec report = AggregateSpec.builder(builder.buildRelation()).group(GroupSelection.of("id", "uid"))
                .aggregate(joined, AggregateExpression.count("id", "orders"))
                .having(ConditionGroup.and().where("orders", ">", 0L).build())
                .orderBy(PageSort.asc("uid")).build();
        FormAggregatePlanner.Plan plan = new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults())
                .planPage(report, PageQuery.of(2, 10), true);
        assertEquals(List.of("paid", 0L), plan.countRequest().orElseThrow().parameters());
        assertEquals(List.of("paid", 0L, 10, 10L), plan.request().parameters());
        assertTrue(plan.request().sql().contains("on (\"t0\".\"id\" = \"t1\".\"user_id\" and (\"t1\".\"status\" = ?))"));
        assertTrue(plan.countRequest().orElseThrow().sql().contains(" having "));
    }

    @Test
    void deepGroupsRemainIterativeAndOnlyConfiguredBudgetsRestrictRequests() {
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(USERS);
        builder.join(JoinType.INNER, ORDERS, joined -> {
            JoinCondition nested = JoinCondition.compare(builder.root().field("id"), "=", joined.field("user_id"));
            for (int depth = 0; depth < 4000; depth++) nested = JoinCondition.and(nested);
            return nested;
        });
        JoinQuerySpec spec = builder.select(builder.root(), "id").build();
        JoinQueryPlanner planner = planner(SqlRenderer.builder().addDefaultTerms().build());
        assertDoesNotThrow(() -> planner.plan(spec, null));
        assertThrows(IllegalArgumentException.class, () -> planner.planGoverned(spec, null,
                FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults().withMaxSqlLength(100)));
    }

    @Test
    void threeSourcesAndSelfJoinPreserveIdentityAndParameterOrder() {
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(USERS);
        JoinSource manager = builder.join(JoinType.LEFT, USERS, joined -> JoinCondition.and(
                JoinCondition.compare(builder.root().field("id"), "<", joined.field("id")),
                JoinCondition.where(joined, ConditionGroup.and().where("minimum", ">", 2).build())));
        JoinSource order = builder.join(JoinType.LEFT, ORDERS, joined -> JoinCondition.compare(manager.field("id"), "=", joined.field("user_id")));
        builder.andOn(order, builder.root(), "id", "id");
        JoinQuerySpec spec = builder.selectAs(manager, "id", "manager").selectAs(order, "id", "order_id").build();
        SqlRequest request = planner(SqlRenderer.builder().addDefaultTerms().build()).plan(spec, null).request();
        assertTrue(request.sql().contains("\"t1\".\"id\" = \"t2\".\"user_id\""));
        assertTrue(request.sql().contains("\"t0\".\"id\" = \"t2\".\"id\""));
        assertEquals(List.of(2), request.parameters());
    }

    @Test
    void onFieldCodecRunsOnceForBothPageRequests() {
        AtomicInteger writes = new AtomicInteger();
        com.flying.orm.core.codec.ValueCodec codec = new com.flying.orm.core.codec.ValueCodec() {
            public boolean supports(Class<?> type) { return type == Status.class; }
            public Object write(Object value) { writes.incrementAndGet(); return ((Status) value).value(); }
            public Object read(Object value, Class<?> type) { return new Status((String) value); }
        };
        com.flying.orm.rdb.mapping.EntityTypeMappingRegistry.Mapping mapping =
                com.flying.orm.rdb.mapping.EntityTypeMappingRegistry.builder()
                        .register("status", Status.class, com.flying.orm.core.type.DatabaseType.of("VARCHAR"), codec)
                        .build().resolve(Status.class);
        java.util.Map<DynamicField, com.flying.orm.rdb.mapping.EntityTypeMappingRegistry.Mapping> fields = new java.util.IdentityHashMap<>();
        fields.put(ORDERS.field("status"), mapping);
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(),
                RdbDialect.postgresql()).withEntityFieldCodecs(fields);
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(USERS);
        String value = "paid' OR 1=1 --";
        builder.join(JoinType.LEFT, ORDERS, joined -> JoinCondition.and(
                JoinCondition.compare(builder.root().field("id"), "=", joined.field("user_id")),
                JoinCondition.where(joined, ConditionGroup.and().where("status", "=", new Status(value)).build())));
        JoinQuerySpec spec = builder.select(builder.root(), "id").orderBy(builder.root(), "id", PageSort.Direction.ASC).build();
        JoinQueryPlanner planner = new JoinQueryPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()), SqlExecutionOptions.unlimited());
        FormReadPlanner.PlannedQuery page = planner.page(spec, PageQuery.of(1, 10), null);
        assertEquals(1, writes.get());
        assertEquals(List.of(value), page.countRequest().parameters());
        assertFalse(page.countRequest().sql().contains(value));
        assertEquals(value, page.request().parameters().getFirst());
    }

    @Test
    void explicitEmptyValuePredicateIsTrueWithoutDroppingOtherConditions() {
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(USERS);
        builder.join(JoinType.LEFT, ORDERS, joined -> JoinCondition.and(
                JoinCondition.compare(builder.root().field("id"), "=", joined.field("user_id")),
                JoinCondition.where(joined, ConditionGroup.and().build())));
        SqlRequest request = planner(SqlRenderer.builder().addDefaultTerms().build())
                .plan(builder.select(builder.root(), "id").build(), null).request();
        assertTrue(request.sql().contains("on (\"t0\".\"id\" = \"t1\".\"user_id\" and (1 = 1))"));
        assertThrows(IllegalArgumentException.class, JoinCondition::or);
    }

    private record Status(String value) { }
}
