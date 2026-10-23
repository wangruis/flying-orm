package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.CursorPosition;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class RelatedQueryTermTest {
    private static final DynamicForm ORDERS = DynamicForm.builder("orders", "orders")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("tenant_id", "BIGINT"))
            .addField(DynamicField.of("name", "VARCHAR")).build();
    private static final DynamicForm ITEMS = DynamicForm.builder("items", "items")
            .addField(DynamicField.of("order_id", "BIGINT"))
            .addField(DynamicField.of("tenant_id", "BIGINT"))
            .addField(DynamicField.of("amount", "INTEGER"))
            .addField(DynamicField.of("status", "VARCHAR"))
            .addField(DynamicField.of("category_id", "BIGINT")).build();
    private static final DynamicForm CATEGORIES = DynamicForm.builder("categories", "categories")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("name", "VARCHAR")).build();

    private static JoinQuerySpec related(Object minimum) {
        JoinQuerySpec.Builder query = JoinQuerySpec.builder(ITEMS);
        JoinSource item = query.root();
        JoinSource category = query.join(JoinType.INNER, CATEGORIES, item, "category_id", "id");
        return query.selectAs(item, "order_id", "id").selectAs(item, "tenant_id", "tenant_id")
                .where(item, ConditionGroup.and().where("amount", ">=", minimum)
                        .add(ConditionGroup.or().where("status", "=", "A")
                                .where("status", "=", "B").build()).build())
                .where(category, ConditionGroup.and().where("name", "=", "x' OR 1=1 --").build())
                .scope(item, DataScope.where(ConditionGroup.and().where("tenant_id", "=", 7L).build()))
                .build();
    }

    @Test
    void compositeKeysAndNestedPredicatesStayInsideOneExistsAcrossCrud() {
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormDataSqlRenderer inner = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), dialect);
            SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms()
                    .addTerm(inner.relationExists("eligible", RelatedQueryTermTest::related))
                    .addTerm(inner.relationNotExists("ineligible", RelatedQueryTermTest::related)).build();
            FormDataSqlRenderer outer = FormDataSqlRenderer.create(conditions, dialect);
            for (String term : List.of("eligible", "ineligible")) {
                ConditionGroup where = conditions.conditions().where("id", term, 10).build();
                List<SqlRequest> requests = List.of(outer.select(ORDERS, where), outer.count(ORDERS, where),
                        outer.select(ORDERS, where, PageQuery.of(1, 10, PageSort.asc("id"))),
                        outer.update(ORDERS, Map.of("name", "selected"), where), outer.delete(ORDERS, where));
                for (SqlRequest request : requests) {
                    String sql = request.sql();
                    assertEquals(1, sql.split("exists \\(", -1).length - 1, sql);
                    assertTrue(sql.contains(dialect.schema().identifier("orders") + "."
                            + dialect.schema().identifier("id")), sql);
                    assertTrue(sql.contains(dialect.schema().identifier("orders") + "."
                            + dialect.schema().identifier("tenant_id")), sql);
                    assertTrue(sql.contains("inner join"), sql);
                    assertFalse(sql.contains("x' OR 1=1 --"), sql);
                    assertTrue(sql.contains(" or "), sql);
                    List<Object> parameters = request.parameters();
                    int start = parameters.indexOf(7L);
                    assertTrue(start >= 0, parameters.toString());
                    assertEquals(List.of(7L, 10, "A", "B", "x' OR 1=1 --"),
                            parameters.subList(start, start + 5));
                    assertEquals(term.equals("ineligible"), sql.contains("not exists"));
                }
            }
        }
    }

    @Test
    void outerJoinAliasesAreNotCapturedByInnerSources() {
        RdbDialect dialect = RdbDialect.postgresql();
        FormDataSqlRenderer inner = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), dialect);
        SqlTermHandler handler = inner.relationExists("eligible", RelatedQueryTermTest::related);
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms().addTerm(handler).build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, dialect);
        JoinQuerySpec.Builder query = JoinQuerySpec.builder(ORDERS);
        query.select(query.root(), "id").where(query.root(),
                conditions.conditions().where("id", "eligible", 10).build());
        JoinQueryPlanner planner = new JoinQueryPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited());
        String sql = planner.plan(query.build(), null).request().sql();
        assertTrue(sql.contains("\"flying_relation\".\"id\" = \"t0\".\"id\""), sql);
        assertTrue(sql.contains("\"flying_relation\".\"tenant_id\" = \"t0\".\"tenant_id\""), sql);
        assertThrows(IllegalArgumentException.class, () -> conditions.renderWhere(
                conditions.conditions().where("id", "eligible", 10).build()));
    }

    @Test
    void sameRelatedRowMustMatchWithoutMultiplyingCountOrPage() throws Exception {
        String url = System.getProperty("related.jdbcUrl", "jdbc:h2:mem:related_query;DATABASE_TO_LOWER=TRUE");
        boolean postgres = url.startsWith("jdbc:postgresql:");
        RdbDialect dialect = postgres ? RdbDialect.postgresql() : RdbDialect.h2();
        FormDataSqlRenderer inner = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), dialect);
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms()
                .addTerm(inner.relationExists("eligible", RelatedQueryTermTest::related))
                .addTerm(inner.relationNotExists("ineligible", RelatedQueryTermTest::related)).build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, dialect);
        try (Connection connection = postgres
                ? DriverManager.getConnection(url, "postgres", System.getenv("FLYING_ORM_PROBE_PASSWORD"))
                : DriverManager.getConnection(url); Statement setup = connection.createStatement()) {
            setup.execute("create temporary table orders(id bigint, tenant_id bigint, name varchar(40))");
            setup.execute("create temporary table items(order_id bigint, tenant_id bigint, amount integer, "
                    + "status varchar(10), category_id bigint)");
            setup.execute("create temporary table categories(id bigint, name varchar(40))");
            setup.execute("insert into orders values (1,7,'one'),(2,7,'split'),(3,7,'other-tenant'),(4,7,'other-category')");
            setup.execute("insert into items values (1,7,20,'A',1),(1,7,30,'B',1),"
                    + "(2,7,5,'A',1),(2,7,20,'C',1),(3,8,30,'A',1),(4,7,30,'A',2)");
            try (PreparedStatement category = connection.prepareStatement("insert into categories values (?,?)")) {
                category.setLong(1, 1L);
                category.setString(2, "x' OR 1=1 --");
                category.executeUpdate();
                category.setLong(1, 2L);
                category.setString(2, "other");
                category.executeUpdate();
            }
            ConditionGroup where = conditions.conditions().where("id", "eligible", 10).build();
            assertEquals(List.of(1L), query(connection, renderer.select(ORDERS, where)));
            assertEquals(List.of(1L), query(connection, renderer.count(ORDERS, where)));
            assertEquals(List.of(1L), query(connection,
                    renderer.select(ORDERS, where, PageQuery.of(1, 1, PageSort.asc("id")))));
            assertEquals(List.of(2L, 3L, 4L), query(connection, renderer.selectOrdered(ORDERS,
                    conditions.conditions().where("id", "ineligible", 10).build(), List.of(PageSort.asc("id")))));
            verifyRelatedCursorPages(connection, renderer, conditions);
            try (PreparedStatement update = bind(connection, renderer.update(ORDERS, Map.of("name", "changed"), where))) {
                assertEquals(1, update.executeUpdate());
            }
            try (PreparedStatement delete = bind(connection, renderer.delete(ORDERS, where))) {
                assertEquals(1, delete.executeUpdate());
            }
            assertEquals(List.of(3L), query(connection, renderer.count(ORDERS, ConditionGroup.and().build())));
        }
    }

    private static void verifyRelatedCursorPages(Connection connection, FormDataSqlRenderer renderer,
                                                 SqlRenderer conditions) throws Exception {
        try (Statement setup = connection.createStatement()) {
            setup.execute("insert into orders values (5,7,'next'),(6,8,'outside-root-scope')");
            setup.execute("insert into items values (5,7,30,'A',1),(5,7,40,'B',1),(6,7,30,'A',1)");
        }
        DataScope scope = DataScope.where(ConditionGroup.and().where("tenant_id", "=", 7L).build())
                .withFields(FieldScope.readable("id", "name"));
        FormReadPlanner planner = new FormReadPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), scope),
                SqlExecutionOptions.unlimited());
        for (String term : List.of("eligible", "ineligible")) {
            QuerySpec spec = QuerySpec.of(ORDERS, conditions.conditions().where("id", term, 10).build())
                    .withProjection(List.of("id"), List.of());
            long first = term.equals("eligible") ? 1L : 2L;
            List<Long> firstRows = term.equals("eligible") ? List.of(1L, 5L) : List.of(2L, 3L);
            List<Long> afterRows = term.equals("eligible") ? List.of(5L) : List.of(3L, 4L);
            KeysetSort cursorSort = KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST);
            KeysetSort keysetSort = KeysetSort.asc("id", NullOrder.LAST);
            assertEquals(firstRows, query(connection, planner.keysetPage(spec,
                    KeysetPageQuery.first(1, cursorSort)).request()), "one lookahead row, no JOIN multiplication");
            assertEquals(afterRows, query(connection, planner.keysetPage(spec,
                    KeysetPageQuery.after(1, com.flying.orm.core.page.CursorPosition.of(List.of(first)), cursorSort)).request()));
            assertEquals(firstRows, query(connection, planner.keysetPage(spec,
                    KeysetPageQuery.first(1, keysetSort)).request()));
            assertEquals(afterRows, query(connection, planner.keysetPage(spec,
                    KeysetPageQuery.after(1, CursorPosition.of(List.of(first)), keysetSort)).request()));
        }
        try (Statement cleanup = connection.createStatement()) {
            cleanup.execute("delete from items where order_id in (5,6)");
            cleanup.execute("delete from orders where id in (5,6)");
        }
    }

    @Test
    void innerScopeAndOuterMetadataStillValidateCorrelationFields() {
        FormDataSqlRenderer inner = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms()
                .addTerm(inner.relationExists("hidden-key", value -> {
                    JoinQuerySpec.Builder query = JoinQuerySpec.builder(ITEMS);
                    return query.selectAs(query.root(), "order_id", "id")
                            .scope(query.root(), DataScope.none().withFields(FieldScope.readable("amount"))).build();
                })).addTerm(inner.relationExists("unknown-outer-key", value -> {
                    JoinQuerySpec.Builder query = JoinQuerySpec.builder(ITEMS);
                    return query.selectAs(query.root(), "order_id", "unknown").build();
                })).build();
        FormDataSqlRenderer outer = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql());
        for (String term : List.of("hidden-key", "unknown-outer-key")) {
            assertThrows(IllegalArgumentException.class, () -> outer.select(ORDERS,
                    conditions.conditions().where("id", term, 1).build()));
        }
    }

    private static PreparedStatement bind(Connection connection, SqlRequest request) throws Exception {
        PreparedStatement statement = connection.prepareStatement(request.sql());
        for (int index = 0; index < request.parameters().size(); index++) {
            statement.setObject(index + 1, request.parameters().get(index));
        }
        return statement;
    }

    @Test
    void secondaryOuterKeysCannotComparePlaintextToCiphertext() {
        DynamicForm secured = DynamicForm.builder("secured", "secured")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", com.flying.orm.core.protection.EncryptedFieldDefinition.builder().build())
                .build();
        FormDataSqlRenderer inner = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms()
                .addTerm(inner.relationExists("secret-key", value -> {
                    JoinQuerySpec.Builder related = JoinQuerySpec.builder(ITEMS);
                    return related.selectAs(related.root(), "order_id", "id")
                            .selectAs(related.root(), "status", "secret").build();
                })).build();
        ConditionGroup where = conditions.conditions().where("id", "secret-key", 1L).build();
        try (com.flying.orm.rdb.protection.ProtectedFieldRuntime runtime =
                     com.flying.orm.rdb.protection.ProtectedFieldRuntime.create(
                             com.flying.orm.rdb.protection.ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql())
                    .withProtectedFields(runtime);
            FormScopeSupport scopes = new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none());
            FormOperationPlanner planner = new FormOperationPlanner(renderer, scopes, SqlExecutionOptions.unlimited());
            assertThrows(IllegalArgumentException.class, () -> planner.reads.select(
                    com.flying.orm.rdb.form.spec.QuerySpec.of(secured, where)));
            assertThrows(IllegalArgumentException.class, () -> planner.update(
                    com.flying.orm.rdb.form.spec.WriteSpec.update(secured, Map.of("id", 2L), where)));
            JoinQuerySpec.Builder joined = JoinQuerySpec.builder(secured);
            joined.select(joined.root(), "id").where(joined.root(), where);
            JoinQueryPlanner joins = new JoinQueryPlanner(renderer, scopes, SqlExecutionOptions.unlimited());
            assertThrows(IllegalArgumentException.class, () -> joins.plan(joined.build(), null));
        }
    }

    private static List<Long> query(Connection connection, SqlRequest request) throws Exception {
        List<Long> rows = new ArrayList<>();
        try (PreparedStatement statement = bind(connection, request); ResultSet result = statement.executeQuery()) {
            while (result.next()) rows.add(result.getLong(1));
        }
        return rows;
    }
}
