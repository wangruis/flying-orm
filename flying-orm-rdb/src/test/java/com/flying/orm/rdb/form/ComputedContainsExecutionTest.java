package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.protection.ProtectedFormLayout;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class ComputedContainsExecutionTest {
    @TestFactory
    Stream<DynamicTest> jdbcAndReactiveVerifyBeforeFirstPageAndCursor() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("reactive=" + reactive,
                () -> execute(reactive)));
    }

    private static void execute(boolean reactive) throws Exception {
        String name = "contains_projection_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + name + ";DATABASE_TO_LOWER=TRUE");
        source.setUser("sa");
        source.setPassword("");
        ConnectionFactory factory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.DRIVER, "h2").option(ConnectionFactoryOptions.PROTOCOL, "mem")
                .option(ConnectionFactoryOptions.DATABASE, name).option(ConnectionFactoryOptions.USER, "sa")
                .option(ConnectionFactoryOptions.PASSWORD, "")
                .option(io.r2dbc.h2.H2ConnectionFactoryProvider.OPTIONS, "DATABASE_TO_LOWER=TRUE").build());
        try (Connection keeper = source.getConnection();
             ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(ProtectedFieldKeyRing.single("v1", new byte[32]));
             FlyingOrmClients clients = FlyingOrmClients.builder(ConnectionAccessTestSupport.jdbc(source),
                     ConnectionAccessTestSupport.reactive(factory)).configuredDialect("h2")
                     .protectedFields(ProtectedFieldKeyRing.single("v1", new byte[32])).build()) {
            seed(keeper, runtime);
            QuerySpec spec = ComputedContainsPlanTest.query().withSorts(List.of(PageSort.desc("total")));
            List<DynamicRow> rows = reactive ? clients.forms().select(spec).collectList().block()
                    : clients.syncForms().select(spec);
            assertEquals(List.of(3L, 1L), rows.stream().map(row -> row.get("id")).toList());
            assertEquals(List.of(37, 17), rows.stream().map(row -> ((Number) row.get("total")).intValue()).toList());
            rows.forEach(row -> assertEquals(List.of("id", "total"), List.copyOf(row.keySet())));
            DynamicRow first = reactive ? clients.forms().first(spec).block() : clients.syncForms().first(spec);
            assertEquals(3L, first.get("id"));
            PageResult<DynamicRow> page = reactive ? clients.forms().page(spec, PageQuery.of(2, 1)).block()
                    : clients.syncForms().page(spec, PageQuery.of(2, 1));
            assertEquals(2, page.total(), "false token matches must not inflate totals");
            assertEquals(1L, page.rows().getFirst().get("id"));
            QuerySpec windowed = spec.withPage(PageQuery.of(2, 1));
            List<DynamicRow> windowRows = reactive ? clients.forms().select(windowed).collectList().block()
                    : clients.syncForms().select(windowed);
            assertEquals(List.of(1L), windowRows.stream().map(row -> row.get("id")).toList(),
                    "the window follows exact verification, not candidate rows");
            if (reactive) {
                reactor.core.publisher.Flux<DynamicRow> repeated = clients.forms().select(windowed);
                assertEquals(1L, repeated.single().block().get("id"));
                assertEquals(1L, repeated.single().block().get("id"));
            } else {
                java.util.ArrayList<Long> visited = new java.util.ArrayList<>();
                assertEquals(1L, clients.syncForms().forEach(windowed, row -> (Long) row.get("id"), visited::add));
                assertEquals(List.of(1L), visited);
                assertEquals(1L, clients.syncForms().select(windowed, IdTotal.class).getFirst().id());
            }
            PageResult<DynamicRow> explicit = reactive ? clients.forms().page(windowed, PageQuery.of(1, 1)).block()
                    : clients.syncForms().page(windowed, PageQuery.of(1, 1));
            assertEquals(2L, explicit.total());
            assertEquals(3L, explicit.rows().getFirst().get("id"));
            QuerySpec sortOnly = QuerySpec.of(ComputedContainsPlanTest.FORM, spec.where())
                    .withProjection(List.of("id"), List.of()).withOrderBy(
                            ValueExpression.call("priority", ValueExpression.field("amount"), ValueExpression.value(30)),
                            PageSort.Direction.ASC);
            PageResult<DynamicRow> ranked = reactive ? clients.forms().page(sortOnly, PageQuery.of(2, 1)).block()
                    : clients.syncForms().page(sortOnly, PageQuery.of(2, 1));
            assertEquals(2, ranked.total());
            assertEquals(1L, ranked.rows().getFirst().get("id"));
            assertEquals(List.of("id"), List.copyOf(ranked.rows().getFirst().keySet()));
            KeysetPageQuery cursor = KeysetPageQuery.after(1, com.flying.orm.core.page.CursorPosition.of(List.of(1L)), KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST));
            KeysetPageResult<DynamicRow> next = reactive
                    ? clients.forms().keysetPage(ComputedContainsPlanTest.query(), cursor).block()
                    : clients.syncForms().keysetPage(ComputedContainsPlanTest.query(), cursor);
            assertEquals(3L, next.rows().getFirst().get("id"));
            assertEquals(37, ((Number) next.rows().getFirst().get("total")).intValue());
            QuerySpec excluded = QuerySpec.of(ComputedContainsPlanTest.FORM, spec.where())
                    .withExcludedFields(List.of("secret", "payload", "amount"));
            PageResult<DynamicRow> excludedPage = reactive ? clients.forms().page(excluded, PageQuery.of(1, 5)).block()
                    : clients.syncForms().page(excluded, PageQuery.of(1, 5));
            assertEquals(2, excludedPage.total(), "excluding encrypted output must preserve exact contains verification");
            excludedPage.rows().forEach(row -> assertEquals(List.of("id"), List.copyOf(row.keySet())));
            QuerySpec aliases = spec.withExpression("payload_alias", ValueExpression.field("payload"))
                    .withProjection(List.of("id", "secret", "total", "payload_alias"), List.of());
            List<DynamicRow> displayed = reactive ? clients.forms().select(aliases).collectList().block()
                    : clients.syncForms().select(aliases);
            assertEquals("********", displayed.getFirst().get("secret"));
            assertEquals(Map.of("n", 3), displayed.getFirst().get("payload_alias"));
            QuerySpec collision = ComputedContainsPlanTest.query()
                    .withExpression("__fop_c0", ValueExpression.field("amount"))
                    .withProjection(List.of("id", "__fop_c0"), List.of())
                    .withSorts(List.of(PageSort.desc("__fop_c0")));
            List<DynamicRow> collisionRows = reactive ? clients.forms().select(collision).collectList().block()
                    : clients.syncForms().select(collision);
            assertEquals(List.of(3L, 1L), collisionRows.stream().map(row -> row.get("id")).toList());
            FieldUsePolicy allowed = FieldUsePolicy.builder()
                    .allow("secret", FieldUse.FILTER).allow("id", FieldUse.FILTER)
                    .visibility("id", FieldVisibility.FULL).visibility("amount", FieldVisibility.FULL)
                    .allow("amount", FieldUse.SORT).build();
            List<DynamicRow> governed = reactive
                    ? clients.forms().withFieldUsePolicy(allowed).select(spec).collectList().block()
                    : clients.syncForms().withFieldUsePolicy(allowed).select(spec);
            assertEquals(List.of("id", "total"), List.copyOf(governed.getFirst().keySet()));
            List<DynamicRow> governedWindow = reactive
                    ? clients.forms().withFieldUsePolicy(allowed).select(windowed).collectList().block()
                    : clients.syncForms().withFieldUsePolicy(allowed).select(windowed);
            assertEquals(List.of(1L), governedWindow.stream().map(row -> row.get("id")).toList());
            FieldUsePolicy denied = FieldUsePolicy.builder()
                    .allow("secret", FieldUse.FILTER).allow("id", FieldUse.FILTER)
                    .visibility("id", FieldVisibility.FULL).build();
            assertThrows(ScopeAccessException.class, () -> {
                if (reactive) clients.forms().withFieldUsePolicy(denied).select(spec).collectList().block();
                else clients.syncForms().withFieldUsePolicy(denied).select(spec);
            });
            if (reactive) {
                assertEquals(List.of(3L, 1L), clients.forms().select(spec).collectList().block()
                        .stream().map(row -> row.get("id")).toList());
            }
            verifyKeyset(clients, keeper, reactive);
        }
    }

    private static void verifyKeyset(FlyingOrmClients clients, Connection connection, boolean reactive) throws Exception {
        QuerySpec query = ComputedContainsPlanTest.query().withProjection(List.of("total"), List.of());
        KeysetSort order = KeysetSort.desc("amount", NullOrder.LAST);
        KeysetPageQuery first = KeysetPageQuery.first(1, order);
        KeysetPageResult<DynamicRow> page = reactive ? clients.forms().keysetPage(query, first).block()
                : clients.syncForms().keysetPage(query, first);
        assertEquals(List.of("total"), List.copyOf(page.rows().getFirst().keySet()));
        assertEquals(37, ((Number) page.rows().getFirst().get("total")).intValue());
        assertTrue(page.hasMore());
        assertEquals(List.of(30, 3L), page.nextPosition().values());
        KeysetPageQuery after = KeysetPageQuery.after(1, page.nextPosition(), order);
        KeysetPageResult<DynamicRow> next = reactive ? clients.forms().keysetPage(query, after).block()
                : clients.syncForms().keysetPage(query, after);
        assertEquals(17, ((Number) next.rows().getFirst().get("total")).intValue());
        assertFalse(next.hasMore(), "false token candidates must not become lookahead matches");
        if (reactive) {
            reactor.core.publisher.Mono<KeysetPageResult<DynamicRow>> repeated = clients.forms().keysetPage(query, first);
            assertEquals(page.nextPosition(), repeated.block().nextPosition());
            assertEquals(page.nextPosition(), repeated.block().nextPosition());
        }
        FieldUsePolicy policy = FieldUsePolicy.builder().allow("secret", FieldUse.FILTER)
                .allow("id", FieldUse.FILTER).allow("id", FieldUse.SORT).allow("amount", FieldUse.SORT)
                .allowInternal("id", com.flying.orm.core.scope.FieldUseOrigin.INTERNAL_TIE_BREAKER, FieldUse.SORT)
                .visibility("id", FieldVisibility.FULL).visibility("amount", FieldVisibility.FULL).build();
        KeysetPageResult<Total> typed = reactive
                ? clients.forms().withFieldUsePolicy(policy).keysetPage(query, first, Total.class).block()
                : clients.syncForms().withFieldUsePolicy(policy).keysetPage(query, first, Total.class);
        assertEquals(List.of(new Total(37)), typed.rows());
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("update \"contains_computed\" set \"amount\"=null where \"id\"=3");
        }
        QuerySpec nullable = QuerySpec.of(ComputedContainsPlanTest.FORM, query.where())
                .withProjection(List.of("id"), List.of());
        KeysetSort nullFirst = KeysetSort.asc("amount", NullOrder.FIRST);
        KeysetPageQuery nullPage = KeysetPageQuery.first(1, nullFirst);
        KeysetPageResult<DynamicRow> nullResult = reactive ? clients.forms().keysetPage(nullable, nullPage).block()
                : clients.syncForms().keysetPage(nullable, nullPage);
        assertEquals(3L, nullResult.rows().getFirst().get("id"));
        assertEquals(java.util.Arrays.asList(null, 3L), nullResult.nextPosition().values());
        KeysetPageQuery nullAfter = KeysetPageQuery.after(1, nullResult.nextPosition(), nullFirst);
        KeysetPageResult<DynamicRow> continued = reactive ? clients.forms().keysetPage(nullable, nullAfter).block()
                : clients.syncForms().keysetPage(nullable, nullAfter);
        assertEquals(1L, continued.rows().getFirst().get("id"));
        assertFalse(continued.hasMore());
    }

    private static void seed(Connection connection, ProtectedFieldRuntime runtime) throws Exception {
        DynamicForm form = ComputedContainsPlanTest.FORM;
        ProtectedFieldRuntime.PreparedContainsQuery query = runtime.prepareContainsQuery(form, form,
                ComputedContainsPlanTest.query().where(), DataScope.none(), ValueCodecRegistry.standard()).orElseThrow();
        try (Statement statement = connection.createStatement()) {
            statement.execute("create table \"contains_computed\"(\"id\" bigint primary key, \"amount\" integer, \"secret\" varbinary, \"payload\" varchar)");
            statement.execute("create table \"" + query.tokenTable()
                    + "\"(\"id\" bigint, \"field_tag\" varchar, \"token_hash\" varbinary)");
        }
        ProtectedFieldRuntime.WriteOperation write = runtime.writeOperation(form, ProtectedFormLayout.physical(form),
                DataScope.none(), ValueCodecRegistry.standard());
        try (PreparedStatement row = connection.prepareStatement("insert into \"contains_computed\" values(?,?,?,?)");
             PreparedStatement token = connection.prepareStatement("insert into \"" + query.tokenTable() + "\" values(?,?,?)")) {
            for (int id = 1; id <= 3; id++) {
                int amount = id == 2 ? 100 : id * 10;
                Map<String, Object> values = Map.of("id", (long) id, "amount", amount,
                        "secret", id == 2 ? "goodbye" : "alphabet");
                row.setLong(1, id);
                row.setInt(2, amount);
                row.setBytes(3, (byte[]) write.prepare(values).ownedValues().get("secret"));
                row.setString(4, "{\"n\":" + id + "}");
                row.executeUpdate();
                // Deliberately include a false candidate to exercise exact verification before slicing.
                for (byte[] hash : query.tokenGroups().getFirst().tokens()) {
                    token.setLong(1, id);
                    token.setString(2, query.fieldTag());
                    token.setBytes(3, hash);
                    token.executeUpdate();
                }
            }
        }
    }

    public record IdTotal(Long id, int total) { }
    public record Total(int total) { }
}
