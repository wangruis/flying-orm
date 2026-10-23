package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.join.QueryRelation;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class RelatedCursorExecutionTest {
    @TestFactory
    Stream<DynamicTest> cursorUsesJoinedIdentityAndBothSourceScopes() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).map(governed -> DynamicTest.dynamicTest(
                (reactive ? "R2DBC" : "JDBC") + " related cursor governed=" + governed, () -> execute(reactive, governed))));
    }

    private void execute(boolean reactive, boolean governed) throws Exception {
        String database = "related_cursor_" + reactive + "_" + governed;
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + database + ";DATABASE_TO_LOWER=TRUE");
        source.setUser("sa");
        ConnectionFactory factory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.DRIVER, "h2").option(ConnectionFactoryOptions.PROTOCOL, "mem")
                .option(ConnectionFactoryOptions.DATABASE, database).option(ConnectionFactoryOptions.USER, "sa")
                .option(ConnectionFactoryOptions.PASSWORD, "")
                .option(io.r2dbc.h2.H2ConnectionFactoryProvider.OPTIONS, "DATABASE_TO_LOWER=TRUE").build());
        AtomicInteger resolutions = new AtomicInteger();
        DynamicForm items = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("item_id", "BIGINT"))
                .addField(DynamicField.of("owner", "BIGINT"))
                .addField(DynamicField.of("tenant", "BIGINT")).build();
        DynamicForm roots = DynamicForm.builder("roots", "roots")
                .addField(DynamicField.primaryKey("root_id", "BIGINT"))
                .addField(DynamicField.of("tenant", "BIGINT"))
                .relation("items", QueryRelation.of(() -> {
                    resolutions.incrementAndGet();
                    return items;
                }, "root_id", "owner").joinType(JoinType.INNER)).build();
        try (Connection keeper = source.getConnection(); Statement statement = keeper.createStatement();
             FlyingOrmClients clients = FlyingOrmClients.builder(ConnectionAccessTestSupport.jdbc(source),
                     ConnectionAccessTestSupport.reactive(factory)).configuredDialect("h2").build()) {
            statement.execute("create table roots(root_id bigint primary key, tenant bigint)");
            statement.execute("create table items(item_id bigint primary key, owner bigint, tenant bigint)");
            statement.execute("insert into roots values(1,7),(2,7),(3,8)");
            statement.execute("insert into items values(10,1,7),(11,1,7),(12,2,7),(13,2,8),(14,3,7)");
            QuerySpec query = QuerySpec.of(roots, ConditionGroup.and().build())
                    .withProjection(List.of("root_id", "items.item_id"), List.of());
            DataScope scope = DataScope.where(ConditionGroup.and().where("tenant", "=", 7L).build());
            SyncFormClient sync = clients.syncForms().withDefaultDataScope(scope);
            ReactiveFormClient async = clients.forms().withDefaultDataScope(scope);
            if (governed) {
                QueryShapeLimits limits = QueryShapeLimits.defaults().withMaxSortCount(2);
                sync = sync.withQueryShapeLimits(limits);
                async = async.withQueryShapeLimits(limits);
            }
            List<Ids> values = new ArrayList<>();
            List<Object> position = List.of();
            for (int index = 0; index < 3; index++) {
                int before = resolutions.get();
                KeysetPageQuery page = new KeysetPageQuery(1,
                        List.of(KeysetSort.asc("root_id", com.flying.orm.core.page.NullOrder.LAST)),
                        com.flying.orm.core.page.CursorPosition.of(position));
                KeysetPageResult<Ids> result;
                if (reactive) {
                    Mono<KeysetPageResult<Ids>> publisher = async.keysetPage(query, page, Ids.class);
                    assertEquals(before, resolutions.get(), "relation resolution must be cold");
                    result = publisher.block();
                } else {
                    result = sync.keysetPage(query, page, Ids.class);
                }
                assertNotNull(result);
                assertEquals(before + 1, resolutions.get());
                assertEquals(1, result.rows().size());
                assertEquals(index < 2, result.hasMore());
                values.addAll(result.rows());
                position = result.nextPosition().values();
                if (index < 2) assertEquals(List.of(1L, 10L + index), position);
            }
            assertEquals(List.of(new Ids(1L, 10L), new Ids(1L, 11L), new Ids(2L, 12L)), values);
            assertTrue(position.isEmpty());
            KeysetPageQuery descending = KeysetPageQuery.first(2, KeysetSort.desc("items.item_id", com.flying.orm.core.page.NullOrder.LAST));
            KeysetPageResult<Ids> reverse = reactive ? async.keysetPage(query, descending, Ids.class).block()
                    : sync.keysetPage(query, descending, Ids.class);
            assertNotNull(reverse);
            assertEquals(List.of(new Ids(2L, 12L), new Ids(1L, 11L)), reverse.rows());
            assertEquals(List.of(11L, 1L), reverse.nextPosition().values());
            QuerySpec aliased = query.withExpression("rootAlias", ValueExpression.field("root_id"))
                    .withExpression("childAlias", ValueExpression.field("items.item_id"))
                    .withProjection(List.of("rootAlias", "childAlias"), List.of());
            KeysetPageQuery first = KeysetPageQuery.first(1, KeysetSort.asc("root_id", com.flying.orm.core.page.NullOrder.LAST));
            KeysetPageResult<DynamicRow> aliases = reactive ? async.keysetPage(aliased, first).block()
                    : sync.keysetPage(aliased, first);
            assertNotNull(aliases);
            assertEquals(List.of(1L, 10L), aliases.nextPosition().values());
            assertEquals(List.of("rootAlias", "childAlias"), new ArrayList<>(aliases.rows().getFirst().keySet()));
            if (reactive) {
                int before = resolutions.get();
                Mono<KeysetPageResult<Ids>> repeated = async.keysetPage(query, first, Ids.class);
                assertEquals(before, resolutions.get());
                assertEquals(repeated.block(), repeated.block());
                assertEquals(before + 2, resolutions.get());
            }
        }
    }

    public record Ids(long root_id, long item_id) { }
}
