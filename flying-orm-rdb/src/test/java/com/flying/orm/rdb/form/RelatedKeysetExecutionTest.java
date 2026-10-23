package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.QueryRelation;
import com.flying.orm.core.page.CursorPosition;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.CursorDirection;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.expression.ValueExpression;
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
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class RelatedKeysetExecutionTest {
    @TestFactory
    Stream<DynamicTest> relatedKeysetUsesCompleteJoinedIdentity() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest(
                reactive ? "R2DBC related keyset" : "JDBC related keyset", () -> execute(reactive)));
    }

    private void execute(boolean reactive) throws Exception {
        String database = "related_keyset_" + reactive;
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
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("owner", "BIGINT"))
                .addField(DynamicField.of("tenant", "BIGINT"))
                .addField(DynamicField.of("label", "VARCHAR")).build();
        DynamicForm roots = DynamicForm.builder("roots", "roots")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("tenant", "BIGINT"))
                .relation("items", QueryRelation.of(() -> {
                    resolutions.incrementAndGet();
                    return items;
                }, "id", "owner")).build();
        try (Connection keeper = source.getConnection(); Statement statement = keeper.createStatement();
             FlyingOrmClients clients = FlyingOrmClients.builder(ConnectionAccessTestSupport.jdbc(source),
                     ConnectionAccessTestSupport.reactive(factory)).configuredDialect("h2").build()) {
            statement.execute("create table roots(id bigint primary key, tenant bigint)");
            statement.execute("create table items(id bigint primary key, owner bigint, tenant bigint, label varchar(20))");
            statement.execute("insert into roots values(1,7),(2,7),(3,7),(4,8),(5,7)");
            statement.execute("insert into items values(10,1,7,'same'),(11,1,7,'same'),(12,2,7,null),(13,3,8,'hidden')");
            QuerySpec query = QuerySpec.of(roots, ConditionGroup.and().build())
                    .withProjection(List.of("items.label"), List.of());
            DataScope scope = DataScope.where(ConditionGroup.and().where("tenant", "=", 7L).build());
            SyncFormClient sync = clients.syncForms().withDefaultDataScope(scope);
            ReactiveFormClient async = clients.forms().withDefaultDataScope(scope);
            KeysetSort sort = new KeysetSort("items.label", CursorDirection.ASC, NullOrder.LAST);
            List<Object> values = new ArrayList<>();
            List<List<Object>> positions = new ArrayList<>();
            CursorPosition position = CursorPosition.first();
            for (int index = 0; index < 5; index++) {
                int before = resolutions.get();
                KeysetPageQuery page = new KeysetPageQuery(1, List.of(sort), position);
                KeysetPageResult<DynamicRow> result;
                if (reactive) {
                    Mono<KeysetPageResult<DynamicRow>> publisher = async.keysetPage(query, page);
                    assertEquals(before, resolutions.get(), "relation binding must be cold");
                    result = publisher.block();
                } else {
                    result = sync.keysetPage(query, page);
                }
                assertNotNull(result);
                assertEquals(before + 1, resolutions.get(), "one model binding per execution");
                assertEquals(1, result.rows().size());
                assertEquals(List.of("label"), new ArrayList<>(result.rows().getFirst().keySet()));
                values.add(result.rows().getFirst().get("label"));
                assertEquals(index < 4, result.hasMore());
                position = result.nextPosition();
                if (result.hasMore()) positions.add(position.values());
            }
            assertEquals(Arrays.asList("same", "same", null, null, null), values);
            assertEquals(List.of(Arrays.asList("same", 1L, 10L), Arrays.asList("same", 1L, 11L),
                    Arrays.asList(null, 2L, 12L), Arrays.asList(null, 3L, null)), positions);
            QuerySpec calculated = query.withExpression("__fo_ks_1", ValueExpression.value("constant"));
            KeysetPageQuery first = KeysetPageQuery.first(1, sort);
            KeysetPageResult<DynamicRow> expression = reactive ? async.keysetPage(calculated, first).block()
                    : sync.keysetPage(calculated, first);
            assertNotNull(expression);
            assertEquals(List.of("label", "__fo_ks_1"), new ArrayList<>(expression.rows().getFirst().keySet()));
            assertEquals("constant", expression.rows().getFirst().get("__fo_ks_1"));
            assertEquals(List.of("same", 1L, 10L), expression.nextPosition().values());

            KeysetPageQuery descending = KeysetPageQuery.first(2,
                    KeysetSort.asc("items.label", NullOrder.FIRST), KeysetSort.desc("id", NullOrder.LAST));
            KeysetPageResult<Label> typed = reactive ? async.keysetPage(query, descending, Label.class).block()
                    : sync.keysetPage(query, descending, Label.class);
            assertNotNull(typed);
            assertEquals(List.of(new Label(null), new Label(null)), typed.rows());
            assertEquals(Arrays.asList(null, 3L, null), typed.nextPosition().values());
            if (reactive) {
                int before = resolutions.get();
                Mono<KeysetPageResult<Label>> publisher = async.keysetPage(query, first, Label.class);
                assertEquals(List.of(new Label("same")), publisher.block().rows());
                assertEquals(List.of(new Label("same")), publisher.block().rows());
                assertEquals(before + 2, resolutions.get());
            }

            DynamicForm selfRelated = DynamicForm.builder("root_owners", "roots")
                    .addField(DynamicField.primaryKey("id", "BIGINT"))
                    .addField(DynamicField.of("tenant", "BIGINT"))
                    .relation("owners", QueryRelation.of(items, "id", "owner")
                            .then(QueryRelation.of(roots, "owner", "id"))).build();
            QuerySpec chain = QuerySpec.of(selfRelated, ConditionGroup.and().build())
                    .withProjection(List.of("owners.id"), List.of());
            KeysetSort ownerSort = KeysetSort.asc("owners.id", NullOrder.LAST);
            position = CursorPosition.first();
            List<Long> owners = new ArrayList<>();
            for (int index = 0; index < 5; index++) {
                KeysetPageQuery page = new KeysetPageQuery(1, List.of(ownerSort), position);
                KeysetPageResult<DynamicRow> result = reactive ? async.keysetPage(chain, page).block()
                        : sync.keysetPage(chain, page);
                assertNotNull(result);
                assertEquals(1, result.rows().size());
                owners.add((Long) result.rows().getFirst().get("id"));
                assertEquals(index < 4, result.hasMore());
                position = result.nextPosition();
            }
            assertEquals(Arrays.asList(1L, 1L, 2L, null, null), owners,
                    "middle-source keys must distinguish repeated root/target pairs");
        }
    }

    public record Label(String label) { }
}
