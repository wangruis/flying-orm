package com.flying.orm.consumer;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.lifecycle.ReactiveEntityListener;
import com.flying.orm.rdb.operator.EntityDmlQueryOperator;
import com.flying.orm.rdb.operator.SyncEntityDmlQueryOperator;
import io.r2dbc.spi.ConnectionFactories;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class EntityCursorQueryContractTest {
    @TestFactory
    Stream<DynamicTest> entityCursorTerminalsShareMappingScopeAndLoading() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest(
                reactive ? "R2DBC entity cursors" : "JDBC entity cursors", () -> verify(reactive)));
    }

    private static void verify(boolean reactive) throws Exception {
        String database = "entity_cursor_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + database + ";DATABASE_TO_LOWER=TRUE");
        try (Connection keeper = source.getConnection(); Statement statement = keeper.createStatement();
             FlyingOrmClients clients = FlyingOrmClients.builder(ConnectionAccessTestSupport.jdbc(source),
                     ConnectionAccessTestSupport.reactive(ConnectionFactories.get(
                             "r2dbc:h2:mem:///" + database + "?options=DATABASE_TO_LOWER=TRUE")))
                     .configuredDialect("h2").build()) {
            statement.execute("create table cursor_users (entity_id bigint primary key, "
                    + "user_name varchar(40), state varchar(1), tenant_id bigint)");
            statement.execute("insert into cursor_users values (1,'Ada','A',7),(2,'Bob','A',7),"
                    + "(3,null,'A',7),(4,'Other','A',9),(5,'Disabled','D',7)");
            AtomicInteger loads = new AtomicInteger();
            ReactiveEntityListener<User> listener = event -> Mono.fromRunnable(() -> {
                assertEquals(EntityLifecyclePhase.POST_LOAD, event.phase());
                loads.incrementAndGet();
                event.entity().userName = "loaded";
            });
            SyncEntityDmlQueryOperator<User> sync = clients.syncRepository(User.class).withListener(listener)
                    .createQuery().scope(DataScope.tenant("tenant_id", 7L))
                    .where(User::getState, State.ACTIVE);
            EntityDmlQueryOperator<User> async = clients.repository(User.class).withListener(listener)
                    .createQuery().scope(DataScope.tenant("tenant_id", 7L))
                    .where(User::getState, State.ACTIVE);
            KeysetSort id = KeysetSort.asc("userId", com.flying.orm.core.page.NullOrder.LAST);
            KeysetPageQuery firstRequest = KeysetPageQuery.first(2, id);
            Mono<KeysetPageResult<User>> publisher = reactive ? async.keysetPage(firstRequest) : null;
            assertEquals(0, loads.get());
            KeysetPageResult<User> first = reactive ? publisher.block() : sync.keysetPage(firstRequest);
            assertEquals(List.of(1L, 2L), first.rows().stream().map(User::getUserId).toList());
            assertTrue(first.hasMore());
            assertEquals(List.of(2L), first.nextPosition().values());
            assertEquals(State.ACTIVE, first.rows().getFirst().state);
            assertEquals("loaded", first.rows().getFirst().userName);
            assertEquals(2, loads.get(), "lookahead row must not notify listeners");
            if (reactive) {
                assertEquals(List.of(2L), publisher.block().nextPosition().values());
                assertEquals(4, loads.get(), "each subscription loads its own result");
            }
            KeysetPageQuery after = KeysetPageQuery.after(2, com.flying.orm.core.page.CursorPosition.of(first.nextPosition().values()), id);
            KeysetPageResult<User> tail = reactive ? async.keysetPage(after).block() : sync.keysetPage(after);
            assertEquals(List.of(3L), tail.rows().stream().map(User::getUserId).toList());
            assertFalse(tail.hasMore());
            assertTrue(tail.nextPosition().values().isEmpty());

            int beforeDto = loads.get();
            KeysetSort nullableName = KeysetSort.asc("userName", NullOrder.LAST);
            KeysetPageQuery keyset = KeysetPageQuery.first(2, nullableName);
            KeysetPageResult<NameView> names = reactive
                    ? async.copy().select(User::getUserName).keysetPage(keyset, NameView.class).block()
                    : sync.copy().select(User::getUserName).keysetPage(keyset, NameView.class);
            assertEquals(List.of(new NameView("Ada"), new NameView("Bob")), names.rows());
            assertTrue(names.hasMore());
            assertEquals(List.of("Bob", 2L), names.nextPosition().values(), "hidden primary key remains in position");
            KeysetPageQuery next = KeysetPageQuery.after(2, names.nextPosition(), nullableName);
            KeysetPageResult<NameView> last = reactive
                    ? async.copy().select(User::getUserName).keysetPage(next, NameView.class).block()
                    : sync.copy().select(User::getUserName).keysetPage(next, NameView.class);
            assertEquals(List.of(new NameView(null)), last.rows());
            assertFalse(last.hasMore());
            KeysetPageResult<IdView> ids = reactive
                    ? async.copy().select(User::getUserId).keysetPage(firstRequest, IdView.class).block()
                    : sync.copy().select(User::getUserId).keysetPage(firstRequest, IdView.class);
            assertEquals(List.of(new IdView(1L), new IdView(2L)), ids.rows());
            assertEquals(beforeDto, loads.get(), "DTOs must not trigger entity lifecycle callbacks");

            KeysetPageResult<User> entities = reactive ? async.keysetPage(keyset).block() : sync.keysetPage(keyset);
            assertEquals(List.of("Bob", 2L), entities.nextPosition().values(), "callbacks cannot rewrite paging position");
            assertEquals("loaded", entities.rows().getFirst().userName);
            assertEquals(beforeDto + 2, loads.get());
            assertThrows(IllegalArgumentException.class, () -> {
                if (reactive) async.copy().orderByAsc(User::getUserId).keysetPage(firstRequest).block();
                else sync.copy().orderByAsc(User::getUserId).keysetPage(firstRequest);
            }, "existing cursor kernel owns sorting rules");
        }
    }

    public record NameView(String userName) { }
    public record IdView(@TableField("entity_id") Long userId) { }

    @TableName("cursor_users")
    public static final class User {
        @TableId("entity_id") private Long userId;
        @TableField("user_name") private String userName;
        private State state;
        private Long tenantId;
        public Long getUserId() { return userId; }
        public String getUserName() { return userName; }
        public State getState() { return state; }
    }

    public enum State {
        ACTIVE("A"), DISABLED("D");
        @EnumValue private final String code;
        State(String code) { this.code = code; }
    }
}
