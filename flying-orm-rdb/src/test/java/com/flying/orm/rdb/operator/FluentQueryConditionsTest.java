package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.ConnectionFactories;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FluentQueryConditionsTest {

    private static final SqlRenderer RENDERER = SqlRenderer.builder().addDefaultTerms().build();

    @Test
    void nestedGroupsKeepBusinessPrecedenceAndScopeOutsideOr() {
        DmlQueryCommand command = command();
        command.where(where -> where.is("active", true)
                .or(group -> group.is("name", "Alice")
                        .and(nested -> nested.where("age", ">=", 18).is("city", "Shanghai"))));
        command.where("id", ">", 0);
        command.scope(DataScope.tenant("tenant_id", 9));

        var request = command.toRequest();
        assertEquals("select * from users where active = ? and (name = ? or (age >= ? and city = ?))"
                + " and id > ? and tenant_id = ?", request.sql());
        assertEquals(List.of(true, "Alice", 18, "Shanghai", 0, 9), request.parameters());
    }

    @Test
    void nestedCustomTermsKeepTheRegisteredCollectionShape() {
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms()
                .addTerm(SqlTermHandler.of("any-of", ConditionValueShape.COLLECTION,
                        SqlTermHandler.in()::render)).build();
        WhereDsl where = new WhereDsl(renderer);
        where.or(group -> group.where("id", "any-of", new String[]{"1", "2"}));

        var fragment = renderer.renderWhere(where.build());
        assertEquals(List.of("1", "2"), fragment.parameters());
        assertTrue(fragment.sql().contains("id in (?, ?)"));
    }

    @Test
    void emptyOptionalGroupsDoNotBroadenAnExistingCondition() {
        WhereDsl where = new WhereDsl(RENDERER);
        where.is("id", 1).or(group -> group.termIfPresent("name", "=", " "));
        assertEquals("id = ?", RENDERER.renderWhere(where.build()).sql());
        assertThrows(NullPointerException.class, () -> where.or(null));
        assertThrows(NullPointerException.class, () -> where.and(null));
    }

    @Test
    void directConditionsSnapshotValuesAndPreviouslyBuiltRequests() {
        DmlQueryCommand command = command();
        List<Integer> ids = new ArrayList<>(List.of(1, 2));
        command.where("id", "in", ids);
        var first = command.toRequest();
        ids.clear();
        command.where("active", "=", true);

        assertEquals(List.of(1, 2), first.parameters());
        assertEquals(List.of(1, 2, true), command.toRequest().parameters());
        assertFalse(first.sql().contains("active"));
    }

    @Test
    void callbackReplacementKeepsItsExistingSemanticsAndSnapshotsRetainedDsl() {
        DmlQueryCommand command = command();
        command.where("id", "=", 1);
        AtomicReference<WhereDsl> retained = new AtomicReference<>();
        command.where(where -> {
            retained.set(where);
            where.is("id", 2);
        });
        retained.get().is("name", "late change");
        command.where("active", "=", true);
        assertEquals(List.of(2, true), command.toRequest().parameters());
    }

    @Test
    void invalidFieldsAndUnregisteredOperatorsCannotBecomeSql() {
        DmlQueryCommand command = command();
        assertThrows(IllegalArgumentException.class,
                () -> command.where("id) or 1=1 --", "=", 1));
        command.where("id", "unregistered", 1);
        assertThrows(IllegalArgumentException.class, command::toRequest);
    }

    @Test
    void syncAndReactiveQueriesBindValuesAndRetainTenantIsolationInH2() throws Exception {
        String database = "fluent_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + database + ";DATABASE_TO_LOWER=TRUE");
        var factory = ConnectionFactories.get("r2dbc:h2:mem:///" + database + "?options=DATABASE_TO_LOWER=TRUE");
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms()
                .addTerm(SqlTermHandler.relationExists("user-in-org", "memberships", "m", "user_id", "org_id"))
                .build();
        try (var keepAlive = source.getConnection();
             var statement = keepAlive.createStatement();
             FlyingOrmClients clients = FlyingOrmClients.builder(ConnectionAccessTestSupport.jdbc(source),
                     ConnectionAccessTestSupport.reactive(factory)).configuredDialect("h2").renderer(renderer).build()) {
            statement.execute("create table users(id int primary key, name varchar(128), tenant_id int)");
            statement.execute("create table memberships(user_id int, org_id int)");
            statement.execute("insert into memberships values (1, 7), (2, 7)");
            String value = "x' or 1=1 --";
            DataScope scope = DataScope.tenant("tenant_id", 9);
            QueryOperator query = clients.operator().withDefaultDataScope(scope).dml().query()
                    .select("id").from("users").where("id", "user-in-org", 7).where("name", value);
            Flux<DynamicRow> pending = query.fetchMap();
            query.where("id", 999);
            try (var insert = keepAlive.prepareStatement("insert into users values (?, ?, ?)")) {
                for (int id = 1; id <= 2; id++) {
                    insert.setInt(1, id);
                    insert.setString(2, value);
                    insert.setInt(3, id == 1 ? 9 : 10);
                    insert.executeUpdate();
                }
            }
            List<DynamicRow> reactive = pending.collectList().block(Duration.ofSeconds(10));
            List<DynamicRow> sync = clients.syncOperator().withDefaultDataScope(scope).dml().query()
                    .select("id").from("users").where("id", "user-in-org", 7).where("name", value).fetchMap();
            assertEquals(1, sync.size());
            assertEquals(1, sync.getFirst().get("id"));
            assertEquals(sync, reactive);
        }
    }

    private static DmlQueryCommand command() {
        DmlQueryCommand command = new DmlQueryCommand(RENDERER, DataScope.none());
        command.from("users");
        return command;
    }
}
