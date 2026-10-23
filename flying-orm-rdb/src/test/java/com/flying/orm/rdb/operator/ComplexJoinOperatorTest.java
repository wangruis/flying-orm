package com.flying.orm.rdb.operator;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.join.JoinCondition;
import com.flying.orm.core.join.QueryRelation;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ComplexJoinOperatorTest {
    @Test
    void namedRelationFiltersAndSortsShareTheSameCommandAcrossBothKernels() {
        Fixture fixture = new Fixture();
        DynamicForm users = fixture.sync.entityModels().metadata(User.class).toDynamicForm();
        DynamicForm orders = fixture.sync.entityModels().metadata(Order.class).toDynamicForm();
        QueryRelation purchases = QueryRelation.of(orders, "id", "user_id");
        ConditionGroup condition = ConditionGroup.or().where("id", "=", 1L)
                .where("purchases.quantity", ">", 2).build();
        ConditionGroup extra = ConditionGroup.and().where("minimum", ">", 3).build();
        new SyncJoinQueryOperator(fixture.sync, fixture.conditions, users).relation("purchases", purchases)
                .select("id").where(condition).where(extra).orderByAsc("purchases.quantity").executeRows();
        Flux<DynamicRow> pending = new JoinQueryOperator(fixture.reactive, fixture.conditions, users)
                .relation("purchases", purchases).select("id").where(condition).where(extra)
                .orderByAsc("purchases.quantity").executeRows();
        assertEquals(1, fixture.requests.size());
        pending.collectList().block();
        assertEquals(2, fixture.requests.size());
        for (SqlRequest request : fixture.requests) {
            assertEquals(List.of(1L, 2, 3), request.parameters());
            assertTrue(request.sql().contains("(\"t0\".\"id\" = ?) or (\"t1\".\"quantity\" > ?)"), request.sql());
            assertTrue(request.sql().contains("order by \"t1\".\"quantity\" asc"), request.sql());
            assertEquals(fixture.requests.getFirst().sql(), request.sql());
        }
    }

    @Test
    void namedRelationSelectionSharesSqlAndRemainsLazyAcrossBothFacades() {
        Fixture fixture = new Fixture();
        DynamicForm users = fixture.sync.entityModels().metadata(User.class).toDynamicForm();
        DynamicForm orders = fixture.sync.entityModels().metadata(Order.class).toDynamicForm();
        QueryRelation purchases = QueryRelation.of(orders, "id", "user_id")
                .where(ConditionGroup.and().where("status", "=", "A").build());
        new SyncJoinQueryOperator(fixture.sync, fixture.conditions, users)
                .relation("purchases", purchases)
                .select("id", "purchases.quantity")
                .selectAs("purchases.status", "purchaseStatus").executeRows();
        JoinQueryOperator reactive = new JoinQueryOperator(fixture.reactive, fixture.conditions, users)
                .relation("purchases", purchases)
                .select("id", "purchases.quantity")
                .selectAs("purchases.status", "purchaseStatus");
        Flux<DynamicRow> pending = reactive.executeRows();
        assertEquals(1, fixture.requests.size(), "building a reactive query must not execute SQL");
        pending.collectList().block();
        SqlRequest request = fixture.requests.getFirst();
        assertEquals(request.sql(), fixture.requests.getLast().sql());
        assertEquals(List.of("A"), request.parameters());
        assertEquals(request.parameters(), fixture.requests.getLast().parameters());
        String sql = request.sql();
        assertEquals(1, sql.split("left outer join", -1).length - 1, sql);
        assertTrue(sql.contains("\"t0\".\"id\" = \"t1\".\"user_id\""), sql);
        assertTrue(sql.contains("\"t1\".\"quantity\""), sql);
        assertTrue(sql.contains("\"purchaseStatus\""), sql);
        assertThrows(IllegalStateException.class, () -> reactive.select("id"));
        assertThrows(IllegalStateException.class, () -> reactive.relation("again", purchases));
    }

    @Test
    void unusedRelationDoesNotResolveTheTargetOrAddJoins() {
        Fixture fixture = new Fixture();
        DynamicForm users = fixture.sync.entityModels().metadata(User.class).toDynamicForm();
        QueryRelation unused = QueryRelation.of(() -> { throw new AssertionError("unused relation resolved"); }, "id", "user_id");
        new SyncJoinQueryOperator(fixture.sync, fixture.conditions, users)
                .relation("unused", unused).select("*").executeRows();
        new JoinQueryOperator(fixture.reactive, fixture.conditions, users)
                .relation("unused", unused).select("*").executeRows().collectList().block();
        assertEquals(2, fixture.requests.size());
        for (SqlRequest request : fixture.requests) {
            assertFalse(request.sql().contains(" join "), request.sql());
            assertTrue(request.sql().contains("\"t0\".\"minimum\""), request.sql());
            assertTrue(request.parameters().isEmpty());
        }
    }

    @Test
    void joinedPreferredOrderingIsSymmetricAndMapsEnumValues() {
        Fixture fixture = new Fixture();
        com.flying.orm.core.form.DynamicForm users = fixture.sync.entityModels().metadata(User.class).toDynamicForm();
        com.flying.orm.core.form.DynamicForm orders = fixture.sync.entityModels().metadata(Order.class).toDynamicForm();
        new SyncJoinQueryOperator(fixture.sync, fixture.conditions, users).join(orders, "id", "user_id")
                .selectAs(users, "id", "uid").orderByFirst(orders, "status", "A").executeRows();
        new JoinQueryOperator(fixture.reactive, fixture.conditions, users).join(orders, "id", "user_id")
                .selectAs(users, "id", "uid").orderByFirst(orders, "status", "A").executeRows().collectList().block();
        new SyncEntityJoinQueryOperator<>(fixture.sync, fixture.conditions, User.class)
                .join(Order.class, User::id, Order::userId).selectAs(User.class, User::id, "uid")
                .orderByFirst(Order.class, Order::status, State.ACTIVE).executeRows();
        new EntityJoinQueryOperator<>(fixture.reactive, fixture.conditions, User.class)
                .join(Order.class, User::id, Order::userId).selectAs(User.class, User::id, "uid")
                .orderByFirst(Order.class, Order::status, State.ACTIVE).executeRows().collectList().block();
        assertEquals(4, fixture.requests.size());
        fixture.requests.forEach(request -> {
            assertEquals(List.of("A"), request.parameters());
            assertEquals(fixture.requests.getFirst().sql(), request.sql());
        });
        new SyncEntityJoinQueryOperator<>(fixture.sync, fixture.conditions, User.class)
                .join(Order.class, User::id, Order::userId).selectAs(User.class, User::id, "uid")
                .orderByLast(Order.class, Order::status, State.ACTIVE).executeRows();
        assertTrue(fixture.requests.get(4).sql().contains("then 1 else 0 end"));
        assertEquals(List.of("A"), fixture.requests.get(4).parameters());
    }

    @Test
    void computedSelectUsesTheSameExpressionAcrossDynamicAndEntityFacades() {
        Fixture fixture = new Fixture();
        com.flying.orm.core.form.DynamicForm users = fixture.sync.entityModels().metadata(User.class).toDynamicForm();
        com.flying.orm.core.form.DynamicForm orders = fixture.sync.entityModels().metadata(Order.class).toDynamicForm();
        SyncJoinQueryOperator dynamic = new SyncJoinQueryOperator(fixture.sync, fixture.conditions, users)
                .leftJoin(orders, "id", "user_id");
        dynamic.select(dynamic.field(users, "minimum").add(dynamic.field(orders, "quantity")), "total").executeRows();
        JoinQueryOperator reactive = new JoinQueryOperator(fixture.reactive, fixture.conditions, users)
                .leftJoin(orders, "id", "user_id");
        Flux<DynamicRow> pending = reactive.select(
                reactive.field(users, "minimum").add(reactive.field(orders, "quantity")), "total").executeRows();
        assertEquals(1, fixture.requests.size());
        pending.collectList().block();
        assertThrows(IllegalStateException.class,
                () -> reactive.select(reactive.field(users, "minimum"), "another"));
        SyncEntityJoinQueryOperator<User> entity = new SyncEntityJoinQueryOperator<>(fixture.sync, fixture.conditions, User.class)
                .join(Order.class, User::id, Order::userId);
        entity.select(entity.field(User.class, User::minimum).add(entity.field(Order.class, Order::quantity)), "total")
                .executeRows();
        EntityJoinQueryOperator<User> reactiveEntity = new EntityJoinQueryOperator<>(fixture.reactive, fixture.conditions, User.class)
                .join(Order.class, User::id, Order::userId);
        reactiveEntity.select(reactiveEntity.field(User.class, User::minimum)
                .add(reactiveEntity.field(Order.class, Order::quantity)), "total").executeRows().collectList().block();
        assertEquals(4, fixture.requests.size());
        assertEquals(fixture.requests.get(0).sql(), fixture.requests.get(1).sql());
        assertEquals(fixture.requests.get(2).sql(), fixture.requests.get(3).sql());
        assertTrue(fixture.requests.getFirst().sql().contains("(\"t0\".\"minimum\" + \"t1\".\"quantity\") as \"total\""));
    }

    @Test
    void allCallbackJoinTypesAgreeAcrossPublicFacadesAndFreezeAtPublication() {
        for (String kind : List.of("inner", "left outer", "right outer", "full outer")) {
            Fixture fixture = new Fixture();
            com.flying.orm.core.form.DynamicForm users = fixture.sync.entityModels().metadata(User.class).toDynamicForm();
            com.flying.orm.core.form.DynamicForm orders = fixture.sync.entityModels().metadata(Order.class).toDynamicForm();
            java.util.function.BiFunction<EntityJoinSource<User>, EntityJoinSource<Order>, JoinCondition> typed =
                    (user, order) -> JoinCondition.and(
                            JoinCondition.compare(user.field(User::id), "=", order.field(Order::userId)),
                            order.where(where -> where.where(Order::status, State.ACTIVE)));
            java.util.function.BiFunction<com.flying.orm.core.join.JoinSource, com.flying.orm.core.join.JoinSource, JoinCondition> dynamic =
                    (user, order) -> JoinCondition.and(
                            JoinCondition.compare(user.field("id"), "=", order.field("user_id")),
                            JoinCondition.where(order, ConditionGroup.and().where("status", "=", "A").build()));
            SyncEntityJoinQueryOperator<User> syncEntity = new SyncEntityJoinQueryOperator<>(fixture.sync, fixture.conditions, User.class);
            EntityJoinQueryOperator<User> reactiveEntity = new EntityJoinQueryOperator<>(fixture.reactive, fixture.conditions, User.class);
            SyncJoinQueryOperator syncForm = new SyncJoinQueryOperator(fixture.sync, fixture.conditions, users);
            JoinQueryOperator reactiveForm = new JoinQueryOperator(fixture.reactive, fixture.conditions, users);
            switch (kind) {
                case "inner" -> {
                    syncEntity.join(Order.class, typed); reactiveEntity.join(Order.class, typed);
                    syncForm.join(orders, dynamic); reactiveForm.join(orders, dynamic);
                }
                case "left outer" -> {
                    syncEntity.leftJoin(Order.class, typed); reactiveEntity.leftJoin(Order.class, typed);
                    syncForm.leftJoin(orders, dynamic); reactiveForm.leftJoin(orders, dynamic);
                }
                case "right outer" -> {
                    syncEntity.rightJoin(Order.class, typed); reactiveEntity.rightJoin(Order.class, typed);
                    syncForm.rightJoin(orders, dynamic); reactiveForm.rightJoin(orders, dynamic);
                }
                default -> {
                    syncEntity.fullJoin(Order.class, typed); reactiveEntity.fullJoin(Order.class, typed);
                    syncForm.fullJoin(orders, dynamic); reactiveForm.fullJoin(orders, dynamic);
                }
            }
            Flux<DynamicRow> entityRows = reactiveEntity.selectAs(User.class, User::id, "uid").executeRows();
            assertThrows(IllegalStateException.class, () -> reactiveEntity.andOn(User::id, Order::userId));
            assertTrue(fixture.requests.isEmpty());
            entityRows.collectList().block();
            reactiveForm.selectAs(users, "id", "uid").executeRows().collectList().block();
            syncEntity.selectAs(User.class, User::id, "uid").executeRows();
            syncForm.selectAs(users, "id", "uid").executeRows();
            assertEquals(4, fixture.requests.size());
            assertTrue(fixture.requests.getFirst().sql().contains(kind + " join"));
            fixture.requests.forEach(request -> assertEquals(fixture.requests.getFirst().sql(), request.sql()));
            fixture.requests.forEach(request -> assertEquals(List.of("A"), request.parameters()));
        }
    }

    @Test
    void fullJoinEqualityOverloadsUseTheSameRelationInAllFourFacades() {
        Fixture fixture = new Fixture();
        com.flying.orm.core.form.DynamicForm users = fixture.sync.entityModels().metadata(User.class).toDynamicForm();
        com.flying.orm.core.form.DynamicForm orders = fixture.sync.entityModels().metadata(Order.class).toDynamicForm();
        new SyncEntityJoinQueryOperator<>(fixture.sync, fixture.conditions, User.class)
                .fullJoin(Order.class, User::id, Order::userId).selectAs(User.class, User::id, "uid").executeRows();
        new EntityJoinQueryOperator<>(fixture.reactive, fixture.conditions, User.class)
                .fullJoin(Order.class, User::id, Order::userId).selectAs(User.class, User::id, "uid")
                .executeRows().collectList().block();
        new SyncJoinQueryOperator(fixture.sync, fixture.conditions, users)
                .fullJoin(orders, "id", "user_id").selectAs(users, "id", "uid").executeRows();
        new JoinQueryOperator(fixture.reactive, fixture.conditions, users)
                .fullJoin(orders, "id", "user_id").selectAs(users, "id", "uid").executeRows().collectList().block();
        assertEquals(4, fixture.requests.size());
        String sql = fixture.requests.getFirst().sql();
        assertTrue(sql.contains("full outer join"), sql);
        fixture.requests.forEach(request -> assertEquals(sql, request.sql()));
    }

    @Test
    void typedOnMapsEnumsAndUsesTheSameSqlForBothExecutors() {
        Fixture fixture = new Fixture();
        EntityJoinQueryOperator<User> reactive = new EntityJoinQueryOperator<>(fixture.reactive, fixture.conditions, User.class);
        Flux<DynamicRow> pending = reactive.leftJoin(Order.class, (user, order) -> JoinCondition.and(
                        JoinCondition.compare(user.field(User::id), "=", order.field(Order::userId)),
                        JoinCondition.compare(order.field(Order::quantity), ">=", user.field(User::minimum)),
                        order.where(where -> where.where(Order::status, State.ACTIVE))))
                .selectAs(User.class, User::id, "uid").executeRows();
        assertTrue(fixture.requests.isEmpty());
        pending.collectList().block();
        new SyncEntityJoinQueryOperator<>(fixture.sync, fixture.conditions, User.class)
                .leftJoin(Order.class, (user, order) -> JoinCondition.and(
                        JoinCondition.compare(user.field(User::id), "=", order.field(Order::userId)),
                        JoinCondition.compare(order.field(Order::quantity), ">=", user.field(User::minimum)),
                        order.where(where -> where.where(Order::status, State.ACTIVE))))
                .selectAs(User.class, User::id, "uid").executeRows();
        assertEquals(fixture.requests.getFirst().sql(), fixture.requests.getLast().sql());
        assertEquals(List.of("A"), fixture.requests.getFirst().parameters());
        assertEquals(fixture.requests.getFirst().parameters(), fixture.requests.getLast().parameters());
        assertTrue(fixture.requests.getFirst().sql().contains("\"t1\".\"user_id\""));
    }

    @Test
    void dynamicOnIsSharedByRowsAndReportsWithoutAnAlternateRenderer() {
        Fixture fixture = new Fixture();
        com.flying.orm.core.form.DynamicForm users = fixture.sync.entityModels().metadata(User.class).toDynamicForm();
        com.flying.orm.core.form.DynamicForm orders = fixture.sync.entityModels().metadata(Order.class).toDynamicForm();
        new SyncJoinQueryOperator(fixture.sync, fixture.conditions, users).leftJoin(orders, (user, order) -> JoinCondition.and(
                JoinCondition.compare(user.field("id"), "=", order.field("user_id")),
                JoinCondition.where(order, ConditionGroup.and().where("status", "=", "A").build())))
                .aggregate(report -> report.aggregate(AggregateExpression.countRows("total")));
        new JoinQueryOperator(fixture.reactive, fixture.conditions, users).leftJoin(orders, (user, order) -> JoinCondition.and(
                JoinCondition.compare(user.field("id"), "=", order.field("user_id")),
                JoinCondition.where(order, ConditionGroup.and().where("status", "=", "A").build())))
                .aggregate(report -> report.aggregate(AggregateExpression.countRows("total"))).collectList().block();
        assertEquals(fixture.requests.getFirst().sql(), fixture.requests.getLast().sql());
        assertEquals(List.of("A"), fixture.requests.getFirst().parameters());
        assertTrue(fixture.requests.getFirst().sql().contains("on (\"t0\".\"id\" = \"t1\".\"user_id\" and (\"t1\".\"status\" = ?))"));
    }

    @TableName("users")
    public record User(Long id, Integer minimum) { }
    @TableName("orders")
    public record Order(@TableField("user_id") Long userId, Integer quantity, State status) { }
    public enum State {
        ACTIVE("A"), DISABLED("D");
        @EnumValue private final String code;
        State(String code) { this.code = code; }
    }

    private static final class Fixture {
        private final List<SqlRequest> requests = new ArrayList<>();
        private final SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms().build();
        private final FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql());
        private final ReactiveFormClient reactive = ReactiveFormClient.create(new ReactiveSqlExecutor() {
            public Flux<DynamicRow> query(SqlRequest request) { return Flux.defer(() -> { requests.add(request); return Flux.empty(); }); }
            public Mono<Long> rowsUpdated(SqlRequest request) { return Mono.error(new UnsupportedOperationException()); }
        }, renderer);
        private final SyncFormClient sync = SyncFormClient.create(new com.flying.orm.rdb.support.ListQueryFixture() {
            public List<DynamicRow> query(SqlRequest request) { requests.add(request); return List.of(); }
            public long rowsUpdated(SqlRequest request) { throw new UnsupportedOperationException(); }
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) { throw new UnsupportedOperationException(); }
        }, (request, completed) -> { throw new UnsupportedOperationException(); }, renderer);
    }
}
