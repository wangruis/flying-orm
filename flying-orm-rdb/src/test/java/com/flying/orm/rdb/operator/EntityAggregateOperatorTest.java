package com.flying.orm.rdb.operator;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.join.JoinCondition;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.aggregate.GroupSelection;
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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class EntityAggregateOperatorTest {
    @Test
    void expressionGroupsUseMappedColumnsOnSingleAndJoinedReactiveAndSyncQueries() {
        Fixture fixture = new Fixture();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("bucket", new BigDecimal("3"));
        result.put("total", new BigDecimal("8"));
        fixture.result = List.of(DynamicRow.copyOf(result));
        Consumer<EntityAggregate> report = spec -> {
            spec.group(spec.field(Order.class, Order::amount).add(com.flying.orm.core.expression.ValueExpression.value(1)), "bucket");
            spec.function("total", "total", com.flying.orm.core.type.LogicalType.DECIMAL,
                    BigDecimal.class, spec.field(Order.class, Order::amount));
        };
        Flux<AggregateRow> pending = EntityDmlOperator.create(fixture.reactive, fixture.conditions, Order.class)
                .query().aggregate(report);
        assertTrue(fixture.requests.isEmpty());
        assertEquals(new BigDecimal("3"), pending.single().block().get("bucket", BigDecimal.class));
        SqlRequest reactive = fixture.last();
        assertEquals(List.of(1), reactive.parameters());
        assertTrue(reactive.sql().contains("sum(\"flying_group_input\".\"__fo_field_0\")"));
        assertTrue(reactive.sql().contains("\"order_amount\""));
        SyncEntityDmlOperator.create(fixture.sync, fixture.conditions, Order.class).query().aggregate(report);
        assertEquals(reactive.sql(), fixture.last().sql());
        fixture.reactiveJoin().aggregate(report).collectList().block();
        SqlRequest joined = fixture.last();
        fixture.syncJoin().aggregate(report);
        assertEquals(joined.sql(), fixture.last().sql());
        assertEquals(List.of(1, 1L, "A"), joined.parameters());
        assertTrue(joined.sql().contains("\"t1\".\"order_amount\""));
    }

    @Test
    void singleEntitySelectorsDoNotRequireRepeatedEntityTypes() {
        Class<?> property = com.flying.orm.core.lambda.EntityProperty.class;
        for (String name : List.of("group", "count", "countDistinct", "sum", "avg",
                "stddevPop", "stddevSample", "variancePop", "varianceSample")) {
            assertDoesNotThrow(() -> EntityAggregate.class.getMethod(name, property, String.class), name);
        }
        assertDoesNotThrow(() -> EntityAggregate.class.getMethod("field", property));
        for (String name : List.of("min", "max")) {
            assertDoesNotThrow(() -> EntityAggregate.class.getMethod(name, property, String.class, Class.class), name);
        }
    }

    @Test
    void directGettersPreserveMappedSqlEnumValuesAndBothExecutionStyles() {
        Fixture fixture = new Fixture();
        Consumer<EntityAggregate> report = spec -> {
            spec.group(Order::status, "state");
            spec.count(Order::amount, "nonnull");
            spec.countDistinct(Order::amount, "distincts");
            spec.sum(Order::amount, "gross");
            spec.avg(Order::amount, "average");
            spec.stddevPop(Order::amount, "dp");
            spec.stddevSample(Order::amount, "ds");
            spec.variancePop(Order::amount, "vp");
            spec.varianceSample(Order::amount, "vs");
            spec.min(Order::status, "low", String.class);
            spec.max(Order::status, "high", String.class);
            spec.sum(spec.field(Order::amount), "computed");
            spec.orderByFirst("state", State.DISABLED);
            spec.orderByLast("low", State.ACTIVE);
        };
        SyncEntityDmlOperator.create(fixture.sync, fixture.conditions, Order.class).query()
                .where(Order::status, State.ACTIVE).aggregate(report);
        SqlRequest sync = fixture.last();
        assertEquals(List.of("A", "D", "A"), sync.parameters());
        assertTrue(sync.sql().contains("sum(\"order_amount\")"));
        assertTrue(sync.sql().contains("count(distinct \"order_amount\")"));
        EntityDmlOperator.create(fixture.reactive, fixture.conditions, Order.class).query()
                .where(Order::status, State.ACTIVE).aggregate(report).collectList().block();
        assertEquals(sync.sql(), fixture.last().sql());
        assertEquals(sync.parameters(), fixture.last().parameters());
        SyncEntityDmlOperator.create(fixture.sync, fixture.conditions, Order.class).query().aggregatePage(report, 1, 5);
        SqlRequest page = fixture.last();
        EntityDmlOperator.create(fixture.reactive, fixture.conditions, Order.class).query()
                .aggregatePage(report, 1, 5).block();
        assertEquals(page.sql(), fixture.last().sql());
        assertEquals(page.parameters(), fixture.last().parameters());
    }

    @Test
    void directGettersDoNotGuessForeignOrJoinedSourcesOrBypassFieldScope() {
        Fixture fixture = new Fixture();
        assertThrows(com.flying.orm.rdb.mapping.MappingException.class,
                () -> SyncEntityDmlOperator.create(fixture.sync, fixture.conditions, Order.class).query()
                        .aggregate(report -> report.count(Customer::id, "n")));
        assertThrows(IllegalArgumentException.class,
                () -> fixture.syncJoin().aggregate(report -> report.sum(Order::amount, "gross")));
        assertThrows(IllegalArgumentException.class,
                () -> fixture.reactiveJoin().aggregate(report -> report.sum(Order::amount, "gross")));
        assertThrows(RuntimeException.class,
                () -> SyncEntityDmlOperator.create(fixture.sync, fixture.conditions, Order.class).query()
                        .scope(DataScope.none().withFields(FieldScope.readable("customer_id")))
                        .aggregate(report -> report.sum(Order::amount, "gross")));
        assertTrue(fixture.requests.isEmpty());
    }

    @Test
    void inheritedGetterUsesTheConfiguredEntityRatherThanTheDeclaringClass() {
        Fixture fixture = new Fixture();
        SyncEntityDmlOperator.create(fixture.sync, fixture.conditions, ChildEntity.class).query()
                .aggregate(report -> { report.group(ParentEntity::getOrgId, "org"); report.countRows("rows"); });
        SqlRequest direct = fixture.last();
        SyncEntityDmlOperator.create(fixture.sync, fixture.conditions, ChildEntity.class).query()
                .aggregate(report -> { report.group(ChildEntity.class, ParentEntity::getOrgId, "org"); report.countRows("rows"); });
        assertEquals(direct.sql(), fixture.last().sql());
        assertTrue(direct.sql().contains("\"child_rows\""));
        assertTrue(direct.sql().contains("\"org_id\""));
    }

    public static class ParentEntity {
        @TableField("org_id") private Long orgId;
        public Long getOrgId() { return orgId; }
    }
    @TableName("child_rows")
    public static final class ChildEntity extends ParentEntity { }

    @Test
    void reportAliasPrioritiesUseEntityEnumStorageOnAllFourEntrypoints() {
        Fixture fixture = new Fixture();
        Consumer<EntityAggregate> report = spec -> {
            spec.orderByFirst("STATE", State.DISABLED, State.ACTIVE);
            spec.group(Order.class, Order::status, "state");
            spec.min(Order.class, Order::status, "lowest", String.class);
            spec.max(spec.field(Order.class, Order::status), "highest", com.flying.orm.core.type.LogicalType.TEXT, String.class);
            spec.orderByLast("lowest", State.DISABLED);
            spec.orderBy(com.flying.orm.core.expression.ValueExpression.firstMatch(
                    com.flying.orm.core.expression.ValueExpression.field("highest").priority(State.ACTIVE),
                    com.flying.orm.core.expression.ValueExpression.lastMatch(
                            com.flying.orm.core.expression.ValueExpression.field("state").priority(State.DISABLED))), PageSort.Direction.ASC);
        };
        SyncEntityDmlOperator.create(fixture.sync, fixture.conditions, Order.class).query().aggregate(report);
        assertEquals(List.of("D", "A", "D", "A", "D"), fixture.last().parameters());
        EntityDmlOperator.create(fixture.reactive, fixture.conditions, Order.class).query().aggregate(report).collectList().block();
        assertEquals(List.of("D", "A", "D", "A", "D"), fixture.last().parameters());
        fixture.syncJoin().aggregate(report);
        assertEquals(List.of(1L, "A", "D", "A", "D", "A", "D"), fixture.last().parameters());
        fixture.reactiveJoin().aggregate(report).collectList().block();
        assertEquals(List.of(1L, "A", "D", "A", "D", "A", "D"), fixture.last().parameters());
    }

    @Test
    void statisticalReportsUseMappedGettersAndComputedOperandsOnBothExecutionStyles() {
        Fixture fixture = new Fixture();
        fixture.result = List.of(DynamicRow.copyOf(ordered("dp", 1d, "ds", 2d, "vp", 3d, "vs", 4d)));
        Consumer<EntityAggregate> direct = spec -> {
            spec.stddevPop(Order.class, Order::amount, "dp");
            spec.stddevSample(Order.class, Order::amount, "ds");
            spec.variancePop(Order.class, Order::amount, "vp");
            spec.varianceSample(Order.class, Order::amount, "vs");
        };
        Consumer<EntityAggregate> computed = spec -> {
            com.flying.orm.core.expression.ValueExpression amount = spec.field(Order.class, Order::amount)
                    .coalesce(com.flying.orm.core.expression.ValueExpression.value(BigDecimal.ZERO));
            spec.stddevPop(amount, "dp");
            spec.stddevSample(amount, "ds");
            spec.variancePop(amount, "vp");
            spec.varianceSample(amount, "vs");
        };
        List<Object> expected = List.of(new BigDecimal("1.0"), new BigDecimal("2.0"),
                new BigDecimal("3.0"), new BigDecimal("4.0"));
        for (Consumer<EntityAggregate> report : List.of(direct, computed)) {
            Mono<AggregateRow> pending = EntityDmlOperator.create(fixture.reactive, fixture.conditions, Order.class)
                    .query().aggregate(report).single();
            int requestsBefore = fixture.requests.size();
            assertEquals(expected, pending.block().values());
            assertEquals(requestsBefore + 1, fixture.requests.size());
            assertEquals(expected, SyncEntityDmlOperator.create(fixture.sync, fixture.conditions, Order.class)
                    .query().aggregate(report).getFirst().values());
            assertEquals(expected, fixture.syncJoin().aggregate(report).getFirst().values());
            assertEquals(expected, fixture.reactiveJoin().aggregate(report).blockFirst().values());
            assertTrue(fixture.last().sql().contains("\"t1\".\"order_amount\""));
        }
    }

    @Test
    void joinedSameNamedEnumColumnsKeepTheirOwnStorageAndNumericResultsDoNotInheritIt() {
        Fixture fixture = new Fixture();
        Consumer<EntityAggregate> report = spec -> {
            spec.group(Order.class, Order::status, "first_state");
            spec.group(OtherOrder.class, OtherOrder::status, "second_state");
            spec.count(Order.class, Order::status, "count");
            spec.orderByFirst("first_state", State.DISABLED);
            spec.orderByFirst("second_state", OtherState.DISABLED);
            spec.orderByFirst("count", 2L);
        };
        new SyncEntityJoinQueryOperator<>(fixture.sync, fixture.conditions, Order.class)
                .leftJoin(OtherOrder.class, Order::customerId, OtherOrder::id).aggregate(report);
        assertEquals(List.of("D", "off", 2L), fixture.last().parameters());
        new EntityJoinQueryOperator<>(fixture.reactive, fixture.conditions, Order.class)
                .leftJoin(OtherOrder.class, Order::customerId, OtherOrder::id).aggregate(report).collectList().block();
        assertEquals(List.of("D", "off", 2L), fixture.last().parameters());
    }

    @Test
    void persistentValuesAndNullKeepTheirExistingRepresentationAndPublishedReportIsFrozen() {
        Fixture fixture = new Fixture();
        AtomicReference<EntityAggregate> captured = new AtomicReference<>();
        Flux<AggregateRow> pending = EntityDmlOperator.create(fixture.reactive, fixture.conditions, Order.class).query()
                .aggregate(spec -> {
                    captured.set(spec);
                    spec.group(Order.class, Order::status, "state");
                    spec.countRows("rows");
                    spec.orderByFirst("state", "D", null);
                });
        captured.get().orderByFirst("state", State.ACTIVE);
        pending.collectList().block();
        assertEquals("D", fixture.last().parameters().getFirst());
        assertEquals(2, fixture.last().parameters().size());
        assertEquals(new com.flying.orm.rdb.internal.binding.SqlNullParameter(String.class), fixture.last().parameters().getLast());
        pending.collectList().block();
        assertEquals(2, fixture.last().parameters().size());
    }

    @Test
    void registeredAggregatesUseMappedGettersAndBothExecutionStyles() {
        Fixture fixture = new Fixture();
        fixture.result = List.of(DynamicRow.copyOf(Map.of("gross", "21.50")));
        AtomicReference<AggregateExpression<BigDecimal>> handle = new AtomicReference<>();
        Consumer<EntityAggregate> report = spec -> handle.set(spec.function("total", "gross",
                com.flying.orm.core.type.LogicalType.DECIMAL, BigDecimal.class,
                spec.field(Order.class, Order::amount)
                        .coalesce(com.flying.orm.core.expression.ValueExpression.value(BigDecimal.ZERO))));
        Mono<AggregateRow> pending = EntityDmlOperator.create(fixture.reactive, fixture.conditions, Order.class)
                .query().aggregate(report).single();
        assertTrue(fixture.requests.isEmpty());
        assertEquals(new BigDecimal("21.50"), pending.block().get(handle.get()));
        assertEquals(new BigDecimal("21.50"), pending.block().get(handle.get()));
        assertEquals(new BigDecimal("21.50"), SyncEntityDmlOperator.create(fixture.sync, fixture.conditions, Order.class)
                .query().aggregate(report).getFirst().get(handle.get()));
        assertTrue(fixture.last().sql().contains("sum(coalesce(\"order_amount\", ?))"));
        assertEquals(List.of(BigDecimal.ZERO), fixture.last().parameters());
        assertEquals(new BigDecimal("21.50"), fixture.syncJoin().aggregate(report).getFirst().get(handle.get()));
        assertEquals(new BigDecimal("21.50"), fixture.reactiveJoin().aggregate(report).blockFirst().get(handle.get()));
        assertTrue(fixture.last().sql().contains("sum(coalesce(\"t1\".\"order_amount\", ?))"));
        assertEquals(List.of(BigDecimal.ZERO, 1L, "A"), fixture.last().parameters());
    }

    @Test
    void computedExtremaAndCountsUseTheSameEntityAndJoinFacade() {
        Fixture fixture = new Fixture();
        fixture.result = List.of(DynamicRow.copyOf(ordered("nonnull", 3, "distincts", 2, "smallest", "5.0", "largest", "20.5")));
        Consumer<EntityAggregate> report = spec -> {
            com.flying.orm.core.expression.ValueExpression amount = spec.field(Order.class, Order::amount)
                    .coalesce(com.flying.orm.core.expression.ValueExpression.value(BigDecimal.ZERO));
            spec.count(amount, "nonnull");
            spec.countDistinct(amount, "distincts");
            spec.min(amount, "smallest", com.flying.orm.core.type.LogicalType.DECIMAL, BigDecimal.class);
            spec.max(amount, "largest", com.flying.orm.core.type.LogicalType.DECIMAL, BigDecimal.class);
        };
        List<Object> expected = List.of(3L, 2L, new BigDecimal("5.0"), new BigDecimal("20.5"));
        assertEquals(expected, SyncEntityDmlOperator.create(fixture.sync, fixture.conditions, Order.class)
                .query().aggregate(report).getFirst().values());
        assertEquals(expected, EntityDmlOperator.create(fixture.reactive, fixture.conditions, Order.class)
                .query().aggregate(report).blockFirst().values());
        assertEquals(expected, fixture.syncJoin().aggregate(report).getFirst().values());
        assertEquals(expected, fixture.reactiveJoin().aggregate(report).blockFirst().values());
        assertTrue(fixture.last().sql().contains("min(coalesce(\"t1\".\"order_amount\", ?))"));
        assertEquals(List.of(BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, 1L, "A"),
                fixture.last().parameters());
    }

    @Test
    void computedReportUsesMappedGettersOnSingleAndJoinedReactiveAndSyncQueries() {
        Fixture fixture = new Fixture();
        fixture.result = List.of(DynamicRow.copyOf(Map.of("gross", "21.50")));
        AtomicReference<AggregateExpression<BigDecimal>> handle = new AtomicReference<>();
        Consumer<EntityAggregate> report = spec -> handle.set(spec.sum(
                spec.field(Order.class, Order::amount)
                        .coalesce(com.flying.orm.core.expression.ValueExpression.value(BigDecimal.ZERO))
                        .multiply(com.flying.orm.core.expression.ValueExpression.value(2)), "gross"));
        Mono<AggregateRow> pending = EntityDmlOperator.create(fixture.reactive, fixture.conditions, Order.class)
                .query().aggregate(report).single();
        assertTrue(fixture.requests.isEmpty());
        assertEquals(new BigDecimal("21.50"), pending.block().get(handle.get()));
        SqlRequest reactive = fixture.last();
        assertEquals(new BigDecimal("21.50"), SyncEntityDmlOperator.create(fixture.sync, fixture.conditions, Order.class)
                .query().aggregate(report).getFirst().get(handle.get()));
        assertEquals(reactive.sql(), fixture.last().sql());
        assertEquals(List.of(BigDecimal.ZERO, 2), fixture.last().parameters());
        fixture.syncJoin().aggregate(report);
        assertTrue(fixture.last().sql().contains("coalesce(\"t1\".\"order_amount\", ?)"));
        fixture.reactiveJoin().aggregate(report).blockLast();
        assertTrue(fixture.last().sql().contains("coalesce(\"t1\".\"order_amount\", ?)"));
    }

    @Test
    void reportGroupsAndExtremaRetainDeclaredAndExplicitMasking() {
        Fixture f = new Fixture();
        f.result = List.of(DynamicRow.copyOf(ordered("name", "Ada", "first", "Ada")));
        Consumer<EntityAggregate> report = spec -> {
            spec.group(PrivateCustomer.class, PrivateCustomer::name, "name");
            spec.min(PrivateCustomer.class, PrivateCustomer::name, "first", String.class);
        };
        List<Object> masked = SyncEntityDmlOperator.create(f.sync, f.conditions, PrivateCustomer.class).query()
                .aggregate(report).getFirst().values();
        assertNotEquals("Ada", masked.getFirst());
        assertNotEquals("Ada", masked.getLast());
        assertEquals(List.of("Ada", "Ada"), EntityDmlOperator.create(f.reactive, f.conditions, PrivateCustomer.class)
                .query().showSensitive().aggregate(report).single().block().values());
        assertEquals(masked, EntityDmlOperator.create(f.reactive, f.conditions, PrivateCustomer.class)
                .query().masked().aggregate(report).single().block().values());
    }

    @Test
    void callerExecutionOptionsReachTheExistingExecutorWithoutNewCaps() {
        Fixture f = new Fixture();
        f.result = List.of(DynamicRow.copyOf(Map.of("gross", "1")), DynamicRow.copyOf(Map.of("gross", "2")));
        Consumer<EntityAggregate> limited = report -> {
            report.sum(Order.class, Order::amount, "gross");
            report.executionOptions(SqlExecutionOptions.maxRows(1));
        };
        assertThrows(com.flying.orm.rdb.execution.SqlRowLimitExceededException.class,
                () -> SyncEntityDmlOperator.create(f.sync, f.conditions, Order.class).query().aggregate(limited));
        assertThrows(com.flying.orm.rdb.execution.SqlRowLimitExceededException.class,
                () -> EntityDmlOperator.create(f.reactive, f.conditions, Order.class).query().aggregate(limited).collectList().block());
        assertEquals(2, f.syncJoin().aggregate(report -> {
            report.sum(Order.class, Order::amount, "gross"); report.executionOptions(SqlExecutionOptions.unlimited());
        }).size());
    }

    @Test
    void joinedReportPageRetainsOnEnumsSourceScopeAndMappedColumns() {
        Fixture f = new Fixture();
        f.result = List.of(DynamicRow.copyOf(ordered("customer", "Ada", "gross", "25.50")));
        AtomicReference<AggregateExpression<BigDecimal>> gross = new AtomicReference<>();
        AtomicReference<GroupSelection> customer = new AtomicReference<>();
        Consumer<EntityAggregate> report = spec -> {
            customer.set(spec.group(Customer.class, Customer::name, "customer"));
            gross.set(spec.sum(Order.class, Order::amount, "gross"));
            spec.having(condition -> condition.where("gross", ">", 10))
                    .orderBy(PageSort.desc("gross"));
        };
        EntityJoinQueryOperator<Customer> query = f.reactiveJoin();
        Mono<PageResult<AggregateRow>> pending = query.aggregatePage(report, 2, 7);
        assertTrue(f.requests.isEmpty());
        assertThrows(IllegalStateException.class, () -> query.where(Customer.class, Customer::id, "=", 99L));
        PageResult<AggregateRow> reactive = pending.block();
        PageResult<AggregateRow> sync = f.syncJoin().aggregatePage(report, PageQuery.of(2, 7));
        assertEquals(3L, sync.total());
        assertEquals("Ada", sync.rows().getFirst().get(customer.get(), String.class));
        assertEquals(new BigDecimal("25.50"), sync.rows().getFirst().get(gross.get()));
        assertEquals(reactive.rows().getFirst().values(), sync.rows().getFirst().values());
        assertEquals(f.requests.get(0).sql(), f.requests.get(2).sql());
        assertEquals(f.requests.get(1).sql(), f.requests.get(3).sql());
        assertTrue(f.requests.get(0).sql().contains("having"));
        assertFalse(f.requests.get(0).sql().contains("order by"));
        assertTrue(f.requests.get(1).sql().endsWith("order by \"gross\" desc limit ? offset ?"));
        assertTrue(f.requests.get(1).sql().contains("sum(\"t1\".\"order_amount\") as \"gross\""));
        assertTrue(f.requests.get(1).sql().contains("\"t0\".\"display_name\""));
        assertEquals(List.of(1L, "A", 10), f.requests.get(0).parameters());
        assertEquals(List.of(1L, "A", 10, 7, 7L), f.requests.get(1).parameters());
        f.requests.clear();
        assertEquals(1, f.syncJoin().aggregate(report).size());
        assertEquals(1, f.reactiveJoin().aggregate(report).collectList().block().size());
        assertEquals(2, f.requests.size());
    }

    @Test
    void singleReportSnapshotsFiltersAndIgnoresEntityProjectionOrderAndLoadCallbacks() {
        Fixture f = new Fixture();
        AtomicInteger loaded = new AtomicInteger();
        AtomicReference<EntityAggregate> captured = new AtomicReference<>();
        EntityDmlQueryOperator<Order> query = EntityDmlOperator.create(f.reactive, f.conditions, Order.class).query()
                .filter(StructuredConditionInput.term("status", "eq", State.ACTIVE))
                .where(Order::customerId, ">", 0L).scope(DataScope.where(ConditionGroup.and().where("customer_id", "<", 10L).build()))
                .select(Order::customerId).exclude(Order::customerId).groupBy(Order::customerId).orderByDesc(Order::amount)
                .onLoad(row -> { loaded.incrementAndGet(); return Mono.empty(); });
        Flux<AggregateRow> pending = query.aggregate(report -> { captured.set(report); report.sum(Order.class, Order::amount, "gross"); });
        query.where(Order::customerId, "=", 999L);
        captured.get().countRows("late");
        assertTrue(f.requests.isEmpty());
        pending.blockLast();
        assertFalse(f.last().sql().contains("late"));
        assertFalse(f.last().sql().contains("group by"));
        assertFalse(f.last().sql().contains("order by"));
        assertFalse(f.last().parameters().contains(999L));
        assertTrue(f.last().parameters().containsAll(List.of("A", 0L, 10L)));
        assertEquals(0, loaded.get());
        SqlRequest reactive = f.last();
        SyncEntityDmlOperator.create(f.sync, f.conditions, Order.class).query()
                .filter(StructuredConditionInput.term("status", "eq", State.ACTIVE)).where(Order::customerId, ">", 0L)
                .scope(DataScope.where(ConditionGroup.and().where("customer_id", "<", 10L).build()))
                .aggregate(report -> report.sum(Order.class, Order::amount, "gross"));
        assertEquals(reactive.sql(), f.last().sql());
        assertEquals(reactive.parameters(), f.last().parameters());
    }

    @Test
    void allFunctionsUseMappedFieldsAndReturnExistingTypedHandles() {
        Fixture f = new Fixture();
        f.result = List.of(DynamicRow.copyOf(ordered("rows", 3, "nonnull", 2, "distincts", 2,
                "gross", "25.5", "average", "12.75", "smallest", "5.0", "largest", "20.5")));
        List<AggregateExpression<?>> handles = new ArrayList<>();
        List<AggregateRow> rows = SyncEntityDmlOperator.create(f.sync, f.conditions, Order.class).query().aggregate(report -> {
            handles.add(report.countRows("rows"));
            handles.add(report.count(Order.class, Order::amount, "nonnull"));
            handles.add(report.countDistinct(Order.class, Order::amount, "distincts"));
            handles.add(report.sum(Order.class, Order::amount, "gross"));
            handles.add(report.avg(Order.class, Order::amount, "average"));
            handles.add(report.min(Order.class, Order::amount, "smallest", BigDecimal.class));
            handles.add(report.max(Order.class, Order::amount, "largest", BigDecimal.class));
        });
        assertEquals(List.of(3L, 2L, 2L, new BigDecimal("25.5"), new BigDecimal("12.75"),
                new BigDecimal("5.0"), new BigDecimal("20.5")), rows.getFirst().values());
        for (int i = 0; i < handles.size(); i++) assertEquals(rows.getFirst().values().get(i), rows.getFirst().get(handles.get(i)));
        assertTrue(f.last().sql().contains("count(distinct \"order_amount\")"));
        assertTrue(f.last().sql().contains("min(\"order_amount\")"));
    }

    @Test
    void singleReportPagesUseBothOverloadsWithoutEntityLoading() {
        Fixture f = new Fixture();
        AtomicInteger loaded = new AtomicInteger();
        Consumer<EntityAggregate> report = spec -> { spec.sum(Order.class, Order::amount, "gross"); spec.orderBy(PageSort.desc("gross")); };
        SyncEntityDmlOperator.create(f.sync, f.conditions, Order.class).query().onLoad(row -> loaded.incrementAndGet())
                .aggregatePage(report, 1, 5);
        EntityDmlOperator.create(f.reactive, f.conditions, Order.class).query().onLoad(row -> { loaded.incrementAndGet(); return Mono.empty(); })
                .aggregatePage(report, PageQuery.of(1, 5)).block();
        assertEquals(4, f.requests.size());
        assertEquals(f.requests.get(0).sql(), f.requests.get(2).sql());
        assertEquals(f.requests.get(1).sql(), f.requests.get(3).sql());
        assertEquals(List.of(5, 0L), f.last().parameters());
        assertTrue(f.last().sql().endsWith("order by \"gross\" desc limit ? offset ?"));
        assertEquals(0, loaded.get());
    }

    @Test
    void foreignSourcesNonpersistentPropertiesAndDuplicateAliasesFailBeforeExecution() {
        Fixture f = new Fixture();
        assertThrows(IllegalArgumentException.class, () -> SyncEntityDmlOperator.create(f.sync, f.conditions, Order.class).query()
                .aggregate(report -> report.count(Customer.class, Customer::id, "n")));
        assertThrows(com.flying.orm.rdb.mapping.MappingException.class, () -> f.syncJoin()
                .aggregate(report -> report.count(Customer.class, Customer::temporary, "n")));
        assertThrows(IllegalArgumentException.class, () -> f.syncJoin().aggregate(report -> {
            report.group(Customer.class, Customer::name, "same"); report.countRows("same");
        }));
        assertTrue(f.requests.isEmpty());
    }

    @Test
    void fieldScopeAndMinCodecTypeRemainEnforcedByThePlanner() {
        Fixture f = new Fixture();
        assertThrows(RuntimeException.class, () -> SyncEntityDmlOperator.create(f.sync, f.conditions, Order.class).query()
                .scope(DataScope.none().withFields(FieldScope.readable("customer_id")))
                .aggregate(report -> report.sum(Order.class, Order::amount, "gross")));
        assertThrows(IllegalArgumentException.class, () -> f.syncJoin()
                .aggregate(report -> report.min(Order.class, Order::amount, "smallest", String.class)));
        assertTrue(f.requests.isEmpty());
    }

    private static Map<String, Object> ordered(Object... pairs) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) values.put((String) pairs[i], pairs[i + 1]);
        return values;
    }

    @TableName("customers")
    public record Customer(Long id, @TableField("display_name") String name,
                           @TableField(exist = false) String temporary) { }
    @TableName("orders")
    public record Order(@TableField("customer_id") Long customerId,
                        @TableField("order_amount") BigDecimal amount, State status) { }
    @TableName("private_customers")
    public record PrivateCustomer(Long id, @com.flying.orm.core.annotation.MaskedField(suffix = 0) String name) { }
    public enum State {
        ACTIVE("A"), DISABLED("D");
        @EnumValue private final String code;
        State(String code) { this.code = code; }
    }

    @TableName("other_orders")
    public record OtherOrder(Long id, OtherState status) { }
    public enum OtherState {
        ACTIVE("on"), DISABLED("off");
        @EnumValue private final String code;
        OtherState(String code) { this.code = code; }
    }

    private static final class Fixture {
        private final List<SqlRequest> requests = new ArrayList<>();
        private List<DynamicRow> result = List.of();
        private final SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms().expressionFunctions(
                com.flying.orm.core.sql.expression.SqlExpressionFunctions.builder()
                        .register("total", "sum", com.flying.orm.core.type.LogicalType.DECIMAL,
                                com.flying.orm.core.type.LogicalType.DECIMAL).build()).build();
        private final FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql());
        private final ReactiveFormClient reactive = ReactiveFormClient.create(new ReactiveSqlExecutor() {
            public Flux<DynamicRow> query(SqlRequest request) { return Flux.defer(() -> Flux.fromIterable(rows(request))); }
            public Mono<Long> rowsUpdated(SqlRequest request) { return Mono.error(new UnsupportedOperationException()); }
        }, renderer);
        private final SyncFormClient sync = SyncFormClient.create(new com.flying.orm.rdb.support.ListQueryFixture() {
            public List<DynamicRow> query(SqlRequest request) { return rows(request); }
            public long queryWhile(SqlRequest request, SqlExecutionOptions options,
                                   java.util.function.Predicate<? super DynamicRow> consumer) {
                long consumed = 0;
                for (DynamicRow row : rows(request)) {
                    if (options != null && options.maxRows() > 0 && consumed >= options.maxRows()) {
                        throw new com.flying.orm.rdb.execution.SqlRowLimitExceededException(
                                com.flying.orm.rdb.observation.SqlStatementType.SELECT, options.maxRows(), consumed);
                    }
                    consumed++;
                    if (!consumer.test(row)) break;
                }
                return consumed;
            }
            public long rowsUpdated(SqlRequest request) { throw new UnsupportedOperationException(); }
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) { throw new UnsupportedOperationException(); }
        }, (request, completed) -> { throw new UnsupportedOperationException(); }, renderer);
        private EntityJoinQueryOperator<Customer> reactiveJoin() {
            return new EntityJoinQueryOperator<>(reactive, conditions, Customer.class)
                    .leftJoin(Order.class, (customer, order) -> JoinCondition.and(
                            JoinCondition.compare(customer.field(Customer::id), "=", order.field(Order::customerId)),
                            order.where(where -> where.where(Order::status, State.ACTIVE))))
                    .scope(Customer.class, DataScope.where(ConditionGroup.and().where("id", ">=", 1L).build()));
        }
        private SyncEntityJoinQueryOperator<Customer> syncJoin() {
            return new SyncEntityJoinQueryOperator<>(sync, conditions, Customer.class)
                    .leftJoin(Order.class, (customer, order) -> JoinCondition.and(
                            JoinCondition.compare(customer.field(Customer::id), "=", order.field(Order::customerId)),
                            order.where(where -> where.where(Order::status, State.ACTIVE))))
                    .scope(Customer.class, DataScope.where(ConditionGroup.and().where("id", ">=", 1L).build()));
        }
        private SqlRequest last() { return requests.getLast(); }
        private List<DynamicRow> rows(SqlRequest request) {
            requests.add(request);
            return request.sql().contains("flying_aggregate_count")
                    ? List.of(DynamicRow.copyOf(Map.of("total", 3L))) : result;
        }
    }
}
