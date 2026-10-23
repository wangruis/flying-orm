package com.flying.orm.consumer;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.MaskedField;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.join.JoinCondition;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.operator.EntityAggregate;
import com.flying.orm.rdb.operator.EntityJoinQueryOperator;
import com.flying.orm.rdb.operator.EntityJoinSource;
import com.flying.orm.rdb.operator.SyncEntityJoinQueryOperator;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Public report calls execute real SQL; fixtures do not replace the SQL or result decoder. */
class JoinedReportExecutionContractTest {

    @TestFactory
    Stream<DynamicTest> joinedReportsKeepMappingScopeMaskingAndFinalGroupTotals() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest(
                reactive ? "R2DBC joined report" : "JDBC joined report", () -> verify(reactive)));
    }

    private static void verify(boolean reactive) throws Exception {
        String database = "joined_report_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + database);
        source.setUser("sa");
        source.setPassword("");
        ConnectionFactory factory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.DRIVER, "h2").option(ConnectionFactoryOptions.PROTOCOL, "mem")
                .option(ConnectionFactoryOptions.DATABASE, database).option(ConnectionFactoryOptions.USER, "sa")
                .option(ConnectionFactoryOptions.PASSWORD, "").build());
        try (Connection keeper = source.getConnection(); Statement statement = keeper.createStatement();
             FlyingOrmClients clients = FlyingOrmClients.builder(ConnectionAccessTestSupport.jdbc(source),
                     ConnectionAccessTestSupport.reactive(factory)).configuredDialect("h2").build()) {
            statement.execute("""
                    create table report_customers(id bigint, tenant_id bigint, display_name varchar)
                    """);
            statement.execute("""
                    create table report_orders(customer_id bigint, tenant_id bigint,
                        order_amount decimal(12,2), quantity integer, status varchar)
                    """);
            statement.execute("""
                    insert into report_customers values (1,7,'Ada'),(2,7,'Bob'),(3,7,'Eve'),(4,9,'Mallory')
                    """);
            statement.execute("""
                    insert into report_orders values
                        (1,7,10,2,'A'),(1,7,null,2,'A'),(1,7,100,2,'D'),(1,9,500,2,'A'),
                        (2,7,15,2,'A'),(2,7,2,3,'A'),(4,9,999,2,'A')
                    """);

            PageResult<AggregateRow> page = page(clients, reactive, true, true);
            assertEquals(2, page.total(), "count final HAVING groups, not joined rows");
            assertEquals(1, page.rows().size());
            assertRow(page.rows().getFirst(), 1L, "Ada", 2L, 1L, "20.00");

            PageResult<AggregateRow> all = page(clients, reactive, false, true);
            assertEquals(3, all.total(), "LEFT JOIN preserves the customer without an order");
            assertEquals(3, all.rows().size());
            assertRow(all.rows().get(0), 2L, "Bob", 2L, 2L, "36.00");
            assertRow(all.rows().get(1), 1L, "Ada", 2L, 1L, "20.00");
            assertRow(all.rows().get(2), 3L, "Eve", 1L, 0L, "0.00");

            PageResult<AggregateRow> masked = page(clients, reactive, true, false);
            assertEquals(page.total(), masked.total());
            List<Object> visible = page.rows().getFirst().values();
            List<Object> hidden = masked.rows().getFirst().values();
            assertEquals(visible.getFirst(), hidden.getFirst());
            assertEquals("A***", hidden.get(1), "grouped names retain the declared prefix and suffix policy");
            assertEquals(visible.subList(2, visible.size()), hidden.subList(2, hidden.size()));
            verifyPreferredOrder(clients, reactive);
            verifyEnumOrder(clients, reactive);
        }
    }

    private static void verifyPreferredOrder(FlyingOrmClients clients, boolean reactive) {
        Consumer<EntityAggregate> report = spec -> {
            spec.group(Customer.class, Customer::name, "name");
            spec.sum(spec.field(Order.class, Order::amount).coalesce(ValueExpression.value(BigDecimal.ZERO)), "gross");
            spec.having(ConditionGroup.and().where("gross", ">=", BigDecimal.ZERO).build());
            spec.orderByFirst("name", "Eve", "Ada").orderByLast("gross", BigDecimal.ZERO);
        };
        PageResult<AggregateRow> page = reactive
                ? clients.operator().dml().joinQuery(Customer.class).leftJoin(Order.class, JoinedReportExecutionContractTest::on)
                        .scope(Customer.class, DataScope.tenant("tenant_id", 7L)).showSensitive().aggregatePage(report, 2, 1).block()
                : clients.syncOperator().dml().joinQuery(Customer.class).leftJoin(Order.class, JoinedReportExecutionContractTest::on)
                        .scope(Customer.class, DataScope.tenant("tenant_id", 7L)).showSensitive().aggregatePage(report, 2, 1);
        assertEquals(3, page.total());
        assertEquals("Ada", page.rows().getFirst().values().getFirst());
        assertEquals(2, page.rows().getFirst().values().size());
    }

    private static void verifyEnumOrder(FlyingOrmClients clients, boolean reactive) {
        Consumer<EntityAggregate> report = spec -> {
            spec.orderByFirst("state", State.DISABLED);
            spec.group(Order.class, Order::status, "state");
            spec.countRows("rows");
            spec.orderBy(PageSort.desc("rows"));
        };
        PageResult<AggregateRow> page = reactive
                ? clients.repository(Order.class).createQuery().aggregatePage(report, 1, 1).block()
                : clients.syncRepository(Order.class).createQuery().aggregatePage(report, 1, 1);
        assertEquals(2, page.total());
        assertEquals(List.of("D", 1L), page.rows().getFirst().values());
    }

    private static PageResult<AggregateRow> page(FlyingOrmClients clients, boolean reactive,
                                                 boolean filtered, boolean sensitive) {
        Consumer<EntityAggregate> report = spec -> {
            spec.group(Customer.class, Customer::id, "customer");
            spec.group(Customer.class, Customer::name, "name");
            spec.countRows("rows");
            spec.count(Order.class, Order::amount, "nonnull");
            spec.sum(spec.field(Order.class, Order::amount).coalesce(ValueExpression.value(BigDecimal.ZERO))
                    .multiply(spec.field(Order.class, Order::quantity).coalesce(ValueExpression.value(1))), "gross");
            if (filtered) {
                spec.having(ConditionGroup.and().where("gross", ">", BigDecimal.ZERO).build());
            }
            spec.orderBy(PageSort.desc("gross"), PageSort.asc("customer"));
        };
        int number = filtered ? 2 : 1;
        int size = filtered ? 1 : 10;
        if (reactive) {
            EntityJoinQueryOperator<Customer> query = clients.operator().dml().joinQuery(Customer.class)
                    .leftJoin(Order.class, JoinedReportExecutionContractTest::on)
                    .scope(Customer.class, DataScope.tenant("tenant_id", 7L));
            if (sensitive) query.showSensitive();
            Mono<PageResult<AggregateRow>> pending = query.aggregatePage(report, number, size);
            PageResult<AggregateRow> first = pending.block();
            PageResult<AggregateRow> repeated = pending.block();
            assertNotNull(first);
            assertNotNull(repeated);
            assertEquals(first.total(), repeated.total());
            assertEquals(first.rows().stream().map(AggregateRow::values).toList(),
                    repeated.rows().stream().map(AggregateRow::values).toList());
            return first;
        }
        SyncEntityJoinQueryOperator<Customer> query = clients.syncOperator().dml().joinQuery(Customer.class)
                .leftJoin(Order.class, JoinedReportExecutionContractTest::on)
                .scope(Customer.class, DataScope.tenant("tenant_id", 7L));
        if (sensitive) query.showSensitive();
        return query.aggregatePage(report, number, size);
    }

    private static JoinCondition on(EntityJoinSource<Customer> customer, EntityJoinSource<Order> order) {
        return JoinCondition.and(
                JoinCondition.compare(customer.field(Customer::id), "=", order.field(Order::customerId)),
                JoinCondition.compare(customer.field(Customer::tenantId), "=", order.field(Order::tenantId)),
                order.where(where -> where.where(Order::status, State.ACTIVE)));
    }

    private static void assertRow(AggregateRow row, long id, String name, long rows, long nonnull, String gross) {
        List<Object> values = row.values();
        assertEquals(List.of(id, name, rows, nonnull), values.subList(0, 4));
        assertEquals(0, new BigDecimal(gross).compareTo((BigDecimal) values.get(4)));
    }

    @TableName("report_customers")
    public record Customer(Long id, Long tenantId,
                           @TableField("display_name") @MaskedField(prefix = 1, suffix = 0) String name) { }

    @TableName("report_orders")
    public record Order(Long customerId, Long tenantId, @TableField("order_amount") BigDecimal amount,
                        Integer quantity, State status) { }

    public enum State {
        ACTIVE("A"), DISABLED("D");
        @EnumValue private final String code;
        State(String code) { this.code = code; }
    }
}
