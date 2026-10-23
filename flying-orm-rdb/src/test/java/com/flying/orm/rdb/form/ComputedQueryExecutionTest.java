package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.rdb.repository.ReactiveFormRepository;
import com.flying.orm.rdb.repository.SyncFormRepository;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.lock.ReadLock;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class ComputedQueryExecutionTest {
    private static final DynamicForm FORM = DynamicForm.builder("projection_sales", "projection_sales")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("amount", "DECIMAL"))
            .addField(DynamicField.of("quantity", "INTEGER")).build();
    private static final ValueExpression SUBTOTAL = ValueExpression.field("amount")
            .multiply(ValueExpression.field("quantity").coalesce(ValueExpression.value(2)));

    @TestFactory
    Stream<DynamicTest> publicComputedQueriesUseBothExecutors() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest(
                reactive ? "R2DBC computed query" : "JDBC computed query", () -> execute(reactive)));
    }

    private void execute(boolean reactive) throws Exception {
        String name = "computed_query_" + reactive;
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + name + ";DATABASE_TO_LOWER=TRUE");
        source.setUser("sa");
        source.setPassword("");
        ConnectionFactory factory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.DRIVER, "h2").option(ConnectionFactoryOptions.PROTOCOL, "mem")
                .option(ConnectionFactoryOptions.DATABASE, name).option(ConnectionFactoryOptions.USER, "sa")
                .option(ConnectionFactoryOptions.PASSWORD, "")
                .option(io.r2dbc.h2.H2ConnectionFactoryProvider.OPTIONS, "DATABASE_TO_LOWER=TRUE").build());
        try (Connection keeper = source.getConnection(); Statement statement = keeper.createStatement();
             FlyingOrmClients clients = FlyingOrmClients.builder(ConnectionAccessTestSupport.jdbc(source),
                     ConnectionAccessTestSupport.reactive(factory)).configuredDialect("h2").build()) {
            statement.execute("create table projection_sales(id bigint primary key, amount decimal(12,2), quantity int)");
            statement.execute("insert into projection_sales values(1,10,3),(2,20,null),(3,null,5)");
            List<SaleTotal> rows;
            if (reactive) {
                Flux<SaleTotal> result = clients.operator().dml().query().from(FORM).select("id")
                        .select(SUBTOTAL, "subtotal").orderByAsc("id").fetch(SaleTotal.class);
                rows = result.collectList().block();
                assertEquals(rows, result.collectList().block(), "each subscription executes its own query");
            } else {
                rows = clients.syncOperator().dml().query().from(FORM).select("id")
                        .select(SUBTOTAL, "subtotal").orderByAsc("id").fetch(SaleTotal.class);
            }
            assertEquals(List.of(new SaleTotal(1L, new BigDecimal("30.00")),
                    new SaleTotal(2L, new BigDecimal("40.00")), new SaleTotal(3L, null)), rows);
            verifyEntityProjection(clients, reactive, rows);
            verifyEntityPropertySorting(clients, reactive);
            verifyPriorityExpression(clients, reactive);
            verifyOrderedMatches(clients, reactive);
            verifyMixedMatches(clients, reactive);
            verifyCursorProjection(clients, reactive, rows);
            verifyKeysetProjection(clients, reactive);
            verifyJoinProjection(clients, reactive);
            verifyJoinPriority(clients, reactive);
            PageResult<SaleTotal> page = reactive
                    ? clients.operator().dml().query().from(FORM).select(SUBTOTAL, "subtotal").select("id")
                            .orderByAsc("id").page(2, 1, SaleTotal.class).block()
                    : clients.syncOperator().dml().query().from(FORM).select(SUBTOTAL, "subtotal").select("id")
                            .orderByAsc("id").page(2, 1, SaleTotal.class);
            assertNotNull(page);
            assertEquals(3, page.total());
            assertEquals(List.of(rows.get(1)), page.rows());
            SaleTotal first = reactive
                    ? clients.operator().dml().query().from(FORM).select("id").select(SUBTOTAL, "subtotal")
                            .where("id", 2L).first(SaleTotal.class).block()
                    : clients.syncOperator().dml().query().from(FORM).select("id").select(SUBTOTAL, "subtotal")
                            .where("id", 2L).first(SaleTotal.class);
            assertEquals(rows.get(1), first);
            List<SaleTotal> sorted = reactive
                    ? clients.operator().dml().query().from(FORM).select("id").select(SUBTOTAL, "subtotal")
                            .where("id", "<", 3L).orderByDesc("SUBTOTAL").fetch(SaleTotal.class).collectList().block()
                    : clients.syncOperator().dml().query().from(FORM).select("id").select(SUBTOTAL, "subtotal")
                            .where("id", "<", 3L).orderByDesc("SUBTOTAL").fetch(SaleTotal.class);
            assertEquals(List.of(rows.get(1), rows.getFirst()), sorted);
            SaleTotal sortedFirst = reactive
                    ? clients.operator().dml().query().from(FORM).select("id").select(SUBTOTAL, "subtotal")
                            .where("id", "<", 3L).orderByDesc("subtotal").first(SaleTotal.class).block()
                    : clients.syncOperator().dml().query().from(FORM).select("id").select(SUBTOTAL, "subtotal")
                            .where("id", "<", 3L).orderByDesc("subtotal").first(SaleTotal.class);
            assertEquals(rows.get(1), sortedFirst);
            PageResult<SaleTotal> sortedPage = reactive
                    ? clients.operator().dml().query().from(FORM).select("id").select(SUBTOTAL, "subtotal")
                            .where("id", "<", 3L).orderByDesc("subtotal").page(2, 1, SaleTotal.class).block()
                    : clients.syncOperator().dml().query().from(FORM).select("id").select(SUBTOTAL, "subtotal")
                            .where("id", "<", 3L).orderByDesc("subtotal").page(2, 1, SaleTotal.class);
            assertNotNull(sortedPage);
            assertEquals(2, sortedPage.total());
            assertEquals(List.of(rows.getFirst()), sortedPage.rows());
            QuerySpec lockingQuery = QuerySpec.of(FORM, ConditionGroup.and().where("id", "=", 2L).build())
                    .withProjection(List.of("id"), List.of()).withExpression("subtotal", SUBTOTAL)
                    .withSorts(List.of(PageSort.desc("subtotal")));
            QuerySpec locking = lockingQuery.withLock(ReadLock.update());
            List<SaleTotal> locked = reactive
                    ? clients.forms().lockingRead(locking, SaleTotal.class).collectList().block()
                    : clients.syncForms().lockingRead(locking, SaleTotal.class);
            assertEquals(List.of(rows.get(1)), locked);
            FieldUsePolicy lockingPolicy = FieldUsePolicy.builder().visibility("id", FieldVisibility.FULL)
                    .visibility("amount", FieldVisibility.FULL).visibility("quantity", FieldVisibility.FULL)
                    .allow("id", FieldUse.FILTER).allow("amount", FieldUse.SORT).allow("quantity", FieldUse.SORT).build();
            List<SaleTotal> governedLocked = reactive
                    ? clients.forms().withFieldUsePolicy(lockingPolicy).lockingRead(locking, SaleTotal.class)
                            .collectList().block()
                    : clients.syncForms().withFieldUsePolicy(lockingPolicy).lockingRead(locking, SaleTotal.class);
            assertEquals(locked, governedLocked);
            long count = reactive
                    ? clients.operator().dml().query().from(FORM).select(SUBTOTAL, "subtotal").count().block()
                    : clients.syncOperator().dml().query().from(FORM).select(SUBTOTAL, "subtotal").count();
            assertEquals(3, count);

            FieldUsePolicy policy = FieldUsePolicy.builder().visibility("amount", FieldVisibility.FULL)
                    .visibility("quantity", FieldVisibility.FULL).build();
            QuerySpec spec = QuerySpec.of(FORM, ConditionGroup.and().build()).withExpression("subtotal", SUBTOTAL);
            List<DynamicRow> governed = reactive
                    ? clients.forms().withFieldUsePolicy(policy).select(spec).collectList().block()
                    : clients.syncForms().withFieldUsePolicy(policy).select(spec);
            assertNotNull(governed);
            assertEquals(3, governed.size());
            assertEquals(new BigDecimal("30.00"), governed.getFirst().get("subtotal"));

            QuerySpec constant = QuerySpec.of(FORM, ConditionGroup.and().where("id", "=", 1L).build())
                    .withExpression("note", ValueExpression.value("safe' value"))
                    .withExpression("missing", ValueExpression.value(null));
            DynamicRow literal = reactive ? clients.forms().first(constant).block() : clients.syncForms().first(constant);
            assertNotNull(literal);
            assertEquals("safe' value", literal.get("note"));
            assertNull(literal.get("missing"));
            assertThrows(IllegalStateException.class, () -> {
                if (reactive) clients.operator().dml().query().from("projection_sales")
                        .select(SUBTOTAL, "subtotal").first().block();
                else clients.syncOperator().dml().query().from("projection_sales")
                        .select(SUBTOTAL, "subtotal").first();
            });
        }
    }

    public record SaleTotal(Long id, BigDecimal subtotal) { }

    private void verifyPriorityExpression(FlyingOrmClients clients, boolean reactive) {
        ValueExpression priority = ValueExpression.call("priority", ValueExpression.field("quantity"),
                ValueExpression.value(5), ValueExpression.value(5), ValueExpression.value(null), ValueExpression.value(3));
        List<RankedSale> rows = reactive
                ? clients.operator().dml().query().from(FORM).select("id").select(priority, "rank_value")
                        .orderByAsc("rank_value").fetch(RankedSale.class).collectList().block()
                : clients.syncOperator().dml().query().from(FORM).select("id").select(priority, "rank_value")
                        .orderByAsc("rank_value").fetch(RankedSale.class);
        assertEquals(List.of(new RankedSale(3L, 0), new RankedSale(1L, 3), new RankedSale(2L, 4)), rows);
        PageResult<RankedSale> page = reactive
                ? clients.repository(Sale.class).createQuery().select(Sale::id)
                        .selectExpression(e -> ValueExpression.call("priority", e.field(Sale::units),
                                e.value(Sale::units, 5), e.value(Sale::units, 3)), "rank_value")
                        .orderByAsc("rank_value").page(2, 1, RankedSale.class).block()
                : clients.syncRepository(Sale.class).createQuery().select(Sale::id)
                        .selectExpression(e -> ValueExpression.call("priority", e.field(Sale::units),
                                e.value(Sale::units, 5), e.value(Sale::units, 3)), "rank_value")
                        .orderByAsc("rank_value").page(2, 1, RankedSale.class);
        assertNotNull(page);
        assertEquals(3, page.total());
        assertEquals(List.of(new RankedSale(1L, 1)), page.rows());
        QuerySpec sortOnly = QuerySpec.of(FORM, ConditionGroup.and().build())
                .withOrderBy(priority, PageSort.Direction.ASC).withExcludedFields(List.of("amount", "quantity"));
        List<DynamicRow> direct = reactive ? clients.forms().select(sortOnly).collectList().block()
                : clients.syncForms().select(sortOnly);
        assertEquals(List.of(3L, 1L, 2L), direct.stream().map(row -> row.get("id")).toList());
        direct.forEach(row -> assertEquals(List.of("id"), List.copyOf(row.keySet())));
        DynamicRow first = reactive ? clients.forms().first(sortOnly).block() : clients.syncForms().first(sortOnly);
        assertEquals(3L, first.get("id"));
        PageResult<DynamicRow> directPage = reactive
                ? clients.forms().page(sortOnly, com.flying.orm.core.page.PageQuery.of(2, 1)).block()
                : clients.syncForms().page(sortOnly, com.flying.orm.core.page.PageQuery.of(2, 1));
        assertEquals(3, directPage.total());
        assertEquals(1L, directPage.rows().getFirst().get("id"));
        List<Sale> orderedEntities;
        List<DynamicRow> fluentRows;
        if (reactive) {
            com.flying.orm.rdb.operator.QueryOperator base = clients.operator().dml().query().from(FORM)
                    .select("id").orderByFirst("quantity", 5, 3);
            com.flying.orm.rdb.operator.QueryOperator copy = base.copy().orderByDesc("id");
            base.where("id", 2L);
            fluentRows = copy.fetch().collectList().block();
            assertEquals(3, copy.count().block());
            assertEquals(1L, copy.page(2, 1).block().rows().getFirst().get("id"));
            orderedEntities = clients.repository(Sale.class).createQuery()
                    .orderByFirst(Sale::units, 5, 3).orderByAsc(Sale::id).fetch().collectList().block();
        } else {
            com.flying.orm.rdb.operator.SyncQueryOperator base = clients.syncOperator().dml().query().from(FORM)
                    .select("id").orderByFirst("quantity", 5, 3);
            com.flying.orm.rdb.operator.SyncQueryOperator copy = base.copy().orderByDesc("id");
            base.where("id", 2L);
            fluentRows = copy.fetch();
            assertEquals(3, copy.count());
            assertEquals(1L, copy.page(2, 1).rows().getFirst().get("id"));
            orderedEntities = clients.syncRepository(Sale.class).createQuery()
                    .orderByFirst(Sale::units, 5, 3).orderByAsc(Sale::id).fetch();
        }
        assertEquals(List.of(3L, 1L, 2L), fluentRows.stream().map(row -> row.get("id")).toList());
        fluentRows.forEach(row -> assertEquals(List.of("id"), List.copyOf(row.keySet())));
        assertEquals(List.of(3L, 1L, 2L), orderedEntities.stream().map(Sale::id).toList());
        PageResult<StateSale> statePage = reactive
                ? clients.repository(StateSale.class).createQuery().select(StateSale::id)
                        .orderByFirst(StateSale::state, SaleState.HIGH, SaleState.LOW).copy().page(2, 1).block()
                : clients.syncRepository(StateSale.class).createQuery().select(StateSale::id)
                        .orderByFirst(StateSale::state, SaleState.HIGH, SaleState.LOW).copy().page(2, 1);
        assertEquals(3, statePage.total());
        assertEquals(List.of(new StateSale(1L, null)), statePage.rows());
    }

    private void verifyOrderedMatches(FlyingOrmClients clients, boolean reactive) {
        ValueExpression matches = ValueExpression.firstMatch(ValueExpression.field("amount").coalesce(ValueExpression.value(10)).priority(10),
                ValueExpression.field("quantity").priority(5));
        List<DynamicRow> first = reactive
                ? clients.operator().dml().query().from(FORM).select("id")
                        .orderBy(matches, PageSort.Direction.ASC).orderByAsc("id").fetch().collectList().block()
                : clients.syncOperator().dml().query().from(FORM).select("id")
                        .orderBy(matches, PageSort.Direction.ASC).orderByAsc("id").fetch();
        assertEquals(List.of(1L, 3L, 2L), first.stream().map(row -> row.get("id")).toList());
        List<DynamicRow> last = reactive
                ? clients.operator().dml().query().from(FORM).select("id")
                        .orderByLast("quantity", 5, 3).fetch().collectList().block()
                : clients.syncOperator().dml().query().from(FORM).select("id")
                        .orderByLast("quantity", 5, 3).fetch();
        assertEquals(List.of(2L, 3L, 1L), last.stream().map(row -> row.get("id")).toList());
        PageResult<StateSale> page = reactive
                ? clients.repository(StateSale.class).createQuery().select(StateSale::id)
                        .orderByLast(StateSale::state, SaleState.HIGH, SaleState.LOW).copy().page(1, 2).block()
                : clients.syncRepository(StateSale.class).createQuery().select(StateSale::id)
                        .orderByLast(StateSale::state, SaleState.HIGH, SaleState.LOW).copy().page(1, 2);
        assertEquals(3, page.total());
        assertEquals(List.of(2L, 3L), page.rows().stream().map(StateSale::id).toList());
        List<StateSale> entities = reactive
                ? clients.repository(StateSale.class).createQuery().orderByExpression(e -> ValueExpression.firstMatch(
                        e.priority(StateSale::state, SaleState.HIGH), e.priority(StateSale::id, 2L)), PageSort.Direction.ASC)
                        .fetch().collectList().block()
                : clients.syncRepository(StateSale.class).createQuery().orderByExpression(e -> ValueExpression.firstMatch(
                        e.priority(StateSale::state, SaleState.HIGH), e.priority(StateSale::id, 2L)), PageSort.Direction.ASC).fetch();
        assertEquals(List.of(3L, 2L, 1L), entities.stream().map(StateSale::id).toList());
    }

    private void verifyMixedMatches(FlyingOrmClients clients, boolean reactive) {
        ValueExpression priority = ValueExpression.firstMatch(
                ValueExpression.lastMatch(ValueExpression.field("id").priority(1L)),
                ValueExpression.field("quantity").priority(3, 5));
        List<DynamicRow> rows = reactive
                ? clients.operator().dml().query().from(FORM).select("id")
                        .orderBy(priority, PageSort.Direction.ASC).fetch().collectList().block()
                : clients.syncOperator().dml().query().from(FORM).select("id")
                        .orderBy(priority, PageSort.Direction.ASC).fetch();
        assertEquals(List.of(3L, 2L, 1L), rows.stream().map(row -> row.get("id")).toList(),
                "row 1 matches both groups but its earlier last rule must win");
        PageResult<StateSale> page = reactive
                ? clients.repository(StateSale.class).createQuery().select(StateSale::id)
                        .orderByExpression(e -> ValueExpression.firstMatch(
                                ValueExpression.lastMatch(e.priority(StateSale::id, 1L)),
                                e.priority(StateSale::state, SaleState.LOW, SaleState.HIGH)), PageSort.Direction.ASC)
                        .page(2, 2).block()
                : clients.syncRepository(StateSale.class).createQuery().select(StateSale::id)
                        .orderByExpression(e -> ValueExpression.firstMatch(
                                ValueExpression.lastMatch(e.priority(StateSale::id, 1L)),
                                e.priority(StateSale::state, SaleState.LOW, SaleState.HIGH)), PageSort.Direction.ASC).page(2, 2);
        assertEquals(3, page.total());
        assertEquals(List.of(1L), page.rows().stream().map(StateSale::id).toList());
    }

    @TableName("projection_sales")
    public record StateSale(@TableId Long id, @TableField("quantity") SaleState state) { }
    public enum SaleState {
        LOW(3), HIGH(5);
        @com.flying.orm.core.annotation.EnumValue private final int value;
        SaleState(int value) { this.value = value; }
    }

    public record RankedSale(Long id, @TableField("rank_value") Integer rankValue) { }

    private void verifyEntityPropertySorting(FlyingOrmClients clients, boolean reactive) {
        StructuredConditionInput filter = StructuredConditionInput.term("price", ">=", 10);
        PageResult<Sale> page = reactive
                ? clients.repository(Sale.class).createQuery().filter(filter).orderByDesc("price")
                        .page(2, 1).block()
                : clients.syncRepository(Sale.class).createQuery().filter(filter).orderByDesc("price")
                        .page(2, 1);
        assertNotNull(page);
        assertEquals(2, page.total());
        assertEquals(List.of(new Sale(1L, new BigDecimal("10.00"), 3)), page.rows());
        List<Sale> byColumn = reactive
                ? clients.repository(Sale.class).createQuery().filter(filter).orderByAsc("amount")
                        .fetch().collectList().block()
                : clients.syncRepository(Sale.class).createQuery().filter(filter).orderByAsc("amount").fetch();
        assertEquals(List.of(1L, 2L), byColumn.stream().map(Sale::id).toList());

        List<NamedPrice> aliases = reactive
                ? clients.repository(Sale.class).createQuery().filter(filter).orderByDesc("PRICE")
                        .select(Sale::id).selectExpression(e -> e.field(Sale::price)
                                .multiply(ValueExpression.value(-1)), "price")
                        .fetch(NamedPrice.class).collectList().block()
                : clients.syncRepository(Sale.class).createQuery().filter(filter).orderByDesc("PRICE")
                        .select(Sale::id).selectExpression(e -> e.field(Sale::price)
                                .multiply(ValueExpression.value(-1)), "price")
                        .fetch(NamedPrice.class);
        assertEquals(List.of(new NamedPrice(1L, new BigDecimal("-10.00")),
                new NamedPrice(2L, new BigDecimal("-20.00"))), aliases);
        assertThrows(IllegalArgumentException.class, () -> {
            if (reactive) clients.repository(Sale.class).createQuery().orderByAsc("missing").fetch().blockLast();
            else clients.syncRepository(Sale.class).createQuery().orderByAsc("missing").fetch();
        });
        FieldUsePolicy policy = FieldUsePolicy.builder().visibility("id", FieldVisibility.FULL)
                .visibility("amount", FieldVisibility.FULL).visibility("quantity", FieldVisibility.FULL).build();
        ScopeAccessException denied = assertThrows(ScopeAccessException.class, () -> {
            if (reactive) {
                ReactiveFormRepository.create(clients.forms().withFieldUsePolicy(policy), FORM, Sale.class)
                        .createQuery().select(Sale::id).orderByAsc("price").fetch().blockLast();
            } else {
                SyncFormRepository.create(clients.syncForms().withFieldUsePolicy(policy), FORM, Sale.class)
                        .createQuery().select(Sale::id).orderByAsc("price").fetch();
            }
        });
        assertEquals("amount", denied.field(), "property mapping must not bypass SORT permission");
    }

    public record NamedPrice(Long id, BigDecimal price) { }

    private void verifyJoinProjection(FlyingOrmClients clients, boolean reactive) {
        com.flying.orm.core.join.JoinQuerySpec.Builder builder = com.flying.orm.core.join.JoinQuerySpec.builder(FORM);
        com.flying.orm.core.join.JoinSource root = builder.root();
        com.flying.orm.core.join.JoinSource other = builder.join(
                com.flying.orm.core.join.JoinType.LEFT, FORM, root, "quantity", "id");
        com.flying.orm.core.join.JoinQuerySpec spec = builder.selectAs(root, "id", "id")
                .select(ValueExpression.field(root.field("amount")).multiply(
                        ValueExpression.field(other.field("quantity")).coalesce(ValueExpression.value(2))), "subtotal")
                .orderBy(root, "id", PageSort.Direction.ASC).build();
        List<DynamicRow> rows;
        if (reactive) {
            Flux<DynamicRow> query = clients.forms().selectJoin(spec);
            rows = query.collectList().block();
            assertEquals(rows, query.collectList().block());
        } else rows = clients.syncForms().selectJoin(spec);
        assertNotNull(rows);
        assertEquals(3, rows.size());
        assertEquals(new BigDecimal("50.00"), rows.get(0).get("subtotal"));
        assertEquals(new BigDecimal("40.00"), rows.get(1).get("subtotal"));
        assertNull(rows.get(2).get("subtotal"));
        PageResult<DynamicRow> page = reactive
                ? clients.forms().pageJoin(spec, com.flying.orm.core.page.PageQuery.of(2, 1)).block()
                : clients.syncForms().pageJoin(spec, com.flying.orm.core.page.PageQuery.of(2, 1));
        assertNotNull(page);
        assertEquals(3, page.total());
        assertEquals(List.of(rows.get(1)), page.rows());
    }

    private void verifyJoinPriority(FlyingOrmClients clients, boolean reactive) {
        com.flying.orm.core.join.JoinQuerySpec.Builder builder = com.flying.orm.core.join.JoinQuerySpec.builder(FORM);
        com.flying.orm.core.join.JoinSource root = builder.root();
        com.flying.orm.core.join.JoinSource other = builder.join(
                com.flying.orm.core.join.JoinType.LEFT, FORM, root, "quantity", "id");
        com.flying.orm.core.join.JoinQuerySpec spec = builder.selectAs(root, "id", "id")
                .orderBy(ValueExpression.lastMatch(ValueExpression.field(other.field("quantity")).priority(5)),
                        PageSort.Direction.ASC).orderBy(root, "id", PageSort.Direction.ASC).build();
        List<DynamicRow> rows = reactive ? clients.forms().selectJoin(spec).collectList().block()
                : clients.syncForms().selectJoin(spec);
        assertEquals(List.of(2L, 3L, 1L), rows.stream().map(row -> row.get("id")).toList());
        PageResult<DynamicRow> page = reactive
                ? clients.forms().pageJoin(spec, com.flying.orm.core.page.PageQuery.of(2, 2)).block()
                : clients.syncForms().pageJoin(spec, com.flying.orm.core.page.PageQuery.of(2, 2));
        assertEquals(3, page.total());
        assertEquals(List.of(rows.get(2)), page.rows());
        rows.forEach(row -> assertEquals(1, row.columnCount()));
    }

    public record Subtotal(BigDecimal subtotal) { }

    private void verifyKeysetProjection(FlyingOrmClients clients, boolean reactive) {
        QuerySpec spec = QuerySpec.of(FORM, ConditionGroup.and().build()).withExpression("subtotal", SUBTOTAL);
        FieldUsePolicy policy = FieldUsePolicy.builder().visibility("id", FieldVisibility.FULL)
                .visibility("amount", FieldVisibility.FULL).visibility("quantity", FieldVisibility.FULL)
                .allow("quantity", FieldUse.SORT)
                .allowInternal("id", com.flying.orm.core.scope.FieldUseOrigin.INTERNAL_TIE_BREAKER, FieldUse.SORT).build();
        KeysetSort sort = KeysetSort.asc("quantity", NullOrder.FIRST);
        KeysetPageQuery page = KeysetPageQuery.first(1, sort);
        KeysetPageResult<Subtotal> fluent = reactive
                ? clients.operator().dml().query(FORM).select(SUBTOTAL, "subtotal")
                        .keysetPage(page, Subtotal.class).block()
                : clients.syncOperator().dml().query(FORM).select(SUBTOTAL, "subtotal")
                        .keysetPage(page, Subtotal.class);
        assertNotNull(fluent);
        assertEquals(List.of(new Subtotal(new BigDecimal("40.00"))), fluent.rows());
        List<Subtotal> expected = List.of(new Subtotal(new BigDecimal("40.00")),
                new Subtotal(new BigDecimal("30.00")), new Subtotal(null));
        for (int index = 0; index < expected.size(); index++) {
            KeysetPageResult<DynamicRow> result;
            if (reactive) {
                reactor.core.publisher.Mono<KeysetPageResult<DynamicRow>> query = clients.forms()
                        .withFieldUsePolicy(policy).keysetPage(spec, page);
                result = query.block();
                assertEquals(result, query.block());
            } else {
                result = clients.syncForms().withFieldUsePolicy(policy).keysetPage(spec, page);
            }
            assertNotNull(result);
            assertEquals(1, result.rows().size());
            assertEquals(1, result.rows().getFirst().columnCount(), "hidden sorts never reach the result");
            assertEquals(expected.get(index).subtotal(), result.rows().getFirst().get("subtotal"));
            assertEquals(index < expected.size() - 1, result.hasMore());
            if (result.hasMore()) page = KeysetPageQuery.after(1, result.nextPosition(), sort);
        }
        QuerySpec locking = spec.withLock(ReadLock.update());
        KeysetPageQuery first = KeysetPageQuery.first(1, sort);
        KeysetPageResult<Subtotal> locked = reactive
                ? clients.forms().withFieldUsePolicy(policy).lockingRead(locking, first, Subtotal.class).block()
                : clients.syncForms().withFieldUsePolicy(policy).lockingRead(locking, first, Subtotal.class);
        assertNotNull(locked);
        assertEquals(fluent.rows(), locked.rows());
    }

    private void verifyCursorProjection(FlyingOrmClients clients, boolean reactive, List<SaleTotal> expected) {
        QuerySpec spec = QuerySpec.of(FORM, ConditionGroup.and().build())
                .withProjection(List.of("id"), List.of()).withExpression("subtotal", SUBTOTAL);
        FieldUsePolicy policy = FieldUsePolicy.builder().visibility("id", FieldVisibility.FULL)
                .visibility("amount", FieldVisibility.FULL).visibility("quantity", FieldVisibility.FULL)
                .allow("id", FieldUse.SORT).build();
        KeysetPageQuery page = KeysetPageQuery.first(1, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST));
        KeysetPageResult<SaleTotal> fluent = reactive
                ? clients.operator().dml().query(FORM).select("id").select(SUBTOTAL, "subtotal")
                        .keysetPage(page, SaleTotal.class).block()
                : clients.syncOperator().dml().query(FORM).select("id").select(SUBTOTAL, "subtotal")
                        .keysetPage(page, SaleTotal.class);
        assertNotNull(fluent);
        assertEquals(List.of(expected.getFirst()), fluent.rows());
        for (int index = 0; index < expected.size(); index++) {
            KeysetPageResult<SaleTotal> result;
            if (reactive) {
                reactor.core.publisher.Mono<KeysetPageResult<SaleTotal>> query = clients.forms()
                        .withFieldUsePolicy(policy).keysetPage(spec, page, SaleTotal.class);
                result = query.block();
                assertEquals(result, query.block());
            } else {
                result = clients.syncForms().withFieldUsePolicy(policy).keysetPage(spec, page, SaleTotal.class);
            }
            assertNotNull(result);
            assertEquals(List.of(expected.get(index)), result.rows());
            assertEquals(index < expected.size() - 1, result.hasMore());
            if (result.hasMore()) page = KeysetPageQuery.after(1, com.flying.orm.core.page.CursorPosition.of(result.nextPosition().values()), KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST));
        }
    }

    private void verifyEntityProjection(FlyingOrmClients clients, boolean reactive, List<SaleTotal> expected) {
        AtomicInteger callbacks = new AtomicInteger();
        if (reactive) {
            com.flying.orm.rdb.operator.EntityDmlQueryOperator<Sale> query = clients.repository(Sale.class).createQuery()
                    .select(Sale::id).selectExpression(e -> e.field(Sale::price)
                            .multiply(e.field(Sale::units).coalesce(ValueExpression.value(2))), "subtotal")
                    .orderByDesc("subtotal").where(Sale::id, "<", 3L)
                    .onLoad(ignored -> { callbacks.incrementAndGet(); return reactor.core.publisher.Mono.empty(); });
            Flux<SaleTotal> result = query.fetch(SaleTotal.class);
            assertEquals(List.of(expected.get(1), expected.getFirst()), result.collectList().block());
            assertEquals(result.collectList().block(), result.collectList().block());
            assertEquals(expected.get(1), query.first(SaleTotal.class).block());
            assertEquals(expected.get(1), query.copy().where(Sale::id, 2L).one(SaleTotal.class).block());
            assertEquals(2L, query.count().block());
            assertEquals(List.of(expected.getFirst()), query.page(2, 1, SaleTotal.class).block().rows());
            assertEquals(2, query.copy().exclude(Sale::id).executeRows().collectList().block().size());
            assertEquals(3, query.copy().selectExpression(e -> e.field(Sale::units), "units_alias")
                    .executeRows().blockFirst().values().size());
            assertEquals(2, query.executeRows().blockFirst().values().size());
            assertThrows(IllegalArgumentException.class,
                    () -> query.selectExpression(e -> e.field(Sale::price), "SUBTOTAL"));
            assertThrows(IllegalStateException.class, query::fetch);
        } else {
            com.flying.orm.rdb.operator.SyncEntityDmlQueryOperator<Sale> query = clients.syncRepository(Sale.class).createQuery()
                    .select(Sale::id).selectExpression(e -> e.field(Sale::price)
                            .multiply(e.field(Sale::units).coalesce(ValueExpression.value(2))), "subtotal")
                    .orderByDesc("subtotal").where(Sale::id, "<", 3L).onLoad(ignored -> callbacks.incrementAndGet());
            assertEquals(List.of(expected.get(1), expected.getFirst()), query.fetch(SaleTotal.class));
            assertEquals(expected.get(1), query.first(SaleTotal.class));
            assertEquals(expected.get(1), query.copy().where(Sale::id, 2L).one(SaleTotal.class));
            assertEquals(2L, query.count());
            assertEquals(List.of(expected.getFirst()), query.page(2, 1, SaleTotal.class).rows());
            assertEquals(2, query.copy().exclude(Sale::id).executeRows().size());
            assertEquals(3, query.copy().selectExpression(e -> e.field(Sale::units), "units_alias")
                    .executeRows().getFirst().values().size());
            assertEquals(2, query.executeRows().getFirst().values().size());
            assertThrows(IllegalArgumentException.class,
                    () -> query.selectExpression(e -> e.field(Sale::price), "SUBTOTAL"));
            assertThrows(IllegalStateException.class, query::fetch);
        }
        assertEquals(0, callbacks.get(), "projection results do not invoke entity lifecycle callbacks");
    }

    @TableName("projection_sales")
    public record Sale(@TableId Long id, @TableField("amount") BigDecimal price,
                       @TableField("quantity") Integer units) { }
}
