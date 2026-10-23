package com.flying.orm.rdb.form;

import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.GroupSelection;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.aggregate.AggregateRowLayout;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ComputedAggregateContractTest {
    private static final DynamicForm SALES = DynamicForm.builder("sales", "sales")
            .addField(DynamicField.of("id", "BIGINT"))
            .addField(DynamicField.of("amount", "DECIMAL"))
            .addField(DynamicField.of("quantity", "INTEGER")).build();

    @Test
    void nestedInputKeepsSelectWhereHavingAndPagingBindingsInOrder() {
        AggregateExpression<BigDecimal> gross = AggregateExpression.sum(
                ValueExpression.field("amount").coalesce(ValueExpression.value(BigDecimal.TEN))
                        .multiply(ValueExpression.field("quantity")), "gross");
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(SALES,
                ConditionGroup.and().where("id", ">", 1L).build())).aggregate(gross)
                .having(ConditionGroup.and().where("gross", ">", 20).build())
                .orderBy(com.flying.orm.core.page.PageSort.asc("gross")).build();
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormAggregatePlanner.Plan plan = planner(dialect, FieldUsePolicy.unrestricted()).planPage(spec, PageQuery.of(1, 10), false);
            assertEquals(List.of(BigDecimal.TEN, 1L, 20), plan.countRequest().orElseThrow().parameters());
            assertTrue(plan.request().sql().contains("coalesce("));
            assertTrue(plan.request().sql().contains("flying_aggregate_result"));
            assertEquals(new BigDecimal("42"), new AggregateResultDecoder(plan)
                    .decode(DynamicRow.copyOf(Map.of("gross", 42))).get(gross));
        }
    }

    @Test
    void computedOrderRepeatsInputBindingsOnlyWhenReferencingTheSameSelectLevel() {
        AggregateSpec.Builder builder = AggregateSpec.builder(QuerySpec.of(SALES,
                        ConditionGroup.and().where("id", ">", 1L).build()))
                .aggregate(AggregateExpression.sum(ValueExpression.field("amount").coalesce(ValueExpression.value(7)), "total"))
                .orderByFirst("total", new BigDecimal("27"), new BigDecimal("30"));
        FormAggregatePlanner planner = planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted());
        FormAggregatePlanner.Plan direct = planner.planPage(builder.build(), PageQuery.of(1, 10), false);
        assertEquals(List.of(7, 1L), direct.countRequest().orElseThrow().parameters());
        assertEquals(List.of(7, 1L, 7, new BigDecimal("27"), new BigDecimal("30"), 10, 0L), direct.request().parameters());
        assertTrue(direct.request().sql().contains("case sum(coalesce(\"amount\", ?)) when ?"));
        builder.having(condition -> condition.where("total", ">", 20));
        FormAggregatePlanner.Plan outer = planner.planPage(builder.build(), PageQuery.of(1, 10), false);
        assertEquals(List.of(7, 1L, 20), outer.countRequest().orElseThrow().parameters());
        assertEquals(List.of(7, 1L, 20, new BigDecimal("27"), new BigDecimal("30"), 10, 0L), outer.request().parameters());
        assertTrue(outer.request().sql().contains("case \"flying_aggregate_result\".\"total\" when ?"));
        AggregateSpec repeated = AggregateSpec.builder(QuerySpec.of(SALES, ConditionGroup.and().build()))
                .aggregate(AggregateExpression.sum(ValueExpression.field("amount").coalesce(ValueExpression.value(7)), "total"))
                .orderBy(ValueExpression.firstMatch(ValueExpression.field("total").priority(BigDecimal.TEN, BigDecimal.ONE)),
                        com.flying.orm.core.page.PageSort.Direction.ASC).build();
        assertEquals(List.of(7, 7, BigDecimal.TEN, 7, BigDecimal.ONE), planner.plan(repeated).request().parameters());
    }

    @Test
    void priorityOrderingRequiresSortPermissionForAllAggregateInputs() {
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(SALES, ConditionGroup.and().build()))
                .aggregate(AggregateExpression.sum(ValueExpression.field("amount").multiply(ValueExpression.field("quantity")), "gross"))
                .orderByFirst("gross", BigDecimal.TEN).build();
        FieldUsePolicy.Builder policy = FieldUsePolicy.builder().allow("amount", FieldUse.AGGREGATE, FieldUse.SORT)
                .allow("quantity", FieldUse.AGGREGATE).visibility("amount", FieldVisibility.FULL)
                .visibility("quantity", FieldVisibility.FULL);
        assertThrows(com.flying.orm.core.scope.ScopeAccessException.class,
                () -> planner(RdbDialect.postgresql(), policy.build()).plan(spec));
        policy.allow("quantity", FieldUse.SORT);
        assertDoesNotThrow(() -> planner(RdbDialect.postgresql(), policy.build()).plan(spec));
    }

    @Test
    void executesNestedAggregateAndFilteredTotalAgainstActualJdbc() throws Exception {
        AggregateExpression<BigDecimal> total = AggregateExpression.sum(
                ValueExpression.field("amount").coalesce(ValueExpression.value(7)), "total");
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(SALES,
                ConditionGroup.and().where("id", ">", 1L).build())).aggregate(total)
                .having(ConditionGroup.and().where("total", ">", 25).build()).build();
        FormAggregatePlanner.Plan plan = planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted())
                .planPage(spec, PageQuery.of(1, 10), false);
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:computed_aggregate;DATABASE_TO_LOWER=TRUE")) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("create table sales(id bigint, amount decimal(10,2), quantity int)");
                statement.execute("insert into sales values(1, 10, 2), (2, null, 3), (3, 20, 4)");
            }
            assertEquals(1L, executeNumber(connection, plan.countRequest().orElseThrow()).longValue());
            assertEquals(new BigDecimal("27.00"), executeNumber(connection, plan.request()));
        }
    }

    @Test
    void computedVisibilityIncludesEveryOperand() {
        AggregateExpression<BigDecimal> gross = AggregateExpression.sum(
                ValueExpression.field("amount").multiply(ValueExpression.field("quantity")), "gross");
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(SALES, ConditionGroup.and().build())).aggregate(gross).build();
        FieldUsePolicy policy = FieldUsePolicy.builder().allow("amount", FieldUse.AGGREGATE)
                .visibility("amount", FieldVisibility.FULL).allow("quantity", FieldUse.AGGREGATE)
                .visibility("quantity", FieldVisibility.HIDDEN).build();
        FormAggregatePlanner.Plan plan = planner(RdbDialect.postgresql(), policy).plan(spec);
        assertNull(new AggregateResultDecoder(plan).decode(DynamicRow.copyOf(Map.of("gross", 42))).get(gross));
    }

    @Test
    void unknownFunctionAndNonNumericOperandsCannotDisappearOrBecomeSql() {
        for (ValueExpression input : List.of(ValueExpression.call("not-registered", ValueExpression.field("amount")),
                ValueExpression.value("1) OR 1=1 --"), ValueExpression.field("absent"))) {
            AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(SALES, ConditionGroup.and().build()))
                    .aggregate(AggregateExpression.sum(input, "result")).build();
            assertThrows(IllegalArgumentException.class, () -> planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted()).plan(spec));
        }
    }

    @Test
    void inputMayReferenceBothJoinSourcesWithoutCollapsingSameNamedFields() {
        JoinQuerySpec.Builder relation = JoinQuerySpec.builder(SALES);
        JoinSource left = relation.root();
        JoinSource right = relation.join(JoinType.LEFT, SALES, left, "id", "id");
        AggregateSpec spec = AggregateSpec.builder(relation.buildRelation())
                .aggregate(AggregateExpression.avg(ValueExpression.field(left.field("amount"))
                        .add(ValueExpression.field(right.field("amount"))), "mean")).build();
        String sql = planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted()).plan(spec).request().sql();
        assertTrue(sql.contains("\"t0\".\"amount\" + \"t1\".\"amount\""), sql);
    }

    @Test
    void numericNullInputCarriesAnExplicitDriverBindingType() {
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(SALES, ConditionGroup.and().build()))
                .aggregate(AggregateExpression.sum(ValueExpression.value(null), "empty")).build();
        Object parameter = planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted()).plan(spec)
                .request().parameters().getFirst();
        assertEquals(new com.flying.orm.rdb.internal.binding.SqlNullParameter(BigDecimal.class), parameter);
    }

    @Test
    void sqlServerDoesNotNarrowDecimalOrLargeIntegralParameters() {
        for (Number value : List.of(new BigDecimal("0.00000000001"), new BigDecimal("1E29"),
                new java.math.BigInteger("100000000000000000000000000000"), 0.00000000001D)) {
            AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(SALES, ConditionGroup.and().build()))
                    .aggregate(AggregateExpression.sum(ValueExpression.value(value), "number")).build();
            com.flying.orm.core.sql.render.SqlRequest request = planner(RdbDialect.sqlServer(), FieldUsePolicy.unrestricted())
                    .plan(spec).request();
            assertTrue(request.sql().contains("sum(?)"), request.sql());
            assertEquals(value, request.parameters().getFirst());
        }
    }

    @Test
    void deepComputedHandlesRemainUsableInTheResultLayout() {
        ValueExpression input = ValueExpression.field("amount");
        ValueExpression equivalent = ValueExpression.field("amount");
        for (int index = 0; index < 5000; index++) {
            input = input.add(ValueExpression.value(index));
            equivalent = equivalent.add(ValueExpression.value(index));
        }
        AggregateExpression<BigDecimal> total = AggregateExpression.sum(input, "total");
        AggregateRowLayout layout = AggregateRowLayout.of(List.of(), List.of(total));
        AggregateRow row = AggregateRow.of(layout, List.of(BigDecimal.TEN));
        assertEquals(BigDecimal.TEN, row.get(total));
        assertEquals(BigDecimal.TEN, row.get(AggregateExpression.sum(equivalent, "total")));
    }

    @Test
    void sqlServerPreservesIntegerFunctionInputsAndPromotesTheAggregateResult() {
        com.flying.orm.core.sql.expression.SqlExpressionFunctions functions =
                com.flying.orm.core.sql.expression.SqlExpressionFunctions.builder().register("integer-only", call -> {
                    call.requireArity(1);
                    assertEquals(com.flying.orm.core.type.LogicalType.INTEGER, call.type(0));
                    call.sql("(").argument(0).sql(" % 2)");
                    return com.flying.orm.core.type.LogicalType.INTEGER;
                }).register("large-integer", call -> {
                    call.sql("1100000000");
                    return com.flying.orm.core.type.LogicalType.INTEGER;
                }).build();
        FormAggregatePlanner planner = new FormAggregatePlanner(FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().expressionFunctions(functions).build(), RdbDialect.sqlServer()),
                new FormScopeSupport(FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().expressionFunctions(functions).build(), RdbDialect.sqlServer()), StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults());
        ValueExpression parity = ValueExpression.call("integer-only", ValueExpression.field("quantity"));
        ValueExpression large = ValueExpression.call("large-integer").add(ValueExpression.value(0));
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(SALES, ConditionGroup.and().build()))
                .aggregate(AggregateExpression.sum(parity, "parity"))
                .aggregate(AggregateExpression.sum(large, "large")).build();
        String sql = planner.plan(spec).request().sql();
        assertTrue(sql.contains("sum(cast(([quantity] % 2) as decimal(38,10)))"), sql);
        assertTrue(sql.contains("sum(cast((1100000000 + ?) as decimal(38,10)))"), sql);
    }

    @Test
    void explicitMaskedOperandCannotBeSilentlyPublishedAsANumericResult() {
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(SALES, ConditionGroup.and().build()))
                .aggregate(AggregateExpression.sum(ValueExpression.field("amount")
                        .multiply(ValueExpression.field("quantity")), "gross")).build();
        FieldUsePolicy policy = FieldUsePolicy.builder().allow("amount", FieldUse.AGGREGATE)
                .visibility("amount", FieldVisibility.FULL).allow("quantity", FieldUse.AGGREGATE)
                .visibility("quantity", FieldVisibility.MASKED).build();
        assertThrows(IllegalArgumentException.class, () -> planner(RdbDialect.postgresql(), policy).plan(spec));
    }

    @Test
    void anUnrelatedJoinSourceCannotBeSmuggledIntoTheOperand() {
        JoinQuerySpec.Builder relation = JoinQuerySpec.builder(SALES);
        JoinSource outsider = new JoinSource(99, SALES);
        AggregateSpec spec = AggregateSpec.builder(relation.buildRelation())
                .aggregate(AggregateExpression.sum(ValueExpression.field(outsider.field("amount")), "gross")).build();
        assertThrows(IllegalArgumentException.class,
                () -> planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted()).plan(spec));
    }

    private static FormAggregatePlanner planner(RdbDialect dialect, FieldUsePolicy policy) {
        return new FormAggregatePlanner(FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), dialect),
                new FormScopeSupport(FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), dialect), StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                policy,
                QueryShapeLimits.defaults());
    }

    private static Number executeNumber(Connection connection, com.flying.orm.core.sql.render.SqlRequest request) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(request.sql())) {
            for (int i = 0; i < request.parameters().size(); i++) statement.setObject(i + 1, request.parameters().get(i));
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                return (Number) result.getObject(1);
            }
        }
    }

    @Test
    void reactiveAndSyncExecutionReturnIdenticalComputedPages() throws Exception {
        org.h2.jdbcx.JdbcDataSource source = new org.h2.jdbcx.JdbcDataSource();
        source.setURL("jdbc:h2:mem:computed_page;DATABASE_TO_LOWER=TRUE");
        source.setUser("sa");
        source.setPassword("");
        io.r2dbc.spi.ConnectionFactory factory = io.r2dbc.spi.ConnectionFactories.get(
                io.r2dbc.spi.ConnectionFactoryOptions.builder()
                        .option(io.r2dbc.spi.ConnectionFactoryOptions.DRIVER, "h2")
                        .option(io.r2dbc.spi.ConnectionFactoryOptions.PROTOCOL, "mem")
                        .option(io.r2dbc.spi.ConnectionFactoryOptions.DATABASE, "computed_page")
                        .option(io.r2dbc.spi.ConnectionFactoryOptions.USER, "sa")
                        .option(io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD, "")
                        .option(io.r2dbc.h2.H2ConnectionFactoryProvider.OPTIONS, "DATABASE_TO_LOWER=TRUE").build());
        try (Connection keeper = source.getConnection(); Statement statement = keeper.createStatement();
             com.flying.orm.rdb.bootstrap.FlyingOrmClients clients = com.flying.orm.rdb.bootstrap.FlyingOrmClients.builder(
                     com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(source),
                     com.flying.orm.rdb.execution.ConnectionAccessTestSupport.reactive(factory)).configuredDialect("h2")
                     .renderer(SqlRenderer.builder().addDefaultTerms().expressionFunctions(
                             com.flying.orm.core.sql.expression.SqlExpressionFunctions.builder()
                                     .register("twice", com.flying.orm.core.sql.expression.SqlExpressionFunction.numeric(1,
                                             com.flying.orm.core.type.LogicalType.DECIMAL,
                                             call -> call.sql("(").argument(0).sql(" + ").argument(0).sql(")")))
                                     .register("text-length", call -> {
                                         call.requireArity(1);
                                         call.sql("length(").argument(0).sql(")");
                                         return com.flying.orm.core.type.LogicalType.INTEGER;
                                     }).build()).build()).build()) {
            statement.execute("create table sales(id bigint, amount decimal(10,2), quantity integer)");
            statement.execute("insert into sales values(1, 10, 2),(2, null, 2),(3,20,3)");
            AggregateExpression<BigDecimal> gross = AggregateExpression.sum(ValueExpression.field("amount")
                    .coalesce(ValueExpression.value(7)).multiply(ValueExpression.field("quantity")), "gross");
            AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(SALES, ConditionGroup.and().build()))
                    .group(GroupSelection.of("quantity", "quantity")).aggregate(gross)
                    .having(ConditionGroup.and().where("gross", ">", 30).build())
                    .orderBy(com.flying.orm.core.page.PageSort.desc("gross")).build();
            com.flying.orm.core.page.PageResult<AggregateRow> sync = clients.syncForms().aggregatePage(spec, PageQuery.of(1, 1));
            reactor.core.publisher.Mono<com.flying.orm.core.page.PageResult<AggregateRow>> pending =
                    clients.forms().aggregatePage(spec, PageQuery.of(1, 1));
            com.flying.orm.core.page.PageResult<AggregateRow> reactive = pending.block();
            assertEquals(2L, sync.total());
            assertEquals(sync.total(), reactive.total());
            assertEquals(new BigDecimal("60.00"), sync.rows().getFirst().get(gross));
            assertEquals(sync.rows().getFirst().values(), reactive.rows().getFirst().values());
            assertEquals(reactive.rows().getFirst().values(), pending.block().rows().getFirst().values());
            AggregateExpression<BigDecimal> empty = AggregateExpression.sum(ValueExpression.value(null), "empty");
            AggregateSpec nullSpec = AggregateSpec.builder(QuerySpec.of(SALES, ConditionGroup.and().build()))
                    .aggregate(empty).build();
            assertNull(clients.syncForms().aggregate(nullSpec).getFirst().get(empty));
            List<AggregateRow> nullRows = clients.forms().aggregate(nullSpec).collectList().block();
            assertEquals(1, nullRows.size());
            assertNull(nullRows.getFirst().get(empty));

            AggregateExpression<BigDecimal> doubled = AggregateExpression.sum(ValueExpression.call("twice",
                    ValueExpression.field("amount").coalesce(ValueExpression.value(7))), "doubled");
            AggregateSpec doubledSpec = AggregateSpec.builder(QuerySpec.of(SALES, ConditionGroup.and().build()))
                    .aggregate(doubled).build();
            assertEquals(new BigDecimal("74.00"), clients.syncForms().aggregate(doubledSpec).getFirst().get(doubled));
            assertEquals(new BigDecimal("74.00"), clients.forms().aggregate(doubledSpec).blockFirst().get(doubled));

            statement.execute("create table labels(name varchar(32))");
            statement.execute("insert into labels values('a'),('OpenAI'),('b')");
            DynamicForm labels = DynamicForm.builder("labels", "labels").addField(DynamicField.of("name", "VARCHAR")).build();
            AggregateExpression<BigDecimal> length = AggregateExpression.sum(
                    ValueExpression.call("text-length", ValueExpression.field("name")), "length");
            AggregateSpec lengthSpec = AggregateSpec.builder(QuerySpec.of(labels, ConditionGroup.and().build()))
                    .aggregate(length).build();
            assertEquals(new BigDecimal("8"), clients.syncForms().aggregate(lengthSpec).getFirst().get(length));
            assertEquals(new BigDecimal("8"), clients.forms().aggregate(lengthSpec).blockFirst().get(length));
            AggregateExpression<BigDecimal> nullableLength = AggregateExpression.sum(ValueExpression.call("text-length",
                    ValueExpression.field("name").coalesce(ValueExpression.value(null))), "nullable_length");
            AggregateSpec nullableLengthSpec = AggregateSpec.builder(QuerySpec.of(labels, ConditionGroup.and().build()))
                    .aggregate(nullableLength).build();
            assertEquals(new BigDecimal("8"), clients.syncForms().aggregate(nullableLengthSpec).getFirst().get(nullableLength));
            assertEquals(new BigDecimal("8"), clients.forms().aggregate(nullableLengthSpec).blockFirst().get(nullableLength));

            AggregateExpression<Long> present = AggregateExpression.count(
                    ValueExpression.field("amount").add(ValueExpression.value(1)), "present");
            AggregateExpression<Long> distinct = AggregateExpression.countDistinct(
                    ValueExpression.field("amount").coalesce(ValueExpression.value(10)), "distincts");
            AggregateExpression<BigDecimal> maximum = AggregateExpression.max(
                    ValueExpression.field("amount").add(ValueExpression.value(1)), "maximum",
                    com.flying.orm.core.type.LogicalType.DECIMAL, BigDecimal.class);
            AggregateSpec extrema = AggregateSpec.builder(QuerySpec.of(SALES, ConditionGroup.and().build()))
                    .aggregate(present).aggregate(distinct).aggregate(maximum).build();
            List<Object> expected = List.of(2L, 2L, new BigDecimal("21.00"));
            assertEquals(expected, clients.syncForms().aggregate(extrema).getFirst().values());
            assertEquals(expected, clients.forms().aggregate(extrema).blockFirst().values());

            statement.execute("insert into labels values(null)");
            ValueExpression text = ValueExpression.field("name").coalesce(ValueExpression.value("z"));
            AggregateExpression<String> first = AggregateExpression.min(text, "first",
                    com.flying.orm.core.type.LogicalType.TEXT, String.class);
            AggregateExpression<String> last = AggregateExpression.max(text, "last",
                    com.flying.orm.core.type.LogicalType.TEXT, String.class);
            AggregateSpec textExtrema = AggregateSpec.builder(QuerySpec.of(labels, ConditionGroup.and().build()))
                    .aggregate(first).aggregate(last).build();
            assertEquals(List.of("OpenAI", "z"), clients.syncForms().aggregate(textExtrema).getFirst().values());
            assertEquals(List.of("OpenAI", "z"), clients.forms().aggregate(textExtrema).blockFirst().values());
            AggregateSpec emptyExtrema = AggregateSpec.builder(QuerySpec.of(labels,
                    ConditionGroup.and().where("name", "=", "absent").build()))
                    .aggregate(AggregateExpression.count(text, "counted")).aggregate(first).aggregate(last).build();
            assertEquals(java.util.Arrays.asList(0L, null, null), clients.syncForms().aggregate(emptyExtrema).getFirst().values());
            assertEquals(java.util.Arrays.asList(0L, null, null), clients.forms().aggregate(emptyExtrema).blockFirst().values());
        }
    }
}
