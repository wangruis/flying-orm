package com.flying.orm.rdb.form;

import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.GroupSelection;
import com.flying.orm.rdb.aggregate.AggregateRow;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.join.JoinFieldRef;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JoinedAggregateContractTest {
    private static final DynamicForm CUSTOMERS = DynamicForm.builder("customers", "customers")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("name", "VARCHAR")).build();
    private static final DynamicForm ORDERS = DynamicForm.builder("orders", "orders")
            .addField(DynamicField.of("customer_id", "BIGINT"))
            .addField(DynamicField.of("amount", "DECIMAL"))
            .addField(DynamicField.of("status", "VARCHAR")).build();
    private static final GroupSelection CUSTOMER = GroupSelection.of("name", "customer");
    private static final AggregateExpression<BigDecimal> GROSS = AggregateExpression.sum("amount", "gross");

    @Test
    void groupedAndMinValuesUseEachOriginalFieldsCodec() {
        DynamicForm left = DynamicForm.builder("left_codes", "left_codes")
                .addField(DynamicField.of("id", "BIGINT")).addField(DynamicField.of("code", "VARCHAR")).build();
        DynamicForm right = DynamicForm.builder("right_codes", "right_codes")
                .addField(DynamicField.of("id", "BIGINT")).addField(DynamicField.of("code", "VARCHAR")).build();
        Map<DynamicField, com.flying.orm.rdb.mapping.EntityTypeMappingRegistry.Mapping> codecs = new java.util.IdentityHashMap<>();
        codecs.put(left.field("code"), mapping("left:"));
        codecs.put(right.field("code"), mapping("right:"));
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                .withEntityFieldCodecs(codecs);
        JoinQuerySpec.Builder input = JoinQuerySpec.builder(left);
        JoinSource joined = input.join(JoinType.LEFT, right, input.root(), "id", "id");
        GroupSelection group = GroupSelection.of("code", "left_code");
        AggregateExpression<Code> minimum = AggregateExpression.min("code", "right_code",
                com.flying.orm.core.type.LogicalType.TEXT, Code.class);
        AggregateSpec spec = AggregateSpec.builder(input.buildRelation()).group(group).aggregate(joined, minimum)
                .orderByFirst("left_code", new Code("A")).orderByLast("right_code", new Code("B")).build();
        FormAggregatePlanner.Plan plan = new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults()).plan(spec);
        assertEquals(List.of("left:A", "right:B"), plan.request().parameters());
        assertTrue(plan.request().sql().contains("case \"t0\".\"code\" when ?"));
        assertTrue(plan.request().sql().contains("case when min(\"t1\".\"code\") = ?"));
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("left_code", "A"); values.put("right_code", "B");
        AggregateRow row = new AggregateResultDecoder(plan).decode(DynamicRow.copyOf(values));
        assertEquals(new Code("left:A"), row.get(group, Code.class));
        assertEquals(new Code("right:B"), row.get(minimum));
    }

    @Test
    void joinedAggregateChargesCallerBudgetsAndRetainsFinalPageParameters() {
        assertThrows(IllegalStateException.class, () -> report().withQueryScope(DataScope.none()));
        FormAggregatePlanner.Plan unrestricted = planner(RdbDialect.postgresql()).planPage(report(), PageQuery.of(2, 7), false);
        assertEquals(List.of("paid", "Ada", BigDecimal.TEN, 7, 7L), unrestricted.request().parameters());
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        for (QueryShapeLimits limits : List.of(QueryShapeLimits.defaults().withMaxBindCount(4),
                QueryShapeLimits.defaults().withMaxProjectionCount(1), QueryShapeLimits.defaults().withMaxJoinCount(0))) {
            FormAggregatePlanner planner = new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                FieldUsePolicy.unrestricted(),
                limits);
            assertThrows(IllegalArgumentException.class, () -> planner.planPage(report(), PageQuery.of(2, 7), false));
        }
    }

    private static com.flying.orm.rdb.mapping.EntityTypeMappingRegistry.Mapping mapping(String prefix) {
        com.flying.orm.core.codec.ValueCodec codec = new com.flying.orm.core.codec.ValueCodec() {
            public boolean supports(Class<?> type) { return type == Code.class; }
            public Object write(Object value) { return prefix + ((Code) value).value(); }
            public Object read(Object value, Class<?> type) { return new Code(prefix + value); }
        };
        return com.flying.orm.rdb.mapping.EntityTypeMappingRegistry.builder()
                .register("code", Code.class, com.flying.orm.core.type.DatabaseType.of("VARCHAR"), codec).build().resolve(Code.class);
    }

    private record Code(String value) implements Comparable<Code> {
        public int compareTo(Code other) { return value.compareTo(other.value); }
    }

    @Test
    void selfJoinKeepsDistinctAuthorityAndMaskingForSameNamedFields() {
        DynamicForm people = DynamicForm.builder("people", "people")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR"))
                .masked("name", MaskedFieldDefinition.builder("full").build()).build();
        JoinQuerySpec.Builder input = JoinQuerySpec.builder(people);
        JoinSource peer = input.join(JoinType.LEFT, people, input.root(), "id", "id");
        JoinQuerySpec relation = input.buildRelation();
        GroupSelection rootName = GroupSelection.of("name", "root_name");
        GroupSelection peerName = GroupSelection.of("name", "peer_name");
        AggregateExpression<Long> rows = AggregateExpression.countRows("rows");
        AggregateSpec spec = AggregateSpec.builder(relation).group(rootName).group(peer, peerName).aggregate(rows).build();
        FieldUsePolicy.Builder policy = FieldUsePolicy.builder()
                .allowJoin(new JoinFieldRef(input.root(), "id"), FieldUse.JOIN)
                .allowJoin(new JoinFieldRef(peer, "id"), FieldUse.JOIN)
                .allowJoin(new JoinFieldRef(input.root(), "name"), FieldUse.GROUP)
                .allowJoin(new JoinFieldRef(peer, "name"), FieldUse.GROUP)
                .joinVisibility(new JoinFieldRef(input.root(), "name"), FieldVisibility.FULL)
                .joinVisibility(new JoinFieldRef(peer, "name"), FieldVisibility.MASKED);
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        FormAggregatePlanner planner = new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                policy.build(),
                QueryShapeLimits.defaults());
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("root_name", "Ada"); values.put("peer_name", "Grace"); values.put("rows", 2L);
        AggregateRow row = new AggregateResultDecoder(planner.plan(spec)).decode(DynamicRow.copyOf(values));
        assertEquals("Ada", row.get(rootName, String.class));
        assertEquals("*****", row.get(peerName, String.class));
        assertEquals(2L, row.get(rows));
        assertThrows(IllegalArgumentException.class, () -> AggregateSpec.builder(relation).group(people, peerName));
        assertThrows(IllegalArgumentException.class, () -> AggregateSpec.builder(relation).aggregate(ORDERS, GROSS));
        FieldUsePolicy missingPeer = FieldUsePolicy.builder()
                .allowJoin(new JoinFieldRef(input.root(), "id"), FieldUse.JOIN)
                .allowJoin(new JoinFieldRef(peer, "id"), FieldUse.JOIN)
                .allowJoin(new JoinFieldRef(input.root(), "name"), FieldUse.GROUP)
                .joinVisibility(new JoinFieldRef(input.root(), "name"), FieldVisibility.FULL).build();
        assertThrows(ScopeAccessException.class, () -> new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                missingPeer,
                QueryShapeLimits.defaults()).plan(spec));
    }

    @Test
    void encryptedJoinedSelectionIsRejectedBeforeKeyPreparation() {
        DynamicForm secrets = DynamicForm.builder("secrets", "secrets")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder().build()).build();
        JoinQuerySpec.Builder input = JoinQuerySpec.builder(CUSTOMERS);
        JoinSource secret = input.join(JoinType.LEFT, secrets, input.root(), "id", "id");
        AggregateSpec report = AggregateSpec.builder(input.buildRelation())
                .aggregate(secret, AggregateExpression.count("secret", "secrets")).build();
        assertThrows(IllegalArgumentException.class, () -> planner(RdbDialect.postgresql()).plan(report));
        AggregateSpec computed = AggregateSpec.builder(input.buildRelation())
                .aggregate(secret, AggregateExpression.count(com.flying.orm.core.expression.ValueExpression.field("secret")
                        .coalesce(com.flying.orm.core.expression.ValueExpression.value("")), "secrets")).build();
        assertThrows(IllegalArgumentException.class, () -> planner(RdbDialect.postgresql()).plan(computed));
    }

    @Test
    void joinedCorrelatedHavingRefersToFinalResultAliases() {
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms().addTerm(SqlTermHandler.relationExists(
                "allowed-gross", "allowed_totals", "a", "gross", "org_id")).build();
        JoinQuerySpec.Builder input = JoinQuerySpec.builder(CUSTOMERS);
        JoinSource orders = input.join(JoinType.LEFT, ORDERS, input.root(), "id", "customer_id");
        AggregateSpec report = AggregateSpec.builder(input.buildRelation()).group(CUSTOMER).aggregate(orders, GROSS)
                .having(ConditionGroup.or(conditions.terms()).where("gross", "allowed-gross", 7L)
                        .where("gross", ">", BigDecimal.TEN).build()).build();
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(), RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, dialect);
            FormAggregatePlanner.Plan plan = new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults()).plan(report);
            String qualifier = dialect.schema().identifier("flying_aggregate_result") + "." + dialect.schema().identifier("gross");
            assertTrue(plan.request().sql().startsWith("select * from (select "));
            assertTrue(plan.request().sql().contains(" = " + qualifier));
            assertTrue(plan.request().sql().contains(" or " + qualifier + " > ?"));
            assertEquals(List.of(7L, BigDecimal.TEN), plan.request().parameters());
        }
    }

    @Test
    void joinedReportPlansGroupHavingAndPageWithoutDummyProjection() {
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormAggregatePlanner.Plan plan = planner(dialect).planPage(report(), PageQuery.of(2, 7), false);
            java.util.function.Function<String, String> q = dialect.schema()::identifier;
            String name = q.apply("t0") + "." + q.apply("name");
            String amount = q.apply("t1") + "." + q.apply("amount");
            String base = "select " + name + " as " + q.apply("customer") + ", sum(" + amount + ") as " + q.apply("gross")
                    + " from " + q.apply("customers") + " " + q.apply("t0")
                    + " left outer join (select * from " + q.apply("orders") + " where " + q.apply("status") + " = ?) " + q.apply("t1")
                    + " on " + q.apply("t0") + "." + q.apply("id") + " = " + q.apply("t1") + "." + q.apply("customer_id")
                    + " where (" + name + " = ?) group by " + name + " having sum(" + amount + ") > ?";
            String count = dialect.name().equals("sqlserver") ? "count_big" : "count";
            assertEquals("select " + count + "(*) as " + q.apply("total") + " from (" + base + ") "
                    + q.apply("flying_aggregate_count"), plan.countRequest().orElseThrow().sql());
            assertEquals(List.of("paid", "Ada", BigDecimal.TEN), plan.countRequest().orElseThrow().parameters());
            assertEquals(dialect.pagination().paginate(base + " order by " + q.apply("gross") + " desc",
                    List.of("paid", "Ada", BigDecimal.TEN), PageQuery.of(2, 7)).sql(), plan.request().sql());
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("customer", "Ada"); values.put("gross", "25.50");
            AggregateRow row = new AggregateResultDecoder(plan).decode(DynamicRow.copyOf(values));
            assertEquals("Ada", row.get(CUSTOMER, String.class));
            assertEquals(new BigDecimal("25.50"), row.get(GROSS));
        }
    }

    private static AggregateSpec report() {
        JoinQuerySpec.Builder input = JoinQuerySpec.builder(CUSTOMERS);
        JoinSource orders = input.join(JoinType.LEFT, ORDERS, input.root(), "id", "customer_id");
        input.scope(orders, DataScope.where(ConditionGroup.and().where("status", "=", "paid").build()));
        input.where(input.root(), ConditionGroup.and().where("name", "=", "Ada").build());
        return AggregateSpec.builder(input.buildRelation()).group(CUSTOMER).aggregate(orders, GROSS)
                .having(ConditionGroup.and().where("gross", ">", BigDecimal.TEN).build())
                .orderBy(PageSort.desc("gross")).build();
    }

    private static FormAggregatePlanner planner(RdbDialect dialect) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), dialect);
        return new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults());
    }
}
