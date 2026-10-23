package com.flying.orm.rdb.form;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.expression.SqlExpressionFunctions;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class UpdateExpressionExecutionTest {
    private static final DynamicForm FORM = DynamicForm.builder("expression_sales", "expression_sales")
            .addField(DynamicField.of("id", "BIGINT"))
            .addField(DynamicField.of("amount", "DECIMAL"))
            .addField(DynamicField.of("quantity", "INTEGER"))
            .addField(DynamicField.of("status", "VARCHAR"))
            .addField(DynamicField.of("version", "BIGINT")).build();

    @TestFactory
    Stream<DynamicTest> executesExpressionUpdatesThroughBothPublicClients() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest(
                reactive ? "R2DBC expressions" : "JDBC expressions", () -> execute(reactive)));
    }

    private void execute(boolean reactive) throws Exception {
        String name = "expression_update_" + reactive;
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
                     ConnectionAccessTestSupport.reactive(factory)).configuredDialect("h2")
                     .renderer(SqlRenderer.builder().addDefaultTerms().expressionFunctions(SqlExpressionFunctions.builder()
                             .register("constant-one", call -> { call.requireArity(1); call.sql("1"); return LogicalType.INTEGER; })
                             .build()).build()).build()) {
            statement.execute("create table expression_sales(id bigint primary key, amount decimal(12,2), quantity int, status varchar(12), version bigint)");
            statement.execute("insert into expression_sales values(1,10,2,null,7),(2,20,3,null,7)");
            ValueExpression amount = ValueExpression.field("amount").add(ValueExpression.value(3))
                    .multiply(ValueExpression.field("quantity"));
            long changed = reactive
                    ? clients.operator().dml().update(FORM).setExpression("amount", amount).where("id", 1L)
                            .optimisticLock(OptimisticLockOptions.increment("version", 7L)).execute().block()
                    : clients.syncOperator().dml().update(FORM).setExpression("amount", amount).where("id", 1L)
                            .optimisticLock(OptimisticLockOptions.increment("version", 7L)).execute();
            assertEquals(1, changed);
            assertRow(statement, 1, "26.00", null, 8);

            if (reactive) clients.forms().entity(Sale.class).update().setExpression(Sale::status,
                    e -> e.field(Sale::status).coalesce(e.value(Sale::status, Status.DISABLED)))
                    .where(Sale::id, 1L).execute().block();
            else clients.syncForms().entity(Sale.class).update().setExpression(Sale::status,
                    e -> e.field(Sale::status).coalesce(e.value(Sale::status, Status.DISABLED)))
                    .where(Sale::id, 1L).execute();
            assertRow(statement, 1, "26.00", "D", 8);

            ValueExpression next = ValueExpression.field("amount").multiply(ValueExpression.value(2)).add(ValueExpression.value(1));
            BatchSpec batch = BatchSpec.update(FORM, Flux.fromIterable(List.of(
                    new BatchUpdate(Map.of("amount", next), where(1)), new BatchUpdate(Map.of("amount", next), where(2)))));
            BatchExecutionEvidence evidence = reactive ? clients.forms().writeBatch(batch).block() : clients.syncForms().writeBatch(batch);
            assertEquals(BatchExecutionState.SUCCESS, evidence.state());
            assertEquals(2, evidence.successfulCount());
            assertRow(statement, 1, "53.00", "D", 8);
            assertRow(statement, 2, "41.00", null, 7);

            long copied = reactive ? clients.operator().dml().update("expression_sales")
                    .setExpression("quantity", ValueExpression.field("version")).where("id", 2L).execute().block()
                    : clients.syncOperator().dml().update("expression_sales")
                    .setExpression("quantity", ValueExpression.field("version")).where("id", 2L).execute();
            assertEquals(1, copied);
            try (ResultSet result = statement.executeQuery("select quantity from expression_sales where id=2")) {
                assertTrue(result.next());
                assertEquals(7, result.getInt(1));
            }

            if (reactive) clients.operator().dml().update(FORM).setExpression("status", ValueExpression.value(null))
                    .where("id", 1L).execute().block();
            else clients.syncOperator().dml().update(FORM).setExpression("status", ValueExpression.value(null))
                    .where("id", 1L).execute();
            assertRow(statement, 1, "53.00", null, 8);

            FieldUsePolicy policy = FieldUsePolicy.builder().allow("amount", FieldUse.UPDATE)
                    .allow("id", FieldUse.FILTER).allow("quantity", FieldUse.FULL_VALUE).build();
            BatchSpec governed = BatchSpec.update(FORM, Flux.just(
                    new BatchUpdate(Map.of("amount", ValueExpression.call("constant-one", ValueExpression.field("quantity"))), where(1)),
                    new BatchUpdate(Map.of("amount", ValueExpression.call("constant-one", ValueExpression.field("version"))), where(2))));
            BatchExecutionEvidenceException rejected = assertThrows(BatchExecutionEvidenceException.class, () -> {
                if (reactive) clients.forms().withFieldUsePolicy(policy).writeBatch(governed).block();
                else clients.syncForms().withFieldUsePolicy(policy).writeBatch(governed);
            });
            assertInstanceOf(ScopeAccessException.class, rejected.getCause());
            assertRow(statement, 2, "41.00", null, 7);
        }
    }

    private static ConditionGroup where(long id) {
        return ConditionGroup.and().where("id", "=", id).build();
    }

    private static void assertRow(Statement statement, long id, String amount, String status, long version) throws Exception {
        try (ResultSet result = statement.executeQuery("select amount,status,version from expression_sales where id=" + id)) {
            assertTrue(result.next());
            assertEquals(new BigDecimal(amount), result.getBigDecimal(1));
            assertEquals(status, result.getString(2));
            assertEquals(version, result.getLong(3));
        }
    }

    enum Status {
        DISABLED("D");
        @EnumValue private final String code;
        Status(String code) { this.code = code; }
    }

    @TableName("expression_sales")
    record Sale(@TableId(type = IdType.INPUT) Long id, Status status) { }
}
