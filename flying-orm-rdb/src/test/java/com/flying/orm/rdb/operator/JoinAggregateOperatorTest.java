package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.page.PageSort;
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

import static org.junit.jupiter.api.Assertions.*;

class JoinAggregateOperatorTest {
    private static final DynamicForm CUSTOMERS = DynamicForm.builder("customers", "customers")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("name", "VARCHAR")).build();
    private static final DynamicForm ORDERS = DynamicForm.builder("orders", "orders")
            .addField(DynamicField.of("customer_id", "BIGINT"))
            .addField(DynamicField.of("amount", "DECIMAL")).build();
    private static final AggregateExpression<BigDecimal> GROSS = AggregateExpression.sum("amount", "gross");

    @Test
    void fluentJoinedReportsUseTheSameTypedExecutorAndPageContract() {
        Fixture fixture = new Fixture();
        Mono<PageResult<AggregateRow>> pending = fixture.reactive().aggregatePage(report -> report
                .group(GroupSelection.of("name", "customer")).aggregate(ORDERS, GROSS)
                .orderBy(PageSort.desc("gross")), 2, 7);
        assertTrue(fixture.requests.isEmpty());
        PageResult<AggregateRow> reactive = pending.block();
        assertEquals(2, fixture.requests.size());
        pending.block();
        assertEquals(4, fixture.requests.size());
        PageResult<AggregateRow> sync = fixture.sync().aggregatePage(report -> report
                .group(GroupSelection.of("name", "customer")).aggregate(ORDERS, GROSS)
                .orderBy(PageSort.desc("gross")), 2, 7);
        assertEquals(reactive.rows().getFirst().values(), sync.rows().getFirst().values());
        assertEquals(3L, sync.total());
        assertEquals(new BigDecimal("25.50"), sync.rows().getFirst().get(GROSS));
        assertEquals(fixture.requests.get(0).sql(), fixture.requests.get(4).sql());
        assertEquals(fixture.requests.get(1).sql(), fixture.requests.get(5).sql());
        fixture.requests.clear();
        assertEquals(new BigDecimal("25.50"), fixture.reactive().aggregate(report -> report
                .group(GroupSelection.of("name", "customer")).aggregate(ORDERS, GROSS)).single().block().get(GROSS));
        assertEquals(new BigDecimal("25.50"), fixture.sync().aggregate(report -> report
                .group(GroupSelection.of("name", "customer")).aggregate(ORDERS, GROSS)).getFirst().get(GROSS));
        assertEquals(2, fixture.requests.size());
    }

    private static final class Fixture {
        private final List<SqlRequest> requests = new ArrayList<>();
        private final SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms().build();
        private final FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql());
        private final ReactiveFormClient reactive = ReactiveFormClient.create(new ReactiveSqlExecutor() {
            public Flux<DynamicRow> query(SqlRequest request) { return Flux.defer(() -> Flux.fromIterable(rows(request))); }
            public Mono<Long> rowsUpdated(SqlRequest request) { return Mono.error(new UnsupportedOperationException()); }
        }, renderer);
        private final SyncFormClient sync = SyncFormClient.create(new com.flying.orm.rdb.support.ListQueryFixture() {
            public List<DynamicRow> query(SqlRequest request) { return rows(request); }
            public long rowsUpdated(SqlRequest request) { throw new UnsupportedOperationException(); }
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
                throw new UnsupportedOperationException();
            }
        }, (request, completed) -> { throw new UnsupportedOperationException(); }, renderer);

        private JoinQueryOperator reactive() {
            return new JoinQueryOperator(reactive, conditions, CUSTOMERS).leftJoin(ORDERS, "id", "customer_id");
        }
        private SyncJoinQueryOperator sync() {
            return new SyncJoinQueryOperator(sync, conditions, CUSTOMERS).leftJoin(ORDERS, "id", "customer_id");
        }
        private List<DynamicRow> rows(SqlRequest request) {
            requests.add(request);
            if (request.sql().startsWith("select count(")) return List.of(DynamicRow.copyOf(Map.of("total", 3L)));
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("customer", "Ada"); row.put("gross", new BigDecimal("25.50"));
            return List.of(DynamicRow.copyOf(row));
        }
    }
}
