package com.flying.orm.rdb.form;

import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.AggregateRow;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.execution.SqlRowLimitExceededException;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import org.h2.jdbcx.JdbcDataSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.concurrent.atomic.AtomicInteger;
import java.sql.Connection;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AggregateJdbcR2dbcDecodeParityTest {

    @Test
    void nativeJdbcBindsOnceAndRetainsCallerRequestedResultBudgets() throws Exception {
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:aggregate_binding;DATABASE_TO_LOWER=TRUE");
        RdbDialect dialect = RdbDialect.h2();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), dialect);
        DynamicForm form = DynamicForm.builder("sales", "sales")
                .addField(DynamicField.of("category", "VARCHAR"))
                .addField(DynamicField.of("amount", "DECIMAL")).build();
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(form, ConditionGroup.and().build())
                        .withSorts(List.of(PageSort.asc("category"))))
                .group("category", "category").sum("amount", "total").countRows("rows").build();
        FormAggregatePlanner.Plan plan = new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults()).plan(spec);
        AggregateResultDecoder decoder = new AggregateResultDecoder(plan);
        AtomicInteger bindings = new AtomicInteger();
        AtomicInteger unboundRows = new AtomicInteger();
        RowMapper<AggregateRow> mapper = new RowMapper<>() {
            public AggregateRow map(DynamicRow row) {
                unboundRows.incrementAndGet();
                return decoder.decode(row);
            }
            public Function<IntFunction<Object>, AggregateRow> bindColumns(DynamicRow columns) {
                bindings.incrementAndGet();
                return decoder.bindColumns(columns);
            }
        };
        try (Connection keeper = source.getConnection(); Statement setup = keeper.createStatement()) {
            setup.execute("create table sales(category varchar(20), amount decimal(10,2))");
            setup.execute("insert into sales values ('a',2),('a',3),('b',7),('c',null)");
            SyncSqlExecutor executor = SyncSqlExecutor.jdbc(ConnectionAccessTestSupport.jdbc(source), dialect);
            List<AggregateRow> rows = executor.queryMapped(plan.request(), plan.options(), mapper, 0);
            assertEquals(1, bindings.get());
            assertEquals(0, unboundRows.get());
            assertEquals(List.of("a", "b", "c"), rows.stream().map(row -> row.get("category", String.class)).toList());
            assertEquals(new BigDecimal("5.00"), rows.getFirst().get("total", BigDecimal.class));
            assertEquals(2L, rows.getFirst().get("rows", Long.class));
            assertEquals(null, rows.getLast().get("total", BigDecimal.class));
            assertThrows(SqlRowLimitExceededException.class, () -> executor.queryMapped(
                    plan.request(), plan.options().withMaxRows(1), decoder, 0));
            assertThrows(SqlResultMemoryLimitExceededException.class, () -> executor.queryMapped(
                    plan.request(), plan.options().withMaxResultBytes(1), decoder, 0));
            SyncFormClient client = SyncFormClient.create(executor,
                    (request, completed) -> { throw new AssertionError("unexpected batch"); }, renderer);
            assertEquals(rows.stream().map(AggregateRow::values).toList(),
                    client.aggregate(spec).stream().map(AggregateRow::values).toList());
            assertThrows(SqlResultMemoryLimitExceededException.class, () -> client
                    .withDefaultExecutionOptions(plan.options().withMaxResultBytes(1)).aggregate(spec));
        }
    }

    @Test
    void jdbcAndR2dbcClientsShareTheSameLayoutAndDecoder() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("amount", "DECIMAL"))
                .addField(DynamicField.of("occurred_at", "TIMESTAMPTZ"))
                .build();
        AggregateExpression<Long> count = AggregateExpression.count("id", "total");
        AggregateExpression<Long> rows = AggregateExpression.countRows("row_count");
        AggregateExpression<BigDecimal> average = AggregateExpression.avg("amount", "average_amount");
        AggregateExpression<Instant> latest = AggregateExpression.max(
                "occurred_at", "latest", LogicalType.OFFSET_TIMESTAMP, Instant.class);
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(
                        form, ConditionGroup.and().build()))
                .aggregate(count)
                .aggregate(rows)
                .aggregate(average)
                .aggregate(latest)
                .build();
        OffsetDateTime timestamp = OffsetDateTime.of(
                2026, 9, 3, 2, 30, 0, 0, ZoneOffset.UTC);
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("total", BigInteger.valueOf(7));
        values.put("row_count", new BigDecimal("8.0"));
        values.put("average_amount", new BigDecimal("2.50"));
        values.put("latest", timestamp);
        DynamicRow raw = DynamicRow.copyOf(values);

        ReactiveSqlExecutor reactiveExecutor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.just(raw);
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.error(new UnsupportedOperationException());
            }
        };
        SyncSqlExecutor syncExecutor = new com.flying.orm.rdb.support.ListQueryFixture() {
            @Override
            public <T> List<T> queryMapped(SqlRequest request, SqlExecutionOptions options,
                                           RowMapper<T> mapper, int rowLimit) {
                Function<IntFunction<Object>, T> bound = mapper.bindColumns(raw);
                assertNotNull(bound, "ordinary aggregate execution must not hide the bound decoder in a wrapper");
                return List.of(bound.apply(raw::value));
            }

            @Override
            public List<DynamicRow> query(SqlRequest request) {
                return List.of(raw);
            }

            @Override
            public long rowsUpdated(SqlRequest request) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request,
                                                           SqlExecutionOptions options, String generatedKeyColumn) {
                throw new UnsupportedOperationException();
            }
        };
        SyncBatchExecutor batchExecutor = new SyncBatchExecutor() {
            @Override
            public BatchExecutionEvidence writeBatch(BatchWriteRequest request, java.util.function.LongConsumer rowCompleted) {
                throw new UnsupportedOperationException();
            }

        };

        AggregateRow reactive = ReactiveFormClient.create(reactiveExecutor, renderer)
                .aggregate(spec).single().block();
        AggregateRow sync = SyncFormClient.create(syncExecutor, batchExecutor, renderer)
                .aggregate(spec).getFirst();

        assertEquals(7L, reactive.get(count));
        assertEquals(8L, reactive.get(rows));
        assertEquals(new BigDecimal("2.50"), reactive.get(average));
        assertEquals(timestamp.toInstant(), reactive.get(latest));
        assertEquals(reactive.values(), sync.values());
    }
}
