package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
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

import static org.junit.jupiter.api.Assertions.assertEquals;

class AggregateJdbcR2dbcDecodeParityTest {

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
        AggregateExpression<BigDecimal> average = AggregateExpression.avg("amount", "average_amount");
        AggregateExpression<Instant> latest = AggregateExpression.max(
                "occurred_at", "latest", LogicalType.OFFSET_TIMESTAMP, Instant.class);
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(
                        form, ConditionGroup.and().build()))
                .aggregate(count)
                .aggregate(average)
                .aggregate(latest)
                .build();
        OffsetDateTime timestamp = OffsetDateTime.of(
                2026, 9, 3, 2, 30, 0, 0, ZoneOffset.UTC);
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("total", BigInteger.valueOf(7));
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
        SyncSqlExecutor syncExecutor = new SyncSqlExecutor() {
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
                                                           SqlExecutionOptions options) {
                throw new UnsupportedOperationException();
            }
        };
        SyncBatchExecutor batchExecutor = new SyncBatchExecutor() {
            @Override
            public BatchWriteResult writeBatch(BatchWriteRequest request) {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
                throw new UnsupportedOperationException();
            }
        };

        AggregateRow reactive = ReactiveFormClient.create(reactiveExecutor, renderer)
                .aggregate(spec).single().block();
        AggregateRow sync = SyncFormClient.create(syncExecutor, batchExecutor, renderer)
                .aggregate(spec).getFirst();

        assertEquals(7L, reactive.get(count));
        assertEquals(new BigDecimal("2.50"), reactive.get(average));
        assertEquals(timestamp.toInstant(), reactive.get(latest));
        assertEquals(reactive.values(), sync.values());
    }
}
