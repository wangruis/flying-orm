package com.flying.orm.rdb.codec;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 用真实 H2 R2DBC 驱动检查大字段参数和返回值，避免只在渲染层看起来能用。
 */
class H2R2dbcLargeObjectIntegrationTest {

    @Test
    void roundTripsBlobAndClobAcrossInsertAndBatchUpsert() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(
                H2ConnectionConfiguration.builder()
                                         .inMemory("form_large_object_roundtrip")
                                         .property("DB_CLOSE_DELAY", "-1")
                                         .build());
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);
        ReactiveFormClient client = ReactiveFormClient.create(
                executor, FormDataSqlRenderer.create(conditionRenderer(), RdbDialect.h2()));
        DynamicForm form = largeObjectForm();

        byte[] firstPayload = new byte[]{1, 2, 3};
        byte[] updatedPayload = new byte[]{4, 5, 6};
        ByteBuffer secondPayload = ByteBuffer.wrap(new byte[]{7, 8, 9});

        Mono<List<DynamicRow>> scenario = executor.rowsUpdated(SqlRequest.nativeSql(
                        "create table Attachments (id bigint primary key, payload blob, content clob)",
                        List.of()))
                .then(client.insert(WriteSpec.insert(
                        form,
                        row("id", 1L,
                                        "payload", firstPayload,
                                        "content", new StringBuilder("first text")))))
                .doOnNext(updated -> assertEquals(1L, updated))
                .then(client.writeBatch(BatchSpec.upsert(
                        form,
                        reactor.core.publisher.Flux.fromIterable(List.of(row("id", 1L,
                                                     "payload", updatedPayload,
                                                     "content", "updated text"),
                                                 row("id", 2L,
                                                     "payload", secondPayload,
                                                     "content", "second text"))))))
                .doOnNext(result -> {
                    assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
                    assertEquals(2L, result.inputCount());
                    assertEquals(2L, result.affectedRows());
                })
                .thenMany(client.select(QuerySpec.of(form, ConditionGroup.and().build())))
                .sort(Comparator.comparingLong(value -> ((Number) value(value, "id")).longValue()))
                .collectList();

        StepVerifier.create(scenario)
                    .assertNext(rows -> {
                        assertEquals(2, rows.size());
                        assertArrayEquals(updatedPayload, (byte[]) value(rows.get(0), "payload"));
                        assertEquals("updated text", value(rows.get(0), "content"));
                        assertArrayEquals(new byte[]{7, 8, 9}, (byte[]) value(rows.get(1), "payload"));
                        assertEquals("second text", value(rows.get(1), "content"));
                    })
                    .verifyComplete();
    }

    private static DynamicForm largeObjectForm() {
        return DynamicForm.builder("attachments", "Attachments")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("payload", "BLOB"))
                          .addField(DynamicField.of("content", "CLOB"))
                          .build();
    }

    private static SqlRenderer conditionRenderer() {
        return SqlRenderer.builder().addTerm(SqlTermHandler.equalsTo()).build();
    }

    private static Map<String, Object> row(Object... pairs) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int index = 0; index < pairs.length; index += 2) {
            values.put((String) pairs[index], pairs[index + 1]);
        }
        return values;
    }

    private static Object value(Map<String, Object> row, String name) {
        return row.entrySet()
                  .stream()
                  .filter(entry -> entry.getKey().equalsIgnoreCase(name))
                  .map(Map.Entry::getValue)
                  .findFirst()
                  .orElse(null);
    }
}
