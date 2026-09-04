package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class ReactiveFormBatchValueSnapshotTest {

    @Test
    void listBatchFreezesByteBufferBeforeColdSubscription() {
        AtomicReference<ByteBuffer> boundValue = new AtomicReference<>();
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.error(new UnsupportedOperationException());
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.error(new UnsupportedOperationException());
            }

            @Override
            public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
                return Flux.from(request.rows())
                           .next()
                           .map(row -> {
                               boundValue.set((ByteBuffer) row[0]);
                               return BatchWriteResult.empty(request.options().mode());
                           });
            }
        };
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer);
        DynamicForm form = DynamicForm.builder("payloads", "payloads")
                                      .addField(DynamicField.of("payload", "BLOB"))
                                      .build();
        ByteBuffer source = ByteBuffer.wrap(new byte[]{1, 2});

        Mono<BatchWriteResult> write = client.operations().batchInserts.insertBatch(
                form, List.of(Map.of("payload", source)), BatchWriteOptions.defaults());
        source.put(0, (byte) 9);
        write.block();

        assertEquals(1, boundValue.get().get(0));
    }

    @Test
    void batchLayoutUsesTheSameVectorCodecAsSingleWrites() {
        DynamicField vector = DynamicField.of("embedding", "VECTOR").withLength(2);
        DynamicForm form = DynamicForm.builder("documents", "documents").addField(vector).build();
        BatchWriteRequest request = renderer().insertBatch(
                form, List.of(Map.of("embedding", List.of(1.5d, 2.5d))));

        Object[] parameters = Flux.from(request.rows()).blockFirst();

        float[] actual = assertInstanceOf(float[].class, parameters[0]);
        assertEquals(1.5f, actual[0]);
        assertEquals(2.5f, actual[1]);
    }

    @Test
    void listBatchFreezesFieldAwareContainersBeforeColdSubscription() {
        AtomicReference<Object[]> bound = new AtomicReference<>();
        ReactiveSqlExecutor executor = capturingExecutor(bound);
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        DynamicForm form = DynamicForm.builder("documents", "documents")
                                      .addField(DynamicField.of("payload", "JSON"))
                                      .addField(DynamicField.of("embedding", "VECTOR").withLength(2))
                                      .build();
        List<Object> jsonItems = new ArrayList<>(List.of("first"));
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("items", jsonItems);
        List<Double> vector = new ArrayList<>(List.of(1.5d, 2.5d));

        Mono<BatchWriteResult> write = client.operations().batchInserts.insertBatch(
                form, List.of(Map.of("payload", json, "embedding", vector)), BatchWriteOptions.defaults());
        jsonItems.add("later");
        vector.set(0, 9.5d);
        write.block();

        String encodedJson = java.util.Arrays.stream(bound.get())
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .findFirst()
                .orElseThrow();
        float[] actual = java.util.Arrays.stream(bound.get())
                .filter(float[].class::isInstance)
                .map(float[].class::cast)
                .findFirst()
                .orElseThrow();
        assertEquals("{\"items\":[\"first\"]}", encodedJson);
        assertEquals(1.5f, actual[0]);
    }

    @Test
    void batchLayoutAppliesUuidToVarcharFallback() {
        DynamicField code = DynamicField.of("code", "VARCHAR");
        DynamicForm form = DynamicForm.builder("documents", "documents").addField(code).build();
        UUID source = UUID.fromString("ed82786d-c33b-4b2d-b3d7-c5769332a773");

        BatchWriteRequest request = renderer().insertBatch(form, List.of(Map.of("code", source)));
        Object[] parameters = Flux.from(request.rows()).blockFirst();

        assertEquals(source.toString(), parameters[0]);
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }

    private static ReactiveSqlExecutor capturingExecutor(AtomicReference<Object[]> bound) {
        return new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.error(new UnsupportedOperationException());
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.error(new UnsupportedOperationException());
            }

            @Override
            public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
                return Flux.from(request.rows()).next().map(row -> {
                    bound.set(row);
                    return BatchWriteResult.empty(request.options().mode());
                });
            }
        };
    }
}
