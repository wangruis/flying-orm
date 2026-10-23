package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FormBatchSqlRendererSequentialListTest {

    @Test
    void defaultOnlyInsertHasNoBindingsAndDoesNotRepeatDefaultValuesClause() {
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.sqlServer(), RdbDialect.oracle(), RdbDialect.h2())) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), dialect);
            String expected = switch (dialect.name()) {
                case "mysql" -> "insert into `batch_rows` () values ()";
                case "oracle" -> "insert into \"batch_rows\" (\"id\") values (default)";
                case "sqlserver" -> "insert into [batch_rows] default values";
                case "h2" -> "insert into batch_rows default values";
                default -> "insert into \"batch_rows\" default values";
            };
            assertEquals(expected, renderer.insert(form(), Map.of()).sql(), dialect.name());
            BatchWriteRequest request = renderer.insertBatch(form(), List.of(Map.of(), Map.of(), Map.of()));
            assertEquals(expected, request.sql(), dialect.name());
            assertEquals(List.of(), request.parameterTypes());
            assertEquals(List.of(List.of(), List.of(), List.of()), parameterRows(request));
            assertEquals(null, com.flying.orm.rdb.internal.plan.SqlExecutionStatements.r2dbcInsertBatch(
                    request.statement(), dialect.name(), 3));
        }
    }

    @Test
    void defaultOnlyInsertDoesNotAllowAnEmptyUpdateOrAnIncompleteUpsertKey() {
        assertThrows(IllegalArgumentException.class, () -> renderer().update(form(), Map.of(),
                com.flying.orm.core.condition.ConditionGroup.and().where("id", "=", 1L).build()));
        assertThrows(IllegalArgumentException.class, () -> renderer().upsertBatch(form(), List.of(Map.of())));
    }

    @Test
    void singleRowRemainsReplayableIndependentAndDemandDriven() {
        for (BatchOperation operation : BatchOperation.values()) {
            Map<String, Object> first = row(1L, "first", (byte) 1);
            CountingLinkedList<Map<String, Object>> rows = new CountingLinkedList<>(List.of(first));
            BatchWriteRequest request = operation.render(renderer(), form(), rows);
            List<List<Object>> expected = parameterRows(request);
            first.put("payload", ByteBuffer.wrap(new byte[]{99, 100}));
            first.clear();
            rows.clear();
            assertEquals(expected, parameterRows(request), operation.name());
            assertEquals(0, rows.indexedGetCalls());

            List<Object[]> received = new ArrayList<>();
            BaseSubscriber<Object[]> subscriber = new BaseSubscriber<>() {
                @Override protected void hookOnSubscribe(Subscription subscription) { }
                @Override protected void hookOnNext(Object[] value) { received.add(value); }
            };
            Flux.from(request.rows()).subscribe(subscriber);
            assertTrue(received.isEmpty(), "no row before demand");
            subscriber.request(1);
            assertEquals(1, received.size());
            subscriber.request(1);
            assertEquals(1, received.size(), "single row must not be emitted twice");
            assertEquals(expected, parameterRows(request), "a new subscription can replay the batch");
        }
    }

    @Test
    void defaultBatchOperationsPrepareSequentialListRowsWithoutIndexedReads() {
        for (BatchOperation operation : BatchOperation.values()) {
            BatchWriteRequest expected = operation.render(renderer(), form(), arrayRows());
            List<List<Object>> expectedParameters = parameterRows(expected);
            CountingLinkedList<Map<String, Object>> sequentialRows = sequentialRows();

            BatchWriteRequest actual = operation.render(renderer(), form(), sequentialRows);

            assertEquals(expected.sql(), actual.sql(), operation.name());
            assertEquals(expected.parameterTypes(), actual.parameterTypes(), operation.name());
            assertEquals(expectedParameters, parameterRows(actual), operation.name());
            assertEquals(0, sequentialRows.indexedGetCalls(), operation.name());

            sequentialRows.getFirst().clear();
            sequentialRows.clear();

            assertEquals(expectedParameters, parameterRows(actual), operation.name());
        }
    }

    @Test
    void laterInvalidRowKeepsItsOriginalBatchIndex() {
        for (BatchOperation operation : BatchOperation.values()) {
            CountingLinkedList<Map<String, Object>> rows = sequentialRows();
            rows.set(1, Map.of("id", 2L));

            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> operation.render(renderer(), form(), rows), operation.name());
            assertEquals("batch insert row [1] fields must match the first row", error.getMessage(), operation.name());
        }
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }

    private static DynamicForm form() {
        return DynamicForm.builder("batch_rows", "batch_rows")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("payload", "BLOB"))
                          .build();
    }

    private static List<Map<String, Object>> arrayRows() {
        return new ArrayList<>(rows());
    }

    private static CountingLinkedList<Map<String, Object>> sequentialRows() {
        return new CountingLinkedList<>(rows());
    }

    private static List<Map<String, Object>> rows() {
        return List.of(row(1L, "first", (byte) 1),
                       row(2L, "second", (byte) 3),
                       row(3L, "third", (byte) 5));
    }

    private static Map<String, Object> row(long id, String name, byte payload) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("name", name);
        row.put("payload", ByteBuffer.wrap(new byte[]{payload, (byte) (payload + 1)}));
        return row;
    }

    private static List<List<Object>> parameterRows(BatchWriteRequest request) {
        return Flux.from(request.rows())
                   .map(row -> Arrays.stream(row)
                                     .map(FormBatchSqlRendererSequentialListTest::parameterValue)
                                     .toList())
                   .collectList()
                   .block();
    }

    private static Object parameterValue(Object value) {
        if (!(value instanceof ByteBuffer buffer)) {
            return value;
        }
        ByteBuffer copy = buffer.asReadOnlyBuffer();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return Arrays.toString(bytes);
    }

    private enum BatchOperation {
        INSERT {
            @Override
            BatchWriteRequest render(FormDataSqlRenderer renderer,
                                     DynamicForm form,
                                     List<Map<String, Object>> rows) {
                return renderer.insertBatch(form, rows);
            }
        },
        UPSERT {
            @Override
            BatchWriteRequest render(FormDataSqlRenderer renderer,
                                     DynamicForm form,
                                     List<Map<String, Object>> rows) {
                return renderer.upsertBatch(form, rows);
            }
        };

        abstract BatchWriteRequest render(FormDataSqlRenderer renderer,
                                          DynamicForm form,
                                          List<Map<String, Object>> rows);
    }

    private static final class CountingLinkedList<E> extends LinkedList<E> {

        private int indexedGetCalls;

        private CountingLinkedList(List<E> values) {
            super(values);
        }

        @Override
        public E get(int index) {
            indexedGetCalls++;
            return super.get(index);
        }

        private int indexedGetCalls() {
            return indexedGetCalls;
        }
    }
}
