package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import com.flying.orm.rdb.execution.SqlLargeObjectLimitExceededException;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 字段解码计划必须同时服务 JDBC 物化结果和响应式结果。 */
class FormResultDecoderTest {

    @Test
    void ordinaryUnprotectedRowsKeepTheOriginalQueryContainers() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
        DynamicForm form = DynamicForm.builder("items", "items")
                                      .addField(DynamicField.of("name", "VARCHAR"))
                                      .build();
        List<DynamicRow> rows = List.of(DynamicRow.copyOf(Map.of("name", "first")));
        Flux<DynamicRow> rowFlux = Flux.fromIterable(rows);

        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            FormResultDecoder decoder = new FormResultDecoder(renderer, models);

            assertSame(rows, decoder.decodeRows(form, rows, SqlExecutionOptions.safeDefaults()));
            assertSame(rowFlux, decoder.decodeRows(form, rowFlux, SqlExecutionOptions.safeDefaults()));
        }
    }

    @Test
    void materializedLargeObjectFieldsDoNotScheduleAdditionalExecutionDeadlines() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
        DynamicForm form = DynamicForm.builder("documents", "documents")
                .addField(DynamicField.of("body", "CLOB"))
                .addField(DynamicField.of("content", "BLOB"))
                .build();
        byte[] content = {1, 2, 3};
        DynamicRow rawRow = DynamicRow.copyOf(Map.of("body", "text", "content", content));
        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults()
                .withTimeout(Duration.ofSeconds(30)).withCleanupTimeout(Duration.ZERO);
        AtomicInteger scheduledTasks = new AtomicInteger();
        String hook = getClass().getName() + ".materializedLargeObjects";

        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            FormResultDecoder decoder = new FormResultDecoder(renderer, models);
            Schedulers.onScheduleHook(hook, task -> {
                scheduledTasks.incrementAndGet();
                return task;
            });
            try {
                DynamicRow decoded = decoder.decodeRows(form, Flux.just(rawRow), options).single().block();

                assertEquals("text", decoded.get("body"));
                assertArrayEquals(content, assertInstanceOf(byte[].class, decoded.get("content")));
                assertEquals(0, scheduledTasks.get(),
                        "already materialized fields must not start another execution deadline");
            } finally {
                Schedulers.resetOnScheduleHook(hook);
            }
        }
    }

    @Test
    void materializedLargeObjectFieldsKeepTheirSizeLimits() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
        DynamicForm form = DynamicForm.builder("documents", "documents")
                .addField(DynamicField.of("body", "CLOB"))
                .addField(DynamicField.of("content", "BLOB"))
                .build();
        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults()
                .withTimeout(Duration.ofSeconds(30)).withCleanupTimeout(Duration.ZERO)
                .withMaxLargeObjectBytes(2).withMaxLargeObjectChars(2);

        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            FormResultDecoder decoder = new FormResultDecoder(renderer, models);
            SqlLargeObjectLimitExceededException binary = assertThrows(
                    SqlLargeObjectLimitExceededException.class,
                    () -> decoder.decodeRows(form,
                            Flux.just(DynamicRow.copyOf(Map.of("content", new byte[]{1, 2, 3}))), options)
                            .single().block());
            SqlLargeObjectLimitExceededException character = assertThrows(
                    SqlLargeObjectLimitExceededException.class,
                    () -> decoder.decodeRows(form,
                            Flux.just(DynamicRow.copyOf(Map.of("body", "text"))), options).single().block());

            assertEquals(SqlLargeObjectLimitExceededException.Kind.BINARY, binary.kind());
            assertEquals(2, binary.maxSize());
            assertEquals(3, binary.actualSize());
            assertEquals(SqlLargeObjectLimitExceededException.Kind.CHARACTER, character.kind());
            assertEquals(2, character.maxSize());
            assertEquals(4, character.actualSize());
        }
    }

    @TestFactory
    Stream<DynamicTest> rawLargeObjectFieldsKeepTheirReadTimeoutAndCancelContent() {
        return Stream.of("BLOB", "CLOB").map(dataType -> DynamicTest.dynamicTest(dataType, () -> {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
            DynamicForm form = DynamicForm.builder("documents", "documents")
                    .addField(DynamicField.of("content", dataType)).build();
            AtomicBoolean cancelled = new AtomicBoolean();
            Object locator = dataType.equals("BLOB")
                    ? Blob.from(Flux.<ByteBuffer>never().doOnCancel(() -> cancelled.set(true)))
                    : Clob.from(Flux.<CharSequence>never().doOnCancel(() -> cancelled.set(true)));
            SqlExecutionOptions options = SqlExecutionOptions.safeDefaults()
                    .withTimeout(Duration.ofSeconds(30)).withCleanupTimeout(Duration.ZERO);
            AtomicReference<Runnable> deadline = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            String hook = getClass().getName() + ".rawLargeObjects";

            try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
                FormResultDecoder decoder = new FormResultDecoder(renderer, models);
                Schedulers.onScheduleHook(hook, task -> {
                    deadline.set(task);
                    return task;
                });
                Disposable subscription = null;
                try {
                    subscription = decoder.decodeRows(form,
                            Flux.just(DynamicRow.copyOf(Map.of("content", locator))), options)
                            .subscribe(ignored -> { }, failure::set);

                    assertNotNull(deadline.get(), "a raw LOB must retain its read deadline");
                    deadline.get().run();
                    assertInstanceOf(SqlExecutionTimeoutException.class, failure.get());
                    assertTrue(cancelled.get(), "a read timeout must cancel the LOB content subscription");
                } finally {
                    if (subscription != null) {
                        subscription.dispose();
                    }
                    Schedulers.resetOnScheduleHook(hook);
                }
            }
        }));
    }

    @Test
    void appliesPrecomputedMySqlScalarPlanToJdbcAndReactiveRows() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql());
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("created_at", "TIMESTAMPTZ(6)"))
                                      .build();
        LocalDateTime rawTime = LocalDateTime.of(2026, 8, 21, 3, 4, 5, 123456000);
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("created_at", rawTime);
        values.put("calculated_alias", "unchanged");
        DynamicRow rawRow = DynamicRow.copyOf(values);

        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            FormResultDecoder decoder = new FormResultDecoder(renderer, models);

            DynamicRow jdbcRow = decoder.decodeRows(
                    form, List.of(rawRow), SqlExecutionOptions.safeDefaults()).getFirst();
            DynamicRow reactiveRow = decoder.decodeRows(
                    form, Flux.just(rawRow), SqlExecutionOptions.safeDefaults()).blockFirst();

            OffsetDateTime expected = rawTime.atOffset(ZoneOffset.UTC);
            assertEquals(expected, assertInstanceOf(OffsetDateTime.class, jdbcRow.value(0)));
            assertEquals(expected, assertInstanceOf(OffsetDateTime.class, reactiveRow.value(0)));
            assertEquals("unchanged", jdbcRow.value(1));
            assertEquals("unchanged", reactiveRow.value(1));
        }
    }

    @Test
    void doesNotTreatAnArbitraryTimestampSuffixAsTheMySqlUtcContract() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql());
        LocalDateTime rawTime = LocalDateTime.of(2026, 8, 21, 3, 4, 5, 123456000);

        Object decoded = renderer.readScalarValue(
                DynamicField.of("created_at", "TIMESTAMPTZ(6) INVALID"), rawTime);

        assertEquals(rawTime, decoded);
    }

    @Test
    void roundTripsStandardOffsetTimeAliasAcrossTextBackedFormPaths() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql());
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("local_clock", "TIME(6) WITH TIME ZONE"))
                                      .build();
        OffsetTime expected = OffsetTime.parse("12:34:56.123456+08:00");
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("local_clock", expected);

        assertEquals(expected.toString(), renderer.insert(form, values).parameters().getFirst());
        BatchInsertPlan plan = renderer.batchRenderer.insertPlan(form, values);
        assertEquals(List.of(String.class), plan.parameterTypes());
        assertEquals(expected.toString(), plan.parameters(values, 0)[0]);

        Map<String, Object> nullValues = new LinkedHashMap<>();
        nullValues.put("local_clock", null);
        assertEquals(String.class,
                     renderer.batchRenderer.insertPlan(form, nullValues).parameterTypes().getFirst());
        SqlNullParameter nullParameter = (SqlNullParameter) renderer.insert(form, nullValues)
                .parameters().getFirst();
        assertEquals(String.class, nullParameter.javaType());

        DynamicRow rawRow = DynamicRow.copyOf(Map.of("local_clock", expected.toString()));
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            FormResultDecoder decoder = new FormResultDecoder(renderer, models);
            DynamicRow jdbcRow = decoder.decodeRows(
                    form, List.of(rawRow), SqlExecutionOptions.safeDefaults()).getFirst();
            DynamicRow reactiveRow = decoder.decodeRows(
                    form, Flux.just(rawRow), SqlExecutionOptions.safeDefaults()).blockFirst();

            assertEquals(expected, assertInstanceOf(OffsetTime.class, jdbcRow.value(0)));
            assertEquals(expected, assertInstanceOf(OffsetTime.class, reactiveRow.value(0)));
        }
    }

    @Test
    void decodesJdbcTemporalArrayElementsFromTheDeclaredFieldType() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("local_times", "TIME(6)[]"))
                                      .addField(DynamicField.of("local_stamps", "TIMESTAMP(6)[]"))
                                      .addField(DynamicField.of("absolute_stamps", "TIMESTAMPTZ(6)[]"))
                                      .build();
        Time rawTime = Time.valueOf("12:34:56");
        LocalDateTime expectedLocalTimestamp = LocalDateTime.of(2026, 8, 22, 10, 11, 12, 123456789);
        Timestamp rawLocalTimestamp = Timestamp.valueOf(expectedLocalTimestamp);
        Timestamp rawAbsoluteTimestamp = Timestamp.from(Instant.parse("2026-08-22T10:11:12.123456789Z"));
        DynamicRow rawRow = DynamicRow.copyOf(Map.of(
                "local_times", new Time[]{rawTime},
                "local_stamps", new Timestamp[]{rawLocalTimestamp},
                "absolute_stamps", new Timestamp[]{rawAbsoluteTimestamp}));

        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            FormResultDecoder decoder = new FormResultDecoder(renderer, models);
            DynamicRow jdbcRow = decoder.decodeRows(
                    form, List.of(rawRow), SqlExecutionOptions.safeDefaults()).getFirst();
            DynamicRow reactiveRow = decoder.decodeRows(
                    form, Flux.just(rawRow), SqlExecutionOptions.safeDefaults()).blockFirst();

            assertEquals(List.of(LocalTime.of(12, 34, 56)), jdbcRow.get("local_times"));
            assertEquals(List.of(expectedLocalTimestamp), jdbcRow.get("local_stamps"));
            assertEquals(List.of(rawAbsoluteTimestamp.toInstant().atOffset(ZoneOffset.UTC)),
                         jdbcRow.get("absolute_stamps"));
            assertEquals(jdbcRow.toMap(), reactiveRow.toMap());
        }
    }

    @Test
    void rejectsJdbcTimeArrayWhenTheDeclaredTypeRequiresAnOffset() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("offset_times", "TIME(6) WITH TIME ZONE[]"))
                                      .build();
        DynamicRow rawRow = DynamicRow.copyOf(Map.of(
                "offset_times", new Time[]{Time.valueOf("12:34:56")}));

        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            FormResultDecoder decoder = new FormResultDecoder(renderer, models);
            assertThrows(IllegalArgumentException.class, () -> decoder.decodeRows(
                    form, List.of(rawRow), SqlExecutionOptions.safeDefaults()));
        }
    }

    @Test
    void bindsOnlyProjectedSpecialFieldsOncePerSharedResultLayout() throws Exception {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql());
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("created_at", "TIMESTAMPTZ(6)"))
                                      .build();
        List<DynamicRow> rows = sharedRows(
                new String[]{"created_at", "plain_alias", "second_alias"},
                new Object[]{LocalDateTime.of(2026, 8, 30, 1, 2), "first", 1},
                new Object[]{LocalDateTime.of(2026, 8, 30, 2, 3), "second", 2});
        DynamicRow first = rows.getFirst();

        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            FormResultDecoder decoder = new FormResultDecoder(renderer, models);
            List<DynamicRow> decoded = decoder.decodeRows(
                    form, rows, SqlExecutionOptions.safeDefaults());

            assertInstanceOf(OffsetDateTime.class, decoded.getFirst().get("created_at"));
            Map<?, ?> bindings = layoutBindings(first);
            assertEquals(1, bindings.size());
            assertEquals(1, assertInstanceOf(List.class, bindings.values().iterator().next()).size());
        }
    }

    @Test
    void synchronousReactiveDecodingPreservesDownstreamDemandWithoutInnerPublishers() throws Exception {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql());
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("created_at", "TIMESTAMPTZ(6)"))
                                      .build();
        List<DynamicRow> rows = sharedRows(
                new String[]{"created_at", "plain_alias"},
                new Object[]{LocalDateTime.of(2026, 8, 30, 1, 2), "first"},
                new Object[]{LocalDateTime.of(2026, 8, 30, 2, 3), "second"});
        AtomicLong firstRequest = new AtomicLong();

        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            FormResultDecoder decoder = new FormResultDecoder(renderer, models);
            Flux<DynamicRow> decoded = decoder.decodeRows(
                    form,
                    Flux.fromIterable(rows).doOnRequest(requested -> firstRequest.compareAndSet(0L, requested)),
                    SqlExecutionOptions.safeDefaults());

            assertEquals(2, decoded.collectList().block().size());
            assertEquals(Long.MAX_VALUE, firstRequest.get());
        }
    }

    @Test
    void unprojectedLargeObjectDoesNotForceScalarRowsThroughSerialInnerPublishers() throws Exception {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql());
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("payload", "BLOB"))
                                      .addField(DynamicField.of("created_at", "TIMESTAMPTZ(6)"))
                                      .build();
        List<DynamicRow> rows = sharedRows(
                new String[]{"created_at"},
                new Object[]{LocalDateTime.of(2026, 8, 30, 1, 2)},
                new Object[]{LocalDateTime.of(2026, 8, 30, 2, 3)});
        List<Long> requests = new ArrayList<>();

        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            FormResultDecoder decoder = new FormResultDecoder(renderer, models);
            List<DynamicRow> decoded = decoder.decodeRows(
                    form,
                    Flux.fromIterable(rows).doOnRequest(requests::add),
                    SqlExecutionOptions.safeDefaults(), DataScope.none(), SensitiveDisplayMode.DECLARED,
                    List.of("created_at")).collectList().block();

            assertEquals(2, decoded.size());
            assertTrue(requests.contains(Long.MAX_VALUE));
        }
    }

    @Test
    void bindsMaskedColumnsOncePerSharedResultLayout() throws Exception {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql());
        DynamicForm form = DynamicForm.builder("people", "people")
                                      .addField(DynamicField.of("secret", "VARCHAR"))
                                      .addField(DynamicField.of("note", "VARCHAR"))
                                      .masked("secret", MaskedFieldDefinition.builder("full").build())
                                      .build();
        List<DynamicRow> rows = sharedRows(
                new String[]{"secret", "note"},
                new Object[]{"first-secret", "first"},
                new Object[]{"second-secret", "second"});

        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            FormResultDecoder decoder = new FormResultDecoder(renderer, models);
            List<DynamicRow> decoded = decoder.decodeRows(
                    form, rows, SqlExecutionOptions.safeDefaults(), DataScope.none(), SensitiveDisplayMode.MASKED);

            assertEquals("************", decoded.getFirst().get("secret"));
            assertEquals("*************", decoded.get(1).get("secret"));
            assertEquals(1, layoutBindings(rows.getFirst()).size());
        }
    }

    private static Map<?, ?> layoutBindings(DynamicRow row) throws Exception {
        Field layoutField = DynamicRow.class.getDeclaredField("layout");
        layoutField.setAccessible(true);
        Object layout = layoutField.get(row);
        Field bindingsField = layout.getClass().getDeclaredField("mappingBindings");
        bindingsField.setAccessible(true);
        return assertInstanceOf(Map.class, bindingsField.get(layout));
    }

    private static List<DynamicRow> sharedRows(String[] columns, Object[]... rows) throws Exception {
        Map<String, Object> firstValues = new LinkedHashMap<>();
        for (int index = 0; index < columns.length; index++) {
            firstValues.put(columns[index], rows[0][index]);
        }
        DynamicRow first = DynamicRow.copyOf(firstValues);
        Field layoutField = DynamicRow.class.getDeclaredField("layout");
        layoutField.setAccessible(true);
        Object layout = layoutField.get(first);
        Constructor<DynamicRow> constructor = DynamicRow.class.getDeclaredConstructor(
                layout.getClass(), Object[].class);
        constructor.setAccessible(true);
        List<DynamicRow> shared = new ArrayList<>(rows.length);
        shared.add(first);
        for (int index = 1; index < rows.length; index++) {
            shared.add(constructor.newInstance(layout, rows[index]));
        }
        return List.copyOf(shared);
    }
}
