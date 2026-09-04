package com.flying.orm.rdb.operator;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.template.SqlTemplate;
import com.flying.orm.rdb.template.SqlTemplateEngine;
import com.flying.orm.rdb.template.SqlTemplateParameterProvider;
import com.flying.orm.rdb.template.SqlTemplateRegistry;
import com.flying.orm.rdb.template.SyncSqlTemplateParameterProvider;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlTemplateExecutionStateSnapshotTest {

    private static final String REPEATED_SQL = "select :buffer, :bytes, :buffer, :bytes";

    @Test
    void standardCodecRenderingCopiesOwnedBinaryPayloadOnlyAtTheRequestBoundary() {
        java.lang.management.ThreadMXBean managementBean = java.lang.management.ManagementFactory.getThreadMXBean();
        assertTrue(managementBean instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocations = (com.sun.management.ThreadMXBean) managementBean;
        if (!allocations.isThreadAllocatedMemoryEnabled()) {
            allocations.setThreadAllocatedMemoryEnabled(true);
        }
        SqlTemplate template = SqlTemplate.query("payload", "select :payload, :payload", Set.of());
        SqlTemplateEngine engine = SqlTemplateEngine.create(
                SqlTemplateRegistry.builder().register(template).build(),
                RdbDialect.postgresql(), ValueCodecRegistry.standard());
        for (Object payload : List.of(new byte[64 * 1_024], ByteBuffer.allocate(64 * 1_024))) {
            SqlTemplateExecutionState state = new SqlTemplateExecutionState(engine, "payload", Set.of());
            state.bind("payload", payload);
            SqlTemplateExecutionState.Snapshot snapshot = state.snapshot();
            for (int iteration = 0; iteration < 100; iteration++) {
                state.render(snapshot, Map.of());
            }
            long threadId = Thread.currentThread().threadId();
            long before = allocations.getThreadAllocatedBytes(threadId);

            SqlRequest request = state.render(snapshot, Map.of());
            long allocated = allocations.getThreadAllocatedBytes(threadId) - before;

            assertEquals(2, request.parameters().size());
            assertTrue(allocated < 100_000L,
                    () -> "standard template rendering copied the binary payload more than once: " + allocated);
        }
    }

    @Test
    void savedTemplateValuesSurviveMutatingCodecsAcrossRenders() {
        SqlTemplateEngine reactive = SqlTemplateEngine.create(
                codecRegistry(), RdbDialect.postgresql(), mutatingCodecs());
        for (SqlTemplateEngine engine : List.of(reactive, reactive.forJdbc())) {
            SqlTemplateExecutionState state = new SqlTemplateExecutionState(engine, "payload", Set.of());
            ByteBuffer buffer = ByteBuffer.wrap(new byte[]{9, 1, 2, 3, 9}).position(1).limit(4);
            byte[] bytes = new byte[]{4, 5, 6};
            state.bindAll(Map.of("buffer", buffer, "bytes", bytes));
            SqlTemplateExecutionState.Snapshot snapshot = state.snapshot();
            buffer.put(1, (byte) 8);
            bytes[0] = 8;

            SqlRequest first = state.render(snapshot, Map.of());
            SqlRequest second = state.render(snapshot, Map.of());

            assertAll(
                    () -> assertCodecParameters(first),
                    () -> assertCodecParameters(second),
                    () -> assertEquals(0, ((ByteBuffer) snapshot.values().get("buffer")).position()),
                    () -> assertArrayEquals(new byte[]{4, 5, 6}, (byte[]) snapshot.values().get("bytes")),
                    () -> assertEquals(1, buffer.position()),
                    () -> assertEquals(8, buffer.get(1)),
                    () -> assertEquals(8, bytes[0]));
        }
    }

    @Test
    void reactiveTemplateResubscriptionPreservesCodecInputs() {
        CapturedRequests requests = new CapturedRequests();
        DatabaseOperator operator = DatabaseOperator.create(requests.reactive(),
                SqlRenderer.builder().valueCodecs(mutatingCodecs()).build(), RdbDialect.postgresql())
                .withSqlTemplates(codecRegistry(), SqlTemplateParameterProvider.none());
        Flux<DynamicRow> query = operator.sqlTemplate("payload").bindAll(codecValues()).query();

        query.collectList().block(Duration.ofSeconds(2));
        query.collectList().block(Duration.ofSeconds(2));

        requests.assertTwoExecutions();
    }

    @Test
    void reactiveNativeRepeatedSlotsPreserveCodecInputs() {
        CapturedRequests requests = new CapturedRequests();
        DatabaseOperator operator = DatabaseOperator.create(requests.reactive(),
                SqlRenderer.builder().valueCodecs(mutatingCodecs()).build(), RdbDialect.postgresql());
        NativeSqlOperator nativeSql = operator.unsafeNativeSql(REPEATED_SQL).bindAll(codecValues());

        nativeSql.query().collectList().block(Duration.ofSeconds(2));
        nativeSql.query().collectList().block(Duration.ofSeconds(2));

        requests.assertTwoExecutions();
    }

    @Test
    void syncTemplateReexecutionPreservesCodecInputs() {
        CapturedRequests requests = new CapturedRequests();
        ValueCodecRegistry codecs = mutatingCodecs();
        SqlTemplateEngine engine = SqlTemplateEngine.create(
                codecRegistry(), RdbDialect.postgresql(), codecs).forJdbc();
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            SyncSqlTemplateOperator query = new SyncSqlTemplateOperator(requests.sync(), codecs,
                    models, engine, "payload", Set.of(), SyncSqlTemplateParameterProvider.none())
                    .bindAll(codecValues());

            query.query();
            query.query();
        }

        requests.assertTwoExecutions();
    }

    @Test
    void syncNativeRepeatedSlotsPreserveCodecInputs() {
        CapturedRequests requests = new CapturedRequests();
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            SyncNativeSqlOperator query = new SyncNativeSqlOperator(requests.sync(), mutatingCodecs(),
                    models, RdbDialect.postgresql(), REPEATED_SQL).bindAll(codecValues());

            query.query();
            query.query();
        }

        requests.assertTwoExecutions();
    }

    private static SqlTemplateRegistry codecRegistry() {
        return SqlTemplateRegistry.builder()
                .register(SqlTemplate.query("payload", REPEATED_SQL, Set.of())).build();
    }

    private static Map<String, Object> codecValues() {
        return Map.of("buffer", ByteBuffer.wrap(new byte[]{1, 2, 3}), "bytes", new byte[]{4, 5, 6});
    }

    private static ValueCodecRegistry mutatingCodecs() {
        return ValueCodecRegistry.standard().withFirst(new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == byte[].class || ByteBuffer.class.isAssignableFrom(targetType);
            }

            @Override
            public Object write(Object value) {
                if (value instanceof byte[] bytes) {
                    byte[] encoded = bytes.clone();
                    Arrays.fill(bytes, (byte) 0);
                    return encoded;
                }
                ByteBuffer buffer = (ByteBuffer) value;
                byte[] encoded = new byte[buffer.remaining()];
                buffer.get(encoded);
                return encoded;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                throw new UnsupportedOperationException();
            }
        });
    }

    private static void assertCodecParameters(SqlRequest request) {
        List<Object> parameters = request.parameters();
        assertEquals(4, parameters.size());
        assertAll(
                () -> assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) parameters.get(0)),
                () -> assertArrayEquals(new byte[]{4, 5, 6}, (byte[]) parameters.get(1)),
                () -> assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) parameters.get(2)),
                () -> assertArrayEquals(new byte[]{4, 5, 6}, (byte[]) parameters.get(3)));
    }

    /** Captures only the rendered request; no database or driver behavior is simulated. */
    private static final class CapturedRequests {
        private final List<SqlRequest> requests = new ArrayList<>();

        private ReactiveSqlExecutor reactive() {
            return new ReactiveSqlExecutor() {
                @Override
                public Flux<DynamicRow> query(SqlRequest request) {
                    requests.add(request);
                    return Flux.empty();
                }

                @Override
                public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.error(new AssertionError("unexpected write"));
                }
            };
        }

        private SyncSqlExecutor sync() {
            return new SyncSqlExecutor() {
                @Override
                public List<DynamicRow> query(SqlRequest request) {
                    requests.add(request);
                    return List.of();
                }

                @Override
                public long rowsUpdated(SqlRequest request) {
                    throw new AssertionError("unexpected write");
                }

                @Override
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                    throw new AssertionError("unexpected generated keys");
                }
            };
        }

        private void assertTwoExecutions() {
            assertEquals(2, requests.size());
            assertAll(
                    () -> assertCodecParameters(requests.get(0)),
                    () -> assertCodecParameters(requests.get(1)));
        }
    }

    @Test
    void bindCapturesMutableValueInsteadOfRetainingCallerStorage() {
        SqlTemplate template = SqlTemplate.query("payload", "select :payload", Set.of());
        SqlTemplateRegistry registry = SqlTemplateRegistry.builder().register(template).build();
        SqlTemplateEngine engine = SqlTemplateEngine.create(
                registry, RdbDialect.postgresql(), ValueCodecRegistry.standard());
        SqlTemplateExecutionState state = new SqlTemplateExecutionState(engine, template.id(), Set.of());
        ByteBuffer source = ByteBuffer.wrap(new byte[]{1});

        state.bind("payload", source);
        source.put(0, (byte) 9);

        ByteBuffer published = (ByteBuffer) state.snapshot().values().get("payload");
        assertEquals(1, published.get(0));
        assertTrue(published.isReadOnly());
    }

    @Test
    void templateEngineFreezesDirectMutableParameters() {
        SqlTemplate template = SqlTemplate.query("payload", "select :payload", Set.of());
        SqlTemplateRegistry registry = SqlTemplateRegistry.builder().register(template).build();
        SqlTemplateEngine engine = SqlTemplateEngine.create(
                registry, RdbDialect.postgresql(), ValueCodecRegistry.standard());
        ByteBuffer source = ByteBuffer.wrap(new byte[]{1});

        SqlRequest request = engine.render("payload", Map.of("payload", source), Map.of());
        source.put(0, (byte) 9);

        ByteBuffer published = (ByteBuffer) request.parameters().getFirst();
        assertEquals(1, published.get(0));
        assertTrue(published.isReadOnly());
    }

    @Test
    void internalStateRenderingKeepsThePublicRequestBoundaryUnforgeable() {
        SqlTemplate template = SqlTemplate.query("payload", "select :payload", Set.of());
        SqlTemplateRegistry registry = SqlTemplateRegistry.builder().register(template).build();
        SqlTemplateEngine engine = SqlTemplateEngine.create(
                registry, RdbDialect.postgresql(), ValueCodecRegistry.standard());
        SqlTemplateExecutionState state = new SqlTemplateExecutionState(engine, template.id(), Set.of());
        state.bind("payload", ByteBuffer.wrap(new byte[]{1}));
        SqlTemplateExecutionState.Snapshot snapshot = state.snapshot();

        SqlRequest request = state.render(snapshot, Map.of());

        ByteBuffer published = (ByteBuffer) request.parameters().getFirst();
        assertNotSame(snapshot.values().get("payload"), published);
        assertEquals(1, published.get(0));
        assertTrue(published.isReadOnly());
    }

    @Test
    void providerValuesAreFrozenExactlyAtTheProviderBoundary() {
        SqlTemplate template = SqlTemplate.query("payload", "select :tenant, :payload", Set.of());
        SqlTemplateRegistry registry = SqlTemplateRegistry.builder().register(template).build();
        SqlTemplateEngine engine = SqlTemplateEngine.create(
                registry, RdbDialect.postgresql(), ValueCodecRegistry.standard());
        SqlTemplateExecutionState state = new SqlTemplateExecutionState(engine, template.id(), Set.of("tenant"));
        state.bind("payload", 7);
        ByteBuffer source = ByteBuffer.wrap(new byte[]{1});

        SqlRequest request = state.render(state.snapshot(), Map.of("tenant", source));
        source.put(0, (byte) 9);

        ByteBuffer published = (ByteBuffer) request.parameters().getFirst();
        assertNotSame(source, published);
        assertEquals(1, published.get(0));
        assertTrue(published.isReadOnly());
    }

    @Test
    void bindAllPublishesNothingWhenValueFreezingFails() {
        SqlTemplate template = SqlTemplate.query("payload", "select :first", Set.of());
        SqlTemplateRegistry registry = SqlTemplateRegistry.builder().register(template).build();
        SqlTemplateEngine engine = SqlTemplateEngine.create(
                registry, RdbDialect.postgresql(), ValueCodecRegistry.standard());
        SqlTemplateExecutionState state = new SqlTemplateExecutionState(engine, template.id(), Set.of());
        Object[] cycle = new Object[1];
        cycle[0] = cycle;
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("first", "would-have-been-published");
        values.put("second", cycle);

        assertThrows(IllegalArgumentException.class, () -> state.bindAll(values));

        assertTrue(state.snapshot().values().isEmpty());
    }
}
