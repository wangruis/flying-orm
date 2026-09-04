package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.codec.SqlTypedValue;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionSequence;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class R2dbcSequenceMixedParameterOwnershipTest {

    @Test
    void freezesAllPhasesBeforeSubscriptionWithoutRecopyingOwnedValues() {
        assertAll(() -> verifyWrapper(false), () -> verifyWrapper(true));
    }

    private static void verifyWrapper(boolean spiParameter) {
        byte[] ordinary = new byte[4096];
        byte[] wrapped = {7, 8};
        Object wrapper = spiParameter ? Parameters.in(R2dbcType.BLOB, wrapped)
                : new SqlTypedValue(SqlTypedValue.Kind.BLOB, wrapped);
        List<SqlRequest> requests = List.of("setup", "work", "cleanup").stream()
                .map(phase -> new SqlRequest("update sample set a=?, b=?, c=? /* " + phase + " */",
                        List.of(ordinary, new Date(1000), wrapper))).toList();
        List<List<Object>> bindings = new ArrayList<>();
        List<List<Object>> observations = new ArrayList<>();
        SqlExecutionObserver observer = new SqlExecutionObserver() {
            @Override public boolean requiresParameterValues() { return true; }
            @Override public void onExecution(SqlExecutionObservation observation) { }
            @Override public void onExecution(SqlExecutionObservation observation, List<Object> values) {
                observations.add(values);
            }
        };
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory(bindings)).withObserver(observer);
        Mono<?> execution = executor.executeInConnection(new SqlExecutionSequence(
                List.of(requests.get(0)), List.of(requests.get(1)), List.of(requests.get(2))),
                SqlExecutionOptions.safeDefaults());
        ordinary[0] = 99;
        wrapped[0] = 99;
        execution.block(Duration.ofSeconds(2));

        assertEquals(3, bindings.size());
        assertEquals(3, observations.size());
        for (int index = 0; index < 3; index++) {
            List<Object> bound = bindings.get(index);
            List<Object> observed = observations.get(index);
            assertArrayEquals(new byte[]{7, 8}, (byte[]) bound.get(2));
            assertEquals(0, ((byte[]) bound.get(0))[0]);
            assertSame(requests.get(index).parameters().get(0), bound.get(0), "reuse ordinary array in each phase");
            assertSame(requests.get(index).parameters().get(1), bound.get(1), "reuse ordinary Date in each phase");
            assertSame(bound.get(0), observed.get(0));
            assertSame(bound.get(1), observed.get(1));
            assertArrayEquals(new byte[]{7, 8}, bytes((ByteBuffer) ((Parameter) observed.get(2)).getValue()));
        }
    }

    private static ConnectionFactory factory(List<List<Object>> bindings) {
        Connection connection = proxy(Connection.class, (p, m, a) -> switch (m.getName()) {
            case "createStatement" -> statement(bindings);
            case "close" -> Mono.empty();
            default -> throw new UnsupportedOperationException(m.getName());
        });
        return new ConnectionFactory() {
            @Override public Publisher<? extends Connection> create() { return Mono.just(connection); }
            @Override public ConnectionFactoryMetadata getMetadata() { return () -> "H2"; }
        };
    }

    private static Statement statement(List<List<Object>> bindings) {
        List<Object> values = new ArrayList<>();
        bindings.add(values);
        Result result = proxy(Result.class, (p, m, a) -> {
            if ("getRowsUpdated".equals(m.getName())) {
                return Flux.from(((Blob) values.get(2)).stream()).single().map(content -> {
                    values.set(2, bytes(content));
                    return 1L;
                });
            }
            throw new UnsupportedOperationException(m.getName());
        });
        return proxy(Statement.class, (p, m, a) -> switch (m.getName()) {
            case "bind" -> { values.add(a[1]); yield p; }
            case "execute" -> Flux.just(result);
            default -> throw new UnsupportedOperationException(m.getName());
        });
    }

    private static byte[] bytes(ByteBuffer content) {
        ByteBuffer view = content.duplicate();
        byte[] bytes = new byte[view.remaining()];
        view.get(bytes);
        return bytes;
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
