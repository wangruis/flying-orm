package com.flying.orm.benchmark.database;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 阶段计时只测内存 Publisher，避免单元测试被真实连接和机器负载干扰。 */
class DatabaseOperationPhaseRecorderTest {

    @Test
    void keepsAcquireExecuteAndReleaseInTheSameReactiveSubscription() {
        DatabaseOperationPhaseRecorder recorder = new DatabaseOperationPhaseRecorder();
        ConnectionFactory timedFactory = new PhaseTimingConnectionFactory(delayedConnectionFactory());

        recorder.track(Mono.usingWhen(
                        Mono.from(timedFactory.create()),
                        ignored -> Mono.delay(Duration.ofMillis(4)).then(),
                        Connection::close))
                .block(Duration.ofSeconds(2));

        DatabasePerformanceReport.PhaseLatency snapshot = recorder.snapshot();
        assertEquals(1, snapshot.acquire().samples());
        assertEquals(1, snapshot.executeAndCommit().samples());
        assertEquals(1, snapshot.release().samples());
        assertEquals(1, snapshot.total().samples());
        assertTrue(snapshot.acquire().maxMillis() > 0);
        assertTrue(snapshot.executeAndCommit().maxMillis() > 0);
        assertTrue(snapshot.release().maxMillis() > 0);
        assertTrue(snapshot.total().maxMillis()
                           >= snapshot.acquire().maxMillis() + snapshot.release().maxMillis());
    }

    private static ConnectionFactory delayedConnectionFactory() {
        Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> {
                    if ("close".equals(method.getName())) {
                        return Mono.delay(Duration.ofMillis(2)).then();
                    }
                    if ("toString".equals(method.getName())) {
                        return "phase-test-connection";
                    }
                    throw new UnsupportedOperationException("test connection does not implement " + method.getName());
                });
        return new ConnectionFactory() {
            @Override
            public Mono<? extends Connection> create() {
                return Mono.delay(Duration.ofMillis(3)).thenReturn(connection);
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "phase-test";
            }
        };
    }
}
