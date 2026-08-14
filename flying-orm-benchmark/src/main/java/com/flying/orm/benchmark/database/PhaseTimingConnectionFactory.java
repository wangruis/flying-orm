package com.flying.orm.benchmark.database;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Objects;

/**
 * benchmark 专用的连接阶段探针。
 *
 * <p>它包在连接池外面，所以 {@link #create()} 测到的是“向池申请到可用连接”的耗时，代理连接的
 * {@code close()} 测到的是“把连接完整归还给池”的耗时。没有阶段诊断开关时 runner 根本不会创建这个包装器。</p>
 */
final class PhaseTimingConnectionFactory implements ConnectionFactory {

    private final ConnectionFactory delegate;

    PhaseTimingConnectionFactory(ConnectionFactory delegate) {
        this.delegate = Objects.requireNonNull(delegate, "timed connection factory delegate must not be null");
    }

    @Override
    public Publisher<? extends Connection> create() {
        return Mono.deferContextual(context -> {
            DatabaseOperationPhaseRecorder.Sample sample = DatabaseOperationPhaseRecorder.currentSample(context);
            if (sample == null) {
                return Mono.from(delegate.create());
            }
            long startedAt = System.nanoTime();
            return Mono.from(delegate.create())
                       .map(connection -> {
                           sample.acquired(System.nanoTime() - startedAt);
                           return timedConnection(connection, sample);
                       });
        });
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return delegate.getMetadata();
    }

    private static Connection timedConnection(Connection connection,
                                              DatabaseOperationPhaseRecorder.Sample sample) {
        Objects.requireNonNull(connection, "acquired connection must not be null");
        return (Connection) Proxy.newProxyInstance(
                connection.getClass().getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> {
                    try {
                        Object result = method.invoke(connection, arguments);
                        if (!"close".equals(method.getName()) || !(result instanceof Publisher<?> closePublisher)) {
                            return result;
                        }
                        return Mono.defer(() -> {
                            long startedAt = System.nanoTime();
                            return Mono.from(closePublisher)
                                       // 先写 release 再向 usingWhen 发送终止信号，外层记录 total 时才能看到完整阶段。
                                       .doOnSuccess(ignored -> sample.released(System.nanoTime() - startedAt))
                                       .doOnError(ignored -> sample.released(System.nanoTime() - startedAt))
                                       .doOnCancel(() -> sample.released(System.nanoTime() - startedAt));
                        });
                    } catch (InvocationTargetException error) {
                        throw error.getCause();
                    }
                });
    }
}
