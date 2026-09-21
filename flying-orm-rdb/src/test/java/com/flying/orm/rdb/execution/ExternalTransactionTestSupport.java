package com.flying.orm.rdb.execution;

import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import com.flying.orm.rdb.reactive.R2dbcConnectionAccess;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.time.Duration;
import java.util.function.Function;

/** Test caller owns real in-process transactions; ORM sees a connection that rejects ownership actions. */
public final class ExternalTransactionTestSupport {
    private static final Duration WAIT = Duration.ofSeconds(10);

    private ExternalTransactionTestSupport() {
    }

    public static <T> T jdbc(DataSource source, Function<JdbcConnectionAccess, T> operation) throws Exception {
        try (Connection connection = source.getConnection()) {
            connection.setAutoCommit(false);
            try {
                T result = operation.apply(ConnectionAccessTestSupport.borrowed(
                        borrowed(Connection.class, connection)));
                connection.commit();
                return result;
            } catch (RuntimeException | Error failure) {
                connection.rollback();
                throw failure;
            }
        }
    }

    public static <T> T reactive(ConnectionFactory factory,
                                 Function<R2dbcConnectionAccess, Mono<T>> operation) {
        io.r2dbc.spi.Connection connection = Mono.from(factory.create()).block(WAIT);
        try {
            Mono.from(connection.beginTransaction()).block(WAIT);
            try {
                T result = operation.apply(ConnectionAccessTestSupport.borrowed(
                        borrowed(io.r2dbc.spi.Connection.class, connection))).block(WAIT);
                Mono.from(connection.commitTransaction()).block(WAIT);
                return result;
            } catch (RuntimeException | Error failure) {
                Mono.from(connection.rollbackTransaction()).block(WAIT);
                throw failure;
            }
        } finally {
            Mono.from(connection.close()).block(WAIT);
        }
    }

    private static <T> T borrowed(Class<T> type, T connection) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (proxy, method, arguments) -> {
                    switch (method.getName()) {
                        case "beginTransaction", "commitTransaction", "rollbackTransaction", "isAutoCommit",
                             "getAutoCommit", "setAutoCommit", "commit", "rollback", "close",
                             "getTransactionIsolation", "getTransactionIsolationLevel", "setTransactionIsolation",
                             "setTransactionIsolationLevel" ->
                                throw new AssertionError("ORM must not own external transaction: " + method.getName());
                        default -> {
                            try {
                                return method.invoke(connection, arguments);
                            } catch (InvocationTargetException failure) {
                                throw failure.getCause();
                            }
                        }
                    }
                }));
    }
}
