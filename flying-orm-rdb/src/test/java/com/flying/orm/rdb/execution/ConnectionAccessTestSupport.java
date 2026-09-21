package com.flying.orm.rdb.execution;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import com.flying.orm.rdb.reactive.R2dbcConnectionAccess;
import io.r2dbc.spi.ConnectionFactory;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/** Test application adapters; only the caller owns physical connection creation and close. */
public final class ConnectionAccessTestSupport {
    private ConnectionAccessTestSupport() {
    }

    public static JdbcConnectionAccess jdbc(DataSource source) {
        return new JdbcConnectionAccess() {
            @Override
            public Connection getConnection(SqlRequest request) throws SQLException {
                return source.getConnection();
            }

            @Override
            public void releaseConnection(Connection connection, SqlRequest request) throws SQLException {
                connection.close();
            }
        };
    }

    public static R2dbcConnectionAccess reactive(ConnectionFactory factory) {
        return new R2dbcConnectionAccess() {
            @Override
            public Publisher<? extends io.r2dbc.spi.Connection> getConnection(SqlRequest request) {
                return Mono.defer(() -> Mono.from(factory.create()));
            }

            @Override
            public Publisher<Void> releaseConnection(SignalType signal, io.r2dbc.spi.Connection connection,
                                                     SqlRequest request) {
                return Mono.defer(() -> Mono.from(connection.close()));
            }
        };
    }

    public static JdbcConnectionAccess borrowed(Connection connection) {
        return new JdbcConnectionAccess() {
            @Override
            public Connection getConnection(SqlRequest request) {
                return connection;
            }

            @Override
            public void releaseConnection(Connection borrowed, SqlRequest request) {
            }
        };
    }

    public static R2dbcConnectionAccess borrowed(io.r2dbc.spi.Connection connection) {
        return new R2dbcConnectionAccess() {
            @Override
            public Publisher<? extends io.r2dbc.spi.Connection> getConnection(SqlRequest request) {
                return Mono.just(connection);
            }

            @Override
            public Publisher<Void> releaseConnection(SignalType signal, io.r2dbc.spi.Connection borrowed,
                                                     SqlRequest request) {
                return Mono.empty();
            }
        };
    }
}
