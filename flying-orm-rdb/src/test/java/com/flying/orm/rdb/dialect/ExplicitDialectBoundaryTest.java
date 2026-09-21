package com.flying.orm.rdb.dialect;

import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;

import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.bootstrap.FlyingOrmClientBuilder;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.lang.reflect.Proxy;
import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.*;

class ExplicitDialectBoundaryTest {

    @Test
    void builderAllowsExplicitVersionedDialectAndDoesNotProbeForMissingConfiguration() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        DataSource source = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{DataSource.class}, (proxy, method, arguments) -> {
                    calls.incrementAndGet();
                    throw new AssertionError("dialect configuration must not inspect a data source");
                });
        assertThrows(IllegalArgumentException.class, () -> FlyingOrmClients.builder(ConnectionAccessTestSupport.jdbc(source)).build());
        assertTrue(Arrays.stream(FlyingOrmClientBuilder.class.getDeclaredMethods())
                .anyMatch(method -> method.getName().equals("dialect")
                        && Arrays.equals(method.getParameterTypes(), new Class<?>[]{RdbDialect.class})),
                "versioned Oracle and custom SQL dialects must remain explicitly configurable");
        var builder = FlyingOrmClients.builder(ConnectionAccessTestSupport.jdbc(source));
        FlyingOrmClientBuilder.class.getMethod("dialect", RdbDialect.class)
                .invoke(builder, RdbDialect.oracle(OracleVersion.V12C));
        try (FlyingOrmClients clients = builder.build()) {
            assertNotNull(clients);
        }
        assertEquals(0, calls.get());
    }

    @Test
    void jdbcExecutorRequiresAnExplicitDialectWithoutDataSourceInspection() {
        for (Class<?> type : new Class<?>[]{JdbcSqlExecutor.class, SyncSqlExecutor.class}) {
            assertFalse(Arrays.stream(type.getDeclaredMethods())
                    .anyMatch(method -> (method.getName().equals("create") || method.getName().equals("jdbc"))
                            && Arrays.equals(method.getParameterTypes(), new Class<?>[]{DataSource.class})),
                    type.getName());
        }
        AtomicInteger calls = new AtomicInteger();
        DataSource source = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{DataSource.class}, (proxy, method, arguments) -> {
                    calls.incrementAndGet();
                    throw new AssertionError("explicit dialect must not inspect a data source");
                });
        assertNotNull(JdbcSqlExecutor.create(ConnectionAccessTestSupport.jdbc(source), RdbDialect.oracle()));
        assertNotNull(SyncSqlExecutor.jdbc(ConnectionAccessTestSupport.jdbc(source), RdbDialect.h2()));
        assertEquals(0, calls.get());
        assertThrows(NullPointerException.class, () -> JdbcSqlExecutor.create(ConnectionAccessTestSupport.jdbc(source), null));
    }

    @Test
    void resolverOnlyAcceptsExplicitDialectNames() {
        assertFalse(Files.exists(Path.of(
                "src/main/java/com/flying/orm/rdb/dialect/JdbcDialectResolver.java")));
        assertTrue(Arrays.stream(RdbDialectResolver.class.getDeclaredMethods())
                .filter(method -> !method.isSynthetic())
                .allMatch(method -> method.getName().equals("tryResolveName")));
        assertEquals("19c", RdbDialectResolver.tryResolveName("oracle").orElseThrow().version());
        assertEquals("postgresql", RdbDialectResolver.tryResolveName("PostgreSQL").orElseThrow().name());
        assertTrue(RdbDialectResolver.tryResolveName("not-a-dialect").isEmpty());
    }

    @Test
    void reactiveExecutorRequiresAnExplicitDialectWithoutFactoryInspection() throws Exception {
        AtomicInteger metadataReads = new AtomicInteger();
        AtomicInteger acquisitions = new AtomicInteger();
        ConnectionFactory factory = new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                acquisitions.incrementAndGet();
                return Mono.empty();
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                metadataReads.incrementAndGet();
                return () -> "MySQL";
            }
        };
        assertTrue(Arrays.stream(R2dbcSqlExecutor.class.getDeclaredMethods())
                .anyMatch(method -> method.getName().equals("create")
                        && Arrays.equals(method.getParameterTypes(),
                                         new Class<?>[]{com.flying.orm.rdb.reactive.R2dbcConnectionAccess.class, RdbDialect.class})),
                "the executor must accept the explicitly selected dialect");
        Object executor = R2dbcSqlExecutor.class.getMethod("create", com.flying.orm.rdb.reactive.R2dbcConnectionAccess.class, RdbDialect.class)
                .invoke(null, ConnectionAccessTestSupport.reactive(factory), RdbDialect.postgresql());
        assertNotNull(executor);
        assertEquals(0, metadataReads.get());
        assertEquals(0, acquisitions.get());
        assertFalse(Arrays.stream(R2dbcSqlExecutor.class.getDeclaredMethods())
                .anyMatch(method -> method.getName().equals("create")
                        && Arrays.equals(method.getParameterTypes(), new Class<?>[]{ConnectionFactory.class})));
    }
}
