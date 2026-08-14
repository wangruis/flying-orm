package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataCache;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionLogOptions;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证所有 Java 生态共用的启动入口只负责装配，不偷偷创建连接或改变事务所有权。 */
class FlyingOrmBootstrapTest {

    @Test
    void createsReactiveClientGraphWithoutInventingJdbcRuntime() {
        AtomicInteger connectionRequests = new AtomicInteger();
        FlyingOrmEnvironment environment = FlyingOrmEnvironment.of(
                connectionFactory("MySQL", connectionRequests));

        FlyingOrmClients clients = FlyingOrmBootstrap.create(FlyingOrmConfiguration.defaults(), environment);

        assertNotNull(clients.executor());
        assertNotNull(clients.forms());
        assertTrue(clients.reactiveAvailable());
        assertFalse(clients.jdbcAvailable());
        assertThrows(IllegalStateException.class, clients::syncForms);
        assertNotNull(clients.operator());
        assertNotNull(clients.schema());
        assertNotNull(clients.metadata());
        assertEquals(0, connectionRequests.get());
    }

    @Test
    void validatesEveryPhysicalDataSourceBeforeBuildingClients() {
        AtomicInteger connectionRequests = new AtomicInteger();
        FlyingOrmEnvironment environment = FlyingOrmEnvironment.of(
                        connectionFactory("routing-proxy", connectionRequests))
                .withPhysicalReactiveDataSources(Map.of(
                        "primary", connectionFactory("MySQL", connectionRequests),
                        "replica", connectionFactory("PostgreSQL", connectionRequests)));

        assertThrows(IllegalArgumentException.class,
                     () -> FlyingOrmBootstrap.create(FlyingOrmConfiguration.defaults(), environment));
        assertEquals(0, connectionRequests.get());
    }

    @Test
    void keepsJdbcAndReactiveKernelsExplicitlySeparate() {
        DataSource jdbc = dataSource();
        ConnectionFactory reactive = connectionFactory("H2", new AtomicInteger());

        FlyingOrmEnvironment jdbcOnly = FlyingOrmEnvironment.of(jdbc);
        assertTrue(jdbcOnly.jdbcDataSource().isPresent());
        assertTrue(jdbcOnly.connectionFactory().isEmpty());
        assertTrue(jdbcOnly.physicalReactiveDataSources().isEmpty());

        FlyingOrmEnvironment reactiveOnly = FlyingOrmEnvironment.of(reactive);
        assertTrue(reactiveOnly.jdbcDataSource().isEmpty());
        assertSame(reactive, reactiveOnly.connectionFactory().orElseThrow());

        FlyingOrmEnvironment both = FlyingOrmEnvironment.of(jdbc, reactive);
        assertSame(jdbc, both.jdbcDataSource().orElseThrow());
        assertSame(reactive, both.connectionFactory().orElseThrow());
    }

    @Test
    void exposesExternalTransactionThroughTheUnifiedClientGraph() {
        Connection transactionConnection = connection();
        R2dbcTransactionContext transaction = R2dbcTransactionContext.external(
                transactionConnection, "primary");
        FlyingOrmEnvironment environment = FlyingOrmEnvironment.of(connectionFactory("H2", new AtomicInteger()))
                                                               .withTransactionParticipant(
                                                                       () -> Mono.just(transaction));

        FlyingOrmClients clients = FlyingOrmBootstrap.create(FlyingOrmConfiguration.defaults(), environment);

        StepVerifier.create(clients.executor().currentTransaction())
                    .assertNext(actual -> assertSame(transaction, actual))
                    .verifyComplete();
    }

    @Test
    void combinesMetricsAndSqlLogObserversWithoutChangingFailure() {
        AtomicInteger connectionRequests = new AtomicInteger();
        List<SqlExecutionObservation> observations = new ArrayList<>();
        List<String> logs = new ArrayList<>();
        FlyingOrmConfiguration configuration = FlyingOrmConfiguration.defaults().withSqlLog(
                FlyingOrmConfiguration.SqlLog.enabled(SqlExecutionLogOptions.defaults()));
        FlyingOrmEnvironment environment = FlyingOrmEnvironment.of(
                        connectionFactory("MySQL", connectionRequests))
                .withObservers(observations::add, ignored -> { })
                .withSqlLogSink(logs::add);
        FlyingOrmClients clients = FlyingOrmBootstrap.create(configuration, environment);

        StepVerifier.create(clients.executor().rowsUpdated(SqlRequest.nativeSql("update users set name = ?",
                                                                                List.of("name"))))
                    .expectErrorMatches(error -> "database unavailable".equals(error.getMessage()))
                    .verify();

        assertEquals(1, connectionRequests.get());
        assertEquals(1, observations.size());
        assertFalse(logs.isEmpty());
    }

    @Test
    void requiresLogSinkOnlyWhenSqlLoggingIsEnabled() {
        FlyingOrmConfiguration configuration = FlyingOrmConfiguration.defaults().withSqlLog(
                FlyingOrmConfiguration.SqlLog.enabled(SqlExecutionLogOptions.defaults()));
        FlyingOrmEnvironment environment = FlyingOrmEnvironment.of(
                connectionFactory("MySQL", new AtomicInteger()));

        IllegalStateException failure = assertThrows(
                IllegalStateException.class,
                () -> FlyingOrmBootstrap.create(configuration, environment));

        assertEquals("SQL logging is enabled but no SQL log sink was provided", failure.getMessage());
    }

    @Test
    void rejectsAnEnvironmentWithoutAnyExecutionKernel() {
        FlyingOrmEnvironment empty = new FlyingOrmEnvironment(
                java.util.Optional.empty(), java.util.Optional.empty(), Map.of(), Map.of(),
                java.util.Optional.empty(), java.util.Optional.empty(), java.util.Optional.empty(),
                java.util.Optional.empty(), java.util.Optional.empty(), java.util.Optional.empty());

        assertThrows(IllegalStateException.class,
                     () -> FlyingOrmBootstrap.create(FlyingOrmConfiguration.defaults(), empty));
    }

    @Test
    void isolatesBothBatchObserversWhenOneFails() {
        AtomicInteger calls = new AtomicInteger();
        BatchExecutionObserver observer = BatchExecutionObserver.composite(
                ignored -> {
                    throw new IllegalStateException("metrics unavailable");
                },
                ignored -> calls.incrementAndGet());

        observer.onExecution((BatchExecutionObservation) null);

        assertEquals(1, calls.get());
    }

    /**
     * 双内核各自持有元数据缓存时，一个失效器失败不能让另一侧保留过期结构。
     */
    @Test
    void combinesMetadataInvalidatorsWithPerDelegateFailureIsolation() {
        AtomicInteger calls = new AtomicInteger();
        MetadataCacheInvalidator failing = new MetadataCacheInvalidator() {
            @Override
            public void invalidate(String table) {
                throw new IllegalStateException("cache unavailable");
            }

            @Override
            public void invalidateAll() {
                throw new IllegalStateException("cache unavailable");
            }
        };
        MetadataCacheInvalidator recording = new MetadataCacheInvalidator() {
            @Override
            public void invalidate(String table) {
                calls.incrementAndGet();
            }

            @Override
            public void invalidate(String schema, String table) {
                calls.incrementAndGet();
            }

            @Override
            public void invalidateAll() {
                calls.incrementAndGet();
            }
        };

        MetadataCacheInvalidator combined = FlyingOrmClientAssembler.combine(failing, recording);
        combined.invalidate("Users");
        combined.invalidate("app", "Orders");
        combined.invalidateAll();

        assertEquals(3, calls.get());
    }

    /**
     * 缓存失效仍要尽力执行全部协作者，但异常图中的虚拟机致命错误必须保持原对象出站。
     */
    @Test
    void propagatesNestedVirtualMachineErrorAfterInvalidatingRemainingMetadataCaches() {
        OutOfMemoryError fatal = new OutOfMemoryError("fatal cache invalidation");
        AtomicInteger calls = new AtomicInteger();
        MetadataCacheInvalidator failing = new MetadataCacheInvalidator() {
            @Override
            public void invalidate(String table) {
                throw new IllegalStateException("cache wrapper", fatal);
            }

            @Override
            public void invalidateAll() {
                throw new IllegalStateException("cache wrapper", fatal);
            }
        };
        MetadataCacheInvalidator recording = new MetadataCacheInvalidator() {
            @Override
            public void invalidate(String table) {
                calls.incrementAndGet();
            }

            @Override
            public void invalidateAll() {
                calls.incrementAndGet();
            }
        };
        MetadataCacheInvalidator combined = FlyingOrmClientAssembler.combine(failing, recording);

        OutOfMemoryError actual = assertThrows(OutOfMemoryError.class,
                                               () -> combined.invalidate("Users"));

        assertSame(fatal, actual);
        assertEquals(1, calls.get());
    }

    /**
     * 内置方言 reader 已由 factory 包装一次；bootstrap 不得再添加重复的缓存包装。
     * 空事务的 metadata miss 仍会由实际 SQL 执行器解析一次，因此总计必须为两次而不是三次。
     */
    @Test
    void avoidsDuplicateCacheTransactionResolutionForBootstrapMetadataCache() {
        AtomicInteger participantCalls = new AtomicInteger();
        FlyingOrmEnvironment environment = FlyingOrmEnvironment.of(connectionFactory("H2", new AtomicInteger()))
                                                               .withTransactionParticipant(() -> Mono.defer(() -> {
                                                                   participantCalls.incrementAndGet();
                                                                   return Mono.empty();
                                                               }));
        FlyingOrmClients clients = FlyingOrmBootstrap.create(FlyingOrmConfiguration.defaults(), environment);

        StepVerifier.create(clients.metadata().readForm("users", "Users"))
                    .expectErrorMatches(error -> "database unavailable".equals(error.getMessage()))
                    .verify();

        assertEquals(2, participantCalls.get());
    }

    /** metadata 缓存命中必须保留空结果，不能把它误判为未缓存并重新读取 delegate。 */
    @Test
    void doesNotRepeatEmptyMetadataRead() {
        AtomicInteger metadataReads = new AtomicInteger();
        ReactiveFormMetadataReader delegate = new ReactiveFormMetadataReader() {
            @Override
            public Mono<DynamicForm> readForm(String formId, String table) {
                return Mono.defer(() -> {
                    metadataReads.incrementAndGet();
                    return Mono.empty();
                });
            }

            @Override
            public Mono<DynamicForm> readForm(String formId, String schema, String table) {
                return readForm(formId, schema + "." + table);
            }
        };
        ReactiveFormMetadataCache cache = ReactiveFormMetadataReaders.cached(delegate);

        StepVerifier.create(cache.readForm("users", "Users"))
                    .verifyComplete();
        StepVerifier.create(cache.readForm("users", "Users"))
                    .verifyComplete();

        assertEquals(1, metadataReads.get());
    }

    private static ConnectionFactory connectionFactory(String metadataName, AtomicInteger requests) {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                requests.incrementAndGet();
                return Mono.error(new IllegalStateException("database unavailable"));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> metadataName;
            }
        };
    }

    private static DataSource dataSource() {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                                                   new Class<?>[]{DataSource.class},
                                                   (proxy, method, arguments) -> null);
    }

    @SuppressWarnings("unchecked")
    private static Connection connection() {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                                                    new Class<?>[]{Connection.class},
                                                    (proxy, method, arguments) -> defaultValue(method));
    }

    private static Object defaultValue(Method method) {
        if (Publisher.class.isAssignableFrom(method.getReturnType())) {
            return Mono.empty();
        }
        if (method.getReturnType() == boolean.class) {
            return false;
        }
        if (method.getReturnType() == int.class) {
            return 0;
        }
        if (method.getReturnType() == long.class) {
            return 0L;
        }
        return null;
    }
}
