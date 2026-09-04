package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.rdb.transaction.R2dbcTransactionCompletion;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import com.flying.orm.rdb.transaction.TransactionOutcome;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RepositoryTransactionResolutionTest {

    @Test
    void reactivePostPersistUsesTheResolvedTransactionExactlyOnce() {
        AtomicInteger transactionLookups = new AtomicInteger();
        AtomicInteger externalExecutions = new AtomicInteger();
        AtomicInteger factoryAcquisitions = new AtomicInteger();
        AtomicInteger postPersistCalls = new AtomicInteger();
        TestTransactionCompletion completion = new TestTransactionCompletion();
        Connection externalConnection = successfulConnection(externalExecutions);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(
                        unusedConnectionFactory(factoryAcquisitions))
                .withTransactionParticipant(() -> {
                    if (transactionLookups.incrementAndGet() != 1) {
                        return Mono.error(new AssertionError("transaction must be resolved exactly once"));
                    }
                    return Mono.just(R2dbcTransactionContext.external(externalConnection, completion));
                });
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        ReactiveFormRepository<Person> repository = ReactiveFormRepository.create(
                        client, client.entityModels().metadata(Person.class).toDynamicForm(), Person.class)
                .withListener(event -> {
                    if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                        postPersistCalls.incrementAndGet();
                    }
                    return Mono.empty();
                });

        Long affectedRows = repository.insert(new Person(7L, "Ada"))
                .block(Duration.ofSeconds(2));

        assertEquals(1L, affectedRows);
        assertEquals(1, transactionLookups.get());
        assertEquals(1, externalExecutions.get());
        assertEquals(0, factoryAcquisitions.get());
        assertEquals(0, postPersistCalls.get());

        completion.complete(TransactionOutcome.COMMITTED);

        assertEquals(1, postPersistCalls.get());
    }

    @Test
    void reactiveGeneratedKeyFailureUsesExecutionTransactionSource() {
        AtomicInteger transactionLookups = new AtomicInteger();
        AtomicInteger externalExecutions = new AtomicInteger();
        AtomicInteger factoryAcquisitions = new AtomicInteger();
        Connection externalConnection = generatedKeyReadFailureConnection(externalExecutions);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(
                        unusedConnectionFactory(factoryAcquisitions))
                .withTransactionParticipant(() -> {
                    if (transactionLookups.incrementAndGet() != 1) {
                        return Mono.error(new AssertionError("transaction must be resolved exactly once"));
                    }
                    return Mono.just(R2dbcTransactionContext.external(externalConnection));
                });
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        ReactiveFormRepository<GeneratedPerson> repository = ReactiveFormRepository.create(
                client, client.entityModels().metadata(GeneratedPerson.class).toDynamicForm(), GeneratedPerson.class);

        GeneratedKeyResolutionException failure = assertThrows(
                GeneratedKeyResolutionException.class,
                () -> repository.insert(new GeneratedPerson("Ada")).block(Duration.ofSeconds(2)));

        assertEquals(GeneratedKeyResolutionException.WriteState.ENLISTED, failure.state());
        assertEquals(1L, failure.affectedRows());
        assertEquals(1, transactionLookups.get());
        assertEquals(1, externalExecutions.get());
        assertEquals(0, factoryAcquisitions.get());
    }

    @Test
    void reactivePostPersistKeepsResolvedAbsenceForTheWholeSubscription() {
        AtomicInteger transactionLookups = new AtomicInteger();
        AtomicInteger ownedExecutions = new AtomicInteger();
        AtomicInteger externalExecutions = new AtomicInteger();
        AtomicInteger factoryAcquisitions = new AtomicInteger();
        AtomicInteger postPersistCalls = new AtomicInteger();
        Connection externalConnection = successfulConnection(externalExecutions);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory(
                        factoryAcquisitions, successfulConnection(ownedExecutions)))
                .withTransactionParticipant(() -> transactionLookups.incrementAndGet() == 1
                        ? Mono.empty()
                        : Mono.just(R2dbcTransactionContext.external(externalConnection)));
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        ReactiveFormRepository<Person> repository = ReactiveFormRepository.create(
                        client, client.entityModels().metadata(Person.class).toDynamicForm(), Person.class)
                .withListener(event -> {
                    if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                        postPersistCalls.incrementAndGet();
                    }
                    return Mono.empty();
                });

        Long affectedRows = repository.insert(new Person(8L, "Grace"))
                .block(Duration.ofSeconds(2));

        assertEquals(1L, affectedRows);
        assertEquals(1, transactionLookups.get());
        assertEquals(1, factoryAcquisitions.get());
        assertEquals(1, ownedExecutions.get());
        assertEquals(0, externalExecutions.get());
        assertEquals(1, postPersistCalls.get());
    }

    @Test
    void reactiveGeneratedKeyFailureKeepsResolvedAbsenceForTheWholeSubscription() {
        AtomicInteger transactionLookups = new AtomicInteger();
        AtomicInteger ownedExecutions = new AtomicInteger();
        AtomicInteger externalExecutions = new AtomicInteger();
        AtomicInteger factoryAcquisitions = new AtomicInteger();
        Connection externalConnection = generatedKeyReadFailureConnection(externalExecutions);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory(
                        factoryAcquisitions, generatedKeyReadFailureConnection(ownedExecutions)))
                .withTransactionParticipant(() -> transactionLookups.incrementAndGet() == 1
                        ? Mono.empty()
                        : Mono.just(R2dbcTransactionContext.external(externalConnection)));
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        ReactiveFormRepository<GeneratedPerson> repository = ReactiveFormRepository.create(
                client, client.entityModels().metadata(GeneratedPerson.class).toDynamicForm(), GeneratedPerson.class);

        GeneratedKeyResolutionException failure = assertThrows(
                GeneratedKeyResolutionException.class,
                () -> repository.insert(new GeneratedPerson("Grace")).block(Duration.ofSeconds(2)));

        assertEquals(GeneratedKeyResolutionException.WriteState.UNKNOWN, failure.state());
        assertEquals(1L, failure.affectedRows());
        assertEquals(1, transactionLookups.get());
        assertEquals(1, factoryAcquisitions.get());
        assertEquals(1, ownedExecutions.get());
        assertEquals(0, externalExecutions.get());
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
    }

    private static ConnectionFactory unusedConnectionFactory(AtomicInteger acquisitions) {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                acquisitions.incrementAndGet();
                return Mono.error(new AssertionError("connection factory must not be used"));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "H2";
            }
        };
    }

    private static ConnectionFactory connectionFactory(AtomicInteger acquisitions, Connection connection) {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                acquisitions.incrementAndGet();
                return Mono.just(connection);
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "H2";
            }
        };
    }

    private static Connection successfulConnection(AtomicInteger executions) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "createStatement" -> successfulStatement(executions);
                    case "toString" -> "repository-external-transaction-connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Statement successfulStatement(AtomicInteger executions) {
        return (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "bind", "bindNull" -> proxy;
                    case "execute" -> {
                        executions.incrementAndGet();
                        yield Flux.just(rowsUpdated(1L));
                    }
                    case "toString" -> "repository-external-transaction-statement";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Connection generatedKeyReadFailureConnection(AtomicInteger executions) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "createStatement" -> generatedKeyReadFailureStatement(executions);
                    case "toString" -> "repository-generated-key-transaction-connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Statement generatedKeyReadFailureStatement(AtomicInteger executions) {
        return (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "bind", "bindNull", "returnGeneratedValues" -> proxy;
                    case "execute" -> {
                        executions.incrementAndGet();
                        yield Flux.just(generatedKeyReadFailureResult());
                    }
                    case "toString" -> "repository-generated-key-failure-statement";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    @SuppressWarnings("unchecked")
    private static Result generatedKeyReadFailureResult() {
        Result.UpdateCount updateCount = (Result.UpdateCount) Proxy.newProxyInstance(
                Result.UpdateCount.class.getClassLoader(),
                new Class<?>[]{Result.UpdateCount.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "value" -> 1L;
                    case "toString" -> "repository-generated-key-update-count";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        Result.Message message = (Result.Message) Proxy.newProxyInstance(
                Result.Message.class.getClassLoader(),
                new Class<?>[]{Result.Message.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "exception" -> new IllegalStateException("generated key row failed");
                    case "toString" -> "repository-generated-key-message";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (Result) Proxy.newProxyInstance(
                Result.class.getClassLoader(),
                new Class<?>[]{Result.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "flatMap" -> {
                        Function<Result.Segment, Publisher<?>> mapper =
                                (Function<Result.Segment, Publisher<?>>) arguments[0];
                        yield Flux.concat(mapper.apply(updateCount), mapper.apply(message));
                    }
                    case "toString" -> "repository-generated-key-failure-result";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Result rowsUpdated(long rows) {
        return (Result) Proxy.newProxyInstance(
                Result.class.getClassLoader(),
                new Class<?>[]{Result.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getRowsUpdated" -> Mono.just(rows);
                    case "toString" -> "repository-external-transaction-result";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static final class TestTransactionCompletion implements R2dbcTransactionCompletion {

        private final AtomicReference<Listener> listener = new AtomicReference<>();

        @Override
        public boolean register(Listener listener) {
            return this.listener.compareAndSet(null, listener);
        }

        private void complete(TransactionOutcome outcome) {
            Listener registered = listener.getAndSet(null);
            if (registered == null) {
                throw new AssertionError("transaction completion listener was not registered");
            }
            Mono.from(registered.afterCompletion(outcome)).block(Duration.ofSeconds(2));
        }
    }

    @TableName("people")
    private static final class Person {

        @TableId(type = IdType.INPUT)
        private final Long id;

        private final String name;

        private Person(Long id, String name) {
            this.id = id;
            this.name = name;
        }

        public Long getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }

    @TableName("generated_people")
    private static final class GeneratedPerson {

        @TableId(type = IdType.AUTO)
        private Long id;

        private final String name;

        private GeneratedPerson(String name) {
            this.name = name;
        }

        public Long getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }
}
