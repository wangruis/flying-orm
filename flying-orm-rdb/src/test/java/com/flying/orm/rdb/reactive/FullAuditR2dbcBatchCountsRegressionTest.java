package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.dialect.RdbDialect;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FullAuditR2dbcBatchCountsRegressionTest {

    private static final Duration TEST_WAIT = Duration.ofSeconds(5);
    private static final String BUSINESS_SQL =
            "insert into business_row(value_col, id) values (?, ?)";
    private static final String TOKEN_SQL =
            "insert into token_index(id, field_tag, token) values (?, ?, ?)";

    @Test
    void sideIndexFailureAfterCompleteBusinessFlowRetainsKnownAffectedRows() {
        IllegalStateException sideFailure = new IllegalStateException("side-index insert failed");
        Fixture fixture = new Fixture(Flux.just(rowsUpdated(Flux.just(1L, 1L))), sideFailure);

        BatchExecutionEvidenceException failure = fixture.writeFailure();

        assertCausedBy(failure, sideFailure);
        BatchExecutionEvidence evidence = failure.evidence();
        BatchExecutionEvidence chunk = onlyFailedChunk(evidence);
        assertTrue(evidence.affectedRows().isKnown());
        assertEquals(2L, evidence.affectedRows().value());
        assertTrue(chunk.affectedRows().isKnown());
        assertEquals(2L, chunk.affectedRows().value());
        assertEquals(List.of(0L, 1L), chunk.successfulOffsets().boxed().toList());
        assertTrue(fixture.businessResultsCompleted.get());
        assertEquals(1, fixture.businessExecutions.get());
        assertEquals(1, fixture.businessAdds.get(), "the main write must use the driver batch path");
        assertEquals(1, fixture.sideExecutions.get());
    }

    @Test
    void failedCountStreamDoesNotPromoteItsEmittedPrefixToKnownBatchCounts() {
        IllegalStateException businessFailure = new IllegalStateException("count stream failed");
        Flux<Long> counts = Flux.concat(Flux.just(1L), Flux.error(businessFailure));

        assertUnknownBusinessFailure(Flux.just(rowsUpdated(counts)), businessFailure);
    }

    @Test
    void failedResultStreamRemainsUnknownEvenWhenAnEarlierResultCompletedItsCounts() {
        IllegalStateException businessFailure = new IllegalStateException("result stream failed");
        Flux<Result> results = Flux.concat(
                Flux.just(rowsUpdated(Flux.just(1L, 1L))),
                Flux.error(businessFailure));

        assertUnknownBusinessFailure(results, businessFailure);
    }

    private static void assertUnknownBusinessFailure(Flux<Result> results, RuntimeException expected) {
        Fixture fixture = new Fixture(results, new IllegalStateException("side index must not execute"));

        BatchExecutionEvidenceException failure = fixture.writeFailure();

        assertCausedBy(failure, expected);
        BatchExecutionEvidence evidence = failure.evidence();
        BatchExecutionEvidence chunk = onlyFailedChunk(evidence);
        assertFalse(evidence.affectedRows().isKnown());
        assertFalse(chunk.affectedRows().isKnown());
        assertEquals(List.of(), chunk.successfulOffsets().boxed().toList());
        assertEquals(1, fixture.businessExecutions.get());
        assertEquals(1, fixture.businessAdds.get(), "the failure must come from the driver batch path");
        assertEquals(0, fixture.sideExecutions.get(), "incomplete business work must not start side-index DML");
    }

    private static BatchExecutionEvidence onlyFailedChunk(BatchExecutionEvidence evidence) {
        assertNotEquals(BatchExecutionState.SUCCESS, evidence.state());
        BatchExecutionEvidence chunk = evidence;
        assertEquals(2, chunk.inputCount());
        assertEquals(List.of(), chunk.failedOffsets().boxed().toList());
        return chunk;
    }

    private static BatchWriteRequest request() {
        return BatchWriteRequests.request(
                BUSINESS_SQL,
                2,
                List.of(String.class, Long.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(protectedRow(1L), protectedRow(2L)),
                BatchWriteOptions.of(2),
                BatchRowCountPolicy.ANY);
    }

    private static Object[] protectedRow(long id) {
        SqlRequest write = new SqlRequest(BUSINESS_SQL, List.of("value-" + id, id));
        byte[] token = ByteBuffer.allocate(Long.BYTES).putLong(id).array();
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                write,
                null,
                List.of("id"),
                Map.of("id", id),
                "id = ?",
                "delete from token_index where id = ? and field_tag = ?",
                TOKEN_SQL,
                List.of(new ProtectedWriteWork.FieldTokens("value_col", List.of(token))));
        return ProtectedBatchRows.extend(write.parameters().toArray(), work);
    }

    private static Result rowsUpdated(Publisher<Long> counts) {
        return proxy(Result.class, (proxy, method, arguments) -> {
            if (method.getName().equals("getRowsUpdated")) {
                return counts;
            }
            throw new AssertionError("unexpected result operation: " + method.getName());
        });
    }

    private static void assertCausedBy(Throwable actual, Throwable expected) {
        for (Throwable current = actual; current != null; current = current.getCause()) {
            if (current == expected) {
                return;
            }
        }
        throw new AssertionError("the original driver failure was not preserved", actual);
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(
                type.getClassLoader(), new Class<?>[]{type}, handler));
    }

    // Uses the same proxy-only external-transaction fixture pattern as existing evidence tests.
    private static final class Fixture {
        private final AtomicInteger businessExecutions = new AtomicInteger();
        private final AtomicInteger businessAdds = new AtomicInteger();
        private final AtomicInteger sideExecutions = new AtomicInteger();
        private final AtomicBoolean businessResultsCompleted = new AtomicBoolean();
        private final R2dbcSqlExecutor executor;

        private Fixture(Flux<Result> results, RuntimeException sideFailure) {
            Connection connection = proxy(Connection.class, (proxy, method, arguments) -> {
                if (method.getName().equals("createStatement")) {
                    return statement((String) arguments[0], results, sideFailure);
                }
                throw new AssertionError("external connection must not be managed: " + method.getName());
            });
            ConnectionFactory unusedFactory = new ConnectionFactory() {
                @Override
                public Publisher<? extends Connection> create() {
                    throw new AssertionError("external transaction must not acquire another connection");
                }

                @Override
                public ConnectionFactoryMetadata getMetadata() {
                    return () -> "H2";
                }
            };
            executor = R2dbcSqlExecutor.create(com.flying.orm.rdb.execution.ConnectionAccessTestSupport.borrowed(connection), RdbDialect.h2());
        }

        private Statement statement(String sql, Flux<Result> results, RuntimeException sideFailure) {
            boolean business = sql.startsWith("insert into business_row");
            if (!business && !sql.startsWith("insert into token_index")) {
                throw new AssertionError("unexpected SQL: " + sql);
            }
            return proxy(Statement.class, (proxy, method, arguments) -> {
                return switch (method.getName()) {
                    case "bind", "bindNull" -> proxy;
                    case "add" -> {
                        if (business) {
                            businessAdds.incrementAndGet();
                        }
                        yield proxy;
                    }
                    case "execute" -> {
                        if (business) {
                            businessExecutions.incrementAndGet();
                            yield results.doOnComplete(() -> businessResultsCompleted.set(true));
                        }
                        sideExecutions.incrementAndGet();
                        yield Flux.<Result>error(sideFailure);
                    }
                    default -> throw new AssertionError("unexpected statement operation: " + method.getName());
                };
            });
        }

        private BatchExecutionEvidenceException writeFailure() {
            return assertThrows(BatchExecutionEvidenceException.class,
                    () -> executor.writeBatchEvidence(request()).block(TEST_WAIT));
        }
    }
}
