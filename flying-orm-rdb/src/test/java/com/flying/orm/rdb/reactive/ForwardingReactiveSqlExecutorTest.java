package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionSequence;
import com.flying.orm.rdb.execution.SqlExecutionSequenceResult;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

class ForwardingReactiveSqlExecutorTest {

    @Test
    void preservesCoreAndConnectionScopedCapabilities() {
        ProbeExecutor delegate = new ProbeExecutor();
        ReactiveSqlExecutor forwarding = delegate.withBatchMemoryLimits(BatchMemoryLimits.defaults());
        SqlRequest request = new SqlRequest("select ?", List.of(1));
        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults();
        SqlExecutionSequence sequence = new SqlExecutionSequence(List.of(), List.of(request), List.of());

        assertSame(delegate.transaction, forwarding.currentTransaction());
        assertSame(delegate.query, forwarding.query(request, options));
        assertSame(delegate.update, forwarding.rowsUpdated(request, options));
        assertSame(delegate.write, forwarding.rowsUpdatedReturningKeys(request, options, "id"));
        assertSame(delegate.sequence, assertInstanceOf(
                ConnectionScopedReactiveSqlExecutor.class, forwarding).executeInConnection(sequence, options));
    }

    @Test
    void decoratorsDoNotInventConnectionScopedCapability() {
        ReactiveSqlExecutor delegate = new PlainExecutor();

        assertFalse(delegate.withObserver(ignored -> {
        }) instanceof ConnectionScopedReactiveSqlExecutor);
        assertFalse(delegate.withDefaultExecutionOptions(
                SqlExecutionOptions.safeDefaults()) instanceof ConnectionScopedReactiveSqlExecutor);
        assertFalse(delegate.withBatchMemoryLimits(
                BatchMemoryLimits.defaults()) instanceof ConnectionScopedReactiveSqlExecutor);
    }

    @Test
    void replacesRepeatedBatchLimitPolicyInsteadOfStackingDecorators() {
        ProbeExecutor delegate = new ProbeExecutor();
        ReactiveSqlExecutor limited = delegate.withBatchMemoryLimits(BatchMemoryLimits.defaults())
                .withBatchMemoryLimits(new BatchMemoryLimits(20_000, 64, 20_000_000L,
                        512L * 1024 * 1024, 131_072));

        assertSame(delegate, ((ForwardingReactiveSqlExecutor) limited).delegate());
    }

    @Test
    void replacesBatchLimitAcrossOtherDecorators() {
        BatchExecutor delegate = new BatchExecutor();
        BatchMemoryLimits oldLimits = new BatchMemoryLimits(
                1, 32, 10, 64L * 1024 * 1024, 100_000);
        BatchMemoryLimits newLimits = new BatchMemoryLimits(
                2, 32, 10, 64L * 1024 * 1024, 100_000);
        ReactiveSqlExecutor configured = delegate.withBatchMemoryLimits(oldLimits)
                .withObserver(ignored -> {
                })
                .withBatchMemoryLimits(newLimits);
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(id) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.empty(),
                BatchWriteOptions.atomic(2));

        assertInstanceOf(ConnectionScopedReactiveSqlExecutor.class, configured);
        assertSame(delegate.result, configured.writeBatch(request).block());
    }

    private static final class ProbeExecutor
            implements ReactiveSqlExecutor, ConnectionScopedReactiveSqlExecutor {

        private final Mono<R2dbcTransactionContext> transaction = Mono.empty();
        private final Flux<DynamicRow> query = Flux.empty();
        private final Mono<Long> update = Mono.just(1L);
        private final Mono<SqlWriteResult> write = Mono.just(new SqlWriteResult(1L, List.of()));
        private final Mono<SqlExecutionSequenceResult> sequence = Mono.empty();

        @Override
        public Mono<R2dbcTransactionContext> currentTransaction() {
            return transaction;
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return query;
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
            return query;
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            return update;
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
            return update;
        }

        @Override
        public Mono<SqlWriteResult> rowsUpdatedReturningKeys(SqlRequest request,
                                                             SqlExecutionOptions options,
                                                             String generatedKeyColumn) {
            return write;
        }

        @Override
        public Mono<SqlExecutionSequenceResult> executeInConnection(SqlExecutionSequence sequence,
                                                                    SqlExecutionOptions options) {
            return this.sequence;
        }
    }

    private static class PlainExecutor implements ReactiveSqlExecutor {

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return Flux.empty();
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            return Mono.just(0L);
        }
    }

    private static final class BatchExecutor extends PlainExecutor
            implements ConnectionScopedReactiveSqlExecutor {

        private final BatchWriteResult result = BatchWriteResult.from(
                BatchWriteOptions.Mode.ATOMIC, List.of());

        @Override
        public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
            return Mono.just(result);
        }

        @Override
        public Mono<SqlExecutionSequenceResult> executeInConnection(
                SqlExecutionSequence sequence,
                SqlExecutionOptions options) {
            return Mono.empty();
        }
    }
}
