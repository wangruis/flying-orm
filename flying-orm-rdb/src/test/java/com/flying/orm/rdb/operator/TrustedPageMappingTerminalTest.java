package com.flying.orm.rdb.operator;

import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TrustedPageMappingTerminalTest {
    private static final SqlRenderer SQL = SqlRenderer.builder().addDefaultTerms().build();
    private static final RdbDialect DIALECT = RdbDialect.postgresql();

    @Test
    void syncTypedPageMapsDuringRowConsumption() {
        Result.mappings.set(0);
        Source source = new Source(3, true);
        PageResult<Result> page = sync(source).page(1, 3, Result.class);
        assertEquals(List.of(1, 2, 3), page.rows().stream().map(Result::id).toList());
        assertEquals(3L, page.total());
        assertEquals(1, source.closed.get());
    }

    @Test
    void reactiveTypedPageMapsDuringRowConsumptionOnEachSubscription() {
        Result.mappings.set(0);
        Source source = new Source(3, true);
        Mono<PageResult<Result>> query = reactive(source).page(1, 3, Result.class);
        assertEquals(0, source.emitted.get());
        for (int subscription = 1; subscription <= 2; subscription++) {
            PageResult<Result> page = query.block();
            assertEquals(List.of(1, 2, 3), page.rows().stream().map(Result::id).toList());
            assertEquals(3L, page.total());
            assertEquals(subscription * 3, Result.mappings.get());
        }
    }

    @Test
    void syncMapperFailureStopsConsumptionAndClosesSource() {
        Source source = new Source(3, false);
        assertThrows(RuntimeException.class, () -> sync(source).page(1, 3, Rejecting.class));
        assertEquals(2, source.emitted.get());
        assertEquals(1, source.closed.get());
    }

    @Test
    void reactiveMapperFailureCancelsRemainingRows() {
        Source source = new Source(3, false);
        assertThrows(RuntimeException.class, () -> reactive(source).page(1, 3, Rejecting.class).block());
        assertEquals(2, source.emitted.get());
        assertEquals(1, source.closed.get());
    }

    @Test
    void emptyCountSkipsDataForBothResultShapesAndExecutionModes() {
        Source source = new Source(0, false);
        assertEquals(List.of(), sync(source).page(1, 3).rows());
        assertEquals(List.of(), sync(source).page(1, 3, Result.class).rows());
        assertEquals(List.of(), reactive(source).page(1, 3).block().rows());
        assertEquals(List.of(), reactive(source).page(1, 3, Result.class).block().rows());
        assertEquals(0, source.emitted.get());
        assertEquals(0, source.closed.get());
    }

    private static SyncQueryOperator sync(Source source) {
        SyncSqlExecutor executor = new SyncSqlExecutor() {
            @Override
            public long queryWhile(SqlRequest request, SqlExecutionOptions options,
                                   Predicate<? super DynamicRow> consumer) {
                if (request.sql().contains("count(")) { consumer.test(source.count()); return 1; }
                int mappedBefore = Result.mappings.get();
                try {
                    for (int id = 1; id <= source.total; id++) {
                        source.emitted.incrementAndGet();
                        if (!consumer.test(DynamicRow.copyOf(Map.of("id", id)))) return id;
                        if (source.assertMapping) assertEquals(mappedBefore + id, Result.mappings.get());
                    }
                    return source.total;
                } finally { source.closed.incrementAndGet(); }
            }
            @Override public long rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected write"); }
        };
        SyncFormClient client = SyncFormClient.create(executor,
                (request, completed) -> { throw new AssertionError("unexpected batch"); },
                FormDataSqlRenderer.create(SQL, DIALECT));
        return new SyncQueryOperator(client, executor, SQL, DataScope.none(), DIALECT).from("users").select("id");
    }

    private static QueryOperator reactive(Source source) {
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                if (request.sql().contains("count(")) return Flux.just(source.count());
                return Flux.defer(() -> {
                    int mappedBefore = Result.mappings.get();
                    return Flux.range(1, source.total).map(id -> DynamicRow.copyOf(Map.of("id", id)))
                            .doOnNext(row -> source.emitted.incrementAndGet())
                            .doOnComplete(() -> {
                                if (source.assertMapping) assertEquals(mappedBefore + source.total, Result.mappings.get());
                            }).doFinally(signal -> source.closed.incrementAndGet());
                });
            }
            @Override public Mono<Long> rowsUpdated(SqlRequest request) { return Mono.error(new AssertionError("unexpected write")); }
        };
        ReactiveFormClient client = ReactiveFormClient.create(executor, FormDataSqlRenderer.create(SQL, DIALECT));
        return new QueryOperator(client, executor, SQL, DataScope.none(), DIALECT).from("users").select("id");
    }

    private record Source(int total, boolean assertMapping, AtomicInteger emitted, AtomicInteger closed) {
        Source(int total, boolean assertMapping) { this(total, assertMapping, new AtomicInteger(), new AtomicInteger()); }
        DynamicRow count() { return DynamicRow.copyOf(Map.of("total", (long) total)); }
    }

    record Result(Integer id) {
        static final AtomicInteger mappings = new AtomicInteger();
        Result { mappings.incrementAndGet(); }
    }

    record Rejecting(Integer id) {
        Rejecting { if (id == 2) throw new IllegalArgumentException("cannot map second row"); }
    }
}
