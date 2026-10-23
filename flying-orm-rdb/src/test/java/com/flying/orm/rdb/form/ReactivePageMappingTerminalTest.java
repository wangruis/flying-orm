package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReactivePageMappingTerminalTest {

    @TestFactory
    Stream<DynamicTest> typedPagesMapWhileConsumingRowsAndPrepareOnlyOnce() {
        return Stream.of(false, true).map(governed -> DynamicTest.dynamicTest("governed=" + governed, () -> {
            Result.mappings.set(0);
            AtomicInteger queries = new AtomicInteger();
            AtomicInteger preparations = new AtomicInteger();
            ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
                @Override
                public Flux<DynamicRow> query(SqlRequest request) {
                    return Flux.defer(() -> {
                        queries.incrementAndGet();
                        if (request.sql().contains("count(")) {
                            return Flux.just(DynamicRow.copyOf(Map.of("total", 3L)));
                        }
                        int before = Result.mappings.get();
                        return Flux.just(DynamicRow.copyOf(Map.of("id", 1L)),
                                        DynamicRow.copyOf(Map.of("id", 2L)))
                                .doOnComplete(() -> assertEquals(before + 2, Result.mappings.get(),
                                        "mapping must not wait for a complete intermediate row list"));
                    });
                }

                @Override
                public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.error(new AssertionError("unexpected write"));
                }
            };
            ReactiveFormClient client = ReactiveFormClient.create(executor,
                    FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql()))
                    .withQueryPreparation(spec -> Mono.fromSupplier(() -> {
                        preparations.incrementAndGet();
                        return spec;
                    }));
            if (governed) client = client.withQueryShapeLimits(QueryShapeLimits.defaults().withMaxProjectionCount(4));
            DynamicForm form = DynamicForm.builder("users", "users")
                    .addField(DynamicField.primaryKey("id", "BIGINT")).build();
            Mono<PageResult<Result>> page = client.page(QuerySpec.of(form, ConditionGroup.and().build()),
                    PageQuery.of(1, 2), Result.class);
            assertEquals(0, preparations.get());
            assertEquals(0, queries.get());
            for (int subscription = 1; subscription <= 2; subscription++) {
                PageResult<Result> result = page.block();
                assertEquals(List.of(1L, 2L), result.rows().stream().map(Result::id).toList());
                assertEquals(3L, result.total());
                assertEquals(subscription, preparations.get());
                assertEquals(subscription * 2, queries.get());
            }
        }));
    }

    record Result(Long id) {
        private static final AtomicInteger mappings = new AtomicInteger();
        Result { mappings.incrementAndGet(); }
    }

    @Test
    void mapperFailureCancelsTheRowsWithoutConsumingTheRemainder() {
        AtomicInteger emitted = new AtomicInteger();
        AtomicBoolean cancelled = new AtomicBoolean();
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                if (request.sql().contains("count(")) return Flux.just(DynamicRow.copyOf(Map.of("total", 3L)));
                return Flux.range(1, 3).map(id -> DynamicRow.copyOf(Map.of("id", id.longValue())))
                        .doOnNext(row -> emitted.incrementAndGet()).doOnCancel(() -> cancelled.set(true));
            }
            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.error(new AssertionError("unexpected write"));
            }
        };
        ReactiveFormClient client = ReactiveFormClient.create(executor,
                FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql()));
        DynamicForm form = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        RuntimeException failure = assertThrows(RuntimeException.class, () -> client.page(
                QuerySpec.of(form, ConditionGroup.and().build()), PageQuery.of(1, 3), Rejecting.class).block());
        Throwable cause = failure;
        while (cause.getCause() != null) cause = cause.getCause();
        assertEquals("cannot map second row", cause.getMessage());
        assertEquals(2, emitted.get());
        assertTrue(cancelled.get());
    }

    record Rejecting(Long id) {
        Rejecting { if (id == 2L) throw new IllegalStateException("cannot map second row"); }
    }
}
