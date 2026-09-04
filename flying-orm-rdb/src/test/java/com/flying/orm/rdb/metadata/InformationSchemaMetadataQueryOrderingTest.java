package com.flying.orm.rdb.metadata;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

class InformationSchemaMetadataQueryOrderingTest {

    @Test
    void readTableSerializesMetadataQueriesOnOneExecutor() {
        RejectingConcurrentExecutor executor = new RejectingConcurrentExecutor();
        InformationSchemaFormMetadataReader reader = new InformationSchemaFormMetadataReader(
                executor, completeQueries());

        assertDoesNotThrow(() -> reader.readTable("app", "orders").block());
        assertEquals(1, executor.maxActive.get());
    }

    @Test
    void readSnapshotSerializesEveryCompleteMetadataQueryOnOneExecutor() {
        RejectingConcurrentExecutor executor = new RejectingConcurrentExecutor();
        InformationSchemaFormMetadataReader reader = new InformationSchemaFormMetadataReader(
                executor, completeQueries());

        assertDoesNotThrow(() -> reader.readSnapshot("app", "orders").block());
        assertEquals(1, executor.maxActive.get());
    }

    private static InformationSchemaFormMetadataReader.Queries completeQueries() {
        return InformationSchemaFormMetadataReader.Queries.complete(
                (schema, table) -> request("columns"),
                (schema, table) -> request("indexes"),
                (schema, table) -> request("foreign_keys"),
                value -> value,
                (schema, table) -> request("table"),
                (schema, table) -> request("primary_key"),
                (schema, table) -> request("unique_constraints"),
                (schema, table) -> request("checks"),
                InformationSchemaFormMetadataReader.SnapshotDialect.H2);
    }

    private static SqlRequest request(String name) {
        return new SqlRequest(name, java.util.List.of());
    }

    private static final class RejectingConcurrentExecutor implements ReactiveSqlExecutor {

        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger maxActive = new AtomicInteger();

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return Flux.defer(() -> {
                int concurrent = active.incrementAndGet();
                maxActive.accumulateAndGet(concurrent, Math::max);
                if (concurrent > 1) {
                    return Flux.<DynamicRow>error(new IllegalStateException("metadata queries overlapped"))
                            .doOnError(ignored -> active.decrementAndGet());
                }
                Flux<DynamicRow> rows = switch (request.sql()) {
                    case "columns" -> Flux.just(row(
                            "COLUMN_NAME", "id", "DATA_TYPE", "BIGINT", "NULLABLE", false,
                            "IS_IDENTITY", false, "COLUMN_REPRESENTABLE", true));
                    case "table" -> Flux.just(row(
                            "TABLE_COMMENT", null, "TABLE_REPRESENTABLE", true));
                    default -> Flux.empty();
                };
                return Mono.delay(Duration.ofMillis(15))
                        .thenMany(rows)
                        .doOnComplete(active::decrementAndGet)
                        .doOnError(ignored -> active.decrementAndGet())
                        .doOnCancel(active::decrementAndGet);
            });
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            return Mono.error(new UnsupportedOperationException());
        }

        private static DynamicRow row(Object... values) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int index = 0; index < values.length; index += 2) {
                row.put((String) values[index], values[index + 1]);
            }
            return DynamicRow.copyOf(row);
        }
    }
}
