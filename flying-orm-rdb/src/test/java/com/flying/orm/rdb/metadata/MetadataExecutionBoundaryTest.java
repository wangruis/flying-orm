package com.flying.orm.rdb.metadata;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class MetadataExecutionBoundaryTest {

    @Test
    void cachedBuiltInReaderUsesOnlySqlAndNeverProbesTransactionState() {
        AtomicInteger queries = new AtomicInteger();
        AtomicInteger transactionProbes = new AtomicInteger();
        ReactiveSqlExecutor executor = (ReactiveSqlExecutor) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{ReactiveSqlExecutor.class},
                (self, method, arguments) -> {
                    if (method.getName().equals("currentTransaction")) {
                        transactionProbes.incrementAndGet();
                        return Mono.empty();
                    }
                    if (!method.getName().equals("query")) {
                        throw new AssertionError("metadata may only execute SQL: " + method.getName());
                    }
                    return Flux.deferContextual(context -> {
                        assertEquals("caller-context", context.get("metadata-test"));
                        queries.incrementAndGet();
                        return Flux.just(DynamicRow.copyOf(Map.of(
                                "COLUMN_NAME", "id", "DATA_TYPE", "BIGINT", "PRIMARY_KEY", true)));
                    });
                });
        ReactiveFormMetadataCache reader = ReactiveFormMetadataReaders.cached(
                new InformationSchemaFormMetadataReader(executor,
                        (schema, table) -> new SqlRequest("select columns", List.of()), type -> type));

        var first = reader.readForm("customers", "customers")
                .contextWrite(context -> context.put("metadata-test", "caller-context")).block();
        var second = reader.readForm("customers", "customers").block();

        assertNotNull(first);
        assertSame(first, second);
        assertEquals(1, queries.get());
        assertEquals(0, transactionProbes.get());
    }
}
