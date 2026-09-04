package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlLargeObjectLimitExceededException;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Type;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class R2dbcLargeObjectRowsTest {

    @Test
    void doesNotCreateLargeObjectScopeForScalarRows() {
        AtomicInteger scopesCreated = new AtomicInteger();
        R2dbcLargeObjectRows.Mapper mapper = R2dbcLargeObjectRows.mapper(
                metadata(),
                SqlExecutionOptions.safeDefaults(),
                () -> {
                    scopesCreated.incrementAndGet();
                    return new R2dbcLargeObjectScope();
                });

        DynamicRow row = mapper.map(row()).block();

        assertEquals(7, row.value(0));
        assertEquals(0, scopesCreated.get());
    }

    @Test
    void createsAPlainMapperHolderForEachSubscription() {
        AtomicInteger subscriptions = new AtomicInteger();
        Flux<DynamicRow> rows = Flux.from(R2dbcLargeObjectRows.map(
                result(subscriptions), SqlExecutionOptions.safeDefaults(), R2dbcLargeObjectScope::new));

        DynamicRow first = rows.single().block();
        DynamicRow second = rows.single().block();

        assertEquals("first", first.columnName(0));
        assertEquals("second", second.columnName(0));
        assertEquals(2, subscriptions.get());
    }

    @Test
    void scalarResultsDoNotSerializeEveryRowThroughAnInnerPublisher() {
        AtomicLong largestRequest = new AtomicLong();
        Flux<DynamicRow> rows = Flux.from(R2dbcLargeObjectRows.map(
                demandAwareResult(metadata(), largestRequest),
                SqlExecutionOptions.safeDefaults(),
                R2dbcLargeObjectScope::new));

        List<DynamicRow> actual = rows.collectList().block();

        assertEquals(4, actual.size());
        assertTrue(largestRequest.get() > 1,
                   "a scalar result must propagate bulk demand instead of requesting one row per inner Mono");
    }

    @Test
    void largeObjectCapableResultsKeepSequentialMaterialization() {
        AtomicLong largestRequest = new AtomicLong();
        Flux<DynamicRow> rows = Flux.from(R2dbcLargeObjectRows.map(
                demandAwareResult(metadata(R2dbcType.BLOB, "content"), largestRequest),
                SqlExecutionOptions.safeDefaults(),
                R2dbcLargeObjectScope::new));

        assertEquals(4, rows.collectList().block().size());
        assertEquals(1, largestRequest.get(),
                     "large-object capable rows must remain serialized for locator ownership and cleanup");
    }

    @Test
    void rejectsClobAlreadyMaterializedAsOversizedString() {
        RowMetadata metadata = metadata(R2dbcType.CLOB, "content");
        R2dbcLargeObjectRows.Mapper mapper = R2dbcLargeObjectRows.mapper(
                metadata,
                SqlExecutionOptions.safeDefaults().withMaxLargeObjectChars(3),
                R2dbcLargeObjectScope::new);

        SqlLargeObjectLimitExceededException failure = assertThrows(
                SqlLargeObjectLimitExceededException.class,
                () -> mapper.map(row("four", metadata)).block());

        assertEquals(SqlLargeObjectLimitExceededException.Kind.CHARACTER, failure.kind());
        assertEquals(3, failure.maxSize());
        assertEquals(4, failure.actualSize());
    }

    @Test
    void doesNotApplyAClobLimitToAnOrdinaryVarcharColumn() {
        RowMetadata metadata = metadata(
                column(R2dbcType.VARCHAR, "summary"),
                column(R2dbcType.CLOB, "content"));
        R2dbcLargeObjectRows.Mapper mapper = R2dbcLargeObjectRows.mapper(
                metadata,
                SqlExecutionOptions.safeDefaults().withMaxLargeObjectChars(3),
                R2dbcLargeObjectScope::new);

        DynamicRow actual = mapper.map(row(new Object[]{"ordinary text", null}, metadata)).block();

        assertEquals("ordinary text", actual.value(0));
    }

    @Test
    void rejectsABlobAlreadyMaterializedAsOversizedBytes() {
        RowMetadata metadata = metadata(R2dbcType.BLOB, "content");
        R2dbcLargeObjectRows.Mapper mapper = R2dbcLargeObjectRows.mapper(
                metadata,
                SqlExecutionOptions.safeDefaults().withMaxLargeObjectBytes(3),
                R2dbcLargeObjectScope::new);

        SqlLargeObjectLimitExceededException failure = assertThrows(
                SqlLargeObjectLimitExceededException.class,
                () -> mapper.map(row(ByteBuffer.wrap(new byte[4]), metadata)).block());

        assertEquals(SqlLargeObjectLimitExceededException.Kind.BINARY, failure.kind());
        assertEquals(3, failure.maxSize());
        assertEquals(4, failure.actualSize());
    }

    private static RowMetadata metadata() {
        return metadata(R2dbcType.INTEGER, "id");
    }

    private static RowMetadata metadata(Type type, String name) {
        return metadata(column(type, name));
    }

    private static ColumnMetadata column(Type type, String name) {
        return new ColumnMetadata() {
            @Override
            public Type getType() {
                return type;
            }

            @Override
            public String getName() {
                return name;
            }
        };
    }

    private static RowMetadata metadata(ColumnMetadata... columns) {
        return new RowMetadata() {
            @Override
            public ColumnMetadata getColumnMetadata(int index) {
                return columns[index];
            }

            @Override
            public ColumnMetadata getColumnMetadata(String name) {
                return Arrays.stream(columns)
                             .filter(column -> column.getName().equals(name))
                             .findFirst()
                             .orElseThrow();
            }

            @Override
            public List<? extends ColumnMetadata> getColumnMetadatas() {
                return List.of(columns);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static Result result(AtomicInteger subscriptions) {
        return (Result) Proxy.newProxyInstance(
                Result.class.getClassLoader(),
                new Class<?>[]{Result.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "map" -> {
                        BiFunction<Row, RowMetadata, Object> mapper =
                                (BiFunction<Row, RowMetadata, Object>) arguments[0];
                        yield Flux.defer(() -> {
                            int subscription = subscriptions.incrementAndGet();
                            String column = subscription == 1 ? "first" : "second";
                            RowMetadata metadata = metadata(R2dbcType.INTEGER, column);
                            return Flux.just(mapper.apply(row(subscription, metadata), metadata));
                        });
                    }
                    case "toString" -> "large-object-rows-test-result";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    @SuppressWarnings("unchecked")
    private static Result demandAwareResult(RowMetadata metadata, AtomicLong largestRequest) {
        return (Result) Proxy.newProxyInstance(
                Result.class.getClassLoader(),
                new Class<?>[]{Result.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "map" -> {
                        BiFunction<Row, RowMetadata, Object> mapper =
                                (BiFunction<Row, RowMetadata, Object>) arguments[0];
                        yield Flux.range(1, 4)
                                  .doOnRequest(request -> largestRequest.accumulateAndGet(request, Math::max))
                                  .map(value -> mapper.apply(row((Object) null, metadata), metadata));
                    }
                    case "toString" -> "scalar-rows-test-result";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Row row() {
        return row(7, metadata());
    }

    private static Row row(Object value, RowMetadata metadata) {
        return row(new Object[]{value}, metadata);
    }

    private static Row row(Object[] values, RowMetadata metadata) {
        return new Row() {
            @Override
            public RowMetadata getMetadata() {
                return metadata;
            }

            @Override
            public <T> T get(int index, Class<T> type) {
                return type.cast(values[index]);
            }

            @Override
            public <T> T get(String name, Class<T> type) {
                List<? extends ColumnMetadata> columns = metadata.getColumnMetadatas();
                for (int index = 0; index < columns.size(); index++) {
                    if (columns.get(index).getName().equals(name)) {
                        return type.cast(values[index]);
                    }
                }
                throw new IllegalArgumentException("unknown column " + name);
            }
        };
    }
}
