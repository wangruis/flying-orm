package com.flying.orm.rdb.reactive;
import com.flying.orm.core.sql.render.*;
import com.flying.orm.rdb.batch.*;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import io.r2dbc.spi.*;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.*;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import static org.junit.jupiter.api.Assertions.*;

class R2dbcBatchGeneratedKeyFactTest {
    @Test @SuppressWarnings("unchecked")
    void sharedCountAndKeySegmentPreservesBatchCountsAndKeyOffsets() {
        ColumnMetadata column = proxy(ColumnMetadata.class, (p,m,a) -> switch(m.getName()) {
            case "getName" -> "id";
            case "getType" -> R2dbcType.BIGINT;
            case "getJavaType" -> Long.class;
            default -> null;
        });
        RowMetadata metadata = proxy(RowMetadata.class, (p,m,a) ->
                m.getName().equals("getColumnMetadatas") ? List.of(column) : column);
        AtomicInteger executes = new AtomicInteger();
        AtomicInteger consumptions = new AtomicInteger();
        Statement statement = proxy(Statement.class, (p,m,a) -> {
            if (!m.getName().equals("execute")) return p;
            long generatedId = 40L + executes.incrementAndGet();
            Row row = proxy(Row.class, (rp,rm,ra) ->
                    rm.getName().equals("getMetadata") ? metadata : generatedId);
            Result result = proxy(Result.class, (resultProxy, method, arguments) -> {
                Function<Result.Segment,Publisher<Object>> consume =
                        (Function<Result.Segment,Publisher<Object>>) arguments[0];
                return Flux.<Result.Segment>just(new CountAndKey(1L, row))
                        .doOnSubscribe(ignored -> consumptions.incrementAndGet())
                        .concatMap(segment -> Flux.from(consume.apply(segment)));
            });
            return Flux.just(result);
        });
        Connection connection = proxy(Connection.class, (p,m,a) -> {
            if (m.getName().equals("createStatement")) return statement;
            throw new AssertionError(m.getName());
        });
        List<Long> offsets = new ArrayList<>();
        List<Object> keys = new ArrayList<>();
        BatchWriteRequest request = new BatchWriteRequest(SqlStatementPlan.canonical(
                "insert into sample(value) values (?)", SqlBindMarkerStyle.CANONICAL, 1),
                List.of(Integer.class), Flux.just(new Object[]{1}, new Object[]{2}), BatchWriteOptions.of(2),
                BatchRowCountPolicy.EXACTLY_ONE, BatchGeneratedKeys.required("id", (offset, value) -> {
                    offsets.add(offset);
                    keys.add(value.value(0));
                }));

        BatchExecutionEvidence actual = R2dbcSqlExecutor.create(
                ConnectionAccessTestSupport.borrowed(connection), RdbDialect.h2()).writeBatch(request).block();

        assertEquals(2, actual.successfulCount());
        assertEquals(2, actual.affectedRows().value());
        assertEquals(List.of(0L, 1L), offsets);
        assertEquals(List.of(41L, 42L), keys);
        assertEquals(2, executes.get());
        assertEquals(2, consumptions.get());
    }

    private record CountAndKey(long value, Row row) implements Result.UpdateCount, Result.RowSegment {
    }

    @Test void batchReadOptionsUseExactlyTheConfiguredCapacity() {
        for (long limit : List.of(1L, 32L * 1024 * 1024, 256L * 1024 * 1024)) {
            BatchWriteRequest request = new BatchWriteRequest(SqlStatementPlan.canonical(
                    "insert into sample(value) values (?)", SqlBindMarkerStyle.CANONICAL, 1),
                    List.of(Integer.class), Flux.empty(), BatchWriteOptions.of(1).withMemoryLimits(10, limit),
                    BatchRowCountPolicy.ANY, BatchGeneratedKeys.none());
            assertEquals(new com.flying.orm.rdb.execution.SqlExecutionOptions(0, limit, limit, limit, 0),
                    R2dbcBatchGeneratedKeyWriter.largeObjectOptions(request));
        }
    }

    @Test @SuppressWarnings("unchecked")
    void completedSqlEvidenceSurvivesInvalidGeneratedKeyLayout() {
        ColumnMetadata column = proxy(ColumnMetadata.class, (p,m,a) -> switch(m.getName()) {
            case "getName" -> "id";
            case "getType" -> R2dbcType.INTEGER;
            case "getJavaType" -> Integer.class;
            default -> null;
        });
        RowMetadata metadata = proxy(RowMetadata.class, (p,m,a) ->
                m.getName().equals("getColumnMetadatas") ? List.of(column) : column);
        Row row = proxy(Row.class, (p,m,a) -> m.getName().equals("getMetadata") ? metadata : null);
        Result.RowSegment key = () -> row;
        Result.UpdateCount count = () -> 1L;
        Result result = proxy(Result.class, (p,m,a) -> {
            Function<Result.Segment,Publisher<Object>> consume = (Function<Result.Segment,Publisher<Object>>) a[0];
            return Flux.<Result.Segment>just(count,key).concatMap(segment -> Flux.from(consume.apply(segment)));
        });
        Statement statement = proxy(Statement.class, (p,m,a) ->
                m.getName().equals("execute") ? Flux.just(result) : p);
        Connection connection = proxy(Connection.class, (p,m,a) -> {
            if(m.getName().equals("createStatement")) return statement;
            throw new AssertionError(m.getName());
        });
        BatchWriteRequest request = new BatchWriteRequest(SqlStatementPlan.canonical(
                "insert into sample(value) values (?)", SqlBindMarkerStyle.CANONICAL, 1),
                List.of(Integer.class), Flux.<Object[]>just(new Object[]{1}), BatchWriteOptions.of(1),
                BatchRowCountPolicy.ANY, BatchGeneratedKeys.required("id", (offset,value)->fail("invalid key delivered")));
        var error = assertThrows(BatchExecutionEvidenceException.class, () -> R2dbcSqlExecutor.create(
                ConnectionAccessTestSupport.borrowed(connection), RdbDialect.h2())
                .writeBatch(request, offset -> Mono.fromRunnable(() -> fail("incomplete key work reached POST"))).block());
        assertEquals(1, error.evidence().successfulCount());
        assertEquals(1, error.evidence().affectedRows().value());
    }
    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }
}
