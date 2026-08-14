package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 在批量 insert 的同一个连接里逐行执行 SQL，并把数据库生成的主键立即交回 Repository。
 *
 * <p>只有必须从数据库取键的实体会走这里；普通批量仍使用驱动原生 batch。生成键按输入偏移逐行回填，
 * 不收集整批主键，内存占用始终受分片大小约束。</p>
 */
final class R2dbcBatchGeneratedKeyWriter {

    private final R2dbcBindMarkers bindMarkers;

    R2dbcBatchGeneratedKeyWriter(R2dbcBindMarkers bindMarkers) {
        this.bindMarkers = Objects.requireNonNull(bindMarkers, "r2dbc bind markers must not be null");
    }

    /**
     * 执行一行 insert。更新计数和生成键必须在同一次 segment 遍历中读取。
     */
    Mono<GeneratedWrite> write(Connection connection,
                               BatchWriteRequest request,
                               Object[] parameters,
                               long inputOffset,
                               R2dbcLargeObjectScope largeObjects) {
        return Mono.defer(() -> {
            Statement statement = Objects.requireNonNull(connection, "r2dbc connection must not be null")
                    .createStatement(bindMarkers.adapt(request.sql(),
                                                       request.parameterCount(),
                                                       request.bindMarkerStyle()));
            bind(statement, request, parameters);
            statement.returnGeneratedValues(request.generatedKeys().columnName());
            SqlExecutionOptions options = largeObjectOptions(request);
            Accumulator accumulator = new Accumulator(request, inputOffset, options, largeObjects);
            return Flux.from(statement.execute())
                    .concatMap(result -> Flux.from(result.flatMap(segment -> consume(segment, accumulator))), 1)
                    .then(Mono.fromSupplier(accumulator::finish));
        });
    }

    private static Mono<Void> consume(Result.Segment segment, Accumulator accumulator) {
        if (segment instanceof Result.UpdateCount count) {
            accumulator.addAffectedRows(count.value());
            return Mono.empty();
        }
        if (segment instanceof Result.RowSegment row) {
            return accumulator.addGeneratedKey(row);
        }
        if (segment instanceof Result.Message message) {
            return Mono.error(message.exception());
        }
        return Mono.empty();
    }

    private static SqlExecutionOptions largeObjectOptions(BatchWriteRequest request) {
        long limit = request.options().maxBufferedBytes();
        return SqlExecutionOptions.safeDefaults()
                                  .withTimeout(request.options().timeout())
                                  .withMaxResultBytes(limit)
                                  .withMaxLargeObjectBytes(limit)
                                  .withMaxLargeObjectChars(limit);
    }

    private static void bind(Statement statement, BatchWriteRequest request, Object[] parameters) {
        if (parameters == null || parameters.length != request.parameterCount()) {
            throw new IllegalArgumentException("batch row parameter count does not match request parameter count");
        }
        for (int index = 0; index < parameters.length; index++) {
            Object value = parameters[index];
            if (value == null) {
                statement.bindNull(index, request.parameterTypes().get(index));
            } else {
                statement.bind(index, R2dbcParameterValues.forBinding(value));
            }
        }
    }

    /** 每一行都使用独立累加器，不把可变状态带入并发分片或下一次订阅。 */
    private static final class Accumulator {

        private final BatchWriteRequest request;
        private final long inputOffset;
        private final SqlExecutionOptions options;
        private final R2dbcLargeObjectScope largeObjects;
        private long affectedRows;
        private boolean updateCountSeen;
        private DynamicRow generatedKey;

        private Accumulator(BatchWriteRequest request,
                            long inputOffset,
                            SqlExecutionOptions options,
                            R2dbcLargeObjectScope largeObjects) {
            this.request = request;
            this.inputOffset = inputOffset;
            this.options = options;
            this.largeObjects = largeObjects;
        }

        private void addAffectedRows(long rows) {
            updateCountSeen = true;
            affectedRows = R2dbcExecutionCounts.add(affectedRows, rows);
        }

        private Mono<Void> addGeneratedKey(Result.RowSegment segment) {
            if (generatedKey != null) {
                return Mono.error(new IllegalStateException(
                        "database returned more than one generated key for batch row " + inputOffset));
            }
            return R2dbcLargeObjectRows.map(segment, options, largeObjects).doOnNext(key -> {
                if (key.columnCount() != 1 || key.value(0) == null) {
                    throw new IllegalStateException(
                            "database did not return one non-null generated key for batch row " + inputOffset);
                }
                generatedKey = key;
            }).then();
        }

        private GeneratedWrite finish() {
            if (generatedKey == null) {
                throw new IllegalStateException("database did not return a generated key for batch row " + inputOffset);
            }
            request.generatedKeys().accept(inputOffset, generatedKey);
            // 少数驱动只发布生成键行而没有 UpdateCount，此时键行本身证明该行已执行。
            return new GeneratedWrite(updateCountSeen ? affectedRows : 1L, generatedKey);
        }

    }

    record GeneratedWrite(long affectedRows, DynamicRow generatedKey) {
    }
}
