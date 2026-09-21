package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * 在批量 insert 的同一个连接里逐行执行 SQL，并把数据库生成的主键立即交回 Repository。
 *
 * <p>只有必须从数据库取键的实体会走这里；普通批量仍使用驱动原生 batch。生成键按输入偏移逐行回填，
 * 不收集整批主键，内存占用始终受输入缓冲大小约束。</p>
 */
final class R2dbcBatchGeneratedKeyWriter {

    /**
     * 执行一行 insert。更新计数和生成键必须在同一次 segment 遍历中读取。
     */
    Mono<GeneratedWrite> write(Connection connection,
                               BatchWriteRequest request,
                               String transportSql,
                               ProtectedBatchRows.RowView row,
                               long inputOffset,
                               Supplier<R2dbcLargeObjectScope> largeObjects) {
        return write(connection, request, transportSql, row, inputOffset, largeObjects, null);
    }

    Mono<GeneratedWrite> write(Connection connection,
                               BatchWriteRequest request,
                               String transportSql,
                               ProtectedBatchRows.RowView row,
                               long inputOffset,
                               Supplier<R2dbcLargeObjectScope> largeObjects,
                               R2dbcBatchEvidenceCounts evidence) {
        return write(connection, request, transportSql, row, inputOffset, largeObjects,
                largeObjectOptions(request), evidence);
    }

    Mono<GeneratedWrite> write(Connection connection,
                               BatchWriteRequest request,
                               String transportSql,
                               ProtectedBatchRows.RowView row,
                               long inputOffset,
                               Supplier<R2dbcLargeObjectScope> largeObjects,
                               SqlExecutionOptions options,
                               R2dbcBatchEvidenceCounts evidence) {
        return Mono.defer(() -> {
            Statement statement = Objects.requireNonNull(connection, "r2dbc connection must not be null")
                    .createStatement(transportSql);
            bind(statement, request, row);
            statement.returnGeneratedValues(request.generatedKeys().columnName());
            Accumulator accumulator = new Accumulator(
                    inputOffset,
                    Objects.requireNonNull(options, "sql execution options must not be null"),
                    largeObjects,
                    evidence);
            if (evidence != null) {
                evidence.startBusinessExecution();
            }
            return Flux.from(statement.execute())
                    .concatMap(result -> Flux.from(result.flatMap(segment -> consume(segment, accumulator))), 1)
                    .doOnComplete(accumulator::completeSql)
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

    static SqlExecutionOptions largeObjectOptions(BatchWriteRequest request) {
        long limit = request.options().maxBufferedBytes();
        return new SqlExecutionOptions(0, limit, limit, limit, 0);
    }

    private static void bind(Statement statement,
                             BatchWriteRequest request,
                             ProtectedBatchRows.RowView row) {
        for (int index = 0; index < row.parameterCount(); index++) {
            Object value = row.row()[index];
            if (value == null) {
                statement.bindNull(index, request.parameterTypes().get(index));
            } else {
                statement.bind(index, R2dbcParameterValues.forOwnedBinding(value));
            }
        }
    }

    /** 每一行都使用独立累加器，不把可变状态带入后续缓冲或下一次订阅。 */
    private static final class Accumulator {

        private final long inputOffset;
        private final SqlExecutionOptions options;
        private final Supplier<R2dbcLargeObjectScope> largeObjects;
        private final R2dbcBatchEvidenceCounts evidence;
        private long affectedRows;
        private boolean updateCountSeen;
        private DynamicRow generatedKey;
        private Throwable keyFailure;

        private Accumulator(long inputOffset,
                            SqlExecutionOptions options,
                            Supplier<R2dbcLargeObjectScope> largeObjects,
                            R2dbcBatchEvidenceCounts evidence) {
            this.inputOffset = inputOffset;
            this.options = options;
            this.largeObjects = largeObjects;
            this.evidence = evidence;
        }

        private void addAffectedRows(long rows) {
            updateCountSeen = true;
            if (rows >= 0) affectedRows = R2dbcExecutionCounts.add(affectedRows, rows);
            if (evidence != null) {
                evidence.recordRowCount(rows);
            }
        }

        private Mono<Void> addGeneratedKey(Result.RowSegment segment) {
            try {
                return R2dbcLargeObjectRows.map(segment, options, largeObjects).doOnNext(key -> {
                    if (generatedKey != null) throw new IllegalStateException(
                            "database returned more than one generated key for batch row " + inputOffset);
                    if (key.columnCount() != 1 || key.value(0) == null) throw new IllegalStateException(
                            "database did not return one non-null generated key for batch row " + inputOffset);
                    generatedKey = key;
                }).then().onErrorResume(this::keyReadFailed);
            } catch (Throwable failure) {
                return keyReadFailed(failure);
            }
        }

        private Mono<Void> keyReadFailed(Throwable failure) {
            VirtualMachineError fatal = com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError(failure);
            if (fatal != null || failure instanceof java.util.concurrent.CancellationException)
                return Mono.error(fatal == null ? failure : fatal);
            keyFailure = R2dbcExecutionSession.merge(keyFailure, failure);
            // Finish draining the driver's result before publishing its SQL facts and the mapping failure.
            return Mono.empty();
        }

        private GeneratedWrite finish() {
            if (keyFailure != null) throw reactor.core.Exceptions.propagate(keyFailure);
            if (generatedKey == null) {
                throw new IllegalStateException("database did not return a generated key for batch row " + inputOffset);
            }
            // 少数驱动只发布生成键行而没有 UpdateCount，此时键行本身证明该行已执行。
            long rows = updateCountSeen ? affectedRows : 1L;
            return new GeneratedWrite(rows, generatedKey);
        }

        private void completeSql() {
            if (evidence == null) return;
            if (!updateCountSeen && generatedKey != null) evidence.recordRowCount(1L);
            evidence.completeRow(affectedRows);
        }

    }

    record GeneratedWrite(long affectedRows, DynamicRow generatedKey) {
    }
}
