package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.execution.SqlRowLimitExceededException;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlStatementType;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 在一次 R2DBC Statement 消费中同时收集更新计数和数据库生成键。
 *
 * <p>{@link Result} 只能消费一次，因此直接遍历 segment，把更新计数和键行汇总到同一结果，避免为取主键
 * 重复执行 insert。实例不保存订阅状态，可以由执行器并发复用。</p>
 */
final class R2dbcGeneratedKeyWriter {

    private final R2dbcExecutionSession session;

    R2dbcGeneratedKeyWriter(R2dbcExecutionSession session) {
        this.session = Objects.requireNonNull(session, "R2DBC execution session must not be null");
    }

    Mono<SqlWriteResult> write(SqlRequest request, SqlExecutionOptions options) {
        return write(request, options, null);
    }

    Mono<SqlWriteResult> write(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        return session.withStatementMono(
                safeRequest,
                safeOptions,
                SqlExecutionOperation.UPDATE,
                (statement, largeObjects) -> collect(statement, safeOptions, largeObjects, generatedKeyColumn));
    }

    static Mono<SqlWriteResult> collect(Statement statement,
                                        SqlExecutionOptions options,
                                        R2dbcLargeObjectScope largeObjects) {
        return collect(statement, options, largeObjects, null);
    }

    static Mono<SqlWriteResult> collect(Statement statement,
                                        SqlExecutionOptions options,
                                        R2dbcLargeObjectScope largeObjects,
                                        String generatedKeyColumn) {
        if (generatedKeyColumn == null) {
            statement.returnGeneratedValues();
        } else {
            String column = generatedKeyColumn.trim();
            if (column.isEmpty()) {
                return Mono.error(new IllegalArgumentException("generated key column must not be blank"));
            }
            statement.returnGeneratedValues(column);
        }
        Accumulator accumulator = new Accumulator(options, largeObjects);
        return Flux.from(statement.execute())
                   .concatMap(result -> Flux.from(result.flatMap(segment -> segment(segment, accumulator))), 1)
                   .then(Mono.fromSupplier(accumulator::result))
                   .onErrorMap(accumulator::wrapFailure);
    }

    private static Mono<Void> segment(Result.Segment segment, Accumulator accumulator) {
        if (segment instanceof Result.UpdateCount count) {
            accumulator.addAffectedRows(count.value());
            return Mono.empty();
        }
        if (segment instanceof Result.RowSegment row) {
            return accumulator.addKey(row);
        }
        if (segment instanceof Result.Message message) {
            return Mono.error(message.exception());
        }
        return Mono.empty();
    }

    /** 每次订阅单独创建，所有可变状态都不会跨请求共享。 */
    private static final class Accumulator {

        private final SqlExecutionOptions options;
        private final R2dbcLargeObjectScope largeObjects;
        private final List<DynamicRow> keys = new ArrayList<>();
        private long affectedRows;
        private long estimatedBytes;
        private boolean updateCountSeen;
        private long generatedRowsSeen;
        private boolean writeObserved;

        private Accumulator(SqlExecutionOptions options,
                            R2dbcLargeObjectScope largeObjects) {
            this.options = options;
            this.largeObjects = largeObjects;
        }

        private void addAffectedRows(long rows) {
            writeObserved = true;
            updateCountSeen = true;
            affectedRows = R2dbcExecutionCounts.add(affectedRows, rows);
        }

        private Mono<Void> addKey(Result.RowSegment segment) {
            writeObserved = true;
            generatedRowsSeen = R2dbcExecutionCounts.add(generatedRowsSeen, 1L);
            if (options.maxRows() > 0 && keys.size() >= options.maxRows()) {
                return Mono.error(new SqlRowLimitExceededException(
                        SqlStatementType.INSERT, options.maxRows(), keys.size()));
            }
            return R2dbcLargeObjectRows.map(segment, options, largeObjects).doOnNext(key -> {
                estimatedBytes = saturatedAdd(estimatedBytes, BatchMemoryBudget.estimateRowBytes(key));
                if (options.maxResultBytes() > 0
                        && (estimatedBytes == Long.MAX_VALUE || estimatedBytes > options.maxResultBytes())) {
                    throw new SqlResultMemoryLimitExceededException(
                            SqlStatementType.INSERT, options.maxResultBytes(), estimatedBytes, keys.size());
                }
                keys.add(key);
            }).then();
        }

        private SqlWriteResult result() {
            // 有些驱动只发布生成键行而不发布 UpdateCount；单行 insert 时每个键行代表一条成功写入。
            long rows = updateCountSeen ? affectedRows : generatedRowsSeen;
            return new SqlWriteResult(rows, keys);
        }

        private Throwable wrapFailure(Throwable failure) {
            VirtualMachineError fatal = ReactiveSqlExecutionProtection.findVirtualMachineError(failure);
            if (fatal != null || !writeObserved || failure instanceof GeneratedKeyReadException) {
                return fatal == null ? failure : fatal;
            }
            long rows = updateCountSeen ? affectedRows : generatedRowsSeen;
            return new GeneratedKeyReadException(rows, failure);
        }

        private static long saturatedAdd(long left, long right) {
            return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
        }
    }
}
