package com.flying.orm.rdb.reactive;

import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlLargeObjectLimitExceededException;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.result.DynamicRowFactory;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Type;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * 在当前 R2DBC 结果资源域内把 Row 映射为 DynamicRow，并把大字段句柄登记到连接级清理域。
 *
 * @author wangr
 * @date 2026-08-13
 * @version v1.0
 */
final class R2dbcLargeObjectRows {

    private R2dbcLargeObjectRows() {
    }

    static Publisher<DynamicRow> map(Result result,
                                     SqlExecutionOptions options,
                                     R2dbcLargeObjectScope cleanupScope) {
        R2dbcLargeObjectScope safeScope = Objects.requireNonNull(
                cleanupScope, "large object cleanup scope must not be null");
        return map(result, options, () -> safeScope);
    }

    static Publisher<DynamicRow> map(Result result,
                                     SqlExecutionOptions options,
                                     Supplier<R2dbcLargeObjectScope> cleanupScope) {
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options,
                                                                  "sql execution options must not be null");
        Supplier<R2dbcLargeObjectScope> safeScope = Objects.requireNonNull(
                cleanupScope, "large object cleanup scope must not be null");
        return Flux.defer(() -> {
            Mapper[] mapper = new Mapper[1];
            Flux<Object> mappedRows = Flux.from(result.map((row, metadata) -> {
                Mapper current = mapper[0];
                if (current == null) {
                    current = mapper(metadata, safeOptions, safeScope);
                    mapper[0] = current;
                }
                return current.mapValue(row);
            }));
            // 一个 Result 的 metadata/Mapper 形态固定：标量行直接透传，只有 LOB 行才串行展开异步物化。
            return mappedRows.switchOnFirst((first, rows) -> {
                if (first.hasValue() && first.get() instanceof DynamicRow) {
                    return rows.cast(DynamicRow.class);
                }
                return rows.concatMap(R2dbcLargeObjectRows::materializedRow, 1);
            });
        }).onErrorResume(failure -> {
            VirtualMachineError fatal = findVirtualMachineError(failure);
            return fatal == null ? Flux.error(failure) : Flux.error(fatal);
        });
    }

    static Mono<DynamicRow> map(Result.RowSegment segment,
                                SqlExecutionOptions options,
                                R2dbcLargeObjectScope cleanupScope) {
        Row row = segment.row();
        return mapper(row.getMetadata(), options, cleanupScope).map(row);
    }

    static Mono<DynamicRow> map(Result.RowSegment segment,
                                SqlExecutionOptions options,
                                Supplier<R2dbcLargeObjectScope> cleanupScope) {
        Row row = segment.row();
        return mapper(row.getMetadata(), options, cleanupScope).map(row);
    }

    static Mono<DynamicRow> map(Row row,
                                RowMetadata metadata,
                                SqlExecutionOptions options,
                                R2dbcLargeObjectScope cleanupScope) {
        return mapper(metadata, options, cleanupScope).map(row);
    }

    static Mono<DynamicRow> map(Row row, RowMetadata metadata, SqlExecutionOptions options) {
        return Mono.usingWhen(Mono.fromSupplier(R2dbcLargeObjectScope::new),
                              scope -> map(row, metadata, options, scope),
                              R2dbcLargeObjectScope::complete,
                              R2dbcLargeObjectScope::error,
                              R2dbcLargeObjectScope::cancel);
    }

    static Mono<DynamicRow> materialize(DynamicRow row, SqlExecutionOptions options) {
        return Mono.usingWhen(Mono.fromSupplier(R2dbcLargeObjectScope::new),
                              scope -> scope.materialize(row, options),
                              R2dbcLargeObjectScope::complete,
                              R2dbcLargeObjectScope::error,
                              R2dbcLargeObjectScope::cancel);
    }

    static Mapper mapper(RowMetadata metadata,
                         SqlExecutionOptions options,
                         R2dbcLargeObjectScope cleanupScope) {
        R2dbcLargeObjectScope safeScope = Objects.requireNonNull(
                cleanupScope, "large object cleanup scope must not be null");
        return mapper(metadata, options, () -> safeScope);
    }

    static Mapper mapper(RowMetadata metadata,
                         SqlExecutionOptions options,
                         Supplier<R2dbcLargeObjectScope> cleanupScope) {
        RowMetadata safeMetadata = Objects.requireNonNull(metadata, "row metadata must not be null");
        return new Mapper(DynamicRowFactory.from(safeMetadata),
                          Objects.requireNonNull(options, "sql execution options must not be null"),
                          Objects.requireNonNull(cleanupScope, "large object cleanup scope must not be null"),
                          largeObjectPlan(safeMetadata));
    }

    private static LargeObjectPlan largeObjectPlan(RowMetadata metadata) {
        List<? extends ColumnMetadata> columns = metadata.getColumnMetadatas();
        LargeObjectKind[] kinds = null;
        for (int index = 0; index < columns.size(); index++) {
            LargeObjectKind kind = largeObjectKind(columns.get(index));
            if (kind != LargeObjectKind.NONE) {
                if (kinds == null) {
                    kinds = new LargeObjectKind[columns.size()];
                    java.util.Arrays.fill(kinds, LargeObjectKind.NONE);
                }
                kinds[index] = kind;
            }
        }
        return kinds == null ? null : new LargeObjectPlan(metadata, kinds);
    }

    private static LargeObjectKind largeObjectKind(ColumnMetadata column) {
        Type type = column.getType();
        if (type == R2dbcType.BLOB) {
            return LargeObjectKind.BINARY;
        }
        if (type == R2dbcType.CLOB || type == R2dbcType.NCLOB) {
            return LargeObjectKind.CHARACTER;
        }
        Class<?> javaType = column.getJavaType();
        if (javaType == null && type != null) {
            javaType = type.getJavaType();
        }
        if (javaType == null || javaType == Object.class) {
            return LargeObjectKind.UNKNOWN;
        }
        if (Blob.class.isAssignableFrom(javaType)) {
            return LargeObjectKind.BINARY;
        }
        if (Clob.class.isAssignableFrom(javaType)) {
            return LargeObjectKind.CHARACTER;
        }
        return LargeObjectKind.NONE;
    }

    private enum LargeObjectKind {
        NONE,
        CHARACTER,
        BINARY,
        UNKNOWN
    }

    private record LargeObjectPlan(RowMetadata metadata, LargeObjectKind[] kinds) {

        private LargeObjectKind kind(int index) {
            return kinds[index];
        }

        private LargeObjectKind kind(String name) {
            return largeObjectKind(metadata.getColumnMetadata(name));
        }
    }

    @SuppressWarnings("unchecked")
    private static Mono<DynamicRow> materializedRow(Object value) {
        if (value instanceof DynamicRow direct) {
            return Mono.just(direct);
        }
        if (value instanceof Mono<?> materialized) {
            return (Mono<DynamicRow>) materialized;
        }
        return Mono.error(new IllegalStateException("row mapper returned an unsupported value"));
    }

    /** 每个 Result 只创建一次映射计划；行级发布器只负责顺序物化和背压传递。 */
    static final class Mapper {

        private final DynamicRowFactory factory;
        private final SqlExecutionOptions options;
        private final Supplier<R2dbcLargeObjectScope> cleanupScope;
        private final LargeObjectPlan largeObjectPlan;

        private Mapper(DynamicRowFactory factory,
                       SqlExecutionOptions options,
                       Supplier<R2dbcLargeObjectScope> cleanupScope,
                       LargeObjectPlan largeObjectPlan) {
            this.factory = factory;
            // 原生查询和生成键都已处于 SQL/批量总时限内，不能让每行、每列 LOB 再启动计时器。
            // 仅在 LOB 布局创建时关闭局部执行超时，大小和清理预算原样保留；普通标量布局直接复用。
            this.options = largeObjectPlan != null && !options.timeout().isZero()
                    ? options.withTimeout(Duration.ZERO) : options;
            this.cleanupScope = cleanupScope;
            this.largeObjectPlan = largeObjectPlan;
        }

        Object mapValue(Row row) {
            if (largeObjectPlan == null) {
                return factory.read(row);
            }
            return mapLargeObjectRow(row);
        }

        @SuppressWarnings("unchecked")
        Mono<DynamicRow> map(Row row) {
            try {
                Object mapped = mapValue(row);
                return mapped instanceof DynamicRow direct ? Mono.just(direct) : (Mono<DynamicRow>) mapped;
            } catch (Throwable failure) {
                return Mono.error(failure);
            }
        }

        private Mono<DynamicRow> mapLargeObjectRow(Row row) {
            CapturingRow capturing = new CapturingRow(
                    row, largeObjectPlan, options.maxLargeObjectChars(), options.maxLargeObjectBytes());
            try {
                DynamicRow mapped = factory.read(capturing);
                return capturing.hasCaptured()
                        ? scope().materialize(mapped, options)
                        : Mono.just(mapped);
            } catch (Throwable failure) {
                return capturing.hasCaptured()
                        ? scope().discardCaptured(capturing.captured(), options, failure)
                                      .then(Mono.error(failure))
                        : Mono.error(failure);
            }
        }

        private R2dbcLargeObjectScope scope() {
            return Objects.requireNonNull(
                    cleanupScope.get(), "large object cleanup scope must not be null");
        }
    }

    /** 在驱动行读取期间立即登记已经交出的 locator，后续列失败时仍能释放它。 */
    private static final class CapturingRow implements Row {

        private final Row delegate;
        private final LargeObjectPlan plan;
        private final long maxChars;
        private final long maxBytes;
        private List<Object> captured;

        private CapturingRow(Row delegate, LargeObjectPlan plan, long maxChars, long maxBytes) {
            this.delegate = Objects.requireNonNull(delegate, "R2DBC row must not be null");
            this.plan = Objects.requireNonNull(plan, "large object plan must not be null");
            this.maxChars = maxChars;
            this.maxBytes = maxBytes;
        }

        @Override
        public RowMetadata getMetadata() {
            return delegate.getMetadata();
        }

        @Override
        public <T> T get(int index, Class<T> type) {
            return capture(delegate.get(index, type), plan.kind(index));
        }

        @Override
        public <T> T get(String name, Class<T> type) {
            return capture(delegate.get(name, type), plan.kind(name));
        }

        private <T> T capture(T value, LargeObjectKind kind) {
            if ((kind == LargeObjectKind.CHARACTER || kind == LargeObjectKind.UNKNOWN)
                    && maxChars > 0 && value instanceof CharSequence text && text.length() > maxChars) {
                throw new SqlLargeObjectLimitExceededException(
                        SqlLargeObjectLimitExceededException.Kind.CHARACTER, maxChars, text.length());
            }
            long binarySize = binarySize(value, kind);
            if (maxBytes > 0 && binarySize > maxBytes) {
                throw new SqlLargeObjectLimitExceededException(
                        SqlLargeObjectLimitExceededException.Kind.BINARY, maxBytes, binarySize);
            }
            if (value instanceof Blob || value instanceof Clob) {
                if (captured == null) {
                    captured = new ArrayList<>(1);
                }
                captured.add(value);
            }
            return value;
        }

        private static long binarySize(Object value, LargeObjectKind kind) {
            if (kind != LargeObjectKind.BINARY && kind != LargeObjectKind.UNKNOWN) {
                return -1L;
            }
            if (value instanceof byte[] bytes) {
                return bytes.length;
            }
            return value instanceof ByteBuffer buffer ? buffer.remaining() : -1L;
        }

        private boolean hasCaptured() {
            return captured != null;
        }

        private List<Object> captured() {
            return captured;
        }
    }
}
