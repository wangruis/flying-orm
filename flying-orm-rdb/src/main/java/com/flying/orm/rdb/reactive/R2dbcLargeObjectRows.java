package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.result.DynamicRowFactory;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Type;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

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
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options,
                                                                  "sql execution options must not be null");
        R2dbcLargeObjectScope safeScope = Objects.requireNonNull(cleanupScope,
                                                                 "large object cleanup scope must not be null");
        AtomicReference<Mapper> mapper = new AtomicReference<>();
        Flux<Object> mappedRows = Flux.from(result.map((row, metadata) ->
                mapper(row, metadata, safeOptions, safeScope, mapper).mapValue(row)));
        Flux<DynamicRow> rows = mappedRows.concatMap(R2dbcLargeObjectRows::materializedRow, 1);
        return rows.onErrorResume(failure -> {
            VirtualMachineError fatal = ReactiveSqlExecutionProtection.findVirtualMachineError(failure);
            return fatal == null ? Flux.error(failure) : Flux.error(fatal);
        });
    }

    static Mono<DynamicRow> map(Result.RowSegment segment,
                                SqlExecutionOptions options,
                                R2dbcLargeObjectScope cleanupScope) {
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
        RowMetadata safeMetadata = Objects.requireNonNull(metadata, "row metadata must not be null");
        return new Mapper(DynamicRowFactory.from(safeMetadata),
                          Objects.requireNonNull(options, "sql execution options must not be null"),
                          Objects.requireNonNull(cleanupScope, "large object cleanup scope must not be null"),
                          mayContainLargeObjects(safeMetadata));
    }

    private static Mapper mapper(Row row,
                                 RowMetadata metadata,
                                 SqlExecutionOptions options,
                                 R2dbcLargeObjectScope cleanupScope,
                                 AtomicReference<Mapper> reference) {
        Mapper current = reference.get();
        if (current != null) {
            return current;
        }
        Mapper created = mapper(metadata, options, cleanupScope);
        return reference.compareAndSet(null, created) ? created : reference.get();
    }

    private static boolean mayContainLargeObjects(RowMetadata metadata) {
        for (io.r2dbc.spi.ColumnMetadata column : metadata.getColumnMetadatas()) {
            Type type = column.getType();
            if (type == R2dbcType.BLOB || type == R2dbcType.CLOB || type == R2dbcType.NCLOB) {
                return true;
            }
            Class<?> javaType = column.getJavaType();
            if (javaType == null && type != null) {
                javaType = type.getJavaType();
            }
            if (javaType == null || javaType == Object.class
                    || Blob.class.isAssignableFrom(javaType) || Clob.class.isAssignableFrom(javaType)) {
                return true;
            }
        }
        return false;
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
        private final R2dbcLargeObjectScope cleanupScope;
        private final boolean mayContainLargeObjects;

        private Mapper(DynamicRowFactory factory,
                       SqlExecutionOptions options,
                       R2dbcLargeObjectScope cleanupScope,
                       boolean mayContainLargeObjects) {
            this.factory = factory;
            this.options = options;
            this.cleanupScope = cleanupScope;
            this.mayContainLargeObjects = mayContainLargeObjects;
        }

        Object mapValue(Row row) {
            if (!mayContainLargeObjects) {
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
            CapturingRow capturing = new CapturingRow(row);
            try {
                DynamicRow mapped = factory.read(capturing);
                return capturing.hasCaptured() ? cleanupScope.materialize(mapped, options) : Mono.just(mapped);
            } catch (Throwable failure) {
                return capturing.hasCaptured()
                        ? cleanupScope.discardCaptured(capturing.captured(), options, failure)
                                      .then(Mono.error(failure))
                        : Mono.error(failure);
            }
        }
    }

    /** 在驱动行读取期间立即登记已经交出的 locator，后续列失败时仍能释放它。 */
    private static final class CapturingRow implements Row {

        private final Row delegate;
        private List<Object> captured;

        private CapturingRow(Row delegate) {
            this.delegate = Objects.requireNonNull(delegate, "R2DBC row must not be null");
        }

        @Override
        public RowMetadata getMetadata() {
            return delegate.getMetadata();
        }

        @Override
        public <T> T get(int index, Class<T> type) {
            return capture(delegate.get(index, type));
        }

        @Override
        public <T> T get(String name, Class<T> type) {
            return capture(delegate.get(name, type));
        }

        private <T> T capture(T value) {
            if (value instanceof Blob || value instanceof Clob) {
                if (captured == null) {
                    captured = new ArrayList<>(1);
                }
                captured.add(value);
            }
            return value;
        }

        private boolean hasCaptured() {
            return captured != null;
        }

        private List<Object> captured() {
            return captured;
        }
    }
}
