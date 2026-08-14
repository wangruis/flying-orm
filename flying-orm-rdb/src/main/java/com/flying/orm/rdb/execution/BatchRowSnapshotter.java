package com.flying.orm.rdb.execution;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.codec.SqlTypedValue;
import com.flying.orm.rdb.internal.InternalApi;
import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Parameters;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.flying.orm.rdb.execution.ProtectedBatchRows.add;
import static com.flying.orm.rdb.execution.ProtectedBatchRows.multiply;

/**
 * 批量输入的内部所有权边界；单次遍历冻结可变参数，并在每次分配前累计复制预算。
 *
 * @author wangr
 * @date 2026-08-13
 * @version v1.0
 */
@InternalApi
public final class BatchRowSnapshotter {
    private static final int MAX_DEPTH = 64;
    private static final Class<?> STANDARD_IN = Parameters.in(Object.class).getClass();
    private static final Class<?> STANDARD_IN_OUT = Parameters.inOut(Object.class).getClass();
    private final long maxCopyBytes;
    private final String limitName;
    private final IdentityHashMap<Object, Object> copies = new IdentityHashMap<>();
    private final IdentityHashMap<Object, Boolean> copying = new IdentityHashMap<>();
    private long copiedBytes;

    private BatchRowSnapshotter(long maxCopyBytes, String limitName) {
        this.maxCopyBytes = maxCopyBytes;
        this.limitName = limitName;
    }

    /** @return 与调用方参数图彻底分离的单行快照 */
    public static Object[] snapshot(Object[] row) {
        Object[] safeRow = Objects.requireNonNull(row, "batch row must not be null");
        if (BatchMemoryBudget.estimateShallowScalarRowBytes(safeRow) >= 0L) return safeRow.clone();
        return new BatchRowSnapshotter(Long.MAX_VALUE, "buffered bytes").snapshotRow(safeRow);
    }

    /** 在分配可变载荷副本前计费；消费者继续按快照后的统一行预算累计分片。 */
    public static Object[] snapshot(Object[] row, int parameterCount, long maxBufferedBytes,
                                    String limitName) {
        Object[] safeRow = Objects.requireNonNull(row, "batch row must not be null");
        if (maxBufferedBytes <= 0L) throw new IllegalArgumentException("batch snapshot byte limit must be positive");
        String safeName = Objects.requireNonNull(limitName, "batch snapshot limit name must not be null");
        if (BatchMemoryBudget.estimateShallowScalarRowBytes(safeRow) >= 0L) return safeRow.clone();
        ProtectedBatchRows.work(safeRow, parameterCount);
        return new BatchRowSnapshotter(maxBufferedBytes, safeName).snapshotRow(safeRow);
    }

    /** 同一次调用返回已取得所有权的参数行及其统一内存权重。 */
    public static Snapshot snapshotAndEstimate(Object[] row, int parameterCount, long maxBufferedBytes,
                                               String limitName) {
        Object[] safeRow = Objects.requireNonNull(row, "batch row must not be null");
        long scalarBytes = BatchMemoryBudget.estimateShallowScalarRowBytes(safeRow);
        if (scalarBytes >= 0L) return new Snapshot(safeRow.clone(), scalarBytes);
        Object[] snapshot = snapshot(safeRow, parameterCount, maxBufferedBytes, limitName);
        return new Snapshot(snapshot, ProtectedBatchRows.estimateRowBytes(snapshot, parameterCount));
    }

    /** 只在批量执行内核内部传递的已冻结行及内存权重。 */
    @InternalApi
    public record Snapshot(Object[] row, long estimatedBytes) {
    }

    private Object[] snapshotRow(Object[] row) {
        return (Object[]) copy(row, 0);
    }

    private void reserve(long bytes) {
        long total = add(copiedBytes, bytes);
        if (total == Long.MAX_VALUE || total > maxCopyBytes) {
            throw new BatchMemoryLimitExceededException(limitName, maxCopyBytes, total);
        }
        copiedBytes = total;
    }

    private Object copy(Object value, int depth) {
        if (value == null) return null;
        if (copies.containsKey(value)) return copies.get(value);
        if (copying.put(value, Boolean.TRUE) != null) {
            throw new IllegalArgumentException("cyclic batch values are not supported");
        }
        Object result;
        try {
            if (value instanceof byte[] bytes) {
                reserve(add(16L, bytes.length));
                result = bytes.clone();
            } else if (value instanceof ByteBuffer buffer) {
                result = snapshotBuffer(buffer);
            } else if (value instanceof CharSequence text && !(value instanceof String)) {
                result = snapshotText(text);
            } else if (depth > MAX_DEPTH) {
                throw new IllegalArgumentException("batch value nesting exceeds 64");
            } else if (value instanceof SqlTypedValue typed) {
                result = copyTypedValue(typed, depth);
            } else if (value instanceof Parameter parameter) {
                result = copyParameter(parameter, depth);
            } else if (value instanceof SqlRequest request) {
                result = copyRequest(request, depth);
            } else if (value instanceof ProtectedWriteWork work) {
                result = copyWork(work, depth);
            } else if (value instanceof ProtectedBatchRows.Metadata metadata) {
                result = ProtectedBatchRows.Metadata.owned(
                        (ProtectedWriteWork) copy(metadata.work(), depth + 1),
                        (Object[]) copy(metadata.rawReceiptParameters(), depth + 1));
            } else if (value.getClass().isArray()) {
                result = copyArray(value, depth);
            } else if (value instanceof List<?> list) {
                result = copyList(list, depth);
            } else if (value instanceof Map<?, ?> map) {
                result = copyMap(map, depth);
            } else if (value instanceof Collection<?>) {
                throw new IllegalArgumentException("unsupported mutable batch collection type");
            } else {
                result = value;
            }
        } finally {
            copying.remove(value);
        }
        if (result != value) copies.put(value, result);
        return result;
    }

    private ByteBuffer snapshotBuffer(ByteBuffer original) {
        ByteBuffer source = original.duplicate().order(original.order());
        reserve(add(24L, source.remaining()));
        ByteBuffer result = ByteBuffer.allocate(source.remaining()).order(source.order());
        result.put(source).flip();
        return result.asReadOnlyBuffer().order(source.order());
    }

    /** 只读取一次长度并在分配前计费，避免状态型文本绕过复制预算。 */
    private String snapshotText(CharSequence text) {
        int length = text.length();
        if (length < 0) throw new IllegalArgumentException("batch text length must not be negative");
        reserve(add(24L, multiply(length, 2L)));
        char[] characters = new char[length];
        for (int index = 0; index < length; index++) characters[index] = text.charAt(index);
        return new String(characters);
    }

    private Object copyTypedValue(SqlTypedValue typed, int depth) {
        Object payload = typed.value();
        Object result = copy(payload, depth + 1);
        if (result == payload) return typed;
        reserve(24L);
        return new SqlTypedValue(typed.kind(), result);
    }

    private Parameter copyParameter(Parameter parameter, int depth) {
        if (!isStandard(parameter)) {
            throw new IllegalArgumentException("custom R2DBC parameter is not supported in batch input");
        }
        Object payload = parameter.getValue();
        Object result = copy(payload, depth + 1);
        if (result == payload) return parameter;
        reserve(24L);
        return parameter.getClass() == STANDARD_IN_OUT
                ? Parameters.inOut(parameter.getType(), result)
                : Parameters.in(parameter.getType(), result);
    }

    private SqlRequest copyRequest(SqlRequest request, int depth) {
        List<Object> source = request.parameters();
        List<Object> target = null;
        for (int index = 0; index < source.size(); index++) {
            Object parameter = source.get(index);
            Object snapshot = copy(parameter, depth + 1);
            if (target != null) {
                target.add(snapshot);
            } else if (snapshot != parameter) {
                reserve(add(24L, multiply(source.size(), 8L)));
                target = new ArrayList<>(source.size());
                target.addAll(source.subList(0, index));
                target.add(snapshot);
            }
        }
        return target == null ? request : new SqlRequest(request.sql(), target, request.bindMarkerStyle());
    }

    @SuppressWarnings("unchecked")
    private ProtectedWriteWork copyWork(ProtectedWriteWork work, int depth) {
        SqlRequest sourceWrite = work.writeRequest();
        SqlRequest sourceQuery = work.ownerQuery();
        Map<String, Object> sourceOwner = work.knownOwnerInternal();
        SqlRequest write = (SqlRequest) copy(sourceWrite, depth + 1);
        SqlRequest query = (SqlRequest) copy(sourceQuery, depth + 1);
        Map<String, Object> owner = (Map<String, Object>) (Map<?, ?>) copy(sourceOwner, depth + 1);
        if (write == sourceWrite && query == sourceQuery && owner == sourceOwner) return work;
        reserve(64L);
        return new ProtectedWriteWork(work.kind(), write, query, work.ownerFields(), owner,
                work.ownerPredicateSql(), work.deleteSql(), work.insertSql(), work.fields());
    }

    private Object copyArray(Object value, int depth) {
        int length = Array.getLength(value);
        Class<?> component = value.getClass().getComponentType();
        int width = component == boolean.class || component == byte.class ? 1
                : component == char.class || component == short.class ? 2
                : component == int.class || component == float.class ? 4 : 8;
        reserve(add(component.isPrimitive() ? 16L : 24L, multiply(length, width)));
        Object result = Array.newInstance(component, length);
        if (component.isPrimitive()) {
            System.arraycopy(value, 0, result, 0, length);
            return result;
        }
        try {
            for (int index = 0; index < length; index++) {
                Array.set(result, index, copy(Array.get(value, index), depth + 1));
            }
            return result;
        } catch (ArrayStoreException error) {
            throw new IllegalArgumentException("batch array component cannot hold immutable snapshot", error);
        }
    }

    private List<Object> copyList(List<?> list, int depth) {
        int declaredSize = list.size();
        if (declaredSize < 0) throw new IllegalArgumentException("batch list size must not be negative");
        reserve(add(24L, multiply(declaredSize, 8L)));
        List<Object> result = new ArrayList<>(declaredSize);
        for (Object item : list) {
            if (result.size() == declaredSize) {
                throw new IllegalArgumentException("batch list size changed while snapshotting");
            }
            result.add(copy(item, depth + 1));
        }
        if (result.size() != declaredSize) {
            throw new IllegalArgumentException("batch list size changed while snapshotting");
        }
        return Collections.unmodifiableList(result);
    }

    private Map<Object, Object> copyMap(Map<?, ?> map, int depth) {
        int declaredSize = map.size();
        if (declaredSize < 0) throw new IllegalArgumentException("batch map size must not be negative");
        reserve(add(32L, multiply(declaredSize, 32L)));
        Map<Object, Object> result = new LinkedHashMap<>(declaredSize);
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (result.size() == declaredSize) {
                throw new IllegalArgumentException("batch map size changed while snapshotting");
            }
            Object key = copy(entry.getKey(), depth + 1);
            if (result.containsKey(key)) throw new IllegalArgumentException("batch map keys collide after snapshot");
            result.put(key, copy(entry.getValue(), depth + 1));
        }
        if (result.size() != declaredSize) {
            throw new IllegalArgumentException("batch map size changed while snapshotting");
        }
        return Collections.unmodifiableMap(result);
    }

    private static boolean isStandard(Parameter parameter) {
        Class<?> type = parameter.getClass();
        return type == STANDARD_IN || type == STANDARD_IN_OUT;
    }
}
