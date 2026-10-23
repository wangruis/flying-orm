package com.flying.orm.core.internal.value;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;

/**
 * Freezes the small set of mutable scalar values supported by SQL binding.
 *
 * <p>This class deliberately does not traverse maps, collections or arbitrary application
 * objects. Those shapes only have meaning at a field-aware boundary such as JSON, SQL ARRAY or
 * VECTOR processing.</p>
 *
 * @author wangr
 * @version v3.1
 */
public final class BindableValueSnapshots {

    /** 快照规则取决于实际类型，不随对象内容变化；ClassValue 不阻止业务类型卸载。 */
    private static final ClassValue<Boolean> SNAPSHOT_TYPES = new ClassValue<>() {
        @Override
        protected Boolean computeValue(Class<?> type) {
            return type.isArray()
                    || ByteBuffer.class.isAssignableFrom(type)
                    || CharSequence.class.isAssignableFrom(type) && type != String.class
                    || java.util.Date.class.isAssignableFrom(type);
        }
    };

    private BindableValueSnapshots() {
    }

    public static Object immutableValue(Object value) {
        if (value == null || !value.getClass().isArray()) {
            return immutableScalar(value, null);
        }
        return new SnapshotSession(true).snapshot(value, null);
    }

    /**
     * 快照尚未交给 codec 的逻辑值。已知可变标量仍隔离，但文本的领域类型不能提前变成 String。
     * 应用自定义标量沿用可信交接约定；这里不猜测其内部结构，也不代替应用克隆业务对象。
     */
    public static Object logicalValue(Object value) {
        if (value == null || !value.getClass().isArray()) {
            return logicalScalar(value);
        }
        return new SnapshotSession(true, true).snapshot(value, null);
    }

    /** 同一逻辑值序列共享数组复制会话，保留元素的领域类型与原有共享关系。 */
    public static List<Object> logicalValues(List<?> values) {
        Objects.requireNonNull(values, "logical values must not be null");
        SnapshotSession session = new SnapshotSession(true, true);
        List<Object> snapshot = new ArrayList<>(values.size());
        values.forEach(value -> snapshot.add(session.snapshot(value, null)));
        return Collections.unmodifiableList(snapshot);
    }

    /** 逻辑标量的独立副本；只有真正选中标准文本 codec 后才允许把文本转为 String。 */
    public static Object logicalScalar(Object value) {
        if (!requiresImmutableSnapshot(value)) {
            return value;
        }
        if (value instanceof CharSequence text && !(text instanceof String)) {
            if (text instanceof StringBuilder) return new StringBuilder(text);
            if (text instanceof StringBuffer) return new StringBuffer(text);
            if (text instanceof java.nio.CharBuffer) {
                return java.nio.CharBuffer.wrap(text.toString()).asReadOnlyBuffer();
            }
            return text;
        }
        return copyMutableScalar(value, null);
    }

    /** Returns whether the supported bind value needs a defensive immutable snapshot. */
    public static boolean requiresImmutableSnapshot(Object value) {
        return value != null && SNAPSHOT_TYPES.get(value.getClass());
    }

    public static List<Object> immutableValues(List<?> values) {
        Objects.requireNonNull(values, "bind values must not be null");
        SnapshotSession session = new SnapshotSession(true);
        List<Object> snapshot = new ArrayList<>(values.size());
        values.forEach(value -> snapshot.add(session.snapshot(value, null)));
        return Collections.unmodifiableList(snapshot);
    }

    /** Copies array payloads without claiming ownership of other scalar types. */
    public static Object arrayGraph(Object value) {
        if (value == null || !value.getClass().isArray()) {
            return value;
        }
        return new SnapshotSession(false).snapshot(value, null);
    }

    public static Object immutableScalar(Object value, Class<?> arrayComponentType) {
        return requiresImmutableSnapshot(value) ? copyMutableScalar(value, arrayComponentType) : value;
    }

    private static Object copyMutableScalar(Object value, Class<?> arrayComponentType) {
        if (value instanceof ByteBuffer buffer) {
            return copyReadableBuffer(buffer);
        }
        if (value instanceof CharSequence text && !(text instanceof String)) {
            return copyText(text, arrayComponentType);
        }
        if (value instanceof java.util.Date date) {
            return copyLegacyTemporal(date);
        }
        return value;
    }

    private static ByteBuffer copyReadableBuffer(ByteBuffer value) {
        ByteBuffer source = value.duplicate().order(value.order());
        ByteBuffer copy = ByteBuffer.allocate(source.remaining()).order(value.order());
        copy.put(source).flip();
        return copy.asReadOnlyBuffer().order(value.order());
    }

    private static Object copyText(CharSequence value, Class<?> componentType) {
        if (componentType == null || componentType.isAssignableFrom(String.class)) {
            return value.toString();
        }
        if (componentType == StringBuilder.class) {
            return new StringBuilder(value);
        }
        if (componentType == StringBuffer.class) {
            return new StringBuffer(value);
        }
        if (componentType == java.nio.CharBuffer.class) {
            return java.nio.CharBuffer.wrap(value.toString()).asReadOnlyBuffer();
        }
        throw new IllegalArgumentException("mutable text array component type cannot be snapshotted safely");
    }

    private static Object copyLegacyTemporal(java.util.Date value) {
        java.util.Date copy = (java.util.Date) value.clone();
        if (copy == value || copy.getClass() != value.getClass()) {
            throw new IllegalArgumentException("mutable date value cannot be snapshotted safely");
        }
        return copy;
    }

    private static final class SnapshotSession {
        private final boolean freezeScalars;
        private final boolean logicalValues;
        private IdentityHashMap<Object, Object> copies;
        private IdentityHashMap<Object, Object> typedTextCopies;

        private SnapshotSession(boolean freezeScalars) {
            this(freezeScalars, false);
        }

        private SnapshotSession(boolean freezeScalars, boolean logicalValues) {
            this.freezeScalars = freezeScalars;
            this.logicalValues = logicalValues;
        }

        private Object snapshot(Object value, Class<?> arrayComponentType) {
            if (!requiresImmutableSnapshot(value)) {
                return value;
            }
            if (copies == null) {
                copies = new IdentityHashMap<>();
            }
            IdentityHashMap<Object, Object> snapshots = copies;
            if (freezeScalars && !logicalValues && value instanceof CharSequence && !(value instanceof String)
                    && arrayComponentType != null && !arrayComponentType.isAssignableFrom(String.class)) {
                // 标量文本绑定为 String，强类型文本数组保留元素类型；两种表示不能共用副本。
                if (typedTextCopies == null) {
                    typedTextCopies = new IdentityHashMap<>();
                }
                snapshots = typedTextCopies;
            }
            Object existing = snapshots.get(value);
            if (existing != null) {
                return existing;
            }
            if (value.getClass().isArray()) {
                return snapshotArray(value);
            }
            if (!freezeScalars) {
                return value;
            }
            Object result = logicalValues ? logicalScalar(value) : immutableScalar(value, arrayComponentType);
            if (result != value) {
                snapshots.put(value, result);
            }
            return result;
        }

        private Object snapshotArray(Object source) {
            Object existing = copies.get(source);
            if (existing != null) {
                return existing;
            }
            int length = Array.getLength(source);
            Class<?> component = source.getClass().getComponentType();
            Object target = Array.newInstance(component, length);
            copies.put(source, target);
            if (component.isPrimitive()) {
                System.arraycopy(source, 0, target, 0, length);
                return target;
            }
            for (int index = 0; index < length; index++) {
                Object item = Array.get(source, index);
                if (item instanceof Object[]) {
                    throw new IllegalArgumentException(
                            "nested object arrays require a field-aware codec");
                }
                try {
                    Array.set(target, index, snapshot(item, component));
                } catch (ArrayStoreException failure) {
                    throw new IllegalArgumentException(
                            "object array cannot hold immutable bind value", failure);
                }
            }
            return target;
        }
    }
}
