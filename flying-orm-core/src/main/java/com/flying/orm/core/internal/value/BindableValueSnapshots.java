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

    private BindableValueSnapshots() {
    }

    public static Object immutableValue(Object value) {
        if (value == null || !value.getClass().isArray()) {
            return immutableScalar(value, null);
        }
        return new SnapshotSession(true).snapshot(value, null);
    }

    /** Returns whether the supported bind value needs a defensive immutable snapshot. */
    public static boolean requiresImmutableSnapshot(Object value) {
        return value != null
                && (value.getClass().isArray()
                || value instanceof ByteBuffer
                || value instanceof CharSequence && !(value instanceof String)
                || value instanceof java.util.Date);
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
        return immutableScalar(value, arrayComponentType, BufferSnapshot.READABLE);
    }

    public static Object immutableScalar(Object value,
                                         Class<?> arrayComponentType,
                                         BufferSnapshot bufferSnapshot) {
        Objects.requireNonNull(bufferSnapshot, "buffer snapshot mode must not be null");
        if (value == null) {
            return null;
        }
        if (value instanceof ByteBuffer buffer) {
            return bufferSnapshot == BufferSnapshot.STATE
                    ? copyBufferState(buffer) : copyReadableBuffer(buffer);
        }
        if (value instanceof CharSequence text && !(text instanceof String)) {
            return copyText(text, arrayComponentType);
        }
        if (value instanceof java.util.Date date) {
            return copyLegacyTemporal(date);
        }
        return value;
    }

    public enum BufferSnapshot {
        READABLE,
        STATE
    }

    private static ByteBuffer copyReadableBuffer(ByteBuffer value) {
        ByteBuffer source = value.duplicate().order(value.order());
        ByteBuffer copy = ByteBuffer.allocate(source.remaining()).order(value.order());
        copy.put(source).flip();
        return copy.asReadOnlyBuffer().order(value.order());
    }

    private static ByteBuffer copyBufferState(ByteBuffer value) {
        ByteBuffer source = value.duplicate().order(value.order());
        int position = source.position();
        int limit = source.limit();
        source.position(0);
        ByteBuffer copy = ByteBuffer.allocate(limit).order(value.order());
        copy.put(source).flip().position(position);
        return copy;
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
        private final IdentityHashMap<Object, Object> copies = new IdentityHashMap<>();

        private SnapshotSession(boolean freezeScalars) {
            this.freezeScalars = freezeScalars;
        }

        private Object snapshot(Object value, Class<?> arrayComponentType) {
            if (value == null) {
                return null;
            }
            Object existing = copies.get(value);
            if (existing != null) {
                return existing;
            }
            if (value.getClass().isArray()) {
                return snapshotArray(value);
            }
            if (!freezeScalars) {
                return value;
            }
            Object result = immutableScalar(value, arrayComponentType);
            if (result != value) {
                copies.put(value, result);
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
