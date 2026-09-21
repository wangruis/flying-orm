package com.flying.orm.core.internal.value;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;

/**
 * 内部逐项构建并一次发布的有序绑定参数所有权载体。
 *
 * @author wangr
 * @version v3.1
 */
public final class OwnedBindableValues {

    private OwnedBindableValues() {
    }

    public static Buffer buffer() {
        return new Buffer(0);
    }

    public static Buffer buffer(int expectedSize) {
        return new Buffer(expectedSize);
    }

    public static boolean isPublished(List<?> values) {
        return ownedValues(values) instanceof Published;
    }

    public static boolean requiresImmutableSnapshot(List<?> values) {
        List<?> owned = ownedValues(values);
        return owned instanceof Published published && published.requiresImmutableSnapshot;
    }

    /**
     * Returns the framework-owned list carried by a public defensive view. Ordinary lists are
     * returned unchanged. Internal execution paths use this after the public request boundary has
     * already established ownership.
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> ownedValues(List<T> values) {
        List<T> safeValues = Objects.requireNonNull(values, "bindable values must not be null");
        return safeValues instanceof DefensiveView view
                ? (List<T>) view.values : safeValues;
    }

    /**
     * Publishes mutable scalar values through a lazy defensive snapshot while preserving the
     * framework-owned list for trusted execution consumers.
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> defensiveView(List<T> values) {
        List<T> safeValues = Objects.requireNonNull(values, "bindable values must not be null");
        if (safeValues instanceof DefensiveView || !containsMutableValue(safeValues)) {
            return safeValues;
        }
        return (List<T>) new DefensiveView((List<Object>) safeValues);
    }

    private static boolean containsMutableValue(List<?> values) {
        List<?> owned = ownedValues(values);
        if (owned instanceof Published published) {
            return published.requiresImmutableSnapshot;
        }
        for (Object value : owned) {
            if (BindableValueSnapshots.requiresImmutableSnapshot(value)) {
                return true;
            }
        }
        return false;
    }

    /** Marks a newly created, framework-owned list without copying its elements again. */
    public static List<Object> publishOwned(List<Object> values) {
        List<Object> safeValues = Objects.requireNonNull(values, "owned bindable values must not be null");
        if (safeValues instanceof Published) {
            return safeValues;
        }
        boolean mutable = false;
        for (Object value : safeValues) {
            if (BindableValueSnapshots.requiresImmutableSnapshot(value)) {
                mutable = true;
                break;
            }
        }
        return new Published(safeValues, mutable);
    }

    public static final class Buffer {

        private final List<Object> values;
        private boolean published;
        private boolean requiresImmutableSnapshot;

        private Buffer(int expectedSize) {
            if (expectedSize < 0) {
                throw new IllegalArgumentException("owned bindable value capacity must not be negative");
            }
            values = new ArrayList<>(expectedSize);
        }

        public void add(Object value) {
            requireOpen();
            values.add(value);
            requiresImmutableSnapshot |= BindableValueSnapshots.requiresImmutableSnapshot(value);
        }

        public void addAll(List<?> additions) {
            requireOpen();
            List<?> owned = ownedValues(additions);
            values.addAll(owned);
            if (owned instanceof Published published) {
                requiresImmutableSnapshot |= published.requiresImmutableSnapshot;
                return;
            }
            for (Object addition : owned) {
                requiresImmutableSnapshot |= BindableValueSnapshots.requiresImmutableSnapshot(addition);
            }
        }

        public int size() {
            return values.size();
        }

        public void truncate(int size) {
            requireOpen();
            if (size < 0 || size > values.size()) {
                throw new IndexOutOfBoundsException("owned bindable value size is out of range");
            }
            values.subList(size, values.size()).clear();
        }

        public List<Object> publish() {
            requireOpen();
            published = true;
            return new Published(values, requiresImmutableSnapshot);
        }

        private void requireOpen() {
            if (published) {
                throw new IllegalStateException("owned bindable values are already published");
            }
        }
    }

    private static final class Published extends AbstractList<Object> implements RandomAccess {

        private final List<Object> values;
        private final boolean requiresImmutableSnapshot;

        private Published(List<Object> values, boolean requiresImmutableSnapshot) {
            this.values = values;
            this.requiresImmutableSnapshot = requiresImmutableSnapshot;
        }

        @Override
        public Object get(int index) {
            return values.get(index);
        }

        @Override
        public int size() {
            return values.size();
        }
    }

    private static final class DefensiveView extends AbstractList<Object> implements RandomAccess {

        private final List<Object> values;
        private volatile List<Object> snapshot;

        private DefensiveView(List<Object> values) {
            this.values = values;
        }

        @Override
        public Object get(int index) {
            return snapshot().get(index);
        }

        @Override
        public int size() {
            return values.size();
        }

        private List<Object> snapshot() {
            List<Object> current = snapshot;
            if (current == null) {
                current = BindableValueSnapshots.immutableValues(values);
                snapshot = current;
            }
            return current;
        }
    }
}
