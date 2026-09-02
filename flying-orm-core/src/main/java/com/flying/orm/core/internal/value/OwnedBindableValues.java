package com.flying.orm.core.internal.value;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
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
        return values instanceof Published;
    }

    public static boolean requiresImmutableSnapshot(List<?> values) {
        return values instanceof Published published && published.requiresImmutableSnapshot;
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
            values.addAll(additions);
            if (additions instanceof Published published) {
                requiresImmutableSnapshot |= published.requiresImmutableSnapshot;
                return;
            }
            for (Object addition : additions) {
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
}
