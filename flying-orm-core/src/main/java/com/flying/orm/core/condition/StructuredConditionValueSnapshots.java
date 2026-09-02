package com.flying.orm.core.condition;

import com.flying.orm.core.internal.value.BindableValueSnapshots;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Owns the bounded JSON-shaped value graph accepted by structured conditions. */
final class StructuredConditionValueSnapshots {

    private StructuredConditionValueSnapshots() {
    }

    static Object snapshot(Object value) {
        try {
            return new StructuredValueSnapshotter().snapshot(value, 0);
        } catch (StructuredConditionException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw valueShapeNotAllowed();
        }
    }

    private static StructuredConditionException limitExceeded(StructuredConditionErrorCode code,
                                                               String boundary) {
        return StructuredConditionException.of(code,
                                               "conditions.value",
                                               "structured condition value graph exceeds " + boundary);
    }

    private static StructuredConditionException valueShapeNotAllowed() {
        return StructuredConditionException.of(StructuredConditionErrorCode.VALUE_SHAPE_NOT_ALLOWED,
                                               "conditions.value",
                                               "structured condition value shape is not allowed at conditions.value");
    }

    private static final class StructuredValueSnapshotter {
        private static final int MAX_DEPTH = 64;
        private static final int MAX_REFERENCES = 20_000;

        private final IdentityHashMap<Object, Object> copies = new IdentityHashMap<>();
        private final Set<Object> active = Collections.newSetFromMap(new IdentityHashMap<>());
        private int references;

        private Object snapshot(Object value, int depth) {
            if (value == null) return null;
            if (active.contains(value)) throw valueShapeNotAllowed();
            Object existing = copies.get(value);
            if (existing != null) return existing;
            if (value.getClass().isArray()) return array(value, depth);
            if (value instanceof Map<?, ?> map) return map(map, depth);
            if (value instanceof List<?> list) return list(list, depth);
            if (value instanceof Set<?> set) return set(set, depth);
            if (value instanceof Collection<?> collection) return collection(collection, depth);
            Object scalar = BindableValueSnapshots.immutableScalar(value, null);
            if (scalar != value) copies.put(value, scalar);
            return scalar;
        }

        private Object array(Object source, int depth) {
            requireDepth(depth);
            int length = Array.getLength(source);
            addReferences(length);
            Class<?> component = source.getClass().getComponentType();
            if (component.isPrimitive()) {
                Object copy = Array.newInstance(component, length);
                System.arraycopy(source, 0, copy, 0, length);
                copies.put(source, copy);
                return copy;
            }
            Object[] copy = new Object[length];
            copies.put(source, copy);
            active.add(source);
            try {
                for (int index = 0; index < length; index++) {
                    copy[index] = snapshot(Array.get(source, index), depth + 1);
                }
            } finally {
                active.remove(source);
            }
            return copy;
        }

        private Object map(Map<?, ?> source, int depth) {
            requireDepth(depth);
            addReferences(Math.multiplyExact(source.size(), 2));
            Map<Object, Object> copy = new LinkedHashMap<>(capacity(source.size()));
            Map<Object, Object> exposed = Collections.unmodifiableMap(copy);
            copies.put(source, exposed);
            active.add(source);
            try {
                source.forEach((key, value) -> {
                    if (key != null && (key.getClass().isArray()
                            || key instanceof Collection<?> || key instanceof Map<?, ?>)) {
                        throw valueShapeNotAllowed();
                    }
                    Object safeKey = snapshot(key, depth + 1);
                    if (copy.containsKey(safeKey)) throw valueShapeNotAllowed();
                    copy.put(safeKey, snapshot(value, depth + 1));
                });
            } finally {
                active.remove(source);
            }
            return exposed;
        }

        private Object list(List<?> source, int depth) {
            List<Object> copy = new ArrayList<>(collectionSize(source, depth));
            return fill(source, copy, Collections.unmodifiableList(copy), depth);
        }

        private Object set(Set<?> source, int depth) {
            Set<Object> copy = new LinkedHashSet<>(capacity(collectionSize(source, depth)));
            return fill(source, copy, Collections.unmodifiableSet(copy), depth);
        }

        private Object collection(Collection<?> source, int depth) {
            List<Object> copy = new ArrayList<>(collectionSize(source, depth));
            return fill(source, copy, Collections.unmodifiableCollection(copy), depth);
        }

        private int collectionSize(Collection<?> source, int depth) {
            requireDepth(depth);
            int size = source.size();
            addReferences(size);
            return size;
        }

        private Object fill(Collection<?> source,
                            Collection<Object> copy,
                            Object exposed,
                            int depth) {
            copies.put(source, exposed);
            active.add(source);
            try {
                for (Object value : source) copy.add(snapshot(value, depth + 1));
            } finally {
                active.remove(source);
            }
            if (copy.size() != source.size()) throw valueShapeNotAllowed();
            return exposed;
        }

        private void requireDepth(int depth) {
            if (depth > MAX_DEPTH) {
                throw limitExceeded(StructuredConditionErrorCode.DEPTH_EXCEEDED, "depth limit");
            }
        }

        private void addReferences(int count) {
            if (count < 0 || count > MAX_REFERENCES - references) {
                throw limitExceeded(StructuredConditionErrorCode.NODE_COUNT_EXCEEDED, "reference limit");
            }
            references += count;
        }

        private static int capacity(int size) {
            return size < 3 ? size + 1 : Math.min(Integer.MAX_VALUE, (int) (size / 0.75f) + 1);
        }
    }
}
