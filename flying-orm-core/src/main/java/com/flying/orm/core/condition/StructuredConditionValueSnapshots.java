package com.flying.orm.core.condition;

import com.flying.orm.core.internal.value.BindableValueSnapshots;

import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Owns JSON-shaped values; resource budgets belong to the caller's compilation policy. */
final class StructuredConditionValueSnapshots {

    private StructuredConditionValueSnapshots() {
    }

    static Object snapshot(Object value) {
        try {
            return new StructuredValueSnapshotter().snapshot(value);
        } catch (StructuredConditionException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw valueShapeNotAllowed();
        }
    }

    private static StructuredConditionException valueShapeNotAllowed() {
        return StructuredConditionException.of(StructuredConditionErrorCode.VALUE_SHAPE_NOT_ALLOWED,
                                               "conditions.value",
                                               "structured condition value shape is not allowed at conditions.value");
    }

    private static final class StructuredValueSnapshotter {
        private final IdentityHashMap<Object, Object> copies = new IdentityHashMap<>();
        private final Set<Object> active = Collections.newSetFromMap(new IdentityHashMap<>());
        private StructuredConditionSnapshotSet.Hashes setHashes;

        private StructuredConditionSnapshotSet.Hashes setHashes() {
            if (setHashes == null) setHashes = new StructuredConditionSnapshotSet.Hashes();
            return setHashes;
        }

        private Object snapshot(Object value) {
            ArrayDeque<ContainerFrame> pending = new ArrayDeque<>();
            Object result = copyValue(value, pending);
            while (!pending.isEmpty()) {
                ContainerFrame frame = pending.peek();
                if (!frame.hasNext()) {
                    result = frame.finish();
                    copies.put(frame.source, result);
                    active.remove(frame.source);
                    pending.pop();
                    if (!pending.isEmpty()) pending.peek().accept(result);
                    continue;
                }
                Object child = frame.nextValue(this);
                int depth = pending.size();
                Object copy = copyValue(child, pending);
                // A container publishes its completed copy when its frame is popped.
                if (pending.size() == depth) frame.accept(copy);
            }
            return result;
        }

        private Object copyValue(Object value, ArrayDeque<ContainerFrame> pending) {
            if (value == null) return null;
            if (active.contains(value)) throw valueShapeNotAllowed();
            Object existing = copies.get(value);
            if (existing != null) return existing;
            if (value.getClass().isArray() && value.getClass().getComponentType().isPrimitive()) {
                int length = Array.getLength(value);
                Object copy = Array.newInstance(value.getClass().getComponentType(), length);
                System.arraycopy(value, 0, copy, 0, length);
                copies.put(value, copy);
                return copy;
            }
            if (value.getClass().isArray() || value instanceof Map<?, ?> || value instanceof Collection<?>) {
                active.add(value);
                pending.push(new ContainerFrame(value, this));
                return null;
            }
            return scalar(value);
        }

        private Object scalar(Object value) {
            if (value == null) return null;
            Object existing = copies.get(value);
            if (existing != null) return existing;
            Object scalar = BindableValueSnapshots.immutableScalar(value, null);
            if (scalar != value) copies.put(value, scalar);
            return scalar;
        }
    }

    /** One active container, with only its next child scheduled at a time. */
    private static final class ContainerFrame {
        private final Object source;
        private final StructuredValueSnapshotter owner;
        private final Object[] array;
        private final Collection<Object> collection;
        private final Map<Object, Object> map;
        private final Object exposed;
        private final Iterator<?> iterator;
        private int index;
        private Object mapKey;

        private ContainerFrame(Object source, StructuredValueSnapshotter owner) {
            this.source = source;
            this.owner = owner;
            if (source.getClass().isArray()) {
                array = new Object[Array.getLength(source)];
                collection = null;
                map = null;
                exposed = array;
                iterator = null;
            } else if (source instanceof Map<?, ?> sourceMap) {
                array = null;
                collection = null;
                map = new LinkedHashMap<>(capacity(sourceMap.size()));
                exposed = Collections.unmodifiableMap(map);
                iterator = sourceMap.entrySet().iterator();
            } else {
                Collection<?> sourceCollection = (Collection<?>) source;
                array = null;
                map = null;
                if (source instanceof Set<?>) {
                    Set<Object> copy = new StructuredConditionSnapshotSet();
                    collection = copy;
                    exposed = Collections.unmodifiableSet(copy);
                } else {
                    List<Object> copy = new ArrayList<>(sourceCollection.size());
                    collection = copy;
                    exposed = source instanceof List<?>
                            ? Collections.unmodifiableList(copy) : Collections.unmodifiableCollection(copy);
                }
                iterator = sourceCollection.iterator();
            }
        }

        private boolean hasNext() {
            return array != null ? index < array.length : iterator.hasNext();
        }

        private Object nextValue(StructuredValueSnapshotter owner) {
            if (array != null) return Array.get(source, index);
            if (map == null) return iterator.next();
            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) iterator.next();
            Object key = entry.getKey();
            if (key != null && (key.getClass().isArray()
                    || key instanceof Collection<?> || key instanceof Map<?, ?>)) {
                throw valueShapeNotAllowed();
            }
            mapKey = owner.scalar(key);
            if (map.containsKey(mapKey)) throw valueShapeNotAllowed();
            return entry.getValue();
        }

        private void accept(Object value) {
            if (array != null) {
                array[index++] = value;
            } else if (map != null) {
                map.put(mapKey, value);
            } else if (collection instanceof StructuredConditionSnapshotSet set) {
                set.addOwned(value, owner.setHashes());
            } else {
                collection.add(value);
            }
        }

        private Object finish() {
            if (array != null) {
                Class<?> component = source.getClass().getComponentType();
                for (Object value : array) {
                    if (value != null && !component.isInstance(value)) return array;
                }
                Object typed = Array.newInstance(component, array.length);
                System.arraycopy(array, 0, typed, 0, array.length);
                return typed;
            }
            if (collection != null && collection.size() != ((Collection<?>) source).size()) {
                throw valueShapeNotAllowed();
            }
            if (collection instanceof StructuredConditionSnapshotSet set) {
                owner.setHashes().remember(exposed, set.constructionHash());
            }
            return exposed;
        }

        private static int capacity(int size) {
            return size < 3 ? size + 1 : (int) Math.min(Integer.MAX_VALUE, (long) (size / 0.75d) + 1);
        }
    }
}
